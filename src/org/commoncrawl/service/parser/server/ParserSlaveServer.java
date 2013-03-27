/**
 * Copyright 2008 - CommonCrawl Foundation
 * 
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 **/

package org.commoncrawl.service.parser.server;


import java.net.URL;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.async.Callback;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.rpc.base.internal.AsyncClientChannel;
import org.commoncrawl.rpc.base.internal.AsyncContext;
import org.commoncrawl.rpc.base.internal.AsyncServerChannel;
import org.commoncrawl.rpc.base.internal.NullMessage;
import org.commoncrawl.rpc.base.internal.AsyncRequest.Status;
import org.commoncrawl.rpc.base.shared.RPCException;
import org.commoncrawl.server.CommonCrawlServer;
import org.commoncrawl.service.parser.ParseRequest;
import org.commoncrawl.service.parser.ParseResult;
import org.commoncrawl.service.parser.ParserServiceSlave;
import org.commoncrawl.service.parser.SlaveStatus;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.FlexBuffer;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

/**
 * 
 * @author rana
 *
 */
public class ParserSlaveServer extends CommonCrawlServer
 implements ParserServiceSlave, AsyncServerChannel.ConnectionCallback{


  private static final Log LOG = LogFactory.getLog(ParserSlaveServer.class);
  private static final int MAX_QUEUE_SIZE_DEFAULT = 10;
  private static final int DEFAULT_WORKER_THREAD_COUNT = 5;
  private int max_queue_size = MAX_QUEUE_SIZE_DEFAULT;
  private int thread_count = DEFAULT_WORKER_THREAD_COUNT;

  private class Request {
    public Request(AsyncContext<ParseRequest, ParseResult> request) { 
      requestContext = request;
    }
    public AsyncContext<ParseRequest, ParseResult> requestContext;
  }
  
  private LinkedBlockingDeque<Request> requestQueue 
    = new LinkedBlockingDeque<Request>();
  
  private HashSet<Long> _activeChannels = new HashSet<Long>();
  private Thread        _parserThreads[];
  private Semaphore     _threadSemaphore = null;
  private AtomicInteger _activeThreads = new AtomicInteger();
  
  @Override
  protected String getDefaultDataDir() {
    return CrawlEnvironment.DEFAULT_DATA_DIR;
  }

  @Override
  protected String getDefaultHttpInterface() {
    return CrawlEnvironment.DEFAULT_HTTP_INTERFACE;
  }

  @Override
  protected int getDefaultHttpPort() {
    return CrawlEnvironment.DEFAULT_PARSER_SLAVE_HTTP_PORT;
  }

  @Override
  protected String getDefaultLogFileName() {
    return "historyserver.log";
  }

  @Override
  protected String getDefaultRPCInterface() {
    return CrawlEnvironment.DEFAULT_RPC_INTERFACE;
  }

  @Override
  protected int getDefaultRPCPort() {
    return CrawlEnvironment.DEFAULT_PARSER_SLAVE_RPC_PORT;
  }

  @Override
  protected String getWebAppName() {
    return CrawlEnvironment.DEFAULT_PARSER_SLAVE_WEBAPP_NAME;
  }

  @Override
  protected boolean initServer() {
    try {
      // create server channel ... 
      AsyncServerChannel channel = new AsyncServerChannel(this, this.getEventLoop(), this.getServerAddress(),this);
      
      // register RPC services it supports ... 
      registerService(channel,ParserServiceSlave.spec);
            
    } catch (Exception e) {
      LOG.error(CCStringUtils.stringifyException(e));
      return false;
    }
    return true;
  }
  
  /** do a clean shutdown (if possible) **/
  @Override
  public void stop() {
    
    // ok, wait to grab the checkpoint thread semaphore 
    LOG.info("Server Shutdown Detected.");
    // ok safe to call super now ... 
    super.stop();
  }
      
  
  @Override
  protected boolean parseArguments(String[] argv) {
    
    for(int i=0; i < argv.length;++i) {
      if (argv[i].equalsIgnoreCase("--queue_size")) { 
        max_queue_size = Integer.parseInt(argv[++i]);
        if (max_queue_size < 1) { 
          throw new RuntimeException("Invalid Queue Size");
        }
      }
      else if (argv[i].equalsIgnoreCase("--worker_threads")) { 
        thread_count = Integer.parseInt(argv[++i]);
        if (thread_count < 1) { 
          throw new RuntimeException("Invalid Thread Count");
        }
      }
    }
    return true;
  }

  @Override
  protected void printUsage() {
    
  }

  @Override
  protected boolean startDaemons() {
    _parserThreads = new Thread[thread_count];
    _threadSemaphore = new Semaphore(-(thread_count - 1));
    for (int i=0;i<thread_count;++i) { 
      _parserThreads[i] = new Thread(new Runnable() {

        @Override
        public void run() {
          
          try { 
            while (true) { 
            try {
              final Request request = requestQueue.take();
              if (request.requestContext == null) { 
                LOG.info("Parser Thread:"+ Thread.currentThread().getId() 
                    +" Exiting.");
                return;
              }
              else { 
                ParseRequest parseRequest= request.requestContext.getInput();
                ParseResult parseResult = request.requestContext.getOutput();
  
                LOG.info("Parser Thread:" + Thread.currentThread().getId()
                    + " got request for url:"+ parseRequest.getDocURL());
                
                try {

                  _activeThreads.incrementAndGet();
                   
                  URL url = new URL(parseRequest.getDocURL());
                  ParseWorker worker = new ParseWorker();
                  worker.parseDocument(
                      request.requestContext.getOutput(),
                      parseRequest.getDomainId(),
                      parseRequest.getDocId(),
                      url,
                      parseRequest.getDocHeaders(),
                      new FlexBuffer(
                          parseRequest.getDocContent().getReadOnlyBytes(),
                          parseRequest.getDocContent().getOffset(),
                          parseRequest.getDocContent().getCount()));
                      
                  
                }
                catch (Exception e) { 
                  LOG.error(CCStringUtils.stringifyException(e));
                  parseResult.setParseSuccessful(false);
                  if (parseResult.getParseFailureReason().length() == 0) {
                    parseResult.setParseFailureReason(CCStringUtils.stringifyException(e));
                  }
                }
                finally { 
                  _activeThreads.decrementAndGet();
                }
                getEventLoop().queueAsyncCallback(new Callback() {
                  
                  @Override
                  public void execute() {
                    try {
                      request.requestContext.completeRequest();
                    } catch (RPCException e) {
                      LOG.error("RPC Exception when processing ParseRequest:"  + CCStringUtils.stringifyException(e));
                    }
                  }
                });
              }
            }
            catch (InterruptedException e) {
            }
            }
          }
          finally {
            _activeThreads.decrementAndGet();
            _threadSemaphore.release();
          }
      }
      });
      _parserThreads[i].start();
    }
    return true;
  }

  @Override
  protected void stopDaemons() {
    if (_parserThreads != null) {
      LOG.info("Stop Daemons Called. Sending Threads Shutdown request");
      for (int i=0;i<_parserThreads.length;++i) { 
        try {
          requestQueue.put(new Request(null));
        } catch (InterruptedException e) {
        }
      }
      LOG.info("Waiting for threads to die");
      // now try to acquire shutdown sempahore 
      _threadSemaphore.acquireUninterruptibly();
      LOG.info("Parser Threads are dead");
    }
  }

  @Override
  public void IncomingClientConnected(AsyncClientChannel channel) {
    synchronized(this) { 
      _activeChannels.add(channel.getChannelId());
    }
  }

  @Override
  public void IncomingClientDisconnected(AsyncClientChannel channel) {
    synchronized(this) { 
      _activeChannels.remove(channel.getChannelId());
    }
  }

  @Override
  public void queryStatus(AsyncContext<NullMessage, SlaveStatus> rpcContext)
      throws RPCException {
      
    rpcContext.getOutput().setActive(true);
    //rpcContext.getOutput().setLoad(
       // ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage());
    
    rpcContext.getOutput().setActiveDocs(_activeThreads.get());
    rpcContext.getOutput().setQueuedDocs(requestQueue.size());
    rpcContext.setStatus(Status.Success);
    rpcContext.completeRequest();
  }

  @Override
  public void parseDocument(AsyncContext<ParseRequest, ParseResult> rpcContext)
      throws RPCException {
    if (requestQueue.size() >= max_queue_size) {  
      rpcContext.setErrorDesc("Queue is Full.Failing Request.");
      rpcContext.setStatus(Status.Error_RequestFailed);
      rpcContext.completeRequest();
    }
    else { 
      requestQueue.addLast(new Request(rpcContext));
    }
  }
  
  
  public static void main(String[] args) {
    Multimap<String,String> options = TreeMultimap.create();
    for (int i=0;i<args.length;++i) { 
      String optionName = args[i];
      if (++i != args.length) { 
        String optionValue = args[i];
        options.put(optionName, optionValue);
      }
    }
    options.removeAll("--server");
    options.put("--server",ParserSlaveServer.class.getName());
    
    Collection<Entry<String,String>> entrySet = options.entries();
    String finalArgs[] = new String[entrySet.size() * 2];
    int index = 0;
    for (Entry entry : entrySet) { 
      finalArgs[index++] = (String)entry.getKey();
      finalArgs[index++] = (String)entry.getValue();
    }
    
    try {
      CommonCrawlServer.main(finalArgs);
    } catch (Exception e) {
      LOG.error(CCStringUtils.stringifyException(e));
    }
  }
}
