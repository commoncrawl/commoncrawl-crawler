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

package org.commoncrawl.service.parser.client;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.StringTokenizer;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.service.parser.ParseRequest;
import org.commoncrawl.service.parser.ParseResult;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.FlexBuffer;

import com.google.common.io.ByteProcessor;
import com.google.common.io.ByteStreams;
import com.google.common.io.InputSupplier;

public class Dispatcher {

  public static final Log LOG = LogFactory.getLog(Dispatcher.class);
  private EventLoop _eventLoop;
  private ArrayList<ParserNode>     _nodeList = new ArrayList<ParserNode>();
  private final PriorityQueue<ParserNode> _onlineNodes = new PriorityQueue<ParserNode>();
  private final ReentrantLock lock = new ReentrantLock(true);
  private final Condition notEmpty = lock.newCondition();
  private AtomicBoolean online = new AtomicBoolean(true);

  /**
   * 
   * @param eventLoop
   * @param slavesFile
   * @throws IOException
   */
  public Dispatcher(EventLoop eventLoop,String slavesFile) throws IOException  { 
    _eventLoop = eventLoop;
    
    LOG.info("Loading Slaves File from:" + slavesFile);
    InputStream stream =null;
    URL resourceURL = CrawlEnvironment.getHadoopConfig().getResource(slavesFile);

    if (resourceURL != null) {
      stream = resourceURL.openStream();
    }
    // try as filename 
    else { 
      LOG.info("Could not load resource as an URL. Trying as an absolute pathname");
      stream = new FileInputStream(new File(slavesFile));
    }
    if (stream == null) {
      throw new FileNotFoundException();
    }
    Reader reader = new InputStreamReader(new BufferedInputStream(stream));
    try {
      parseSlavesFile(reader);
    }
    finally { 
      reader.close();
    }
  }
  
  public Dispatcher(EventLoop eventLoop,Reader slavesFileReader) throws IOException { 
    _eventLoop = eventLoop;
    parseSlavesFile(slavesFileReader);
  }

  /**
   * issue a blocking request to the next least loaded parser node .. 
   * 
   * @param request
   * @return
   */
  public ParseResult dispatchRequest(ParseRequest request){ 
    // block and wait for a node .. 
    ParserNode candidate = take();
    
    LOG.info("TID:" + Thread.currentThread().getId() + " Candidate is:" + 
        ((candidate != null) ? candidate.getNodeName() : "NULL"));
    if (candidate != null) { 
      // ok .. got node ... go ahead and dispatch
      try {
        return candidate.dispatchRequest(request);
      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
      }
    }
    else { 
      LOG.error("Unable to get ParseNode candidate for URL:" + request.getDocURL());
    }
    return null;
  }


  public ReentrantLock getQueueLock() { 
    return lock;
  }

  private void parseSlavesFile(Reader srcReader)throws IOException {

    if (srcReader == null) { 
      throw new IOException("Null SlaveFile Reader Specified!");
    }

    BufferedReader reader = new BufferedReader(srcReader);

    String hostAndPort = null;

    LOG.info("Loading slaves file");
    while ((hostAndPort = reader.readLine()) != null) {
      if (!hostAndPort.startsWith("#")) {
        StringTokenizer tokenizer = new StringTokenizer(hostAndPort,":");
        if (tokenizer.countTokens() != 2){
          throw new IOException("Invalid Node Entry:" + hostAndPort + " in nodes File");
        }
        else {
          String nodeName = tokenizer.nextToken();
          int    port     = Integer.parseInt(tokenizer.nextToken());

          ParserNode node = new ParserNode(this,_eventLoop,nodeName,
              new InetSocketAddress(InetAddress.getByName(nodeName),port));


          try { 
            node.startup();
            LOG.info("Adding node:" + nodeName);
            _nodeList.add(node);            
          }
          catch (IOException e) { 
            LOG.error("Unable to add node:" + nodeName);
            LOG.error(CCStringUtils.stringifyException(e));
          }
        }
      }
    }
  }

  public void nodeOnline(ParserNode theNode) throws IOException {
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      boolean ok = _onlineNodes.add(theNode);
      assert ok;
      notEmpty.signal();
    } finally {
      lock.unlock();
    }
  }

  public void nodeOffline(ParserNode theNode) {
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      _onlineNodes.remove(theNode);
    } finally {
      lock.unlock();
    }
  }

  public void nodeStatusChanged(ParserNode theNode) {
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      _onlineNodes.remove(theNode);
      _onlineNodes.add(theNode);
      notEmpty.signal();
    } finally {
      lock.unlock();
    }    
  }
  public ParserNode take(){
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      try {
        while (_onlineNodes.size() == 0)
          notEmpty.await();
      } catch (InterruptedException ie) {
        if (online.get()) { 
          notEmpty.signal(); // propagate to non-interrupted thread
        }
      }
      ParserNode x = _onlineNodes.poll();
      x.touch();
      assert x != null;
      _onlineNodes.add(x);
      return x;
    } finally {
      lock.unlock();
    }
  }

  private static final int TEST_THREAD_COUNT = 100;
  private static final int ITERATIONS_PER_THREAD = 1000;
  public static void main(String[] args) throws IOException {
    Configuration conf = new Configuration();
    CrawlEnvironment.setHadoopConfig(conf);
    String baseURL = "http://unknown.com/";
    if (args.length != 0) { 
      baseURL = args[0];
    }
    URL baseURLObj;
    try {
      baseURLObj = new URL(baseURL);
    } catch (MalformedURLException e2) {
      throw new IOException("Invalid Base Link");
    }
    final URL finalBaseURL = (baseURLObj != null) ? baseURLObj : null;
    final DataOutputBuffer headerBuffer = new DataOutputBuffer();
    final DataOutputBuffer contentBuffer = new DataOutputBuffer();
    
    try {
      ByteStreams.readBytes(
          new InputSupplier<InputStream>() {

            @Override
            public InputStream getInput() throws IOException {
              return System.in;
            }
          }
          ,new ByteProcessor<Long>() {

        @Override
        public Long getResult() {
          return 0L;
        }

        int currLineCharCount = 0;
        boolean processingHeaders = true;
        @Override
        public boolean processBytes(byte[] buf, int start, int length)
            throws IOException {
          
          if (processingHeaders) { 
            int current = start;
            int end   = current + length;
            while (processingHeaders && current != end) {
              if (buf[current] != '\r' && buf[current] != '\n') { 
                currLineCharCount++;
              }
              else if (buf[current] == '\n') { 
                if (currLineCharCount == 0){ 
                  headerBuffer.write(buf,start,current - start + 1);
                  processingHeaders = false;
                }
                currLineCharCount = 0;
              }
              current++;
            }
            if (processingHeaders) { 
              headerBuffer.write(buf,start,length);
            }
            else { 
              length -= current-start;
              start = current;
            }
          }
          if (!processingHeaders) { 
            contentBuffer.write(buf,start,length);
          }
          return true;
        }
      });
      
      LOG.info("HEADER LEN:" + headerBuffer.getLength());
      // System.out.println(new String(headerBuffer.getData(),0,headerBuffer.getLength(),Charset.forName("UTF-8")));
      LOG.info("CONTENT LEN:" + contentBuffer.getLength());
      //System.out.println(new String(contentBuffer.getData(),0,contentBuffer.getLength(),Charset.forName("UTF-8")));
      // decode header bytes ... 
      String header = "";
      if (headerBuffer.getLength() != 0) { 
        try { 
          header = new String(headerBuffer.getData(),0,headerBuffer.getLength(),Charset.forName("UTF-8"));
        }
        catch (Exception e) { 
          LOG.warn(CCStringUtils.stringifyException(e));
          header = new String(headerBuffer.getData(),0,headerBuffer.getLength(),Charset.forName("ASCII"));
        }
      }
      final String headersFinal = (header!=null) ? header : "";
    
      LOG.info("Starting Event Loop");
      final EventLoop eventLoop = new EventLoop();
      eventLoop.start();
  
      try { 
        // create fake hosts file ...  
        //String hosts = "10.0.20.101:8072";
        // reader 
        //Reader reader = new StringReader(hosts);
        // dispatcher init 
        LOG.info("initializing Dispatcher");
        final Dispatcher dispatcher = new Dispatcher(eventLoop,"parserNodes");
        LOG.info("Waiting for a few seconds");
        Thread.sleep(5000);
        Thread threads[] = new Thread[TEST_THREAD_COUNT];
        final Semaphore threadWaitSem = new Semaphore(-TEST_THREAD_COUNT -1);
        // start 100 threads 
        for (int threadIdx=0;threadIdx<TEST_THREAD_COUNT;++threadIdx) { 
          threads[threadIdx] = new Thread(new Runnable(){

            @Override
            public void run() {
              for (int i=0;i<ITERATIONS_PER_THREAD;++i) { 
                // build parse request 
                ParseRequest request = new ParseRequest();
                request.setDocId(1);
                request.setDomainId(1);
                request.setDocURL(finalBaseURL.toString());
                request.setDocHeaders(headersFinal);
                request.setDocContent(new FlexBuffer(
                    contentBuffer.getData(),
                    0,
                    contentBuffer.getLength()));
                //LOG.info("Dispatching parse request");
                ParseResult result = dispatcher.dispatchRequest(request);
                LOG.info("TID[" + Thread.currentThread().getId()
                    +"]ReqID["+i+"]"
                    + " Success:" + ((result != null) ? result.getParseSuccessful() : false)
                    + " LinkCount:" + ((result != null) ? result.getExtractedLinks().size() :0));
              }
              LOG.info("Thread:"  + Thread.currentThread().getId() + " Exiting");
              threadWaitSem.release();
            } 
            
          });
          threads[threadIdx].start();
        }
        
        LOG.info("Waiting for threads to die");
        threadWaitSem.acquireUninterruptibly();
        LOG.info("All Threads dead.");
        
      }
      finally { 
        eventLoop.stop();
      }
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
    } catch (InterruptedException e) {
    }
  }

}
