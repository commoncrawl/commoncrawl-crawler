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
package org.commoncrawl.service.dns;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.spi.LoggingEvent;
import org.commoncrawl.async.Timer;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.io.NIODNSQueryResult;
import org.commoncrawl.io.NIODNSCache;
import org.commoncrawl.io.NIODNSLocalResolver;
import org.commoncrawl.io.NIODNSQueryClient;
import org.commoncrawl.io.NIODNSResolver;
import org.commoncrawl.io.NIODNSCache.Node;
import org.commoncrawl.rpc.base.internal.AsyncClientChannel;
import org.commoncrawl.rpc.base.internal.AsyncContext;
import org.commoncrawl.rpc.base.internal.AsyncRequest;
import org.commoncrawl.rpc.base.internal.AsyncServerChannel;
import org.commoncrawl.rpc.base.internal.NullMessage;
import org.commoncrawl.rpc.base.shared.RPCException;
import org.commoncrawl.server.CommonCrawlServer;
import org.commoncrawl.service.crawler.filters.FilterResults;
import org.commoncrawl.service.crawler.filters.Filter.FilterResult;
import org.commoncrawl.service.directory.DirectoryServiceCallback;
import org.commoncrawl.service.directory.DirectoryServiceItemList;
import org.commoncrawl.service.directory.DirectoryServiceRegistrationInfo;
import org.commoncrawl.service.directory.DirectoryServiceServer;
import org.commoncrawl.service.directory.DirectoryServiceSubscriptionInfo;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.CustomLogger;
import org.commoncrawl.util.IPAddressUtils;
import org.commoncrawl.util.IntrusiveList;
import org.commoncrawl.util.JVMStats;
import org.commoncrawl.util.URLUtils;

/**
 * 
 * @author rana
 *
 */
public class DNSServiceServer 
extends CommonCrawlServer 
implements DNSService,
           DirectoryServiceCallback,
           AsyncClientChannel.ConnectionCallback,
           AsyncServerChannel.ConnectionCallback, 
           NIODNSLocalResolver.Logger {

  private static class CustomLoggerLayout extends Layout { 
    
    StringBuffer sbuf = new StringBuffer(1024);

    @Override
    public String format(LoggingEvent event) {
      sbuf.setLength(0);
      sbuf.append(event.getRenderedMessage());
      sbuf.append(LINE_SEP);
      return sbuf.toString();
    }

    @Override
    public boolean ignoresThrowable() {
      return true;
    }

    public void activateOptions() {
    } 
    
  }
  
  static final int     DEFAULT_DNS_TIMEOUT = 30000;
  static final int     CACHE_CHECKPOINT_INTERVAL = 60 * 60 * 1000; // checkpoint the caches every 60 minutes or so
  static final int     FULL_STATS_DUMP_INTERVAL = 60 * 60 * 1000;
  /** the max age for a checkpointed terminal node **/
  static final int     DEFAULT_MAX_CHECKPOINT_ITEM_AGE = 30 * 60 * 1000; // 30 minutes  
  
  static final String  GOOD_NAMES_CACHE_CHECKPOINT_FILE = "dnsServiceGoodNamesCheckpoint.log";
  static final String  BAD_NAMES_CACHE_CHECKPOINT_FILE  = "dnsServiceBadNamesCheckpoint.log";
  
  FileSystem   _fileSystem = null;
  CustomLogger _DNSSuccessLog;
  CustomLogger _DNSFailureLog;
  CustomLogger _DNSFailureDetailLog;
  String       _serversFile;
  IntrusiveList<NIODNSLocalResolver> _resolvers = new IntrusiveList<NIODNSLocalResolver>();
  NIODNSLocalResolver _nextResolver = null;
  Timer        _statsTimer = null;
  Timer        _checkpointTimer = null;
  long         _directCacheHits=0;
  InetAddress  _directoryServiceAddress;
  DNSNoCacheFilter _noCacheFilter;
  DNSRewriteFilter _rewriteFilter;
  long        _directoryServiceCallbackCookie = 0;
  long        _lastFullStatsDumpTime = -1;
  
  AsyncClientChannel _directoryServiceChannel;
  DirectoryServiceServer.AsyncStub _directoryServiceStub;
  
  
  @Override
  protected String getDefaultHttpInterface() {
    return CrawlEnvironment.DEFAULT_HTTP_INTERFACE;
  }

  @Override
  protected int getDefaultHttpPort() {
    return CrawlEnvironment.DNS_SERVICE_HTTP_PORT;
  }

  @Override
  protected String getDefaultLogFileName() {
    return "dnsservice.log";
  }

  @Override
  protected String getDefaultRPCInterface() {
    return CrawlEnvironment.DEFAULT_RPC_INTERFACE;
  }

  @Override
  protected int getDefaultRPCPort() {
    return CrawlEnvironment.DNS_SERVICE_RPC_PORT;
  }

  @Override
  protected String getWebAppName() {
    return CrawlEnvironment.DNS_SERVICE_WEBAPP_NAME;
  }

  //@Override
  protected String  getDefaultDataDir() { 
    return CrawlEnvironment.DEFAULT_DATA_DIR;
  }
  
  private File getGoodNamesCheckpointFileName() { 
    return new File(getLogDirectory(),GOOD_NAMES_CACHE_CHECKPOINT_FILE);
  }

  private File getBadNamesCheckpointFileName() { 
    return new File(getLogDirectory(),BAD_NAMES_CACHE_CHECKPOINT_FILE);
  }
  
  @Override
  protected boolean initServer() {
    try { 
      _fileSystem = CrawlEnvironment.getDefaultFileSystem();
      _DNSSuccessLog = new CustomLogger("DNSSuccessLog");
      _DNSFailureLog = new CustomLogger("DNSFailureLog");
      _DNSFailureDetailLog = new CustomLogger("DNSFailureDetailLog");
      _DNSSuccessLog.addAppender(new DailyRollingFileAppender(new CustomLoggerLayout(),getLogDirectory() + "/dnsServiceDNSSuccess.log","yyyy-MM-dd"));
      _DNSFailureLog.addAppender(new DailyRollingFileAppender(new CustomLoggerLayout(),getLogDirectory() + "/dnsServiceDNSFailures.log","yyyy-MM-dd"));
      _DNSFailureDetailLog.addAppender(new DailyRollingFileAppender(new CustomLoggerLayout(),getLogDirectory() + "/dnsServiceDNSFailuresDetail.log","yyyy-MM-dd"));
      NIODNSLocalResolver.setLogger(this);
      
      if (_serversFile == null) {
        LOG.fatal("Servers file (--servers) is NULL!");
        return false;
      }
      
      LOG.info("Loading Servers File");
      // parse server file 
      parseServersFile();
      
      if (_resolvers.size() == 0) { 
        LOG.fatal("No Slave DNS Servers Specified in Servers File!");
        return false;
      }


      // create server channel ... 
      AsyncServerChannel channel = new AsyncServerChannel(this, this.getEventLoop(),this.getServerAddress(),this);
      
      // register RPC services it supports ... 
      registerService(channel,DNSService.spec);
      registerService(channel,DirectoryServiceCallback.spec);
      
      channel.open();
      
      // load filters ... 
      reloadDNSFilters();
      
      // now register with the directory service ... 
      _directoryServiceChannel = new AsyncClientChannel(_eventLoop,new InetSocketAddress(0),new InetSocketAddress(_directoryServiceAddress,CrawlEnvironment.DIRECTORY_SERVICE_RPC_PORT),this);
      _directoryServiceChannel.open();
      _directoryServiceStub   = new DirectoryServiceServer.AsyncStub(_directoryServiceChannel);
      
      // load cache 
      LOG.info("Loading Good Item Cache");
      preloadGoodHostDNSCache();
      LOG.info("Loading Bad Item Cache");
      preloadBadHostDNSCache();
      
      
      _statsTimer = new Timer(1000,true, new Timer.Callback() {

        @Override
        public void timerFired(Timer timer) {
          publishStats();
        } 
        
      });

      // setup cache checkpoint timer ... 
      _checkpointTimer = new Timer(CACHE_CHECKPOINT_INTERVAL,true,new Timer.Callback() {

        @Override
        public void timerFired(Timer timer) {
          LOG.info("Cache Checkpoint Timer Fired");
          checkpointDNSCache(NIODNSLocalResolver.getDNSCache(),getGoodNamesCheckpointFileName(),
              new NIODNSCache.LoadFilter() {

            @Override
            public boolean loadItem(String hostName, String ipAddress,String name, long expireTime,long lastTouchedTime) {
              FilterResults filterResults = new FilterResults();
              if (expireTime >= System.currentTimeMillis()) {
                if (lastTouchedTime == -1 || (System.currentTimeMillis() - lastTouchedTime) < DEFAULT_MAX_CHECKPOINT_ITEM_AGE ) {
                	String rootDomainName = URLUtils.extractRootDomainName(hostName);
                	if (rootDomainName != null) { 
                		return (_noCacheFilter == null || _noCacheFilter.filterItem(rootDomainName,hostName, null, null,filterResults) == FilterResult.Filter_NoAction);
                	}
                }
              }
              return false;
            }

            @Override
            public String validateName(String hostName) {
              FilterResults filterResults = new FilterResults();
              
              String rootDomain = URLUtils.extractRootDomainName(hostName);
              if (rootDomain != null) { 
	              // rewrite if necessary 
	              if (_rewriteFilter != null && _rewriteFilter.filterItem(rootDomain,hostName, null, null, filterResults) == FilterResult.Filter_Modified) { 
	                // LOG.info("Rewrote:" + hostName + " to:" + filterResults.getRewrittenDomainName());
	                return filterResults.getRewrittenDomainName();
	              }
              }
              return hostName;
            }
          });
              
          checkpointDNSCache(NIODNSLocalResolver.getBadHostCache(),getBadNamesCheckpointFileName(),null);
        } 
      });
      
      _eventLoop.setTimer(_statsTimer);
      _eventLoop.setTimer(_checkpointTimer);
      
      return true;
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
    }
    return false;
  }

  @Override
  protected boolean parseArguements(String[] argv) {
    for(int i=0; i < argv.length;++i) {
      if (argv[i].equalsIgnoreCase("--servers")) { 
        if (i+1 < argv.length) { 
          _serversFile = argv[++i];
        }
      }
      else if (argv[i].equalsIgnoreCase("--directoryserver")) { 
        if (i+1 < argv.length) { 
          try {
            _directoryServiceAddress = InetAddress.getByName(argv[++i]);
          } catch (UnknownHostException e) {
            LOG.error(CCStringUtils.stringifyException(e));
          }
        }
      }
    }
    return (_serversFile != null && _directoryServiceAddress != null);
  }

  @Override
  protected void printUsage() {
    // TODO Auto-generated method stub
    
  }

  @Override
  protected boolean startDaemons() {
    // TODO Auto-generated method stub
    return true;
  }

  @Override
  protected void stopDaemons() {
    // TODO Auto-generated method stub
    
  }

  private static abstract class NIODNSQueryClientWithHopCount implements NIODNSQueryClient { 
    protected int _hopCount = 0;
    protected String _hostName;
    protected boolean _skipCache = false;
  }
  
  @Override
  public void doQuery(final AsyncContext<DNSQueryInfo, DNSQueryResponse> rpcContext) throws RPCException {
    
    NIODNSQueryClientWithHopCount queryClient = new NIODNSQueryClientWithHopCount() {

      @Override
      public void AddressResolutionFailure(NIODNSResolver eventSource,String hostName, Status status,String errorDesc) {
        
        if (eventSource != null) { 
          rpcContext.getOutput().setSourceServer(((NIODNSLocalResolver)eventSource).getName());
        }
        else { 
          rpcContext.getOutput().setSourceServer("CACHE");
        }
        if (status == Status.RESOLVER_FAILURE)
          rpcContext.getOutput().setStatus(DNSQueryResponse.Status.RESOLVER_FAILURE);
        else if (status == Status.SERVER_FAILURE) {
          // if first hop on server fail .. try a different resolver ... 
          if (this._hopCount++ == 0 && this._hostName != null) { 
            LOG.info("Rescheduling Resolution of Host on first hop with SERVFAIL result:" + rpcContext.getInput().getHostName());
            DNSServiceSession session = getSessionForClient(rpcContext.getClientChannel());
            
            if (session != null) { 
	            try {
	              session.queuedWorkItems.add(getNextResolver((NIODNSLocalResolver)eventSource).resolve(this,_hostName,_skipCache,rpcContext.getInput().getIsHighPriorityRequest(),DEFAULT_DNS_TIMEOUT));
	            } catch (IOException e) {
	              LOG.error(CCStringUtils.stringifyException(e));
	              rpcContext.setErrorDesc(CCStringUtils.stringifyException(e));
	              rpcContext.setStatus(AsyncRequest.Status.Error_RequestFailed);
	              try {
	                rpcContext.completeRequest();
	              } catch (RPCException e1) {
	                LOG.error(CCStringUtils.stringifyException(e1));
	              }              
	            }
	            // first SERVER_FAILURE triggers a second try
	            return;
            }
            else { 
            	LOG.error("Session Object NULL when Servicing AddressResolutionFailure for Host:" + hostName);
            }
          }
          else { 
            rpcContext.getOutput().setStatus(DNSQueryResponse.Status.SERVER_FAILURE);
          }
        }
        
        rpcContext.getOutput().setErrorDesc(errorDesc);
        
        rpcContext.setStatus(org.commoncrawl.rpc.base.internal.AsyncRequest.Status.Success);
        try {
          rpcContext.completeRequest();
        } catch (RPCException e) {
          LOG.error(CCStringUtils.stringifyException(e));
        }
      }

      @Override
      public void AddressResolutionSuccess(NIODNSResolver eventSource,String hostName, String cName, InetAddress address, long addressTTL) {
        
        if (eventSource != null) { 
          rpcContext.getOutput().setSourceServer(((NIODNSLocalResolver)eventSource).getName());
        }
        else { 
          rpcContext.getOutput().setSourceServer("CACHE");
        }
        
        rpcContext.getOutput().setStatus(DNSQueryResponse.Status.SUCCESS);
        rpcContext.getOutput().setAddress(IPAddressUtils.IPV4AddressToInteger(address.getAddress()));
        rpcContext.getOutput().setTtl(addressTTL);
        if (cName != null) {
          rpcContext.getOutput().setCname(cName);
        }
        rpcContext.setStatus(org.commoncrawl.rpc.base.internal.AsyncRequest.Status.Success);
        try {
          rpcContext.completeRequest();
        } catch (RPCException e) {
          LOG.error(CCStringUtils.stringifyException(e));
        }
        
      }

      @Override
      public void DNSResultsAvailable() {
        
      }

      @Override
      public void done(NIODNSResolver eventSource,FutureTask<org.commoncrawl.io.NIODNSQueryResult> task) {
        
        if (eventSource != null) { 
          rpcContext.getOutput().setSourceServer(((NIODNSLocalResolver)eventSource).getName());
        }
        else { 
          rpcContext.getOutput().setSourceServer("CACHE");
        }
        
        DNSServiceSession session = getSessionForClient(rpcContext.getClientChannel());
        if (session != null) { 
          session.queuedWorkItems.remove(task);
        }
      } 
  
    };    
    
    
    try { 
    
      String dnsName = rpcContext.getInput().getHostName();
      
      FilterResults filterResults = new FilterResults();
      
      String rootName = URLUtils.extractRootDomainName(dnsName);
      if (rootName != null) { 
	      // rewrite if necessary 
	      if (_rewriteFilter != null && _rewriteFilter.filterItem(rootName,dnsName, null, null, filterResults) == FilterResult.Filter_Modified) { 
	        // LOG.info("Rewrote:" + dnsName + " to:" + filterResults.getRewrittenDomainName());
	        dnsName = filterResults.getRewrittenDomainName();
	      }
      }
      
      boolean skipCache = (_noCacheFilter != null && rootName != null && _noCacheFilter.filterItem(rootName,dnsName, null, null,filterResults) == FilterResult.Filter_Accept); 
      if (skipCache) { 
        LOG.info("Skiiping Cache check for name:" + dnsName);
      }
      
       
      final NIODNSQueryResult cachedResult = (!skipCache) ? NIODNSLocalResolver.checkCache(queryClient, dnsName) : null;
      
      if (cachedResult != null) { 
        _eventLoop.setTimer(new Timer(0,false,new Timer.Callback() {

          @Override
          public void timerFired(Timer timer) {
            // LOG.info("Query for Host:" + rpcContext.getInput().getHostName() + " Processed via cache");
            _directCacheHits++;
            cachedResult.fireCallback();
          } 
          
        }));
        
      }
      else { 
        DNSServiceSession session = getSessionForClient(rpcContext.getClientChannel());
        
        queryClient._hostName = dnsName;
        queryClient._skipCache = skipCache;
        
        session.queuedWorkItems.add(getNextResolver(null).resolve(queryClient,dnsName,skipCache,rpcContext.getInput().getIsHighPriorityRequest(),DEFAULT_DNS_TIMEOUT));
      }
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      rpcContext.setErrorDesc(CCStringUtils.stringifyException(e));
      rpcContext.setStatus(AsyncRequest.Status.Error_RequestFailed);
      rpcContext.completeRequest();
    }
  }

  @Override
  public void IncomingClientConnected(AsyncClientChannel channel) {
    LOG.info("Incoming Client Connected");
    _sessions.add( new DNSServiceSession(channel));
  }

  @Override
  public void IncomingClientDisconnected(AsyncClientChannel channel) {
    DNSServiceSession selectedSessionObj = getSessionForClient(channel);
    if (selectedSessionObj != null) { 
      selectedSessionObj.cancelWorkItems();
      _sessions.remove(selectedSessionObj);
    }
  }

  
  
  /***************************************************************/
  /* Internal Implementation */
  /***************************************************************/
  
  private void publishStats() { 
    
    LOG.info("**Resolver Stats. DirectCacheHits:" +_directCacheHits + ")**");
    for (NIODNSLocalResolver resolver : _resolvers) { 
      String resolverStats = "Resolver:" + resolver.getName() + " QueueSize:" + resolver.getQueuedItemCount() + " CacheHits:" + resolver.getCacheHitCount();
      LOG.info(resolverStats);
    }
    
    if (_lastFullStatsDumpTime == -1) {
      _lastFullStatsDumpTime = System.currentTimeMillis();
    }
    else { 

      if (System.currentTimeMillis() - _lastFullStatsDumpTime >= FULL_STATS_DUMP_INTERVAL) {
        
        long timeStart = System.currentTimeMillis();
        
        LOG.info("Dumping Hot IP nodes - Locking Cache");
        synchronized (NIODNSLocalResolver.getDNSCache()) {
          
          NIODNSCache cache = NIODNSLocalResolver.getDNSCache();
          
          List<Node> terminalIPNodes = new Vector<Node>();
          cache.collectTerminalIPNodes(terminalIPNodes);
          LOG.info("Sorting nodes");
          Collections.sort(terminalIPNodes,new Comparator<NIODNSCache.Node>() {
      
            @Override
            public int compare(Node o1, Node o2) {
              return o1.getTimeToLive() > o2.getTimeToLive() ? -1 : o1.getTimeToLive() < o2.getTimeToLive() ? 1 : 0; 
            } 
          });
          LOG.info("Top 100 Hot Nodes");
          int maxNodes = Math.min(1000, terminalIPNodes.size());
          for (int i=0;i<maxNodes;++i) {
            LOG.info("Node:" + terminalIPNodes.get(i).getFullName() + " CName:" + terminalIPNodes.get(i).getCannonicalName() + " HitCount:" + terminalIPNodes.get(i).getTimeToLive());
          }
        }
        
        long timeEnd = System.currentTimeMillis();
        
        LOG.info("Full Stat Dump took:" + (timeEnd - timeStart) + "MS");
        
        
        _lastFullStatsDumpTime = System.currentTimeMillis();
      }
    }
  }
  
  private DNSServiceSession getSessionForClient(AsyncClientChannel channel) { 
    for (DNSServiceSession session : _sessions) { 
      if (session.channel == channel) { 
        return session;
      }
    }
    return null;
  }
  private LinkedList<DNSServiceSession> _sessions = new LinkedList<DNSServiceSession>();
  private class DNSServiceSession {

    public DNSServiceSession(AsyncClientChannel channel) { 
      this.channel = channel;
    }
   
    public void cancelWorkItems() { 
      LOG.info("Cancelling Work Items for Client");
      for (Future<NIODNSQueryResult> task : queuedWorkItems) { 
        task.cancel(false);
      }
      queuedWorkItems.clear();
      LOG.info("DONE -Cancelling Work Items for Client");
    }
    
    public AsyncClientChannel channel;
    public LinkedList<Future<NIODNSQueryResult>> queuedWorkItems = new LinkedList<Future<org.commoncrawl.io.NIODNSQueryResult>>(); 
  }
  
  NIODNSLocalResolver getResolverForName(String hostName) { 
    int resolverNumber = hostName.hashCode() % _resolvers.size();
    int i=0;
    for (NIODNSLocalResolver resolver : _resolvers) {
      if (i++ == resolverNumber)
        return resolver;
    }
    return null;
  }
  
  NIODNSLocalResolver getNextResolver(NIODNSLocalResolver skipThisResolver) { 
    NIODNSLocalResolver selectedResolver = null;
    for (NIODNSLocalResolver resolver : _resolvers) { 
      if (selectedResolver == null || resolver.getQueuedItemCount() < selectedResolver.getQueuedItemCount()) { 
        if (skipThisResolver != resolver) { 
          selectedResolver = resolver;
        }
      }
    }
    return (selectedResolver == null) ? skipThisResolver : selectedResolver;
    /*
    NIODNSResolver resolverOut = (_nextResolver == null) ? _resolvers.getHead() : _nextResolver;
    _nextResolver = (resolverOut.getNext() != null) ? resolverOut.getNext() : null;
    
    return resolverOut;
    */
  }
  
  @Override
  public void logDNSFailure(String hostName, String errorDescription) {
    synchronized(_DNSFailureLog) { 
      _DNSFailureLog.error(hostName + "," + errorDescription);
    }
  }

  @Override
  public void logDNSQuery(String hostName, InetAddress address, long ttl,String opCName) {
    synchronized(_DNSSuccessLog) { 
      _DNSSuccessLog.info(hostName + "," + address.toString() + "," + ttl  + "," + opCName);
    }
  }

  @Override
  public void logDNSException(String hostName, String exceptionDesc) {
    synchronized (_DNSFailureDetailLog) {
      _DNSFailureDetailLog.info(hostName + "," + exceptionDesc);
    }
  }

  private static class DirectByteBufferAccessStream extends ByteArrayOutputStream { 
    
    public DirectByteBufferAccessStream(int initialSize) { 
      super(initialSize);
    }
    
    public byte[] getBuffer() { return buf; }
  }
  
  private void checkpointDNSCache(final NIODNSCache cache,final File checkpointFileName,NIODNSCache.LoadFilter filter) { 
    LOG.info("Starting Cache Checkpoint");
    final DirectByteBufferAccessStream streamOut = new DirectByteBufferAccessStream(1024*1024);
    
    try {
      // lock the cache for the duration of the atomic operation ...
      synchronized (cache) { 
        long timeStart = System.currentTimeMillis();
        
        cache.dumpNameTree(streamOut, new NIODNSCache.NodeDumpFilter() {

          @Override
          public boolean dumpTerminalNode(Node node) {
            if (node.getTimeToLive() >= System.currentTimeMillis()){ 
              return true;
            }
            return false;
          } 
        });
        long timeEnd = System.currentTimeMillis();
        
        final byte dataBuffer[] = streamOut.getBuffer();
        
        LOG.info("Name Tree Dump took:" + (timeEnd-timeStart) + "MS and produced DataBuffer of size:" + dataBuffer.length);
        
        LOG.info("Reloading cache from data buffer ");
        timeStart = System.currentTimeMillis();
        cache.clear();
        System.gc();
        cache.loadTree(new ByteArrayInputStream(dataBuffer,0,streamOut.size()), filter);
        timeEnd  = System.currentTimeMillis();
        LOG.info("Reload took:" + (timeEnd-timeStart) + "MS");
        
        LOG.info("Starting cache writer thread");
        new Thread(new Runnable() {

          final static int WRITE_CHUNK_SIZE = 4096 * 4;
          
          @Override
          public void run() {
            LOG.info("Cache writer thread writing new checkpoint file to path:" + checkpointFileName);
            File oldCheckpointFileName = new File(checkpointFileName.getParentFile(),checkpointFileName.getName() + ".OLD");
            if (checkpointFileName.exists()) {
              File oldFileNewName = new File(oldCheckpointFileName.getParentFile(),oldCheckpointFileName.getName() + "." + System.currentTimeMillis());
              oldCheckpointFileName.renameTo(oldFileNewName);
              oldCheckpointFileName.delete();
              checkpointFileName.renameTo(oldCheckpointFileName);
            }
            BufferedOutputStream outputStream = null;
            try {
              outputStream = new BufferedOutputStream(new FileOutputStream(checkpointFileName));
              for (int offset=0;offset<streamOut.size();offset += WRITE_CHUNK_SIZE) { 
                outputStream.write(dataBuffer,offset,Math.min(WRITE_CHUNK_SIZE,streamOut.size() - offset));
              }
              outputStream.flush();
              LOG.info("Cache Writer Thread Succesfully wrote Checkpoint File:" + checkpointFileName);
            } 
            catch (IOException e) { 
              LOG.error(CCStringUtils.stringifyException(e));
              try {
                outputStream.close();
                outputStream = null;
                checkpointFileName.delete();
              } catch (IOException e1) {
                LOG.error(CCStringUtils.stringifyException(e1));                
              }
            }
            finally {
              if (outputStream != null) { 
                try {
                  outputStream.close();
                } catch (IOException e) {
                  LOG.error(CCStringUtils.stringifyException(e));
                }
              }
            }
            LOG.info("Checkpoint File Writer Thread Exiting");
          } 
        }).start();
      }
      
    } catch (IOException e) {
      LOG.error("Good Cache Checkpoint Failed with Exception:" + CCStringUtils.stringifyException(e));
    }    
  }
  
  private void loadCacheFromCheckpointFile(NIODNSCache cache,File checkpointFile,NIODNSCache.LoadFilter filter)throws IOException { 
    LOG.info("Pre-Loading DNS Cache from Checkpoint File:" + checkpointFile);
    
    InputStream stream = null;
    
    try { 
      try {
        stream = new BufferedInputStream(new FileInputStream(checkpointFile));
        long timeStart = System.currentTimeMillis();
        LOG.info("Starting Cache Load");
        cache.loadTree(stream, filter);          
        long timeEnd = System.currentTimeMillis();
        LOG.info("Load Took:" + (timeEnd-timeStart) + "MS");
      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
      }
    }
    finally { 
      if (stream != null) { 
        try {
          stream.close();
        } catch (IOException e) {
          LOG.error(CCStringUtils.stringifyException(e));
        }
      }
    }
  }
  
  private void preloadGoodHostDNSCache() { 
    
    File logFilePath= new File(getLogDirectory() + "/dnsServiceDNSSuccess.log");
    File checkpointFile = getGoodNamesCheckpointFileName();
    
    if (checkpointFile.exists()) {
      try{ 
        loadCacheFromCheckpointFile(NIODNSLocalResolver.getDNSCache(),checkpointFile, new NIODNSCache.LoadFilter() {

          @Override
          public boolean loadItem(String hostName, String ipAddress,String name, long expireTime,long lastTouchedTime) {
            FilterResults filterResults = new FilterResults();
            if (expireTime >= System.currentTimeMillis()) { 
            	String rootName = URLUtils.extractRootDomainName(hostName);
            	if (rootName != null) { 
            		return (_noCacheFilter == null || _noCacheFilter.filterItem(rootName,hostName, null, null,filterResults) == FilterResult.Filter_NoAction);
            	}
            }
            return false;
          }

          @Override
          public String validateName(String hostName) {
            FilterResults filterResults = new FilterResults();
            
          	String rootName = URLUtils.extractRootDomainName(hostName);
          	if (rootName != null) { 
	            // rewrite if necessary 
	            if (_rewriteFilter != null && _rewriteFilter.filterItem(rootName,hostName, null, null, filterResults) == FilterResult.Filter_Modified) { 
	              // LOG.info("Rewrote:" + hostName + " to:" + filterResults.getRewrittenDomainName());
	              return filterResults.getRewrittenDomainName();
	            }
          	}
            return hostName;
          }
        });

      }
      catch (IOException e) { 
        LOG.error("Good Host Cache Load from Checkpoint File failed with:" + CCStringUtils.stringifyException(e));
      }
    }
    else { 
      LOG.info("Pre-loading DNS Cache from Log File:" + logFilePath);
      JVMStats.dumpMemoryStats();
      
      FileReader reader = null;
  
      NIODNSCache cache = NIODNSLocalResolver.getDNSCache();
      
      try {
  
        reader = new FileReader(logFilePath);
  
        BufferedReader lineReader = new BufferedReader(reader);
        
        int lineCount =0;
        String line = null;
        
        HashSet<String> hostSet = new HashSet<String>();
        
        while ((line = lineReader.readLine()) != null) { 
          
          int firstDelimiterIdx = line.indexOf(",/");
          int secondDelimiterIdx = -1;
          int thirdDelimiterIdx = -1;
          if (firstDelimiterIdx != -1) { 
            secondDelimiterIdx = line.indexOf(',',firstDelimiterIdx+2);
            thirdDelimiterIdx = line.indexOf(',',secondDelimiterIdx + 1);
          }
          
          if (firstDelimiterIdx != -1 && secondDelimiterIdx != -1) { 
            String hostName = line.substring(0,firstDelimiterIdx);
            String ipAddress = line.substring(firstDelimiterIdx + 2,secondDelimiterIdx);
            String cname      = null;
            long  ttl = -1;
            if (thirdDelimiterIdx != -1) { 
             ttl = Long.parseLong(line.substring(secondDelimiterIdx + 1,thirdDelimiterIdx));
             cname = line.substring(thirdDelimiterIdx + 1);
            }
            else {
             ttl = Long.parseLong(line.substring(secondDelimiterIdx + 1));
            }
            int ipAddressInteger = IPAddressUtils.IPV4AddressStrToInteger(ipAddress);
            
            FilterResults filterResults = new FilterResults();
            
          	String rootName = URLUtils.extractRootDomainName(hostName);
          	if (rootName != null) { 
	            // rewrite if necessary 
	            if (_rewriteFilter != null && _rewriteFilter.filterItem(rootName,hostName, null, null, filterResults) == FilterResult.Filter_Modified) { 
	              // LOG.info("Rewrote:" + hostName + " to:" + filterResults.getRewrittenDomainName());
	              hostName = filterResults.getRewrittenDomainName();
	            }
          	}
            
            boolean skipCache = (_noCacheFilter != null && rootName != null && _noCacheFilter.filterItem(rootName,hostName, null, null,filterResults) == FilterResult.Filter_Accept); 
            if (skipCache) { 
              LOG.info("Skiiping Cache check for name:" + hostName);
            }
            else { 
              cache.cacheIPAddressForHost(hostName, ipAddressInteger, ttl, cname);
            }
          }
          
          if (++lineCount % 1000 == 0)
            LOG.info("Processed " + lineCount + " lines");
        }
        
        LOG.info("Done Processing Cache Log. Total Lines Processed:" + lineCount + " DNSCacheNode Count:" + cache.getActiveNodeCount());
        JVMStats.dumpMemoryStats();
        cache.dumpIPAddressTree(System.out);
        
      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
      }
      finally { 
        try {
          reader.close();
        } catch (IOException e) {
          LOG.error(CCStringUtils.stringifyException(e));
        }
      }
    }
  }

  private void preloadBadHostDNSCache() { 
    
    
    File logFilePath= new File(getLogDirectory() + "/dnsServiceDNSFailures.log");
    File checkpointFile = getBadNamesCheckpointFileName();
    
    if (checkpointFile.exists()) {
      try{ 
        loadCacheFromCheckpointFile(NIODNSLocalResolver.getBadHostCache(),checkpointFile,new NIODNSCache.LoadFilter() {

          @Override
          public boolean loadItem(String hostName, String ipAddress,String name, long expireTime,long lastTouchedTime) {
            return (expireTime >= System.currentTimeMillis());
          }

          @Override
          public String validateName(String hostName) {
            return hostName;
          }
        });            
      }
      catch (IOException e) { 
        LOG.error("Bad Host Cache Load from Checkpoint File failed with:" + CCStringUtils.stringifyException(e));
      }
    }
    else { 
      LOG.info("Pre-loading Bad Host DNS Cache from Log File:" + logFilePath);
      JVMStats.dumpMemoryStats();
      
      FileReader reader = null;
  
      NIODNSCache cache = NIODNSLocalResolver.getBadHostCache();
      
      try {
  
        reader = new FileReader(logFilePath);
  
        BufferedReader lineReader = new BufferedReader(reader);
        
        int lineCount =0;
        String line = null;
       
        while ((line = lineReader.readLine()) != null) { 
          
          int lastDelimiterIdx = line.lastIndexOf(",");
          
          if (lastDelimiterIdx != -1) {
  
            String hostName = line.substring(0,lastDelimiterIdx);
            String errorCode = line.substring(lastDelimiterIdx + 1);
            
            if (errorCode.length() != 0) {
              if (errorCode.equals("NXDOMAIN")) {
                cache.cacheIPAddressForHost(hostName,0,System.currentTimeMillis() + NIODNSLocalResolver.NXDOMAIN_FAIL_BAD_HOST_LIFETIME,null);
                
  
              }
              else { 
                cache.cacheIPAddressForHost(hostName,0,System.currentTimeMillis() + NIODNSLocalResolver.SERVER_FAIL_BAD_HOST_LIFETIME ,null);
              }
            }
          }
          
          if (++lineCount % 1000 == 0)
            LOG.info("Processed " + lineCount + " lines");
        }
        
        LOG.info("Done Processing Bad Host Cache Log. Total Lines Processed:" + lineCount + " DNSCacheNode Count:" + cache.getActiveNodeCount());
        JVMStats.dumpMemoryStats();
        
      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
      }
      finally { 
        try {
          reader.close();
        } catch (IOException e) {
          LOG.error(CCStringUtils.stringifyException(e));
        }
      }
    }
  }
  
  
  void parseServersFile()throws IOException {
    
    LOG.info("Loading Servers File from:" + _serversFile);
    InputStream stream =null;

    URL resourceURL = CrawlEnvironment.getHadoopConfig().getResource(_serversFile);
    
    if (resourceURL != null) {
      stream = resourceURL.openStream();
    }
    // try as filename 
    else { 
      LOG.info("Could not load resource as an URL. Trying as an absolute pathname");
      stream = new FileInputStream(new File(_serversFile));
    }
    
    if (stream == null) { 
      throw new FileNotFoundException();
    }
         
    BufferedReader reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(stream)));

    String dnsServerAddress = null;
    
    int serverCount = 0;
    
    LOG.info("Loading servers file");
    while ((dnsServerAddress = reader.readLine()) != null) {
      if (!dnsServerAddress.startsWith("#")) {
        int ipAddress = IPAddressUtils.IPV4AddressToInteger(InetAddress.getByName(dnsServerAddress).getAddress());
        LOG.info("Added DNS Resolver with IP:" + IPAddressUtils.IntegerToIPAddressString(ipAddress) + " to list.");
        _resolvers.addTail(
            new NIODNSLocalResolver(
                IPAddressUtils.IntegerToIPAddressString(ipAddress),
                getEventLoop(),
                Executors.newFixedThreadPool(20),
                Executors.newFixedThreadPool(15),
                true));
      }
    }
  }

  @Override
  public void initialize(AsyncContext<DirectoryServiceRegistrationInfo, NullMessage> rpcContext) throws RPCException {
    LOG.info("Received Initialization Request on Callback Channel");
    if (rpcContext.getInput().getRegistrationCookie() == _directoryServiceCallbackCookie) {
      LOG.info("Cookies Match! Sending Subscription information");
      
      rpcContext.completeRequest();
      
      DirectoryServiceSubscriptionInfo subscription = new DirectoryServiceSubscriptionInfo();
      subscription.setSubscriptionPath("/lists/dns_.*");
      
      LOG.info("Subscribing to /lists/dns_.*");
      _directoryServiceStub.subscribe(subscription,new AsyncRequest.Callback<DirectoryServiceSubscriptionInfo,DirectoryServiceItemList>() {
  
        @Override
        public void requestComplete(AsyncRequest<DirectoryServiceSubscriptionInfo, DirectoryServiceItemList> request) {
          if (request.getStatus() == AsyncRequest.Status.Success){
            LOG.info("Subscription Successfull!");
          }
          else { 
            LOG.info("Subscription Failed!");
          }
        }
      });
    }
  }

  @Override
  public void itemChanged(AsyncContext<DirectoryServiceItemList, NullMessage> rpcContext) throws RPCException {
    LOG.info("Received item changed from directory service");
    reloadDNSFilters();
    rpcContext.completeRequest();
  }

  @Override
  public void OutgoingChannelConnected(AsyncClientChannel channel) {
    
    LOG.info("Connected to Directory Server. Registering for Callbacks");
    DirectoryServiceRegistrationInfo registerationInfo = new DirectoryServiceRegistrationInfo();
    
    _directoryServiceCallbackCookie = System.currentTimeMillis();
    
    registerationInfo.setConnectionString(getServerAddress().getAddress().getHostAddress() + ":" + getServerAddress().getPort());
    registerationInfo.setRegistrationCookie(_directoryServiceCallbackCookie);
    registerationInfo.setConnectionName("DNS Service");
    
    try {
      _directoryServiceStub.register(registerationInfo, new AsyncRequest.Callback<DirectoryServiceRegistrationInfo,NullMessage>() {
        
        @Override
        public void requestComplete(AsyncRequest<DirectoryServiceRegistrationInfo, NullMessage> request) {
          LOG.info("Received Registration Compelte Callback from Directory Server with Status:" + request.getStatus());
        } 
        
      });
    } catch (RPCException e) {
      LOG.error(CCStringUtils.stringifyException(e));
    }
  }

  @Override
  public boolean OutgoingChannelDisconnected(AsyncClientChannel channel) {
    return false;
  }
  
  void reloadDNSFilters() { 
    LOG.info("Loading DNS Filters");
    _noCacheFilter = new DNSNoCacheFilter();
    _rewriteFilter = new DNSRewriteFilter();
    try { 
      _noCacheFilter.loadFromPath(_directoryServiceAddress,CrawlEnvironment.DNS_NOCACHE_RULES ,false);
      _rewriteFilter.loadFromPath(_directoryServiceAddress, CrawlEnvironment.DNS_REWRITE_RULES, false);
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
    }
  }
}
