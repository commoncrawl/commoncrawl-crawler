package org.commoncrawl.service.statscollector;

import java.io.File;
import java.io.IOException;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.rpc.base.internal.AsyncClientChannel;
import org.commoncrawl.rpc.base.internal.AsyncContext;
import org.commoncrawl.rpc.base.internal.AsyncServerChannel;
import org.commoncrawl.rpc.base.internal.NullMessage;
import org.commoncrawl.rpc.base.internal.AsyncRequest.Status;
import org.commoncrawl.rpc.base.shared.RPCException;
import org.commoncrawl.server.CommonCrawlServer;
import org.commoncrawl.util.CCStringUtils;

public class CrawlStatsCollectorService 
  extends CommonCrawlServer 
  implements CrawlerStatsService,
             AsyncServerChannel.ConnectionCallback {

  
  
  private FileSystem _fileSystem = null;
  private File       _localDataDir = null;
  private static final Log LOG = LogFactory.getLog(CrawlStatsCollectorService.class);
  private StatsLogManager _logManager;
  public static TreeMap<String,StatsCollection> _statsCollectionMap = new TreeMap<String,StatsCollection>();
  private static CrawlStatsCollectorService _singleton;
  
  
  public static CrawlStatsCollectorService getSingleton() { return _singleton; }
  
  public FileSystem getFileSystem() { 
    return _fileSystem;
  }
  
  private File getLocalRoot() {
    return _localDataDir;
  }
  
  @Override
  protected String getDefaultHttpInterface() {
    return CrawlEnvironment.DEFAULT_HTTP_INTERFACE;
  }

  @Override
  protected int getDefaultHttpPort() {
    return CrawlEnvironment.CRAWLSTATSCOLLECTOR_SERVICE_HTTP_PORT;
  }

  @Override
  protected String getDefaultLogFileName() {
    return "crawlstats_service.log";
  }

  @Override
  protected String getDefaultRPCInterface() {
    return CrawlEnvironment.DEFAULT_RPC_INTERFACE;
  }

  @Override
  protected int getDefaultRPCPort() {
    return CrawlEnvironment.CRAWLSTATSCOLLECTOR_SERVICE_RPC_PORT;
  }

  @Override
  protected String getWebAppName() {
    return CrawlEnvironment.CRAWLSTATSCOLLECTOR_SERVICE_WEBAPP_NAME;
  }

  @Override
  protected boolean initServer() {
    
    _singleton = this;
    
    try {
      
      _fileSystem = CrawlEnvironment.getDefaultFileSystem();

      File workingDirectory = new File(getDataDirectory(),"stats_server_data");
      workingDirectory.mkdirs();
      // initialize log manager 
      _logManager = new StatsLogManager(getEventLoop(), workingDirectory);
      // load collections 
      loadCollections(workingDirectory);
      // create server channel ... 
      AsyncServerChannel channel = new AsyncServerChannel(this, this.getEventLoop(),this.getServerAddress(),this);
      
      // register RPC services it supports ... 
      registerService(channel,CrawlerStatsService.spec);
      
      getWebServer().addServlet("tq", "/tq", CrawlerStatsQuery.class);
      
      return true;
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
    }
    return false;
  }
  
  private void loadCollections(File workingDirectory) throws IOException { 
    //TODO: THIS ONE BIG HACK :-(
    File files[] = workingDirectory.listFiles();
    for (File file : files) {
      LOG.info("Loader Found File:" + file.getName());
      if (file.getName().endsWith(".events")){
        
        int indexOfFirstDash = file.getName().indexOf('-');
        String groupKey = file.getName().substring(0,indexOfFirstDash);
        String uniqueKey = file.getName().substring(indexOfFirstDash + 1,file.getName().length() - ".events".length());
        
        
        if (groupKey.equalsIgnoreCase(CrawlerStatsCollection.GROUP_KEY)) {
          LOG.info("Found CrawlStatsCollection Prefix:" + groupKey + " UniqueKey:" + uniqueKey);
          _statsCollectionMap.put(StatsLogManager.makeCollectionName(groupKey, uniqueKey),new CrawlerStatsCollection(_logManager,uniqueKey));
        }
      }
    }
  }


  @Override
  protected boolean parseArguements(String[] argv) {
    return true;
  }
  
  @Override
  protected void overrideConfig(Configuration conf) { 
    
  }
  
  @Override
  protected void printUsage() {
   
    
  }

  @Override
  protected boolean startDaemons() {
    return true;
  }

  @Override
  protected void stopDaemons() {
    
  }



  @Override
  protected String getDefaultDataDir() {
    // TODO Auto-generated method stub
    return null;
  }


  @Override
  public void IncomingClientConnected(AsyncClientChannel channel) {
    
  }

  @Override
  public void IncomingClientDisconnected(AsyncClientChannel channel) {
    
  }

  @Override
  public void logCrawlerStats(AsyncContext<LogCrawlStatsRequest, NullMessage> rpcContext) throws RPCException {
    LOG.info("Received Stats From Crawler:"+ rpcContext.getInput().getCrawlerName());
    synchronized (_statsCollectionMap) { 
      try {
        String collectionName = StatsLogManager.makeCollectionName(CrawlerStatsCollection.GROUP_KEY,rpcContext.getInput().getCrawlerName());
        CrawlerStatsCollection statsCollection 
          = (CrawlerStatsCollection) _statsCollectionMap.get(collectionName);
        if (statsCollection == null) { 
          statsCollection = new CrawlerStatsCollection(_logManager,rpcContext.getInput().getCrawlerName());
          _statsCollectionMap.put(collectionName, statsCollection);
        }
        statsCollection.addValue(rpcContext.getInput().getCrawlerStats().getTimestamp(), rpcContext.getInput().getCrawlerStats());
      }
      catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
        rpcContext.setStatus(Status.Error_RequestFailed);
        rpcContext.setErrorDesc(CCStringUtils.stringifyException(e));
      }
      rpcContext.completeRequest();
    }
  }
}
