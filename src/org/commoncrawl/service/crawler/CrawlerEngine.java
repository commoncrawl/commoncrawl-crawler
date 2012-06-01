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

package org.commoncrawl.service.crawler;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.servlet.jsp.JspWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Appender;
import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.spi.LoggingEvent;
import org.commoncrawl.util.SuffixStringMatcher;
import org.commoncrawl.async.Callback;
import org.commoncrawl.async.ConcurrentTask;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.async.Timer;
import org.commoncrawl.async.ConcurrentTask.CompletionCallback;
import org.commoncrawl.common.Environment;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.io.DNSQueryResult;
import org.commoncrawl.io.NIODNSCache;
import org.commoncrawl.io.NIODNSQueryClient;
import org.commoncrawl.io.NIODNSResolver;
import org.commoncrawl.io.NIOHttpConnection;
import org.commoncrawl.io.NIOHttpHeaders;
import org.commoncrawl.protocol.CrawlSegment;
import org.commoncrawl.protocol.CrawlSegmentDetail;
import org.commoncrawl.protocol.CrawlSegmentHost;
import org.commoncrawl.protocol.CrawlSegmentStatus;
import org.commoncrawl.protocol.CrawlSegmentURL;
import org.commoncrawl.protocol.CrawlURL;
import org.commoncrawl.protocol.CrawlURLMetadata;
import org.commoncrawl.protocol.URLFP;
import org.commoncrawl.rpc.base.internal.AsyncRequest;
import org.commoncrawl.rpc.base.internal.NullMessage;
import org.commoncrawl.rpc.base.shared.RPCException;
import org.commoncrawl.service.crawler.CrawlLog.CheckpointCompletionCallback;
import org.commoncrawl.service.crawler.CrawlLog.LogFlusherStopActionCallback;
import org.commoncrawl.service.crawler.CrawlSegmentLog.CrawlSegmentFPMap;
import org.commoncrawl.service.crawler.SegmentLoader.LoadProgressCallback;
import org.commoncrawl.service.crawler.filters.Filter.FilterResult;
import org.commoncrawl.service.crawler.util.URLFPBloomFilter;
import org.commoncrawl.service.statscollector.CrawlerStats;
import org.commoncrawl.service.statscollector.LogCrawlStatsRequest;
import org.commoncrawl.util.AsyncAppender;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.CustomLogger;
import org.commoncrawl.util.FPGenerator;
import org.commoncrawl.util.FlexBuffer;
import org.commoncrawl.util.HttpHeaderInfoExtractor;
import org.commoncrawl.util.IPAddressUtils;
import org.commoncrawl.util.JVMStats;
import org.commoncrawl.util.MovingAverage;
import org.commoncrawl.util.RuntimeStatsCollector;
import org.commoncrawl.util.SessionIDURLNormalizer;
import org.commoncrawl.util.SubDomainComparator;
import org.commoncrawl.util.URLUtils;

/**
 * Class that manages the crawler process state
 * 
 * @author rana
 *
 */
public final class CrawlerEngine  {

  /** database keys **/ 
  static final String CRAWLER_CRAWL_SEGMENT_TYPE_PARENT_KEY 	= "TWUnit";
  static final String CrawlSegmentKeyPrefix = "CSInfo_";
  static final int     DEFAULT_MAX_SIMULTANEOUS_CONNECTIONS = 500;
  static final int     HOSTID_CACHE_SIZE = 10000;
  static final int     DEFAULT_DNS_TIMEOUT = 30000;
  static final int     STATS_COLLECTION_INTERVAL = 500;
  static final int     DEFAULT_MAX_ACTIVE_URLS = 2000000000;
  static final int     LOADER_OVERFLOW_ALLOWED= 100000;
  static final int     MAX_PENDING_URLS=100000;
  static final int     DEFAULT_MAX_ACTIVE_HOSTS = 10000;
  static final int     DEFAULT_LOCAL_BLOOMFILTER_ELEMENT_SIZE = 500000000;
  static final int     DEFAULT_LOCAL_BLOOMFILTER_BITS_PER_ELEMENT = 11;
  static final int     DEFAULT_LOCAL_BLOOMFILTER_BITS_NUM_HASH_FUNCTIONS = 10;
  


  // the loader will stall if memory utilization reaches or exceeds specified number .. 
  static final float  DEFAULT_LOADER_STALL_MEMORY_UTILIZATION_RATIO = .70f;
  // if the loader is stalled, it will not resume until memory utilization reaches specified ratio .. 
  static final float  DEFAULT_LOADER_RESUME_MEMORY_UTILIZATION_RATIO = .55f;
  // default list id 
  static final long   DEFAULT_LIST_ID = 1;    

  /** logging **/
  private static final Log LOG = LogFactory.getLog("org.commoncrawl.crawler.CrawlEngine");
  /** database id of crawler **/
  String 			  	_databaseId;
  /** back pointer to crawl server **/
  CrawlerServer _server;
  /** http crawl queue **/
  CrawlQueue _httpCrawlQueue;
  /** crawl interface list **/
  InetSocketAddress[] _crawlInterfaces;
  /** max tcp connect sockets **/
  int  _maxTCPSockets = -1;
  /** max active urls **/
  int  _maxActiveURLS = DEFAULT_MAX_ACTIVE_URLS;
  /** max active hosts **/
  int  _maxActiveHosts = DEFAULT_MAX_ACTIVE_HOSTS;
  /** custom logger **/
  CustomLogger _failureLog;
  CustomLogger _SuccessLog;
  CustomLogger _GETLog;
  CustomLogger _DNSSuccessLog;
  CustomLogger _DNSFailureLog;
  CustomLogger _RobotsLog;
  CustomLogger _CrawlLogLog;
  CustomLogger _CookieLogger;
  SimpleDateFormat robotsLogDateFormat = new SimpleDateFormat("yyyy.MM.dd hh:mm:ss.SSS");  
  /** the crawl log **/
  CrawlLog _crawlLog;
  /** stats collector **/
  RuntimeStatsCollector _stats = new RuntimeStatsCollector();
  /** stats collector (remote) stats **/
  CrawlerStats _crawlerStats = new CrawlerStats();
  /** last stats upload time **/
  private long _lastCrawlerStatsUploadTime = -1;
  /** crawler stats flush interval **/
  private static final int CRAWLER_STATS_FLUSH_INTERVAL = 5 * 60 * 1000; // 5 minutes
  /** stats collecetor timer **/
  private Timer     _statsCollectionTimer = null;
  /** startup time **/
  private long _startupTime = System.currentTimeMillis();


  /** failure/ success counts **/
  long  _totalAvailableURLCount =0;
  long  _totalProcessedURLCount = 0;
  long  _failedURLCount = 0;
  long  _successURLCount = 0;
  long  _loadCount = 0;

  /** queue stats **/
  int  _pendingCount = 0;
  int  _queuedCount  = 0;

  /** queue stats **/
  int  _activeHosts = 0;

  /** segment load counts **/
  int   _activeLoadCount = 0;
  int   _segmentScanPending = 0;

  /** crawl active flag **/
  boolean _crawlActive = false;
  /** crawl was stopped **/
  boolean _crawlStopped = false;

  /** shutdown flag **/
  boolean _shutdownFlag = false;

  /** local crawl history bloom filter **/
  URLFPBloomFilter _localBloomFilter;

  ReentrantLock _loaderStalledLock = new ReentrantLock();
  Condition        _loaderStalledCondition = _loaderStalledLock.newCondition();
  Condition        _loaderStalledEvent = null;

  enum LoaderStallReason { 
    None,
    Memory, // stalled due to low memory 
    MaxURLS, // loader stalled due to max urls allowed in queue 
    ActiveHostCount // load stalled due to too many active hosts in queue
  }
  LoaderStallReason _loaderStallReason = LoaderStallReason.None;

  Queue<CrawlSegmentStatus> _segmentLoadQueue = new PriorityQueue<CrawlSegmentStatus>(100,
      new Comparator<CrawlSegmentStatus>() {

    public int compare(CrawlSegmentStatus o1, CrawlSegmentStatus o2) {
      if (o1.getLoadHint() > o2.getLoadHint()) { 
        return 1;
      }
      else if (o1.getLoadHint() < o2.getLoadHint()) { 
        return -1;
      }
      return 0;
    } 
  });

  //Queue<CrawlSegmentStatus> _segmentLoadQueue = new LinkedList<CrawlSegmentStatus>();

  /** dns stats **/
  MovingAverage _dnsProcessResultsTime      = new MovingAverage(25);


  /** work unit status map **/
  private Map<Long,CrawlSegmentStatus> _statusMap = new LinkedHashMap<Long,CrawlSegmentStatus>() ;



  /** DNS Resolver Thread Pool **/
  private int             _hostQueuedForResolution = 0;
  private int             _urlsPendingResolution  = 0;
  private int             _dnsHighWaterMark;
  private int             _dnsLowWaterMark;
  private boolean         _highWaterMarkHit = false;
  private long            _cycleTime = -1;
  private LinkedList<CrawlSegmentDetail> _dnsDeferedSegments = new LinkedList<CrawlSegmentDetail>();
  private LinkedList<CrawlSegmentHost>  _dnsDeferedHosts  = new LinkedList<CrawlSegmentHost>();

  /** Various Lists **/
  private static SuffixStringMatcher _blackListedHostsMatcher;
  private static SessionIDURLNormalizer _sessionIDNormalizer = new SessionIDURLNormalizer();
  /** inverse cache **/
  private static NIODNSCache    _badDomainCache = new NIODNSCache();
  /** shared SubDomain Comparator instance **/
  SubDomainComparator _subDomainComparator = new SubDomainComparator();
  /** the active list id we are operating on **/
  int _activeListId = -1;

  /** constructor **/
  public CrawlerEngine(CrawlerServer server,int maxSockets,int dnsHighWaterMark,int dnsLowWaterMark,long cycleTime, int activeListId) { 
    _maxTCPSockets = maxSockets;
    _server = server;
    _dnsHighWaterMark = dnsHighWaterMark;
    _dnsLowWaterMark = dnsLowWaterMark;
    _cycleTime = cycleTime;
    _activeListId = activeListId;
  }

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


  private Appender createLogFileAppender(String logFileName) throws IOException { 

    DailyRollingFileAppender drfaAppender = new DailyRollingFileAppender(new CustomLoggerLayout(),_server.getLogDirectory() + "/" + logFileName,"yyyy-MM-dd");
    AsyncAppender asyncAppender = new AsyncAppender(8192);
    asyncAppender.addAppender(drfaAppender);
    return asyncAppender;
  }

  /** initialization
   * 
   * 
   */
  public boolean initialize(InetSocketAddress[] crawlInterfaceList) {

    _crawlInterfaces = crawlInterfaceList;
    _failureLog = new CustomLogger("CrawlerFailureLog");
    _GETLog = new CustomLogger("GETLog");
    _SuccessLog = new CustomLogger("SuccessLog");
    _DNSSuccessLog = new CustomLogger("DNSSuccessLog");
    _DNSFailureLog = new CustomLogger("DNSFailureLog");
    _RobotsLog = new CustomLogger("RobotsLog");
    _CrawlLogLog = new CustomLogger("CrawlLogLog");
    _CookieLogger = new CustomLogger("CookieLogger");
    

    NIOHttpConnection.setCookieLogger(_CookieLogger);

    try {

      _failureLog.addAppender(createLogFileAppender("crawlerFailures.log"));
      _GETLog.addAppender(createLogFileAppender("crawlerGETs.log"));
      _SuccessLog.addAppender(createLogFileAppender("crawlerSuccess.log"));
      _DNSSuccessLog.addAppender(createLogFileAppender("crawlerDNS.log"));
      _DNSFailureLog.addAppender(createLogFileAppender("crawlerDNSFailures.log"));
      _RobotsLog.addAppender(createLogFileAppender("robotsFetchLog.log"));
      _CrawlLogLog.addAppender(createLogFileAppender("crawlLog.log"));
      _CookieLogger.addAppender(createLogFileAppender("cookieLog.log"));

    } catch (IOException e) {
      e.printStackTrace();
    }

    LOG.info("Allocating BloomFilter");
    _localBloomFilter 
      = new URLFPBloomFilter(
          
          DEFAULT_LOCAL_BLOOMFILTER_ELEMENT_SIZE,
          DEFAULT_LOCAL_BLOOMFILTER_BITS_NUM_HASH_FUNCTIONS,
          DEFAULT_LOCAL_BLOOMFILTER_BITS_PER_ELEMENT);
    
    try {



      // register ourselves as log sink for dns events ... 
      // NIODNSLocalResolver.setLogger(this);

      if (Environment.detailLogEnabled())
        LOG.info("initialize - Recordstore says Database Id is: " + _databaseId);

      if (Environment.detailLogEnabled())
        LOG.info("initialize - Loading State");
      loadState();
      if (Environment.detailLogEnabled())
        LOG.info("initialize http CrawlQueue");

      if (_maxTCPSockets == -1) { 
        if (Environment.detailLogEnabled())
          LOG.info("Max TCP Sockets Unspecified. Defaulting to:" + DEFAULT_MAX_SIMULTANEOUS_CONNECTIONS);
        _maxTCPSockets = DEFAULT_MAX_SIMULTANEOUS_CONNECTIONS;
      }
      else { 
        if (Environment.detailLogEnabled())
          LOG.info("Max TCP Sockets is:" + _maxTCPSockets);
      }

      LOG.info("Starting CrawlDomain Disk Queueing Thread");
      CrawlList.startDiskQueueingThread(this.getEventLoop(),getServer().getDomainQueueDir());
      LOG.info("Initialize HTTP Crawl Queue");
      HttpFetcher fetcher = new HttpFetcher(_maxTCPSockets,crawlInterfaceList,getServer().getHostName());
      _httpCrawlQueue = new CrawlQueue(CrawlQueue.Protocol.HTTP,fetcher);

      if (getServer().enableCrawlLog()) { 
        if (Environment.detailLogEnabled())
          LOG.info("initializing crawl engine log ... ");
        try { 

          _crawlLog = new CrawlLog(this);
        }
        catch (IOException e) { 
          LOG.fatal("Exception thrown while initializing CrawlLog:" + CCStringUtils.stringifyException(e));
          return false;
        }
      }

      if (Environment.detailLogEnabled())
        LOG.info("loading Crawl Segments");

      if (getServer().externallyManageCrawlSegments() && !CrawlEnvironment.inUnitTestMode()) {
        kickOffCrawl();
      }

      return true;
    }
    catch (IOException e) { 
      LOG.fatal(e);

      return false;
    }
  }

  /** return the socket address associated with a crawl interface 
   * 
   * @param index the crawl interface index 
   * @return the socket address associated with that index ... 
   */
  public InetAddress getCrawlInterfaceGivenIndex(int index) { 
    if (index != -1 && index < _crawlInterfaces.length) { 
      return _crawlInterfaces[index].getAddress();
    }
    return null;
  }

  private boolean potentiallyAddToParseQueue(CrawlURL urlObject) throws IOException { 
    boolean enqueued = false;
    if (urlObject.getLastAttemptResult() == CrawlURL.CrawlResult.SUCCESS) { 
      NIOHttpHeaders finalHeaders = NIOHttpHeaders.parseHttpHeaders(urlObject.getHeaders());
      if (finalHeaders != null && finalHeaders.getValue(0) != null) { 
        int httpResult = HttpHeaderInfoExtractor.parseStatusLine(finalHeaders.getValue(0)).e0;
        if (httpResult == 200) { 
          // queue it up for parsing ...
          
        }
      }
    }
    return enqueued;
  }
  
  /** kick off crawl segment loader **/
  public void loadCrawlSegments() { 
    try { 
      loadCrawlSegments( new Callback() {
        // this callback is executed when the last segment has been successfully loaded
        public void execute() {

          LOG.info("loadCrawlSegment Completion Callback Excecuted - Checking to see if Checkpoint Possilbe");

          long currentTime = System.currentTimeMillis();

          if (getServer().enableCrawlLog() && _crawlLog.isCheckpointPossible(currentTime)) { 
            LOG.info("Delaying Crawl Startup - Checkpointing Logs to HDFS FIRST...");

            // start the checkpoint ... 
            _crawlLog.checkpoint(currentTime,new CheckpointCompletionCallback() {

              public void checkpointComplete(long checkpointId,Vector<Long> completedSegmentList) {
                LOG.info("CrawlLog Checkpoint:" + checkpointId + " completed");

                if (completedSegmentList != null) { 
                  // walk completed segments ... updating their crawl state ... 
                  for (long packedSegmentId : completedSegmentList) { 
                    // notify crawler engine of status change ... 
                    crawlSegmentComplete(packedSegmentId);
                  }
                }

                // now kick off the crawl ...
                kickOffCrawl();
              }

              public void checkpointFailed(long checkpointId,Exception e) {
                LOG.error("Checkpoint Failed for Checkpoint:" + checkpointId + " With Exception:" + CCStringUtils.stringifyException(e));
                throw new RuntimeException("Checkpoint Failed During Startup With Exception:" + CCStringUtils.stringifyException(e));
              } 

            }, currentTime);
          }
          else { 
            // kick off crawl immediately .. 
            kickOffCrawl();
          }
        }
      });
    }
    catch (IOException e) { 
      LOG.fatal("Caught IOException while loadCrawlSegments! Exception:" +CCStringUtils.stringifyException(e));
    }
  }


  public void shutdown() {
    LOG.info("Shuting down crawl engine");
    _shutdownFlag = true;
    if (_crawlActive) { 
      stopCrawl(null);
      while (_crawlActive) { 
        if (Thread.currentThread() == getEventLoop().getEventThread()) {
          LOG.info("Polling Selector while waiting for crawl to stop");
          try {
            getEventLoop().waitForIO(1000);
          } catch (IOException e) {
            LOG.error(CCStringUtils.stringifyException(e));
          }
        }
        else {
          LOG.info("Waiting for crawl to stop");
          try {
            Thread.currentThread().sleep(1000);
          } catch (InterruptedException e) {
          }
        }
      }
    }
    // clear crawl queue 
    _httpCrawlQueue.shutdown();
    _httpCrawlQueue = null;

    System.gc();

    // wait for loader / dns threads to exit 
    while (_activeLoadCount != 0 || _urlsPendingResolution != 0) { 
      if (Thread.currentThread() == getServer().getEventLoop().getEventThread()) { 
        try {
          LOG.info("Polling Event Thread Selector while waiting for loadcount / resolution count to go to zero. LoadCount:" 
              + _activeLoadCount + " PendingResolutionCount:" + _urlsPendingResolution);
          getEventLoop().waitForIO(1000);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      else { 
        LOG.info("Waiting for loadcount / resolution count to go to zero");
        try {
          Thread.currentThread().sleep(1000);
        } catch (InterruptedException e) {
        }
      }
    }
    LOG.info("load count / resolution count went to zero!");
    _dnsDeferedSegments.clear();
    _dnsDeferedHosts.clear();

    // null out crawl log 
    _crawlLog = null;
  }

  public void stopCrawlerCleanly() { 
    LOG.info("Clean Shutdown - Stopping Crawl");
    stopCrawl(new CrawlStopCallback() {

      public void crawlStopped() {
        LOG.info("Clean Shutdown - Crawl Stopped. Stopping Server");
        _server.stop();
        LOG.info("Clean Shutdown - Stopping Hadoop");
        try {
          FileSystem.closeAll();
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }

        LOG.info("Clean Shutdown - Exiting App");
        System.exit(1);

      } 

    });
  }

  public void kickOffCrawl() { 

    if (_cycleTime != -1 && !_server.disableCycleTimer()) { 

      SimpleDateFormat formatter = new SimpleDateFormat("yyyy.MM.dd hh:mm:ss z");
      LOG.info("Cycle Time is Set. Will AutoShutdown Crawler at:" + formatter.format(new Date(_cycleTime)));

      long delay = _cycleTime - System.currentTimeMillis();
      Timer cycleTimer = new Timer(delay,false,new Timer.Callback() {

        public void timerFired(Timer timer) {
          stopCrawlerCleanly();
        } 
      });
      getEventLoop().setTimer(cycleTimer);
    }

    LOG.info("kicking off crawl - Loading Crawl Segment");
    // now try to load a segment ... 
    potentiallyLoadNextSegment();
    LOG.info("Starting Crawl");
    // and then start the crawl ...
    startCrawl();

    // finally, if in unit test mode ... 
    if (CrawlEnvironment.inUnitTestMode()) {
      LOG.info("UnitTest Mode Detected - Runing Test...");
    }
  }

  private static int MS_IN_AN_SECOND = 1000;
  private static int MS_IN_AN_MINUTE = MS_IN_AN_SECOND * 60;
  private static int MS_IN_AN_HOUR    = MS_IN_AN_MINUTE * 60;

  private void startStatsCollector() { 

    _lastCrawlerStatsUploadTime = System.currentTimeMillis();
    _statsCollectionTimer = new Timer(STATS_COLLECTION_INTERVAL,true,new Timer.Callback() {

      public void timerFired(Timer timer) {

        synchronized(_stats) {

          //async stats ... 
          //getServer().getEventLoop().collectStats(_stats);

          // engine stats ...
          synchronized(this) { 


            long msSinceStartup = _startupTime - System.currentTimeMillis();

            int hours = (int) (msSinceStartup / MS_IN_AN_HOUR);
            msSinceStartup -= (hours * MS_IN_AN_HOUR);
            int minutes = (int)(msSinceStartup / MS_IN_AN_MINUTE);
            msSinceStartup -= (minutes * MS_IN_AN_MINUTE);
            int seconds = (int)(msSinceStartup / MS_IN_AN_SECOND);

            String upTimeString = String.format("%1$d Hours %2$d Minutes %3$d Seconds",hours,minutes,seconds);

            _stats.setStringValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.CrawlerEngine_UpTime,upTimeString);
            _stats.setStringValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.CrawlerEngine_BuildTime,System.getProperty("commoncrawl.build.date"));

            _stats.setLongValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.CrawlerEngine_TotalProcessedURLCount,_totalProcessedURLCount);
            //_crawlerStats.setUrlsProcessed(_crawlerStats.getUrlsProcessed() + _)
            _stats.setLongValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.CrawlerEngine_FetchFailedCount,_failedURLCount);
            _stats.setLongValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.CrawlerEngine_FetchSucceededCount,_successURLCount);

            _stats.setIntValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.CrawlerEngine_ActiveLoadCount,_activeLoadCount);
            _stats.setIntValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.CrawlerEngine_DeferredLoadCount,_segmentLoadQueue.size());
            _stats.setIntValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.CrawlerEngine_PendingCount,_pendingCount);
            _stats.setIntValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.CrawlerEngine_QueuedCount,_queuedCount);


            // resolver stats ... 
            _stats.setLongValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.DNS_TotalDNSQueries, getServer().getEventLoop().getResolver().getQueryCount());
            _stats.setLongValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.DNS_TotalCacheHits, getServer().getEventLoop().getResolver().getCacheHitCount());
            _stats.setLongValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.DNS_TotalCacheMisses, getServer().getEventLoop().getResolver().getCacheMissCount());

            //_stats.setDoubleValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.CrawlerEngine_DNSAddToCacheTime,NIODNSResolver.getDNSCache()._dnsAddToCacheTime.getAverage());
            //_stats.setDoubleValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.CrawlerEngine_DNSLookupFromCacheTime,NIODNSResolver.getDNSCache()._dnsLookupFromCacheTime.getAverage());
            // _stats.setLongValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.CrawlerEngine_DNSCacheNodeCount,NIODNSLocalResolver.getDNSCache().getActiveNodeCount());
            _stats.setDoubleValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.CrawlerEngine_DNSProcessResultTime,_dnsProcessResultsTime.getAverage());
          }


          // server stats ...
          getServer().collectStats(_stats);

          // crawlqueue / fetcher stats ... 
          _httpCrawlQueue.collectStats(_crawlerStats,_stats);

          if (getServer().enableCrawlLog()) { 
            // crawl log stats ... 
            _crawlLog.collectStats(_stats);
          }

          if (System.currentTimeMillis() - _lastCrawlerStatsUploadTime >= CRAWLER_STATS_FLUSH_INTERVAL) { 

            synchronized(_crawlerStats) { 
              _crawlerStats.setUrlsInFetcherQueue(_pendingCount);
              _crawlerStats.setUrlsInLoaderQueue(_queuedCount);
              _crawlerStats.setActiveDNSRequests(_hostQueuedForResolution);
              _crawlerStats.setQueuedDNSRequests(_dnsDeferedHosts.size());
              _crawlerStats.setCrawlerMemoryUsedRatio(JVMStats.getHeapUtilizationRatio());
            }

            CrawlerStats statsOut = null;
            // clone the stats 
            try {
              statsOut = (CrawlerStats) _crawlerStats.clone();
              statsOut.setTimestamp(System.currentTimeMillis());
            } catch (CloneNotSupportedException e) {
            }
            // clear collector stats ... 
            _crawlerStats.clear();
            // reset stats collection timer 
            _lastCrawlerStatsUploadTime = System.currentTimeMillis();
            // send the stats out ... 
            if (getServer().getStatsCollectorStub() != null) { 
              LogCrawlStatsRequest requestOut = new LogCrawlStatsRequest();

              requestOut.setCrawlerName(getServer().getHostName());
              requestOut.setCrawlerStats(statsOut);

              try {
                getServer().getStatsCollectorStub().logCrawlerStats(requestOut, new AsyncRequest.Callback<LogCrawlStatsRequest, NullMessage>() {

                  @Override
                  public void requestComplete(AsyncRequest<LogCrawlStatsRequest, NullMessage> request) {

                  }
                });
              } catch (RPCException e) {
                LOG.error(CCStringUtils.stringifyException(e));
              }
            }

          }
        }
      }
    });
    getServer().getEventLoop().setTimer(_statsCollectionTimer);
  }


  private void stopStatsCollector() { 
    if (_statsCollectionTimer != null) { 
      getServer().getEventLoop().cancelTimer(_statsCollectionTimer);
    }
  }

  RuntimeStatsCollector getStats() { return _stats; }

  public boolean isBlackListedHost(String hostName) {
    if (getServer().getDomainBlackListFilter() != null || getServer().getTemporaryBlackListFilter()!= null){
      String rootDomain = URLUtils.extractRootDomainName(hostName);
      if (rootDomain != null) { 
        if (getServer().getDomainBlackListFilter() != null){
          if (getServer().getDomainBlackListFilter().filterItem(rootDomain,hostName, "", null, null) == FilterResult.Filter_Reject) { 
            LOG.info("### FILTER Flagged:" + hostName + " as a BlackListed Host");
            return true;
          }
        }
        if (getServer().getTemporaryBlackListFilter()!= null) {
          if (getServer().getTemporaryBlackListFilter().filterItem(rootDomain,hostName, "", null, null) == FilterResult.Filter_Reject) { 
            LOG.info("### FILTER Flagged:" + hostName + " as a Temporarily BlackListed Host");
            return true;
          }
        }
      }
      else {
        LOG.warn("Got Invalid HostName during isBlackListedHost:" + hostName);
        return true;
      }
    }
    return false;
  }

  private static CrawlURLMetadata ipTestMetadata = new CrawlURLMetadata();
  private boolean isBlackListedIPAddress(int ipAddress) {

    if (getServer().getIPAddressFilter() != null) {
      ipTestMetadata.setServerIP(ipAddress);
      if (getServer().getIPAddressFilter().filterItem(null,null, null, ipTestMetadata, null) == FilterResult.Filter_Reject) {
        LOG.info("### FILTER IPAddress:" + IPAddressUtils.IntegerToIPAddressString(ipAddress) + " as a BlackListed IP");        
        return true;
      }
    }
    return false;
  }


  /** get the database uuid **/
  public String getDatabaseId() { return _databaseId; }


  /** get access to the server object **/
  public CrawlerServer getServer() { return _server; }

  /** get at the event loop **/
  public EventLoop getEventLoop() { return _server.getEventLoop(); }

  /** get access to the specialized crawler failure log **/
  public CustomLogger getFailureLog() { return _failureLog; }

  /** get access to the specialized crawler failure log **/
  public CustomLogger getGETLog() { return _GETLog; }

  /** get access to the specialized crawler success log **/
  public CustomLogger getSuccessLog() { return _SuccessLog; }

  /** get access to the specialized crawl log log **/
  public CustomLogger getCrawlLogLog() { return _CrawlLogLog; }

  /** get cookie log **/
  public CustomLogger getCookieLog() { return _CookieLogger; }

  /** get local bloom filter **/
  public URLFPBloomFilter getLocalBloomFilter() { return _localBloomFilter; }
  
  /** get access to the crawler stats data structure **/
  public CrawlerStats getCrawlerStats() { return _crawlerStats; }

  /** get access to the dns logger **/
  //public CustomLogger getDNSLog() { return _DNSSuccessLog; }

  /** get access to the dns failure logger **/
  //public CustomLogger getDNSFailureLog() { return _DNSFailureLog; }

  /** get the subdomain comparator **/
  public SubDomainComparator getSubDomainComparator() { return _subDomainComparator; }

  /** get pending url count **/
  public synchronized int getPendingURLCount() { return _pendingCount ; }
  /** get the total number of active urls in the system (pending + queued) **/
  public synchronized int getActiveURLCount() { return _pendingCount + _queuedCount; }
  /** get the number of active hosts in the queue **/
  public synchronized int getActiveHosts() { return _activeHosts; }
  /** increment / decrement active host count **/
  public void incDecActiveHostCount(int incDecAmount) {

    boolean loaderStalled = false;

    int activeCount = 0;

    synchronized(this) { 
      _activeHosts += incDecAmount;
      activeCount = _activeHosts;
      //LOG.info("### ACTIVEHOST Count:" + _activeHosts);
    }

    _loaderStalledLock.lock();

    if (_loaderStalledEvent != null && _loaderStallReason == LoaderStallReason.ActiveHostCount) { 
      loaderStalled = true;
    }

    _loaderStalledLock.unlock();


    if (loaderStalled) {
      if (activeCount < getMaxActiveHostsThreshold()) { 
        LOG.info("### LOADER Event Thread Acquiring Lock to Loader");
        // grab loader lock 
        _loaderStalledLock.lock();

        _loaderStallReason = LoaderStallReason.None;

        // at this point the event may have been nulled out ...         
        if (_loaderStalledEvent != null) { 
          // trigger event ... thus releasing loader thread (to continue loading the active segment)...
          LOG.info("### LOADER Event Thread Signalling Stall Event (activeHosts < MaxActiveThreshold)");
          _loaderStalledEvent.signal();
          // clear the event ... 
          _loaderStalledEvent = null;
        }
        //release loader lock 
        _loaderStalledLock.unlock();
        LOG.info("Releasing Lock - Signaled Loader");
      }
    }
  }

  private long _lastLoaderStalledDebugEvt = 0;

  /** increment decrement pending count **/
  public void incDecPendingQueuedURLCount(int pendingAmount,int queuedAmount) {

    int activeCount = 0;

    boolean loaderStalled = false;

    // atomically increment queue counts ...
    synchronized (this) {
      _pendingCount += pendingAmount; 
      _queuedCount += queuedAmount;
      // and safely calculate queued count .. 
      activeCount = _pendingCount + _queuedCount;
    }



    _loaderStalledLock.lock();

    if (_loaderStalledEvent != null && _loaderStallReason == LoaderStallReason.MaxURLS) { 
      loaderStalled = true;
    }

    _loaderStalledLock.unlock();

    if (loaderStalled) { 
      if (_lastLoaderStalledDebugEvt == 0 || (System.currentTimeMillis() - _lastLoaderStalledDebugEvt) >= 60000) {

        _lastLoaderStalledDebugEvt = System.currentTimeMillis();

        if (activeCount >= getMaxActiveThreshold()) { 
          LOG.info("### LOADER Loader Event Set but will not trigger because Active URL Count: " + activeCount + " >= " + getMaxActiveThreshold());
        }
        else if (_activeLoadCount != 1) { 
          LOG.info("### LOADER Event Set but will not trigger because Load Count: " + _activeLoadCount + " != 1");
        }
      }
    }

    // if the loader is waiting on the queue and active url count is less than threshold ...
    if (_activeLoadCount == 1 && loaderStalled && activeCount < getMaxActiveThreshold()) {
      LOG.info("### LOADER Event Thread Acquiring Lock to Loader");
      // grab loader lock 
      _loaderStalledLock.lock();

      _loaderStallReason = LoaderStallReason.None;

      // at this point the event may have been nulled out ...         
      if (_loaderStalledEvent != null) { 
        // trigger event ... thus releasing loader thread (to continue loading the active segment)...
        LOG.info("### LOADER Event Thread Signalling Stall Event (activeCount < MaxActiveThreshold)");
        _loaderStalledEvent.signal();
        // clear the event ... 
        _loaderStalledEvent = null;
      }
      //release loader lock 
      _loaderStalledLock.unlock();
      LOG.info("Releasing Lock - Signaled Loader");
    }
  }


  /** get set the max active url threshold **/
  public void setMaxActiveURLThreshold(int thresholdValue) { 
    _maxActiveURLS = thresholdValue;
  }

  public int getMaxActiveThreshold() { 
    return _maxActiveURLS;
  }

  public int getMaxActiveHostsThreshold() { 
    return _maxActiveHosts;
  }


  /** load state **/
  private void loadState() throws IOException { 

  }


  /** internal helper routine to load crawl segment metdata given list id **/
  private List<CrawlSegment> populateCrawlSegmentsFromHDFS(int listId) throws IOException {
    
    ArrayList<CrawlSegment> crawlSegments = new ArrayList<CrawlSegment>();
    
    LOG.info("Populating CrawlSegment(s) from HDFS for List:" + listId);
    // get root path for crawl segment data for the specified list id 
    Path hdfsSearchPath = CrawlSegmentLog.buildHDFSCrawlSegmentSearchPathForListId(listId,_server.getHostName());
    // scan hdfs for relevant path information for crawl segments
    FileSystem hdfs = CrawlEnvironment.getDefaultFileSystem();
    LOG.info("Searching for crawl segments with hdfs search path:"+ hdfsSearchPath);
    // scan hdfs for matching files ... 
    FileStatus fileStatusArray[] = hdfs.globStatus(hdfsSearchPath);
    LOG.info("Found:" + fileStatusArray.length + " segments at path:"+ hdfsSearchPath);
    
    // now walk matched set 
    for (FileStatus fileStatus : fileStatusArray) { 
      // segment id is the parent path name of the matched file
      String segmentName = fileStatus.getPath().getParent().getName();
      int segmentId = Integer.parseInt(segmentName);
      //now populate crawl segment information 
      CrawlSegment crawlSegment = new CrawlSegment();

      crawlSegment.setListId(listId);
      crawlSegment.setSegmentId(segmentId);

      LOG.info("adding crawl segment:"+crawlSegment.getSegmentId() + " for List:" + listId);

      crawlSegments.add(crawlSegment);
    }
    return crawlSegments;
  }

  /** internal load work unit routine **/
  private void loadCrawlSegments(final Callback completionCallback) throws IOException {

    LOG.info("Loading Crawl Segments");

    List<CrawlSegment> crawlSegments = populateCrawlSegmentsFromHDFS(_activeListId);

    LOG.info("defer loading lists");

    float loadPosition = 0.0f;


    // now sort the list by segment id 
    Collections.sort(crawlSegments,new Comparator<CrawlSegment>() {

      @Override
      public int compare(CrawlSegment o1, CrawlSegment o2) {
        return o1.getSegmentId() - o2.getSegmentId();
      } 
    });

    // now queue up load requests ... 
    for (CrawlSegment crawlSegment : crawlSegments) { 

      // if the segment has not been marked as completed but it is marked as crawling (we loaded the last time we ran)
      if (!crawlSegment.getIsComplete()) { 
        LOG.info("Delay Loading CrawlSegment:" + crawlSegment.getSegmentId() + " for List:" + crawlSegment.getListId());
        // delay load this segment ... 
        queueSegment(crawlSegment,loadPosition++);
      }
      else {
        if (Environment.detailLogEnabled())
          LOG.info("skipping already completed segment:" + crawlSegment.getSegmentId() + " during load");
      }
    }
    // call outer completion callback ... 
    LOG.info("Last Segment Loaded. Calling Completion Callback");
    completionCallback.execute();
  }

  
  public static CrawlSegment crawlSegmentFromCrawlSegmentStatus(CrawlSegmentStatus status) { 
    CrawlSegment segment = new CrawlSegment();
    
    segment.setListId(status.getListId());
    segment.setSegmentId(status.getSegmentId());

    return segment;
  }
  
  /** load the specified crawl segment 
   * 
   * @param crawlSegment
   * @param loadPosition
   * @return
   */

  public CrawlSegmentStatus queueSegment(final CrawlSegment crawlSegment,final float loadPosition) { 

    final CrawlSegmentStatus status = new CrawlSegmentStatus();

    // create a log object 
    final CrawlSegmentLog log = new CrawlSegmentLog(getServer().getDataDirectory(),crawlSegment.getListId(),crawlSegment.getSegmentId(),getServer().getHostName());

    status.setListId(crawlSegment.getListId());
    status.setSegmentId(crawlSegment.getSegmentId());
    status.setLoadStatus(CrawlSegmentStatus.LoadStatus.LOAD_PENDING);
    status.setCrawlStatus(CrawlSegmentStatus.CrawlStatus.UNKNOWN);
    status.setLoadHint(loadPosition);
    status.setUrlCount(0);
    status.setUrlsComplete(0);
    status.setIsDirty(true);

    _statusMap.put(CrawlLog.makeSegmentLogId(crawlSegment.getListId(), crawlSegment.getSegmentId()),status);

    if (getServer().enableCrawlLog()) { 
      // activate the segment log ... 
      activateSegmentLog(log);
    }

    LOG.info("Adding Segment:" + status.getSegmentId() +" to DelayLoad Queue with Hint:" + status.getLoadHint());
    // add item to load queue 
    _segmentLoadQueue.add(status);

    // and finally try to load the next segment if possible ... 
    if (_segmentScanPending == 0 ) { 
      potentiallyLoadNextSegment();
    }

    return status;
  }

  private LoadProgressCallback createLoadProgressCallback(final CrawlSegmentStatus status) { 

    return new LoadProgressCallback() {

      public boolean hostAvailable(final CrawlSegmentHost host,final int originalURLCount,final int completedURLCount) {

        if (_shutdownFlag == true) { 
          return false;
        }

        if (Environment.detailLogEnabled())
          LOG.info("### LOADER hostAvailable called on for host:" + host.getHostName());

        // check bad hosts table 
        if (isBadDomain(host.getHostName())) {
          if (CrawlEnvironment.detailLoggingEnabled)
            LOG.info("### LOADER Ignoring Bad Host during Segment Load. HostName:" + host.getHostName());
          return true;
        }

        final int availableCount = originalURLCount - completedURLCount;

        // increment a separate load count (how many urls have been loaded to date)
        _loadCount += availableCount;

        // check memory utilization ... if it has reached target threshold..
        if (!_shutdownFlag && JVMStats.getHeapUtilizationRatio() >= DEFAULT_LOADER_STALL_MEMORY_UTILIZATION_RATIO || (getPendingURLCount() + availableCount) > MAX_PENDING_URLS) {

          if (JVMStats.getHeapUtilizationRatio() >= DEFAULT_LOADER_STALL_MEMORY_UTILIZATION_RATIO)
            LOG.info("### LOADER Stalling ... Memory Utilization Reached or Exceeded Target Ratio:" + DEFAULT_LOADER_STALL_MEMORY_UTILIZATION_RATIO);
          else 
            LOG.info("### LOADER Stalling ... Pending URL Count:" + (getPendingURLCount() +availableCount) + " Exceeds Max Allowed:" + MAX_PENDING_URLS); 

          long waitStartTime = System.currentTimeMillis();
          long waitInterval = 60000;
          while (!_shutdownFlag && JVMStats.getHeapUtilizationRatio() >= DEFAULT_LOADER_RESUME_MEMORY_UTILIZATION_RATIO  || (getPendingURLCount() + availableCount) > MAX_PENDING_URLS) { 
            try {
              Thread.sleep(5000);

              if (System.currentTimeMillis() - waitStartTime >= waitInterval) {
                if (JVMStats.getHeapUtilizationRatio() >= DEFAULT_LOADER_RESUME_MEMORY_UTILIZATION_RATIO) { 
                  LOG.info("### LOADER Doing Full GC To Try and Reclaim Memory");
                  System.gc();
                  waitInterval = waitInterval *= 2;
                }
                waitStartTime = System.currentTimeMillis();
              }
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
          LOG.info("### LOADER Resuming Load. Memory Utilization:" + JVMStats.getHeapUtilizationRatio() + " Pending URL Count:" + (getPendingURLCount() + availableCount));
        }

        int pendingDiskOperationCount = CrawlList.getPendingDiskOperationCount();

        if (!_shutdownFlag && pendingDiskOperationCount > 10000) { 
          do {
            LOG.info("### LOADER Disk Queue: Waiting for pendingDiskOperationCount to drop below threshold - " + pendingDiskOperationCount);
            try {
              Thread.sleep(1);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
            pendingDiskOperationCount = CrawlList.getPendingDiskOperationCount();
          }
          while (pendingDiskOperationCount != 0 && !_shutdownFlag);

          LOG.info("### LOADER Disk Queue: pendingDiskOperationCount drop below threshold .. continuing load ... ");
        }


        // if the loading of this will exceed our max
        if (!_shutdownFlag && getActiveURLCount() >=  (getMaxActiveThreshold() + LOADER_OVERFLOW_ALLOWED)) { 
          LOG.info("### LOADER Exceeded Max Allowed Active URLS Threshold. Going to Sleep...");


          // acquire the loader stalled lock ... 
          _loaderStalledLock.lock();

          try { 

            while (!_shutdownFlag && getActiveURLCount() >=  (getMaxActiveThreshold() + LOADER_OVERFLOW_ALLOWED)) { 
              // set the stall event ... 
              _loaderStalledEvent = _loaderStalledCondition;
              _loaderStallReason  = LoaderStallReason.MaxURLS;

              LOG.info("## LOADER Stalling on MaxURLS. Waiting on Event...");

              // and release lock and wait for the condition to be set ... 
              try {
                while (!_shutdownFlag && _loaderStalledEvent.await(5000, TimeUnit.MILLISECONDS) == false);
              } catch (InterruptedException e) {

              } 
            }
            LOG.info("## LOADER Woke Up from Sleep - Continuing Load ...");
          }
          finally {
            // null out loader stalled event ... 
            _loaderStalledEvent = null;
            // we have to unlock the lock once we meet the criteria of our wait state condition ...
            _loaderStalledLock.unlock();
          }
        }

        // if the loading of this will exceed our max
        if (!_shutdownFlag && getActiveHosts() >=  getMaxActiveHostsThreshold()) { 
          LOG.info("### LOADER Active Hosts Count:" + getActiveHosts() +" Exceeds Threshold:" + getMaxActiveHostsThreshold() + " Going to Sleep...");


          // acquire the loader stalled lock ... 
          _loaderStalledLock.lock();

          try { 

            // set the stall event ... 
            _loaderStalledEvent = _loaderStalledCondition;
            _loaderStallReason  = LoaderStallReason.ActiveHostCount;

            LOG.info("## LOADER Stalling on MaxActiveHosts. Waiting on Event...");

            // and release lock and wait for the condition to be set ... 
            try {
              while (!_shutdownFlag && _loaderStalledEvent.await(5000, TimeUnit.MILLISECONDS) == false);
            } catch (InterruptedException e) {

            } 
            LOG.info("## LOADER Woke Up from Sleep - Continuing Load ...");
          }
          finally {
            // null out loader stalled event ... 
            _loaderStalledEvent = null;
            // we have to unlock the lock once we meet the criteria of our wait state condition ...
            _loaderStalledLock.unlock();
          }
        }


        // schedule an async event in the main thread ... 
        _server.getEventLoop().setTimer(new Timer(0,false,new Timer.Callback() {

          public void timerFired(Timer timer) {

            if (!_shutdownFlag) { 
              // update segment status ... 
              status.setUrlCount(status.getUrlCount() +originalURLCount );
              status.setUrlsComplete(status.getUrlsComplete() + completedURLCount);
              // update crawl status if neccessary ... 
              if (completedURLCount != originalURLCount && status.getCrawlStatus() != CrawlSegmentStatus.CrawlStatus.CRAWLING) { 
                status.setCrawlStatus(CrawlSegmentStatus.CrawlStatus.CRAWLING);
              }
              else if (status.getLoadStatus() == CrawlSegmentStatus.LoadStatus.LOAD_SUCCEEDED && status.getUrlCount() == status.getUrlsComplete()) { 
                status.setCrawlStatus(CrawlSegmentStatus.CrawlStatus.CRAWL_COMPLETE);
                status.setIsComplete(true);
              }
              // set dirty flag for segment 
              status.setIsDirty(true);

              // update total available count ... 
              _totalAvailableURLCount += originalURLCount - completedURLCount;

              // increment pending count ...
              incDecPendingQueuedURLCount(originalURLCount - completedURLCount, 0);
              // and finally, submit the host for distribution ...
              if (availableCount != 0 && !_shutdownFlag) { 
                distributeSegmentHost(host);
              }
            }
          } 

        }));

        return (!_shutdownFlag);
      } 
    };
  }

  CompletionCallback<CrawlSegmentStatus> createCompletionCallback(final CrawlSegment crawlSegment,final CrawlSegmentStatus status) { 

    return new CompletionCallback<CrawlSegmentStatus>() {

      public void taskComplete(CrawlSegmentStatus foo) {
        if (CrawlEnvironment.detailLoggingEnabled)
          LOG.info("### SYNC Task Completion Callback for List:" + crawlSegment.getListId() + " Segment:" + crawlSegment.getSegmentId());
        if (!_shutdownFlag) { 
          if (Environment.detailLogEnabled())
            LOG.info("### LOADER Load for Segment:"+crawlSegment.getSegmentId() + " SUCCEEDED");

          // update status ... 
          status.setLoadStatus(CrawlSegmentStatus.LoadStatus.LOAD_SUCCEEDED);
          // mark the status dirty ... 
          status.setIsDirty(true);
        }

        --_activeLoadCount;

        if (!_shutdownFlag) { 
          // now potentially load any deferred segments ... 
          potentiallyLoadNextSegment();
        }

      }

      public void taskFailed(Exception e) {
        if (CrawlEnvironment.detailLoggingEnabled)
          LOG.info("### LOADER Load for Segment:" + crawlSegment.getSegmentId() + " FAILED With Error: " + StringUtils.stringifyException(e));

        status.setUrlsComplete(0);
        status.setLoadStatus(CrawlSegmentStatus.LoadStatus.LOAD_FAILED);
        status.setIsDirty(true);

        --_activeLoadCount;

        if (!_shutdownFlag) { 
          // now potentially load any deferred segments ... 
          potentiallyLoadNextSegment();
        }

      }
    };	  
  }


  /** internal loadWorkUnit routine **/
  private CrawlSegmentStatus loadCrawlSegment(final CrawlSegment crawlSegment) {

    _activeLoadCount++;

    // mark the segment as crawling ... 
    crawlSegment.setIsCrawling(true);

    final CrawlSegmentStatus status = new CrawlSegmentStatus();

    status.setListId(crawlSegment.getListId());
    status.setSegmentId(crawlSegment.getSegmentId());
    status.setLoadStatus(CrawlSegmentStatus.LoadStatus.LOADING);
    status.setCrawlStatus(CrawlSegmentStatus.CrawlStatus.UNKNOWN);
    status.setUrlCount(0);
    status.setUrlsComplete(0);

    status.setIsDirty(true);

    _statusMap.put(CrawlLog.makeSegmentLogId(crawlSegment.getListId(), crawlSegment.getSegmentId()),status);


    if (Environment.detailLogEnabled())
      LOG.info("loading crawl segment:"+crawlSegment.getSegmentId());


    if (!getServer().externallyManageCrawlSegments()) { 

      // remove crawl segment log from crawl log data structure 
      // (we need to do this to protect the data structure from corruption, since the underlying 
      //  worker thread walks the log and reconciles it against the segment data)
      final CrawlSegmentLog segmentLogObj = (getServer().enableCrawlLog()) ? _crawlLog.removeSegmentLog(crawlSegment.getListId(),crawlSegment.getSegmentId()) : null;

      if (segmentLogObj == null && getServer().enableCrawlLog()) {
        _activeLoadCount--;
        throw new RuntimeException("Expected Non-NULL CrawlSegmentLog for Segment:" + crawlSegment.getSegmentId());
      }


      getServer().getDefaultThreadPool().execute(new ConcurrentTask<CrawlSegmentStatus>(getServer().getEventLoop(), 

          new Callable<CrawlSegmentStatus>(){

        public CrawlSegmentStatus call() throws Exception {

          try { 


            LOG.info("### SYNC:Loading SegmentFPInfo for List:" + crawlSegment.getListId() + " Segment:" + crawlSegment.getSegmentId());
            // load work unit fingerprint detail  ...
            final CrawlSegmentFPMap urlFPMap = SegmentLoader.loadCrawlSegmentFPInfo(crawlSegment.getListId(),crawlSegment.getSegmentId(),CrawlerEngine.this.getServer().getHostName(),
                new SegmentLoader.CancelOperationCallback() {

              @Override
              public boolean cancelOperation() {
                return _shutdownFlag;
              }
            });

            if (_shutdownFlag) { 
              LOG.info("### SYNC:EXITING LOAD OF List:" + crawlSegment.getListId() + " Segment:" + crawlSegment.getSegmentId());
              return new CrawlSegmentStatus();
            }

            if (getServer().enableCrawlLog()) { 
              LOG.info("### SYNC: Syncing Log to SegmentFPInfo for List:" + crawlSegment.getListId() + " Segment:" + crawlSegment.getSegmentId());
              // re-sync log to segment ... 
              segmentLogObj.syncToLog(urlFPMap);
            }

            LOG.info("### SYNC: Sync for List:" + crawlSegment.getListId() + " Segment:" + crawlSegment.getSegmentId() + " Returned:" + urlFPMap._urlCount + " Total URLS and " + urlFPMap._urlsComplete + " CompleteURLS");

            if (!_shutdownFlag) { 
              // now activate the segment log ... 
              final Semaphore segActiveSemaphore = new Semaphore(0);


              // check for completion here ... 
              if (urlFPMap._urlCount == urlFPMap._urlsComplete && !_shutdownFlag) { 
                LOG.info("### SYNC: For List:" + crawlSegment.getListId() + " Segment:" + crawlSegment.getSegmentId() +" indicates Completed Segment.");

                _server.getEventLoop().setTimer(new Timer(1,false,new Timer.Callback() {

                  public void timerFired(Timer timer) {
                    LOG.info("### SYNC: For List:" + crawlSegment.getListId() + " Segment:" + crawlSegment.getSegmentId() +" setting Status to CompletedCompleted Segment.");

                    if (!_shutdownFlag) { 
                      // update segment status ... 
                      status.setUrlCount(urlFPMap._urlCount);
                      status.setUrlsComplete(urlFPMap._urlCount);
                      // update crawl status  
                      status.setCrawlStatus(CrawlSegmentStatus.CrawlStatus.CRAWL_COMPLETE);
                      status.setIsComplete(true);
                      // set dirty flag for segment 
                      status.setIsDirty(true);
                    }                      
                    // and release semaphore ... 
                    segActiveSemaphore.release();

                  } 

                }));
              }
              else { 

                _server.getEventLoop().setTimer(new Timer(1,false,new Timer.Callback() {

                  public void timerFired(Timer timer) {
                    if (!_shutdownFlag) { 
                      if (getServer().enableCrawlLog()) { 
                        //back in primary thread context, so go ahead and SAFELY re-activate the segment log ... 
                        activateSegmentLog(segmentLogObj);
                      }
                    }
                    // and release semaphore ... 
                    segActiveSemaphore.release();
                  } 

                }));
              }
              // wait for segment activation ... 
              segActiveSemaphore.acquireUninterruptibly();  
            }

            // now if complete return immediately 
            if (urlFPMap._urlCount != urlFPMap._urlsComplete && !_shutdownFlag) {


              LOG.info("### LOADER Loading CrawlSegment Detail for Segment:" + crawlSegment.getSegmentId());

              SegmentLoader.loadCrawlSegment(
                  crawlSegment.getListId(),
                  crawlSegment.getSegmentId(),
                  CrawlerEngine.this.getServer().getHostName(),
                  urlFPMap,
                  null,
                  createLoadProgressCallback(status),
                  new SegmentLoader.CancelOperationCallback() {

                    @Override
                    public boolean cancelOperation() {
                      return _shutdownFlag;
                    }
                  }
                  );
            }

          }
          catch (Exception e) { 
            LOG.error(StringUtils.stringifyException(e));
            throw e;
          }

          return status;
        } 
      },
      createCompletionCallback(crawlSegment,status)          
          ) 
          );
    }
    else { 
      getServer().loadExternalCrawlSegment(crawlSegment,createLoadProgressCallback(status),createCompletionCallback(crawlSegment, status),status);
    }
    return status;
  }	

  /** active the specified segment log **/
  private void activateSegmentLog(CrawlSegmentLog log) {
    if (getServer().enableCrawlLog()) { 
      if (Environment.detailLogEnabled())
        LOG.info("activating segment log for segment:" + log.getSegmentId());
      _crawlLog.addSegmentLog(log);
    }
  }

  private CrawlLog getCrawlLog() { 
    return _crawlLog;
  }

  private static String extractHostNameFromCrawlSegmentHost(CrawlSegmentHost host) { 
    String hostNameOut = null;
    if (host.getUrlTargets().size() != 0) {
      CrawlSegmentURL url = host.getUrlTargets().get(0);
      if (url != null) { 
        String urlString = url.getUrl();
        //LOG.info("Extracting HostName from url:" + urlString);
        String extractedHostName = URLUtils.fastGetHostFromURL(urlString);
        //LOG.info("Extracted HostName:" + extractedHostName);
        return extractedHostName;
      }
    }
    else { 
      LOG.error("Zero URL Target for Host:" + host.getHostName() + "!");
    }
    return null;
  }

  /** resolve IP addresses for remaining hosts in work unit **/
  private void distributeSegmentHost(final CrawlSegmentHost host) {

    if (_shutdownFlag)
      return;

    if (Environment.detailLogEnabled())
      LOG.info("Distributing Host:" + host.getHostName() + "URL Count:" + host.getUrlTargets().size());

    // get host name 
    final String hostName = extractHostNameFromCrawlSegmentHost(host);

    if (hostName == null) { 
      LOG.error("No Valid Extracted HostName found for host: " + host.getHostName());
      processHostIPResolutionResult(host,false,null);
      return;
    }
 
    // cache lookup happened in segment load ... so just check to see if IP is populated ... 
    if (host.isFieldDirty(CrawlSegmentHost.Field_IPADDRESS)) {
      if (Environment.detailLogEnabled())
        LOG.info("Immediately Processing " + host.getUrlTargets().size() + " Items for SuperHost:" + host.getHostName());
      // if so, immediately process results
      processHostIPResolutionResult(host,false,null);
    }
    // otherwise queue up for result ... 
    else {

      if ((_highWaterMarkHit && _hostQueuedForResolution <= _dnsHighWaterMark) || (!_highWaterMarkHit && _hostQueuedForResolution < _dnsHighWaterMark)) {

        // increment pending resolution count ... 
        _hostQueuedForResolution++;
        // and increment urls pending resolution

        if (_hostQueuedForResolution == _dnsHighWaterMark) {
          if (!_highWaterMarkHit) { 
            if (CrawlEnvironment.detailLoggingEnabled)
              LOG.info("### DNS High Water Mark Hit. PendingResolutions:" + _hostQueuedForResolution);
            _highWaterMarkHit = true;
          }
        }

        if (Environment.detailLogEnabled())
          LOG.info("Scheduling Resolution for Host:" + hostName);

        // schedule resolution ... 
        NIODNSQueryClient queryClient = new NIODNSQueryClient() {

          @Override
          public void AddressResolutionFailure(NIODNSResolver source,String hostName, Status status, String errorDesc) {
            // LOG.info("Got AddressResolutionFailure for host:" + hostName);
            // decrement pending resolution count ... 
            _hostQueuedForResolution--;

            if (_hostQueuedForResolution <= _dnsLowWaterMark && _highWaterMarkHit) {
              if (!_shutdownFlag) { 
                feedDNSQueue();
              }
            }

            logDNSFailure(host.getHostName(), errorDesc);

            LOG.error("Host IP Resolution for Host:"+host.getHostName() + " FAILED with Status:" + status.toString() + " ErrorDesc:" + errorDesc);

            if (!_shutdownFlag) { 
              // now react to the result ... 
              processHostIPResolutionResult(host,true,null);
            }
          }

          @Override
          public void AddressResolutionSuccess(NIODNSResolver source,String hostName, String name, InetAddress address, long addressTTL) {
            //LOG.info("Got AddressResolutionSuccess for host:" + hostName);
            // decrement pending resolution count ... 
            _hostQueuedForResolution--;

            logDNSQuery(hostName, address, addressTTL, null);

            if (_hostQueuedForResolution <= _dnsLowWaterMark && _highWaterMarkHit) {
              if (!_shutdownFlag) { 
                feedDNSQueue();
              }
            }

            int hostAddress = 0;

            if (address != null && address.getAddress() != null) {

              byte[] addr = address.getAddress();

              if (addr.length == 4) {
                hostAddress  = IPAddressUtils.IPV4AddressToInteger(addr);
              }
            }

            if (hostAddress != 0) { 
              // and update segment host's ip info ... 
              host.setIpAddress(hostAddress);
              host.setTtl(Math.max(addressTTL,System.currentTimeMillis() + CrawlEnvironment.MIN_DNS_CACHE_TIME));

              // log it 
              //  LOG.info(host.getHostName() + " " +IPAddressUtils.IntegerToIPAddressString(hostAddress) + " " + result.getTTL());

              if (!_shutdownFlag) { 
                // now react to the result ... 
                processHostIPResolutionResult(host,false,null);
              }
            }
            else { 

              //if (Environment.detailLogEnabled())
              LOG.error("Host IP Resolution for Host:"+host.getHostName() + " FAILED with Zero IP");

              if (!_shutdownFlag) { 
                // now react to the result ... 
                processHostIPResolutionResult(host,true,null);
              }
            }
          }

          @Override
          public void DNSResultsAvailable() {}

          @Override
          public void done(NIODNSResolver source,FutureTask<DNSQueryResult> task) {
          }

        };
        try {
          getServer().getDNSServiceResolver().resolve(queryClient, hostName,false, false,DEFAULT_DNS_TIMEOUT);
        } catch (IOException e) {
          LOG.error(CCStringUtils.stringifyException(e));

          // decrement pending resolution count ... 
          _hostQueuedForResolution--;

          if (_hostQueuedForResolution <= _dnsLowWaterMark && _highWaterMarkHit) {
            if (!_shutdownFlag) { 
              feedDNSQueue();
            }
          }

          LOG.error("Host IP Resolution for Host:"+host.getHostName() + " FAILED with Exception:" + CCStringUtils.stringifyException(e));
          // now react to the result ...
          if (!_shutdownFlag) { 
            processHostIPResolutionResult(host,true,null);
          }
        }

      }
      //otherwise if we need to defer this item ... 
      else {

        if (Environment.detailLogEnabled())
          LOG.info("Deferring DNS Lookup for Host:" + host.getHostName());

        _dnsDeferedHosts.add(host);
      }
    }
  }

  Collection<CrawlSegmentHost> breakoutHostsBySubDomain(CrawlSegmentHost incomingHost) { 
    if (CrawlEnvironment.detailLoggingEnabled)
      LOG.info("Breaking out hosts by subdomain for incoming host:" + incomingHost.getHostName());
    long timeStart = System.currentTimeMillis();

    Map<String,CrawlSegmentHost> mapOfHostsByName = new TreeMap<String,CrawlSegmentHost>();
    CrawlSegmentHost lastHost = null;

    for (CrawlSegmentURL target : incomingHost.getUrlTargets()) {

      String url = target.getUrl();

      String hostName = URLUtils.fastGetHostFromURL(url);
      if (hostName.length() != 0) {
        CrawlSegmentHost host = lastHost;

        if (host != null && !host.equals(hostName)) { 
          host = mapOfHostsByName.get(hostName);
        }
        if (host == null) { 
          host = new CrawlSegmentHost();

          host.setHostName(hostName);
          byte hostNameAsBytes[] = hostName.getBytes();
          host.setHostFP(FPGenerator.std64.fp(hostNameAsBytes,0,hostNameAsBytes.length));
          host.setSegmentId(incomingHost.getSegmentId());
          mapOfHostsByName.put(hostName, host);
        }
        lastHost = host;

        host.getUrlTargets().add(target);
      }
    }
    long timeEnd = System.currentTimeMillis();
    if (CrawlEnvironment.detailLoggingEnabled)
      LOG.info("Breakout took:" + (timeEnd - timeStart) + "MS and returned:" + mapOfHostsByName.size() + " Elements");

    return mapOfHostsByName.values();
  }

  void feedDNSQueue() { 

    if (Environment.detailLogEnabled())
      LOG.info("### DNS Feeding DNS Queue. PendingResolutions:" + _hostQueuedForResolution);

    _highWaterMarkHit = false;

    while(!_highWaterMarkHit && _hostQueuedForResolution < _dnsHighWaterMark) {
      if (_dnsDeferedHosts.size() != 0) {
        distributeSegmentHost(_dnsDeferedHosts.removeFirst());
        /*
        CrawlSegmentHost masterHost = ;
        for (CrawlSegmentHost host : breakoutHostsBySubDomain(masterHost)) { 
          ;
        }
         */
      }
      else { 
        break;
      }
    }
  }

  /** process host ip resolution results **/
  public void processHostIPResolutionResult(CrawlSegmentHost host,boolean failed,CrawlItemStatusCallback callback) {

    synchronized (_crawlerStats) { 
      if (failed) { 
        _crawlerStats.setFailedDNSRequests(_crawlerStats.getFailedDNSRequests() + 1);
      }
      else { 
        _crawlerStats.setSuccessfullDNSRequests(_crawlerStats.getSuccessfullDNSRequests() + 1);
      }
    }

    long timeStart = System.currentTimeMillis();

    boolean blackListedHost = false;
    // check to see if the host is blocked ...
    if (isBlackListedHost(host.getHostName())) { 
      if (Environment.detailLogEnabled())
        LOG.info("Rejecting " + host.getUrlTargets().size() + " URLS for Black Listed Host:" + host.getHostName());
      blackListedHost = true;
    }

    if (isBlackListedIPAddress(host.getIpAddress())) { 
      if (Environment.detailLogEnabled())
        LOG.info("Rejecting " + host.getUrlTargets().size() + " URLS for Black Listed IP:" + IPAddressUtils.IntegerToIPAddressString(host.getIpAddress()));
      blackListedHost = true;
    }

    // walk urls in host ... 
    CrawlSegmentURL segmentURL = null;

    // capture original url count ... 
    int originalURLCount = host.getUrlTargets().size();

    // decrement pending url count ... and artificially inflate queued count (for now)
    incDecPendingQueuedURLCount(-originalURLCount,originalURLCount);
    // increment processed url count ... 
    _totalProcessedURLCount += originalURLCount;

    // ok, sort targets by positon first 
    Collections.sort(host.getUrlTargets(),new Comparator<CrawlSegmentURL>() {

      @Override
      public int compare(CrawlSegmentURL o1, CrawlSegmentURL o2) {
        return (o1.getOriginalPosition() < o2.getOriginalPosition()) ? -1 : (o1.getOriginalPosition() > o2.getOriginalPosition()) ? 1 : 0;  
      } 
    });

    // now walk targets ... 
    for (int i=0;i<host.getUrlTargets().size();++i) { 

      // get the url at the current index .. 
      segmentURL = host.getUrlTargets().get(i);

      boolean badSessionIDURL = false;
      boolean malformedURL = false;
      
      URLFP fp = URLUtils.getURLFPFromURL(segmentURL.getUrl(),false);
      
      if (fp == null) {
        malformedURL = true;
      }

      if (!failed && !blackListedHost) { 
        /*
	      try {
          if (_sessionIDNormalizer.normalize(segmentURL.getUrl(), "") != segmentURL.getUrl()) { 
            badSessionIDURL = true;
          }
        } catch (MalformedURLException e) {
          LOG.error("Malformed URL Detected during SessionID Normalize. URL:" + segmentURL.getUrl() + " Exception:" + CCStringUtils.stringifyException(e));
          malformedURL = true;
        }
         */        
      }

      // if ip address is zero... this indicates a dns failure ... react accordingly ... 
      if (failed || blackListedHost || badSessionIDURL || malformedURL) {
        // remove the item from the list ... 
        host.getUrlTargets().remove(i);
        --i;
        if (failed) { 
          CrawlTarget.failURL(CrawlTarget.allocateCrawlURLFromSegmentURL(host.getSegmentId(),host,segmentURL,false),null, CrawlURL.FailureReason.DNSFailure,"DNS Failed during URL Distribution");
        }
        else  { 
          if (blackListedHost) 
            CrawlTarget.failURL(CrawlTarget.allocateCrawlURLFromSegmentURL(host.getSegmentId(),host,segmentURL,false),null, CrawlURL.FailureReason.BlackListedHost,"URL Rejected - Black Listed Host");
          else if (badSessionIDURL)
            CrawlTarget.failURL(CrawlTarget.allocateCrawlURLFromSegmentURL(host.getSegmentId(),host,segmentURL,false),null, CrawlURL.FailureReason.MalformedURL,"URL Rejected - Bad SessionID URL");
          else 
            CrawlTarget.failURL(CrawlTarget.allocateCrawlURLFromSegmentURL(host.getSegmentId(),host,segmentURL,false),null, CrawlURL.FailureReason.MalformedURL,"URL Rejected - Malforned URL");
        }
      }
      else { 

        String url = segmentURL.getUrl();
        // ... identify protocol ... 
        CrawlQueue.Protocol protocol = CrawlQueue.identifyProtocol(url);

        // we only support http for now ... 
        if (protocol != CrawlQueue.Protocol.HTTP) { 
          LOG.error("No protocol available for URL:"+url + " Segment:"+host.getSegmentId());
          // remove the item from the list ... 
          host.getUrlTargets().remove(i);
          --i;
          // immedialtely fail this url ... 
          CrawlTarget.failURL(CrawlTarget.allocateCrawlURLFromSegmentURL(host.getSegmentId(),host,segmentURL,false),null, CrawlURL.FailureReason.UnknownProtocol,"Uknown Protocol encountered during URL Distribution.");
        }
      }
    }

    // now if we have some urls to crawl .. 
    if (host.getUrlTargets().size() != 0) {
      // remember original size .. 
      int originalSize = host.getUrlTargets().size();
      // submit it to the http crawl queue
      int queuedSize = _httpCrawlQueue.queueHost(host.getSegmentId(),host.getListId(),host,callback);
      // check delta .. 
      if (queuedSize < originalSize) { 
        if (Environment.detailLogEnabled())
          LOG.info(Integer.toString(originalSize - queuedSize) + " Entries DROPPED for Domain:" + host.getHostName());
        // decrement queued count again by the number of entries that were dropped on the floor ... 
        incDecPendingQueuedURLCount(0,-(originalSize - queuedSize));
      }
    }

    host.clear();

    long timeEnd  = System.currentTimeMillis();
    _dnsProcessResultsTime.addSample((double)timeEnd-timeStart);
  }


  private static String buildCrawlSegmentKey(int listId,int segmentId) { 
    CrawlSegment segment = new CrawlSegment();
    segment.setListId(listId);
    segment.setSegmentId(segmentId);
    return segment.getKey();
  }

  private void potentiallyLoadNextSegment() {

    if (_segmentScanPending == 0) {

      // if active url count is less than max threshold .. 
      if (_activeLoadCount == 0 && _segmentLoadQueue.size()  != 0 && getActiveURLCount() < getMaxActiveThreshold()) { 

        CrawlSegmentStatus  loadTarget = _segmentLoadQueue.remove();

        // now if a load target was found ... 
        if (loadTarget != null) { 

          // get the crawl segment object ... 
          CrawlSegment segment = crawlSegmentFromCrawlSegmentStatus(loadTarget);
          if (Environment.detailLogEnabled())
            LOG.info("potenitallyLoadNextSegment returned segment:" + segment.getSegmentId() + " from list:" + segment.getListId());
          loadCrawlSegment(segment);
        }
      }
    }
  }

  public interface CrawlStopCallback { 
    public void crawlStopped();
  }


  /** stop the crawl process and clear all host queues **/
  public void stopCrawl(final CrawlStopCallback callback) { 

    if (_crawlActive) { 
      LOG.info("stopCrawl - stopping HttpQueue");

      _httpCrawlQueue.stopCrawl();

      // stop disk queue thread 
      CrawlList.stopDiskQueueingThread();

      stopStatsCollector();

      if (getServer().enableCrawlLog()) { 
        LOG.info("stopCrawl - stopping LogFlusher");
        _crawlLog.stopLogFlusher(new LogFlusherStopActionCallback() {

          public void stopComplete() {
            _crawlActive = false;
            _crawlStopped = true;
            LOG.info("stopCrawl - LogFlusher Stop Complete");
            // notify caller if necessary ... 
            if (callback != null) {
              LOG.info("stopCrawl - LogFlusher Stop Complete - Notifying Caller");
              callback.crawlStopped();
            }
          } 
        });
      }
      else { 
        _crawlActive = false;
        if (callback != null) { 
          callback.crawlStopped();
        }
      }
    }
    else { 
      if (callback != null) { 
        callback.crawlStopped();
      }
    }
  }

  /** start crawling **/
  public void startCrawl() {
    if (!_crawlActive) { 
      if (Environment.detailLogEnabled())
        LOG.info("startCrawl");
      startStatsCollector();
      _httpCrawlQueue.startCrawl(_crawlStopped);
      _crawlStopped = false;
      if (getServer().enableCrawlLog()) {
        _crawlLog.startLogFlusher();
      }
      _crawlActive = true;
    }
  }

  /** clear persistent and in memory state **/
  public void clearState() { 

    // stop the crawl ... 
    stopCrawl(null);
    // stop stats collection timer ... 
    stopStatsCollector();
    // clear queues ... 
    _httpCrawlQueue.clear();
    // clear internal data structures ...
    _statusMap.clear();

  }

  public final void logSuccessfulRobotsGET(NIOHttpConnection connection, CrawlTarget url) { 

    StringBuffer sb = new StringBuffer();

    sb.append(String.format("%1$20.20s ",CCStringUtils.dateStringFromTimeValue(System.currentTimeMillis())));
    sb.append(String.format("%1$16.16s ",(connection.getLocalAddress()!=null) ? connection.getLocalAddress().getAddress() : "UNDEFINED"));
    sb.append(String.format("%1$16.16s ",connection.getResolvedAddress()));
    sb.append(String.format("%1$4.4s ",  url.getResultCode()));
    sb.append(String.format("%1$10.10s ",connection.getDownloadLength()));
    sb.append(String.format("%1$10.10s ",connection.getConnectTime()));
    sb.append(String.format("%1$10.10s ",connection.getUploadTime()));
    sb.append(String.format("%1$10.10s ",connection.getDownloadTime()));
    if ((url.getFlags() & CrawlURL.Flags.IsRedirected) != 0) { 
      sb.append(url.getRedirectURL());
      sb.append(" ");
    }
    sb.append(url.getOriginalURL());
    getSuccessLog().info(sb.toString());
  }


  public final void logSuccessfulGET(NIOHttpConnection connection, CrawlURL url) { 

    StringBuffer sb = new StringBuffer();

    sb.append(String.format("%1$20.20s ",CCStringUtils.dateStringFromTimeValue(System.currentTimeMillis())));
    sb.append(String.format("%1$16.16s ",(connection.getLocalAddress()!=null) ? connection.getLocalAddress().getAddress() : "UNDEFINED"));
    sb.append(String.format("%1$16.16s ",connection.getResolvedAddress()));
    sb.append(String.format("%1$4.4s ",  url.getResultCode()));
    sb.append(String.format("%1$10.10s ",url.getContentRaw().getCount()));
    sb.append(String.format("%1$10.10s ",connection.getConnectTime()));
    sb.append(String.format("%1$10.10s ",connection.getUploadTime()));
    sb.append(String.format("%1$10.10s ",connection.getDownloadTime()));
    if ((url.getFlags() & CrawlURL.Flags.IsRedirected) != 0) { 
      sb.append(url.getRedirectURL());
      sb.append(" ");
    }
    sb.append(url.getUrl());
    getSuccessLog().info(sb.toString());
  }

  void fetchStarting(CrawlTarget target,NIOHttpConnection connection) { 
    _server.fetchStarting(target,connection);
  }

  /** callback triggered whenever a crawl of an item succeeds or fails .. **/
  void crawlComplete(NIOHttpConnection connection,CrawlURL url,CrawlTarget optTargetObj,boolean success) {

    if (getServer().enableCrawlLog()) { 

      // if robots get 
      if ((url.getFlags() & CrawlURL.Flags.IsRobotsURL) != 0) {
        getCrawlLog().getRobotsSegment().completeItem(url);
      }
      // else regular fetch ... 
      else { 
        // ok check to see if we have a parse queue 
        if (_server.isParseQueueEnabled()) { 
          if (success && url.isFieldDirty(CrawlURL.Field_CRAWLDIRECTIVEJSON) 
              && url.getCrawlDirectiveJSONAsTextBytes().getLength() != 0) { 
            
            try {
              if (potentiallyAddToParseQueue(url)) { 
                // mark as in parse queue 
                url.setFlags(url.getFlags() | CrawlURL.Flags.InParseQueue);
              }
              
            } catch (IOException e) {
              LOG.error(CCStringUtils.stringifyException(e));
            }
            
          }
        }
        //update segment log ...
        CrawlSegmentLog segmentLog = getCrawlLog().getLogForSegment(url.getListId(),(int)url.getCrawlSegmentId());

        if (segmentLog != null) { 
          segmentLog.completeItem(url);
        }
        else { 
          LOG.error("Segement Log for List:" + url.getListId() + " Segment:"+ url.getCrawlSegmentId() + "  is NULL (during CrawlComplete) for URL:" + url.getUrl());
        }

        // IFF target has no valid segment id ... then this is a high priority request ... delegate to outer controller 
        if (url.getCrawlSegmentId() == -1) { 
          // notify server (in case it delegates the call)
          getServer().crawlComplete(connection, url, optTargetObj,success);
        }
        else { 

          CrawlSegmentStatus workUnitStatus = _statusMap.get(CrawlLog.makeSegmentLogId(url.getListId(), (int)url.getCrawlSegmentId()));

          // update work unit stats ... 
          if (workUnitStatus != null) { 

            workUnitStatus.setUrlsComplete(workUnitStatus.getUrlsComplete() + 1);

            if (workUnitStatus.getUrlCount() == workUnitStatus.getUrlsComplete()) { 
              workUnitStatus.setCrawlStatus(CrawlSegmentStatus.CrawlStatus.CRAWL_COMPLETE);
            }
            workUnitStatus.setIsDirty(true);

            if (!getServer().externallyManageCrawlSegments()) { 
              getServer().updateCrawlSegmentStatus((int)url.getCrawlSegmentId(),workUnitStatus);
            }
          }
          else { 
            LOG.error("CrawlSegmentStatus for List:" + url.getListId() + " Segment:"+ url.getCrawlSegmentId() + "  is NULL (during CrawlComplete).");
          }
        }
      }
    }
    else { 
      // notify server (in case it delegates the call)
      getServer().crawlComplete(connection, url, optTargetObj,success);
    }

    // update stats 
    if (success) {
      _successURLCount++;
      synchronized(_crawlerStats) { 
        _crawlerStats.setUrlsSucceeded(_crawlerStats.getUrlsSucceeded() + 1);
        _crawlerStats.setUrlsProcessed(_crawlerStats.getUrlsProcessed() + 1);

        if ((url.getFlags() & CrawlURL.Flags.IsRedirected) != 0) { 
          switch (url.getOriginalResultCode()) { 
            case 301: _crawlerStats.setHttp301Count(_crawlerStats.getHttp301Count() + 1);break;
            case 302: _crawlerStats.setHttp302Count(_crawlerStats.getHttp302Count() + 1);break;
            default:  _crawlerStats.setHttp300Count(_crawlerStats.getHttp300Count() + 1);break;
          }

          if (optTargetObj != null) { 
            switch (optTargetObj.getRedirectCount()) { 
              case 1: _crawlerStats.setRedirectResultAfter1Hops(_crawlerStats.getRedirectResultAfter1Hops() + 1);break;
              case 2: _crawlerStats.setRedirectResultAfter2Hops(_crawlerStats.getRedirectResultAfter2Hops() + 1);break;
              case 3: _crawlerStats.setRedirectResultAfter3Hops(_crawlerStats.getRedirectResultAfter3Hops() + 1);break;
              default: _crawlerStats.setRedirectResultAfterGT3Hops(_crawlerStats.getRedirectResultAfterGT3Hops() + 1);break;
            }
          }
        }
        // ok now process the http result code ... 
        if (url.getResultCode() >= 200 && url.getResultCode() < 300) { 
          _crawlerStats.setHttp200Count(_crawlerStats.getHttp200Count() + 1);
        }
        else if (url.getResultCode() >= 300 && url.getResultCode() < 400) { 
          switch (url.getResultCode()) { 
            case 301: _crawlerStats.setHttp301Count(_crawlerStats.getHttp301Count() + 1);break;
            case 302: _crawlerStats.setHttp302Count(_crawlerStats.getHttp302Count() + 1);break;
            case 304: _crawlerStats.setHttp304Count(_crawlerStats.getHttp304Count() + 1);break;
            default:  _crawlerStats.setHttp300Count(_crawlerStats.getHttp300Count() + 1);break;
          }
        }
        else if (url.getResultCode() >= 400 && url.getResultCode() < 500) {
          switch (url.getResultCode()) { 
            case 403: _crawlerStats.setHttp403Count(_crawlerStats.getHttp403Count() + 1);break;
            case 404: _crawlerStats.setHttp404Count(_crawlerStats.getHttp404Count() + 1);break;
            default:  _crawlerStats.setHttp400Count(_crawlerStats.getHttp400Count() + 1);break;
          }
        }
        else if (url.getResultCode() >= 500 && url.getResultCode() < 600) {
          _crawlerStats.setHttp500Count(_crawlerStats.getHttp500Count() + 1);
        }
        else { 
          _crawlerStats.setHttpOtherCount(_crawlerStats.getHttpOtherCount() + 1);
        }
      }
      logSuccessfulGET(connection,url);

    }
    else { 
      _failedURLCount++;
      synchronized(_crawlerStats) {	    
        _crawlerStats.setUrlsFailed(_crawlerStats.getUrlsFailed() + 1);
        _crawlerStats.setUrlsProcessed(_crawlerStats.getUrlsProcessed() + 1);

        switch (url.getLastAttemptFailureReason()) { 

          case CrawlURL.FailureReason.UnknownProtocol: _crawlerStats.setHttpErrorUnknownProtocol(_crawlerStats.getHttpErrorUnknownProtocol() + 1);break;
          case CrawlURL.FailureReason.MalformedURL: _crawlerStats.setHttpErrorMalformedURL(_crawlerStats.getHttpErrorMalformedURL() + 1);break;
          case CrawlURL.FailureReason.Timeout: _crawlerStats.setHttpErrorTimeout(_crawlerStats.getHttpErrorTimeout() + 1);break;
          case CrawlURL.FailureReason.DNSFailure: _crawlerStats.setHttpErrorDNSFailure(_crawlerStats.getHttpErrorDNSFailure() + 1);break;
          case CrawlURL.FailureReason.ResolverFailure: _crawlerStats.setHttpErrorResolverFailure(_crawlerStats.getHttpErrorResolverFailure() + 1);break;
          case CrawlURL.FailureReason.IOException: _crawlerStats.setHttpErrorIOException(_crawlerStats.getHttpErrorIOException() + 1);break;
          case CrawlURL.FailureReason.RobotsExcluded: _crawlerStats.setHttpErrorRobotsExcluded(_crawlerStats.getHttpErrorRobotsExcluded() + 1);break;
          case CrawlURL.FailureReason.NoData: _crawlerStats.setHttpErrorNoData(_crawlerStats.getHttpErrorNoData() + 1);break;
          case CrawlURL.FailureReason.RobotsParseError: _crawlerStats.setHttpErrorRobotsParseError(_crawlerStats.getHttpErrorRobotsParseError() + 1);break;
          case CrawlURL.FailureReason.RedirectFailed: _crawlerStats.setHttpErrorRedirectFailed(_crawlerStats.getHttpErrorRedirectFailed() + 1);break;
          case CrawlURL.FailureReason.RuntimeError: _crawlerStats.setHttpErrorRuntimeError(_crawlerStats.getHttpErrorRuntimeError() + 1);break;
          case CrawlURL.FailureReason.ConnectTimeout: _crawlerStats.setHttpErrorConnectTimeout(_crawlerStats.getHttpErrorConnectTimeout() + 1);break;
          case CrawlURL.FailureReason.BlackListedHost: _crawlerStats.setHttpErrorBlackListedHost(_crawlerStats.getHttpErrorBlackListedHost() + 1);break;
          case CrawlURL.FailureReason.BlackListedURL: _crawlerStats.setHttpErrorBlackListedURL(_crawlerStats.getHttpErrorBlackListedURL() + 1);break;
          case CrawlURL.FailureReason.TooManyErrors: _crawlerStats.setHttpErrorTooManyErrors(_crawlerStats.getHttpErrorTooManyErrors() + 1);break;
          case CrawlURL.FailureReason.InCache: _crawlerStats.setHttpErrorInCache(_crawlerStats.getHttpErrorInCache() + 1);break;
          case CrawlURL.FailureReason.InvalidResponseCode: _crawlerStats.setHttpErrorInvalidResponseCode(_crawlerStats.getHttpErrorInvalidResponseCode() + 1);break;
          case CrawlURL.FailureReason.BadRedirectData: _crawlerStats.setHttpErrorBadRedirectData(_crawlerStats.getHttpErrorBadRedirectData() + 1);break;
          default: _crawlerStats.setHttpErrorUNKNOWN(_crawlerStats.getHttpErrorUNKNOWN() + 1); 
        }

      }
    }

    if (getServer().enableCrawlLog()) { 
      // either way decrement queued count ...
      incDecPendingQueuedURLCount(0,-1);

      // now potentially load any deferred segments ... 
      potentiallyLoadNextSegment();
    }
  }

  /** callback used by crawl log to notify engine of a segment that should be marked as completed ... **/
  void crawlSegmentComplete(long packedSegmentId) { 
    CrawlSegmentStatus segmentStatus = _statusMap.get(packedSegmentId);
    if (segmentStatus != null) { 
      // mark the segment as completed ... 
      segmentStatus.setIsComplete(true);
    }
  }

  /*
  void purgeCrawlSegments(CrawlSegmentList segmentList) {

    for (long packedSegmentId : segmentList.getSegments()) { 

      CrawlSegmentStatus status = _statusMap.get(packedSegmentId);

      if (status != null && status.getIsComplete() == true) { 
        CrawlSegmentLog segmentLog = _crawlLog.getLogForSegment(CrawlLog.getListIdFromLogId(packedSegmentId),CrawlLog.getSegmentIdFromLogId(packedSegmentId)); 
        if (segmentLog == null || segmentLog.isSegmentComplete()) {

          // remove from map ... 
          _statusMap.remove(packedSegmentId);
          // remove from log ... 
          _crawlLog.removeSegmentLog(CrawlLog.getListIdFromLogId(packedSegmentId),CrawlLog.getSegmentIdFromLogId(packedSegmentId));

          try { 
            // remove from database 
            _recordStore.beginTransaction();
            _recordStore.deleteRecordByKey(CrawlSegmentKeyPrefix + buildCrawlSegmentKey(status.getListId(),status.getSegmentId()));
            _recordStore.commitTransaction();
          }
          catch (RecordStoreException e) { 
            LOG.error("purge of crawl segment record with list id:"+ status.getListId() + "segment id:" + status.getSegmentId() + "  threw Exception:" + CCStringUtils.stringifyException(e));
          }
        }
      }
    }
  }
   */

  public void dumpQueueDetailsToHTML(JspWriter out)throws IOException { 
    _httpCrawlQueue.dumpDetailsToHTML(out);
  }

  public void dumpHostDetailsToHTML(JspWriter out, int hostIP)throws IOException { 
    _httpCrawlQueue.dumpHostDetailsToHTML(out, hostIP);
  }

  public void pauseFetch() { 
    LOG.info("PAUSING CRAWL");
    _httpCrawlQueue.pauseCrawl();
  }

  public void resumeFetch() { 
    LOG.info("RESUMING CRAWL");
    _httpCrawlQueue.resumeCrawl();
  }

  public void logDNSFailure(String hostName, String errorDescription) {
    synchronized(_DNSFailureLog) { 
      _DNSFailureLog.error(hostName + "," + errorDescription);
    }
  }

  public void logDNSQuery(String hostName, InetAddress address, long ttl,String opCName) {
    synchronized(_DNSSuccessLog) { 
      _DNSSuccessLog.info(hostName + "," + address.toString() + "," + ttl  + "," + opCName);
    }
  }


  /** black list the given host name **/
  public void failDomain(String domainName) { 
    _badDomainCache.cacheIPAddressForHost(domainName, 0,0, null);
  }

  public boolean isBadDomain(String domainName) { 
    return _badDomainCache.findNode(domainName) != null;
  }

  public void queueExternalURL(final String url,final long fingerprint,final boolean highPriorityRequest,final CrawlItemStatusCallback callback) { 
    // validate the url ... 
    String hostName = URLUtils.fastGetHostFromURL(url);

    if (hostName == null) {
      if (callback != null) { 
        callback.crawlComplete(null,CrawlTarget.allocateCrawlURLForFailure(url,fingerprint,CrawlURL.FailureReason.MalformedURL,"URL Rejected - Bad SessionID URL"),null,false);
      }
      else {
        if (Environment.detailLogEnabled())
          LOG.error("queueExternalURL for URL:" + url + " Failed with:URL Rejected - Bad SessionID URL");
      }
    }
    else { 

      // schedule resolution ... 
      NIODNSQueryClient queryClient = new NIODNSQueryClient() {

        @Override
        public void AddressResolutionFailure(NIODNSResolver source,String hostName, Status status, String errorDesc) {
          // LOG.info("DNS Failed for High Priority Request:" + hostName + " Errror:" + errorDesc);
          if (callback != null) { 
            callback.crawlComplete(null,CrawlTarget.allocateCrawlURLForFailure(url,fingerprint,CrawlURL.FailureReason.DNSFailure,errorDesc),null,false);
          }
          else { 
            if (Environment.detailLogEnabled())
              LOG.error("queueExternalURL for URL:" + url + " Failed with:DNS Failed for High Priority Request:" + hostName + " Errror:" + errorDesc);
          }
        }
        @Override
        public void AddressResolutionSuccess(NIODNSResolver source,String hostName, String name, InetAddress address, long addressTTL) {
          int hostAddress = 0;
          if (address != null && address.getAddress() != null) {
            byte[] addr = address.getAddress();
            if (addr.length == 4) {
              hostAddress  = IPAddressUtils.IPV4AddressToInteger(addr);
            }
          }

          if (hostAddress != 0) {
            // LOG.info("DNS Success for High Priority URL:" + url + "IP:" + address.toString());
            _httpCrawlQueue.queueExternalURLRequest(url, getServer().getHighPriorityListId(), fingerprint, hostName, hostAddress, addressTTL + 30000000,highPriorityRequest, callback);
          }
          else { 
            // LOG.error("DNS Failed for High Priority URL:"+ url + " with Zero IP");
            if (callback != null) { 
              callback.crawlComplete(null,CrawlTarget.allocateCrawlURLForFailure(url,fingerprint,CrawlURL.FailureReason.DNSFailure,"Invalid IP Address"),null,false);
            }
            else { 
              if (Environment.detailLogEnabled())
                LOG.error("queueExternalURL for URL:" + url + " Failed with:DNS Failed for High Priority URL:"+ url + " with Zero IP");
            }

          }
        }
        @Override
        public void DNSResultsAvailable() {}

        @Override
        public void done(NIODNSResolver source,FutureTask<DNSQueryResult> task) {
        }
      };
      try {
        getServer().getDNSServiceResolver().resolve(queryClient, hostName,false, true,DEFAULT_DNS_TIMEOUT);
      } catch (IOException e) {
        // LOG.error("Failed to Dispatch DNS Query for High Priority URL:" + url + " Exception:" + CCStringUtils.stringifyException(e));
        if (callback != null) { 
          callback.crawlComplete(null,CrawlTarget.allocateCrawlURLForFailure(url,fingerprint,CrawlURL.FailureReason.ResolverFailure,CCStringUtils.stringifyException(e)),null,false);
        }
        else { 
          if (Environment.detailLogEnabled())
            LOG.error("queueExternalURL for URL:" + url + " Failed with:Exception:" + CCStringUtils.stringifyException(e));
        }
      }
    }
  }

  public void queueExternalCrawlSegmentHost(CrawlSegmentHost host,CrawlItemStatusCallback callback){
    processHostIPResolutionResult(host,false,callback);
  }

  public enum RobotsLogEventType { 
    HTTP_GET_Complete,
    HTTP_GET_Failed,
    Parse_Succeeded,
    Parse_Failed
  }

  public static final int RobotsParseFlag_ExcludesAll = 1 << 0;
  public static final int RobotsParseFlag_ExplicitMention = 1 << 1;
  public static final int RobotsParseFlag_HasCrawlDelay = 1 << 2;
  public static final int RobotsParseFlag_ContentDecodeFailed = 1 << 3;
  public static final int RobotsParseFlag_ContentWasHTML = 1 << 3;

  /** log robots fetch **/
  public synchronized void logRobots(long fetchTime,String domain,int httpResultCode,String robotsData,RobotsLogEventType eventType, int flags) {
    StringBuffer sb = new StringBuffer(2048);
    sb.append(String.format("%1$24.24s ",robotsLogDateFormat.format(new Date(fetchTime))));
    sb.append(String.format("%1$40.40s ",domain));
    switch (eventType) { 
      case HTTP_GET_Complete: { 
        sb.append(String.format("%1$12.12s ","GET_COMPLETE"));
      }
      break;
      case HTTP_GET_Failed: { 
        sb.append(String.format("%1$12.12s ","GET_FAILURE"));
      }
      break;
      case Parse_Succeeded: { 
        sb.append(String.format("%1$12.12s ","PARSE_SUCCESS"));
      }
      break;
      case Parse_Failed: { 
        sb.append(String.format("%1$12.12s ","PARSE_FAILED"));
      }
      break;
    }
    sb.append(String.format("%1$4.4s ",httpResultCode));

    if (eventType == RobotsLogEventType.HTTP_GET_Complete) { 
      if (robotsData != null && robotsData.length() != 0) {
        sb.append("\n****CONTENT-START****\n");
        sb.append(robotsData,0,Math.min(robotsData.length(),8192));
        sb.append("\n****CONTENT-END ****\n");
      }
      else if ((flags & RobotsParseFlag_ContentDecodeFailed) != 0) { 
        sb.append(" ContentDecodeFailed");
      }
      else if ((flags & RobotsParseFlag_ContentWasHTML) != 0) { 
        sb.append(" ContentWasHTML");
      }

    }
    else if (eventType == RobotsLogEventType.Parse_Succeeded) { 
      if ((flags & RobotsParseFlag_ExcludesAll) != 0) {
        sb.append("ExcludesAll ");
      }
      if ((flags & RobotsParseFlag_ExplicitMention) != 0) { 
        sb.append("ExplicitMention ");
      }
      if ((flags & RobotsParseFlag_HasCrawlDelay) != 0) { 
        sb.append("HasCrawlDelay ");
      }
    }
    _RobotsLog.error(sb.toString());
  }

  FlexBuffer getActiveHostListAsBuffer() throws IOException { 
    if (_crawlActive && _httpCrawlQueue != null) {

      DataOutputBuffer outputBuffer = new DataOutputBuffer();

      Set<Integer> ipAddressSet = _httpCrawlQueue.getActiveHostIPs();

      WritableUtils.writeVInt(outputBuffer, ipAddressSet.size());

      for (int hostIP : ipAddressSet) { 
        WritableUtils.writeVInt(outputBuffer, hostIP);
      }

      return new FlexBuffer(outputBuffer.getData(),0,outputBuffer.getLength());
    }
    return null;
  }

}