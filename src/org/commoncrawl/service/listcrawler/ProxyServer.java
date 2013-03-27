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
package org.commoncrawl.service.listcrawler;


import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.spi.LoggingEvent;
import org.commoncrawl.async.Timer;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.db.RecordStore;
import org.commoncrawl.io.DNSQueryResult;
import org.commoncrawl.io.NIODNSQueryClient;
import org.commoncrawl.io.NIODNSResolver;
import org.commoncrawl.io.NIOHttpConnection;
import org.commoncrawl.protocol.CrawlSegmentHost;
import org.commoncrawl.protocol.CrawlSegmentURL;
import org.commoncrawl.protocol.CrawlURL;
import org.commoncrawl.protocol.URLFP;
import org.commoncrawl.rpc.base.internal.AsyncClientChannel;
import org.commoncrawl.server.ServletLauncher;
import org.commoncrawl.service.crawler.CrawlTarget;
import org.commoncrawl.service.crawler.CrawlerServer;
import org.commoncrawl.service.crawler.filters.URLPatternBlockFilter;
import org.commoncrawl.service.crawler.filters.Filter.FilterResult;
import org.commoncrawl.service.queryserver.QueryServerMaster;
import org.commoncrawl.util.URLFingerprint;
import org.commoncrawl.util.URLUtils;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.CustomLogger;
import org.commoncrawl.util.IPAddressUtils;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.servlet.ServletHolder;

import com.ibm.icu.text.SimpleDateFormat;

/** 
 * An advanced version of the basic crawler that maintains a long term history and cache of 
 * crawled content and that also supports lists based crawling 
 * 
 * @author rana
 *
 */
public class ProxyServer extends CrawlerServer implements CrawlQueueLoader {

  private static final int            MAX_QUEUED_DNS_REQUESTS = 1000;
  static ProxyServer                  _server                 = null;

  private CacheManager                _cache;
  private CrawlHistoryManager         _crawlHistoryManager;
  private int                         _cacheFlushThreshold    = -1;
  private CustomLogger                _requestLog;
  private InetSocketAddress           _queryMasterAddress;
  private AsyncClientChannel          _queryMasterChannel;
  private QueryServerMaster.AsyncStub _queryMasterStub;
  private boolean                     _queryMasterAvailable   = false;
  private URLPatternBlockFilter       _urlBlockFilter         = null;
  private int                         _debugMode              = 0;
  private File                        _crawlHistoryLogDir     = null;
  private RecordStore                 _recordStore = new RecordStore();

  public ProxyServer() {

  }

  public CacheManager getCache() {
    return _cache;
  }

  /**
   * 
   * @return crawl history manager
   */
  public CrawlHistoryManager getCrawlHistoryManager() {
    return _crawlHistoryManager;
  }

  /**
   * get reference to the singleton server instance
   * 
   */
  public static ProxyServer getSingleton() {
    return _server;
  }

  /**
   * 
   * @return
   */
  public File getCrawlHistoryDataDir() {
    return _crawlHistoryLogDir;
  }

  /**
   * get the request log file name
   * 
   */
  public static String getRequestLogFileName() {
    return "requestLog.log";
  }

  /**
   * get at the async stub for the query master service
   * 
   */
  public QueryServerMaster.AsyncStub getQueryMasterStub() {
    return _queryMasterStub;
  }

  /**
   * get the connected / disconnected status of the query master service
   * connection
   * 
   */
  public boolean isConnectedToQueryMaster() {
    return _queryMasterAvailable;
  }

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
    return CrawlEnvironment.PROXY_SERVICE_HTTP_PORT;
  }

  @Override
  protected String getDefaultLogFileName() {
    return "proxyServer.log";
  }

  @Override
  protected String getDefaultRPCInterface() {
    return CrawlEnvironment.DEFAULT_RPC_INTERFACE;
  }

  @Override
  protected int getDefaultRPCPort() {
    return CrawlEnvironment.PROXY_SERVICE_RPC_PORT;
  }

  @Override
  protected String getWebAppName() {
    return "proxy";
  }

  SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY.MM.dd-HH:mm:ss.SSS");

  synchronized void logProxyFailure(int httpResultCode, String failureDesc,
      String originalURL, String finalURL, long startTime) {
    StringBuffer sb = new StringBuffer(2048);
    sb.append(String.format("%1$24.24s ", dateFormat
        .format(new Date(startTime))));
    sb.append(String.format("%1$8.8s ",
        (System.currentTimeMillis() - startTime)));
    sb.append(String.format("%1$4.4s ", httpResultCode));
    sb.append(String.format("%1$40.40s ", failureDesc));
    sb.append(originalURL);
    sb.append(" ");
    sb.append(finalURL);

    _requestLog.error(sb.toString());
  }

  synchronized void logProxySuccess(int httpResultCode, String origin,
      String originalURL, String finalURL, long startTime) {

    StringBuffer sb = new StringBuffer(2048);

    sb.append(String.format("%1$24.24s ", dateFormat
        .format(new Date(startTime))));
    sb.append(String.format("%1$8.8s ",
        (System.currentTimeMillis() - startTime)));
    sb.append(String.format("%1$4.4s ", httpResultCode));
    sb.append(String.format("%1$10.10s ", origin));
    sb.append(originalURL);
    sb.append(" ");
    sb.append(finalURL);

    _requestLog.error(sb.toString());
  }

  private static class CustomLoggerLayout extends Layout {

    StringBuffer sbuf = new StringBuffer(1024);

    @Override
    public String format(LoggingEvent event) {
      sbuf.setLength(0);
      sbuf.append(event.getMessage());
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

  @Override
  protected boolean initServer() {
    _server = this;
    if (super.initServer()) {
      try {
        
        // get database path ... 
        File databasePath = new File(getDataDirectory().getAbsolutePath() + "/" + CrawlEnvironment.PROXY_SERVICE_DB);
        LOG.info("Config says Proxy db path is: "+databasePath);
        // initialize record store
        _recordStore.initialize(databasePath, null);
        
        _requestLog = new CustomLogger("RequestLog");
        LOG.info("Initializing Proxy Request Log");
        _requestLog.addAppender(new DailyRollingFileAppender(
            new CustomLoggerLayout(), _server.getLogDirectory()
                + "/requestLog.log", "yyyy-MM-dd"));
        LOG.info("Constructing CacheManager. HDFSPath:"
            + CrawlEnvironment.getDefaultFileSystem() + " LocalDataPath:"
            + getDataDirectory());
        _cache = new CacheManager(CrawlEnvironment.getDefaultFileSystem(),
            getDataDirectory(), getEventLoop());

        LOG.info("Initializing CacheManager");
        if (_cacheFlushThreshold != -1) {
          _cache.setCacheFlushThreshold(_cacheFlushThreshold);
        }
        int cacheManagerInitFlags = 0;
        if (_debugMode == 1) {
          cacheManagerInitFlags |= CacheManager.INIT_FLAG_SKIP_HDFS_WRITER_INIT
              | CacheManager.INIT_FLAG_SKIP_INDEX_LOAD;
        }
        _cache.initialize(cacheManagerInitFlags);

        LOG.info("Initializing History Manager");
        _crawlHistoryLogDir = new File(getDataDirectory(), "historyData");
        _crawlHistoryLogDir.mkdir();

        // default to no init flags for history manager
        int historyManagerFlags = 0;

        // but if in debug mode, disable a whole bunch of things (for now)
        if (_debugMode == 1) {
          historyManagerFlags = CrawlHistoryManager.INIT_FLAG_SKIP_LOG_WRITER_THREAD_INIT;
        }

        _crawlHistoryManager = new CrawlHistoryManager(CrawlEnvironment
            .getDefaultFileSystem(), new Path("crawl/proxy/history"),
            _crawlHistoryLogDir, getEventLoop(), historyManagerFlags);
        // start queueing thread

        LOG.info("Starting Communications with Query Master At:"
            + _queryMasterAddress);
        _queryMasterChannel = new AsyncClientChannel(_eventLoop,
            new InetSocketAddress(0), _queryMasterAddress, this);
        _queryMasterChannel.open();
        _queryMasterStub = new QueryServerMaster.AsyncStub(_queryMasterChannel);

      } catch (IOException e) {
        LOG.error("Failed to Initialize CacheManager. Exception:"
            + CCStringUtils.stringifyException(e));
      }

      /** init jersey framework **/
      String classesRoot = System.getProperty("commoncrawl.classes.root");
      LOG.info("Classes Root is:" + classesRoot);
      try {
        ArrayList<URL> urls = new ArrayList<URL>();
        urls.add(new File(classesRoot).toURL());
        LOG.info("URL is:" + urls.get(0).toString());
        URLClassLoader loader = new URLClassLoader(urls.toArray(new URL[0]),
            Thread.currentThread().getContextClassLoader());
        _webServer.getWebAppContext().setClassLoader(loader);
      } catch (MalformedURLException e) {
        LOG.error(CCStringUtils.stringifyException(e));
      }
      ServletHolder holder = _webServer.addServlet(null, "/*",
          ServletLauncher.class);

      holder.setInitParameter(ServletLauncher.SERVLET_REGISTRY_KEY,
          ProxyServletRegistry.class.getCanonicalName());
      // holder.setInitParameter(ServletContainer.APPLICATION_CONFIG_CLASS,
      // "org.commoncrawl.crawl.proxy.ProxyServerUIApp");
      // holder.setInitParameter("x-hack-nocache","true");
      // holder.setInitParameter(ServletContainer.RESOURCE_CONFIG_CLASS,"com.sun.jersey.api.core.ScanningResourceConfig");

      getWebServer().setThreads(20, 175, 1);
      // add list uploader filter
      getWebServer().getWebAppContext().addFilter(MultiPartFilter.class,
          "/ListUploader", Handler.ALL);

      getWebServer().addServlet("proxyRequest", "/proxy", ProxyServlet.class);
      // getWebServer().addServlet("testProxyRequest", "*",
      // ProxyServlet2.class);
      getWebServer().addServlet("logRequest", RequestLogServlet.servletPath,
          RequestLogServlet.class);
      // add uploader servlet
      getWebServer().addServlet("uploader", "/ListUploader",
          ListUploadServlet.class);
      getWebServer().addServlet("uploader", "/ListUploaderDirect",
          ListUploadServlet.class);
      // add upload form
      getWebServer().addServlet("uploadForm", "/ListUploadForm",
          ListUploadServlet.ListUploadForm.class);
      // view lists form
      getWebServer().addServlet("viewLists", "/CrawlLists",
          CrawlListsServlet.class);
      // hack
      getWebServer().addServlet("requeueList", "/Requeue",
          ListUploadServlet.ListRequeueServlet.class);
      getWebServer().addServlet("requeueBrokenLists", "/RequeueBrokenLists",
          ListUploadServlet.RequeueBrokenListsServlet.class);


      // add doc uploader filter and servlet
      //getWebServer().getWebAppContext().addFilter(
      //    DocUploadMultiPartFilter.class, "/DocUploader", Handler.ALL);
      //getWebServer().addServlet("docuploader-check", "/DocInCache",
      //    DocUploadServlet.DocInCacheCheck.class);

      // disable list loader if in debug mode

      if (_debugMode == 1) {
        LOG.warn("List Loader Disabled in Debug Mode");
      } else {

        // ok do a delayed list loader initialization
        getEventLoop().setTimer(new Timer(10000, false, new Timer.Callback() {

          @Override
          public void timerFired(Timer timer) {
            initListLoader();
          }
        }));

      }

    }
    return true;
  }

  /**
   * get database name for this instance
   * 
   */
  @Override
  public String getDatabaseName() {
    return CrawlEnvironment.PROXY_SERVICE_DB;
  }

  /**
   * enable the crawl log (yes by default)
   * 
   */
  @Override
  public boolean enableCrawlLog() {
    return false;
  }

  /**
   * externally manage crawl segments
   * 
   */
  @Override
  public boolean externallyManageCrawlSegments() {
    return true;
  }

  /**
   * crawl completed for the specified crawl target
   * 
   */
  @Override
  public void crawlComplete(NIOHttpConnection connection, CrawlURL url,
      CrawlTarget optTargetObj, boolean successOrFailure) {

    // log it into the history log
    _crawlHistoryManager.crawlComplete(url);
    // cache if necessary
    if (url.getLastAttemptResult() == CrawlURL.CrawlResult.SUCCESS
        && optTargetObj != null && optTargetObj.getCompletionCallback() != null) {
      _server.logProxySuccess(url.getResultCode(), "origin", url.getUrl(), url
          .getRedirectURL(), optTargetObj.getRequestStartTime());
    } else {
      if (optTargetObj != null && optTargetObj.getCompletionCallback() != null) {
        // if (url.getLastAttemptFailureReason() !=
        // CrawlURL.FailureReason.RobotsExcluded &&
        // url.getLastAttemptFailureReason() !=
        // CrawlURL.FailureReason.BlackListedURL) {
        _server.logProxyFailure(url.getResultCode(), CrawlURL.FailureReason
            .toString(url.getLastAttemptFailureReason())
            + " - " + url.getLastAttemptFailureReason(), url.getUrl(), url
            .getRedirectURL(), (optTargetObj != null && optTargetObj
            .getRequestStartTime() != -1) ? optTargetObj.getRequestStartTime()
            : System.currentTimeMillis());
        // }
      }
    }

    if (optTargetObj != null && optTargetObj.getCompletionCallback() != null) {
      // delegate to callback
      optTargetObj.getCompletionCallback().crawlComplete(connection, url,
          optTargetObj, successOrFailure);
    } else {
      // completion callback is null... we need to handle the caching of this
      // object direct
      if (successOrFailure
          && url.getLastAttemptResult() == CrawlURL.CrawlResult.SUCCESS) {
        // LOG.info("### CACHING Calling cacheCrawlURL for URL:" +
        // url.getUrl());
        ProxyServlet.cacheCrawlURLResult(url, null);
      } else {
        // LOG.info("### CACHING Skipping Write of crawlURL:" + url.getUrl() +
        // "SuccessOrFailFlag:" + successOrFailure + " LastAttemptResult:" +
        // url.getLastAttemptResult());
      }
    }
  }

  /**
   * Inject an externally populated crawl url into the proxy server's queues
   * 
   * @param crawlURL
   */
  public void injectCrawlURL(final CrawlURL crawlURL,
      final Semaphore completionSemaphore) {
    getEventLoop().setTimer(new Timer(0, false, new Timer.Callback() {

      @Override
      public void timerFired(Timer timer) {

        LOG.info("Received Injected URL:" + crawlURL.getUrl());
        // log it into the history log
        _crawlHistoryManager.crawlComplete(crawlURL);
        // log it
        _server.logProxySuccess(crawlURL.getResultCode(), "injection", crawlURL
            .getUrl(), crawlURL.getRedirectURL(), 0);
        // and cache it
        ProxyServlet.cacheCrawlURLResult(crawlURL, completionSemaphore);
      }

    }));
  }

  /**
   * notification that a fetch is starting on the target url
   * 
   */
  @Override
  public void fetchStarting(CrawlTarget target, NIOHttpConnection connection) {

  }

  @Override
  protected boolean parseArguments(String[] argv) {
    if (super.parseArguments(argv)) {
      for (int i = 0; i < argv.length; ++i) {
        if (argv[i].equalsIgnoreCase("--querymaster")) {
          if (i + 1 < argv.length) {
            _queryMasterAddress = new InetSocketAddress(argv[++i],
                CrawlEnvironment.DEFAULT_QUERY_MASTER_RPC_PORT);
          }
        } else if (argv[i].equalsIgnoreCase("--cacheFlushThreshold")) {
          if (i + 1 < argv.length) {
            _cacheFlushThreshold = Integer.parseInt(argv[++i]);
          }
        } else if (argv[i].equalsIgnoreCase("--debugMode")) {
          if (i + 1 < argv.length) {
            _debugMode = Integer.parseInt(argv[++i]);
          }
        }

      }
      return (_queryMasterAddress != null);
    } else {
      return false;
    }
  }

  @Override
  public void OutgoingChannelConnected(AsyncClientChannel channel) {
    if (channel == _queryMasterChannel) {
      LOG.info("Connected to QueryMaster Server");
      _queryMasterAvailable = true;
    } else {
      super.OutgoingChannelConnected(channel);
    }
  }

  @Override
  public boolean OutgoingChannelDisconnected(AsyncClientChannel channel) {
    if (channel == _queryMasterChannel) {
      // LOG.info("QueryMaster Server Disconnected");
      _queryMasterAvailable = false;
      return false;
    } else {
      return super.OutgoingChannelDisconnected(channel);
    }
  }

  private static final int MAX_TARGETS_PER_ITERATION = 100;
  static final int         DEFAULT_DNS_TIMEOUT       = 30000;

  /**
   * should we use black lists
   * 
   */
  @Override
  public boolean useGlobalBlockLists() {
    return false;
  }

  /**
   * check host stats for failures
   * 
   */
  public boolean failHostsOnStats() {
    return true;
  }

  /** reload custom filters on directory service change **/
  @Override
  protected void reloadFilters() {
    // load crawler's filters ...
    super.reloadFilters();
    // and load our custom filters ..
    _urlBlockFilter = new URLPatternBlockFilter();

    try {
      LOG.info("### Loading URL Block Filter");
      _urlBlockFilter.loadFromPath(getDirectoryServiceAddress(),
          CrawlEnvironment.PROXY_URL_BLOCK_LIST_FILTER_PATH, false);
      LOG.info("### IP Address Block Filter ");
    } catch (IOException e) {
      LOG.error(CCStringUtils.stringifyException(e));
    }
  }

  /**
   * 
   * @return the path to the crawl rate override filter
   */
  @Override
  public String getCrawlRateOverrideFilterPath() {
    return CrawlEnvironment.PROXY_CRAWL_RATE_MOD_FILTER_PATH;
  }
  

  /** is url in server block list **/
  @Override
  public boolean isURLInBlockList(URL url) {
    String rootDomainName = URLUtils.extractRootDomainName(url.getHost());
    if (rootDomainName != null) {
      return _urlBlockFilter.filterItem(rootDomainName, url.getHost(), url
          .getPath(), null, null) == FilterResult.Filter_Reject;
    }
    LOG.warn("Invalid Domain passed to isURLInBlockList via URL:"
        + url.toString());
    return true;
  }

  @Override
  public int getMaxRobotsExlusionsInLoopOverride() {
    return 20;
  }

  /**
   * get the host idle flush threshold
   * 
   * the number of milliseconds a host needs to be idle for it to be purged from
   * memory
   * **/
  @Override
  public int getHostIdleFlushThreshold() {
    return 120000;
  }

  /**
   * disable cycle timer
   * 
   */
  @Override
  public boolean disableCycleTimer() {
    return true;
  }

  /************************************************************************/
  // List Loader Support Routines
  /************************************************************************/

  private CrawlSegmentHost _activeLoadHost       = null;
  private long             _loaderLastUpdateTime = -1;

  private Semaphore        _loaderDNSSemaphore   = new Semaphore(
                                                     MAX_QUEUED_DNS_REQUESTS);
  private Semaphore        _loaderQueueSemaphore = new Semaphore(1);
  private Thread           _loaderQueuePollThread;
  private boolean          _shutdownPollThread   = false;

  void initListLoader() {

    _loaderQueuePollThread = new Thread(new Runnable() {

      @Override
      public void run() {
        while (!_shutdownPollThread) {
          try {
            CrawlSegmentHostQueueItem queueItem = _loaderQueue.take();
            if (queueItem._host != null) { 
              dispatchHost(queueItem._host);
            }
            else { 
              return;
            }
          } catch (InterruptedException e1) {
          }
        }
      }
    });
    _loaderQueuePollThread.start();
    _crawlHistoryManager.startQueueLoaderThread(this);
  }

  void shutdownListLoader() {
    _shutdownPollThread = true;
    try {
      _loaderQueue.put(new CrawlSegmentHostQueueItem());
    } catch (InterruptedException e1) {
    }
    try {
      _loaderQueuePollThread.join();
    } catch (InterruptedException e) {
    }
    _loaderQueuePollThread = null;
    _shutdownPollThread = false;
    _crawlHistoryManager.stopQueueLoaderThread();
  }

  public static class CrawlSegmentHostQueueItem {
    
    public CrawlSegmentHostQueueItem(CrawlSegmentHost host) { 
      _host = host;
    }
    public CrawlSegmentHostQueueItem() { 
      _host = null;
    }
    CrawlSegmentHost _host;
  }
  static final int MAX_QUEUED_HOSTS = 40000;
  LinkedBlockingQueue<CrawlSegmentHostQueueItem> _loaderQueue = new LinkedBlockingQueue<CrawlSegmentHostQueueItem>(MAX_QUEUED_HOSTS);
  
  @Override
  public void queueURL(URLFP urlfp, String url) {

    _loaderLastUpdateTime = System.currentTimeMillis();

    // LOG.info("Received QueueURL Request for URL:" + url);

    String hostName = URLUtils.fastGetHostFromURL(url);

    if (hostName == null) { 
      LOG.error("###queueURL failed for url:" + url + " with null HostName!");
      return;
    }
    
    if (hostName.length() != 0) {

      CrawlSegmentHost dispatchHost = null;

      try {
        _loaderQueueSemaphore.acquireUninterruptibly();

        // ok is there an active host ...
        if (_activeLoadHost != null
            && !_activeLoadHost.getHostName().equals(hostName)) {
          // ok time to dispatch this guy immediately
          dispatchHost = _activeLoadHost;
          _activeLoadHost = null;
        }
        if (_activeLoadHost == null) {
          _activeLoadHost = new CrawlSegmentHost();
          _activeLoadHost.setHostName(hostName);
          _activeLoadHost.setHostFP(URLFingerprint
              .generate64BitURLFPrint(hostName));
          _activeLoadHost.setSegmentId(-1);
          _activeLoadHost.setListId(ProxyServer.getServer()
              .getHighPriorityListId());
        }

        // queue the url
        CrawlSegmentURL urlObject = new CrawlSegmentURL();
        urlObject.setUrl(url);
        urlObject.setUrlFP(urlfp.getUrlHash());
        _activeLoadHost.getUrlTargets().add(urlObject);

        // if target count exceeds threshold ...
        if (_activeLoadHost.getUrlTargets().size() >= 1000) {
          dispatchHost = _activeLoadHost;
          _activeLoadHost = null;
        }
      } finally {
        _loaderQueueSemaphore.release();
      }

      if (dispatchHost != null) {
        try {
          _loaderQueue.put(new CrawlSegmentHostQueueItem(dispatchHost));
        } catch (InterruptedException e) {
        }
      }
    }
  }
  
  @Override
  public void flush() {
    CrawlSegmentHost dispatchHost = null;
    
    _loaderQueueSemaphore.acquireUninterruptibly();
    try {
      dispatchHost = _activeLoadHost;
      _activeLoadHost = dispatchHost; 
    }
    finally { 
      _loaderQueueSemaphore.release();
    }
    if (dispatchHost != null) { 
      try {
        _loaderQueue.put(new CrawlSegmentHostQueueItem(dispatchHost));
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

  private void dispatchHost(final CrawlSegmentHost host) {

    // LOG.info("Dispatch Host Called for Host:" + host.getHostName());
    // acquire loader semaphore ...
    try {
      while (!_loaderDNSSemaphore.tryAcquire(100, TimeUnit.MILLISECONDS)) {
        LOG.info("###URLLoader Waiting on DNS Queue");
      }
    } catch (InterruptedException e1) {
      LOG.error(CCStringUtils.stringifyException(e1));
    }

    host.setIpAddress(0);

    // schedule resolution ...
    NIODNSQueryClient queryClient = new NIODNSQueryClient() {

      @Override
      public void AddressResolutionFailure(NIODNSResolver source,String hostName, Status status, String errorDesc) {
        LOG.info("queueExternalURL for Failed with:DNS Failed for High Priority Request:"
                + hostName + " Errror:" + errorDesc);
        
        _loaderDNSSemaphore.release();
        // fail the urls ... 
        getEngine().processHostIPResolutionResult(host,true,null);
      }

      @Override
      public void AddressResolutionSuccess(NIODNSResolver source,
          String hostName, String name, InetAddress address, long addressTTL) {
        int hostAddress = 0;
        if (address != null && address.getAddress() != null) {
          byte[] addr = address.getAddress();
          if (addr.length == 4) {
            hostAddress = IPAddressUtils.IPV4AddressToInteger(addr);
          }
        }

        if (hostAddress != 0) {
          // LOG.info("DNS Success for Host:" + hostName + "Queueing");
          // set the address into the host object
          host.setIpAddress(hostAddress);
          host.setTtl(addressTTL + 30000000);

          queueExternalHost(host, null);

        } else {
          // LOG.error("DNS Failed for High Priority URL:"+ url +
          // " with Zero IP");
          LOG.info("queueExternalURL Failed with:DNS Failed for Host:"
              + host.getHostName() + " with Zero IP");
        }
        _loaderDNSSemaphore.release();
      }

      @Override
      public void DNSResultsAvailable() {
      }

      @Override
      public void done(NIODNSResolver source, FutureTask<DNSQueryResult> task) {
      }
    };

    try {

      getServer().getDNSServiceResolver().resolve(queryClient,
          host.getHostName(), false, true, DEFAULT_DNS_TIMEOUT);

    } catch (IOException e) {
      // LOG.error("Failed to Dispatch DNS Query for High Priority URL:" + url +
      // " Exception:" + CCStringUtils.stringifyException(e));
      LOG.info("queueExternalURL for Host:" + host.getHostName()
          + " Failed with:Exception:" + CCStringUtils.stringifyException(e));
      _loaderDNSSemaphore.release();
    }
  }

  @Override
  public void stop() {
    LOG.info("ProxyServer Stop Called");
    if (_crawlHistoryManager != null) {
      LOG.info("Shutting Down CrawlHistory List Loader ");
      shutdownListLoader();
      LOG.info("Shutting Down CrawlHistoryManager");
      _crawlHistoryManager.shutdown();
    }

    if (_cache != null) {
      LOG.info("Shutting Down CacheManager");
      _cache.shutdown();
    }
    LOG.info("ProxyServer Callign Super Stop");
    super.stop();
  }

  private static final String CRAWL_LIST_RECORD_PARENT_ID = "CRAWL_LIST_RECORD_TYPE";
  private static final String CRAWL_LIST_RECORD_PREFIX    = "CrawlList_";

  void requeueList(final long originalListId, final File listDataFile) {
    final Semaphore blockingSemaphore = new Semaphore(0);

    getEventLoop().setTimer(new Timer(0, false, new Timer.Callback() {

      @Override
      public void timerFired(Timer timer) {
        try {
          // get the existing database record
          CrawlListDatabaseRecord record = (CrawlListDatabaseRecord) _recordStore
              .getRecordByKey(CRAWL_LIST_RECORD_PREFIX + originalListId);

          if (record != null) {
            LOG.info("### Found Record for ListId:" + originalListId);

            // delete record

            // tell history manager to load list ...
            LOG.info("### Reloading List");
            long newListId = _crawlHistoryManager.loadList(listDataFile,record.getRefreshInterval());
            LOG.info("### Reloaded List Id is:" + newListId);

            // update list id
            record.setListId(newListId);
            // update list filename
            record.setTempFileName(listDataFile.getName());

            LOG.info("### Upading Database Record");
            _recordStore.beginTransaction();
            _recordStore.deleteRecordById(record.getRecordId());
            _recordStore.insertRecord(CRAWL_LIST_RECORD_PARENT_ID,
                CRAWL_LIST_RECORD_PREFIX + newListId, record);
            _recordStore.commitTransaction();
            LOG.info("### Updated Database Record");
          }
        } catch (IOException e) {

        }
      }
    }));
  }

  long queueListImportRequest(final CrawlListDatabaseRecord record) {

    final Semaphore blockingSemaphore = new Semaphore(0);

    getEventLoop().setTimer(new Timer(0, false, new Timer.Callback() {

      @Override
      public void timerFired(Timer timer) {
        try {

          File listDataPath = new File(getCrawlHistoryDataDir(), record
              .getTempFileName());

          long listId = _crawlHistoryManager.loadList(listDataPath,record.getRefreshInterval());

          record.setListId(listId);

          _recordStore.beginTransaction();
          _recordStore.insertRecord(CRAWL_LIST_RECORD_PARENT_ID,
              CRAWL_LIST_RECORD_PREFIX + listId, record);
          _recordStore.commitTransaction();
        } catch (IOException e) {
          record.setFieldClean(CrawlListDatabaseRecord.Field_LISTID);
          LOG.error(CCStringUtils.stringifyException(e));
        } finally {
          blockingSemaphore.release();
        }

      }
    }));

    blockingSemaphore.acquireUninterruptibly();

    return (record.isFieldDirty(CrawlListDatabaseRecord.Field_LISTID)) ? record
        .getListId() : -1;
  }

  private static class MutableBoolean { 
    public boolean result = false;
  }
  
  public boolean doesListBelongToCustomer(final long listId,final String customerId) {
    final MutableBoolean resultValue = new MutableBoolean();
    
    final Runnable runnable = new Runnable() {

      @Override
      public void run() {
        try {    
          CrawlListDatabaseRecord record = (CrawlListDatabaseRecord) _recordStore
          .getRecordByKey(CRAWL_LIST_RECORD_PREFIX + listId);
          
          if (record != null) { 
            resultValue.result = (record.getCustomerName().equals(customerId));
          }
        }
        catch (IOException e) {
          LOG.error(CCStringUtils.stringifyException(e));
        }
      }
    };
    if (Thread.currentThread() != getEventLoop().getEventThread()) {
      final Semaphore blockingSemaphore = new Semaphore(0);
      getEventLoop().setTimer(new Timer(0, false, new Timer.Callback() {

        @Override
        public void timerFired(Timer timer) {
          runnable.run();
          blockingSemaphore.release();
        }
      }));
      blockingSemaphore.acquireUninterruptibly();
    } else {
      runnable.run();
    }    
    return resultValue.result;
  }
  
  /**
   * get the list ids associated with the specified customer id
   * 
   * @param customerId
   * @return Set of list ids
   */
  public Map<Long, CrawlListDatabaseRecord> getListInfoForCustomerId(
      final String customerId) {
    final TreeMap<Long, CrawlListDatabaseRecord> listRecords = new TreeMap<Long, CrawlListDatabaseRecord>();

    final Runnable runnable = new Runnable() {

      @Override
      public void run() {
        try {
          Vector<Long> recordIds = _recordStore
              .getChildRecordsByParentId(CRAWL_LIST_RECORD_PARENT_ID);

          for (long recordId : recordIds) {
            CrawlListDatabaseRecord databaseRecord = (CrawlListDatabaseRecord) _recordStore
                .getRecordById(recordId);

            if (databaseRecord != null
                && (customerId.equals("*") || databaseRecord.getCustomerName()
                    .equals(customerId))) {
              listRecords.put(databaseRecord.getListId(), databaseRecord);
            }
          }
        } catch (IOException e) {
          LOG.error(CCStringUtils.stringifyException(e));
        }
      }
    };

    if (Thread.currentThread() != getEventLoop().getEventThread()) {
      final Semaphore blockingSemaphore = new Semaphore(0);
      getEventLoop().setTimer(new Timer(0, false, new Timer.Callback() {

        @Override
        public void timerFired(Timer timer) {
          runnable.run();
          blockingSemaphore.release();
        }
      }));
      blockingSemaphore.acquireUninterruptibly();
    } else {
      runnable.run();
    }

    return listRecords;
  }
}
