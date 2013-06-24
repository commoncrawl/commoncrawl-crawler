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
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.Semaphore;

import javax.servlet.jsp.JspWriter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.WritableUtils;
import org.commoncrawl.async.Timer;
import org.commoncrawl.async.ConcurrentTask.CompletionCallback;
import org.commoncrawl.common.Environment;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.io.NIODNSResolver;
import org.commoncrawl.io.NIOHttpConnection;
import org.commoncrawl.protocol.ActiveHostInfo;
import org.commoncrawl.protocol.CrawlHistoryStatus;
import org.commoncrawl.protocol.CrawlMaster;
import org.commoncrawl.protocol.CrawlSegment;
import org.commoncrawl.protocol.CrawlSegmentHost;
import org.commoncrawl.protocol.CrawlSegmentStatus;
import org.commoncrawl.protocol.CrawlURL;
import org.commoncrawl.protocol.CrawlerAction;
import org.commoncrawl.protocol.CrawlerHistoryService;
import org.commoncrawl.protocol.CrawlerService;
import org.commoncrawl.protocol.CrawlerStatus;
import org.commoncrawl.protocol.SlaveHello;
import org.commoncrawl.protocol.SlaveRegistration;
import org.commoncrawl.rpc.base.internal.AsyncClientChannel;
import org.commoncrawl.rpc.base.internal.AsyncContext;
import org.commoncrawl.rpc.base.internal.AsyncRequest;
import org.commoncrawl.rpc.base.internal.AsyncServerChannel;
import org.commoncrawl.rpc.base.internal.NullMessage;
import org.commoncrawl.rpc.base.internal.AsyncRequest.Callback;
import org.commoncrawl.rpc.base.internal.AsyncRequest.Status;
import org.commoncrawl.rpc.base.shared.RPCException;
import org.commoncrawl.server.AsyncWebServerRequest;
import org.commoncrawl.server.CommonCrawlServer;
import org.commoncrawl.service.crawler.CrawlLog.CheckpointCompletionCallback;
import org.commoncrawl.service.crawler.CrawlerEngine.CrawlStopCallback;
import org.commoncrawl.service.crawler.SegmentLoader.LoadProgressCallback;
import org.commoncrawl.service.crawler.filters.DomainFilterData;
import org.commoncrawl.service.crawler.filters.FilterResults;
import org.commoncrawl.service.crawler.filters.CrawlRateOverrideFilter;
import org.commoncrawl.service.crawler.filters.DomainFilter;
import org.commoncrawl.service.crawler.filters.IPAddressBlockFilter;
import org.commoncrawl.service.crawler.filters.Filter.FilterResult;
import org.commoncrawl.service.directory.DirectoryServiceCallback;
import org.commoncrawl.service.directory.DirectoryServiceItemList;
import org.commoncrawl.service.directory.DirectoryServiceRegistrationInfo;
import org.commoncrawl.service.directory.DirectoryServiceServer;
import org.commoncrawl.service.directory.DirectoryServiceSubscriptionInfo;
import org.commoncrawl.service.dns.DNSRewriteFilter;
import org.commoncrawl.service.dns.DNSService;
import org.commoncrawl.service.dns.DNSServiceResolver;
import org.commoncrawl.service.statscollector.CrawlerStatsService;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.FlexBuffer;
import org.commoncrawl.util.IPAddressUtils;
import org.commoncrawl.util.RuntimeStatsCollector;
import org.commoncrawl.util.URLUtils;


/**
 * Crawler Server (CommonCrawlerServer derived class)
 * 
 * @author rana
 *
 */
public class CrawlerServer extends CommonCrawlServer 
  implements CrawlerService, 
	           AsyncClientChannel.ConnectionCallback, 
	           AsyncServerChannel.ConnectionCallback,
	           DirectoryServiceCallback, 
	           Timer.Callback {

	
	enum HandshakeStatus {
		
		Uninitialized,
		SendingInitToMaster,
		MasterConnected,
		SentUpdateToMaster
	}
	
	
	private 	InetSocketAddress 	_masterAddress = null;
	private int                   _maxSockets = -1;
	private static 	CrawlerEngine	_engine;
	private static  CrawlerServer _server;
	private CrawlerStatus         _crawlerStatus;
	
	
  /** DNS Resolver Service **/
	private InetSocketAddress     _dnsServiceAddress;
  private AsyncClientChannel    _dnsServiceChannel;
  private DNSService.AsyncStub  _dnsServiceStub;
  private DNSServiceResolver    _dnsServiceResolver;
	
  /** Directory Service Stub **/
  InetAddress                       _directoryServiceAddress;  
  AsyncClientChannel                _directoryServiceChannel;
  DirectoryServiceServer.AsyncStub  _directoryServiceStub;
  long                              _directoryServiceCallbackCookie = 0;

  /** StatsCollector Service Stub **/
  InetSocketAddress                 _statsCollectorAddress;  
  AsyncClientChannel                _statsCollectorServiceChannel;
  CrawlerStatsService.AsyncStub     _statsCollectorStub;
  
  /** History Service Stub **/
  InetSocketAddress                 _historyServiceAddress;  
  AsyncClientChannel                _historyServiceChannel;
  CrawlerHistoryService.AsyncStub   _historyServiceStub;
  
  /** Crawl Content FS **/
  FileSystem  _crawlContentFS;
  
  /** filters **/
  private DomainFilter     _blockedDomainFilter = null;
  private DomainFilter     _temporarilyBlockedDomainFilter = null;
  private DNSRewriteFilter _rewriteFilter = null;
  private IPAddressBlockFilter _ipAddressBlockFilter = null;
  private long             _filterUpdateTime = -1;
  private CrawlRateOverrideFilter _crawlRateOverrideFilter = null;
  
  /** record store **/
  static final String CrawlerStateKey = "CrawlerState2";

  /** master crawl controller support **/
  private static final int ACTIVE_HOST_LIST_REFRESH_INTERVAL_MASTER = 20 * 1000; // the master refreshed its list every 20 seconds
  private static final int ACTIVE_HOST_LIST_REFRESH_INTERVAL_CLIENT = 1 * 60000; // the clients refresh their list every minute
  private int 					_pauseStateTimestampIncremental = -1;
  private long 					_pauseStateTimestamp = -1;
  // used on the master side to store latest state 
  private FlexBuffer 		_masterPauseStateBuffer = null;
  // slave pause state 
  private Set<Integer>	_pausedHostsSet = null;
  
  InetSocketAddress         _masterCrawlerAddress = null;  
  AsyncClientChannel        _masterCrawlerServiceChannel = null;
  CrawlerService.AsyncStub  _masterCrawlerStub = null;
  Timer											_masterCrawlerHostListRefreshTimer = null;
  
	
	private String                     _unitTestName = null;
	private File                       _domainQueueDirectory = null;
	private InetSocketAddress[]        _crawlInterface = null;
	private InetSocketAddress          _proxyAddress = null;

  private static final int DEFAULT_DNS_HIGH_WATER_MARK = 500;
  private static final int DEFAULT_DNS_LOW_WATER_MARK  = 10;
  private static final  String DEFAULT_DOMAIN_QUEUE_DIR_NAME = "domainQueue";
  
  private static final int DEFAULT_HOST_IDLE_FLUSH_THRESHOLD =60000;
  
  private static int _dnsHighWaterMark =DEFAULT_DNS_HIGH_WATER_MARK; 
  private static int _dnsLowWaterMark  = DEFAULT_DNS_LOW_WATER_MARK; 
  private static int _maxActiveURLS = -1;
  private static long _cycleTime = -1;
  
  private static int  _crawlLogCheckpointItemThreshold = CrawlLog.DEFAULT_LOG_FILE_CHECKPOINT_ITEM_COUNT_THRESHOLD;
  private static long _crawlLogCheckpointLogSizeThreshold = CrawlLog.DEFAULT_LOG_FILE_SIZE_CHECKPOINT_THRESHOLD;
  private static int  _crawlLogCheckpointInterval = CrawlLog.DEFAULT_LOG_CHECKPOINT_INTERVAL;
  private static int  _crawlLogFlushInterval = CrawlLog.DEFAULT_LOG_FLUSH_INTERVAL;
  
  /*** NEW MASTER / SLAVE HANDSHAKE STUFF **/
  
  enum HandshakeState { 
    NOT_INITIATED,
    INITIATING,
    IDLE,
    RENEWING,
    SHUTTING_DOWN
  }
  
  HandshakeState _handshakeState = HandshakeState.NOT_INITIATED;
  boolean _connectedToMaster = false;
  SlaveRegistration _registration = null;
  
  AsyncClientChannel _masterChannel = null;
  CrawlMaster.AsyncStub _masterRPCStub;
  
  int         _masterPort = -1;
  /** timers **/
  Timer _handshakeTimer;

  
	public static CrawlerEngine getEngine() { 
	  return _engine;
	}
	
	public static CrawlerServer getServer() { 
	  return _server;
	}
	
	/**
	 * 
	 * @return FileSystem used to store crawled content
	 */
	FileSystem getCrawlContentFileSystem() { 
	  return _crawlContentFS;
	}
	
	/** get the domain queue storage directory name **/
	public File getDomainQueueDir() { return _domainQueueDirectory; }
	
	/** get the dns service resolver **/
	public NIODNSResolver getDNSServiceResolver() { return _dnsServiceResolver; }
	
	/** get directory service address **/
	public InetAddress getDirectoryServiceAddress() { return _directoryServiceAddress; }
	
	/** get history service stub **/
	public CrawlerHistoryService.AsyncStub getHistoryServiceStub() { return _historyServiceStub; }
	
	/** get stats service stub **/
	public CrawlerStatsService.AsyncStub getStatsCollectorStub() { return _statsCollectorStub; }
	
	/** has parse queue **/
	public boolean isParseQueueEnabled() { return true; }
	
  //@Override
  protected boolean initServer() {
	  
    _server = this;
    
    CrawlList.setServerSingleton(this);
    
    String dataPath = _server.getDataDirectory().getAbsolutePath() + "/";
    String dbPath = dataPath;
    
    if (CrawlEnvironment.inUnitTestMode()) {
      dbPath += "UnitTest_" + CrawlEnvironment.CRAWLER_DB;
      if (Environment.detailLogEnabled())
        LOG.info("In Unit Test Mode: DB Path is:" +dbPath );
    }
    else { 
      dbPath += getServer().getDatabaseName();
      if (Environment.detailLogEnabled())
        LOG.info("initialize - Config says Crawler Segment db path is: "+dbPath);
    }
    
    
    File databasePath = new File(dbPath);
    
    // if in unit test mode ... delete existing db file 
    if (CrawlEnvironment.inUnitTestMode()) {
      // delete existing datapase path if any ...   
      if (databasePath.exists()) 
        databasePath.delete();
    }    

    _crawlerStatus = new CrawlerStatus();
    _crawlerStatus.setActiveListNumber(0);
    _crawlerStatus.setCrawlerState(CrawlerStatus.CrawlerState.IDLE);

    
    // initialize domain queue directory
    if (_domainQueueDirectory == null) { 
      _domainQueueDirectory  = new File(getDataDirectory(),DEFAULT_DOMAIN_QUEUE_DIR_NAME);
    }
    
    if (!_domainQueueDirectory.isDirectory()) { 
      if (!_domainQueueDirectory.mkdir()) { 
        LOG.error("Unable to Initialize Domain Queue Directory at:" + _domainQueueDirectory.getAbsolutePath());
        return false;
      }
    }
    try {
      LOG.info("Starting Communications with DNS Server At:" + _dnsServiceAddress);
      _dnsServiceChannel = new AsyncClientChannel(_eventLoop,new InetSocketAddress(0),_dnsServiceAddress,this);
      _dnsServiceChannel.open();
      _dnsServiceStub = new DNSService.AsyncStub(_dnsServiceChannel);
      _dnsServiceResolver = new DNSServiceResolver(_dnsServiceStub);
      
      LOG.info("Loading Filters");
      reloadFilters();
      
      // start communications with the directory service
      LOG.info("Starting Communications with Directory Service Server At:" + _dnsServiceAddress);
      _directoryServiceChannel = new AsyncClientChannel(_eventLoop,new InetSocketAddress(0),new InetSocketAddress(_directoryServiceAddress,CrawlEnvironment.DIRECTORY_SERVICE_RPC_PORT),this);
      _directoryServiceChannel.open();
      _directoryServiceStub   = new DirectoryServiceServer.AsyncStub(_directoryServiceChannel);

      // start communications with the directory service
      LOG.info("Starting Communications with Stats Server At:" + _statsCollectorAddress);
      _statsCollectorServiceChannel = new AsyncClientChannel(_eventLoop,new InetSocketAddress(0),_statsCollectorAddress,this);
      _statsCollectorServiceChannel.open();
      _statsCollectorStub   = new CrawlerStatsService.AsyncStub(_statsCollectorServiceChannel);
      
      
      // and with the history server
      if (!externallyManageCrawlSegments()){ 
        LOG.info("Starting Communications with History Service Server At:" + _historyServiceAddress);
        _historyServiceChannel = new AsyncClientChannel(_eventLoop,new InetSocketAddress(0),_historyServiceAddress,this);
        _historyServiceChannel.open();
        _historyServiceStub   = new CrawlerHistoryService.AsyncStub(_historyServiceChannel);
      }
      
      // see if we have a master crawler ... 
      if (_masterCrawlerAddress != null) { 
        _masterCrawlerServiceChannel = new AsyncClientChannel(_eventLoop,new InetSocketAddress(0),_masterCrawlerAddress,this);
        _masterCrawlerServiceChannel.open();
        _masterCrawlerStub   = new CrawlerService.AsyncStub(_masterCrawlerServiceChannel);
      }
      
      // initialize logging servlet 
      getWebServer().addServlet("tailLog", "/tailLog",RequestLogServlet.class); 
    }
    catch (IOException e) { 
      LOG.fatal(CCStringUtils.stringifyException(e));
      return false;
    }
    
    if (externallyManageCrawlSegments()) { 
      if (!initializeEngine(0)) {  
        return false;
      }
    }
	  
	  // create server channel ... 
	  AsyncServerChannel channel = new AsyncServerChannel(this, this.getEventLoop(), this.getServerAddress(),this);
	  
	  // register RPC services it supports ... 
	  registerService(channel,CrawlerService.spec);
	  registerService(channel,DirectoryServiceCallback.spec);

	  // create connection to master ...
    InetSocketAddress masterLocalInterfaceAddress = new InetSocketAddress(_serverAddress.getAddress(),0);
    
    try {
      _masterChannel = new AsyncClientChannel(getEventLoop(),masterLocalInterfaceAddress,_masterAddress,this);
      _masterRPCStub = new CrawlMaster.AsyncStub(_masterChannel);
      _masterChannel.open();

      
      _handshakeTimer = new Timer(1000,true,this);
      getEventLoop().setTimer(_handshakeTimer);
      
      return true;

    } catch (IOException e) {
      LOG.error(CCStringUtils.stringifyException(e));
    }
    
    return false;
  }

  
  private boolean initializeEngine(int activeListId) { 
    // initialize the crawl engine ... 
    _engine = new CrawlerEngine(this,_maxSockets,_dnsHighWaterMark,_dnsLowWaterMark,_cycleTime,activeListId);
    
    if (_maxActiveURLS != -1) { 
      _engine.setMaxActiveURLThreshold(_maxActiveURLS);
    }
    
    InetSocketAddress crawlInterfaces[] = null;
    if (_crawlInterface != null && _crawlInterface.length != 0) { 
      LOG.info("Crawl Interfaces are:");
      for (InetSocketAddress address : _crawlInterface) { 
        LOG.info(address.toString());
      }
      crawlInterfaces = _crawlInterface;
    }
    else { 
      crawlInterfaces = new InetSocketAddress[] { new InetSocketAddress(0) };
    }
    
    if (!_engine.initialize(crawlInterfaces)) { 
      LOG.fatal("Crawl Engine initialization failed!. Exiting... ");
      return false;
    }    
    return true;
  }
  
  //@Override
  protected boolean parseArguements(String[] argv) {
	  
    getConfig().set("http.agent.name",CrawlEnvironment.CCBOT_UA);
    
	  for(int i=0; i < argv.length;++i) {
	      if (argv[i].equalsIgnoreCase("--master")) { 
	        if (i+1 < argv.length) { 
	          _masterAddress = CCStringUtils.parseSocketAddress(argv[++i]);
	        }
	      }
	      else if (argv[i].equalsIgnoreCase("--dnsservice")) { 
          if (i+1 < argv.length) { 
            _dnsServiceAddress = new InetSocketAddress(argv[++i],CrawlEnvironment.DNS_SERVICE_RPC_PORT);
          }
        }
	      else if (argv[i].equalsIgnoreCase("--maxSockets")) { 
          if (i+1 < argv.length) { 
            _maxSockets = Integer.parseInt(argv[++i]);
          }
        }
	      else if (argv[i].equalsIgnoreCase("--unitTest")) { 
	        CrawlEnvironment.setUnitTestMode(true);
	        _unitTestName = argv[++i];
	      }

        else if (argv[i].equalsIgnoreCase("--dnsHighMark")) { 
          _dnsHighWaterMark = Integer.parseInt(argv[++i]);
        }

        else if (argv[i].equalsIgnoreCase("--dnsLowMark")) { 
          _dnsLowWaterMark = Integer.parseInt(argv[++i]);
        }
        else if (argv[i].equalsIgnoreCase("--maxActiveURLS")){
          _maxActiveURLS = Integer.parseInt(argv[++i]);
        }
        else if (argv[i].equalsIgnoreCase("--cycleTimer")){ 
          _cycleTime = System.currentTimeMillis() + (Integer.parseInt(argv[++i]) * 1000);
        }
        else if (argv[i].equalsIgnoreCase("--domainQueueDir")) { 
          _domainQueueDirectory = new File(argv[++i]);
        }
        else if (argv[i].equalsIgnoreCase("--crawlInterface")) { 
          String interfaceList[] = argv[++i].split(";");
          _crawlInterface = new InetSocketAddress[interfaceList.length];
          for(int j=0;j<_crawlInterface.length;++j) {  
            try {
              _crawlInterface[j] = new InetSocketAddress(InetAddress.getByName(interfaceList[j]),0);
            } catch (UnknownHostException e) {
              LOG.error(CCStringUtils.stringifyException(e));
              return false;
            }
          }
        }
        else if (argv[i].equalsIgnoreCase("--useProxyServer")) { 
          _proxyAddress = CCStringUtils.parseSocketAddress(argv[++i]);
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
        else if (argv[i].equalsIgnoreCase("--statscollector")) { 
          if (i+1 < argv.length) { 
            try {
              _statsCollectorAddress = new InetSocketAddress(InetAddress.getByName(argv[++i]),CrawlEnvironment.CRAWLSTATSCOLLECTOR_SERVICE_RPC_PORT);
            } catch (UnknownHostException e) {
              LOG.error(CCStringUtils.stringifyException(e));
            }
          }
        }
        else if (argv[i].equalsIgnoreCase("--historyserver")) { 
          if (i+1 < argv.length) { 
            _historyServiceAddress = CCStringUtils.parseSocketAddress(argv[++i]);
          }
        }
        else if (argv[i].equalsIgnoreCase("--mastercrawler")) { 
        	_masterCrawlerAddress = CCStringUtils.parseSocketAddress(argv[++i]);
        }
        else if (argv[i].equalsIgnoreCase("--defaultFS")) { 
          try {
            CrawlEnvironment.setDefaultHadoopFSURI(argv[++i]);
          } catch (Exception e) {
            LOG.error(CCStringUtils.stringifyException(e));
          }          
        }
        else if (argv[i].equalsIgnoreCase("--contentFS")) { 
          try {
            _crawlContentFS = FileSystem.get(new URI(argv[++i]),getConfig());
          } catch (Exception e) {
            LOG.error(CCStringUtils.stringifyException(e));
          }          
        }
        else if (argv[i].equalsIgnoreCase("--storageBase")) {
          CrawlEnvironment.setCCRootDir(argv[++i]);
          LOG.info("Changed CC_ROOT_DIR to:" + CrawlEnvironment.CC_ROOT_DIR);
        }
        else if (argv[i].equalsIgnoreCase("--segmentLogsDir")) {
          CrawlEnvironment.setCrawlSegmentLogsDirectory(argv[++i]);
        }
        else if (argv[i].equalsIgnoreCase("--segmentDataDir")) {
          CrawlEnvironment.setCrawlSegmentDataDirectory(argv[++i]);
        }
        else if (argv[i].equalsIgnoreCase("--bloomFilterSize")) {
          CrawlerEngine.BLOOM_FILTER_SIZE = Integer.parseInt(argv[++i]);
        }
        else if (argv[i].equalsIgnoreCase("--userAgent")) {
          getConfig().set("http.agent.name",argv[++i]);
        }
        else if (argv[i].equalsIgnoreCase("--crawlLogItemThreshold")) { 
          _crawlLogCheckpointItemThreshold = Integer.parseInt(argv[++i]);
        }
        else if (argv[i].equalsIgnoreCase("--crawlLogSizeThreshold")) {
          _crawlLogCheckpointLogSizeThreshold = Long.parseLong(argv[++i]);
        }
        else if (argv[i].equalsIgnoreCase("--crawlLogCheckpointInterval")) {
          _crawlLogCheckpointInterval = Integer.parseInt(argv[++i]);
        }
        else if (argv[i].equalsIgnoreCase("--crawlLogFlushInterval")) {
          _crawlLogFlushInterval = Integer.parseInt(argv[++i]);
        }
	  }
	  LOG.info(_crawlContentFS);
	  return (_masterAddress != null 
	      && _dnsServiceAddress != null 
	      && _statsCollectorAddress != null
	      && _directoryServiceAddress != null
	      && _crawlContentFS != null
	      && (_historyServiceAddress != null || externallyManageCrawlSegments()));
  }

  //@Override
  protected void printUsage() {
	  System.out.println("Crawler Startup Args: --master ["+ _masterAddress +"] " 
	      + " --crawlInterface ["+ _crawlInterface +"] "
	      + " --dnsservice ["+ _dnsServiceAddress +"] "
	      + " --directoryserver ["+ _directoryServiceAddress +"] "
	      + " --statscollector ["+ _statsCollectorAddress +"] "
	      + " --historyserver ["+ _historyServiceAddress +"] "
	      + " --contentFS ["+ _crawlContentFS +"]"
	      );
  }

  //@Override
  protected boolean startDaemons() {
    return true;
  }

  //@Override
  protected void stopDaemons() {
    
  }

  //@Override
  protected String   getDefaultLogFileName() { 
    return "crawler";
  }
  
  //@Override
  protected int getDefaultRPCPort() {
    return CrawlEnvironment.DEFAULT_CRAWLER_RPC_PORT;
  }

   //@Override
  protected String getDefaultHttpInterface() {
  	return CrawlEnvironment.DEFAULT_HTTP_INTERFACE;
  }

  //@Override
  protected String getDefaultRPCInterface() {
  	return CrawlEnvironment.DEFAULT_RPC_INTERFACE;
  }  

  //@Override
  protected String getWebAppName() {
    return "crawler";
  }

  //@Override
  protected int getDefaultHttpPort() {
    return CrawlEnvironment.DEFAULT_CRAWLER_HTTP_PORT;
  }
  
  //@Override
  protected String  getDefaultDataDir() { 
	  return CrawlEnvironment.DEFAULT_DATA_DIR;
  }
  
  public String getUnitTestName() { 
    return _unitTestName;
  }

  public InetSocketAddress getProxyAddress() { 
    return _proxyAddress;
  }
  
  @Override
  public String getHostName() {
    if (_registration != null) { 
      return CrawlEnvironment.getCrawlerNameGivenId(_registration.getInstanceId());
    }
    else { 
      LOG.error("Get HostName Called but no Lease!");
      return super.getHostName();
    }
  }
  
  public int  getCrawlLogCheckpointItemThreshold() { 
    return _crawlLogCheckpointItemThreshold;
  }
  
  public long getCrawlLogCheckpointLogSizeThreshold() { 
    return _crawlLogCheckpointLogSizeThreshold;
  }
  
  public int  getCrawlLogCheckpointInterval() { 
    return _crawlLogCheckpointInterval;
  }
  
  public int  getCrawlLogFlushInterval() { 
    return _crawlLogFlushInterval;
  }

  
	/*
	public void addCrawlSegment(AsyncContext<CrawlSegment, CrawlerStatus> rpcContext) throws RPCException {
	  _engine.addCrawlSegment(rpcContext);
	}
	
	*/

	public void OutgoingChannelConnected(AsyncClientChannel channel) {
	  if (channel == _dnsServiceChannel) { 
	    LOG.info("Connected to DNS Service");
	  }
	  else if (channel == _historyServiceChannel) { 
	    LOG.info("Connected to History Server. ");
	  }
	  else if (channel == _statsCollectorServiceChannel) { 
	    LOG.info("Connected to StatsCollector Server");
	  }
	  else if (channel == _directoryServiceChannel) { 
	    LOG.info("Connected to Directory Server. Registering for Callbacks");
	    
	    getEventLoop().setTimer(new Timer(1000,false,new Timer.Callback() {

        @Override
        public void timerFired(Timer timer) {

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
	      
	    }));
	  }
	  else if (channel == _masterCrawlerServiceChannel) { 
	  	LOG.info("Connected to Master Crawler at:" + _masterCrawlerAddress.toString() );
	  	refreshMasterCrawlerActiveHostList();
	  }
	  else if (channel == _masterChannel) {
	    LOG.info("Connected to Master");
	    if (_handshakeState == HandshakeState.NOT_INITIATED) {
	      LOG.info("Initiating Handshake with Master");
	      initiateHandshake();
	    }
	  }
	}

	
	
	public boolean OutgoingChannelDisconnected(AsyncClientChannel channel) {
	  if (channel == _dnsServiceChannel) {  
	    return true;
	  }
	  else if (channel == _masterCrawlerServiceChannel) { 
	  	LOG.info("Disconnected from Master Crawler at:" + _masterCrawlerAddress);
	  	if (_masterCrawlerHostListRefreshTimer != null) {
	  		_eventLoop.cancelTimer(_masterCrawlerHostListRefreshTimer);
	  		_masterCrawlerHostListRefreshTimer = null;
	  	}
	  }
	  else if (channel == _masterChannel) { 
	    LOG.info("Master Channel Disconnected. Initiating Clean Shutdown");
	    try {
        shutdownServices();
      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
      }
	  }
	  return false;
	}
	
	public void IncomingClientConnected(AsyncClientChannel channel) {
	  if (Environment.detailLogEnabled())
	    LOG.info("Master INCOMING Channel Connected");
	}
	
	public void IncomingClientDisconnected(AsyncClientChannel channel) {

	}

  public void dumpStats(final JspWriter out) { 
    
    final RuntimeStatsCollector stats = _engine.getStats();
    
    AsyncWebServerRequest webRequest = new AsyncWebServerRequest("dumpStats",out) {

      @Override
      public boolean handleRequest(Semaphore completionSemaphore)throws IOException { 
        synchronized(stats) { 
          stats.dumpStatsToHTML(out);
        }
        return false;
      } 
    };
    webRequest.dispatch(_eventLoop);
    webRequest = null;
  }
  
  public void dumpQueueDetails(final JspWriter out) { 
    
    
    AsyncWebServerRequest webRequest = new AsyncWebServerRequest("dumpStats",out) {

      @Override
      public boolean handleRequest(Semaphore completionSemaphore)throws IOException { 
        _engine.dumpQueueDetailsToHTML(out);
        return false;
      } 
    };
    webRequest.dispatch(_eventLoop);
    webRequest = null;
  }
  
  public void dumpHostDetails(final JspWriter out,String hostId)throws IOException { 
    
    if (hostId != null) {
      final int hostIP = Integer.parseInt(hostId);
      
      AsyncWebServerRequest webRequest = new AsyncWebServerRequest("dumpStats",out) {

        @Override
        public boolean handleRequest(Semaphore completionSemaphore)throws IOException { 
          
          _engine.dumpHostDetailsToHTML(out,hostIP);
          
          return false;
        } 
      };
      webRequest.dispatch(_eventLoop);
      webRequest = null;

      
    }
    else { 
      out.write("ERROR:Invalid Host ID");
    }
  }  
  
  
  public void shutdownCleanly(final JspWriter out) { 
  	
  	/*
    
    AsyncWebServerRequest webRequest = new AsyncWebServerRequest("shutdown",out) {

      @Override
      public boolean handleRequest(Semaphore completionSemaphore)throws IOException { 
        LOG.info("Shutdown Initiated Via Web Interface");
        _engine.stopCrawlerCleanly();
        return false;
      } 
    };
    webRequest.dispatch(_eventLoop);
    webRequest = null;
    */
  	
  	System.exit(-1);
  }

  protected  void subscribeToList(String listPath) throws IOException { 
    DirectoryServiceSubscriptionInfo subscription = new DirectoryServiceSubscriptionInfo();
    subscription.setSubscriptionPath(listPath);
    
    LOG.info("Subscribing to:" + listPath);
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
  
  @Override
  public void initialize(AsyncContext<DirectoryServiceRegistrationInfo, NullMessage> rpcContext) throws RPCException {
    LOG.info("Received Initialization Request on Callback Channel");
    if (rpcContext.getInput().getRegistrationCookie() == _directoryServiceCallbackCookie) {
      LOG.info("Cookies Match! Sending Subscription information");
      
      rpcContext.completeRequest();
      
        getEventLoop().setTimer(new Timer(1000,false,new Timer.Callback() {

          @Override
          public void timerFired(Timer timer) {
            try {
              subscribeToList("/lists/.*");
            } catch (IOException e) {
              LOG.error("List subscription failed with exception:" + CCStringUtils.stringifyException(e));
            }
          } 
          
        }));
        
    }
  }

  @Override
  public void itemChanged(AsyncContext<DirectoryServiceItemList, NullMessage> rpcContext) throws RPCException {
    LOG.info("### Directory Service List Change Message Received");
    reloadFilters();
    
    rpcContext.completeRequest();
  }
  
  /**
   * 
   * @return the path to the crawl rate override filter 
   */
  public String getCrawlRateOverrideFilterPath() { 
    return CrawlEnvironment.CRAWL_RATE_MOD_FILTER_PATH;
  }
  
  protected void reloadFilters() { 
    LOG.info("Reloading Filters");
    
    // update filter update time 
    _filterUpdateTime = System.currentTimeMillis();
    
    LOG.info("Initializing Filters");
    // _spamFilter           = new DomainFilter(DomainFilterData.Type.Type_ExlusionFilter);
    if(useGlobalBlockLists()) { 
      _blockedDomainFilter  = new DomainFilter(DomainFilterData.Type.Type_ExlusionFilter);
      _temporarilyBlockedDomainFilter = new DomainFilter(DomainFilterData.Type.Type_ExlusionFilter);
      _ipAddressBlockFilter = new IPAddressBlockFilter();
    }
    _rewriteFilter = new DNSRewriteFilter();
    _crawlRateOverrideFilter = new CrawlRateOverrideFilter();
    
    
    try { 
      // LOG.info("### Loading Spam Filter");
      // _spamFilter.loadFromPath(_directoryServiceAddress,CrawlEnvironment.SPAM_DOMAIN_LIST,true);
      if(useGlobalBlockLists()) {
        LOG.info("### Loading Blocked Domains Filter");
        _blockedDomainFilter.loadFromPath(_directoryServiceAddress,CrawlEnvironment.BLOCKED_DOMAIN_LIST,false);
        LOG.info("### Loading Temporarily Blocked Domains Filter");
        _temporarilyBlockedDomainFilter.loadFromPath(_directoryServiceAddress,CrawlEnvironment.TEMPORARILY_BLOCKED_DOMAIN_LIST,false);
        LOG.info("### IP Address Block Filter ");
        _ipAddressBlockFilter.loadFromPath(_directoryServiceAddress, CrawlEnvironment.IP_BLOCK_LIST, false);
      }
      LOG.info("### Loading DNS Rewrite Filter ");
      _rewriteFilter.loadFromPath(_directoryServiceAddress, CrawlEnvironment.DNS_REWRITE_RULES, false);
      LOG.info("### Loading Crawl Rate Override Filter from path:" + getCrawlRateOverrideFilterPath());
      _crawlRateOverrideFilter.loadFromPath(getDirectoryServiceAddress(),getCrawlRateOverrideFilterPath(),false);
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
    }
  }
  
  public DomainFilter getDomainBlackListFilter() { return _blockedDomainFilter; }
  public DomainFilter getTemporaryBlackListFilter() { return _temporarilyBlockedDomainFilter; }
  public DNSRewriteFilter getDNSRewriteFilter() { return _rewriteFilter; }
  public IPAddressBlockFilter getIPAddressFilter() { return _ipAddressBlockFilter; }
  public long getFilterUpdateTime() { return _filterUpdateTime; }
  
  /** get database name for this instance 
   * 
   */
  public String getDatabaseName() { 
    return CrawlEnvironment.CRAWLER_DB;
  }
  
  
  /** queue up a high priority http get 
   * 
   */
  public void queueHighPriorityURL(String url,long fingerprint,CrawlItemStatusCallback callback) { 
    _engine.queueExternalURL(url, fingerprint,true, callback);
  }
  
  /** queue up a low priority http get 
   * 
   */
  public void queueLowPriorityURL(String url,long fingerprint,CrawlItemStatusCallback callback) { 
    _engine.queueExternalURL(url, fingerprint,false, callback);
  }

  /** queue low priority crawlsegmetHost
   * 
   */
  public void queueExternalHost(CrawlSegmentHost host,CrawlItemStatusCallback callback) { 
    _engine.queueExternalCrawlSegmentHost(host,callback);
  }
  
  
  /** enable the crawl log (yes by default)
   * 
   */
  public boolean enableCrawlLog() { 
    return true;
  }
  
  
  /** crawl completed for the specified crawl target 
   * 
   */
  public void crawlComplete(NIOHttpConnection connection,CrawlURL url,CrawlTarget optTargetObj,boolean successOrFailure) { 
    // NOOP
  }
  
  /** externally manage crawl segments 
   * 
   */
  public boolean externallyManageCrawlSegments() { 
    return false;
  }
  
  /** load the given crawl segment in a background thread 
   * 
   */
  public void loadExternalCrawlSegment(final CrawlSegment segment,final LoadProgressCallback loadCallback,final CompletionCallback<CrawlSegmentStatus> completionCallback,final CrawlSegmentStatus status) { 
    // NOOP
  } 
  
  /** update crawl segment status 
   * 
   */
  public void updateCrawlSegmentStatus(int crawlSegmentId,CrawlSegmentStatus status) { 
    
  }
  
  /** get the fixed list id for high priority urls
   * 
   */
  public int getHighPriorityListId() { 
    return 0;
  }
  
  /** notification that a fetch is starting on the target url 
   * 
   */
  public void fetchStarting(CrawlTarget target,NIOHttpConnection connection) { 

  }
  
  /** should we use black lists 
   * 
   */
  public boolean useGlobalBlockLists() { 
    return true;
  }
  
  /** check host stats for failures
   * 
   */
  public boolean failHostsOnStats() { 
    return true;
  }
  
  /** check for crawl rate override for the specified domain / url**/
  public int checkForCrawlRateOverride(URL url) { 
    FilterResults resultsOut = new FilterResults();
    String rootDomainName = URLUtils.extractRootDomainName(url.getHost());
    if (rootDomainName != null) { 
	    if (_crawlRateOverrideFilter.filterItem(rootDomainName,url.getHost(), url.getPath(), null, resultsOut) == FilterResult.Filter_Modified) { 
	      return resultsOut.getCrawlRateOverride();
	    }
    }
    return -1;
  }
  
  /** is url in server block list **/
  public boolean isURLInBlockList(URL url) { 
    return false;
  }
  
  /** get max robots exclusion in crawl loop override **/
  public int getMaxRobotsExlusionsInLoopOverride() { 
    return -1;
  }
  
  /** get the host idle flush threshold 
   * 
   *  the number of milliseconds a host needs to be idle  for it   
   *  to be purged from memory 
   * **/
  public int getHostIdleFlushThreshold() { 
    return DEFAULT_HOST_IDLE_FLUSH_THRESHOLD;
  }
  
  /** return true to disable cycle timer
   * 
   */
  public boolean disableCycleTimer() {
    //TODO: DISABLING CYCLE TIME BY DEFAULT...
    return true;
  }

  // crawler service related methods ...
  @Override
  public void queryStatus(AsyncContext<NullMessage, CrawlerStatus> rpcContext) throws RPCException {
    if (Environment.detailLogEnabled())
      LOG.info("Received Heartbeat Request From Master");
    rpcContext.getOutput().setCrawlerState(_crawlerStatus.getCrawlerState());
    rpcContext.getOutput().setActiveListNumber(_crawlerStatus.getActiveListNumber());
      
    rpcContext.setStatus(AsyncRequest.Status.Success);
    rpcContext.completeRequest();
  }
  
  private void populateCrawlStatusRepsonse(CrawlerStatus responseObjectOut) { 
    try {
      responseObjectOut.merge(_crawlerStatus);
    } catch (CloneNotSupportedException e) {
    }
  }

  @Override
  public void doAction(final AsyncContext<CrawlerAction, CrawlerStatus> rpcContext) throws RPCException {
    LOG.info("Received Action Cmd:" + rpcContext.getInput().getActionType() + " ListId:" + rpcContext.getInput().getActiveListNumber());
    switch (rpcContext.getInput().getActionType()) { 
      case CrawlerAction.ActionType.FLUSH: { 
        if (_crawlerStatus.getCrawlerState() == CrawlerStatus.CrawlerState.ACTIVE 
            || _crawlerStatus.getCrawlerState() == CrawlerStatus.CrawlerState.IDLE ) { 

          // shift state to pausing ...
          _crawlerStatus.setCrawlerState(CrawlerStatus.CrawlerState.FLUSHING);

          if (getEngine() != null) {
            // stop the crawl ... wait for completion ... 
            getEngine().stopCrawl(new CrawlStopCallback() {
              
              @Override
              public void crawlStopped() {
                // ok, now see if we can initiate a flush ... 
                if (getEngine() != null && getEngine()._crawlLog != null) { 
                  getEngine()._crawlLog.forceFlushAndCheckpointLog(new CheckpointCompletionCallback() {
                    
                    @Override
                    public void checkpointFailed(long checkpointId, Exception e) {
                      try {
                        // log the error and keep going :-(

                        LOG.error(CCStringUtils.stringifyException(e));
                        
                        _crawlerStatus.setCrawlerState(CrawlerStatus.CrawlerState.FLUSHED);
                        populateCrawlStatusRepsonse(rpcContext.getOutput());
                        rpcContext.completeRequest();
                      } catch (RPCException e1) {
                        LOG.error(CCStringUtils.stringifyException(e));
                      }                      
                    }
                    
                    @Override
                    public void checkpointComplete(long checkpointId,Vector<Long> completedSegmentList) {
                      _crawlerStatus.setCrawlerState(CrawlerStatus.CrawlerState.FLUSHED);
                      populateCrawlStatusRepsonse(rpcContext.getOutput());
                      try {
                        rpcContext.completeRequest();
                      } catch (RPCException e) {
                        LOG.error(CCStringUtils.stringifyException(e));
                      }
                    }
                  });
                }
                else { 
                  _crawlerStatus.setCrawlerState(CrawlerStatus.CrawlerState.FLUSHED);
                  populateCrawlStatusRepsonse(rpcContext.getOutput());
                  try {
                    rpcContext.completeRequest();
                  } catch (RPCException e) {
                    LOG.error(CCStringUtils.stringifyException(e));
                  }
                }
              }
            });
          }
          else { 
            _crawlerStatus.setCrawlerState(CrawlerStatus.CrawlerState.FLUSHED);
            populateCrawlStatusRepsonse(rpcContext.getOutput());
            try {
              rpcContext.completeRequest();
            } catch (RPCException e) {
              LOG.error(CCStringUtils.stringifyException(e));
            }
          }
        }
        else { 
          rpcContext.setStatus(Status.Error_RequestFailed);
          rpcContext.setErrorDesc("Invalid State");
          rpcContext.completeRequest();
        }
      }
      break;
      case CrawlerAction.ActionType.PURGE: { 
        if (_crawlerStatus.getCrawlerState() == CrawlerStatus.CrawlerState.FLUSHED 
            || _crawlerStatus.getCrawlerState() == CrawlerStatus.CrawlerState.IDLE ) { 
          LOG.info("Received PURGE REQUEST WHILE IDLE OR PAUSED - Shutting down engine");
          if (_engine != null) { 
            _engine.shutdown();
          }
          LOG.info("Engine shutdown complete.");
          _engine = null;
          // clear data directory 
          CrawlLog.purgeDataDirectory(getDataDirectory());
          // update state ...
          _crawlerStatus.setCrawlerState(CrawlerStatus.CrawlerState.PURGED);
          populateCrawlStatusRepsonse(rpcContext.getOutput());
          rpcContext.completeRequest();
        }
        else { 
          rpcContext.setStatus(Status.Error_RequestFailed);
          rpcContext.setErrorDesc("Invalid State");
          rpcContext.completeRequest();
        }
      }
      break;
      
      case CrawlerAction.ActionType.RESUME_CRAWL: { 
        LOG.info("Receieved Resume Crawl Notificarion");
        if (_crawlerStatus.getCrawlerState() == CrawlerStatus.CrawlerState.FLUSHED 
            && _engine != null 
              && _crawlerStatus.getActiveListNumber() == rpcContext.getInput().getActiveListNumber()) { 
          // ok just resume crawl ...
          LOG.info("Crawler is paused and list ids match. Just restarting the crawl");
          _engine.startCrawl();
          _crawlerStatus.setCrawlerState(CrawlerStatus.CrawlerState.ACTIVE);
          populateCrawlStatusRepsonse(rpcContext.getOutput());
          rpcContext.completeRequest();
        }
        else if (_crawlerStatus.getCrawlerState() == CrawlerStatus.CrawlerState.IDLE 
            || _crawlerStatus.getCrawlerState() == CrawlerStatus.CrawlerState.PURGED){ 
          LOG.info("Crawler is Idle. Starting Crawl from scratch");
          _crawlerStatus.setActiveListNumber(rpcContext.getInput().getActiveListNumber());
          initializeCrawl(rpcContext);
        }
        else if (_crawlerStatus.getCrawlerState() == CrawlerStatus.CrawlerState.ACTIVE  && _crawlerStatus.getActiveListNumber() == rpcContext.getInput().getActiveListNumber()) { 
          
          LOG.info("Received Resume Crawl on already valid active crawl. Ignoring.");
          populateCrawlStatusRepsonse(rpcContext.getOutput());
          rpcContext.completeRequest();
        }
        else { 
          rpcContext.setStatus(Status.Error_RequestFailed);
          rpcContext.setErrorDesc("Invalid State");
          rpcContext.completeRequest();
        }
      }
      break;
    }
  }
  
  private void initializeCrawl(final AsyncContext<CrawlerAction, CrawlerStatus> rpcContext) throws RPCException {
    // ok , first things first, send init to history server
    CrawlHistoryStatus crawlStatus = new CrawlHistoryStatus();
    
    crawlStatus.setActiveCrawlNumber(_crawlerStatus.getActiveListNumber());
    
    LOG.info("Sending Sync to HistoryServer");
    _historyServiceStub.sync(crawlStatus,new Callback<CrawlHistoryStatus, NullMessage>() {

      @Override
      public void requestComplete(AsyncRequest<CrawlHistoryStatus, NullMessage> request) {

        LOG.info("Received response from HistoryServer");
        
        if (request.getStatus() == Status.Success) {
          LOG.info("History Server Sync Successfull - Initializing Engine");
          if (initializeEngine(_crawlerStatus.getActiveListNumber())) {
            LOG.info("Engine Initialization Successfull. Starting Crawl for List:" + _crawlerStatus.getActiveListNumber());
            // kick off the load process 
            _engine.loadCrawlSegments();
            _crawlerStatus.setCrawlerState(CrawlerStatus.CrawlerState.ACTIVE);
            populateCrawlStatusRepsonse(rpcContext.getOutput());
            try {
              rpcContext.completeRequest();
            } catch (RPCException e) {
              LOG.error(CCStringUtils.stringifyException(e));
            }
            // exit on this path ... 
            return;
          }
        }
        // failure path ... 
        rpcContext.getOutput().setCrawlerState(CrawlerStatus.CrawlerState.IDLE);
        rpcContext.setStatus(Status.Error_RequestFailed);
        if (request.getStatus() != Status.Success) { 
          rpcContext.setErrorDesc("History Server Sync Failed");
        }
        else { 
          rpcContext.setErrorDesc("Engine Initialization Failed");
        }
        try {
          rpcContext.completeRequest();
        } catch (RPCException e) {
          LOG.error(CCStringUtils.stringifyException(e));
        }
      }
    });
  }
 
  @Override
  public void stop() {
  	LOG.info("Crawler Server Stop Called");
  	if (_engine != null) { 
  		LOG.info("Shutting Down Crawler Engine");
  		_engine.stopCrawlerCleanly();
  		_engine = null;
  	}
  	LOG.info("CrawlerServer: Calling Super Stop");
  	super.stop();
  }

  
	@Override
  public void queryActiveHosts(AsyncContext<NullMessage, ActiveHostInfo> rpcContext) throws RPCException {
	  // a slave server is asking us about our set of active hosts
		
		rpcContext.setStatus(Status.Success); // default to success ... 
		
		// check to see if we need to refresh the list ... 
		if (_pauseStateTimestamp == -1 || (System.currentTimeMillis() - _pauseStateTimestamp) >= ACTIVE_HOST_LIST_REFRESH_INTERVAL_MASTER) {
			
			LOG.info("Refreshing Active Host List");
			
			_masterPauseStateBuffer = null;
			// ok refresh the list ... 
			if (_engine != null) { 
				
				// ok ... update the host list via the engine ... 
				try { 
					_masterPauseStateBuffer = _engine.getActiveHostListAsBuffer();
				}
				catch (IOException e) { 
					LOG.error("queryActiveHosts threw Exception:"+ CCStringUtils.stringifyException(e));
				}
			}
			_pauseStateTimestamp = System.currentTimeMillis();
			_pauseStateTimestampIncremental++;
		}
	  
		if (_masterPauseStateBuffer != null) { 
			rpcContext.getOutput().setActiveHostIds(_masterPauseStateBuffer);
		}
		// no matter echo current timestamp (serial version)
		rpcContext.getOutput().setPauseStateTimestamp(_pauseStateTimestampIncremental);
		
		rpcContext.completeRequest();
  }
	
	void refreshMasterCrawlerActiveHostList() { 
		// ok if there is a master crawler, and it is online ... 
		if (_masterCrawlerServiceChannel != null && _masterCrawlerServiceChannel.isOpen()) { 
			try {
	      _masterCrawlerStub.queryActiveHosts(new Callback<NullMessage, ActiveHostInfo>() {

	      	@Override
	        public void requestComplete(AsyncRequest<NullMessage, ActiveHostInfo> request) {
	          if (request.getStatus() == Status.Success) { 
	          	// ok update timestamp no matter what 
	          	_pauseStateTimestampIncremental = request.getOutput().getPauseStateTimestamp();
	          	// and clear set ... 
	          	_pausedHostsSet = null;
	          	// now see if we have a valid response ... 
	          	if (request.getOutput().getActiveHostIds().getCount() != 0) { 
	          		LOG.info("Received New Active Host Set From Master Crawler At:" + _masterCrawlerAddress);
	          		// ok we have a valid list of hosts ... 
	          		// create a reader stream 
	          		DataInputBuffer inputStream = new DataInputBuffer();
	          		inputStream.reset(request.getOutput().getActiveHostIds().getReadOnlyBytes(),0,request.getOutput().getActiveHostIds().getCount());
	          		
	          		try { 
	          			// create a set ... 
	          			Set<Integer> ipAddressSet = new TreeSet<Integer>();
	          			// populate it 
	          			int ipAddressCount = WritableUtils.readVInt(inputStream);
	          			for (int i=0;i<ipAddressCount;++i) { 
	          				ipAddressSet.add(WritableUtils.readVInt(inputStream));
	          			}
	          			
	          			LOG.info("Successfully updated Active Host Set");
	          			// ok replace set ... 
	          			_pausedHostsSet = ipAddressSet;
	          		}
	          		catch (IOException e) { 
	          			LOG.error(CCStringUtils.stringifyException(e));
	          		}
	          	}
	          }
	        }
	      });
      } catch (RPCException e) {
	      LOG.error(CCStringUtils.stringifyException(e));	      
      }
		}
		
		// ok no matter what... check to see if we need to set up refresh timer ... 
		if (_masterCrawlerHostListRefreshTimer == null) { 
			_masterCrawlerHostListRefreshTimer = new Timer(ACTIVE_HOST_LIST_REFRESH_INTERVAL_CLIENT,true,new Timer.Callback() {
				
				@Override
				public void timerFired(Timer timer) {
					// call refresh again ... 
					refreshMasterCrawlerActiveHostList();
				}
			});
			_eventLoop.setTimer(_masterCrawlerHostListRefreshTimer);
		}
	}
	
	/** get the serial pause state timestamp **/
  final public int getPauseStateSerialTimestamp() { return _pauseStateTimestampIncremental; }
  
  /** check to see if a host is paused by a master controller **/
  final public boolean isHostPaused(CrawlHost host) { 
  	if (_pausedHostsSet != null) { 
  		return _pausedHostsSet.contains(host.getIPAddress());
  	}
  	return false;
  }

  
  @Override
  public void timerFired(Timer timer) {
    if (timer == _handshakeTimer) { 
      if (_handshakeState == HandshakeState.NOT_INITIATED) { 
        initiateHandshake();
      }
      else if (_handshakeState == HandshakeState.IDLE) { 
        if (_registration != null) { 
          if (System.currentTimeMillis() - _registration.getLastTimestamp() >= 1000) { 
            _handshakeState = HandshakeState.RENEWING;
            try {
              _masterRPCStub.extendRegistration(_registration, new AsyncRequest.Callback<SlaveRegistration, NullMessage>() {

                @Override
                public void requestComplete(AsyncRequest<SlaveRegistration, NullMessage> request) {
                  if (request.getStatus() == Status.Success) { 
                    _registration.setLastTimestamp(System.currentTimeMillis());
                    _handshakeState = HandshakeState.IDLE;
                  }
                  else { 
                    LOG.error("Handshake Extension Failed! - Initiating Shutdown!");
                    try {
                      shutdownServices();
                    } catch (IOException e) {
                      LOG.error(CCStringUtils.stringifyException(e));
                    }
                  }
                } 
                
              });
            } catch (RPCException e) {
              LOG.error("Lease Renewal Failed with Error:"+ CCStringUtils.stringifyException(e));
              try {
                shutdownServices();
              } catch (IOException e1) {
                LOG.error(CCStringUtils.stringifyException(e1));
              }
            }
          }
        }
      }
    }
  }
  
  public void initiateHandshake() { 
    _handshakeState = HandshakeState.INITIATING;
    LOG.info("Connected to Master. Initiating Handshake");
    SlaveHello slaveHello = new SlaveHello();
    
    slaveHello.setIpAddress(IPAddressUtils.IPV4AddressToInteger(_serverAddress.getAddress().getAddress()));
    slaveHello.setCookie(System.currentTimeMillis());
    slaveHello.setServiceName("crawler");
    
    try { 
      _masterRPCStub.registerSlave(slaveHello,new AsyncRequest.Callback<SlaveHello, SlaveRegistration>() {
  
        @Override
        public void requestComplete(AsyncRequest<SlaveHello, SlaveRegistration> request) {
          if (request.getStatus() == Status.Success) {
            LOG.info("Master Handshake Successfull");
            _registration = request.getOutput();
            _registration.setLastTimestamp(System.currentTimeMillis());
            _handshakeState = HandshakeState.IDLE;
            LOG.info("Starting Crawler");
            try {
              startServices();
            } catch (IOException e) {
              LOG.info("Crawler Start Failed with Exception:" +CCStringUtils.stringifyException(e));
              try {
                shutdownServices();
              } catch (IOException e1) {
                LOG.error(CCStringUtils.stringifyException(e1));
              }
            }
          }
          else { 
            LOG.error("Handshake to Master Failed");
            _handshakeState = HandshakeState.NOT_INITIATED;
          }
        }
      });
    }
    catch (Exception e) { 
      LOG.error(CCStringUtils.stringifyException(e));
    }
  }
  
  void startServices() throws IOException { 
    // ok , first things first, send init to history server
    CrawlHistoryStatus crawlStatus = new CrawlHistoryStatus();
    
    crawlStatus.setActiveCrawlNumber(_crawlerStatus.getActiveListNumber());
    
    LOG.info("Sending Sync to HistoryServer");
    _historyServiceStub.sync(crawlStatus,new Callback<CrawlHistoryStatus, NullMessage>() {

      @Override
      public void requestComplete(AsyncRequest<CrawlHistoryStatus, NullMessage> request) {

        LOG.info("Received response from HistoryServer");
        
        if (request.getStatus() == Status.Success) {
          LOG.info("History Server Sync Successfull - Initializing Engine");
          if (initializeEngine(_crawlerStatus.getActiveListNumber())) {
            LOG.info("Engine Initialization Successfull. Starting Crawl for List:" + _crawlerStatus.getActiveListNumber());
            // kick off the load process 
            _engine.loadCrawlSegments();
            _crawlerStatus.setCrawlerState(CrawlerStatus.CrawlerState.ACTIVE);
          }
        }
        else { 
          LOG.error("History Server Sync Failed! Shutting Down Services!");
          try {
            shutdownServices();
          } catch (IOException e) {
            LOG.error(CCStringUtils.stringifyException(e));
          }
        }
      }
    });
  }
  
  void doFinalEngineCleanup() throws IOException { 
    if (_engine != null) { 
      _engine.shutdown();
    }
    LOG.info("Engine shutdown complete.");
    _engine = null;
    // clear data directory 
    CrawlLog.purgeDataDirectory(getDataDirectory());
    
    _crawlerStatus.setCrawlerState(CrawlerStatus.CrawlerState.IDLE);
    
    _registration = null;
    _handshakeState = HandshakeState.NOT_INITIATED;
  }
  
  void shutdownServices()throws IOException {
    LOG.info("Shutdown Services Initiated");
    _handshakeState = HandshakeState.SHUTTING_DOWN;
    
    if (_crawlerStatus.getCrawlerState() == CrawlerStatus.CrawlerState.ACTIVE  || _crawlerStatus.getCrawlerState() == CrawlerStatus.CrawlerState.IDLE ) {
      // shift state to pausing ...
      _crawlerStatus.setCrawlerState(CrawlerStatus.CrawlerState.FLUSHING);
      
      if (getEngine() != null) {
        LOG.info("Stopping Crawl");
        // stop the crawl ... wait for completion ... 
        getEngine().stopCrawl(new CrawlStopCallback() {
          
          @Override
          public void crawlStopped() {
            LOG.info("Crawl Stopped");
            // ok, now see if we can initiate a flush ... 
            if (getEngine() != null) {
              
              if (getEngine()._crawlLog != null) {
                LOG.info("Checkpointing CrawlLog");
                getEngine()._crawlLog.forceFlushAndCheckpointLog(new CheckpointCompletionCallback() {
                  
                  @Override
                  public void checkpointFailed(long checkpointId, Exception e) {
                    LOG.error("Checkpoint Failed!");
                    try {
                      doFinalEngineCleanup();
                    } catch (IOException e1) {
                      LOG.error(CCStringUtils.stringifyException(e1));
                    }
                  }
                  
                  @Override
                  public void checkpointComplete(long checkpointId,Vector<Long> completedSegmentList) {
                    LOG.info("Checkpoint Complete");
                    try {
                      doFinalEngineCleanup();
                    } catch (IOException e) {
                      LOG.error(CCStringUtils.stringifyException(e));
                    }
                  }
                });
              }
            }
            else {
              LOG.info("Crawl Stopped but CrawlerEngine NULL");
              try {
                doFinalEngineCleanup();
              } catch (IOException e) {
                LOG.error(CCStringUtils.stringifyException(e));
              }
            }
          }
        });
        
      }
      else {
        LOG.info("Shutdown Called but Engine Already NULL");
        doFinalEngineCleanup();
      }
    }
  }
}
