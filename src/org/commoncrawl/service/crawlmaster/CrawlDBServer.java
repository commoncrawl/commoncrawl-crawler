package org.commoncrawl.service.crawlmaster;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.Semaphore;

import javax.servlet.jsp.JspWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.commoncrawl.async.Timer;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.db.RecordStore.RecordStoreException;
import org.commoncrawl.mapred.CrawlDBSegment;
import org.commoncrawl.mapred.CrawlDBState;
import org.commoncrawl.mapred.CrawlDBState.CrawlMasterState;
import org.commoncrawl.protocol.CrawlDBService;
import org.commoncrawl.protocol.CrawlHistoryStatus;
import org.commoncrawl.protocol.CrawlerStatus;
import org.commoncrawl.protocol.LongQueryParam;
import org.commoncrawl.protocol.MapReduceTaskIdAndData;
import org.commoncrawl.protocol.SimpleByteResult;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.rpc.base.internal.AsyncContext;
import org.commoncrawl.rpc.base.internal.AsyncRequest.Status;
import org.commoncrawl.rpc.base.internal.AsyncServerChannel;
import org.commoncrawl.rpc.base.internal.NullMessage;
import org.commoncrawl.rpc.base.shared.RPCException;
import org.commoncrawl.server.AsyncWebServerRequest;
import org.commoncrawl.server.CommonCrawlServer;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.S3BulkUploader;
import org.commoncrawl.util.S3BulkUploader.UploadCandidate;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

public class CrawlDBServer extends CommonCrawlServer  implements CrawlDBService   {

  
  public static final Log LOG = LogFactory.getLog(CrawlDBServer.class);

  ///////////////////////////////////////////////////////
  /* MASTER CONSTANTS */
  ///////////////////////////////////////////////////////
  
  
  
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //CONSTANTS 
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  
  

  /** Constants **/
  public static final String CRAWLDB_CRAWL_SEGMENT_TYPE_PARENT_KEY = "CrawlSegment";
  public static final String CRAWLDB_PARSE_SEGMENT_TYPE_PARENT_KEY = "ParseSegment";

  private static final String CrawlDBStateKey = "DBState";
  public static final String CrawlSegmentKeyPrefix = "CSeg_";
  public  static final String ParseSegmentKeyPrefix = "PSeg_";
  
  
  private static final int      STATE_DUMP_INTERVAL = 60000;
  
  /** async heartbeat timer .. **/
  private static final int      CRAWLER_HEARTBEAT_TIMER_INTERVAL = 10000;
  private static final int      CRAWLER_HEARTBEAT_THRESHOLD = 5000;
    
  /** placeholder **/
  public static final int      CRAWLDB_CRAWL_NUMBER = 1;

  
  private static String s3ACL =  "<?xml version='1.0' encoding='UTF-8'?>"+
  "<AccessControlPolicy xmlns='http://s3.amazonaws.com/doc/2006-03-01/'>"+
  " <Owner><ID>eb5386645db2723fedc2c42173e3d45ce30e8ce0849a36771d976f80a2b4b0d8</ID><DisplayName>gil</DisplayName></Owner>"+
  " <AccessControlList>"+
  "   <Grant>"+
  "     <Grantee xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:type='CanonicalUser'><ID>eb5386645db2723fedc2c42173e3d45ce30e8ce0849a36771d976f80a2b4b0d8</ID><DisplayName>gil</DisplayName></Grantee>"+
  "     <Permission>FULL_CONTROL</Permission>"+
  "   </Grant>"+
  "   <Grant>"+
  "     <Grantee xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:type='Group'>"+
  "       <URI>http://acs.amazonaws.com/groups/global/AuthenticatedUsers</URI>"+
  "     </Grantee>"+
  "     <Permission>READ</Permission>"+
  "   </Grant>"+
  " </AccessControlList>"+
  "</AccessControlPolicy>";

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//DATA MEMBERS 
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  // hadoop job status ... 
  enum HadoopStatus { 
    // Idle, doing nothing
    IDLE,
    // Generating a segment ... 
    GENERATING,
    // Parsing a segment ... 
    PARSING,
    // updating ... 
    UPDATING,
    //running custom job
    RUNNING_CUSTOM_JOB
  }
  
  enum LastGeneratorStatus { 
    UNKNOWN,
    LAST_GEN_SUCCESSFULL,
    LAST_GEN_FAILED
  }
  
  
  public static CrawlDBServer _server = null;
  private HadoopStatus          _hadoopStatus = HadoopStatus.IDLE;
  private LastGeneratorStatus _lastCrawlSegmentGenStatus  = LastGeneratorStatus.UNKNOWN;
  private long                       _lastUpdateTime = -1;
  private long                      _lastStateDumpTime = -1;

  
  private boolean                _disableUpdater = true;
  private boolean                _enableS3Uploader = false;
  
  private S3BulkUploader         _uploader = null;
  private String               _s3AccessKey;
  private String               _s3Secret;
  public  String               _s3Bucket;

  /** the master state (storing sequential ids etc.) **/
  private CrawlDBState _serverState;
  /** job state map **/
  private Multimap<String,MapReduceTaskIdAndData> _taskStateMap = TreeMultimap.create();
    

  
  /** get access to the server singleton 
   * 
   */
  static CrawlDBServer getSingleton() { 
    return _server;
  }
  
////////////////////////////////////////////////////////////////////////////////
//MASTER RELATED VARIABLES ... 
////////////////////////////////////////////////////////////////////////////////
  
  /** list of crawlers offcially online **/
  private Map<String,OnlineCrawlerState> _crawlers     = new TreeMap<String,OnlineCrawlerState> ();
  private Map<String,OnlineHistoryServerState> _historyServers     = new TreeMap<String,OnlineHistoryServerState> ();

  private Timer   _crawlerHeartbeatTimer = null;
    
  private String _crawlersFile;

  
////////////////////////////////////////////////////////////////////////////////
//Server Initialization 
////////////////////////////////////////////////////////////////////////////////
	

  @Override
  protected boolean initServer() {
    
    _server = this;
    
    try { 

      // initialize database ... 
      File databasePath = new File(getDataDirectory().getAbsolutePath() + "/" + CrawlEnvironment.CRAWLDB_DB);
      LOG.info("Config says CrawlDB State db path is: "+databasePath);
     
            
      // load db state ... 
      _serverState = null;
      
      if (_serverState == null) { 
        
        _serverState = new CrawlDBState();
        _serverState.setDbCookie(0);
        _serverState.setLastCrawlSegmentId(0);
      }
      
      LOG.info("Parsing Crawlers File");
      parseCrawlersFile();
      LOG.info("Successfully Parsed Crawlers File. Known Crawlers are:");
      for (OnlineCrawlerState crawler : _crawlers.values()) { 
        LOG.info(crawler.toString());
      }
      LOG.info("History Servers are:");
      for (OnlineHistoryServerState historyServer: _historyServers.values()) { 
        LOG.info(historyServer.toString());
      }
      
      // create server channel ... 
      AsyncServerChannel channel = new AsyncServerChannel(this, getEventLoop(), getServerAddress(),null);
      // register RPC services it supports ... 
      registerService(channel,CrawlDBService.spec);
      // open the server channel ..
      channel.open();

      // and start heartbeat timer ... 
      setCrawlerHeartbeatTime();
    }
    catch (RecordStoreException e){
      LOG.fatal(CCStringUtils.stringifyException(e));
      return false;
    }
    catch (IOException e) { 
      LOG.fatal(CCStringUtils.stringifyException(e));
      return false;
    }
    
    getWebServer().addServlet("modifyCrawlMasterState", "/modifyMasterCrawlState.jsp",Servlets.ModifyMasterCrawlState.class);
    getWebServer().addServlet("modifyCrawlNumber", "/modifyCrawlNumber.jsp",Servlets.ModifyCrawlNumber.class);
    getWebServer().addServlet("setHistoryServerTranstionState", "/setHistoryServerTransitionState.jsp",Servlets.SetHistoryServerTransitionState.class);
    getWebServer().addServlet("getCrawlerNames","/getCrawlerNames.jsp",Servlets.GetCrawlerNamesServlet.class);
    return true;

  }
  
  /** get list of crawler names **/
  public Vector<String> getCrawlerNames() { 
    Vector<String> crawlerNames = new Vector<String>();
    for (OnlineCrawlerState crawlerState : _crawlers.values()) { 
      crawlerNames.add(crawlerState.getHostname());
    }
    return crawlerNames;
  }
  
  public final int getActiveGeneratedListId() { 
    return _serverState.getActiveGeneratedListId();
  }
  
  public final int getCrawlerCrawlNumber() { 
    return Math.max(_serverState.getCrawlerCrawlNumber(),1);
  }
  
  public void setCrawlerCrawlNumber(int newCrawlNumber) throws IOException{ 
    _serverState.setCrawlerCrawlNumber(newCrawlNumber);
    updateServerState(true);
  }

  public void setHistoryServerCrawlNumber(int newCrawlNumber) throws IOException{ 
    if (_serverState.getHistoryServerCrawlNumber() != newCrawlNumber) { 
      _serverState.setHistoryServerCheckpointState(CrawlHistoryStatus.CheckpointState.ACTIVE);      
    }
    _serverState.setHistoryServerCrawlNumber(newCrawlNumber);
    updateServerState(true);
  }
  
  public void setHistoryServerTransitionState() throws IOException{ 
    if (_serverState.getHistoryServerCheckpointState() == CrawlHistoryStatus.CheckpointState.ACTIVE) { 
      _serverState.setHistoryServerCheckpointState(CrawlHistoryStatus.CheckpointState.TRANSITIONING);      
    }
    updateServerState(true);
  }
  
  public final int getHistoryServerCrawlNumber() { 
    return Math.max(_serverState.getHistoryServerCrawlNumber(),1);
  }

  public final int getHistoryServerCheckpointState() { 
    return _serverState.getHistoryServerCheckpointState();
  }


  public final int getNextListId() throws RecordStoreException { 
    // extract next segment id from database state 
    int nextListId = _serverState.getLastUsedListId() + 1;
    // update state ... 
    _serverState.setLastUsedListId(nextListId);

    updateServerState(true);

    return nextListId;
  }
  
  public final int getNextCrawlSegmentId() throws RecordStoreException { 
    // extract next segment id from database state 
    int nextSegmentId = _serverState.getLastCrawlSegmentId() + 1;
    
    // update state ... 
    _serverState.setLastCrawlSegmentId(nextSegmentId);
    updateServerState(true);

    return nextSegmentId;
  }

  public final void updateLastCrawlSegmentId(int segmentId) throws RecordStoreException  { 
    // update state ... 
    _serverState.setLastCrawlSegmentId(segmentId);
    updateServerState(true);
  }
   
  private final void potentiallyStartMapReduceJob() { 
    
  }
   
  
  private void potentiallyTransitionCrawlState() { 
    
    int desiredCrawlState = -1;
    
    switch (_serverState.getCrawlMasterState()) { 
      case CrawlDBState.CrawlMasterState.ACTIVE: { 
        desiredCrawlState = CrawlerStatus.CrawlerState.ACTIVE;
      }
      break;
      
      case CrawlDBState.CrawlMasterState.CHECKPOINTING: {
        desiredCrawlState = CrawlerStatus.CrawlerState.FLUSHED;
      }
      break;
      
      case CrawlDBState.CrawlMasterState.CHECKPOINTED: 
      case CrawlDBState.CrawlMasterState.GENERATING:
      case CrawlDBState.CrawlMasterState.GENERATED: { 
        desiredCrawlState = CrawlerStatus.CrawlerState.ACTIVE;
      }
      break;
      
      case CrawlDBState.CrawlMasterState.READY_TO_DISTRIBUTE: { 
        desiredCrawlState = CrawlerStatus.CrawlerState.PURGED;
      }
      break;
    }
    
    boolean allCrawlersInDesiredState = true; 
    
    if (desiredCrawlState != -1) { 
      for (OnlineCrawlerState crawler : _crawlers.values()) {
        if (crawler.getLastKnownStatus().getCrawlerState() != desiredCrawlState || 
            crawler.getLastKnownStatus().getActiveListNumber() != getCrawlerCrawlNumber()) {
          
          allCrawlersInDesiredState = false;
          crawler.transitionToState(desiredCrawlState, getCrawlerCrawlNumber());
        }
      }
    }
    
    // ok now are all the crawlers in the desired state ... 
    if (allCrawlersInDesiredState && desiredCrawlState != -1) { 
      potentiallyTransitionMasterCrawlState();
    }
  }
  
  /** explicitly set crawl state **/
  void explicitlySetMasterCrawlState(int newState) { 
    _serverState.setCrawlMasterState(newState);
  }
  
  private void potentiallyCheckpointHistoryServers() { 
   for (OnlineHistoryServerState historyServer : _historyServers.values()) { 
     if (historyServer.isOnline() && historyServer.getLastKnownStatus().getActiveCrawlNumber() != getHistoryServerCrawlNumber() 
         || historyServer.getLastKnownStatus().getCheckpointState() < getHistoryServerCheckpointState()) { 
       if (!historyServer.isCommandActive()) { 
         LOG.info("Sending Checkpoint Command to History Server:" + historyServer.getHostname() + " CrawlNumber:" + getHistoryServerCrawlNumber());
         historyServer.sendCheckpointCommand(getHistoryServerCrawlNumber(),getHistoryServerCheckpointState());
       }
     }
   }
  }
  
  /** gets called when all crawlers are in a suitable crawl state for a master state transition 
   * 
   */
  private void potentiallyTransitionMasterCrawlState() { 
    // see if we can transition master state ... 
    switch (_serverState.getCrawlMasterState()) { 
      case CrawlDBState.CrawlMasterState.CHECKPOINTING: {
        LOG.info("Crawl State is CHECKPOINTING. TRANSITIONING TO CHECKPOINTED");
        _serverState.setCrawlMasterState(CrawlDBState.CrawlMasterState.CHECKPOINTED);
        //TODO: parse remaining segments and transition to generating state  
      }
      break;
      
      
      case CrawlDBState.CrawlMasterState.GENERATED: { 
        LOG.info("Crawl State is GENERATED. TRANSITIONING TO READY_TO_DISTRIBUTE");
        _serverState.setCrawlMasterState(CrawlDBState.CrawlMasterState.READY_TO_DISTRIBUTE);
      }
      break;
      
      case CrawlDBState.CrawlMasterState.READY_TO_DISTRIBUTE: { 
        LOG.info("Crawl State is READY TO DISTRIBUTE. TRANSITIONING TO DISTRIBUTING");
        _serverState.setCrawlMasterState(CrawlDBState.CrawlMasterState.DISTRIBUTING);
      }
      break;
      
      case CrawlDBState.CrawlMasterState.DISTRIBUTED: { 
        LOG.info("Crawl State is READY TO DISTRIBUTING. TRANSITIONING TO ACTIVE");
        _serverState.setCrawlMasterState(CrawlDBState.CrawlMasterState.ACTIVE);
      }
      break;
    }
  }
  
  /** setCrawlerHeartbeatTime **/
  private void setCrawlerHeartbeatTime() { 

    // setup async timer ... 
    _crawlerHeartbeatTimer = new Timer(CRAWLER_HEARTBEAT_TIMER_INTERVAL,true,new Timer.Callback() {

      public void timerFired(Timer timer) {
        
        //LOG.info("Heartbeat Timer Fired");
        Date now = new Date();
        
        // walk online crawlers and send heartbeats as appropriate ... 
        for (OnlineCrawlerState crawler : _crawlers.values()) { 
          
          if (crawler.isOnline()) { 
            if (now.getTime() - crawler.getLastUpdateTime().getTime() >= CRAWLER_HEARTBEAT_THRESHOLD) { 
              // time for a heartbeat ... 
              crawler.sendHeartbeat();
            }
          }
        }
        // walk online crawlers and send heartbeats as appropriate ... 
        for (OnlineHistoryServerState historyServer : _historyServers.values()) { 
          
          if (historyServer.isOnline()) { 
            if (now.getTime() - historyServer.getLastUpdateTime().getTime() >= CRAWLER_HEARTBEAT_THRESHOLD) { 
              // time for a heartbeat ... 
              historyServer.sendHeartbeat();
            }
          }
        }
        
        // figure out if history servers need to be checkpointed ... 
        potentiallyCheckpointHistoryServers();
        
        // figure out of a crawl state transition needs to happen 
        potentiallyTransitionCrawlState();
        
        // now check for potential map reduce job transition ... 
        potentiallyStartMapReduceJob();
      } 
    });
    getEventLoop().setTimer(_crawlerHeartbeatTimer);
  }
  
 
  /** persist the crawldb state to disk **/
  private final void updateServerState(boolean update) throws RecordStoreException  { 
  }  

////////////////////////////////////////////////////////////////////////////////
//CommonCrawlServer Overloads 
////////////////////////////////////////////////////////////////////////////////
  
  
  //@Override
  protected String   getDefaultLogFileName() { 
    return "crawldb";
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
    return CrawlEnvironment.DEFAULT_DATABASE_HTTP_PORT;
  }

  @Override
  protected String getDefaultRPCInterface() {
    return CrawlEnvironment.DEFAULT_RPC_INTERFACE;
  }

  @Override
  protected int getDefaultRPCPort() {
    return CrawlEnvironment.DEFAULT_DATABASE_RPC_PORT;
  }

  @Override
  protected String getWebAppName() {
    return CrawlEnvironment.CRAWLMASTER_WEBAPP_NAME;
  }
  
  
  @Override
  protected boolean parseArguements(String[] argv) {

    for(int i=0; i < argv.length;++i) {
      if (argv[i].equalsIgnoreCase("--crawlers")) { 
        if (i+1 < argv.length) { 
          _crawlersFile = argv[++i];
        }
      }
      else if (argv[i].equalsIgnoreCase("--awsAccessKey")) { 
        if (i+1 < argv.length) { 
          _s3AccessKey = argv[++i];
        }
      }
      else if (argv[i].equalsIgnoreCase("--awsSecret")) { 
        if (i+1 < argv.length) { 
          _s3Secret = argv[++i];
        }
      }
      else if (argv[i].equalsIgnoreCase("--awsBucket")) { 
        if (i+1 < argv.length) { 
          _s3Bucket = argv[++i];
        }
      }
      
    }
    return true;
  }

  @Override
  protected void printUsage() {
    System.out.println("Database Startup Args: --dataDir [data directory]");
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
  
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// CALLBACKS   
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  
  
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//MASTER INTEGRATION CODE 
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /** make packed log id from list id and segment log id **/
  public static long makeSegmentLogId(int listId,int segmentId) { 
    return (((long)listId) << 32) | (long)segmentId;
  }

  /** get segment log id from packed id**/
  public static int getSegmentIdFromLogId(long logId) { 
    return (int) (logId & 0xFFFFFFFF);
  }
  /** get list id from packed id**/
  public static int getListIdFromLogId(long logId) { 
    return (int) ((logId >> 32) & 0xFFFFFFFF);
  }
  
  ///////////////////////////////////////////////////////
  /* Online Crawler State Object */
  ///////////////////////////////////////////////////////

  //////////////////////////////////////////////////////////////////////////////////////////////////
  // HTML CONSOLE SUPPORT ROUTINES ... 
  ////////////////////////////////////////////////////////////////////////////////////////////////
  static String timeValueToString(long timeValue) { 
    if (timeValue == -1) { 
      return "Undefined";
    }
    else { 
      Date theDate = new Date(timeValue);
      SimpleDateFormat formatter = new SimpleDateFormat("yyyy.MM.dd G 'at' hh:mm:ss z");
      return formatter.format(theDate);
    }
  }
  
  
  void flipBooleanValue(final String valueName) { 

    final Semaphore waitState = new Semaphore(0);
    
    _eventLoop.setTimer(new Timer(0,false,new Timer.Callback() {

      public void timerFired(Timer timer) {
        
        if (valueName.equals("_disableUpdater")) { 
          _disableUpdater = !_disableUpdater;
        }
        else if (valueName.equals("_enableS3Uploader")) { 
          _enableS3Uploader = !_enableS3Uploader;
          
          if (_enableS3Uploader && _uploader == null) { 
            try {
              Path arcFileInTransitPath  = new Path(CrawlEnvironment.CC_ROOT_DIR  + "/arc_files_in_transit");
              Path arcFileSourcePath  = new Path(CrawlEnvironment.CC_ROOT_DIR  + "/arc_files_out");
              
              if (_s3AccessKey == null || _s3Secret == null || _s3Bucket == null) { 
                throw new IOException("Invalid S3 AccessKey/Secert/Bucket");
              }
              
              intializeS3Uploader(
                  arcFileSourcePath,
                  arcFileInTransitPath,
                  _s3Bucket,
                  _s3AccessKey,
                  _s3Secret,
                  2*1024*1024,
                  150,
                  new UploaderController() {
                    
                    @Override
                    public boolean continueUpload() {
                      return _enableS3Uploader;
                    }
                  }
              );
                  
                  
            }
            catch (IOException e) { 
              LOG.error("Failed to INITIALIZE S3 Uploader with Exception:" + CCStringUtils.stringifyException(e));
            }
          }
        }
        
        waitState.release();
      } 
     }));
      waitState.acquireUninterruptibly();
  }
  
  public void writeCrawlDBServerStateTable(final JspWriter out) throws IOException { 
    final Semaphore waitState = new Semaphore(0);
    
    _eventLoop.setTimer(new Timer(0,false,new Timer.Callback() {

      public void timerFired(Timer timer) {
    
        
        
        try {
          
          out.print("<table border=\"1\"cellpadding=\"2\" cellspacing=\"2\">");
          out.print("<tr><td>Variable</td><td>Value</td><td>Action</td></tr>");
          // out.print("<tr><td>FileSystem</td><td>"+CrawlEnvironment.getDefaultFileSystem().getUri() +"</td><td></td></tr>");
          if (_hadoopStatus != HadoopStatus.RUNNING_CUSTOM_JOB)          
            out.print("<tr><td>HadoopStatus</td><td>"+_hadoopStatus.toString()+"</td><td>&nbsp;</td></tr>");
          else 
          out.print("<tr><td>CrawlStatus</td><td>"+CrawlMasterState.toString(_serverState.getCrawlMasterState())+"</td><td>&nbsp;</td></tr>");
          out.print("<tr><td>CrawlerCrawlNumber</td><td>"+getCrawlerCrawlNumber()+"</td><td>&nbsp;</td></tr>");
          out.print("<tr><td>HistoryServerCrawlNumber</td><td>"+getHistoryServerCrawlNumber()+"</td><td>&nbsp;</td></tr>");
          out.print("<tr><td>HistoryServerCheckpointState</td><td>"+CrawlHistoryStatus.CheckpointState.toString(getHistoryServerCheckpointState())+"</td><td>&nbsp;</td></tr>");
          out.print("<tr><td>LastGeneratorStatus</td><td>"+_lastCrawlSegmentGenStatus.toString()+"</td><td>&nbsp;</td></tr>");
          out.print("<tr><td>LastUpdateTime</td><td>"+timeValueToString(_lastUpdateTime)+"</td><td>&nbsp;</td></tr>");
          out.print("<tr><td>DisableUpdater</td><td>"+_disableUpdater+"</td><td><a href=\"changeValue.jsp?name=_disableUpdater\">Flip Value</a></td></tr>");
          out.print("<tr><td>EnableS3Uploader</td><td>"+_enableS3Uploader+"</td><td><a href=\"changeValue.jsp?name=_enableS3Uploader\">Flip Value</a></td></tr>");
          
          out.print("</table>");
          
        }
        catch (IOException e) { 
          
          try {
            out.print("<pre> writeCrawlDBServerStateTable threw exception: " + CCStringUtils.stringifyException(e) + " </pre>");
          } catch (IOException e1) {
            LOG.error(e1);
          }
        }
        waitState.release();
      } 
     }));
    
      waitState.acquireUninterruptibly();
  }
  
    
  public void dumpCrawlerTable(final JspWriter out) throws IOException {
    
    final Semaphore waitState = new Semaphore(0);
    
    _eventLoop.setTimer(new Timer(0,false,new Timer.Callback() {

      public void timerFired(Timer timer) {
    
        try {
          
          out.print("<table border=\"1\"cellpadding=\"2\" cellspacing=\"2\">");
          out.print("<tr><td>Crawler Name</td><td>Online State</td><td>Crawler Status</td><td>Active Crawler Number</td></tr>");
          
          for (OnlineCrawlerState state : _crawlers.values()) { 
            out.print("<tr><td><a href=\"crawlerDetails.jsp?crawlerNode=" +state.getHostname() +"\">"+ state.getHostname()+"</a></td>");
            if (state.isOnline()) { 
              out.print("<td>online</td>");
            }
            else { 
              out.print("<td>offline</td>");
            }
            if (state.isOnline()) { 
              out.print("<TD>" + CrawlerStatus.CrawlerState.toString(state.getLastKnownStatus().getCrawlerState()) +"</TD>");
              out.print("<TD>" + Integer.toString(state.getLastKnownStatus().getActiveListNumber()) +"</TD>");
            }
            else { 
              out.print("<TD>&nbsp;</td>");
              out.print("<TD>&nbsp;</td>");
            }
            
            out.print("</tr>");
          }
          
          out.print("</table>");
        }
        catch (IOException e) { 
          
          try {
            out.print("<pre> dumpCrawlerTable threw exception: " + CCStringUtils.stringifyException(e) + " </pre>");
          } catch (IOException e1) {
            LOG.error(e1);
          }
        }
        waitState.release();
      } 
     }));
    
      waitState.acquireUninterruptibly();
  }

  public void dumpHistoryServerTable(final JspWriter out) throws IOException {
    
    final Semaphore waitState = new Semaphore(0);
    
    _eventLoop.setTimer(new Timer(0,false,new Timer.Callback() {

      public void timerFired(Timer timer) {
    
        try {
          
          out.print("<table border=\"1\"cellpadding=\"2\" cellspacing=\"2\">");
          out.print("<tr><td>Server Name</td><td>Online State</td><td>Active Crawler Number</td><td>Checkpoint State</td></tr>");
          
          for (OnlineHistoryServerState state : _historyServers.values()) { 
            out.print("<tr><td>" + state.getHostname() +"</td>");
            if (state.isOnline()) { 
              out.print("<td>online</td>");
            }
            else { 
              out.print("<td>offline</td>");
            }
            if (state.isOnline()) { 
              out.print("<TD>" + Integer.toString(state.getLastKnownStatus().getActiveCrawlNumber()) +"</TD>");
              out.print("<TD>" + CrawlHistoryStatus.CheckpointState.toString(state.getLastKnownStatus().getCheckpointState()) +"</TD>");
            }
            else { 
              out.print("<TD>&nbsp;</td>");
            }
            
            out.print("</tr>");
          }
          
          out.print("</table>");
        }
        catch (IOException e) { 
          
          try {
            out.print("<pre> dumpHistoryTable threw exception: " + CCStringUtils.stringifyException(e) + " </pre>");
          } catch (IOException e1) {
            LOG.error(e1);
          }
        }
        waitState.release();
      } 
     }));
    
      waitState.acquireUninterruptibly();
  }
  
  
  void dumpCrawlerState(final JspWriter out, final String crawlerName) { 
    
    final Semaphore waitState = new Semaphore(0);
    
    _eventLoop.setTimer(new Timer(0,false,new Timer.Callback() {

      public void timerFired(Timer timer) {
        waitState.release();
      } 
     }));
    
      waitState.acquireUninterruptibly();
   
  }
 
  
  static class FileSystemSize { 
    public long fileSize;
    public long blockSize;
    public Vector<String> paths = new Vector<String>();
  };
  
  static FileSystemSize buildRecursiveFileStatus(FileStatus statusIn,FileSystem fileSystem)throws IOException { 
    
    FileSystemSize sizeOut = new FileSystemSize();

    sizeOut.fileSize =statusIn.getLen();
    sizeOut.blockSize =statusIn.getBlockSize();
    
    if (statusIn.isDir()) { 

      FileStatus[] nestedStatus = fileSystem.globStatus(new Path(statusIn.getPath(),"*"));
      
      if (nestedStatus != null) { 
       
        for (int i=0;i<nestedStatus.length;++i){

          if (nestedStatus[i].isDir()) { 

            FileSystemSize nestedSize = buildRecursiveFileStatus(nestedStatus[i],fileSystem);

            sizeOut.fileSize += nestedSize.fileSize;
            sizeOut.blockSize += nestedSize.blockSize;
            sizeOut.paths.add(nestedStatus[i].getPath().toString() + " " + formatNumber(nestedSize.fileSize) + " " + formatNumber(nestedSize.blockSize) );
            sizeOut.paths.addAll(nestedSize.paths);
          }
          else { 
            sizeOut.fileSize += nestedStatus[i].getLen();
            sizeOut.blockSize += nestedStatus[i].getLen() + (nestedStatus[i].getLen() % nestedStatus[i].getBlockSize());
          }
        }
      }
    }
    return sizeOut;
  }
  

  private static String formatNumber(long number) {
    
    double numberOut = (double)number;
    String unit = "";
    if (number >= 1000000000000L) { 
      numberOut = ((double)number) / 1000000000000.0;
      unit = "TB";
    }
    else if (number >= 1000000000) { 
      numberOut = ((double)number) / 1000000000.0;
      unit = "GB";
    }
    else if (number >= 1000000) { 
      numberOut = ((double)number) / 1000000.0;
      unit = "MB";
    }
    return ((Double)numberOut).toString() + unit;
  }
  
  public void generateDiskUsageReport(final JspWriter out) {

    AsyncWebServerRequest webRequest = new AsyncWebServerRequest("dumpStats",out) {

      
      @Override
      public boolean handleRequest(Semaphore completionSemaphore)throws IOException { 
        
        FileSystem hdfs = CrawlEnvironment.getDefaultFileSystem();
                
        String paths[] = { "crawl/crawldb/current",
                                 "crawl/header_db",
                                 "crawl/domain_db",
                                 "crawl/purge_db",
                                 "crawl/crawl_segments",
                                 "crawl/parse_segments"
        };
        
        out.println("<table border=1>");
        out.println("<tr><td>Path</td><td>File Size</td><td>Block Size</td></tr>");
        
        Vector<String> cumilativePaths = new Vector<String>();
        try { 
          
          for (String path : paths) { 
            
            FileStatus pathStatus  = hdfs.getFileStatus(new Path(path));
            FileSystemSize size    = buildRecursiveFileStatus(pathStatus, hdfs);
            
            cumilativePaths.add("<b>" + path + " " + formatNumber(size.fileSize) + " " + formatNumber(size.blockSize) + "</b>" );
            cumilativePaths.addAll(size.paths);
            out.println("<tr><td>" + path +"<td>"+ formatNumber(size.fileSize) + "<td>" + formatNumber(size.blockSize) + "</tr>");
            
            LOG.info("Path:" + path + "FileSize:" + formatNumber(size.fileSize) + " BlockSize:" + formatNumber(size.blockSize));
            
          }
          
          out.println("</table>");
          
          out.println("<pre>");
          for (String path : cumilativePaths) { 
            out.println(path);
          }
          out.println("</pre>");
        }
        catch (Exception e) { 
          LOG.error(CCStringUtils.stringifyException(e));
          out.println("</table><pre>");
          out.println(CCStringUtils.stringifyException(e));
          out.println("</pre>");
        }
        
        return false;
      }
    };
    webRequest.dispatch(_eventLoop);
    webRequest = null;
  }

  static final String jobPackageCandidates[] = { 
  	"org.commoncrawl.crawl.database.reports",
  	"org.commoncrawl.crawl.database.cleanupjobs",
  	"org.commoncrawl.crawl.database.crawlpipeline",
  	"org.commoncrawl.crawl.database.tests",
  	"org.commoncrawl.crawl.database.utilities"
  };
  
  public static Class findCustomJobClass(String jobName) { 
    for (int i=0;i<jobPackageCandidates.length;++i) { 
      try { 
        String fullyQualifiedName = jobPackageCandidates[i] + "." + jobName;
        
        Class theClass = Class.forName(fullyQualifiedName);
        
        if (theClass != null) { 
          return theClass;
        }
        
      }
      catch (ClassNotFoundException e) {
        //LOG.error(CCStringUtils.stringifyException(e));
      }
    }
    return null;
  }
  

  /*
  public Vector<CrawlerState> getCrawlerStates() { 

    final Vector<CrawlerState> crawlerStates = new Vector<CrawlerState>();
    AsyncWebServerRequest webRequest = new AsyncWebServerRequest("getCrawlSegments",null) {

      @Override
      public boolean handleRequest(Semaphore completionSemaphore)throws IOException { 
        // load crawler states ...
        crawlerStates.addAll(_crawlerStateMap.values());
        
        return false;
      }
    };
    
    webRequest.dispatch(_eventLoop);
    webRequest = null;
    
   
    return crawlerStates;
  }
  */
  
  public static String crawlDBSegmentDescFromCode(int statusCode) { 
    
    String status = "UNKNOWN";
    
    switch (statusCode) { 
        case CrawlDBSegment.Status.BAD: status = "BAD";break;
        case CrawlDBSegment.Status.GENERATING: status = "GENERATING";break;
        case CrawlDBSegment.Status.GENERATED: status = "GENERATED";break;
        case CrawlDBSegment.Status.PENDING: status = "PENDING";break;
        case CrawlDBSegment.Status.PARSING: status = "PARSING";break;
        case CrawlDBSegment.Status.PARSED: status = "PARSED";break;
        case CrawlDBSegment.Status.MERGED: status = "MERGED";break;
        case CrawlDBSegment.Status.TRANSFERRING: status = "TRANSFERRING";break;
        case CrawlDBSegment.Status.TRANSFERRED: status = "TRANSFERRED";break;
    }
    return status;
  }
  
  
  private static SimpleDateFormat S3_TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy/MM/dd/");

  private String hdfsNameToS3ArcFileName(String arcFileName) { 
    int partDelimIndex = arcFileName.indexOf("_");
    int extensionDelimIdx = arcFileName.indexOf('.', partDelimIndex);
    long date = Long.parseLong(arcFileName.substring(0,partDelimIndex));
    int   partNo = Integer.parseInt(arcFileName.substring(partDelimIndex + 1, extensionDelimIdx));
    return S3_TIMESTAMP_FORMAT.format(new Date(date))  +partNo + "/" +  arcFileName;
  }
  
  public static interface UploaderController { 
    public boolean continueUpload();
  }
  
  private void intializeS3Uploader(final Path arcFileSourcePath,final Path arcFileInTransitPath,
      String s3Bucket,String s3AccessId, String s3SecretKey,
      int bandwidthPerUploader,int maxUploaders,final UploaderController controller) throws IOException { 

    if (_uploader == null) { 

     LOG.info("Initializing Bulk Uploader ... ");
    
     final FileSystem fileSystem = CrawlEnvironment.getDefaultFileSystem();
     //final Path arcFileInTransitPath  = new Path(CrawlEnvironment.HDFS_CrawlDBBaseDir  + "/arc_files_in_transit");
     //final Path arcFileSourcePath  = new Path(CrawlEnvironment.HDFS_CrawlDBBaseDir  + "/arc_files_out");
     final TreeSet<Path> candidateList = new TreeSet<Path>();
     
     if (!fileSystem.exists(arcFileInTransitPath)) { 
       fileSystem.mkdirs(arcFileInTransitPath);
     }
     
     FileStatus orphanedItems[] = fileSystem.globStatus(new Path(arcFileInTransitPath,"*"));
     
     for (FileStatus orphanedItem : orphanedItems) { 
       LOG.info("Moving orphaned arc file:" + orphanedItem.getPath().getName() + " to queue directory");
       boolean result = fileSystem.rename(orphanedItem.getPath(), new Path(arcFileSourcePath,orphanedItem.getPath().getName()));
       if (!result) { 
         LOG.error("FAILED to move orphaned arc file:" + orphanedItem.getPath().getName() + " to queue directory");
       }
     }
     
     _uploader = new S3BulkUploader(
         _eventLoop,CrawlEnvironment.getDefaultFileSystem(),
           
         new S3BulkUploader.Callback() {
           
           public UploadCandidate getNextUploadCandidate() {
             
             if (candidateList.size() == 0) { 
               //     LOG.info("S3 Uploader CandidateList is empty. Rescaning Directory");
               try { 
                 FileStatus candidateItems[] = fileSystem.globStatus(new Path(arcFileSourcePath,"*"));
                 for (FileStatus candidate : candidateItems) { 
                   candidateList.add(candidate.getPath());
                 }
               }
               catch (IOException e) { 
                 LOG.error("Faield to build S3 Upload CandidateList with Exception:" + CCStringUtils.stringifyException(e));
               }
             }
             
             if (controller.continueUpload() && candidateList.size() != 0) { 
               // get first available candidate ... 
               Path candidateName = candidateList.first();
               candidateList.remove(candidateName);
               // move it to staging ... 
               Path stagingPathName = new Path(arcFileInTransitPath,candidateName.getName());
               try { 
                 
                 fileSystem.rename(candidateName, stagingPathName);
               
                 LOG.info("Queuing S3 Upload Candidate:" + stagingPathName.toString() + " S3Name:" + hdfsNameToS3ArcFileName(stagingPathName.getName()));
                 return new UploadCandidate(stagingPathName,hdfsNameToS3ArcFileName(stagingPathName.getName()),"application/x-gzip",s3ACL);
               }
               catch (IOException e) { 
                 candidateList.add(candidateName);
                 LOG.error("Failed to Move S3 Upload Candidate from Source:" + candidateName.toString() + " to Staging:" + stagingPathName.toString() + " with Exception:" + CCStringUtils.stringifyException(e));
               }
               catch (Exception e) { 
                 LOG.error("Failed to Move S3 Upload Candidate from Source:" + candidateName.toString() + " to Staging:" + stagingPathName.toString() + " with Exception:" + CCStringUtils.stringifyException(e));
               }
             }
             return null; 
           }

           public void uploadComplete(Path path, String bandwidthStats) {
             LOG.info("Upload Complete for arc file:" + path.getName() + " Bandwidth Stats:" + bandwidthStats);
             LOG.info("Deleting arc file:" + path);
             try { 
               fileSystem.delete(path,false);
             }
             catch (IOException e) { 
               LOG.error("Failed to Delete uploaded S3 ARC File:" + path + " with Exception:" + CCStringUtils.stringifyException(e));
             }
           }

           public void uploadFailed(Path path, IOException e) {
             if (e != null)
               LOG.error("Upload Failed for:" + path.getName() + " with Exception:" + CCStringUtils.stringifyException(e));
             else 
               LOG.error("Upload Failed for:" + path.getName() + " with NUL Exception");
             // move from staging to queued ... 
             Path candidatePathName = new Path(arcFileSourcePath,path.getName());
             try {
               LOG.error("Moving Path:" + path+ " to:" + candidatePathName);
               fileSystem.rename(path, candidatePathName);
               LOG.error("Done Moving Path:" + path+ " to:" + candidatePathName);
               // and add back to set ... 
               candidateList.add(candidatePathName);
               LOG.error("Added Path:"+ candidatePathName + " to candidateList");
             }
             catch (IOException e2) { 
               LOG.error("Failed to move FAILED upload S3 Candidate from staging:" + path +" to source:" + candidatePathName + " with Exception:" + CCStringUtils.stringifyException(e2));
             }
           } 
         },
         s3Bucket,
         s3AccessId,
         s3SecretKey,
         bandwidthPerUploader,
         maxUploaders
         );
     
     _uploader.startUpload();
     
   }
  }

  /** upgrade parse segment status for all segments that match from status to the passed in toStatus value
   *  
   * @param fromStatus
   * @param toStatus
   * @return A list of segments affected by the operation
   */
  
  /** purge all map-reduce task values related to a job **/
  public void purgeTaskValuesForJob(long jobId) { 
    synchronized (_taskStateMap) {
      _taskStateMap.removeAll(Long.toString(jobId));
    }
  }

  @Override
  public void purgeMapReduceTaskValue(
      AsyncContext<MapReduceTaskIdAndData, NullMessage> rpcContext)
      throws RPCException {
 
    final String jobId = rpcContext.getInput().getJobId();
    final String taskId = rpcContext.getInput().getTaskId();
    
    // LOG.info("Received purgeMapReduceTaskValue request for job:" + jobId + " taskId:" + taskId);
    
    synchronized (_taskStateMap) {
      // get candidate list based on job id ... 
      Iterator<MapReduceTaskIdAndData> iterator= _taskStateMap.get(jobId).iterator();
      
      
      while (iterator.hasNext()) { 
        MapReduceTaskIdAndData item = iterator.next();
        // if task id matches, remove this item .. .
        if (item.getTaskId().equals(taskId)) { 
          iterator.remove();
        }
      }
    }
    
    rpcContext.completeRequest();
  }

  @Override
  public void queryMapReduceTaskValue(AsyncContext<MapReduceTaskIdAndData, MapReduceTaskIdAndData> rpcContext) throws RPCException {

    final String jobId = rpcContext.getInput().getJobId();
    final String taskId = rpcContext.getInput().getTaskId();
    
    // LOG.info("Received queryMapReduceTaskValue request for job:" + jobId + " taskId:" + taskId + " key:" + rpcContext.getInput().getDataKey());
    
    rpcContext.getOutput().setJobId(jobId);
    rpcContext.getOutput().setTaskId(taskId);
    rpcContext.getOutput().setDataKey(rpcContext.getInput().getDataKey());
    
    synchronized (_taskStateMap) {
      
      Iterator<MapReduceTaskIdAndData> taskItems = Iterables.filter(_taskStateMap.get(jobId),new Predicate<MapReduceTaskIdAndData>() {
  
        @Override
        public boolean apply(MapReduceTaskIdAndData input) {
          return input.getTaskId().equals(taskId);
        }
      }).iterator();
      
      if (taskItems.hasNext()) { 
        rpcContext.getOutput().setDataValue(taskItems.next().getDataValue());
      }
    }
    
    rpcContext.completeRequest();
  }

  @Override
  public void updateMapReduceTaskValue(AsyncContext<MapReduceTaskIdAndData, NullMessage> rpcContext)throws RPCException {

    final String jobId = rpcContext.getInput().getJobId();
    final String taskId = rpcContext.getInput().getTaskId();
    
    // LOG.info("Received queryMapReduceTaskValue request for job:" + jobId + " taskId:" + taskId + " key:" + rpcContext.getInput().getDataKey() + " value:" + rpcContext.getInput().getDataValue());
    
    synchronized (_taskStateMap) {    
    
      Iterator<MapReduceTaskIdAndData> taskItems = _taskStateMap.get(jobId).iterator();
      
      while(taskItems.hasNext()) { 
        MapReduceTaskIdAndData item = taskItems.next();
        if (item.getTaskId().equals(taskId)) { 
          taskItems.remove();
        }
      }
      
      _taskStateMap.get(jobId).add(rpcContext.getInput());
    }
    
    rpcContext.completeRequest();    
  }
  
  void parseCrawlersFile()throws IOException {
    
    LOG.info("Loading Servers File from:" + _crawlersFile);
    InputStream stream =null;

    URL resourceURL = CrawlEnvironment.getHadoopConfig().getResource(_crawlersFile);
    
    if (resourceURL != null) {
      stream = resourceURL.openStream();
    }
    // try as filename 
    else { 
      LOG.info("Could not load resource as an URL. Trying as an absolute pathname");
      stream = new FileInputStream(new File(_crawlersFile));
    }
    
    if (stream == null) { 
      throw new FileNotFoundException();
    }
         
    BufferedReader reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(stream)));

    String crawlerDetailLine = null;
    String crawlerName = null;
    InetAddress crawlerIPAddress;
    int crawelerPort;
    
    int serverCount = 0;
    
    LOG.info("Loading servers file");
    while ((crawlerDetailLine = reader.readLine()) != null) {
      if (!crawlerDetailLine.startsWith("#")) {
        LOG.info("Got Crawler Line:" + crawlerDetailLine);
        String items[] = crawlerDetailLine.split(";");
        if (items.length == 3) { 
          crawlerName = items[0];
          
          InetSocketAddress crawlerAddress =CCStringUtils.parseSocketAddress(items[1]);
          // and create a paired address for the related history server ... 
          InetSocketAddress historyServerAddress = CCStringUtils.parseSocketAddress(items[2]);
          
          if (_crawlers.get(crawlerName) != null || crawlerAddress == null) { 
            LOG.error("Duplicate Crawler Name or Invalid Crawler Name Detected for Entry:" + crawlerDetailLine);
          }
          else { 
            // add a reference to the crawler instance  
            _crawlers.put(crawlerName, new OnlineCrawlerState(this,crawlerName,crawlerAddress));
            // and a reference to its related history server instance 
            _historyServers.put(crawlerName, new OnlineHistoryServerState(this,crawlerName,historyServerAddress));
          }
        }
        else { 
          LOG.error("Crawler Line did not parse into proper components. Got component count:" + items.length);
        }
      }
    }
  }

	
  /**
   * Support for multi-phase dedup detection job
   * This method does a binary search against a preloaded set of fingerprints  of known duplicate items
   * and returns true if the passed in fingerprint is present in the set.
   */
	
	@Override
  public void queryDuplicateStatus(
      AsyncContext<URLFPV2, SimpleByteResult> rpcContext)
      throws RPCException {
		
		if (_duplicatesTable != null) { 
			try {
	      rpcContext.getOutput().setByteResult((byte)getDuplicateStatus(rpcContext.getInput()));
	      rpcContext.setStatus(Status.Success);
      } catch (IOException e) {
	      LOG.error(CCStringUtils.stringifyException(e));
	      rpcContext.setStatus(Status.Error_RequestFailed);
	      rpcContext.setErrorDesc(CCStringUtils.stringifyException(e));
      }
		}
		else { 
			rpcContext.setStatus(Status.Error_RequestFailed);
			rpcContext.setErrorDesc("Table Uninitialized!");
		}
		rpcContext.completeRequest();
  }
	
  byte[][]		_duplicatesTable = null;
  DataInputBuffer _lookupBuffer = new DataInputBuffer();
	DataOutputBuffer _lookupUpdater = new DataOutputBuffer();
	
	/**
	 * initialize duplicate data   
	 */
	public void initializeDuplicatesTable(Path duplicateDataTable)throws IOException {
	
		FileSystem hdfs = CrawlEnvironment.getDefaultFileSystem();
		
  	LOG.info("Duplicate Lookup Table at Path:" + duplicateDataTable);
  	
  	// find out how many parts there are 
  	FileStatus parts[] = hdfs.globStatus(new Path(duplicateDataTable.toString() + "/part-*"));

  	if (parts.length == 0) { 
  		throw new IOException("Invalid Lookup Table Path:" + duplicateDataTable);
  	}
  	// allocate lookup table array 
  	_duplicatesTable = new byte[parts.length][];
  	
  	// index
  	int index = 0;
  	
  	// load individual parts 
  	for (FileStatus part : parts) {
  		LOG.info("Loading Part:" + part.getPath());
  		
	    long lookupTableStreamLength = hdfs.getLength(part.getPath());
	    
	    if (lookupTableStreamLength <= 0) {
	    	LOG.error("Duplicate Lookup Table Invalid");
	    	throw new RuntimeException("Duplicate Lookup Table Invalid");
	    }
			
			
	    FSDataInputStream lookupTableStream = hdfs.open(part.getPath());
	    
	    try { 
	    	_duplicatesTable[index] = new byte[(int)lookupTableStreamLength];
	    	LOG.info("Loading Duplicates Table at Path:" + duplicateDataTable + " Size:" + lookupTableStreamLength);
	    	lookupTableStream.readFully(_duplicatesTable[index]);
	    	LOG.info("Loaded Duplicates Table at Path:" + duplicateDataTable + " Size:" + lookupTableStreamLength);
	    }
	    finally { 
	    	lookupTableStream.close();
	    }
	    
	    ++index;
  	}
	}
	
	/**
	 * release duplicate lookup table 
	 */
	public void resleaseDuplicatesTable() { 
	  _duplicatesTable = null;
	  _lookupBuffer = new DataInputBuffer();
	}
	
	/** check to see if a fingerprint is a duplicate 
	 * 
	 * @param targetFingerprint
	 * @return
	 * @throws IOException
	 */
	
	int duplicateRequestCounter = 0;
	
  public int getDuplicateStatus(URLFPV2 targetFingerprint)throws IOException {
  	duplicateRequestCounter++;
  	if (duplicateRequestCounter % 100000 == 0) {
  		System.out.println("Hit: " + duplicateRequestCounter + " duplicateQuery Requests. Pausing... ");
	  	try { 
	  		Thread.sleep(1000);
	  	}
	  	catch (InterruptedException e) { 
	  		
	  	}
  	}
  	
  	// shard the incoming fingerprint based on number of shard in duplicte table 
  	int shardId = (targetFingerprint.hashCode()  & Integer.MAX_VALUE) % _duplicatesTable.length;
    int low = 0;
    int high = (int)(_duplicatesTable[shardId].length / 9) -1;
    
    int iterationNumber = 0;
    
    while (low <= high) {
    	
    	++iterationNumber;
      
    	int mid = low + ((high - low) / 2);

    	//_lookupBuffer.reset(_duplicatesTable[shardId],0,_duplicatesTable[shardId].length);
    	//_lookupBuffer.skip(mid * 9);
    	
    	long currentValue 	=  (
    			((long)_duplicatesTable[shardId][(mid * 9) + 0] << 56) +
          ((long)(_duplicatesTable[shardId][(mid * 9) +1] & 255) << 48) +
          ((long)(_duplicatesTable[shardId][(mid * 9) +2] & 255) << 40) +
          ((long)(_duplicatesTable[shardId][(mid * 9) +3] & 255) << 32) +
          ((long)(_duplicatesTable[shardId][(mid * 9) +4] & 255) << 24) +
          ((_duplicatesTable[shardId][(mid * 9) +5] & 255) << 16) +
          ((_duplicatesTable[shardId][(mid * 9) +6] & 255) <<  8) +
          ((_duplicatesTable[shardId][(mid * 9) +7] & 255) <<  0));
    	
      // deserialize 
      //long currentValue 	= _lookupBuffer.readLong();
      //int  dupStatus      = _lookupBuffer.read();
    	int  dupStatus      	= _duplicatesTable[shardId][(mid * 9) +8];
      
      // now compare it against desired hash value ...
      int comparisonResult = ((Long)currentValue).compareTo(targetFingerprint.getUrlHash());
      
      if (comparisonResult > 0)
          high = mid - 1;
      else if (comparisonResult < 0)
          low = mid + 1;
      else {
      	if (dupStatus == 0) {
      		_duplicatesTable[shardId][(mid * 9) + 8] = -1;
      		return 0;
      	}
      	return dupStatus;
      }
    }
    return 0;
  }
  
  
  byte[][]      _fpLookupTable = null;    
  /**
   * initialize duplicate data   
   */
  public void initializeFPLookupTable(Path lookupTableDataPath)throws IOException {

    FileSystem hdfs = CrawlEnvironment.getDefaultFileSystem();

    LOG.info("Duplicate Lookup Table at Path:" + lookupTableDataPath);

    // find out how many parts there are 
    FileStatus parts[] = hdfs.globStatus(new Path(lookupTableDataPath.toString() + "/part-*"));

    if (parts.length == 0) { 
      throw new IOException("Invalid Lookup Table Path:" + lookupTableDataPath);
    }
    // allocate lookup table array 
    _fpLookupTable = new byte[parts.length][];

    // index
    int index = 0;

    // load individual parts 
    for (FileStatus part : parts) {
      LOG.info("Loading Part:" + part.getPath());

      long lookupTableStreamLength = hdfs.getLength(part.getPath());

      if (lookupTableStreamLength <= 0) {
        LOG.error("Lookup Table Invalid");
        throw new RuntimeException("Lookup Table Invalid");
      }


      FSDataInputStream lookupTableStream = hdfs.open(part.getPath());

      try { 
        _fpLookupTable[index] = new byte[(int)lookupTableStreamLength];
        LOG.info("Loading Duplicates Table at Path:" + part.getPath() + " Size:" + lookupTableStreamLength);
        lookupTableStream.readFully(_fpLookupTable[index]);
        LOG.info("Loaded Duplicates Table at Path:" + part.getPath() + " Size:" + lookupTableStreamLength);
      }
      finally { 
        lookupTableStream.close();
      }

      ++index;
    }
  }
    
  
  
  /** check to see if a fingerprint is in the set  
   * 
   * @param targetFingerprint
   * @return
   * @throws IOException
   */
  public int getFingerprintStatus(URLFPV2 targetFingerprint)throws IOException {

    // shard the incoming fingerprint based on number of shard in duplicte table 
    int shardId = (targetFingerprint.hashCode()  & Integer.MAX_VALUE) % _fpLookupTable.length;
    int low = 0;
    int high = (int)(_fpLookupTable[shardId].length / 8) -1;

    int iterationNumber = 0;

    while (low <= high) {

      ++iterationNumber;

      int mid = low + ((high - low) / 2);

      //_lookupBuffer.reset(_duplicatesTable[shardId],0,_duplicatesTable[shardId].length);
      //_lookupBuffer.skip(mid * 9);

      long currentValue   =  (
          ((long)_fpLookupTable[shardId][(mid * 8) + 0] << 56) +
          ((long)(_fpLookupTable[shardId][(mid * 8) +1] & 255) << 48) +
          ((long)(_fpLookupTable[shardId][(mid * 8) +2] & 255) << 40) +
          ((long)(_fpLookupTable[shardId][(mid * 8) +3] & 255) << 32) +
          ((long)(_fpLookupTable[shardId][(mid * 8) +4] & 255) << 24) +
          ((_fpLookupTable[shardId][(mid * 8) +5] & 255) << 16) +
          ((_fpLookupTable[shardId][(mid * 8) +6] & 255) <<  8) +
          ((_fpLookupTable[shardId][(mid * 8) +7] & 255) <<  0));


      // now compare it against desired hash value ...
      int comparisonResult = ((Long)currentValue).compareTo(targetFingerprint.getUrlHash());

      if (comparisonResult > 0)
        high = mid - 1;
      else if (comparisonResult < 0)
        low = mid + 1;
      else {
        return 1;
      }
    }
    return 0;
  }
  
  /**
   * release duplicate lookup table 
   */
  public void resleaseFPLookupTable() { 
    _fpLookupTable = null;
  }

  @Override
  public void queryFingerprintStatus(org.commoncrawl.rpc.base.internal.AsyncContext<URLFPV2,SimpleByteResult> rpcContext) throws RPCException {
    rpcContext.setStatus(Status.Error_RequestFailed);
    try { 
      if (_fpLookupTable != null) {
        try {
          rpcContext.getOutput().setByteResult((byte)getFingerprintStatus(rpcContext.getInput()));
          rpcContext.setStatus(Status.Success);
        } catch (IOException e) {
          LOG.error(CCStringUtils.stringifyException(e));
        }
      }
    }
    finally {
      rpcContext.completeRequest();
    }
  }
	
  @Override
  public void queryLongValue(
      AsyncContext<LongQueryParam, LongQueryParam> rpcContext)
      throws RPCException {
  }
 
}