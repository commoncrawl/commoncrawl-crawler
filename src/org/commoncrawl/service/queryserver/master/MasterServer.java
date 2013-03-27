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

package org.commoncrawl.service.queryserver.master;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.zip.CRC32;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.commoncrawl.async.ConcurrentTask;
import org.commoncrawl.async.Timer;
import org.commoncrawl.async.ConcurrentTask.CompletionCallback;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.db.RecordStore;
import org.commoncrawl.protocol.ArchiveInfo;
import org.commoncrawl.protocol.shared.ArcFileItem;
import org.commoncrawl.rpc.base.internal.AsyncClientChannel;
import org.commoncrawl.rpc.base.internal.AsyncContext;
import org.commoncrawl.rpc.base.internal.AsyncRequest;
import org.commoncrawl.rpc.base.internal.AsyncServerChannel;
import org.commoncrawl.rpc.base.shared.RPCException;
import org.commoncrawl.rpc.base.shared.RPCStruct;
import org.commoncrawl.server.CommonCrawlServer;
import org.commoncrawl.service.queryserver.BaseConfig;
import org.commoncrawl.service.queryserver.ClientQueryInfo;
import org.commoncrawl.service.queryserver.Common;
import org.commoncrawl.service.queryserver.ContentQueryRPCInfo;
import org.commoncrawl.service.queryserver.ContentQueryRPCResult;
import org.commoncrawl.service.queryserver.MasterState;
import org.commoncrawl.service.queryserver.PersistentQueryInfo;
import org.commoncrawl.service.queryserver.QueryServerMaster;
import org.commoncrawl.service.queryserver.QueryStatus;
import org.commoncrawl.service.queryserver.ShardIndexHostNameTuple;
import org.commoncrawl.service.queryserver.SlaveStatus;
import org.commoncrawl.service.queryserver.index.DatabaseIndexV2;
import org.commoncrawl.service.queryserver.query.Query;
import org.commoncrawl.service.queryserver.query.QueryCompletionCallback;
import org.commoncrawl.service.queryserver.query.QueryProgressCallback;
import org.commoncrawl.service.queryserver.query.QueryRequest;
import org.commoncrawl.service.queryserver.query.QueryResult;
import org.commoncrawl.service.queryserver.query.RemoteQueryCompletionCallback;
import org.commoncrawl.service.queryserver.query.ShardMapper;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.FileUtils;

/**
 * 
 * @author rana
 *
 */
public class MasterServer 
extends CommonCrawlServer 
implements QueryServerMaster, ShardMapper, AsyncServerChannel.ConnectionCallback
{


  /////////////////////////////////////////////////////////////////////////////////////////////////
  // react to a status change in a query slave's state ..
  private static final String MasterDBStateKey = "DBState";
  private static final String CachedQueryIDPrefix = "CQID_";
  private static final String CachedQueryPrefix = "CQ_";

  private String     _slavesFile;
  private File       _cacheDirs[];
  private File       _tempFileDir = null;
  private long 			 _tempFileDirSeed = - 1;
  private File       _webAppRoot  = null;
  private Vector<QueryServerSlaveState> _slaves = new Vector<QueryServerSlaveState>();
  private Map<String,QueryServerSlaveState> _slaveNameToOnlineStateMap = new TreeMap<String,QueryServerSlaveState>();
  private Map<String, SlaveStatus> _slaveStatusMap = new TreeMap<String,SlaveStatus>();
  private long       _slavesFileCRC = -1;
  private String     _hdfsWorkingDir      = "crawl/querydb/temp";
  private String     _hdfsResultsDir      = "crawl/querydb/results";
  private String     _hdfsResultsCacheDir = "crawl/querydb/cache";
  private long 			 _databaseId = -1;
  private Path 			 _localDataDir = null;
  private int				 _dataDriveCount = -1;
  private DatabaseIndexV2.MasterDatabaseIndex _masterIndex = null;
  private Path       _queryDBPath = null;
  /** record store object used to persist state **/
  private RecordStore _recordStore = new RecordStore();
  private MasterState _masterState = null;
  @SuppressWarnings("unchecked")
  private LinkedList<QueryRequest> _queuedClientQueries = new LinkedList<QueryRequest>();

  @SuppressWarnings("unchecked")
  private Map<Long,QueryRequest>  _activeRemoteOrLocalQueries    = new HashMap<Long,QueryRequest>();
  @SuppressWarnings("unchecked")
  private Set<QueryRequest>       _activeClientQueries          = new HashSet<QueryRequest>();

  @SuppressWarnings("unused")
  private QueryServerFE _queryServerFE;
  private ExecutorService _s3DownloaderThreadPool = Executors.newCachedThreadPool();

  private File getLocalCacheDirForQuery(long queryId) {
    int paritionId = ((int)queryId) % _cacheDirs.length;
    return _cacheDirs[paritionId];
  }

  public MasterServer() { 
    //setAsyncWebDispatch(true);
  }

  public DatabaseIndexV2.MasterDatabaseIndex getDatabaseIndex() { return _masterIndex; }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // INTERNAL ROUTINES 
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  public static class BlockingQueryResult<KeyType,ValueType> {

    public BlockingQueryResult(QueryResult<KeyType,ValueType> resultObject) { 
      querySucceeded = true;
      this.resultObject = resultObject;
    }

    public BlockingQueryResult(String failureReason) { 
      querySucceeded = false;
      this.errorString = failureReason;
    }

    public boolean querySucceeded = false;
    public QueryResult<KeyType,ValueType> resultObject;
    public String errorString;
  }

  <DataType extends RPCStruct,KeyType,ValueType> BlockingQueryResult<KeyType,ValueType> blockingQueryRequest(final Query<DataType,KeyType,ValueType> queryObject,final ClientQueryInfo queryInfo) throws IOException { 
    final LinkedBlockingQueue<BlockingQueryResult<KeyType,ValueType>> queue = new LinkedBlockingQueue<BlockingQueryResult<KeyType,ValueType>>(1);

    getEventLoop().setTimer(new Timer(0,false,new Timer.Callback() {

      @Override
      public void timerFired(Timer timer) {
        try {
          queueClientQueryRequest(queryObject,queryInfo,new QueryCompletionCallback<DataType,KeyType, ValueType>() {

            @Override
            public void queryComplete(QueryRequest<DataType,KeyType,ValueType> request,QueryResult<KeyType, ValueType> queryResult) {
              LOG.info("Recevied QueryComplete for query:" + request.getSourceQuery().getQueryId());
              BlockingQueryResult<KeyType,ValueType> result = new BlockingQueryResult<KeyType,ValueType>(queryResult);
              try {
                LOG.info("Queing response for Query:" + request.getSourceQuery().getQueryId());
                queue.put(result);
                LOG.info("Queued response for Query:" + request.getSourceQuery().getQueryId());
              } catch (InterruptedException e) {
                LOG.error(CCStringUtils.stringifyException(e));
              }
            }

            @Override
            public void queryFailed(QueryRequest<DataType,KeyType,ValueType> request, String reason) {
              LOG.info("Received queryFailed for request:" + request.getSourceQuery().getQueryId());
              BlockingQueryResult<KeyType,ValueType> result = new BlockingQueryResult<KeyType,ValueType>(reason);
              try {
                queue.put(result);
              } catch (InterruptedException e) {
                LOG.error(CCStringUtils.stringifyException(e));
              }
            } 

          });
        } catch (IOException e) {
          LOG.error(CCStringUtils.stringifyException(e));
        }
      } 

    }));

    try {
      return queue.take();
    } catch (InterruptedException e) {
      LOG.error(CCStringUtils.stringifyException(e));
    }
    return null;
  }

  <DataType extends RPCStruct,KeyType,ValueType> void queueClientQueryRequest(Query<DataType,KeyType,ValueType> queryObject,ClientQueryInfo theClientRequest,QueryCompletionCallback<DataType,KeyType,ValueType> callback) throws IOException { 

    // set query info
    queryObject.setClientQueryInfo(theClientRequest);

    // get the cannonical id for this query 
    String queryCanonicalId = queryObject.getCanonicalId();

    LOG.info("Received Query Request with CannonicalId:" + queryCanonicalId);

    // now check cache for persistent query cache info ...
    PersistentQueryInfo persistentQueryInfo = getPersistentQueryInfo(queryCanonicalId);
    // ok, cached query found ... 
    if (persistentQueryInfo != null) { 
      LOG.info("Existing Query Id found:" + persistentQueryInfo.getQueryId() + " for Request with CannonicalId:" + queryCanonicalId);
      // found cached query... set id of source query object  
      queryObject.setQueryId(persistentQueryInfo.getQueryId());
    }
    else { 
      // assign the query a new id 
      queryObject.setQueryId(getNextQueryId());

      LOG.info("Assigning Query Id:" + queryObject.getQueryId() + " for Request with CannonicalId:" + queryCanonicalId);

      // and store the relationship
      persistentQueryInfo = new PersistentQueryInfo();
      persistentQueryInfo.setCannonicalQueryId(queryCanonicalId);
      persistentQueryInfo.setQueryId(queryObject.getQueryId());
      persistentQueryInfo.setCreateTime(System.currentTimeMillis());

      LOG.info("Inserting Persistent Query Record");
      // insert new structure into database ... 
      insertUpdatePersistentInfo(persistentQueryInfo,false);
    }

    // establish hdfs working directory 
    Path hdfsWorkingDir = new Path(_hdfsWorkingDir,Long.toString(queryObject.getQueryId()));

    // remove existing directory if present ... 
    CrawlEnvironment.getDefaultFileSystem().delete(hdfsWorkingDir, true);

    // create the working directory
    CrawlEnvironment.getDefaultFileSystem().mkdirs(hdfsWorkingDir);

    // establish the hdfs working directory ...
    queryObject.getCommonQueryInfo().setQueryResultPath(hdfsWorkingDir.toString());

    // establish the query cache directory 
    File localQueryDirectory = getLocalCacheDirForQuery(queryObject.getQueryId());

    LOG.info("Query Cache Directory for Query:" + queryObject.getQueryId() + " is:" + localQueryDirectory.getAbsolutePath());
    // make sure it exists ... 
    localQueryDirectory.mkdirs();

    // allocate  client request object ...
    QueryRequest<DataType,KeyType,ValueType> clientQueryObj = new QueryRequest<DataType,KeyType,ValueType>(queryObject,theClientRequest,localQueryDirectory,callback);

    // setup context ... 
    queryObject.setContext(clientQueryObj);

    LOG.info("Query Client Request");

    // add it to queue ... 
    _queuedClientQueries.addLast(clientQueryObj);

    //potentially start the next query ... 
    potentiallyStartNextQuery();
  }

  @SuppressWarnings("unchecked")
  private void potentiallyStartNextQuery()throws IOException { 

    FileSystem fileSystem = CrawlEnvironment.getDefaultFileSystem();

    LinkedList<QueryRequest> requeueList = new LinkedList<QueryRequest>();

    while (_queuedClientQueries.size() != 0 && _activeClientQueries.size() < Common.MAX_CONCURRENT_QUERIES) { 

      QueryRequest request = _queuedClientQueries.removeFirst();

      LOG.info("Processing Query:" + request.getSourceQuery().getQueryId() + " ActiveCount:" + _activeClientQueries.size());

      try {
        // first see if a remote (or local) query is active ... 
        if (_activeRemoteOrLocalQueries.get(request.getSourceQuery().getQueryId())  != null) { 
          LOG.info("Cannot Dispatch ClientRequest:" + request.getClientQueryInfo().getClientQueryId() + " because existing query in progress");
          // Fail the query immediately for now ..
          request.getCompletionCallback().queryFailed(request,"A similar query is already running and may take some time to complete. Please try again later.");
        }
        else {
          ArrayList<ShardIndexHostNameTuple> shardIdMapping = new ArrayList<ShardIndexHostNameTuple>();
          // first check to see if cached results are available ... 
          if (request.getSourceQuery().cachedResultsAvailable(fileSystem,_configuration,request)) {
            // add to active ... 
            _activeClientQueries.add(request);
            LOG.info("Running Cache Query for Query:" + request.getSourceQuery().getQueryId());            
            runCacheQuery(request);
          }
          // check to see if remote dispatch is required ..
          else if (request.getSourceQuery().requiresRemoteDispatch(fileSystem,_configuration,this,request,shardIdMapping)) { 
            // ok, we need at least on shard to run on ... 
            if (shardIdMapping.size() == 0) { 
              LOG.error("Query:" + request.getSourceQuery().getQueryId() + " FAILED WITH EMPTY HOSTS(TO RUN ON) LIST");
              throw new IOException("Empty Host List prior to remoteDispath!");
            }
            // set shard id to host mapping into query 
            request.getSourceQuery().setShardIdToHostMapping(shardIdMapping);
            // ok, we ready for remote dispatch ... 
            // add to active ... 
            _activeClientQueries.add(request);
            // add to remote dispatch id set 
            _activeRemoteOrLocalQueries.put(request.getSourceQuery().getQueryId(),request);
            LOG.info("Running Remote Query for Query:" + request.getSourceQuery().getQueryId());
            // and dispatch request .. 
            runRemoteQuery(fileSystem,request);
          }
          // otherwise .. run Local Request
          else { 
            // add to active ... 
            _activeClientQueries.add(request);
            // add to remote dispatch id set 
            _activeRemoteOrLocalQueries.put(request.getSourceQuery().getQueryId(),request);
            LOG.info("Running Local Query for Query:" + request.getSourceQuery().getQueryId());
            // and dispatch request .. 
            runLocalQuery(request);
          }
        }
      }
      catch (IOException e) { 
        LOG.error("Client Request:" + request.getClientQueryInfo().getClientQueryId() + " Failed with Exception:" + CCStringUtils.stringifyException(e));
        request.getCompletionCallback().queryFailed(request,CCStringUtils.stringifyException(e));
      }
    }
    _queuedClientQueries.addAll(requeueList);
  }

  @SuppressWarnings("unchecked")
  private void deactivateRequest(QueryRequest request) { 
    LOG.info("DeActivating Query:" + request.getSourceQuery().getQueryId());

    // first things first, delete temp file!!!
    File queryTempFile = getTempDirForQuery(request.getSourceQuery().getQueryId());
    LOG.info("** Deleting Temp File for Query:" + request.getSourceQuery().getQueryId() + " At:" + queryTempFile.getAbsolutePath());
    FileUtils.recursivelyDeleteFile(queryTempFile);

    _activeClientQueries.remove(request);
    if (request.getRunState() == QueryRequest.RunState.RUNNING_REMOTE || request.getRunState() == QueryRequest.RunState.RUNNING_LOCAL) { 
      _activeRemoteOrLocalQueries.remove(request.getSourceQuery().getQueryId());
    }
    request.setRunState(QueryRequest.RunState.IDLE);


    try {
      potentiallyStartNextQuery();
    } catch (IOException e) {
      LOG.error(CCStringUtils.stringifyException(e));
    }

  }

  @SuppressWarnings("unchecked")
  private void requeueRequest(QueryRequest request) {
    deactivateRequest(request);
    LOG.info("ReQueueing Query:" + request.getSourceQuery().getQueryId());
    _queuedClientQueries.addFirst(request);
    try {
      potentiallyStartNextQuery();
    } catch (IOException e) {
      LOG.error(CCStringUtils.stringifyException(e));
    }
  }

  @SuppressWarnings("unchecked")
  private void runRemoteQuery(final FileSystem remoteFileSystem,final QueryRequest request) { 

    //LOG.info("runRemoteQuery Called for Query:" + request.getSourceQuery().getQueryId());
    if (!request.setRunState(QueryRequest.RunState.RUNNING_REMOTE)) { 
      deactivateRequest((QueryRequest)request.getSourceQuery().getContextObject());
      request.getCompletionCallback().queryFailed(request, "Unable to transition to RUNNING_REMOTE");
      return;
    }

    try { 
      request.getSourceQuery().startRemoteQuery(_slaveNameToOnlineStateMap,request.getSourceQuery().getShardIdToHostMapping(), 
          new QueryProgressCallback() {

        @Override
        public boolean updateProgress(Query theQueryObject,float percentComplete) {
          LOG.info("Got updateProgress callback for:" + theQueryObject.getQueryId());
          return true;
        } 

      },

      new RemoteQueryCompletionCallback() {

        @Override
        public void queryComplete(Query query, long resultCount) {
          LOG.info("Recevied QueryComplete for Query:" + request.getSourceQuery().getQueryId());

          // call remote dispatch complete
          try { 
            query.remoteDispatchComplete(remoteFileSystem,_configuration,request,resultCount);

            if (resultCount > 0) { 
              LOG.info("Remote Query:" + request.getSourceQuery().getQueryId() + " returned:" + resultCount  + " results");
              // deactive request first ...
              requeueRequest((QueryRequest)query.getContextObject());
            }
            else {

              LOG.info("Query:" + request.getSourceQuery().getQueryId() + " returned zero results");

              deactivateRequest(request);

              QueryResult result = new QueryResult();
              result.setTotalRecordCount(0);
              request.getCompletionCallback().queryComplete(request, result);

              LOG.info("Query:" + request.getSourceQuery().getQueryId() + " DONE DUDE");                	
            }
          }
          catch (IOException e) {
            String error = "Query: " + request.getSourceQuery().getQueryId() + " Failed with Exception:" + CCStringUtils.stringifyException(e); 
            LOG.error(error);
            // deactivate the request 
            deactivateRequest((QueryRequest)query.getContextObject());

            request.getCompletionCallback().queryFailed(request, error);
          }
        }

        @Override
        public void queryFailed(Query query, final String reason) {
          LOG.info("Recevied QueryFailed for Query:" + request.getSourceQuery().getQueryId() + " Reason:" + reason);
          // inform query of failure 
          query.remoteDispatchFailed(remoteFileSystem);
          // deactivate the request 
          deactivateRequest((QueryRequest)query.getContextObject());

          request.getCompletionCallback().queryFailed(request, reason);
        } 
      });
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      deactivateRequest(request);
      request.getCompletionCallback().queryFailed(request, CCStringUtils.stringifyException(e));
    }
  }

  @SuppressWarnings("unchecked")
  private void runLocalQuery(final QueryRequest request) {
    //LOG.info("runLocalQuery Called for Query:" + request.getSourceQuery().getQueryId());

    if (!request.setRunState(QueryRequest.RunState.RUNNING_LOCAL)) { 
      deactivateRequest((QueryRequest)request.getSourceQuery().getContextObject());
      request.getCompletionCallback().queryFailed(request, "Unable to transition to RUNNING_LOCAL");
      return;
    }

    try { 
      request.getSourceQuery().startLocalQuery(
          CrawlEnvironment.getDefaultFileSystem(),
          _configuration,_masterIndex,
          getTempDirForQuery(request.getSourceQuery().getQueryId()),          
          getEventLoop(),
          request,
          new RemoteQueryCompletionCallback() {

            @Override
            public void queryComplete(Query query, long resultCount) {
              LOG.info("Recevied QueryComplete for Query:" + request.getSourceQuery().getQueryId());
              if (resultCount > 0) {
                LOG.info("Local Query:" + request.getSourceQuery().getQueryId() + " returned:" + resultCount  + " results");
                // requeue request ...
                requeueRequest((QueryRequest)query.getContextObject());
              }
              else {
                LOG.info("Query:" + request.getSourceQuery().getQueryId() + " returned zero results");

                // deactive ... 
                deactivateRequest((QueryRequest)query.getContextObject());

                // initiate callback 

                QueryResult result = new QueryResult();
                result.setTotalRecordCount(0);
                request.getCompletionCallback().queryComplete(request, result);
                LOG.info("Query:" + request.getSourceQuery().getQueryId() + " DONE DUDE");
              }
            }

            @Override
            public void queryFailed(Query query, final String reason) {
              LOG.info("Recevied QueryFailed for Query:" + request.getSourceQuery().getQueryId() + " Reason:" + reason);
              deactivateRequest((QueryRequest)query.getContextObject());
              request.getCompletionCallback().queryFailed(request, reason);
            } 
          });
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      deactivateRequest(request);
      request.getCompletionCallback().queryFailed(request, CCStringUtils.stringifyException(e));
    }
  }

  @SuppressWarnings("unchecked")
  private void runCacheQuery(QueryRequest request) {
    //LOG.info("runCacheQuery Called for Query:" + request.getSourceQuery().getQueryId());
    if (!request.setRunState(QueryRequest.RunState.RUNNING_CACHE)) { 
      deactivateRequest((QueryRequest)request.getSourceQuery().getContextObject());
      request.getCompletionCallback().queryFailed(request, "Unable to transition to RUNNING_CACHE");
      return;
    }


    try { 
      request.getSourceQuery().startCacheQuery(
          _masterIndex,
          CrawlEnvironment.getDefaultFileSystem(),
          _configuration,
          getEventLoop(),
          request,
          new QueryCompletionCallback() {

            @Override
            public void queryComplete(QueryRequest request,QueryResult queryResult) {
              deactivateRequest(request);
              request.getCompletionCallback().queryComplete(request, queryResult);
            }

            @Override
            public void queryFailed(QueryRequest request, String reason) {
              deactivateRequest(request);
              request.getCompletionCallback().queryFailed(request, reason);
            } 
          }
          );
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      deactivateRequest(request);
      request.getCompletionCallback().queryFailed(request, CCStringUtils.stringifyException(e));
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // COMMONCRAWL SERVER OVERRIDES
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////// 

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
    return CrawlEnvironment.DEFAULT_QUERY_MASTER_HTTP_PORT;
  }

  @Override
  protected String getDefaultLogFileName() {
    return "qmaster";
  }

  @Override
  protected String getDefaultRPCInterface() {
    return CrawlEnvironment.DEFAULT_RPC_INTERFACE;
  }

  @Override
  protected int getDefaultRPCPort() {
    return CrawlEnvironment.DEFAULT_QUERY_MASTER_RPC_PORT;
  }

  @Override
  protected String getWebAppName() {
    return CrawlEnvironment.QUERY_MASTER_WEBAPP_NAME;
  }



  /**
   * Get the pathname to the <code>patch</code> files.
   * @param path Path to find.
   * @return the pathname as a URL
   */
  private static String getWebAppsPath(final String path) throws IOException {
    URL url = MasterServer.class.getClassLoader().getResource(path);
    if (url == null) 
      throw new IOException("webapps not found in CLASSPATH"); 
    return url.toString();
  } 

  private File getDefaultWebAppPath() throws IOException { 
    return new File(getWebAppsPath("webapps") + File.separator + getWebAppName());
  }

  @Override
  protected boolean initServer() {

    _tempFileDirSeed = System.currentTimeMillis();

    if (_slavesFile == null) { 
      LOG.error("Slaves File not specified. Specify Slaves file via --slaves");
      return false;
    }
    else { 
      try {
        // get a pointer to the hdfs file system 
        // _fileSystem = CrawlEnvironment.getDefaultFileSystem();
        // parse slaves file ..
        parseSlavesFile();
      }
      catch (IOException e) { 
        LOG.error(CCStringUtils.stringifyException(e));
        return false;
      }
    }

    // initialize database ... 
    File databasePath = new File(getDataDirectory().getAbsolutePath() + "/" + CrawlEnvironment.QMASTER_DB);
    LOG.info("Config says QMaster db path is: "+databasePath);
    // initialize master index 
    if (_databaseId == -1 || _localDataDir == null || _dataDriveCount == -1 || _cacheDirs == null) {
      if (_databaseId == -1) 
        LOG.error("Database Id is Not Defined");
      if (_localDataDir == null)
        LOG.error("Local DataDir is NULL");
      if (_dataDriveCount == -1)  
        LOG.error("Data Drive Count is not Defined");
      if (_cacheDirs == null) 
        LOG.error("CacheDirs is NULL");
      return false;
    }

    try { 
      FileSystem remoteFS = CrawlEnvironment.getDefaultFileSystem();
      // fully resolve slave names 
      HashSet<String> onlineSlaves = new HashSet<String>();
      for (String slave : _slaveNameToOnlineStateMap.keySet()) { 
        LOG.info("Slave:" + slave + " maps to FQN:" + InetAddress.getByName(slave).getCanonicalHostName());
        onlineSlaves.add(InetAddress.getByName(slave).getCanonicalHostName());
      }
      
      // load master index .. 
      _masterIndex = new DatabaseIndexV2.MasterDatabaseIndex(_configuration, remoteFS,_dataDriveCount, _databaseId,onlineSlaves);

      // initialize record store
      _recordStore.initialize(databasePath, null);

      // load db state ... 
      _masterState = (MasterState) _recordStore.getRecordByKey(MasterDBStateKey);

      if (_masterState == null) { 
        _masterState = new MasterState();
        _masterState.setLastQueryId(0);
        _recordStore.beginTransaction();
        _recordStore.insertRecord("", MasterDBStateKey, _masterState);
        _recordStore.commitTransaction();
      }

      // create server channel ... 
      AsyncServerChannel channel = new AsyncServerChannel(this, this.getEventLoop(), this.getServerAddress(),this);

      // register RPC services it supports ... 
      registerService(channel,QueryServerMaster.spec);
    }
    catch (IOException e) { 
      LOG.error("Database Initialization Failed with Exception:" + CCStringUtils.stringifyException(e));
      return false;
    }

    if (_tempFileDir == null) {
      _tempFileDir = new File(getDataDirectory(),"qserver_temp");
      _tempFileDir.mkdirs();

      LOG.info("TempFilr Dir is null. Setting TempFile Dir to:" + _tempFileDir.getAbsolutePath());
    }

    if (_webAppRoot == null) { 
      try {
        _webAppRoot = getDefaultWebAppPath();
      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
        return false;
      }
      LOG.info("WebApp Root not specified.Using default at:" + _webAppRoot.getAbsolutePath());
    }


    try {
      // load database state ... 
      // loadState();
      // connect to slaves ...
      connectToSlaves();
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      return false;
    }

    // clear working directory... 
    try {
      LOG.info("Clearing working directory:" + _hdfsWorkingDir);
      CrawlEnvironment.getDefaultFileSystem().delete(new Path(_hdfsWorkingDir,"*"),true);
      LOG.info("Cleared working directory:" + _hdfsWorkingDir);
    } catch (IOException e1) {
      LOG.error(CCStringUtils.stringifyException(e1));
    }

    try { 
      // locate query db path 
      _queryDBPath = locateQueryDBPath();

    }
    catch (IOException e) { 
      LOG.error("Failed to locate QueryDB Path with Exception:" + CCStringUtils.stringifyException(e));
      return false;
    }

    if (_queryDBPath == null) { 
      LOG.error("Failed to find queryDB candidate.");
      return false;
    }

    try {
      _queryServerFE = new QueryServerFE(this,_webAppRoot);
    } catch (IOException e) {
      LOG.error(CCStringUtils.stringifyException(e));
      return false;
    }    

    return true;
  }

  private File getTempDirForQuery(long queryId) { 
    return new File(_tempFileDir,Long.toString(queryId) + "-" + _tempFileDirSeed);
  }

  private void writeMasterState() throws IOException { 
    _recordStore.beginTransaction();
    _recordStore.updateRecordByKey(MasterDBStateKey, _masterState);
    _recordStore.commitTransaction();
  }

  private long getNextQueryId() throws IOException { 
    long nextQueryId = _masterState.getLastQueryId() + 1;
    _masterState.setLastQueryId(nextQueryId);
    writeMasterState();
    return nextQueryId;
  }

  private void insertUpdatePersistentInfo(PersistentQueryInfo persistentQueryInfo,boolean isUpdate) throws IOException { 

    persistentQueryInfo.setLastAccessTime(System.currentTimeMillis());

    _recordStore.beginTransaction();
    if (isUpdate) { 
      _recordStore.updateRecordByKey(CachedQueryPrefix + persistentQueryInfo.getCannonicalQueryId(), persistentQueryInfo);
    }
    else { 
      _recordStore.insertRecord(CachedQueryIDPrefix + persistentQueryInfo.getQueryId(), CachedQueryPrefix + persistentQueryInfo.getCannonicalQueryId(), persistentQueryInfo);
    }
    _recordStore.commitTransaction();

  }


  private PersistentQueryInfo getPersistentQueryInfo(String canonicalId) throws IOException { 
    return (PersistentQueryInfo) _recordStore.getRecordByKey(CachedQueryPrefix+canonicalId);
  }

  @Override
  protected boolean parseArguments(String[] argv) {

    for(int i=0; i < argv.length;++i) {
      if (argv[i].equalsIgnoreCase("--slaves")) {
        _slavesFile = argv[++i];
      }
      else if (argv[i].equalsIgnoreCase("--databaseId")) {
        _databaseId = Long.parseLong(argv[++i]);
      }
      else if (argv[i].equalsIgnoreCase("--localDataDir")) { 
        _localDataDir = new Path(argv[++i]);
      }
      else if (argv[i].equalsIgnoreCase("--dataDriveCount")) {
        _dataDriveCount = Integer.parseInt(argv[++i]);
      }
      else if (argv[i].equalsIgnoreCase("--cacheFileDir")) { 
        String paths = argv[++i];
        String splitPaths[] = paths.split(",");
        _cacheDirs = new File[splitPaths.length];
        int index=0;
        for (String path : splitPaths) { 
          _cacheDirs[index] = new File(path);
          _cacheDirs[index].mkdirs();
          if (!_cacheDirs[index].isDirectory()) { 
            LOG.error("Invalid Cache Directory Specified:" + _cacheDirs[index].getAbsolutePath());
            return false;
          }
          index++;
        }
      }
      else if (argv[i].equalsIgnoreCase("--tempFileDir")) { 
        _tempFileDir = new File(argv[++i]);
        // delete the directory contents up front 
        FileUtils.recursivelyDeleteFile(_tempFileDir);
        // and recreate
        _tempFileDir.mkdirs();
        if (!_tempFileDir.isDirectory()) { 
          LOG.error("Invalid Temp Directory Specified:" + _tempFileDir.getAbsolutePath());
          return false;
        }
      }
      else if (argv[i].equalsIgnoreCase("--webAppRoot")) { 
        _webAppRoot = new File(argv[++i]);
        if (!_webAppRoot.isDirectory()) { 
          LOG.error("Invalid Web App Directory Specified:" + _webAppRoot.getAbsolutePath());
          return false;
        }
      }


    }
    return true;
  }

  @Override
  protected void printUsage() {
    System.out.println( "Required Parameters: --domainFile domainFilePath --cacheFileDir cacheFileDirectory");
  }

  @Override
  protected boolean startDaemons() {
    return true;
  }

  @Override
  protected void stopDaemons() {
  }


  private Path locateQueryDBPath()throws IOException { 
    FileSystem fs = CrawlEnvironment.getDefaultFileSystem();

    FileStatus statusArray[] = fs.globStatus(new Path("crawl/querydb/db/*"));

    Path candidatePath = null;
    for (FileStatus fileStatus : statusArray) { 
      if (candidatePath == null) { 
        candidatePath = fileStatus.getPath();
      }
      else { 
        long prevTimestamp = Long.parseLong(candidatePath.getName());
        long currentTimestamp = Long.parseLong(fileStatus.getPath().getName());
        if (currentTimestamp > prevTimestamp) { 
          candidatePath = fileStatus.getPath();
        }
      }
    }
    if (candidatePath != null) { 
      LOG.info("Selected Candidate Path:" + candidatePath);
    }
    return candidatePath;
  }

  public BaseConfig getBaseConfigForSlave(QueryServerSlaveState slave) { 

    BaseConfig baseConfig = new BaseConfig();

    baseConfig.setBaseWorkingDir(_hdfsWorkingDir);
    baseConfig.setQueryResultsDir(_hdfsResultsDir);
    baseConfig.setQueryCacheDir(_hdfsResultsCacheDir);
    baseConfig.setQueryDBPath(_queryDBPath.toString());
    // baseConfig.setFileSystem(_fileSystem.getUri().toString());
    baseConfig.setDatabaseTimestamp(_databaseId);

    return baseConfig;
  }

  void connectToSlaves() throws IOException {
    LOG.info("Connecting to Slaves");
    for (QueryServerSlaveState slave : _slaves) { 
      slave.connect();
    }
  }

  void parseSlavesFile()throws IOException {

    LOG.info("Loading Slaves File from:" + _slavesFile);
    InputStream stream =null;
    URL resourceURL = CrawlEnvironment.getHadoopConfig().getResource(_slavesFile);

    if (resourceURL != null) {
      stream = resourceURL.openStream();
    }
    // try as filename 
    else { 
      LOG.info("Could not load resource as an URL. Trying as an absolute pathname");
      stream = new FileInputStream(new File(_slavesFile));
    }

    if (stream == null) { 
      throw new FileNotFoundException();
    }

    BufferedReader reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(stream)));

    String slaveHostPlusCount = null;

    LOG.info("Loading slaves file");
    while ((slaveHostPlusCount = reader.readLine()) != null) {
      if (!slaveHostPlusCount.startsWith("#")) {
        StringTokenizer tokenizer = new StringTokenizer(slaveHostPlusCount,":");
        if (tokenizer.countTokens() != 3){
          throw new IOException("Invalid Slave Entry:" + slaveHostPlusCount + " in slaves File");
        }
        else {
          String slaveName = tokenizer.nextToken();
          //TODO:INSTANCE COUNT IS IGNORED !!!
          QueryServerSlaveState state = new QueryServerSlaveState(this,slaveName);
          LOG.info("Adding slave:" + slaveName);
          _slaves.add(state);
          // map host name to onlinestate 
          _slaveNameToOnlineStateMap.put(slaveName,state);
          // and add SlaveState entry 
          _slaveStatusMap.put(slaveName, new SlaveStatus());
        }
      }
    }

    // now close the file and reopen to to compute the crc ... 
    reader.close();
    stream.close();

    CRC32 fileCRC = new CRC32();

    InputStream crcStream = null;

    if (resourceURL != null) { 
      crcStream = resourceURL.openStream();
    }
    else { 
      LOG.info("Could not load resource as an URL. Trying as an absolute pathname");
      crcStream = new FileInputStream(new File(_slavesFile));
    }

    byte[] buf = new byte[4096];
    int nRead = 0;
    while ( (nRead = crcStream.read(buf, 0, buf.length)) > 0 ) {
      fileCRC.update(buf, 0, nRead);
    }

    _slavesFileCRC = fileCRC.getValue();
    LOG.info("Slaves File CRC is:" + _slavesFileCRC);

    crcStream.close();

  }

  @SuppressWarnings("unchecked")
  void slaveStatusChanged(QueryServerSlaveState slave,SlaveStatus slaveStatus) {
    // LOG.info("Received slaveStatusChanged from slave:" + slave.getFullyQualifiedName());

    if (slaveStatus != null && slaveStatus.getQueryStatus().size() != 0) { 

      //LOG.info("Received:" + slaveStatus.getQueryStatus() + " QueryStatus updated from Slave:" +  slave.getFullyQualifiedName());

      // broadcast all query changes ... 
      for (QueryStatus queryStatus : slaveStatus.getQueryStatus()) { 
        //LOG.info("RCVD Status for Query:" + queryStatus.getQueryId() + " Status:" + QueryStatus.Status.toString(queryStatus.getStatus()));
        QueryRequest request = _activeRemoteOrLocalQueries.get(queryStatus.getQueryId());
        if (request != null) { 
          //LOG.info("FOUND QueryRequestObj:" + request + " for Query:" + queryStatus.getQueryId());
          try {
            request.getSourceQuery().updateQueryStatusForSlave(slave.getHostName(),queryStatus);
          } catch (IOException e) {
            LOG.error("Error Updating QueryStatus for Query:" 
                + request.getSourceQuery().getQueryId() 
                + " Slave:" + slave.getFullyQualifiedName() 
                + " Error:" + CCStringUtils.stringifyException(e));
          }
        }
        else { 
          LOG.error("DID NOT FIND QueryRequestObj for Query:" + queryStatus.getQueryId());
        }
      }
      // clear query status array ...
      slaveStatus.getQueryStatus().clear();
    }

    try {
      if (slaveStatus != null) { 
        _slaveStatusMap.get(slave.getHostName()).merge(slaveStatus);
      }
      else { 
        _slaveStatusMap.get(slave.getHostName()).clear();
      }
    } catch (CloneNotSupportedException e) {
    }

    try { 
      potentiallyStartNextQuery();
    }
    catch (IOException e) { 
      LOG.error("Error encountered calling startNextQuery. Exception:" + CCStringUtils.stringifyException(e));
    }
  }


  private void completeContentQuery(AsyncContext<ContentQueryRPCInfo, ContentQueryRPCResult> rpcContext,ArcFileItem item) { 
    if (item != null) { 
      rpcContext.getOutput().setSuccess(true);
      rpcContext.getOutput().setArcFileResult(item);
      rpcContext.setStatus(AsyncRequest.Status.Success);
      try {
        rpcContext.completeRequest();
      } catch (RPCException e) {
        LOG.error(CCStringUtils.stringifyException(e));
      }
    }
    else { 
      rpcContext.getOutput().setSuccess(false);
      rpcContext.setStatus(AsyncRequest.Status.Error_RequestFailed);
      try {
        rpcContext.completeRequest();
      } catch (RPCException e) {
        LOG.error(CCStringUtils.stringifyException(e));
      }
    }
  }

  private void startS3Download(final AsyncContext<ContentQueryRPCInfo, ContentQueryRPCResult> rpcContext,final ArchiveInfo archiveInfo) { 
    _s3DownloaderThreadPool.submit(new ConcurrentTask<ArcFileItem>(_eventLoop,new Callable<ArcFileItem>() {

      @Override
      public ArcFileItem call() throws Exception {
        LOG.info("Starting S3 Download for URL:" + rpcContext.getInput().getUrl());
        return S3Helper.retrieveArcFileItem(archiveInfo, _eventLoop);
      } 

    },
    new CompletionCallback<ArcFileItem>() {

      @Override
      public void taskComplete(ArcFileItem loadResult) {
        LOG.info("S3 Download for URL:" + rpcContext.getInput().getUrl() +  " Completed with " + ((loadResult == null) ? "NULL": "Valid") + "load Result");
        completeContentQuery(rpcContext,loadResult);
      }

      @Override
      public void taskFailed(Exception e) {
        LOG.error("S3 Download for URL:" + rpcContext.getInput().getUrl() +  " Failed with Exception:" + CCStringUtils.stringifyException(e));
        completeContentQuery(rpcContext,null);
      } 

    }));
  }

  @Override
  public void doContentQuery(final AsyncContext<ContentQueryRPCInfo, ContentQueryRPCResult> rpcContext) throws RPCException {
    /*    
    LOG.info("Got ContentQuery RPC for URL:" + rpcContext.getInput().getUrl() + "Sending directly to slaves");

    final ContentQueryState queryState = new ContentQueryState();

    for (QueryServerSlaveState slaveState : _slaves) { 
      if (slaveState.getRemoteStub() != null) {
        queryState.totalDispatchCount++;
        slaveState.getRemoteStub().doMetadataQuery(rpcContext.getInput(),new AsyncRequest.Callback<ContentQueryRPCInfo, CrawlDatumAndMetadata>() {

          @Override
          public void requestComplete(final AsyncRequest<ContentQueryRPCInfo, CrawlDatumAndMetadata> request) {
            queryState.completedCount++;
            if (request.getStatus() == AsyncRequest.Status.Success && !queryState.done) {
              queryState.done = true;
              // found a valid result ...
              LOG.info("Found Metadata for URL:" + rpcContext.getInput().getUrl()); 
              // check to see if archive information is available for the this url ...
              ArchiveInfo archiveInfo = null;
              if (request.getOutput().getMetadata().getArchiveInfo().size() != 0) { 
                Collections.sort(request.getOutput().getMetadata().getArchiveInfo(), new Comparator<ArchiveInfo> () {

                  @Override
                  public int compare(ArchiveInfo o1, ArchiveInfo o2) {
                    return (o1.getArcfileDate() < o2.getArcfileDate()) ? -1 : (o1.getArcfileDate() > o2.getArcfileDate()) ? 1 : 0; 
                  } 

                });
                archiveInfo = request.getOutput().getMetadata().getArchiveInfo().get(request.getOutput().getMetadata().getArchiveInfo().size()-1);
              }
              // if archive info is available ... 
              if (archiveInfo != null) {
                LOG.info("Archive Info Found for URL:" + rpcContext.getInput().getUrl() + " Starting S3Download");
                // start a download thread ...
                startS3Download(rpcContext,archiveInfo);
              }
              // otherwsie ... fail request ... 
              else { 
                LOG.info("Archive Info not Found for URL:" + rpcContext.getInput().getUrl() + " Failing Request");
                completeContentQuery(rpcContext,null);
              }
            }

            if (!queryState.done && queryState.completedCount == queryState.totalDispatchCount) { 
              // ok all the queries failed to return results ... fail the request ...
              LOG.info("All Queries Completed and Failed for MetadataQuery for URL:" + rpcContext.getInput().getUrl());
              rpcContext.setStatus(AsyncRequest.Status.Error_RequestFailed);
              try {
                rpcContext.completeRequest();
              } catch (RPCException e) {
                LOG.error(CCStringUtils.stringifyException(e));
              }
            }
          } 
        });
      }
    }
     */
    try {
      rpcContext.completeRequest();
    } catch (RPCException e) {
      LOG.error(CCStringUtils.stringifyException(e));
    }

  }

  @Override
  public void IncomingClientConnected(AsyncClientChannel channel) {

  }

  @Override
  public void IncomingClientDisconnected(AsyncClientChannel channel) {

  }

  @Override
  public ArrayList<ShardIndexHostNameTuple> mapShardIdsForIndex(String indexName)throws IOException {
    ArrayList<ShardIndexHostNameTuple> tupleListOut = _masterIndex.mapShardIdsForIndex(indexName);
    if (tupleListOut == null) { 
      throw new IOException("Unable to find tupleListMapping for Index:" + indexName);
    }
    return tupleListOut;
  }
}
