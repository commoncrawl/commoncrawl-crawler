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

package org.commoncrawl.service.queryserver.slave;


import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import com.hadoop.compression.lzo.LzoCodec;
import org.commoncrawl.async.Callback;
import org.commoncrawl.async.ConcurrentTask;
import org.commoncrawl.async.Timer;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.rpc.base.internal.AsyncClientChannel;
import org.commoncrawl.rpc.base.internal.AsyncContext;
import org.commoncrawl.rpc.base.internal.AsyncRequest;
import org.commoncrawl.rpc.base.internal.AsyncServerChannel;
import org.commoncrawl.rpc.base.internal.NullMessage;
import org.commoncrawl.rpc.base.shared.BinaryProtocol;
import org.commoncrawl.rpc.base.shared.RPCException;
import org.commoncrawl.rpc.base.shared.RPCStruct;
import org.commoncrawl.server.CommonCrawlServer;
import org.commoncrawl.service.queryserver.BaseConfig;
import org.commoncrawl.service.queryserver.Common;
import org.commoncrawl.service.queryserver.QueryCommon;
import org.commoncrawl.service.queryserver.QueryServerSlave;
import org.commoncrawl.service.queryserver.QueryStatus;
import org.commoncrawl.service.queryserver.RemoteQueryInfo;
import org.commoncrawl.service.queryserver.SlaveStatus;
import org.commoncrawl.service.queryserver.index.DatabaseIndexV2;
import org.commoncrawl.service.queryserver.query.Query;
import org.commoncrawl.service.queryserver.query.QueryProgressCallback;
import org.commoncrawl.service.queryserver.query.RemoteQueryCompletionCallback;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.FileUtils;

@SuppressWarnings("unchecked")
/**
 * @author rana
 */
public class SlaveServer  
  extends CommonCrawlServer 
  implements QueryServerSlave,
             AsyncServerChannel.ConnectionCallback,
             RemoteQueryCompletionCallback,
             QueryProgressCallback
             {

  static final String QUERY_THREAD_POOL_ID = "query.thread.pool";
 
  
  private static final int MIN_INSTANCE_ID = 0;
  private static final int MAX_INSTANCE_ID = 9;
  private static final int DEFAULT_THREAD_POOL_SIZE=8*4;
  private int   _instanceId = -1;
  private int   _threadPoolSize = DEFAULT_THREAD_POOL_SIZE;
  private boolean    _cancelling = false;
  private BaseConfig _baseConfig;
  private SlaveStatus _slaveStatus = new SlaveStatus();
  private FileSystem _fileSystem = null;
  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  private File _tempFileDir = null;
  LzoCodec codec;
 
  private LinkedList<Query>  _pendingQueries   = new LinkedList<Query>();
  private Map<Long,Query>     _activeQueries    = new HashMap<Long,Query>();
  private HashSet<Long>      _cancelledQueries = new HashSet<Long>(); 
  
  DatabaseIndexV2.SlaveDatabaseIndex _index;
  SlaveState    	_slaveState;  
  
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }
  
  public FileSystem getFileSystem() { 
    return _fileSystem;
  }
  
  public BaseConfig getBaseConfig() { 
    return _baseConfig;
  }
  
  public static String getPartId(int shardIndex) { 
    return "part-" + NUMBER_FORMAT.format(shardIndex);
  }
  
  
  public File getJobLocalPath() { 
    return new File(getDataDirectory(),"jobLocal");
  }

  @Override
  protected String getDefaultHttpInterface() {
    return CrawlEnvironment.DEFAULT_HTTP_INTERFACE;
  }

  @Override
  protected int getDefaultHttpPort() {
    return CrawlEnvironment.DEFAULT_QUERY_SLAVE_HTTP_PORT + (_instanceId * 2);
  }

  @Override
  protected String getDefaultLogFileName() {
    return "prslave.log";
  }

  @Override
  protected String getDefaultRPCInterface() {
    return CrawlEnvironment.DEFAULT_RPC_INTERFACE;
  }

  @Override
  protected int getDefaultRPCPort() {
    return CrawlEnvironment.DEFAULT_QUERY_SLAVE_RPC_PORT + (_instanceId * 2);
  }

  @Override
  protected String getWebAppName() {
    return CrawlEnvironment.QUERY_SLAVE_WEBAPP_NAME;
  }

  @SuppressWarnings("deprecation")
  @Override
  protected boolean initServer() {
    
    codec = new LzoCodec();
    
    if (_tempFileDir == null) {
      _tempFileDir = new File(getDataDirectory(),"qslave_temp");
      LOG.info("Temp File Dir does not existing. Defaulting to:"+ _tempFileDir.getAbsolutePath());
    }
    
    // create server channel ... 
    AsyncServerChannel channel = new AsyncServerChannel(this, this.getEventLoop(), this.getServerAddress(),this);
    
    // register RPC services it supports ... 
    registerService(channel,QueryServerSlave.spec);
    
    // make job local directory 
    getJobLocalPath().mkdirs();
    
    return true;
  }

  @Override
  protected boolean parseArguements(String[] argv) {
    for(int i=0; i < argv.length;++i) {
      
      if (argv[i].equalsIgnoreCase("--instance")) { 
        if (i+1 < argv.length) { 
          _instanceId = Integer.parseInt(argv[++i]);
          if (_instanceId < MIN_INSTANCE_ID || _instanceId > MAX_INSTANCE_ID) {
            System.err.println("Invalid Instance Id specified. Instance Id must be between " + MIN_INSTANCE_ID + " and " + MAX_INSTANCE_ID);
            return false;
          }
        }
      }
      else if (argv[i].equalsIgnoreCase("--tempFileDir")) { 
        _tempFileDir = new File(argv[++i]);
        _tempFileDir.mkdirs();
        if (!_tempFileDir.isDirectory()) { 
          LOG.error("Invalid Temp Directory Specified:" + _tempFileDir.getAbsolutePath());
          return false;
        }
      }
      
      else if (argv[i].equalsIgnoreCase("--threadPoolSize")) { 
        if (i+1 < argv.length) { 
          _threadPoolSize = Integer.parseInt(argv[++i]);
        }
      }
    }
    if (_instanceId == -1) { 
      System.err.println("Instance Id (--instance) and (optional) Thread Pool Size (--threadPoolSize) are required parameters.");
      return false;
    }
    return true;
  }
  
  @Override
  protected void overrideConfig(Configuration conf) { 
    conf.setInt("org.commoncrawl.threadpool.max.threads", _threadPoolSize);
  }
  
  @Override
  protected void printUsage() {
    // TODO Auto-generated method stub
    
  }

  @Override
  protected boolean startDaemons() {
    return true;
  }

  @Override
  protected void stopDaemons() {
    
  }

  @Override
  public void initialize(final AsyncContext<BaseConfig, SlaveStatus> rpcContext)throws RPCException {

    // terminate all active queries ... 
    terminateAndFlushAllQueries(
        
        new Callback() {

          @Override
          public void execute() {

            // we are still in the async thread here ... all existing queries have been cancelled at this point ... 
            // clear query info 
            _activeQueries.clear();
            _pendingQueries.clear();
            
            // clear out state ... 
            _slaveStatus.clear();
            _slaveStatus.setState(SlaveStatus.State.INITIALIZING);
            
            // reset cancel flag 
            _cancelling = false;
            
            // set up base config ... 
            try {
              _baseConfig = (BaseConfig) rpcContext.getInput().clone();
            } catch (CloneNotSupportedException e) {
            }
            // initialize the file system ... 
            try {
              _fileSystem = CrawlEnvironment.getDefaultFileSystem();
            } catch (Exception e) {
              // log the error
              LOG.error(CCStringUtils.stringifyException(e));
              // and fail the request ... 
              failRequest(rpcContext, "Unable to Initialize FileSystem.\n" + CCStringUtils.stringifyException(e));
              
              return;
            } 

            if (!_baseConfig.isFieldDirty(BaseConfig.Field_QUERYDBPATH)) { 
              LOG.error("No QueryDB Path Specified in BaseConfig");
              failRequest(rpcContext, "No QueryDB Path Specified in BaseConfig");
            }
            new Thread(new Runnable() {

              @Override
              public void run() {
                boolean loaded = false;
                try { 
                  LOG.info("Loading SlaveDatabase Index");
                  _index = new DatabaseIndexV2.SlaveDatabaseIndex(_configuration, _fileSystem, _baseConfig.getDatabaseTimestamp());
                  LOG.info("Loaded Database Index");    
                      
                  // register thread pool 
                  loaded = true;
                }
                catch (IOException e) { 
                  LOG.error("Data File Load Failed with exception:" +CCStringUtils.stringifyException(e));
                }
                
                final boolean loadedStatus = loaded;
                  getEventLoop().setTimer(new Timer(1,false,new Timer.Callback() {

                    @Override
                    public void timerFired(Timer timer) {
                      if (loadedStatus) { 
                        LOG.info("All Data Files successfully loaded. finishing initialization");
                        finishInitialize(rpcContext);
                      }
                      else { 
                        failRequest(rpcContext, "Failed to load Data Files");
                      }
                    }
                  }));
              }
            }).start();
            
          } 
          
        });
  }
  
  private File copyAcrossQueryDBFile(Path remotePath)throws IOException {
    FileSystem fileSystem = CrawlEnvironment.getDefaultFileSystem();
    
    // get the status of the specified file 
    FileStatus fileStatus = fileSystem.getFileStatus(remotePath);
    File       localDirectory  = new File(getJobLocalPath(),remotePath.getParent().getName());
    if (!localDirectory.exists()) { 
      localDirectory.mkdirs();
    }

    File       localFile  = new File(localDirectory,remotePath.getName());
    
    if (localFile.exists() == false || localFile.length() != fileStatus.getLen()) { 
      localFile.delete();
      LOG.info("Copying Remote File:" + remotePath + " to " + localFile);
      fileSystem.copyToLocalFile(remotePath, new Path(localFile.getAbsolutePath()));
    }
    else { 
      LOG.info("Skipping Copy of Remote File:" + remotePath + " to " + localFile);
    }
    return localFile;
  }
    
  private File getTempDirForQuery(long queryId) { 
    return new File(_tempFileDir,Long.toString(queryId));
  }

  
  private void finishInitialize(AsyncContext<BaseConfig, SlaveStatus> rpcContext) { 
    // and update slave status state 
    _slaveStatus.setState(SlaveStatus.State.READY);
    // create a slave state object ... 
    _slaveState = new SlaveState(getHostName(),_index);
    
    sendStatusResponse(rpcContext);
  }
  
  private void sendStatusResponse(AsyncContext<? extends RPCStruct,SlaveStatus> context) { 
    try {
      // get base status
      context.setOutput((SlaveStatus) _slaveStatus.clone());
      
      // log it ... 
      if (context.getOutput().getQueryStatus().size() != 0) { 
        LOG.info("Sending a non-zero query status list in heartbeat response");
      }
      // clear query status in slave status ... 
      _slaveStatus.getQueryStatus().clear();
      
    } catch (CloneNotSupportedException e) {
    }
    try {
      context.completeRequest();
    } catch (RPCException e) {
      LOG.error("fail to send StatusResponse to incoming RPC. CLOSING RPC Channel");
      try {
        context.getClientChannel().close();
      } catch (IOException e1) {
        LOG.error(e1);
      }
    }
  }
  
  private void potentiallyStartNextQuery() { 
    while (_activeQueries.size() < Common.MAX_CONCURRENT_QUERIES && _pendingQueries.size() != 0) { 
      // remove next from queue 
    	Query queryObject = _pendingQueries.removeFirst();
      // and activate 
    	activateQuery(queryObject);
    }
  }

  private void activateQuery(Query queryObject) { 
    LOG.info("Activating Query:" + queryObject.getQueryId());
    _activeQueries.put(queryObject.getQueryId(),queryObject);
    // create temporary work directory 
    File queryTempDir = getTempDirForQuery(queryObject.getQueryId());
    
    LOG.info("Query TempDir for Query:" + queryObject.getQueryId() + " is:" + queryTempDir.getAbsolutePath());

    try {
      LOG.info("Deleting Query TempDir");
      FileUtils.recursivelyDeleteFile(queryTempDir);
      LOG.info("Re-creating TempDir");
      queryTempDir.mkdirs();
      
      LOG.info("Starting Slave Query for Query:" + queryObject.getQueryId());
      // start the query thread ... 
      queryObject.startSlaveQuery(this._fileSystem,this._configuration,getEventLoop(),_index,queryTempDir,this, this);
      // and update the status.
      updateSlaveStatusForQueryObject(queryObject);
    } catch (IOException e) {
      LOG.error("Query Activation for Query:"+ queryObject.getQueryId() +" Failed with Exception:" + CCStringUtils.stringifyException(e));
      // remove from active list ... 
      _activeQueries.remove(queryObject.getQueryId());
      // mark as failed ... 
      queryObject.getQueryStatus().setStatus(QueryStatus.Status.ERROR);
      queryObject.getQueryStatus().setOptErrorReason(CCStringUtils.stringifyException(e));

      FileUtils.recursivelyDeleteFile(queryTempDir);
      
      updateSlaveStatusForQueryObject(queryObject);
    }
  	
  }
  private void updateSlaveStatusForQueryObject(Query theQueryObject) { 
    boolean found = false;
    
    LOG.info("updateSlaveStatusForQueryObject called for Query:" + theQueryObject.getQueryId());
    LOG.info("Updating Query Status for Query:" + theQueryObject.getQueryId() + " Status:" + QueryStatus.Status.toString(theQueryObject.getQueryStatus().getStatus()));
    
    for (QueryStatus status : _slaveStatus.getQueryStatus()) { 
      // if query ids match 
      if (status.getQueryId() == theQueryObject.getQueryId()) { 
        try {
          LOG.info("Merging into Existing Query Status");
          status.merge(theQueryObject.getQueryStatus());
          found = true;
        } catch (CloneNotSupportedException e) {
          LOG.error(CCStringUtils.stringifyException(e));
        }
        break;
      }
    }
    if (!found) { 
      QueryStatus queryStatus = null;
      try {
        LOG.info("Cloning a NEW Query Status");
        queryStatus = (QueryStatus) theQueryObject.getQueryStatus().clone();
      } catch (CloneNotSupportedException e) {
        LOG.error(CCStringUtils.stringifyException(e));
      }
      _slaveStatus.getQueryStatus().add(queryStatus);
    }
  }
 
  
  @Override
  public void doQuery(AsyncContext<RemoteQueryInfo, QueryStatus> rpcContext) throws RPCException {
    LOG.info("Adding Query Type:"+ rpcContext.getInput().getQueryClassType() + "Id:" + rpcContext.getInput().getCommonInfo().getQueryId() + " to Queue.");
    try {
      // extract object type 
      String queryObjectType = rpcContext.getInput().getQueryClassType();
      LOG.info("QueryId:" + rpcContext.getInput().getCommonInfo().getQueryId() + " ObjectType:" + queryObjectType);
      // and data type 
      String queryDataType   = rpcContext.getInput().getQueryDataClassType();
      LOG.info("QueryId:" + rpcContext.getInput().getCommonInfo().getQueryId() + " QueryDataType:" + queryDataType);
      // allocate the object data type .. 
      RPCStruct queryData = (RPCStruct) Class.forName(queryDataType).newInstance();
      LOG.info("QueryId:" + rpcContext.getInput().getCommonInfo().getQueryId() + " DeSerializing Query Data");
      // allocate an input stream  
      DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(rpcContext.getInput().getQueryDataBuffer().getReadOnlyBytes()));
      // and deserialize into the structure 
      queryData.deserialize(inputStream,new BinaryProtocol());
      LOG.info("QueryId:" + rpcContext.getInput().getCommonInfo().getQueryId() + " Allocating Query Object");
      // now allocate query object 
      Query queryObject = (Query) Class.forName(queryObjectType).newInstance();
      LOG.info("QueryId:" + rpcContext.getInput().getCommonInfo().getQueryId() + " Initializing QueryObject");
      // initialize query 
      queryObject.initializeRemoteQuery(rpcContext.getInput().getClientQueryData(), _slaveState,rpcContext.getInput().getShardMapping(),rpcContext.getInput().getCommonInfo(),queryData);
      LOG.info("QueryId:" + rpcContext.getInput().getCommonInfo().getQueryId() + " Adding to Pending Queue");
      //TODO: SEE IF WE CAN IMMEDIATELY EXECUTE QUERY ...
      if (queryObject.isHighPriorityQuery()) { 
      	// high priority query ... dispatch immediately ...
      	activateQuery(queryObject);
      }
      else { 
	      // add to pending set ... 
	      _pendingQueries.add(queryObject);
      }
      // add query to query status structure ...
      updateSlaveStatusForQueryObject(queryObject);
      // now potentially start next query ... 
      potentiallyStartNextQuery();
      // now send the query's current status back to caller 
      rpcContext.getOutput().merge(queryObject.getQueryStatus());
    }
    catch (Exception e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      LOG.error("Query Dispatch for Query Id:" + rpcContext.getInput().getCommonInfo().getQueryId() + " Failed with Exception:" + CCStringUtils.stringifyException(e));
      rpcContext.setStatus(AsyncRequest.Status.Error_RequestFailed);
      rpcContext.setErrorDesc(CCStringUtils.stringifyException(e));
    }
    // complete request ... 
    rpcContext.completeRequest();
  }
  
  

  @Override
  public void heartbeat(AsyncContext<NullMessage, SlaveStatus> rpcContext)throws RPCException {
    //LOG.info("Got Heartbeat from Master - Sending Status to Master");
    sendStatusResponse(rpcContext);
  }
 
  private void failRequest(AsyncContext<? extends RPCStruct,? extends RPCStruct> rpcContext,String reason) {
    LOG.info("failRequest called");
    // not good... time to fail the request ... 
    rpcContext.setStatus(AsyncRequest.Status.Error_RequestFailed);
    rpcContext.setErrorDesc(reason);
    try {
      rpcContext.completeRequest();
    } catch (RPCException e) {
      LOG.error(CCStringUtils.stringifyException(e));
      try {
        rpcContext.getClientChannel().close();
      } catch (IOException e2) {
      }
    }
  }
  
  @Override
  protected String getDefaultDataDir() {
    return "data";
  }

  @Override
  public void IncomingClientConnected(AsyncClientChannel channel) {
    LOG.info("Incoming Channel Connected");
  }

  @Override
  public void IncomingClientDisconnected(AsyncClientChannel channel) {
    LOG.info("Channel Disconnected");
  }


  private void terminateAndFlushAllQueries(final Callback callback) { 
    
    _cancelling = true;
    
    if (_activeQueries.size() == 0) {
      // execute callback immediately 
      callback.execute();
    }
    else { 
      // otherwise terminate queries in a background thread ... 
      final Vector<Query> activeQueries = new Vector<Query>(_activeQueries.values());
          
      getDefaultThreadPool().submit(new ConcurrentTask<Boolean>(_eventLoop,new Callable<Boolean>() {
  
        
        @Override
        public Boolean call() throws Exception {
          LOG.info("Starting Cancel Thread");
           for (Query query : activeQueries) {
             LOG.info("Cancelling Query:" + query.getQueryId());
             try { 
               query.cancelSlaveQuery();
             }
             catch (Exception e) { 
               LOG.error("Error Cancelling Query:" + query.getQueryId() + " Error:" + CCStringUtils.stringifyException(e));
             }
             LOG.info("Cancelled Query:" + query.getQueryId());
           }
           
           return true;
        } 
        
      }, new ConcurrentTask.CompletionCallback<Boolean>() {
  
        @Override
        public void taskComplete(Boolean loadResult) {
          _cancelling = false;
          callback.execute();
        }
  
        @Override
        public void taskFailed(Exception e) {
          _cancelling = false;
          LOG.error(CCStringUtils.stringifyException(e));
          callback.execute();
        } 
        
      }));
    }
  }

  @Override
  public void queryComplete(Query theQueryObject, long resultCount) {
    
    LOG.info("QueyComplete received for Query:" + theQueryObject.getQueryId() + " resultCount:" + resultCount);
    // this callback occurs in the context of the async thread ...
    if (!_cancelling) {
      synchronized (_cancelledQueries) { 
        // if this query was cancelled ... 
        if (_cancelledQueries.contains(theQueryObject.getQueryId())) {
          // clear out the entry in the array 
          _cancelledQueries.remove(theQueryObject.getQueryId());
          LOG.info("Query Seems to have been cancelled. Explicitly cancelling Query:" + theQueryObject.getQueryId());
          // override status 
          theQueryObject.getQueryStatus().setStatus(QueryStatus.Status.CANCELLED);
        }
      }
      
      
      // update the slave status according to the query status
      updateSlaveStatusForQueryObject(theQueryObject);
      // remove the query from the active queue ... 
      _activeQueries.remove(theQueryObject.getQueryId());
      
      FileUtils.recursivelyDeleteFile(getTempDirForQuery(theQueryObject.getQueryId()));
    }
  }

  @Override
  public void queryFailed(Query theQueryObject, String reason) {
    LOG.info("QueryFailed received for Query:" + theQueryObject.getQueryId() + " reason:" + reason);
    if (!_cancelling) {
     
     synchronized (_cancelledQueries) { 
       // if this query was cancelled ... 
       if (_cancelledQueries.contains(theQueryObject.getQueryId())) {
         // clear out the entry in the array 
         _cancelledQueries.remove(theQueryObject.getQueryId());
         // override status 
         theQueryObject.getQueryStatus().setStatus(QueryStatus.Status.CANCELLED);
       }
     }
     // update the slave status according to the query status
     updateSlaveStatusForQueryObject(theQueryObject);
     // remove the query from the active queue ... 
     _activeQueries.remove(theQueryObject.getQueryId());
     
     FileUtils.recursivelyDeleteFile(getTempDirForQuery(theQueryObject.getQueryId()));
   }
  }

  @Override
  public boolean updateProgress(final Query theQueryObject, float percentComplete) {
    
    LOG.info("Update Progress Received for Query:" + theQueryObject.getQueryId() + "pctComplete:" + percentComplete);

    //TODO: WE NEED TO UPDATE slave status for this query here .
    if (!_cancelling) { 
      synchronized (_cancelledQueries) {
       // if the query object is in the cancelled set ... 
       if (_cancelledQueries.contains(theQueryObject.getQueryId())) {
         // remove it from the cancel set
         _cancelledQueries.remove(theQueryObject.getQueryId());
         // return false to indicate that query execution should terminate prematurely 
         return false;
       }
       // return true to indicate that query execution should continue 
       return true;
      }
    }
    else { 
      // return false to indicate that query execution should terminate prematurely 
      return false;
    }
  }

  @Override
  public void cancelQuery(AsyncContext<QueryCommon, NullMessage> rpcContext)throws RPCException {
    if (_activeQueries.containsKey(rpcContext.getInput().getQueryId())) { 
      //TODO: WE WILL NEED TO PERIODICALLY FLUSH THIS SET ...
      _cancelledQueries.add(rpcContext.getInput().getQueryId());
    }
  }
}
