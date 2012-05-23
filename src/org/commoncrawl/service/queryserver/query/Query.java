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
package org.commoncrawl.service.queryserver.query;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.record.Buffer;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.async.Timer;
import org.commoncrawl.rpc.base.internal.AsyncRequest;
import org.commoncrawl.rpc.base.internal.AsyncRequest.Callback;
import org.commoncrawl.rpc.base.shared.BinaryProtocol;
import org.commoncrawl.rpc.base.shared.RPCStruct;
import org.commoncrawl.service.queryserver.ClientQueryInfo;
import org.commoncrawl.service.queryserver.QueryCommon;
import org.commoncrawl.service.queryserver.QueryStatus;
import org.commoncrawl.service.queryserver.RemoteQueryInfo;
import org.commoncrawl.service.queryserver.ShardIndexHostNameTuple;
import org.commoncrawl.service.queryserver.index.DatabaseIndexV2;
import org.commoncrawl.service.queryserver.master.QueryServerSlaveState;
import org.commoncrawl.service.queryserver.slave.SlaveState;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.FileUtils;

/**
 * 
 * @author rana
 *
 * @param <DataType>
 * @param <ResultKeyType>
 * @param <ResultValueType>
 */
public abstract class Query<DataType extends RPCStruct,ResultKeyType,ResultValueType> {
  
  private static final Log LOG = LogFactory.getLog(Query.class);
  
  private class SlaveStatusInfo {
    
    public SlaveStatusInfo(QueryServerSlaveState slaveState,QueryStatus queryStatus) { 
      _onlineState = slaveState;
      _queryStatus = queryStatus;
     
    }
    
    
    
    private QueryServerSlaveState _onlineState;
    private QueryStatus           _queryStatus;
    public  boolean               _logged = false;
    
    public QueryServerSlaveState getOnlineState() { return _onlineState; } 
    public QueryStatus     getQueryStatus() { return _queryStatus; }
  }
  
  protected static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }

  protected ClientQueryInfo _clientQueryInfo;
  protected QueryCommon     _commonInfo;
  protected DataType        _queryData;
  private   Object          _contextObj;
  private   RemoteQueryInfo _queryDetails;
  protected QueryStatus     _queryStatus;
  protected SlaveState      _slaveState;
  protected DatabaseIndexV2.SlaveDatabaseIndex   _slaveDatabaseIndex;
  protected Thread          _queryThread;
  protected boolean         _cancelQuery;
  protected TreeMap<String,SlaveStatusInfo> _remoteQueryStates = null;
  protected RemoteQueryCompletionCallback _completionCallback;
	protected ArrayList<ShardIndexHostNameTuple> _shardIdToHostMapping = new ArrayList<ShardIndexHostNameTuple>();

  
  /** master side query object constructor **/
  public Query() { 
    _commonInfo = new QueryCommon();
    _queryStatus = new QueryStatus();
    _queryStatus.setStatus(QueryStatus.Status.PENDING);
  }
  
  protected void setQueryData(DataType queryData) { _queryData = queryData; }
  protected DataType getQueryData() { return _queryData; }
  
  
  /** remote query initialization *
   * @return */
  public void initializeRemoteQuery(ClientQueryInfo queryInfo,SlaveState slaveState,ArrayList<ShardIndexHostNameTuple> shardMappings,QueryCommon commonInfo,RPCStruct queryData) { 
    try {
      _slaveState = slaveState;
      _clientQueryInfo  = queryInfo;
      // iterate mappings and pickup shard ids assigned to this host 
      for (ShardIndexHostNameTuple tuple : shardMappings) { 
      	if (tuple.getHostName().compareTo(_slaveState.getHostName()) == 0) { 
      		commonInfo.getRelevantShardIds().add(tuple.getShardId());
      	}
      }
      _commonInfo = (QueryCommon)commonInfo.clone();
      _queryData  = (DataType) queryData.clone();
      _slaveDatabaseIndex = _slaveState.getSharedIndex();
      _queryStatus.setQueryId(_commonInfo.getQueryId());
    } catch (CloneNotSupportedException e) {
    }
  }
  
  /** get query status **/
  public QueryStatus getQueryStatus() { return _queryStatus; }
  
  // get the common query info 
  public  QueryCommon getCommonQueryInfo() { return _commonInfo; }
  
  // get client query info 
  public ClientQueryInfo getClientQueryInfo() { return _clientQueryInfo; }
  // set client query info 
  public void setClientQueryInfo(ClientQueryInfo info) { _clientQueryInfo = info; } 
  
  // get shard id to host mapping - only valid at master 
  public ArrayList<ShardIndexHostNameTuple> getShardIdToHostMapping() { return _shardIdToHostMapping; }
  // set shard id to host mapping - only set by master 
  public void setShardIdToHostMapping(ArrayList<ShardIndexHostNameTuple> mapping) { _shardIdToHostMapping = mapping; } 
  
  
  public Object getContextObject() { return _contextObj; }
  public void   setContext(Object context) { _contextObj = context; }
  
  /** start a slave query thread **/
  public void startSlaveQuery(
      final FileSystem fileSystem,
      final Configuration conf,
      final EventLoop eventLoop,
      final DatabaseIndexV2.SlaveDatabaseIndex sourceIndex,
      final File tempFileDir,
      final QueryProgressCallback<DataType,ResultKeyType,ResultValueType> progressCallback,
      final RemoteQueryCompletionCallback completionCallback) throws IOException {
    // update query status ...
    _queryStatus.setStatus(QueryStatus.Status.RUNNING);
    
    final DatabaseIndexV2.SlaveDatabaseIndex instanceIndex = sourceIndex;
    final File queryTempDir = new File(tempFileDir,Long.toString(getQueryId()));
    
    // delete the tempfile directory ...
    FileUtils.recursivelyDeleteFile(queryTempDir);
    // and recreate it ... 
    queryTempDir.mkdirs();
    
    _queryThread = new Thread(new Runnable() {

      @Override
      public void run() {
        try {
          LOG.info("Slave Query Thread:Executing for Query:" + getQueryId());
          // execute the specific query object.
          long resultCount = executeRemote(fileSystem,conf,eventLoop,instanceIndex,queryTempDir,new QueryProgressCallback<DataType,ResultKeyType,ResultValueType>() {

            @Override
            public boolean updateProgress(final Query<DataType,ResultKeyType,ResultValueType> theQueryObject,float percentComplete) {
              synchronized (Query.this) {
                if (_cancelQuery) {
                  _queryStatus.setStatus(QueryStatus.Status.CANCELLED);
                  return false;
                }
                else { 
                  _queryStatus.setProgress(percentComplete);
                }
              }
              return progressCallback.updateProgress(theQueryObject,percentComplete);
            } 
            
          });
          
          // udpate query status 
          synchronized (Query.this) {
            if (_queryStatus.getStatus() != QueryStatus.Status.CANCELLED) { 
              _queryStatus.setStatus(QueryStatus.Status.FINISHED);
              _queryStatus.setOptResultCount(resultCount);
            }
          }
          
          if (eventLoop != null) { 
            final long finalResultCount = resultCount;
            //schedule call asynchronously ... 
            eventLoop.setTimer(new Timer(0,false,new Timer.Callback() {

              @Override
              public void timerFired(Timer timer) {
                // execute directly from thread ... 
                LOG.info("Query:" + getQueryId() +" Completed with:" + finalResultCount + " results.");
                // and complete... 
                completionCallback.queryComplete(Query.this,finalResultCount);
              } 
            }));
          }
          else {
            // execute directly from thread ... 
            LOG.info("Query:" + getQueryId() +" Completed with:" + resultCount + " results.");
            // and complete... 
            completionCallback.queryComplete(Query.this,resultCount);
          }
        }
        catch (final Exception e){
          
          synchronized (Query.this) {
            _queryStatus.setStatus(QueryStatus.Status.ERROR);
            _queryStatus.setOptErrorReason(CCStringUtils.stringifyException(e));
          }
          
          if (eventLoop != null) { 
            //schedule call asynchronously ... 
            eventLoop.setTimer(new Timer(0,false,new Timer.Callback() {

              @Override
              public void timerFired(Timer timer) {
                LOG.error(CCStringUtils.stringifyException(e));
                completionCallback.queryFailed(Query.this, "Query:" + getQueryId() +" Failed with Exception:" + CCStringUtils.stringifyException(e));
              } 
            }));
          }
          else {
            //execute callback in current thread's context
            LOG.error(CCStringUtils.stringifyException(e));
            completionCallback.queryFailed(Query.this, "Query:" + getQueryId() +" Failed with Exception:" + CCStringUtils.stringifyException(e));
          }
        }
        finally { 
          // delete temp file directory no matter what 
          FileUtils.recursivelyDeleteFile(queryTempDir);
        }
        
      } 
    });
    
    LOG.info("Starting Slave Query Thread for Query:" + getQueryId());
    _queryThread.start();
  }
  
  /** start client query thread**/
  public void startLocalQuery(final FileSystem fileSystem,final Configuration conf,final DatabaseIndexV2.MasterDatabaseIndex index,final File tempFileDir,final EventLoop eventLoop,final QueryRequest<DataType,ResultKeyType,ResultValueType> queryRequest,final RemoteQueryCompletionCallback completionCallback) {

    // set up appropriate status ... 
    queryRequest.getQueryStatus().setStatus(QueryStatus.Status.RUNNING);
    
    _queryThread = new Thread(new Runnable() {

      @Override
      public void run() {
        LOG.info("Client Query Thread.Executing for Query:" + getQueryId());
        // execute the specific query object.
        try {
          
          // create temp file dir ... 
          FileUtils.recursivelyDeleteFile(tempFileDir);
          
          LOG.info("Client Query Thread for:" + getQueryId() +" creating temp file directory:" + tempFileDir.getAbsolutePath());
          tempFileDir.mkdirs();
          
          LOG.info("Executing Local Query for Query:" + getQueryId());
          // execute local query 
          final long resultCount = executeLocal(fileSystem,conf,index,eventLoop,tempFileDir,queryRequest);
          
          // and callback in async thread context...
          eventLoop.setTimer(new Timer(0,false,new Timer.Callback() {
            @Override
            public void timerFired(Timer timer) {
              LOG.info("Local QueryComplete for Query:" + getQueryId());
              completionCallback.queryComplete(queryRequest.getSourceQuery(),resultCount);
            }
          }));
          
        } catch (final IOException e) {
          LOG.error("Query: " + getQueryId() + " Failed on executeLocal with Error:" + CCStringUtils.stringifyException(e));
          final String error = CCStringUtils.stringifyException(e);
          eventLoop.setTimer(new Timer(0,false,new Timer.Callback() {
            @Override
            public void timerFired(Timer timer) {
              completionCallback.queryFailed(queryRequest.getSourceQuery(),error);
            }
          }));
        }
      } 
    });
    
    LOG.info("Starting Local Query Thread for Query:" + getQueryId());
    _queryThread.start();
  }

  /** start client query thread**/
  public void startCacheQuery(final DatabaseIndexV2.MasterDatabaseIndex index,final FileSystem fileSystem,final Configuration conf,final EventLoop eventLoop,final QueryRequest<DataType,ResultKeyType,ResultValueType> queryRequest,final QueryCompletionCallback<DataType,ResultKeyType,ResultValueType> completionCallback) {

    // set up appropriate status ... 
    queryRequest.getQueryStatus().setStatus(QueryStatus.Status.RUNNING);
    
    _queryThread = new Thread(new Runnable() {

      @Override
      public void run() {
        LOG.info("Executing Cache Query for Query:" + getQueryId());
        // execute the specific query object.
        try {
          // execute cache query 
          getCachedResults(fileSystem,conf,eventLoop,index,queryRequest, new QueryCompletionCallback<DataType,ResultKeyType,ResultValueType>() {

            @Override
            public void queryComplete(final QueryRequest<DataType,ResultKeyType,ResultValueType> request,final QueryResult<ResultKeyType,ResultValueType> queryResult) {
              // and call outer callback in async thread context...
              eventLoop.setTimer(new Timer(0,false,new Timer.Callback() {
                @Override
                public void timerFired(Timer timer) {
                  LOG.info("Calling queryComplete on cacheRequest for Query:" + getQueryId());
                  completionCallback.queryComplete(queryRequest,queryResult);
                  //LOG.info("Finished Calling queryComplete for Query:" + getQueryId());

                }
              }));
            }

            @Override
            public void queryFailed(final QueryRequest<DataType,ResultKeyType,ResultValueType> request, final String reason) {
              // and call outer callback in async thread context...
              eventLoop.setTimer(new Timer(0,false,new Timer.Callback() {
                @Override
                public void timerFired(Timer timer) {
                  LOG.info("Calling queryFailed on cacheRequest for Query:" + getQueryId() + " Reason:" + reason);
                  completionCallback.queryFailed(queryRequest,reason);
                  //LOG.info("Finished Calling queryFailed for Query:" + getQueryId());
                }
              }));
            }


          });
          
        } catch (final IOException e) {
          LOG.error("Query: " + getQueryId() + " Failed on cacheQuery with Error:" + CCStringUtils.stringifyException(e));
          final String error = CCStringUtils.stringifyException(e);
          eventLoop.setTimer(new Timer(0,false,new Timer.Callback() {
            @Override
            public void timerFired(Timer timer) {
              completionCallback.queryFailed(queryRequest,error);
            }
          }));
        }
      } 
    });
    
    LOG.info("Starting Cache Query Thread for Query:" + getQueryId());
    _queryThread.start();
  }  
  
  /** start the remote query **/
  public void startRemoteQuery(Map<String,QueryServerSlaveState> slaveToOnlineStateMapping,ArrayList<ShardIndexHostNameTuple> shardIdMapping,
  		final QueryProgressCallback<DataType,ResultKeyType,ResultValueType> progressCallback, final RemoteQueryCompletionCallback completionCallback) throws IOException {
    LOG.info("Starting Remote(Master)Query for Query:" + getQueryId());
    
    // construct a query details object 
    _queryDetails = new RemoteQueryInfo();
    
    // populate it ... 
    _queryDetails.setCommonInfo(getCommonQueryInfo());
    // populate client query info 
    _queryDetails.setClientQueryData(getClientQueryInfo());
    // set class info based on 'this'
    _queryDetails.setQueryClassType(getClass().getName());
    // set data type
    _queryDetails.setQueryDataClassType(_queryData.getClass().getName());
    // set shard id mapping 
    _queryDetails.getShardMapping().addAll(shardIdMapping);
    // marshall data buffer
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(baos);
    _queryData.serialize(outputStream,new BinaryProtocol());
    // and add it to query details
    _queryDetails.setQueryDataBuffer(new Buffer(baos.toByteArray()));
       
    
    // store completion callback 
    _completionCallback = completionCallback;
    
    // update query status ...
    _queryStatus.setStatus(QueryStatus.Status.RUNNING);
    
    // allocate query state vector 
    _remoteQueryStates = new TreeMap<String,SlaveStatusInfo>();
    
    // build a dispatch list
    Collection<QueryServerSlaveState> hostsToDispatchTo = buildDispatchListGivenShardIdMap(slaveToOnlineStateMapping,shardIdMapping);
    
    
    // populate it ... 
    for (QueryServerSlaveState slave : hostsToDispatchTo) {
      LOG.info("Dispatching Query:" + getQueryId() + " to Slave:" + slave.getFullyQualifiedName());  
      // allocate a new query status 
      QueryStatus queryStatus = new QueryStatus();
      // setup basic structure ... 
      queryStatus.setQueryId(getQueryId());
      queryStatus.setStatus(QueryStatus.Status.PENDING);
      // allocate super structure 
      SlaveStatusInfo statusInfo = new SlaveStatusInfo(slave,queryStatus);
      // add it to vector ... 
      _remoteQueryStates.put(slave.getHostName(),statusInfo);
    }
    LOG.info("Dispatching Slave Queries for Query:" + getQueryId());
    updateMasterQueryStatus();
  }
  
  public void updateQueryStatusForSlave(String hostName,QueryStatus statusUpdate) throws IOException{
  	SlaveStatusInfo slaveInfo = _remoteQueryStates.get(hostName);
    if (slaveInfo != null) { 
      //LOG.info("Recevied Query Status Update for Query:" + getQueryId() + " Slave:" + slaveInfo.getOnlineState().getFullyQualifiedName());
      if (statusUpdate.getStatus() == QueryStatus.Status.ERROR && slaveInfo._queryStatus.getStatus() != QueryStatus.Status.ERROR) { 
        LOG.info("Slave:" + slaveInfo.getOnlineState().getFullyQualifiedName() + " Reported Error:" + statusUpdate.getOptErrorReason() + " for Query:" + getQueryId());
      }
      else if (statusUpdate.getStatus() == QueryStatus.Status.FINISHED && slaveInfo._queryStatus.getStatus() != QueryStatus.Status.FINISHED) { 
        LOG.info("Slave:" + slaveInfo.getOnlineState().getFullyQualifiedName() + " Reported FINISHED for Query:" + getQueryId() + " ResultCount:" + statusUpdate.getOptResultCount());
      }
      try {
        slaveInfo._queryStatus.merge(statusUpdate);
      } catch (CloneNotSupportedException e) {
      }
      
      updateMasterQueryStatus();
    }
    else { 
    	LOG.error("Query: " + getQueryId() + " Received Status Update from Unknown Host:" + hostName);
    }
  }
  
  private void updateMasterQueryStatus() throws IOException { 
    int completedCount = 0;
    int failedCount    = 0;
    long totalResultCount =0;
    String failureReason = "";
    
    //LOG.info("Update MasterQueryStatus called for Query:" + getQueryId());
    
    for (SlaveStatusInfo slaveStatus : _remoteQueryStates.values()) { 
      if (slaveStatus._queryStatus.getStatus() == QueryStatus.Status.PENDING) {
        
        if (slaveStatus._onlineState.isOnline()) {
          // LOG.info("Sending Remote Query to Slave:" + slaveStatus.getOnlineState().getFullyQualifiedName() + " for Query:" + getQueryId() );
          try { 
            // flip status to running 
            slaveStatus._queryStatus.setStatus(QueryStatus.Status.RUNNING);
            // and displatch the actual query ...
            dispatchQueryToSlave(slaveStatus);
          }
          catch (IOException e){
            LOG.error("Remote RPC For Query:" + getQueryId() + " to Slave:" + slaveStatus.getOnlineState().getFullyQualifiedName()
                + " Failed with Exception:" + CCStringUtils.stringifyException(e));
            
          }
        }
      }
      else if (slaveStatus._queryStatus.getStatus() == QueryStatus.Status.FINISHED) {
        
        /*
        if (!slaveStatus._logged) { 
          LOG.info("Received Remote Query Finished from Slave:" + slaveStatus.getOnlineState().getFullyQualifiedName() + " Query:" + getQueryId() + 
              " ResultCount:" + slaveStatus._queryStatus.getOptResultCount());
          slaveStatus._logged = true;
        }
        */
          
        totalResultCount += slaveStatus._queryStatus.getOptResultCount();
        
        completedCount++;
      }
      else if (slaveStatus._queryStatus.getStatus() == QueryStatus.Status.ERROR) {
        failureReason +="\n";
        failureReason += "Failed on Slave:" + slaveStatus._onlineState.getHostName() + " for Query:" + getQueryId() +"\n";
        failureReason += "Failure Reason:" + slaveStatus._queryStatus.getOptErrorReason();
        failedCount++;
      }
    }
    
    // if completed + failed count == slave count ... 
    if ( completedCount + failedCount == _remoteQueryStates.size() || (completedCount == 1 && totalResultCount == 1 && isSingleRequestQuery()) ) {
      //TODO: right now partial failures are considered a complete failure ... 
      if (failedCount != 0) {
        
        LOG.info("Query:" + getQueryId() + " Had :" + failedCount + " Failures");
        
        //if (completedCount == 0) { 
          // udpate query status 
          _queryStatus.setStatus(QueryStatus.Status.ERROR);
          _queryStatus.setOptErrorReason(failureReason);
          _completionCallback.queryFailed(this,failureReason);
        // }
      }
      
      // if (completedCount != 0)
      else
      {
        LOG.info("Query:" + getQueryId() + " Completed with Result Count:" + totalResultCount);
        // udpate query status 
        _queryStatus.setStatus(QueryStatus.Status.FINISHED);
        _queryStatus.setOptResultCount(totalResultCount);
        _completionCallback.queryComplete(this,totalResultCount);
      }
      
      // clear state ... 
      _remoteQueryStates.clear();
    }
  }
  
 
  public long getQueryId() { return getCommonQueryInfo() .getQueryId(); }
  public void setQueryId(long queryId) { 
    
    getCommonQueryInfo().setQueryId(queryId);
    _queryStatus.setQueryId(queryId);
  }
  
  /** cancel a slave query **/
  public void cancelSlaveQuery() { 
    synchronized (this) {
      _cancelQuery = true;
    }
    if (_queryThread != null) { 
      try {
        _queryThread.join();
      } catch (InterruptedException e) {
      }
    }
  }
  
  public void waitOnQuery() { 
    if (_queryThread != null) { 
      try {
        _queryThread.join();
      } catch (InterruptedException e) {
      }
    }
  }

  final void dispatchQueryToSlave(final SlaveStatusInfo slave)throws IOException { 
    //LOG.info("Dispatching Query:" + getQueryId() + " to Slave:" + slave.getOnlineState().getFullyQualifiedName());
    slave.getOnlineState().getRemoteStub().doQuery(_queryDetails, new Callback<RemoteQueryInfo, QueryStatus>() {
      @Override
      public void requestComplete(AsyncRequest<RemoteQueryInfo, QueryStatus> request) {
        
        if (request.getStatus() != AsyncRequest.Status.Success) {
          //LOG.error("Query:" + getQueryId() + " To Slave:" + slave.getOnlineState().getFullyQualifiedName()  + " Failed with RPC Error:" + request.getStatus());
          _remoteQueryStates.get(slave.getOnlineState().getHostName())._queryStatus.setStatus(QueryStatus.Status.PENDING);
        }
        else { 
        	LOG.info("Query:" + getQueryId() + " To Slave:" + slave.getOnlineState().getFullyQualifiedName()  + " return Status:" + request.getOutput().getStatus());
        	try {
	          _remoteQueryStates.get(slave.getOnlineState().getHostName())._queryStatus.merge(request.getOutput());
          } catch (CloneNotSupportedException e) {
          	// NOOP
          }
        }
      } 
    });
    //LOG.info("Done Dispatching Query:" + getQueryId() + " to Slave:" + slave.getOnlineState().getFullyQualifiedName());
  }
  

  /** run the local porition of this query (runs on master)**/
  protected long executeLocal(final FileSystem fileSyste,final Configuration conf,DatabaseIndexV2.MasterDatabaseIndex index,final EventLoop eventLoop,final File tempFirDir,QueryRequest<DataType,ResultKeyType,ResultValueType> requestObject)throws IOException { return 0; }

  /** get cached results (runs on master only)**/
  public    abstract void    getCachedResults(final FileSystem fileSystem,final Configuration conf,final EventLoop eventLoop,final DatabaseIndexV2.MasterDatabaseIndex masterIndex,final QueryRequest<DataType,ResultKeyType,ResultValueType> theClientRequest,final QueryCompletionCallback<DataType,ResultKeyType,ResultValueType> callback)throws IOException;
  
  /** get a unique canonical id for this query **/
  public    abstract String getCanonicalId();
  
  /** are cached results available (runs on master only)**/
  public    abstract boolean cachedResultsAvailable(FileSystem fileSystem,Configuration conf,QueryRequest<DataType,ResultKeyType,ResultValueType> theClientRequest) throws IOException;
  
  /** does the specified client query require a remote dispatch (runs on master only)
   * 	returns TRUE if query requires execution on one or more shards. The shardsToRunOn is populated
   *  on return with the shards on which this query should execute on.
   * **/
  public    abstract boolean requiresRemoteDispatch(FileSystem fileSystem,Configuration conf,ShardMapper shardMapper,QueryRequest<DataType,ResultKeyType,ResultValueType> theClientRequest,ArrayList<ShardIndexHostNameTuple> shardIdToHostNameMapping) throws IOException;
  
  /** is high priority query (can be run on master or slave)
   * 	return TRUE if this query should be dispatched immediately, instead of being queued.
   */
  public 		boolean isHighPriorityQuery() { return false; }
  
  /** run the remote part of this query  (runs remote code on slave) **/
  protected abstract long executeRemote(final FileSystem fileSyste,final Configuration conf,final EventLoop eventLoop,final DatabaseIndexV2.SlaveDatabaseIndex instanceIndex,final File tempFirDir,final QueryProgressCallback<DataType,ResultKeyType,ResultValueType> progressCallback)throws IOException;
  
  /** remote dispatch complete notification (runs on master)**/
  public    void remoteDispatchComplete(FileSystem fileSystem,Configuration conf,QueryRequest<DataType,ResultKeyType,ResultValueType> theClientRequest,long resultCount)throws IOException {} 
  

  /** remote dispatch failed notification (runs on master)**/
  public    void remoteDispatchFailed(FileSystem fileSystem) {} 
  
  
  /** is single result query **/
  //TODO: AHAD - ELEMINATE THIS 
  public  boolean  isSingleRequestQuery() { return false; }
  
  
  //////////////////////////////////////////////////////////////////////////////////////////////////
  // internal methods ...   
  //////////////////////////////////////////////////////////////////////////////////////////////////
  protected synchronized static String getPartNameForSlave(int slaveIndex) { 
    return "part-" + NUMBER_FORMAT.format(slaveIndex);
  }
  protected Path getHDFSQueryResultsPath() { return new Path(getCommonQueryInfo().getQueryResultPath()); }
  protected Path getHDFSQueryResultsFilePathForShard(int shardIndex) {
    return new Path(getHDFSQueryResultsPath(),getPartNameForSlave(shardIndex));
  }
  @SuppressWarnings("unchecked")
  static protected String getLocalQueryResultsPathPrefix(QueryRequest request) { return new Path(request.getLocalCacheDirectory().getAbsolutePath(),"QID_" + request.getSourceQuery().getQueryId() +"_").toString(); }
  

  final static String[] hex = {
    "%00", "%01", "%02", "%03", "%04", "%05", "%06", "%07",
    "%08", "%09", "%0a", "%0b", "%0c", "%0d", "%0e", "%0f",
    "%10", "%11", "%12", "%13", "%14", "%15", "%16", "%17",
    "%18", "%19", "%1a", "%1b", "%1c", "%1d", "%1e", "%1f",
    "%20", "%21", "%22", "%23", "%24", "%25", "%26", "%27",
    "%28", "%29", "%2a", "%2b", "%2c", "%2d", "%2e", "%2f",
    "%30", "%31", "%32", "%33", "%34", "%35", "%36", "%37",
    "%38", "%39", "%3a", "%3b", "%3c", "%3d", "%3e", "%3f",
    "%40", "%41", "%42", "%43", "%44", "%45", "%46", "%47",
    "%48", "%49", "%4a", "%4b", "%4c", "%4d", "%4e", "%4f",
    "%50", "%51", "%52", "%53", "%54", "%55", "%56", "%57",
    "%58", "%59", "%5a", "%5b", "%5c", "%5d", "%5e", "%5f",
    "%60", "%61", "%62", "%63", "%64", "%65", "%66", "%67",
    "%68", "%69", "%6a", "%6b", "%6c", "%6d", "%6e", "%6f",
    "%70", "%71", "%72", "%73", "%74", "%75", "%76", "%77",
    "%78", "%79", "%7a", "%7b", "%7c", "%7d", "%7e", "%7f",
    "%80", "%81", "%82", "%83", "%84", "%85", "%86", "%87",
    "%88", "%89", "%8a", "%8b", "%8c", "%8d", "%8e", "%8f",
    "%90", "%91", "%92", "%93", "%94", "%95", "%96", "%97",
    "%98", "%99", "%9a", "%9b", "%9c", "%9d", "%9e", "%9f",
    "%a0", "%a1", "%a2", "%a3", "%a4", "%a5", "%a6", "%a7",
    "%a8", "%a9", "%aa", "%ab", "%ac", "%ad", "%ae", "%af",
    "%b0", "%b1", "%b2", "%b3", "%b4", "%b5", "%b6", "%b7",
    "%b8", "%b9", "%ba", "%bb", "%bc", "%bd", "%be", "%bf",
    "%c0", "%c1", "%c2", "%c3", "%c4", "%c5", "%c6", "%c7",
    "%c8", "%c9", "%ca", "%cb", "%cc", "%cd", "%ce", "%cf",
    "%d0", "%d1", "%d2", "%d3", "%d4", "%d5", "%d6", "%d7",
    "%d8", "%d9", "%da", "%db", "%dc", "%dd", "%de", "%df",
    "%e0", "%e1", "%e2", "%e3", "%e4", "%e5", "%e6", "%e7",
    "%e8", "%e9", "%ea", "%eb", "%ec", "%ed", "%ee", "%ef",
    "%f0", "%f1", "%f2", "%f3", "%f4", "%f5", "%f6", "%f7",
    "%f8", "%f9", "%fa", "%fb", "%fc", "%fd", "%fe", "%ff"
  };


  protected static String encodePatternAsFilename(String s)
  {
    StringBuffer sbuf = new StringBuffer();
    int len = s.length();
    for (int i = 0; i < len; i++) {
      int ch = s.charAt(i);
      if ('A' <= ch && ch <= 'Z') {   // 'A'..'Z'
        sbuf.append((char)ch);
      } else if ('a' <= ch && ch <= 'z') {  // 'a'..'z'
         sbuf.append((char)ch);
      } else if ('0' <= ch && ch <= '9') {  // '0'..'9'
         sbuf.append((char)ch);
      } else if (ch <= 0x007f) {    // other ASCII
         sbuf.append(hex[ch]);
      } else if (ch <= 0x07FF) {    // non-ASCII <= 0x7FF
         sbuf.append(hex[0xc0 | (ch >> 6)]);
         sbuf.append(hex[0x80 | (ch & 0x3F)]);
      } else {          // 0x7FF < ch <= 0xFFFF
         sbuf.append(hex[0xe0 | (ch >> 12)]);
         sbuf.append(hex[0x80 | ((ch >> 6) & 0x3F)]);
         sbuf.append(hex[0x80 | (ch & 0x3F)]);
      }
    }
    return sbuf.toString();
  }
  
  
  private Collection<QueryServerSlaveState> buildDispatchListGivenShardIdMap(Map<String,QueryServerSlaveState> slaveToOnlineStateMapping,ArrayList<ShardIndexHostNameTuple> shardIdMapping)throws IOException { 
  	Map<String,QueryServerSlaveState> dispatchMap = new HashMap<String,QueryServerSlaveState>();
  	
  	// iterate the shards this query needs to run on ... 
  	for (ShardIndexHostNameTuple tuple: shardIdMapping) { 
  		if (!dispatchMap.containsKey(tuple.getHostName())) { 
  			QueryServerSlaveState onlineState =  slaveToOnlineStateMapping.get(tuple.getHostName());
  			if (onlineState != null) { 
  				dispatchMap.put(tuple.getHostName(),onlineState);
  			}
  			else { 
  				throw new IOException("Failed to map host:" + tuple.getHostName() + " to OnlineState!");
  			}
  		}
  	}
  	
  	return dispatchMap.values(); 
  }
}
