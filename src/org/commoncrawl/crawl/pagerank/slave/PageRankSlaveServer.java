/**
 * Copyright 2008 - CommonCrawl Foundation
 * 
 * CommonCrawl licenses this file to you under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.commoncrawl.crawl.pagerank.slave;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.text.NumberFormat;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.commoncrawl.async.Callback;
import org.commoncrawl.async.CallbackWithResult;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.crawl.pagerank.BaseConfig;
import org.commoncrawl.crawl.pagerank.BeginPageRankInfo;
import org.commoncrawl.crawl.pagerank.BlockTransfer;
import org.commoncrawl.crawl.pagerank.BlockTransferAck;
import org.commoncrawl.crawl.pagerank.CheckpointInfo;
import org.commoncrawl.crawl.pagerank.FileInfo;
import org.commoncrawl.crawl.pagerank.IterationInfo;
import org.commoncrawl.crawl.pagerank.PageRankJobConfig;
import org.commoncrawl.crawl.pagerank.PageRankSlave;
import org.commoncrawl.crawl.pagerank.SlaveStatus;
import org.commoncrawl.crawl.pagerank.slave.BeginPageRankTask.BeginPageRankTaskResult;
import org.commoncrawl.rpc.base.internal.AsyncClientChannel;
import org.commoncrawl.rpc.base.internal.AsyncContext;
import org.commoncrawl.rpc.base.internal.AsyncRequest;
import org.commoncrawl.rpc.base.internal.AsyncServerChannel;
import org.commoncrawl.rpc.base.internal.NullMessage;
import org.commoncrawl.rpc.base.shared.RPCException;
import org.commoncrawl.rpc.base.shared.RPCStruct;
import org.commoncrawl.server.CommonCrawlServer;
import org.commoncrawl.util.shared.CCStringUtils;
import org.commoncrawl.util.shared.JVMStats;

/** 
 * PageRank Slave Server 
 * 
 * @author rana
 *
 */
public class PageRankSlaveServer  extends CommonCrawlServer implements PageRankSlave ,AsyncServerChannel.ConnectionCallback{

  private static final int MIN_INSTANCE_ID = 0;
  private static final int MAX_INSTANCE_ID = 9;
  private static final int DEFAULT_THREAD_POOL_SIZE=20;
  private InetAddress _directoryServiceAddress;
  private int   _instanceId = -1;
  private int   _threadPoolSize = DEFAULT_THREAD_POOL_SIZE;
  private BaseConfig _baseConfig;
  private PageRankJobConfig _activeJobConfig;
  private PageRankTask  _activeTask;
  private PageRankUtils.PRValueMap _valueMap = null;
  private TaskInstantiationCallback  _queuedTaskInstantiator;
  private SlaveStatus _slaveStatus = new SlaveStatus();
  private FileSystem _fileSystem = null;
  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }
  private String  _partId = null;
  private Vector<InetSocketAddress> _slaveAddresses = new Vector<InetSocketAddress>();
  
  public FileSystem getFileSystem() { 
    return _fileSystem;
  }
  
  public BaseConfig getBaseConfig() { 
    return _baseConfig;
  }
  
  public PageRankJobConfig getActiveJobConfig() { 
    return _activeJobConfig;
  }
  
  public String getPartId() { 
    return _partId;
  }
  
  public File getJobLocalPath() { 
    return new File(getDataDirectory(),"jobLocal");
  }

  public File getActiveJobLocalPath() {
    if (getActiveJobConfig() == null) 
      throw new RuntimeException("getActiveJobLocalDir called in Invalid State.");
    else 
      return new File(getJobLocalPath(),"job-" + getActiveJobConfig().getJobId());
  }
  
  public PageRankUtils.PRValueMap getValueMap() { 
    return _valueMap;
  }
  
  public int getNodeIndex() { 
    if (_baseConfig != null) {
      return _baseConfig.getSlaveId();
    }
    else {
      LOG.error("Invalid call to getNodeIndex. No baseConfig!");
      return -1;
    }
  }

  @Override
  protected String getDefaultHttpInterface() {
    return CrawlEnvironment.DEFAULT_HTTP_INTERFACE;
  }

  @Override
  protected int getDefaultHttpPort() {
    return CrawlEnvironment.DEFAULT_PAGERANK_SLAVE_HTTP_PORT + (_instanceId * 2);
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
    return CrawlEnvironment.DEFAULT_PAGERANK_SLAVE_RPC_PORT + (_instanceId * 2);
  }

  @Override
  protected String getWebAppName() {
    return CrawlEnvironment.PAGERANK_SLAVE_WEBAPP_NAME;
  }

  @Override
  protected boolean initServer() {
    LOG.info("PageRankSlave Initializing. AvailableMemory:" + JVMStats.getHeapUtilizationRatio());
    JVMStats.dumpMemoryStats();
    // create server channel ... 
    AsyncServerChannel channel = new AsyncServerChannel(this, this.getEventLoop(),  this.getServerAddress(),this);
    
    // register RPC services it supports ... 
    registerService(channel,PageRankSlave.spec);
    
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
      else if (argv[i].equalsIgnoreCase("--threadPoolSize")) { 
        if (i+1 < argv.length) { 
          _threadPoolSize = Integer.parseInt(argv[++i]);
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

    // clear out state ... 
    _slaveStatus.clear();
    _slaveStatus.setState(SlaveStatus.State.INITIALIZING);
    
    // if there is an active task on the queue ... cancel it asyncrhonously
    if (_activeTask != null) { 
      _activeTask.cancel(new Callback() {
        // stop complete ... 
        public void execute() {
          _activeTask = null;
          finishInitialize(rpcContext);
        } 
      });
    }
    // otherwise call directly ... 
    else { 
      finishInitialize(rpcContext);
    }
  }
  
  /** get directory service address **/
  public InetAddress getDirectoryServiceAddress() { return _directoryServiceAddress; }

  // get the list of slaves
  public Vector<InetSocketAddress> getSlavesList() { return _slaveAddresses; }
  
  private void parseSlavesList(String slavesList) { 
    String slaves[] = slavesList.split(",");
    for (String slaveName : slaves) { 
      String nameParts[] = slaveName.split(":");
      _slaveAddresses.add(new InetSocketAddress(nameParts[0],Integer.parseInt(nameParts[1])));
      LOG.info("Slave At Index:"+ (_slaveAddresses.size() - 1) 
          + " is:" + _slaveAddresses.get(_slaveAddresses.size() - 1).toString());
    }
  }
    
  private void finishInitialize(AsyncContext<BaseConfig, SlaveStatus> rpcContext) { 
    // set up base config ... 
    try {
      _baseConfig = (BaseConfig) rpcContext.getInput().clone();
      _partId = "part-" + NUMBER_FORMAT.format(_baseConfig.getSlaveId());
      // parse slaves list 
      parseSlavesList(_baseConfig.getSlavesList());
    } catch (CloneNotSupportedException e) {
    }
    _activeJobConfig = null;
    _activeTask = null;
    // zero out the value array 
    _valueMap = null;
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
    // and update slave status state 
    _slaveStatus.setState(SlaveStatus.State.IDLE);
    
    sendStatusResponse(rpcContext);
  }
  
  private void sendStatusResponse(AsyncContext<? extends RPCStruct,SlaveStatus> context) {
  	  	
    try {
      context.setOutput((SlaveStatus) _slaveStatus.clone());
      if (_activeTask != null) { 
      	_slaveStatus.setPercentComplete(_activeTask._percentComplete);
      }
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
  
  public static interface TaskInstantiationCallback { 
    PageRankTask instantiateTask();
  }
  
  @Override
  public void beginPageRank(final AsyncContext<BeginPageRankInfo, SlaveStatus> rpcContext)throws RPCException {

  	final PageRankJobConfig jobConfig = rpcContext.getInput().getJobConfig();
  	
  	
    LOG.info("GOT Begin Page Rank Command. Job Id Is:" + jobConfig.getJobId()); 

    activateTask(new TaskInstantiationCallback() {

      @Override
      public PageRankTask instantiateTask() {

        // intialze the page rank config
        try {
          _activeJobConfig = (PageRankJobConfig) jobConfig.clone();
          
          LOG.info("BeginPageRank starting. FreeMemory:" + Runtime.getRuntime().freeMemory());
          // construct the begin page rank task 
          BeginPageRankTask beginPageRankTask = new BeginPageRankTask(_activeJobConfig,rpcContext.getInput().getServerStatus(),PageRankSlaveServer.this,new CallbackWithResult<BeginPageRankTaskResult>() {

            @Override
            public void execute(BeginPageRankTaskResult result) {
              if (result.succeeded()) { 
                LOG.error("BeginPageRankTask succeeded");
                _valueMap = result._valueMap;
                
                _slaveStatus.setActiveJobId(_activeJobConfig.getJobId());
                _slaveStatus.setCurrentIteration(0);
                LOG.info("Setting State to STARTED_ILDE");
                _slaveStatus.setState(SlaveStatus.State.STARTED_IDLE);                
              }
              else { 
                LOG.error("BeginPageRankTask failed with Exception:" + result.getErrorDesc());
                _valueMap = null;
                _slaveStatus.setState(SlaveStatus.State.ERROR);
              }
              LOG.info("Sending Response to Master");
              sendStatusResponse(rpcContext);
            } 
            
          });
          return beginPageRankTask;
          
        } catch (CloneNotSupportedException e) {
        }
        
        return null;
      }
    });
    
  }

  @Override
  public void doIteration(final AsyncContext<IterationInfo, SlaveStatus> rpcContext)throws RPCException {
    
    LOG.info("GOT doIteration Command. Phase:" + IterationInfo.Phase.toString(rpcContext.getInput().getPhase()) + " Iteration:" + rpcContext.getInput().getIterationNumber());
    
    if (_activeTask != null) { 
    	LOG.error("doIteration called while Task still active:" + _activeTask.getDescription());
      LOG.info("Setting State to: ERROR");
      _slaveStatus.setState(SlaveStatus.State.ERROR);
      rpcContext.completeRequest();
      return;
    }
    
    if (rpcContext.getInput().getJobId() == _activeJobConfig.getJobId()) {
      switch (rpcContext.getInput().getPhase()) { 
        
        case IterationInfo.Phase.DISTRIBUTE: { 
          
          
          _activeJobConfig.setIterationNumber(rpcContext.getInput().getIterationNumber());
          _slaveStatus.setCurrentIteration(rpcContext.getInput().getIterationNumber());
          
          LOG.info("Setting State to: DISTRIBUTING");
          _slaveStatus.setState(SlaveStatus.State.DISTRIBUTING);
          
          activateTask(new TaskInstantiationCallback() {
            
            @Override
            public PageRankTask instantiateTask() {
                
              return new DistributeRankTask(PageRankSlaveServer.this,new CallbackWithResult<DistributeRankTask.DistributeRankTaskResult>() {
      
                @Override
                public void execute(DistributeRankTask.DistributeRankTaskResult result) {
                  LOG.info("Done with Iteration:" + rpcContext.getInput().getIterationNumber() + " for Phase:" + IterationInfo.Phase.toString(rpcContext.getInput().getPhase()) 
                      + "Result:" + result.isDone());
                  if (result.isDone()) { 
                    LOG.info("Setting State to: DONE_DISTRIBUTING");
                    _slaveStatus.setState(SlaveStatus.State.DONE_DISTRIBUTING);
                  }
                  else { 
                    LOG.info("Distribution Failed with Result:" +result.getErrorDesc() + ".Setting State to: ERROR");
                    _slaveStatus.setState(SlaveStatus.State.ERROR);
                  }
                } 
              });
            }
          });
        }
        break;
        
        case IterationInfo.Phase.CALCULATE: { 
          
          _activeJobConfig.setIterationNumber(rpcContext.getInput().getIterationNumber());
          _slaveStatus.setCurrentIteration(rpcContext.getInput().getIterationNumber());
          LOG.info("Setting State to: CALCULATING");
          _slaveStatus.setState(SlaveStatus.State.CALCULATING);
          
          activateTask(new TaskInstantiationCallback() {
            
            @Override
            public PageRankTask instantiateTask() {
                
              return new CalculateRankTask(PageRankSlaveServer.this,new CallbackWithResult<CalculateRankTask.CalculateRankTaskResult>() {
      
                @Override
                public void execute(CalculateRankTask.CalculateRankTaskResult result) {
                  LOG.info("Done with Iteration:" + rpcContext.getInput().getIterationNumber() + " for Phase:" + IterationInfo.Phase.toString(rpcContext.getInput().getPhase()) 
                      + "Result:" + result.isDone());
                  if (result.isDone()) { 
                    switch (rpcContext.getInput().getPhase()) { 
                      case IterationInfo.Phase.DISTRIBUTE: {
                        LOG.info("Setting State to: DONE_DISTRIBUTING");
                        _slaveStatus.setState(SlaveStatus.State.DONE_DISTRIBUTING);
                      }
                      break;
                      
                      case IterationInfo.Phase.CALCULATE:  { 
                        LOG.info("Setting State to: DONE_CALCULATING");
                        _slaveStatus.setState(SlaveStatus.State.DONE_CALCULATING);
                      }
                      break;
                    }
                  }
                  else { 
                    LOG.info("Setting State to: ERROR");
                    _slaveStatus.setState(SlaveStatus.State.ERROR);
                  }
                  
                } 
                
              });
            }
            
          });
          
        }
        break;
      }
    }
    else { 
      LOG.error("Incoming Job Id:" + rpcContext.getInput().getJobId()+ " Different from Active Job Id:" + rpcContext.getInput().getJobId());
      LOG.info("Setting State to: ERROR");
      _slaveStatus.setState(SlaveStatus.State.ERROR);

    }
    LOG.info("Sending Response to Master");
    sendStatusResponse(rpcContext);
  }

  @Override
  public void endPageRank(final AsyncContext<NullMessage, SlaveStatus> rpcContext)throws RPCException {
    LOG.info("GOT endPageRank Command");
    
    if (_slaveStatus.getState() == SlaveStatus.State.STARTED_IDLE || _slaveStatus.getState() == SlaveStatus.State.DONE_CALCULATING) {
      
      activateTask(new TaskInstantiationCallback() {
  
        @Override
        public PageRankTask instantiateTask() {
            
          return new TestTask(PageRankSlaveServer.this,new CallbackWithResult<TestTask.TestTaskResult>() {
  
            @Override
            public void execute(TestTask.TestTaskResult result) {
              
              LOG.info("ended Page Rank for Job:" + _activeJobConfig.getJobId() + " with Result:" + result.isDone()); 

              if (result.isDone()) {
                _activeJobConfig.clear();
                _slaveStatus.setState(SlaveStatus.State.IDLE);
              }
              else {
                _activeJobConfig.clear();
                _slaveStatus.setState(SlaveStatus.State.ERROR);
              }
              
              LOG.info("Sending Response to Master");
              
              sendStatusResponse(rpcContext);    
              
            } 
            
          },"END PAGE RANK",15000);
        }
        
      });
    }
    else { 
      LOG.info("Setting State to: ERROR");
      _slaveStatus.setState(SlaveStatus.State.ERROR);

    }
  }

	@Override
  public void checkpoint(final AsyncContext<CheckpointInfo, SlaveStatus> rpcContext)throws RPCException {
		try { 
			if (_slaveStatus.getState() != SlaveStatus.State.DISTRIBUTING && _slaveStatus.getState() != SlaveStatus.State.CALCULATING) {
				LOG.info("Recevied Checkpoint Cmd. TxnId:" + rpcContext.getInput().getTxnId() 
						+ " Phase:" + rpcContext.getInput().getCurrentPhase()
						+ " Iteration:" + rpcContext.getInput().getCurrentIterationNumber());
				
				final CheckpointInfo checkpointInfo = rpcContext.getInput();

				// check to see iteration number matches 
				if (checkpointInfo.getCurrentIterationNumber() == _slaveStatus.getCurrentIteration()) { 
				
					
					// do a phase to current state match
					if (checkpointInfo.getCurrentPhase() == IterationInfo.Phase.CALCULATE && _slaveStatus.getState() == SlaveStatus.State.DONE_CALCULATING) { 
						// ok this is a valid txn id ... set it as active 
						_slaveStatus.setCurrentCheckpointId(checkpointInfo.getTxnId());
						
						// activate appropriate task 
	          activateTask(new TaskInstantiationCallback() {
	            
	            @Override
	            public PageRankTask instantiateTask() {
	                
	              return new CalculateRankCommitTask(PageRankSlaveServer.this,checkpointInfo,new CallbackWithResult<CalculateRankCommitTask.TaskResult>() {
	      
	                @Override
	                public void execute(CalculateRankCommitTask.TaskResult result) {

	          				LOG.info("Finished CalculateRank Commit Task. TxnId:" + rpcContext.getInput().getTxnId() 
	          						+ " Phase:" + rpcContext.getInput().getCurrentPhase()
	          						+ " Iteration:" + rpcContext.getInput().getCurrentIterationNumber() + " Result:" + result.isDone());
	          				
	                  if (result.isDone()) { 
	                    _slaveStatus.setCommittedCheckpointId(checkpointInfo.getTxnId());
	                  }
	                  else { 
	                    LOG.info("Commit Failed with Result:" +result.getErrorDesc() + ".Setting State to: ERROR");
	                    _slaveStatus.setState(SlaveStatus.State.ERROR);
	                  }
	                } 
	              });
	            }
	          });						
						
						
					}
					else if (checkpointInfo.getCurrentPhase() == IterationInfo.Phase.DISTRIBUTE && _slaveStatus.getState() == SlaveStatus.State.DONE_DISTRIBUTING) { 
						// ok this is a valid txn id ... set it as active 
						_slaveStatus.setCurrentCheckpointId(checkpointInfo.getTxnId());
						
						// activate appropriate task 
	          activateTask(new TaskInstantiationCallback() {
	            
	            @Override
	            public PageRankTask instantiateTask() {
	                
	              return new DistributeRankCommitTask(PageRankSlaveServer.this,checkpointInfo,new CallbackWithResult<DistributeRankCommitTask.TaskResult>() {
	      
	                @Override
	                public void execute(DistributeRankCommitTask.TaskResult result) {

	          				LOG.info("Finished DistributeRankCommitTask. TxnId:" + rpcContext.getInput().getTxnId() 
	          						+ " Phase:" + rpcContext.getInput().getCurrentPhase()
	          						+ " Iteration:" + rpcContext.getInput().getCurrentIterationNumber() + " Result:" + result.isDone());
	          				
	                  if (result.isDone()) { 
	                    _slaveStatus.setCommittedCheckpointId(checkpointInfo.getTxnId());
	                  }
	                  else { 
	                    LOG.info("Commit Failed with Result:" +result.getErrorDesc() + ".Setting State to: ERROR");
	                    _slaveStatus.setState(SlaveStatus.State.ERROR);
	                  }
	                } 
	              });
	            }
	          });						
						
					}
				}
			}
		}
		finally { 
			// now matter what, send a status response ... 
			sendStatusResponse(rpcContext);
		}
  }

  
  @Override
  public void heartbeat(AsyncContext<NullMessage, SlaveStatus> rpcContext)throws RPCException {
    //LOG.info("Got Heartbeat from Master - Sending Status to Master");
    sendStatusResponse(rpcContext);
  }
 
  private final void failRequest(AsyncContext<? extends RPCStruct,? extends RPCStruct> rpcContext,String reason) { 
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
  
  /** activate the specified task*/ 
  private void activateTask(final TaskInstantiationCallback callback) { 
    
    _queuedTaskInstantiator = callback;
    
    if (_activeTask != null) { 
      _activeTask.cancel(new Callback() {

        @Override
        public void execute() {
          _activeTask = null;
          instantiateQueuedTask();
        } 
        
      });
    }
    else {
      _activeTask = null;
      instantiateQueuedTask();
    }
  }
  
  /** instantiate the queued task **/
  private void instantiateQueuedTask()  {
    if (_queuedTaskInstantiator != null) { 
      _activeTask = _queuedTaskInstantiator.instantiateTask();
      if (_activeTask != null) { 
        _activeTask.start();
      }
    }
  }
  
  /** task starting callback **/
  void taskStarting(PageRankTask task) { 
    LOG.info("Task:" + task.getDescription() + " Starting");
  }
  
  /** task complete callback **/
  void taskComplete(PageRankTask task) {
    LOG.info("Task:" + task.getDescription() + " Complete");
    if (_activeTask == task) { 
      _activeTask = null;
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

  

  
  @Override
  public void deleteFile(AsyncContext<FileInfo, NullMessage> rpcContext)
      throws RPCException {
    
  }

  @Override
  public void commitFile(AsyncContext<FileInfo, NullMessage> rpcContext)
      throws RPCException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void createJobFile(AsyncContext<FileInfo, FileInfo> rpcContext)
      throws RPCException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void transferBlock(
      AsyncContext<BlockTransfer, BlockTransferAck> rpcContext)
      throws RPCException {
    // TODO Auto-generated method stub
    
  }

}
