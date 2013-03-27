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

package org.commoncrawl.service.pagerank.master;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.Vector;
import java.util.zip.CRC32;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.commoncrawl.async.Timer;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.db.RecordStore;
import org.commoncrawl.server.CommonCrawlServer;
import org.commoncrawl.service.pagerank.BaseConfig;
import org.commoncrawl.service.pagerank.IterationInfo;
import org.commoncrawl.service.pagerank.PRMasterState;
import org.commoncrawl.service.pagerank.PageRankJobConfig;
import org.commoncrawl.service.pagerank.SlaveStatus;
import org.commoncrawl.service.pagerank.SlaveStatus.State;
import org.commoncrawl.util.CCStringUtils;

/**
 * 
 * @author rana
 *
 */
public class PageRankMaster extends CommonCrawlServer {

  private static final int MAX_ITERATION_DEFAULT = 50;
  private static final int POLL_TIMER_INTERVAL = 1000;
  private static final int DEFAULT_INSTANCES_PER_SLAVE=1;
  private static final String PRMasterStateKey = "PRMasterState";
  private static final String PRJobConfigKey =   "PRJobConfig";
  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }
  

  private String     _slavesFile;
  private long       _slavesFileCRC = -1;
  private FileSystem _fileSystem;
  private String     _hdfsWorkingDir = "crawl/pageRank/jobs";
  private long 	     _jobId = -1;
  private boolean    _pageRankStarted = false;
  
  private Timer      _pollTimer = null;
  /** record store object used to persist state **/
  private RecordStore   _recordStore = new RecordStore();
  private boolean    _serverPaused = false;

  private Vector<PageRankRemoteSlave> _slaves = new Vector<PageRankRemoteSlave>();
  private String _slavesList = null;
  private SlaveStatus _slaveStates[] = null;
  
  private PRMasterState              _serverState = null;
  private PageRankJobConfig 				 _jobConfig = null;
  //private Vector<PageRankJobConfig>  _jobQueue  = new Vector<PageRankJobConfig>();
  //private Vector<PageRankJobConfig>  _jobList   = new Vector<PageRankJobConfig>();


  public PageRankMaster() { 
    setAsyncWebDispatch(true);    
  }
  
  public PageRankJobConfig getActiveJobConfig() { 
    return _serverState.getActiveJobConfig();
  }
  
  public String getMasterState() { return PRMasterState.ServerStatus.toString(_serverState.getServerStatus()); }
  public PageRankJobConfig getActiveJob() { 
    if (_serverState.getServerStatus() != PRMasterState.ServerStatus.IDLE) { 
      return _serverState.getActiveJobConfig();
    }
    return null;
  }
  public String getActiveJobName() { 
    if (_serverState.getServerStatus() != PRMasterState.ServerStatus.IDLE) { 
      return getJobName(_serverState.getActiveJobConfig());
    }
    return "";
  }
  //public Vector<PageRankJobConfig> getJobQueue() { return _jobQueue; }
  public static String getJobName(PageRankJobConfig jobConfig) { 
    return "job-" + jobConfig.getJobId();
  }
  /*
  public Vector<PageRankJobConfig> getQueueableJobList() { 
    Vector<PageRankJobConfig> listOut = new Vector<PageRankJobConfig>();
    HashSet<PageRankJobConfig> queuedJobs = new HashSet<PageRankJobConfig>();
    queuedJobs.addAll(_jobQueue);
    for (PageRankJobConfig job : _jobList) { 
      if (!queuedJobs.contains(job)) { 
        listOut.add(job);
      }
    }
    return listOut;
  }
  */
  
  public void createNewJob(String inputValuePath,String graphPath,int maxIterations, int slaveCount) throws IOException {
  	
    PageRankJobConfig jobConfig = new PageRankJobConfig();
    
    jobConfig.setJobId(System.currentTimeMillis());
    jobConfig.setIterationNumber(0);
    jobConfig.setMaxIterationNumber(maxIterations);
    jobConfig.setSlaveCount(slaveCount);
    jobConfig.setInputValuesPath(inputValuePath);
    jobConfig.setOutlinksDataPath(graphPath);
    // set up job dir ... 
    Path jobPath = new Path(_hdfsWorkingDir,Long.toString(jobConfig.getJobId()));
    // mk the job dir 
    // _fileSystem.mkdirs(jobPath);
    jobConfig.setJobWorkPath(jobPath.toString());
    jobConfig.setAlgorithmId(0);
    jobConfig.setAlpha(.85f);
    
    //serializeJobConfig(jobConfig);
    
    //_jobList.add(jobConfig);
  }
  
  /*
  public void queueJob(String jobIdStr) {
    
    long jobId = Long.parseLong(jobIdStr);
    
    for (PageRankJobConfig job : _jobList) { 
      if (job.getJobId() == jobId) { 
        _jobList.remove(job);
        _jobQueue.add(job);
        break;
      }
    }
  }
  */
  
  Vector<PageRankRemoteSlave> getSlaves() { return _slaves; }
  
  public boolean isServerIdle() { 
    return _serverState.getServerStatus() == PRMasterState.ServerStatus.IDLE;
  }
  
  public boolean isPageRankActive() { 
    return _serverState.getServerStatus() != PRMasterState.ServerStatus.IDLE;
  }
  
  public boolean isPageRankTerminating() { 
    return _serverState.getServerStatus() == PRMasterState.ServerStatus.FINISHING;
  }
  
  public boolean isServerPaused() { 
    return _serverPaused; 
  }
  
  public boolean isIterationActive() { 
   return _serverState.getServerStatus() >= PRMasterState.ServerStatus.ITERATING_DISTRIBUTING 
     && _serverState.getServerStatus() <= PRMasterState.ServerStatus.ITERATING_CALCULATING;
  }
  
  public int getSlaveIterationPhase() { 
    if (_serverState.getServerStatus() == PRMasterState.ServerStatus.ITERATING_CALCULATING) { 
      return IterationInfo.Phase.CALCULATE;
    }
    else { 
      return IterationInfo.Phase.DISTRIBUTE;
    }
  }
  
  public long getCurrentJobNumber() { 
    return (isPageRankActive()) ? _serverState.getActiveJobConfig().getJobId() : -1;
  }
  
  public int getCurrentIterationNumber() { 
    return (isPageRankActive()) ? _serverState.getActiveJobConfig().getIterationNumber() : -1; 
  }
  
  public int getMaxIteration() { 
    return (isPageRankActive()) ? _serverState.getActiveJobConfig().getMaxIterationNumber() : -1;
  }
  
  public BaseConfig getBaseConfigForSlave(PageRankRemoteSlave slave) { 
    
    BaseConfig baseConfig = new BaseConfig();
    
    baseConfig.setBaseWorkingDir(_hdfsWorkingDir);
    // baseConfig.setFileSystem(_fileSystem.getUri().toString());
    baseConfig.setSlaveCount(_slaves.size());
    baseConfig.setSlaveId(slave.getSlaveId());
    baseConfig.setSlavesList(_slavesList);
    
    return baseConfig;
  }
  
  public void createJob() { 
    
  }
  
  //@Override
  protected String   getDefaultLogFileName() { 
    return "prmaster";
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
    return CrawlEnvironment.DEFAULT_PAGERANK_MASTER_HTTP_PORT;
  }

  @Override
  protected String getDefaultRPCInterface() {
    return CrawlEnvironment.DEFAULT_RPC_INTERFACE;
  }

  @Override
  protected int getDefaultRPCPort() {
    return CrawlEnvironment.DEFAULT_PAGERANK_MASTER_RPC_PORT;
  }

  @Override
  protected String getWebAppName() {
    return CrawlEnvironment.PAGERANK_MASTER_WEBAPP_NAME;
  }
  

  @Override
  protected boolean initServer() {
    
    if (_slavesFile == null || _jobId == -1) { 
      LOG.error("Slaves File not specified. Specify Slaves file via --slaves");
      return false;
    }
    else { 
      try {
        // get a pointer to the hdfs file system 
        // _fileSystem = CrawlEnvironment.getDefaultFileSystem();
        // parse slaves file ..
        parseSlavesFile();
        // load database state ... 
        loadState();
        // init slave states array 
        _slaveStates = new SlaveStatus[_slaves.size()];
        // and populate it ... 
        for (int i=0;i<_slaveStates.length;++i) { 
          _slaveStates[i] = new SlaveStatus();
        }
        // connect to slaves ...
        connectToSlaves();
        // setup poll timer
        _pollTimer = new  Timer(POLL_TIMER_INTERVAL,true,new Timer.Callback() {

          @Override
          public void timerFired(Timer timer) {
            
            potentaillyUpdateServerStatus();
            
          } 
          
        });
        
        getEventLoop().setTimer(_pollTimer);
        
        return true;
      }
      catch (IOException e) { 
        LOG.error(CCStringUtils.stringifyException(e));
      }
    }
    return false;
  }

  @Override
  protected boolean parseArguments(String[] argv) {
    
    for(int i=0; i < argv.length;++i) {
    
      if (argv[i].equalsIgnoreCase("--slaves")) { 
        if (i+1 < argv.length) { 
          _slavesFile = argv[++i];
        }
      }
      else if (argv[i].equalsIgnoreCase("--jobId")) { 
      	_jobId = Long.parseLong(argv[++i]);
      }
      		
    }
    
    return true;
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

  
  
  void parseSlavesFile()throws IOException {
    
    StringBuffer slavesListWriter = new StringBuffer();
    
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
    
    int slaveCount = 0;
    
    LOG.info("Loading slaves file");
    while ((slaveHostPlusCount = reader.readLine()) != null) {
      if (!slaveHostPlusCount.startsWith("#")) {
        StringTokenizer tokenizer = new StringTokenizer(slaveHostPlusCount,":");
        if (tokenizer.countTokens() != 3){
          throw new IOException("Invalid Slave Entry:" + slaveHostPlusCount + " in slaves File");
        }
        else {
          String slaveName = tokenizer.nextToken();
          int    instanceCount = Integer.parseInt(tokenizer.nextToken());
          String localInterfacePort = tokenizer.nextToken();
          
          for (int i=0;i<instanceCount;++i) { 
            PageRankRemoteSlave state = new PageRankRemoteSlave(this,slaveCount++,slaveName,i);
            LOG.info("Adding slave:" + slaveName + "instance:" + i);
            _slaves.add(state);
            slavesListWriter.append(state.getFullyQualifiedName() + ",");
          }
        }
      }
      // update finalized slaves list 
      _slavesList = slavesListWriter.toString();
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
  
  void connectToSlaves() throws IOException {
    LOG.info("Connecting to Slaves");
    for (PageRankRemoteSlave slave : _slaves) { 
      slave.connect(); 
    }
  }
  
  
  // react to a status change in a pagerank slave ..
  void slaveStatusChanged(PageRankRemoteSlave slave) {
    
    LOG.info("slaveStatusChanged from slave:" + slave.getFullyQualifiedName() 
        + " NewStatus:" + slave.getLastKnowStatus() + " ServerState:"  
        + _serverState.getServerStatus()
        );

    try {
      _slaveStates[slave.getSlaveId()].merge(slave.getLastKnowStatus());
    } catch (CloneNotSupportedException e) {
    }
    
    if (_serverState.getServerStatus() == PRMasterState.ServerStatus.IDLE) {

      if (!_pageRankStarted) {
        
        int idleCount = 0;
        // check to see if all slaves are ready 
        for (SlaveStatus slaveState : _slaveStates) {
          // if this slave is finished with current iteration .. 
          if (slaveState.getState() == SlaveStatus.State.IDLE) { 
            ++idleCount;
          }
        }
        // if all slaves are in idle state 
        if (idleCount == _slaveStates.length) {
          // ready to send page rank start command 
          LOG.info("All Slaves Online and Idle. Sending Start PageRank Cmd");
          // and send out start page rank command
          sendSlavesStartPageRankCmd(PRMasterState.ServerStatus.STARTED);
          _pageRankStarted = true;
        }
        else { 
          LOG.info(idleCount + " Slave Idle/Online. Waiting on:" + (_slaveStates.length - idleCount));
        }
      }
    }
    else { 
      potentiallyResyncSlaveState(slave.getSlaveId());

      potentaillyUpdateServerStatus();
    }
  }
  
  
  private void potentiallyResyncSlaveState(int slaveIdx) { 
    
    SlaveStatus status = _slaveStates[slaveIdx];
    
    //check to see if the slave even has a potentially valid state ... 
    if (status.isFieldDirty(SlaveStatus.Field_STATE)) { 
      // do we want to reset this slave...
      boolean resetSlave = false;
      // ok now try to see if it is out of sync with the master ... 
      switch (status.getState()) { 
        
        // slave is in an initialized but idle state ...
        case SlaveStatus.State.IDLE: { 
          // if page rank is active ... send the appropriate command to the slave ... 
          if (isPageRankActive()) { 
            LOG.info("Slave is IDLE while Master has PageRankActive. Sending Start Page Rank to Slave:" + _slaves.get(slaveIdx).getFullyQualifiedName());
            _slaves.get(slaveIdx).sendStartPageRankCmd(_serverState.getServerStatus());
          }
        }
        break;
        
        case SlaveStatus.State.STARTED_IDLE:
        case SlaveStatus.State.DONE_CALCULATING:
        { 
          // if page rank is active ... figure out next steps based on host state 
          if (isPageRankActive()) { 
            // if we are iterating ... 
            if (isIterationActive()) {
              boolean sendDoIterationCommand = true;
              if (status.getState() == SlaveStatus.State.DONE_CALCULATING && _serverState.getServerStatus() == PRMasterState.ServerStatus.ITERATING_CALCULATING) {
                if (status.getCurrentIteration() == _serverState.getActiveJobConfig().getIterationNumber()) { 
                  sendDoIterationCommand = false;
                  LOG.info("Slave is DONE_CALCULATING and Master is in CALCULATING... IGORING Slave:" + _slaves.get(slaveIdx).getFullyQualifiedName());
                }
              }
              if (sendDoIterationCommand) { 
                LOG.info("Slave is in "+ SlaveStatus.State.toString(status.getState()) +  " while Master is in Iteration. Sending StartIteration to Slave:" + _slaves.get(slaveIdx).getFullyQualifiedName());
                sendSlaveDoIterationCmd(_slaves.get(slaveIdx));
              }
            }
            // are we terminating ... ? 
            else if (isPageRankTerminating()) { 
              LOG.info("Slave is in "+ SlaveStatus.State.toString(status.getState()) +" while Master is in PageRankTerminating. Sending EndPageRank to Slave:" + _slaves.get(slaveIdx).getFullyQualifiedName());
              // send end page rank command to slave ... 
              _slaves.get(slaveIdx).sendEndPageRankCmd();
            }
          }
          else { 
            // bad slave is out of sync ... reset it ...
            resetSlave = true;
          }
        }
        break;
        
        // if slave is in iteration state ... 
        case SlaveStatus.State.DISTRIBUTING:
        case SlaveStatus.State.DONE_DISTRIBUTING:
        case SlaveStatus.State.CALCULATING: { 
          
          if (status.getState() == State.DONE_DISTRIBUTING && _deferredSlaves.size() != 0) {
            PageRankRemoteSlave nextSlave = _deferredSlaves.first();
            LOG.info("Sending Do Iteration To Deferred Slave:" + nextSlave.getFullyQualifiedName());
            _deferredSlaves.remove(nextSlave);
            sendSlaveDoIterationCmd(nextSlave);
          }
          // validate that master is in sync ... 
          if (!isPageRankActive() && !isIterationActive()) {
            resetSlave = true;
          }
        }
        break;
      }
      
      if (resetSlave) { 
        LOG.error("Slave:" + _slaves.get(slaveIdx).getFullyQualifiedName()  +" out of Sync with Master." + 
            "Master State:" + _serverState + 
            "Slave State:" + status.getState() + 
            " -- Sending RESET");
        
        // reset slave state 
        _slaveStates[slaveIdx].clear();
        // and send reset cmd to slave...
        _slaves.get(slaveIdx).sendResetCmd();
      }
    }
  }
  
  
  private void potentaillyUpdateServerStatus() { 
    try { 

      // if the server is in an idle state ... 
      if (_serverState.getServerStatus() == PRMasterState.ServerStatus.IDLE) {
        // start the next job potentially 
        //potentiallyStartNextPRJob();
      }
      
    	// paused state handling ...
      /*
      if (_serverState.getServerStatus() == PRMasterState.ServerStatus.PAUSED) { 
        
        // if server is in a paused state but we are ok to start iterating ... 
        if (!isServerPaused()) { 
          LOG.info("Server Moving from PAUSED to NextIteration State");
          advanceToNextPRIteration();
        }
      }
      */
      else if (_serverState.getServerStatus() == PRMasterState.ServerStatus.STARTED) {
        int completionCount = 0;
        for (SlaveStatus slaveState : _slaveStates) {
          // if this slave is finished with current iteration .. 
          if (slaveState.getState() == SlaveStatus.State.STARTED_IDLE && slaveState.getActiveJobId() == getActiveJobConfig().getJobId()) { 
            ++completionCount;
          }
        }
        if (completionCount == _slaves.size()) { 
          LOG.info("Server in STARTED STATE and All Clients are STARTED_IDLE. Moving to NextIteration");
          // and update server state ... 
          _serverState.setServerStatus(PRMasterState.ServerStatus.ITERATING_DISTRIBUTING);
          // reset txn id 
          _serverState.setCurrentTxnId(System.currentTimeMillis());
          // finally serialize state 
          //serializeServerState();
          // and send out the next iteration command to the slaves ... 
          sendSlavesDoIterationCmd();
          
        }
      }
      // if in iteration state ... 
      else if (_serverState.getServerStatus() == PRMasterState.ServerStatus.ITERATING_DISTRIBUTING 
          || _serverState.getServerStatus() == PRMasterState.ServerStatus.ITERATING_CALCULATING ) { 
        int completedSlaveCount = 0;
        int desiredTransitionState = (_serverState.getServerStatus() == PRMasterState.ServerStatus.ITERATING_DISTRIBUTING) ? 
            SlaveStatus.State.DONE_DISTRIBUTING : SlaveStatus.State.DONE_CALCULATING;
            
        for (int slaveIdx=0;slaveIdx<_slaveStates.length;++slaveIdx) {
        	
        	SlaveStatus slaveState = _slaveStates[slaveIdx];
        	
          // if this slave is finished with current iteration .. 
          if (slaveState.getState() == desiredTransitionState && slaveState.getCurrentIteration() == getCurrentIterationNumber()) { 
            ++completedSlaveCount;
          }
        }
        // if all slaves are done with the current iteration ...
        if (completedSlaveCount == _slaves.size()) {
        	
        	int completedCheckpointsCount = 0;
        	
          for (int slaveIdx=0;slaveIdx<_slaveStates.length;++slaveIdx) {
          	
          	SlaveStatus slaveState = _slaveStates[slaveIdx];
          	
          	// check to see if we need to send this slave a checkpoint command  
            if (slaveState.getCurrentCheckpointId() != _serverState.getCurrentTxnId()) {
            	LOG.info("Sending Slave:" + _slaves.get(slaveIdx).getFullyQualifiedName() + " Checkpoint Command - CurrentCheckpointId:" + slaveState.getCurrentCheckpointId());
            	slaveState.setCurrentCheckpointId(_serverState.getCurrentTxnId());
              sendSlaveCheckpointCommand(slaveIdx,_serverState.getCurrentTxnId(),getSlaveIterationPhase(),getCurrentIterationNumber());
            }
            else {
            	// ok the current slave completed active checkpoint command
            	if (slaveState.getCommittedCheckpointId() == _serverState.getCurrentTxnId()) {
            		// increment completion count ...
            		completedCheckpointsCount++;
            		LOG.info("Slave:"+_slaves.get(slaveIdx).getFullyQualifiedName() + " Has Completed Checkpoint. Completed CheckpointCount:" + completedCheckpointsCount);
            	}
            }
          }
        	
          // if everyone completed active checkpoint 
          if (completedCheckpointsCount == _slaves.size()) {
          	// OK. advance to next MASTER state ..
          	LOG.info("All Slaves Report Successful Checkpoint Status for Txn:" + _serverState.getCurrentTxnId());
          
	          // if we were in the distribute phase ... 
	          if (_serverState.getServerStatus() == PRMasterState.ServerStatus.ITERATING_DISTRIBUTING) { 
	            LOG.info("All Clients Done DISTRIBUTING. Moving to CALCULATING State");
	            // advance master state 
	            _serverState.setServerStatus(PRMasterState.ServerStatus.ITERATING_CALCULATING);
	            // reset txn id 
	            _serverState.setCurrentTxnId(System.currentTimeMillis());
	            // serialize the state 
//	            /serializeServerState();
	            // notify slaves ... 
	            sendSlavesDoIterationCmd();
	          }
	          // otherwise, if in the calculation phase ... 
	          else { 
	            // if we reached the last iteration .. .we need to do a clean shutdown ... 
	            if (getCurrentIterationNumber() == getMaxIteration()) { 
	              LOG.info("All Clients Done CALCULATING and Iteration Number == Max Iteration.Moving to FINISHING STATE");
	              // set our appropriate state ... 
	              _serverState.setServerStatus(PRMasterState.ServerStatus.FINISHING);
	              // serialize the state 
	              //serializeServerState();
	              // and send shutdown command to slaves ... 
	              sendSlavesEndPageRankCmd();
	            }
	            // otherwise,  
	            else { 
	              // if server is not paused ... 
	              if (!isServerPaused()) { 
	                LOG.info("All Clients Done CALCULATING and Iteration Number < Max Iteration.Moving to Next Iteration");
	                advanceToNextPRIteration();
	              }
	              else { 
	                LOG.info("All Clients Done CALCULATING and Iteration Number < Max Iteration BUT Server is PAUSED.Moving to PAUSED STATE");
	                // set pause state 
	                _serverState.setServerStatus(PRMasterState.ServerStatus.PAUSED);
	                // serialize state 
	                //serializeServerState();
	              }
	            }
	          }
          }
        }
      }
      // shutdown state handling ...
      else if (_serverState.getServerStatus() == PRMasterState.ServerStatus.FINISHING) { 
        int idledCount = 0;
        for (SlaveStatus slaveState : _slaveStates) { 
          // if this slave is finished with current iteration .. 
          if ((slaveState.getState() == SlaveStatus.State.DONE_CALCULATING || slaveState.getState() == SlaveStatus.State.STARTED_IDLE) && slaveState.getCurrentIteration() == getCurrentIterationNumber()) { 
            ++idledCount;
          }
        }
        
        // all slaves are done cleaning up ... 
        if (idledCount == _slaves.size()) { 
          LOG.info("SERVER in FINISHING State and ALL Clients FINISHED.Finishing PR Job");
          // do cleanup ... 
          //finishPageRankJob(_serverState.getActiveJobConfig(),false);
          
          _serverState.getActiveJobConfig().clear();
          _serverState.setFieldClean(PRMasterState.Field_ACTIVEJOBCONFIG);
          // and reset state ... 
          _serverState.setServerStatus(PRMasterState.ServerStatus.IDLE);
          // and serialize state 
          //serializeServerState();
        }
      }
    }
    catch (IOException e) { 
      LOG.error("Unexpected IOException: " + CCStringUtils.stringifyException(e));
      //TODO: KILL SERVER HERE ...
    }
  }
  
  private void advanceToNextPRIteration() throws IOException { 
    //advance iteration number and restart distribution ...
    _serverState.getActiveJobConfig().setIterationNumber(_serverState.getActiveJobConfig().getIterationNumber() + 1);
    // and update server state ... 
    _serverState.setServerStatus(PRMasterState.ServerStatus.ITERATING_DISTRIBUTING);
    // update transaction id 
    _serverState.setCurrentTxnId(System.currentTimeMillis());
    // finally serialize state 
    //serializeServerState();
    // and send out the next iteration command to the slaves ... 
    sendSlavesDoIterationCmd();
  }
  
  
  /*
  private void serializeServerState() throws IOException { 
    _recordStore.beginTransaction();
    _recordStore.updateRecordByKey(PRMasterStateKey, _serverState);
    if (_serverState.isFieldDirty(PRMasterState.Field_ACTIVEJOBCONFIG)) { 
      // update the disk state of the active job config
      serializeJobConfig(_serverState.getActiveJobConfig());
    }
    _recordStore.commitTransaction();
  }
  */
  
  private void clearAllCheckpointAndDistributionFiles(FileSystem fs,Path jobDataPath) throws IOException { 
    // scan job directory for best value candidate 
    Path checkpointSearchPattern = new Path(jobDataPath,"*-CheckpointComplete-*");
    Path distroSearchPattern = new Path(jobDataPath,"OutlinkPR-*");
    
    FileStatus checkpointCandidates[] = fs.globStatus(checkpointSearchPattern);
    for (FileStatus candidate : checkpointCandidates) {
      LOG.info("Deleting:" + candidate.getPath());
      fs.delete(candidate.getPath(),false);
    }
    FileStatus distroCandidates[] = fs.globStatus(distroSearchPattern);
    for (FileStatus candidate : distroCandidates) { 
      LOG.info("Deleting:" + candidate.getPath());
      fs.delete(candidate.getPath(),false);
    }
    
  }
  private int findListValidIteration(FileSystem fs,Path jobDataPath) throws IOException { 
		// scan job directory for best value candidate 
		Path valueSearchPattern = new Path(jobDataPath,"value_*-00000");
		
		FileStatus candidates[] = fs.globStatus(valueSearchPattern);
		
		int lastValidIterationNo = -1;
		
		ArrayList<Path> iterationSpecificValues = new ArrayList<Path>();
		
		for (FileStatus candidate : candidates) { 
			// extract iteration portion of name 
			String iterationStr = candidate.getPath().getName().substring("value_".length(),"value_".length() + 5);
			// parse 
			try {
        int iterationId = NUMBER_FORMAT.parse(iterationStr).intValue();
        // now see if we up to PR_NUM_SLAVES values 
        Path iterationSpecificSearchPattern = new Path(jobDataPath,"value_" + iterationStr + "-*");
        // count result 
        FileStatus iterationSpecificEntires[] = fs.globStatus(iterationSpecificSearchPattern);
        
        if (iterationSpecificEntires.length == CrawlEnvironment.PR_NUMSLAVES) { 
        	LOG.info("Iteration Number:" + iterationId + " has the proper number of results");
        	
        	if (lastValidIterationNo == -1 || lastValidIterationNo < iterationId) { 
        		// set this iteration as the valid iteration
        		lastValidIterationNo = iterationId;
        		LOG.info("Setting Iteration:"+ iterationId + " as last valid iteration number");
        		// clear candidate list 
        		iterationSpecificValues.clear();
        		// add paths to candidate list ... 
        		for (FileStatus iterationSpecificEntry : iterationSpecificEntires) { 
        			iterationSpecificValues.add(iterationSpecificEntry.getPath());
        		}
        	}
        }
        else { 
        	LOG.error("Skipping Iteration Number:" + iterationId + ". It only has:" + iterationSpecificEntires.length + "results");
        }
        
      } catch (ParseException e) {
      	LOG.error(CCStringUtils.stringifyException(e));
      }
		}
  	return lastValidIterationNo;
  }
  
  private void loadState() throws IOException { 
  	FileSystem fs = CrawlEnvironment.getDefaultFileSystem();
  	
  	// allocate a new server state ... 
  	_serverState = new PRMasterState();
  	// initially in idle state 
  	_serverState.setServerStatus(PRMasterState.ServerStatus.IDLE);
  	// paths 
  	Path valuesFilePath = new Path("crawl/pageRank/seed/" + _jobId + "/values");
  	Path edgesFilePath = new Path("crawl/pageRank/seed/" + _jobId + "/edges");
  	// figure out number of values
  	int itemCount = fs.globStatus(new Path(valuesFilePath,"value_*")).length;
  	LOG.info("There are:" + itemCount + " values for job:" + _jobId);
  	Path jobsPath = new Path("crawl/pageRank/jobs/" + _jobId);
  	fs.mkdirs(jobsPath);
  	// find last valid iteration 
  	int lastValidIteration = findListValidIteration(fs,jobsPath);
  	LOG.info("Last Valid Iteration for Job:" + _jobId + " is:" + lastValidIteration);
  	int nextIteration = lastValidIteration + 1;
  	// clear all check point and distribution files 
  	clearAllCheckpointAndDistributionFiles(fs,jobsPath);
  	
  	
    PageRankJobConfig jobConfig = new PageRankJobConfig();
    
    jobConfig.setJobId(_jobId);
    jobConfig.setIterationNumber(nextIteration);
    jobConfig.setMaxIterationNumber(1000);
    jobConfig.setSlaveCount(itemCount);
    jobConfig.setInputValuesPath(valuesFilePath.toString());
    jobConfig.setOutlinksDataPath(edgesFilePath.toString());
    // set up job dir ... 
    Path jobPath = new Path(_hdfsWorkingDir,Long.toString(jobConfig.getJobId()));
    jobConfig.setJobWorkPath(jobPath.toString());
    jobConfig.setAlgorithmId(0);
    jobConfig.setAlpha(.85f);
    
    _jobConfig = jobConfig;
    
    // update server state ...
    _serverState.setActiveJobConfig(_jobConfig);
    _serverState.setFieldDirty(PRMasterState.Field_ACTIVEJOBCONFIG);
    _serverState.setServerStatus(PRMasterState.ServerStatus.STARTED);
    
  }
	/*
  private void loadState() throws IOException {
    // initialize database ... 
    File databasePath = new File(getDataDirectory().getAbsolutePath() + "/" + CrawlEnvironment.PRMASTER_DB);
    LOG.info("Config says PRMaster State db path is: "+databasePath);
   
    
    // initialize record store
    _recordStore.initialize(databasePath, null);
    
    // load db state ... 
    _serverState = (PRMasterState) _recordStore.getRecordByKey(PRMasterStateKey);
    
    if (_serverState == null) {
      // allocate a brand new state 
      _serverState = new PRMasterState();
      // update crc 
      _serverState.setLastKnownSlaveFileCRC(_slavesFileCRC);
      // and write to disk ... 
      _recordStore.beginTransaction();
      _recordStore.insertRecord(null, PRMasterStateKey, _serverState);
      _recordStore.commitTransaction();
      
    }
    else { 
      // validate crc 
      if (_serverState.getLastKnownSlaveFileCRC() != _slavesFileCRC) { 
        LOG.warn("Slave Config changed since last load. Discarding any pending transactions");
        if (_serverState.isFieldDirty(PRMasterState.Field_ACTIVEJOBCONFIG)) { 
          // clear disk state for last job ... 
          finishPageRankJob(_serverState.getActiveJobConfig(),true);
          // clear server state 
          _serverState.getActiveJobConfig().clear();
          // set field clean on job config
          _serverState.setFieldClean(PRMasterState.Field_ACTIVEJOBCONFIG);
          // set state to idle ... 
          _serverState.setServerStatus(PRMasterState.ServerStatus.IDLE);
          // and write new state to disk 
          serializeServerState();
        }
      }
    }
    
    // now load job configs ... 
    Vector<Long> jobRecordList = _recordStore.getChildRecordsByParentId(PRJobConfigKey);
    
    LOG.info("Found "+ jobRecordList.size() + " serialized jobs. Loading job configs...");
    
    for (long recordId : jobRecordList) { 
      PageRankJobConfig jobConfig = (PageRankJobConfig) _recordStore.getRecordById(recordId);
      _jobList.add(jobConfig);
    }
    //TODO:HACK
    _serverState.setServerStatus(PRMasterState.ServerStatus.ITERATING_CALCULATING);
    _serverState.getActiveJobConfig().setIterationNumber(10);
    
  }
  */
  
  /*
  private void serializeJobConfig(PageRankJobConfig jobConfig) throws IOException { 
    if (jobConfig.getRecordId() != 0) { 
      _recordStore.beginTransaction();
      _recordStore.updateRecordById(jobConfig.getRecordId(), jobConfig);
      _recordStore.commitTransaction();
    }
    else { 
      _recordStore.beginTransaction();
      _recordStore.insertRecord(PRJobConfigKey, PRJobConfigKey + "_" + jobConfig.getJobId(), jobConfig);
      _recordStore.commitTransaction();
    }
  }
  */
  
  private static synchronized String getOutputName(String prefix,int instanceId) { 
    return prefix + NUMBER_FORMAT.format(instanceId);
  }
  
  /*
  private void finishPageRankJob(PageRankJobConfig config,boolean jobFailed)throws IOException { 
    LOG.info("Finishing PageRankJob:" + config.getJobId() +  "JobFailed:" + jobFailed);
    
    Path jobBasePath = new Path(config.getJobWorkPath());
    
    LOG.info("Constructing Output Directory:" + config.getOutputValuesPath());
    if (!jobFailed) { 
      Path outputPath = new Path(config.getOutputValuesPath());
      //_fileSystem.mkdirs(outputPath);
      //_fileSystem.delete(new Path(outputPath,"*"), true);
      LOG.info("Copying data files to output directory");
      
      // iterate slaves collecting data ... 
      for (int i=0;i<_slaves.size();++i) { 
        Path jobInstancePath = new Path(jobBasePath,getOutputName("",i));
        Path jobOutputPath = new Path(jobInstancePath,Constants.PR_VALUES_FILE_DIR + "/part-0");
        Path finalDestinationPath = new Path(outputPath,getOutputName("part-",i));
        LOG.info("Moving " + jobOutputPath + " to:" + finalDestinationPath);
        //_fileSystem.rename(jobOutputPath, finalDestinationPath);
      }
    }
    LOG.info("Purging Job Dir");
    //_fileSystem.delete(jobBasePath,true);
  }
  */
  
  private String dumpPageRankJobInfo(PageRankJobConfig config) { 
    
    return "InputPath:" + config.getInputValuesPath() 
    + " OutputPath:" + config.getOutputValuesPath() 
    + " Algorithm:" + config.getAlgorithmId() 
    + " Alpha:" + config.getAlpha();
  }
  
  /*
  private void potentiallyStartNextPRJob() throws IOException { 
    
    if (true) { 
      while (_jobQueue.size() != 0) { 
        // get first element off of queue ...
        PageRankJobConfig config = _jobQueue.remove(0);
        
        LOG.info("Processing Job:" + dumpPageRankJobInfo(config));
              
        /*
        // validate the input path ... 
        Path inputValuePath = new Path(config.getInputValuesPath());
        FileStatus[] valuesPaths = _fileSystem.globStatus(new Path(inputValuePath,"part-?????"));
  
        Path outlinksPath = new Path(config.getOutlinksDataPath());
        FileStatus[] outlinksPaths = _fileSystem.globStatus(new Path(inputValuePath,"part-?????"));
        */
        
/*        
        if (outlinksPaths.length != _slaves.size()) { 
          LOG.error("Rejecting Job. NEED " + _slaves.size() + " Outlink Files - Found:" + outlinksPaths.length);
        }
        else
 */        
  /*

  			{ 
          // set up job dir ... 
          Path jobPath = new Path(_hdfsWorkingDir,"job-" + config.getJobId());
          // mk the job dir dir 
          _fileSystem.mkdirs(jobPath);
          LOG.info("Assigning Job WorkingDir:" + jobPath);
          config.setJobWorkPath(jobPath.toString());
          */
        	/*      
          // update server state ...
          _serverState.setActiveJobConfig(config);
          _serverState.setFieldDirty(PRMasterState.Field_ACTIVEJOBCONFIG);
          _serverState.setServerStatus(PRMasterState.ServerStatus.STARTED);
          // serilalize the state ... 
          serializeServerState();
          
          // and send out start page rank command
          sendSlavesStartPageRankCmd(PRMasterState.ServerStatus.STARTED);
          
          break;
        }
      }
    }
  }
  */
  
  void sendSlavesStartPageRankCmd(int serverStatus){ 
    for (PageRankRemoteSlave slave : _slaves) { 
      slave.sendStartPageRankCmd(serverStatus); 
     }
  }
  
  TreeSet<PageRankRemoteSlave> _deferredSlaves = new TreeSet<PageRankRemoteSlave>();
  
  void sendSlavesDoIterationCmd() {
    if (_serverState.getServerStatus() == PRMasterState.ServerStatus.ITERATING_DISTRIBUTING) {
      int i = 0;
      int desiredInitialRunCount = _slaves.size();  
      for (PageRankRemoteSlave slave : _slaves) { 
        if (i++ < desiredInitialRunCount) { 
          sendSlaveDoIterationCmd(slave);
        }
        else { 
         _deferredSlaves.add(slave); 
        }
      }
    }
    else { 
      for (PageRankRemoteSlave slave : _slaves) {
        sendSlaveDoIterationCmd(slave);
      }
    }
  }
  void sendSlaveDoIterationCmd(PageRankRemoteSlave slave) {
    if (_serverState.getServerStatus() == PRMasterState.ServerStatus.ITERATING_DISTRIBUTING) { 
      if (_deferredSlaves.contains(slave)) { 
        LOG.error("Invalid doIteration Cmd on Deferred Slave!");
        return;
      }
    }
    slave.sendDoIterationCmd();
  }
  
  void sendSlaveCheckpointCommand(int slaveIndex,long txnId,int currentPhase,int currentIterationNumber) { 
  	PageRankRemoteSlave slave = _slaves.get(slaveIndex);
  	slave.sendCheckpointCommand(txnId,currentPhase,currentIterationNumber);
  }
  
  void sendSlavesEndPageRankCmd() { 
    for (PageRankRemoteSlave slave : _slaves) { 
      slave.sendEndPageRankCmd(); 
    }
  }
}
