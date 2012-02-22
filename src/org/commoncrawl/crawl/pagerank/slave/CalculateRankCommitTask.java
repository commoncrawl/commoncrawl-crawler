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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.commoncrawl.async.CallbackWithResult;
import org.commoncrawl.crawl.pagerank.CheckpointInfo;
import org.commoncrawl.crawl.pagerank.Constants;
import org.commoncrawl.crawl.pagerank.IterationInfo;
import org.commoncrawl.crawl.pagerank.slave.PageRankTask.PageRankTaskResult;


/** 
 * CalculateRankCommit Task -  Called when all nodes have finished the Calculation Task 
 * 
 * for the current iteration 
 * @author rana
 *
 */
public class CalculateRankCommitTask extends PageRankTask<CalculateRankCommitTask.TaskResult>{

  private static final Log LOG = LogFactory.getLog(CalculateRankCommitTask.class);

  
  CheckpointInfo _checkpointInfo;
  
  public CalculateRankCommitTask(PageRankSlaveServer server,CheckpointInfo checkpointInfo,CallbackWithResult<CalculateRankCommitTask.TaskResult> completionCallback) {
    super(server,CalculateRankCommitTask.TaskResult.class, completionCallback);
    
    _checkpointInfo = checkpointInfo;
  }

  public static class TaskResult extends PageRankTaskResult { 
    public boolean done = false;
    
    public TaskResult() { 
      
    }
    
    public TaskResult(boolean done) { 
      this.done = done;
    }
    
    public boolean isDone() { return done; }

  }
  

  @Override
  protected void cancelTask() {
    
  }

  @Override
  public String getDescription() {
    return "Calculate Rank Commit Task";
  }

  @Override
  protected TaskResult runTask() throws IOException {
  	
  	LOG.info("Starting CalculateRankCommit Task");
  	// ok time to delete previous iteration's outlink data 
  	if (_server.getActiveJobConfig().getIterationNumber() != 0) { 
  		LOG.info("Deleting data from pervious iteration");
  		
  		// get values path ... 
      Path oldValuesPath = new Path(_server.getActiveJobConfig().getJobWorkPath(),PageRankUtils.makeUniqueFileName(Constants.PR_VALUE_FILE_PREFIX,
      		_server.getActiveJobConfig().getIterationNumber() - 1 ,_server.getNodeIndex()));
      // and delete the associated file ...
      LOG.info("Deleting:" + oldValuesPath);
      _server.getFileSystem().delete(oldValuesPath,false);
  		
      //TODO:HACK
      LOG.info("Deleting Distribution Files for Iteration:" + (_server.getActiveJobConfig().getIterationNumber() - 1));
      // purge distribution files for previous iteration ...
  		PageRankUtils.purgeNodeDistributionFilesForIteration(
  				_server.getFileSystem(), 
  				_server.getActiveJobConfig().getJobWorkPath(),
  				_server.getNodeIndex(), _server.getBaseConfig().getSlaveCount(),
  				_server.getActiveJobConfig().getIterationNumber() - 1);
  		
  		// ok also delete checkpoint files for previous iteration ... 
    	Path calculateCheckpointFilePath = PageRankUtils.getCheckpointFilePath(new Path(_server.getActiveJobConfig().getJobWorkPath()),
    			IterationInfo.Phase.CALCULATE, 
    			_server.getActiveJobConfig().getIterationNumber() - 1, 
    			_server.getNodeIndex());  		

    	Path distributeCheckpointFilePath = PageRankUtils.getCheckpointFilePath(new Path(_server.getActiveJobConfig().getJobWorkPath()),
    			IterationInfo.Phase.DISTRIBUTE, 
    			_server.getActiveJobConfig().getIterationNumber() - 1, 
    			_server.getNodeIndex());  		
    	
    	LOG.info("Deleting:" + calculateCheckpointFilePath);
    	_server.getFileSystem().delete(calculateCheckpointFilePath,false);
    	LOG.info("Deleting:" + distributeCheckpointFilePath);
    	_server.getFileSystem().delete(distributeCheckpointFilePath,false);
  	}
  	  	
    return new TaskResult(true);
  }
}
