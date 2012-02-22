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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.commoncrawl.async.CallbackWithResult;
import org.commoncrawl.crawl.pagerank.Constants;
import org.commoncrawl.crawl.pagerank.IterationInfo;
import org.commoncrawl.crawl.pagerank.slave.PageRankTask.PageRankTaskResult;
import org.commoncrawl.util.shared.CCStringUtils;

/**
 * DistributeRank Task
 * distribute rank to other shards
 * 
 * @author rana
 *
 */
public class DistributeRankTask extends PageRankTask<DistributeRankTask.DistributeRankTaskResult>{

  private static final Log LOG = LogFactory.getLog(DistributeRankTask.class);

  public DistributeRankTask(PageRankSlaveServer server,CallbackWithResult<DistributeRankTaskResult> completionCallback) {
    super(server,DistributeRankTask.DistributeRankTaskResult.class, completionCallback);
    
  }

  public static class DistributeRankTaskResult extends PageRankTaskResult { 
    public boolean done = false;
    
    public DistributeRankTaskResult() { 
      
    }
    
    public DistributeRankTaskResult(boolean done) { 
      this.done = done;
    }
    
    public boolean isDone() { return done; }

  }
  

  @Override
  protected void cancelTask() {
    
  }

  @Override
  public String getDescription() {
    return "Distribute Rank Task";
  }

  @Override
  protected DistributeRankTaskResult runTask() throws IOException {
    
  	 
  	
  	
  	// check to see if checkpoint already exists ... 
  	Path checkpointFilePath = PageRankUtils.getCheckpointFilePath(new Path(_server.getActiveJobConfig().getJobWorkPath()),
  			IterationInfo.Phase.DISTRIBUTE, 
  			_server.getActiveJobConfig().getIterationNumber(), 
  			_server.getNodeIndex());
  	
  	// only run distribute task if checkpoint file is not present 
  	if (!_server.getFileSystem().exists(checkpointFilePath)) { 
	    LOG.info("Starting Distribute Task - Iteration Number:" + _server.getActiveJobConfig().getIterationNumber());
      // take value map and distribute it
	    if (_server.getValueMap() == null) {
	      throw new IOException("Value Map NULL! Operation Failed");
	    }
	    
	    File localOutlinksFilePath = new File(_server.getActiveJobLocalPath(),PageRankUtils.makeUniqueFileName(Constants.PR_OUTLINKS_FILE_PREFIX,0,_server.getNodeIndex()));
	    LOG.info("Local Outlinks Path is:" + localOutlinksFilePath);
	    
	    try { 
	      PageRankUtils.distributeRank(_server.getValueMap(),new Path(localOutlinksFilePath.getAbsolutePath()),false,_server.getActiveJobLocalPath(), 
	          _server.getActiveJobConfig().getJobWorkPath(), _server.getNodeIndex(), _server.getBaseConfig().getSlaveCount(), 
	          _server.getActiveJobConfig().getIterationNumber(),
	          new PageRankUtils.ProgressAndCancelCheckCallback() {
							
							@Override
							public boolean updateProgress(float percentComplete) {
								synchronized (DistributeRankTask.this) {
	                _percentComplete = percentComplete; 
                }
								return false;
							}
						});
	      
	      // ok write out the checkpoint file ... 
	      return new DistributeRankTaskResult(_server.getFileSystem().createNewFile(checkpointFilePath));
	    }
	    catch (IOException e) { 
	      LOG.error("Distribute Rank Failed with Error:" +CCStringUtils.stringifyException(e));
	      DistributeRankTaskResult result = new DistributeRankTaskResult(false);
	      result.setFailed(CCStringUtils.stringifyException(e));
	      return result;
	    }
  	}
  	// otherwise ... task already complete. skip ... 
  	else { 
  		LOG.info("Checkpoint File:" + checkpointFilePath + " exists. Skipping DistributeRankTask");
      return new DistributeRankTaskResult(true);
  	}
  }
}
