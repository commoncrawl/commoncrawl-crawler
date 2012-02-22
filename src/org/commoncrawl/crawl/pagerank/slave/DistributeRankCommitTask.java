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
import org.commoncrawl.async.CallbackWithResult;
import org.commoncrawl.crawl.pagerank.CheckpointInfo;
import org.commoncrawl.crawl.pagerank.slave.PageRankTask.PageRankTaskResult;


/** 
 * DistributeRankCommitTask 
 * 
 * Executed when all nodes in the cluster have succeffully distributed their rank for the current iteration ... 
 * 
 * @author rana
 *
 */
public class DistributeRankCommitTask extends PageRankTask<DistributeRankCommitTask.TaskResult>{

  private static final Log LOG = LogFactory.getLog(DistributeRankCommitTask.class);

  CheckpointInfo _checkpointInfo;
  
  public DistributeRankCommitTask(PageRankSlaveServer server,CheckpointInfo checkpointInfo,CallbackWithResult<TaskResult> completionCallback) {
    super(server,DistributeRankCommitTask.TaskResult.class, completionCallback);
    
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
    return "Distribute Rank Commit Task";
  }

  @Override
  protected TaskResult runTask() throws IOException {
  	
    return new TaskResult(true);
  }
}
