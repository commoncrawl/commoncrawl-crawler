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
package org.commoncrawl.service.pagerank.slave;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.async.CallbackWithResult;
import org.commoncrawl.service.pagerank.CheckpointInfo;


/** 
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

  public static class TaskResult extends PageRankTask.PageRankTaskResult { 
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
