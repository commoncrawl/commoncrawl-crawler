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

/**
 * 
 * @author rana
 *
 */
public class TestTask extends PageRankTask<TestTask.TestTaskResult>{

  private static final Log LOG = LogFactory.getLog(TestTask.class);
  private String _description;
  private int    _runtimeInMilliSeconds;

  public TestTask(PageRankSlaveServer server,CallbackWithResult<TestTaskResult> completionCallback,String description,int runTimeInMilliseconds) {
    super(server,TestTask.TestTaskResult.class, completionCallback);
    
    _description = description;
    _runtimeInMilliSeconds = runTimeInMilliseconds;
  }

  public static class TestTaskResult extends PageRankTask.PageRankTaskResult { 
    public boolean done = false;
    
    public TestTaskResult(boolean done) { 
      this.done = done;
    }
    
    public boolean isDone() { return done; }

  }
  

  @Override
  protected void cancelTask() {
    
  }

  @Override
  public String getDescription() {
    return "TEST TASK";
  }

  @Override
  protected TestTaskResult runTask() throws IOException {
    long endTime = System.currentTimeMillis() + _runtimeInMilliSeconds;
    
    while (!isCancelled()) { 
      LOG.info("Test Task" + _description + " Running ... ");
      
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
      
      if (System.currentTimeMillis() >= endTime)
        break;
    }
    return new TestTaskResult(true);
  }
}
