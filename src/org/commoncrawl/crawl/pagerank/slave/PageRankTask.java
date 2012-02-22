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
import org.commoncrawl.async.Callback;
import org.commoncrawl.async.CallbackWithResult;
import org.commoncrawl.util.shared.CCStringUtils;

/** 
 * PageRank Task abstraction 
 * 
 * @author rana
 *
 * @param <ResultType>
 */
public abstract class PageRankTask<ResultType extends PageRankTask.PageRankTaskResult> implements CancelableTask {

  private static final Log LOG = LogFactory.getLog(PageRankTask.class);

  PageRankSlaveServer _server;  
  CallbackWithResult<ResultType> _callback;
  boolean _cancelling = false;
  Thread  _taskThread = null;
  ResultType _result;
  public 	float _percentComplete = 0.0f; 
  public  long  _startTime;
  
  public static class PageRankTaskResult { 
    protected boolean _failed = false;
    protected String  _failureReason = null;
    
    public boolean succeeded() { 
      return !_failed;
    }
    
    public void setFailed(String failureReason) { 
      _failed = true;
      _failureReason = failureReason;
    }
    
    public String getErrorDesc() { 
      return _failureReason;
    }
    
    
  }
  
  
  public PageRankTask(PageRankSlaveServer server,Class<ResultType> resultTypeClass,CallbackWithResult<ResultType> completionCallback) { 
    _server = server;
    _callback = completionCallback;
    try {
      _result = resultTypeClass.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /** get the task description **/
  public abstract String getDescription();
  
  /** cancel check **/
  public synchronized boolean isCancelled() { 
    return _cancelling;
  }
  
  /** start the task thread **/
  public void start() { 
  	_startTime = System.currentTimeMillis();
    LOG.info("Task start called");
    _server.taskStarting(this);
    
    _taskThread = new Thread(new Runnable() {

      @Override
      public void run() {
        
        LOG.info("Task Thread Running");
        // start the actual task 
        try {
          LOG.info("Entering runTask");
          _result = runTask();
          LOG.info("Exiting runTask");
        } catch (Exception e) {
          LOG.error(CCStringUtils.stringifyException(e));
          _result.setFailed(CCStringUtils.stringifyException(e));
        }

        boolean doCallback = true;
        
        // now synchronize on this 
        synchronized(this) { 
          
          // if NOT cancelling ...  
          if (!_cancelling) { 
            LOG.info("Exiting Task Thread Normally");
            // set done to true ... 
            _taskThread = null;
          }
          // otherwise skip callback ... 
          else { 
            LOG.info("Exiting Task via Cancel");
            doCallback = false;
          }
        }
        // now if callback is required ... 
        if (doCallback) {
          
          LOG.info("Task Thread Queueing Callback");
          // schedule an async callback with result ...
          _server.getEventLoop().queueAsyncCallback(new Callback() {

            @Override
            public void execute() {
              LOG.info("Task Thread Executing Async Callback");
              _callback.execute(_result);
              _server.taskComplete(PageRankTask.this);
            } 
            
          });
        }
      } 
    });
    LOG.info("starting Task Thread");
    _taskThread.start();
  }
  
  
  protected abstract ResultType runTask() throws IOException;
  protected abstract void cancelTask();
  
  public void cancel(final Callback cancelCompleteCallback) { 
    
    // synchronize on this ... 
    synchronized(this) { 

      _cancelling = true;

      // if the underlying task is already done, then we are late to the party ...
      if (_taskThread == null) {  
        cancelCompleteCallback.execute();
      }
      // otherwise ... set the cancelling flag ...
    }
      
    if (_taskThread != null) {
      new Thread(new Runnable() {

        @Override
        public void run() {
          try {
            // cancel the task ...
            cancelTask();
            // and wait for task thread to exit
            _taskThread.join();
            // and now null task thread ... 
            synchronized(this) { 
              _taskThread = null;
            }
          } catch (InterruptedException e) {
          }
          
          _server.getEventLoop().queueAsyncCallback(new Callback() {

            @Override
            public void execute() {
              cancelCompleteCallback.execute();
            } 
            
          });
        }
      }).start();
    }
    else { 
      cancelCompleteCallback.execute();
    }
    
  }
    

}
