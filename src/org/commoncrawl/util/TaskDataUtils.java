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

package org.commoncrawl.util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.protocol.CrawlDBService;
import org.commoncrawl.protocol.MapReduceTaskIdAndData;
import org.commoncrawl.protocol.SimpleByteResult;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.rpc.base.internal.AsyncClientChannel;
import org.commoncrawl.rpc.base.internal.AsyncRequest;
import org.commoncrawl.rpc.base.internal.NullMessage;

/**
 * 
 * @author rana
 *
 */
public class TaskDataUtils {

  private static final Log LOG = LogFactory.getLog(TaskDataUtils.class);
  
  /** used to set task data connection info into job config BEFORE submitting the job to the JobTracker**/
  public static final void initializeTaskDataJobConfig(JobConf jobconf,long jobId,InetSocketAddress connectionInfo) {
    jobconf.set("taskdata.service.connectionInfo", connectionInfo.getAddress().getHostAddress() + ":" + connectionInfo.getPort());
    jobconf.set("taskdata.service.jobid", Long.toString(jobId));
  }
  
  public static final long getTaskDataJobIdFromJobConfig(JobConf jobconf) {
    return Long.parseLong(jobconf.get("taskdata.service.jobid", "0"));
  }
  
  
  /** used by the Map/Reduce Task to construct a task data client **/
  public static final TaskDataClient getTaskDataClientForTask(JobConf jobconf) throws IOException { 
    return new TaskDataClient(jobconf);
  }
  
  
  public static class TaskDataClient implements AsyncClientChannel.ConnectionCallback { 

    EventLoop                 _eventLoop = null;
    AsyncClientChannel        _channel;
    CrawlDBService.AsyncStub  _asyncStub;
    Semaphore                 _blockingCallSemaphore = null;
    IOException               _lastIOException = null;

    
    private String _connectionData;
    private String _jobId;
    private String _taskId;

    
    /** internal constructor used to create a new taskdataclient and establish a connection to the 
     * task data server **/
    TaskDataClient(JobConf jobConf) throws IOException {
      _connectionData = jobConf.get("taskdata.service.connectionInfo");
      _jobId          = jobConf.get("taskdata.service.jobid");
      
      if (_connectionData == null || _jobId == null) { 
        throw new IOException("No TaskData Server Information in JobConfig!");
      }
      // extract task id ... 
      TaskAttemptID attemptId = TaskAttemptID.forName(jobConf.get("mapred.task.id"));
      _taskId = Integer.toString(attemptId.getTaskID().getId());
      
      // start event loop ... 
      _eventLoop = new EventLoop();
      _eventLoop.start();
      
      // parse connection sting 
      String connectionParts[] = _connectionData.split(":");
      if (connectionParts.length != 2) { 
        throw new IOException("Invalid Connection String!");
      }
      else { 
        // construct socket address 
        InetSocketAddress endPoint = new InetSocketAddress(InetAddress.getByName(connectionParts[0]),Integer.parseInt(connectionParts[1]));
        
        LOG.info("Connecting to server at:" + endPoint);
        _channel = new AsyncClientChannel(_eventLoop,null,endPoint,this);
        _blockingCallSemaphore = new Semaphore(0);
        _channel.open();
        _asyncStub = new CrawlDBService.AsyncStub(_channel);
        LOG.info("Waiting on Connect... ");
        _blockingCallSemaphore.acquireUninterruptibly();
        LOG.info("Connect Semaphore Released... ");
        _blockingCallSemaphore = null;
        
        if (!_channel.isOpen()) { 
          throw new IOException("Connection Failed!");
        }
      }
    }
    
    
    /** used by the map/reduce task to set task data by key **/
    public void updateTaskData(String key,String value) throws IOException { 
      
      _blockingCallSemaphore = new Semaphore(0);
      _lastIOException = null;
      
      MapReduceTaskIdAndData taskIdAndData = new MapReduceTaskIdAndData();
      
      taskIdAndData.setJobId(_jobId);
      taskIdAndData.setTaskId(_taskId);
      taskIdAndData.setDataKey(key);
      taskIdAndData.setDataValue(value);
      
      
      _asyncStub.updateMapReduceTaskValue(taskIdAndData, new AsyncRequest.Callback<MapReduceTaskIdAndData, NullMessage> () {

        @Override
        public void requestComplete(AsyncRequest<MapReduceTaskIdAndData, NullMessage> request) {
          if (request.getStatus() != AsyncRequest.Status.Success) { 
            LOG.error("updateMapReduceTaskValue Request Failed");
            _lastIOException = new IOException("updateMapReduceTaskValue Request Failed");
          }
          //release blocking semaphore
          _blockingCallSemaphore.release();
        } 
      });
      //wait for async request to complete ... 
      _blockingCallSemaphore.acquireUninterruptibly();
      
      // if async call failed and generated an exception on the remote end ... throw the exception now
      if (_lastIOException != null) { 
        throw _lastIOException;
      }
    }
    
    /** used by the map/reduce task to query for task data by key **/
    public String queryTaskData(String key) throws IOException { 

      _blockingCallSemaphore = new Semaphore(0);
      _lastIOException = null;
      
      final MapReduceTaskIdAndData taskIdAndDataInOut = new MapReduceTaskIdAndData();
      
      taskIdAndDataInOut.setJobId(_jobId);
      taskIdAndDataInOut.setTaskId(_taskId);
      taskIdAndDataInOut.setDataKey(key);
      
      _asyncStub.queryMapReduceTaskValue(taskIdAndDataInOut, new AsyncRequest.Callback<MapReduceTaskIdAndData, MapReduceTaskIdAndData> () {

        @Override
        public void requestComplete(AsyncRequest<MapReduceTaskIdAndData, MapReduceTaskIdAndData> request) {
          if (request.getStatus() != AsyncRequest.Status.Success) { 
            LOG.error("queryTaskData Request Failed");
            _lastIOException = new IOException("queryTaskData Request Failed");
          }
          else { 
            if (request.getOutput().isFieldDirty(MapReduceTaskIdAndData.Field_DATAVALUE)) { 
              taskIdAndDataInOut.setDataValue(request.getOutput().getDataValue());
            }
          }
          //release blocking semaphore
          _blockingCallSemaphore.release();
        } 
      });
      //wait for async request to complete ... 
      _blockingCallSemaphore.acquireUninterruptibly();
      
      // if async call failed and generated an exception on the remote end ... throw the exception now
      if (_lastIOException != null) { 
        throw _lastIOException;
      }
      else { 
        if (taskIdAndDataInOut.isFieldDirty(MapReduceTaskIdAndData.Field_DATAVALUE)) { 
          return taskIdAndDataInOut.getDataValue();
        }
        return null;
      }
    }

    
    /** used by the map/reduce task to query for duplicate exclusion status **/
    public int queryDuplicateStatus(final URLFPV2 key) throws IOException { 

      _blockingCallSemaphore = new Semaphore(0);
      _lastIOException = null;
      final SimpleByteResult result = new SimpleByteResult();
      
      _asyncStub.queryDuplicateStatus(key, new AsyncRequest.Callback<URLFPV2, SimpleByteResult> () {

        @Override
        public void requestComplete(AsyncRequest<URLFPV2, SimpleByteResult> request) {
          if (request.getStatus() != AsyncRequest.Status.Success) { 
            LOG.error("queryTaskData Request Failed");
            _lastIOException = new IOException("queryTaskData Request Failed");
          }
          else { 
          	result.setByteResult(request.getOutput().getByteResult());
          }
          //release blocking semaphore
          _blockingCallSemaphore.release();
        } 
      });
      //wait for async request to complete ... 
      _blockingCallSemaphore.acquireUninterruptibly();
      
      // if async call failed and generated an exception on the remote end ... throw the exception now
      if (_lastIOException != null) { 
        throw _lastIOException;
      }
      else { 
      	return result.getByteResult();
      }
    }
    
    /** used by the map/reduce task to query for fingprint status **/
    public int queryFingerprintStatus(final URLFPV2 key) throws IOException { 

      _blockingCallSemaphore = new Semaphore(0);
      _lastIOException = null;
      final SimpleByteResult result = new SimpleByteResult();
      
      _asyncStub.queryFingerprintStatus(key, new AsyncRequest.Callback<URLFPV2, SimpleByteResult> () {

        @Override
        public void requestComplete(AsyncRequest<URLFPV2, SimpleByteResult> request) {
          if (request.getStatus() != AsyncRequest.Status.Success) { 
            LOG.error("queryTaskData Request Failed");
            _lastIOException = new IOException("queryTaskData Request Failed");
          }
          else { 
            result.setByteResult(request.getOutput().getByteResult());
          }
          //release blocking semaphore
          _blockingCallSemaphore.release();
        } 
      });
      //wait for async request to complete ... 
      _blockingCallSemaphore.acquireUninterruptibly();
      
      // if async call failed and generated an exception on the remote end ... throw the exception now
      if (_lastIOException != null) { 
        throw _lastIOException;
      }
      else { 
        return result.getByteResult();
      }
    }    
    
    /** used by the map/reduce task to purge all data related to the task **/
    public void purgeTaskData() throws IOException { 

      _blockingCallSemaphore = new Semaphore(0);
      _lastIOException = null;
      
      MapReduceTaskIdAndData taskIdAndData = new MapReduceTaskIdAndData();
      
      taskIdAndData.setJobId(_jobId);
      taskIdAndData.setTaskId(_taskId);
      
      
      _asyncStub.purgeMapReduceTaskValue(taskIdAndData, new AsyncRequest.Callback<MapReduceTaskIdAndData, NullMessage> () {

        @Override
        public void requestComplete(AsyncRequest<MapReduceTaskIdAndData, NullMessage> request) {
          if (request.getStatus() != AsyncRequest.Status.Success) { 
            LOG.error("purgeMapReduceTaskValue Request Failed");
            _lastIOException = new IOException("purgeMapReduceTaskValue Request Failed");
          }
          //release blocking semaphore
          _blockingCallSemaphore.release();
        } 
      });
      //wait for async request to complete ... 
      _blockingCallSemaphore.acquireUninterruptibly();
      
      // if async call failed and generated an exception on the remote end ... throw the exception now
      if (_lastIOException != null) { 
        throw _lastIOException;
      }
    }
    
    /** shutdown **/
    public void shutdown() { 
      if (_channel != null) { 
        try {
          _channel.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
        _channel = null;
        _eventLoop.stop();
      }
    }


    @Override
    public void OutgoingChannelConnected(AsyncClientChannel channel) {
      LOG.info("OutgoingChannelConnected... ");
      if (_blockingCallSemaphore != null) { 
        _blockingCallSemaphore.release();
      }
    }

    @Override
    public boolean OutgoingChannelDisconnected(AsyncClientChannel channel) {
      LOG.info("OutgoingChannelDisconnected... ");
      if (_blockingCallSemaphore != null) { 
        _blockingCallSemaphore.release();
      }
      return true;
    }
  }
}
