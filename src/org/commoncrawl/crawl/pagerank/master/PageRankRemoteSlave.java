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

package org.commoncrawl.crawl.pagerank.master;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.async.Timer;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.crawl.pagerank.BaseConfig;
import org.commoncrawl.crawl.pagerank.BeginPageRankInfo;
import org.commoncrawl.crawl.pagerank.CheckpointInfo;
import org.commoncrawl.crawl.pagerank.IterationInfo;
import org.commoncrawl.crawl.pagerank.PageRankJobConfig;
import org.commoncrawl.crawl.pagerank.PageRankSlave;
import org.commoncrawl.crawl.pagerank.SlaveStatus;
import org.commoncrawl.rpc.base.internal.AsyncClientChannel;
import org.commoncrawl.rpc.base.internal.AsyncRequest;
import org.commoncrawl.rpc.base.internal.NullMessage;
import org.commoncrawl.rpc.base.internal.AsyncRequest.Callback;
import org.commoncrawl.rpc.base.internal.AsyncRequest.Status;
import org.commoncrawl.util.shared.CCStringUtils;


/**
 * Helper object used to encapsulate an online page-rank slave's state information
 * @author rana
 *
 */
public class PageRankRemoteSlave implements AsyncClientChannel.ConnectionCallback, Comparable<PageRankRemoteSlave> { 
 
  private static final int  HEARTBEAT_TIMER_INTERVAL = 10000;
  private static final Log LOG = LogFactory.getLog(PageRankRemoteSlave.class);  
  private int               _slaveId;
  private int               _instanceId;
  private String            _hostName;
  private InetSocketAddress _hostAddress;
  private long              _lastUpdateTime = -1;
  private PageRankMaster    _master;
  private Timer             _heartbeatTimer = null;
  private SlaveStatus       _lastKnownStatus = new SlaveStatus();
  private boolean           _ignoreHeartbeats = false;
  private boolean           _online = false;
  
  private AsyncClientChannel _channel;
  private PageRankSlave.AsyncStub _slaveService;

  public PageRankRemoteSlave(PageRankMaster master,int slaveId,String hostName,int instanceId){ 
    
    _master = master;
    _slaveId  = slaveId;
    _hostName   = hostName;
    _instanceId = instanceId;
    InetAddress slaveAddress = null;
    try {
      LOG.info("Resolving Slave Address for Slave:" + hostName);
      slaveAddress = InetAddress.getByName(hostName);
      LOG.info("Resolving Slave Address for Slave:" + hostName + " to:" + slaveAddress.getHostAddress());
    } catch (UnknownHostException e) {
      LOG.error("Unable to Resolve Slave HostName:" + hostName + " Exception:" + CCStringUtils.stringifyException(e));
      throw new RuntimeException("Unable to Resolve Slave HostName:" + hostName + " Exception:" + CCStringUtils.stringifyException(e));
    }
    _hostAddress = new InetSocketAddress(slaveAddress.getHostAddress(),CrawlEnvironment.DEFAULT_PAGERANK_SLAVE_RPC_PORT + (_instanceId * 2));
    
    if (_hostAddress == null) { 
      throw new RuntimeException("Invalid HostName String in PageRank Slave Registration: " + _hostName);
    }
    else { 
      LOG.info("Host Address for Slave:" + hostName +" is:" + _hostAddress);
    }
  }

  
  /**
   * connect to remote 
   * 
   * @throws IOException
   */
  public void connect() throws IOException { 
    LOG.info("Opening Channel to Host:" + _hostName);
    // initialize channel ... 
    _channel = new AsyncClientChannel(_master.getEventLoop(),_master.getServerAddress(),_hostAddress,this);
    _channel.open();
    _slaveService = new PageRankSlave.AsyncStub(_channel);
  }
  

  public int          getSlaveId() { return _slaveId; } 
  public String       getHostName() { return _hostName; } 
  public int          getPort() { return CrawlEnvironment.DEFAULT_PAGERANK_SLAVE_RPC_PORT + (_instanceId * 2); }
  public String       getFullyQualifiedName() { return getHostName() + ":" + getPort(); }
  public InetSocketAddress getHostAddress() { return _hostAddress; }

  public long         getLastUpdateTime() { return _lastUpdateTime; }
  
  public String       getStatusText() { 
    if (_online) { 
      String statusText = SlaveStatus.State.toString(_lastKnownStatus.getState());
      if (_lastKnownStatus.isFieldDirty(SlaveStatus.Field_PERCENTCOMPLETE)) { 
      	statusText += " (" + _lastKnownStatus.getPercentComplete() + "% Complete)";
      }
      return statusText;
    }
    else { 
      return "Offline";
    }
  }
  public SlaveStatus  getLastKnowStatus() { return _lastKnownStatus; }
  
  private void enableHeartbeats() { _ignoreHeartbeats = false; }
  private void disableHeartbeats() { _ignoreHeartbeats = true; }
  private boolean areHeartbeatsDisabled() { return _ignoreHeartbeats; }
  
  public void OutgoingChannelConnected(AsyncClientChannel channel) {
    LOG.info("Connected to PageRank Slave:" + _hostName);
    slaveOnline();
  }

  public boolean OutgoingChannelDisconnected(AsyncClientChannel channel) {
    //LOG.info("Disconnect detected for Slave : "+ _hostName);
    slaveOffline();
    return false;
  }
  
  private void slaveOnline() {
    
    try { 
      // initialize the slave ... 
      _slaveService.initialize(_master.getBaseConfigForSlave(this), new Callback<BaseConfig,SlaveStatus> () {
  
          @Override
          public void requestComplete(AsyncRequest<BaseConfig, SlaveStatus> request) {
            if (request.getStatus() != Status.Success) { 
              LOG.error("resetState failed on Slave:" + getFullyQualifiedName());
              slaveOffline();
            }
            else {
              _online = true;
              
              // notify master of status change ...
              updateSlaveStatus(request.getOutput());
              // start the heartbeat timer ... 
              startHeartbeatTimer();
            }
          }
      });
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      slaveOffline();
    }
  }
  
  private void slaveOffline() { 
    boolean wasOnline = _online;
    _online = false;
    
    // kill heartbeats... 
    killHeartbeatTimer();
    // clear out last know status 
    _lastKnownStatus.clear();
    if (wasOnline) { 
      // inform master ... 
      _master.slaveStatusChanged(this);
    }

    // reconnect channel
    if (_channel != null) { 
      try {
        _channel.reconnect();
      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
      }
    }
  }
  
  void sendStartPageRankCmd(int serverStatus) { 
    if (_channel != null && _channel.isOpen()) { 
      try { 
        
        // disable heartbeats during this async call ... 
        disableHeartbeats();
        
        BeginPageRankInfo pageRankInfo = new BeginPageRankInfo();
        try {
	        pageRankInfo.setJobConfig((PageRankJobConfig) _master.getActiveJobConfig().clone());
        } catch (CloneNotSupportedException e) {
        }
        pageRankInfo.setServerStatus(serverStatus);
        
        _slaveService.beginPageRank(pageRankInfo,new Callback<BeginPageRankInfo,SlaveStatus>() {
  
          @Override
          public void requestComplete(AsyncRequest<BeginPageRankInfo, SlaveStatus> request) {
            if (request.getStatus() == Status.Success) { 
              // update cached status
              updateSlaveStatus(request.getOutput());
            }
            else { 
              LOG.error("beginPageRank to slave:" + getFullyQualifiedName() + " Failed with status:" + request.getStatus());
              // reset connection 
              slaveOffline();
            }
            // enable heartbeats here ...
            enableHeartbeats();
          } 
        });
      }
      catch (IOException e) {
        // we have to renable heartbeats here ... 
        enableHeartbeats();
        
        LOG.error(CCStringUtils.stringifyException(e));
        // restart connection ... 
        slaveOffline();
      }
    }
    else { 
      LOG.error("sendStartPageRank called on Slave with Invalid State. Slave:" + getFullyQualifiedName());
    }
    
  }
  
  void sendEndPageRankCmd() { 
    if (_channel != null && _channel.isOpen()) {
      
      // disable heartbeats during this async call ... 
      disableHeartbeats();
      
      try { 
        _slaveService.endPageRank(new Callback<NullMessage,SlaveStatus>() {
  
          @Override
          public void requestComplete(AsyncRequest<NullMessage, SlaveStatus> request) {
            if (request.getStatus() == Status.Success) { 
              // update cached status
              updateSlaveStatus(request.getOutput());
            }
            else { 
              LOG.error("RPC Failed during sendEndPageRank call to Host:" + getFullyQualifiedName());
              slaveOffline();
            }
            // no matter what - renable heartbeats 
            enableHeartbeats();
          }
          
        });
      }
      catch (IOException e) { 
        // we have to renable heartbeats here ... 
        enableHeartbeats();
       
        LOG.error(CCStringUtils.stringifyException(e));
      }
    }
    else { 
      LOG.error("sendEndPageRank called on Slave with Invalid State. Slave:" + getFullyQualifiedName());
    }
    
  }
  
  void sendCheckpointCommand(long txnId, int currentPhase,int currentIterationNumber) {
  	
  	if (_channel != null && _channel.isOpen()) {
  		this._lastKnownStatus.setCurrentCheckpointId(txnId);

  		// disable heartbeats during this async call ... 
      disableHeartbeats();
      
      CheckpointInfo checkpointInfo = new CheckpointInfo();
      
      checkpointInfo.setTxnId(txnId);
      checkpointInfo.setCurrentPhase(currentPhase);
      checkpointInfo.setCurrentIterationNumber(currentIterationNumber);
      
      try { 
      	_slaveService.checkpoint(checkpointInfo, new Callback<CheckpointInfo,SlaveStatus>() {

					@Override
          public void requestComplete(AsyncRequest<CheckpointInfo, SlaveStatus> request) {
						try { 
	            if (request.getStatus() == Status.Success) { 
	              // update cached status
	              updateSlaveStatus(request.getOutput());
	            }
	            else { 
	              LOG.error("RPC Failed during sendCheckpoint call to Host:" + getFullyQualifiedName());
	              slaveOffline();
	            }
						}
						finally { 
							// we have to renable heartbeats here ... 
							enableHeartbeats();
						}
          } 
      		
      	});
      }
      catch (IOException e) {
        
        // we have to renable heartbeats here ... 
        enableHeartbeats();
        
        LOG.error(CCStringUtils.stringifyException(e));
        // reboot the connection...
        slaveOffline();
      }      
  	}
  	else { 
      LOG.error("sendCheckpoint called on Slave with Invalid State. Slave:" + getFullyQualifiedName());
    }  	
  	
  }
  
  void sendDoIterationCmd() { 
    if (_channel != null && _channel.isOpen()) {
      
      // disable heartbeats during this async call ... 
      disableHeartbeats();
      
      IterationInfo iterationInfo = new IterationInfo();
      
      iterationInfo.setJobId(_master.getCurrentJobNumber());
      iterationInfo.setIterationNumber(_master.getCurrentIterationNumber());
      iterationInfo.setPhase(_master.getSlaveIterationPhase());
      
      try { 
        _slaveService.doIteration(iterationInfo, new Callback<IterationInfo,SlaveStatus>() {
  
          @Override
          public void requestComplete(AsyncRequest<IterationInfo, SlaveStatus> request) {
            if (request.getStatus() == Status.Success) { 
              // update cached status
              updateSlaveStatus(request.getOutput());
            }
            else { 
              LOG.error("RPC Failed during sendEndPageRank call to Host:" + getFullyQualifiedName());
              slaveOffline();
            }
            // we have to renable heartbeats here ... 
            enableHeartbeats();
          }
        });
      }
      catch (IOException e) {
        
        // we have to renable heartbeats here ... 
        enableHeartbeats();
        
        LOG.error(CCStringUtils.stringifyException(e));
        // reboot the connection...
        slaveOffline();
      }
    }
    else { 
      LOG.error("sendDoIteration called on Slave with Invalid State. Slave:" + getFullyQualifiedName());
    }
  }

  void sendResetCmd() {
    if (_channel != null && _channel.isOpen()) {
      try { 
        
        // disable heartbeats during this async call ... 
        disableHeartbeats();
        
        // re-initialize the slave ... 
        _slaveService.initialize(_master.getBaseConfigForSlave(this), new Callback<BaseConfig,SlaveStatus> () {
    
            @Override
            public void requestComplete(AsyncRequest<BaseConfig, SlaveStatus> request) {
              if (request.getStatus() != Status.Success) { 
                LOG.error("resetState failed on Slave:" + getFullyQualifiedName());
                slaveOffline();
              }
              else {
                // notify master of status change ...
                updateSlaveStatus(request.getOutput());
              }
              // we have to re-enable heartbeats here ... 
              enableHeartbeats();
            }
        });
      }
      catch (IOException e) {
        // we have to re-enable heartbeats here ... 
        enableHeartbeats();

        LOG.error(CCStringUtils.stringifyException(e));
        slaveOffline();
      }
    }
  }
  
  private void updateSlaveStatus(SlaveStatus status) { 
    _lastUpdateTime = System.currentTimeMillis();
    _lastKnownStatus.clear();
    try {
      _lastKnownStatus.merge(status);
    } catch (CloneNotSupportedException e) {
    }
    // and inform master ... 
    _master.slaveStatusChanged(this);
  }
  
  
  private void startHeartbeatTimer() { 
    _heartbeatTimer = new Timer(HEARTBEAT_TIMER_INTERVAL,false,new Timer.Callback() {

      @Override
      public void timerFired(final Timer timer) {

        LOG.info("Heartbeat Timer Fired. Seconding heartbeat message to slave:" + getFullyQualifiedName());
        try { 
          _slaveService.heartbeat(new Callback<NullMessage,SlaveStatus>() {

            public void requestComplete(AsyncRequest<NullMessage, SlaveStatus> request) {
              
              LOG.info("Received Heartbeat message Response from Slave:"+ getFullyQualifiedName());
              
              boolean forceDisconnect = false;
              
              if (request.getStatus() == AsyncRequest.Status.Success) { 

                if (!areHeartbeatsDisabled()) {
                  LOG.info("updating SlaveStatus from heartbeat response for Slave:"+ getFullyQualifiedName());
                  // update slave status ...
                  updateSlaveStatus(request.getOutput());
                }
                else { 
                  LOG.info("heartbeats are disabled. Skipping response for Slave:"+ getFullyQualifiedName());
                }

                // need to SET timer because we are not in timerFired context anymore 
                _master.getEventLoop().setTimer(timer);
                
              }
              else { 
                LOG.error("Heartbeat request to slave: " + getFullyQualifiedName() +" failed with Status: " + request.getStatus().toString());
                forceDisconnect = true;
              }
              
              if (forceDisconnect) { 
                slaveOffline();
              }
            } 
            
          });
        }
        catch (IOException e ){ 
          slaveOffline();
          LOG.error(CCStringUtils.stringifyException(e));
        }
      } 
      
    });
    
    _master.getEventLoop().setTimer(_heartbeatTimer);
  }
  
  private void killHeartbeatTimer() { 
    if (_heartbeatTimer != null) { 
      _master.getEventLoop().cancelTimer(_heartbeatTimer);
      _heartbeatTimer = null;
    }
  }


  @Override
  public int compareTo(PageRankRemoteSlave other) {
    return (_slaveId < other._slaveId) ? -1 : (_slaveId > other._slaveId) ? 1 : 0;
  }
  
  
}
