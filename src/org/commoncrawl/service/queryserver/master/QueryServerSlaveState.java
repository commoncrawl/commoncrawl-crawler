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
package org.commoncrawl.service.queryserver.master;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.async.Timer;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.rpc.base.internal.AsyncClientChannel;
import org.commoncrawl.rpc.base.internal.AsyncRequest;
import org.commoncrawl.rpc.base.internal.NullMessage;
import org.commoncrawl.rpc.base.internal.AsyncRequest.Callback;
import org.commoncrawl.rpc.base.internal.AsyncRequest.Status;
import org.commoncrawl.service.queryserver.BaseConfig;
import org.commoncrawl.service.queryserver.QueryServerSlave;
import org.commoncrawl.service.queryserver.SlaveStatus;
import org.commoncrawl.util.CCStringUtils;

///////////////////////////////////////////////////////
/* Online Crawler State Object */
///////////////////////////////////////////////////////

/** helper object used to encapsulate an online crawler's state information 
 * 
 * @author rana 
 */
public class QueryServerSlaveState implements AsyncClientChannel.ConnectionCallback { 
  
  private static final int  HEARTBEAT_TIMER_INTERVAL = 50;
  private static final Log LOG = LogFactory.getLog(QueryServerSlaveState.class);  
  private String            _hostName;
  private InetSocketAddress _hostAddress;
  private long              _lastUpdateTime = -1;
  private MasterServer      _master;
  private Timer             _heartbeatTimer = null;
  private boolean           _ignoreHeartbeats = false;
  private boolean           _online = false;
  
  private AsyncClientChannel _channel;
  private QueryServerSlave.AsyncStub _slaveService;

  public QueryServerSlaveState(MasterServer master,String hostName){ 
    
    _master = master;
    _hostName   = hostName;
    InetAddress slaveAddress = null;
    try {
      LOG.info("Resolving Slave Address for Slave:" + hostName);
      slaveAddress = InetAddress.getByName(hostName);
      LOG.info("Resolving Slave Address for Slave:" + hostName + " to:" + slaveAddress.getHostAddress());
    } catch (UnknownHostException e) {
      LOG.error("Unable to Resolve Slave HostName:" + hostName + " Exception:" + CCStringUtils.stringifyException(e));
      throw new RuntimeException("Unable to Resolve Slave HostName:" + hostName + " Exception:" + CCStringUtils.stringifyException(e));
    }
    _hostAddress = new InetSocketAddress(slaveAddress.getHostAddress(),CrawlEnvironment.DEFAULT_QUERY_SLAVE_RPC_PORT);
    
    if (_hostAddress == null) { 
      throw new RuntimeException("Invalid HostName String in Query Slave Registration: " + _hostName);
    }
    else { 
      LOG.info("Host Address for Slave:" + hostName +" is:" + _hostAddress);
    }
  }

  public void connect() throws IOException { 
    //LOG.info("Opening Channel to Host:" + _hostName);
    // initialize channel ... 
    _channel = new AsyncClientChannel(_master.getEventLoop(),_master.getServerAddress(),_hostAddress,this);
    _channel.open();
    _slaveService = new QueryServerSlave.AsyncStub(_channel);
  }
  

  public String       getHostName() { return _hostName; } 
  public int          getPort() { return CrawlEnvironment.DEFAULT_QUERY_SLAVE_RPC_PORT; }
  public String       getFullyQualifiedName() { return getHostName() + ":" + getPort(); }

  public long         getLastUpdateTime() { return _lastUpdateTime; }
  
  public boolean isOnline() { return _online; }
  
  public QueryServerSlave.AsyncStub getRemoteStub() { return _slaveService; } 
  
  void enableHeartbeats() { _ignoreHeartbeats = false; }
  void disableHeartbeats() { _ignoreHeartbeats = true; }
  boolean areHeartbeatsDisabled() { return _ignoreHeartbeats; }
  
  public void OutgoingChannelConnected(AsyncClientChannel channel) {
    LOG.info("Connected to Query Slave:" + _hostName);
    slaveOnline();
  }

  public boolean OutgoingChannelDisconnected(AsyncClientChannel channel) {
    LOG.info("Disconnect detected for Slave : "+ _hostName);
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
    
    _online = false;
    
    // kill heartbeats... 
    killHeartbeatTimer();
    // inform master ... 
    _master.slaveStatusChanged(this,null);

    // reconnect channel
    if (_channel != null) { 
      try {
        _channel.reconnect();
      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
      }
    }
  }
  
  
  private void updateSlaveStatus(SlaveStatus status) { 
    _lastUpdateTime = System.currentTimeMillis();
    // and inform master ... 
    _master.slaveStatusChanged(this,status);
  }
  
  
  private void startHeartbeatTimer() { 
    _heartbeatTimer = new Timer(HEARTBEAT_TIMER_INTERVAL,false,new Timer.Callback() {

      @Override
      public void timerFired(final Timer timer) {

        // LOG.info("Heartbeat Timer Fired. Seconding heartbeat message to slave:" + getFullyQualifiedName());
        try { 
          _slaveService.heartbeat(new Callback<NullMessage,SlaveStatus>() {

            public void requestComplete(AsyncRequest<NullMessage, SlaveStatus> request) {
              
              // LOG.info("Received Heartbeat message Response from Slave:"+ getFullyQualifiedName());
              
              boolean forceDisconnect = false;
              
              if (request.getStatus() == AsyncRequest.Status.Success) { 

                if (!areHeartbeatsDisabled()) {
                  if (request.getOutput().getQueryStatus().size() != 0) { 
                    // LOG.info("Received non-zero QueryStatus list from slave:" + QueryServerSlaveState.this.getFullyQualifiedName());
                  }
                  // LOG.info("updating SlaveStatus from heartbeat response for Slave:"+ getFullyQualifiedName());
                  // update slave status ...
                  updateSlaveStatus(request.getOutput());
                }
                else { 
                  // LOG.info("heartbeats are disabled. Skipping response for Slave:"+ getFullyQualifiedName());
                }

                // need to SET timer because we are not in timerFired context anymore 
                _master.getEventLoop().setTimer(timer);
                
              }
              else { 
                // LOG.error("Heartbeat request to slave: " + getFullyQualifiedName() +" failed with Status: " + request.getStatus().toString());
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
  
  
}
