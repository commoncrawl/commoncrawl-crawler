package org.commoncrawl.service.crawlmaster;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.protocol.CrawlHistoryStatus;
import org.commoncrawl.protocol.CrawlerHistoryService;
import org.commoncrawl.rpc.base.internal.AsyncClientChannel;
import org.commoncrawl.rpc.base.internal.AsyncRequest;
import org.commoncrawl.rpc.base.internal.AsyncRequest.Callback;
import org.commoncrawl.rpc.base.internal.AsyncRequest.Status;
import org.commoncrawl.rpc.base.internal.NullMessage;
import org.commoncrawl.rpc.base.shared.RPCException;
import org.commoncrawl.util.CCStringUtils;

/** helper object used to encapsulate an online crawler's state information **/
class OnlineHistoryServerState implements AsyncClientChannel.ConnectionCallback { 
  
  public static final Log LOG = LogFactory.getLog(OnlineHistoryServerState.class);

  private String              _hostName;
  private InetSocketAddress   _serverIpAddressAndPort;
  private Date          _lastUpdateTime = new Date();
  private CrawlHistoryStatus _lastKnownStatus = new CrawlHistoryStatus();
  
  private CrawlDBServer _server;
  private boolean       _online = false;
  private boolean       _commandActive = false;
  
  private AsyncClientChannel _channel;
  private CrawlerHistoryService.AsyncStub _historyService;

  public OnlineHistoryServerState(CrawlDBServer server,String hostName,InetSocketAddress ipAndPort) throws IOException,RPCException  { 

    _server = server;
    _hostName = hostName;
    _serverIpAddressAndPort = ipAndPort;
    
    CrawlDBServer.LOG.info("OnlineHistoryServerState - Opening Channel to Host:" + _hostName);
    // initialize channel ... 
    _channel = new AsyncClientChannel(_server.getEventLoop(),_server.getServerAddress(),_serverIpAddressAndPort,this);
    _channel.open();
    _historyService = new CrawlerHistoryService.AsyncStub(_channel);
  }

  boolean isCommandActive() { 
    return _commandActive;
  }
  
  boolean isOnline() { 
    return _online;
  }
  
  @Override
  public String toString() {
    return "HistoryService for ("+_hostName+") IPAddress:" + _serverIpAddressAndPort.getAddress().getHostAddress() + " Port:" + _serverIpAddressAndPort.getPort();      
  }

  public String       getHostname() { return _hostName; } 

  public Date         getLastUpdateTime() { return _lastUpdateTime; }
  public void         setLastUpdateTime(Date time) { _lastUpdateTime = time; }
  public CrawlHistoryStatus getLastKnownStatus() { return _lastKnownStatus; }
  
  public void sendCheckpointCommand(int activeListNumber, int checkpointState) {
    
    _commandActive = true;
    
    CrawlHistoryStatus action = new CrawlHistoryStatus();
    
    action.setActiveCrawlNumber(activeListNumber);
    action.setCheckpointState(checkpointState);
    
    try {
      _historyService.checkpoint(action,new Callback<CrawlHistoryStatus, CrawlHistoryStatus>() {

        @Override
        public void requestComplete(final AsyncRequest<CrawlHistoryStatus, CrawlHistoryStatus> request) {
          _commandActive  = false;
          if (request.getStatus() == Status.Success) { 
            processStatusResponse(request.getOutput());
          }
        }
      });
    } catch (RPCException e) {
      LOG.error(CCStringUtils.stringifyException(e));
      _commandActive = false;
    }
  }
  
  private void processStatusResponse(CrawlHistoryStatus newStatus) { 
    // update update time ... 
    setLastUpdateTime(new Date());
    
    // clone the status 
    try {
      _lastKnownStatus = (CrawlHistoryStatus) newStatus.clone();
    } catch (CloneNotSupportedException e) {
    }
  }
  
  public void sendHeartbeat() { 
    if (!_commandActive) { 

      try { 
        _historyService.queryStatus(new Callback<NullMessage,CrawlHistoryStatus>() {
  
          public void requestComplete(AsyncRequest<NullMessage, CrawlHistoryStatus> request) {
            
            boolean forceDisconnect  = false;
            if (request.getStatus() == Status.Success) { 
              processStatusResponse(request.getOutput());          
            }
            else { 
              CrawlDBServer.LOG.error("Heartbeat request to history server: " + getHostname() + " failed with Status: " + request.getStatus().toString());
              forceDisconnect = true;
            }
            
            if (forceDisconnect) { 
              try {
                _channel.close();
              } catch (IOException e) {
                e.printStackTrace();
              }
            }
          } 
          
        });
      }
      catch (RPCException e ){ 
        CrawlDBServer.LOG.error(e);
        // force disconnect 
        try {
          _channel.close();
        } catch (IOException e1) {
          e1.printStackTrace();
        }       
      }
    }
  }

  public void OutgoingChannelConnected(AsyncClientChannel channel) {
    CrawlDBServer.LOG.info("HistoryServer - Connected to Host:" + _hostName);
    serverOnline();
  }

  /** crawler online callback - triggered when a crawler comes online  **/
  private void serverOnline() {
    _online = true;
  }
  
  /** crawler offline callback - triggered when crawler has gone offline (socket disconnect etc.) **/
  private void serverOffline() {
    _commandActive = false;
    _online = false;
  }
  
  
  
  public boolean OutgoingChannelDisconnected(AsyncClientChannel channel) {
    //CrawlDBServer.LOG.info("Disconnect detected on OUTGOING Connection to Host: "+ _hostName);
    serverOffline();
    return true;
  }
}