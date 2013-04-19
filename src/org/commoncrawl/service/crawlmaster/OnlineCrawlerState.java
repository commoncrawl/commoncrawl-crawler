package org.commoncrawl.service.crawlmaster;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.protocol.CrawlerAction;
import org.commoncrawl.protocol.CrawlerService;
import org.commoncrawl.protocol.CrawlerStatus;
import org.commoncrawl.rpc.base.internal.AsyncClientChannel;
import org.commoncrawl.rpc.base.internal.AsyncRequest;
import org.commoncrawl.rpc.base.internal.AsyncRequest.Callback;
import org.commoncrawl.rpc.base.internal.AsyncRequest.Status;
import org.commoncrawl.rpc.base.internal.NullMessage;
import org.commoncrawl.rpc.base.shared.RPCException;
import org.commoncrawl.util.CCStringUtils;

/** helper object used to encapsulate an online crawler's state information **/
class OnlineCrawlerState implements AsyncClientChannel.ConnectionCallback { 
  
  public static final Log LOG = LogFactory.getLog(OnlineCrawlerState.class);

  private String              _hostName;
  private InetSocketAddress   _crawlerIpAddressAndPort;
  private Date          _lastUpdateTime = new Date();
  private CrawlerStatus _lastKnownStatus = new CrawlerStatus();
  
  private  int  _desiredState = CrawlerStatus.CrawlerState.IDLE;
  private  int  _activeCrawlNumber = -1;
  
  private CrawlDBServer _server;
  private boolean       _crawlerOnline = false;
  private boolean       _commandActive = false;
  private boolean 			_heartbeatActive = false;
  
  private AsyncClientChannel _channel;
  private CrawlerService.AsyncStub _crawlerService;

  public OnlineCrawlerState(CrawlDBServer server,String hostName,InetSocketAddress ipAndPort) throws IOException,RPCException  { 

    _server = server;
    _hostName = hostName;
    _crawlerIpAddressAndPort = ipAndPort;
    
    CrawlDBServer.LOG.info("OnlineCrawlerState - Opening Channel to Host:" + _hostName);
    // initialize channel ... 
    _channel = new AsyncClientChannel(_server.getEventLoop(),_server.getServerAddress(),_crawlerIpAddressAndPort,this);
    _channel.open();
    _crawlerService = new CrawlerService.AsyncStub(_channel);
  }

  
  boolean isOnline() { 
    return _crawlerOnline;
  }
  
  @Override
  public String toString() {
    return "CrawlerState for ("+_hostName+") IPAddress:" + _crawlerIpAddressAndPort.getAddress().getHostAddress() + " Port:" + _crawlerIpAddressAndPort.getPort();      
  }

  public String       getHostname() { return _hostName; } 

  public Date         getLastUpdateTime() { return _lastUpdateTime; }
  public void         setLastUpdateTime(Date time) { _lastUpdateTime = time; }
  public CrawlerStatus getLastKnownStatus() { return _lastKnownStatus; }
  
  
  public void transitionToState(int crawlerState,int activeCrawlNumber) {
    //LOG.info("Transitioning Crawler:" + getHostname() + " to State:" + CrawlerStatus.CrawlerState.toString(crawlerState) + " crawlNumber:" + activeCrawlNumber);
    _desiredState = crawlerState;
    _activeCrawlNumber = activeCrawlNumber;
  }
  
  private void sendCrawlerCommand(int desiredAction,int activeListNumber) {
    LOG.info("Sending Command:" + desiredAction + " ListId:" + activeListNumber + " to Crawler:" + getHostname());
    _commandActive = true;
    CrawlerAction action = new CrawlerAction();
    action.setActionType(desiredAction);
    action.setActiveListNumber(activeListNumber);
    
    try {
      _crawlerService.doAction(action,new Callback<CrawlerAction, CrawlerStatus>() {

        @Override
        public void requestComplete(final AsyncRequest<CrawlerAction, CrawlerStatus> request) {
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
  
  private void processStatusResponse(CrawlerStatus newStatus) { 
    // update update time ... 
    setLastUpdateTime(new Date());
    
    // clone the status 
    try {
      _lastKnownStatus = (CrawlerStatus) newStatus.clone();
    } catch (CloneNotSupportedException e) {
    }
    
    // now validte against desired state ... 
    switch (_desiredState) { 
      case CrawlerStatus.CrawlerState.IDLE: {
        if (_lastKnownStatus.getCrawlerState() == CrawlerStatus.CrawlerState.ACTIVE) {
          sendCrawlerCommand(CrawlerAction.ActionType.FLUSH,_activeCrawlNumber);
        }
        else if (_lastKnownStatus.getCrawlerState() == CrawlerStatus.CrawlerState.FLUSHED) { 
          sendCrawlerCommand(CrawlerAction.ActionType.PURGE,_activeCrawlNumber);
        }
      }
      break;
      
      case CrawlerStatus.CrawlerState.ACTIVE: { 
        if (_lastKnownStatus.getCrawlerState() == CrawlerStatus.CrawlerState.ACTIVE) { 
          if (_lastKnownStatus.getActiveListNumber() != _activeCrawlNumber) {
            sendCrawlerCommand(CrawlerAction.ActionType.FLUSH,_activeCrawlNumber);
          }
          else { 
            //LOG.info("Crawler:" + getHostname() + " is active processing crawl no:" + _activeCrawlNumber);
          }
        }
        else if (_lastKnownStatus.getCrawlerState() == CrawlerStatus.CrawlerState.FLUSHED) { 
          if (_lastKnownStatus.getActiveListNumber() != _activeCrawlNumber) {
            LOG.info("Crawler:" + getHostname() + " desired state active, current state flushed but active crawl != current crawl. sending PURGE");
            sendCrawlerCommand(CrawlerAction.ActionType.PURGE,_activeCrawlNumber);
          }
          else { 
            LOG.info("Crawler:" + getHostname() + " desired state active, current state flushed. sending RESUME");
            sendCrawlerCommand(CrawlerAction.ActionType.RESUME_CRAWL,_activeCrawlNumber);
          }
        }
        else if (_lastKnownStatus.getCrawlerState() == CrawlerStatus.CrawlerState.PURGED || 
            _lastKnownStatus.getCrawlerState() == CrawlerStatus.CrawlerState.IDLE) { 
          LOG.info("Crawler:" + getHostname() + " desired state active, current state purged or idle. sending RESUME");
          sendCrawlerCommand(CrawlerAction.ActionType.RESUME_CRAWL,_activeCrawlNumber);
        }
      }
      break;
      
      case CrawlerStatus.CrawlerState.FLUSHED: { 
        if (_lastKnownStatus.getCrawlerState() == CrawlerStatus.CrawlerState.ACTIVE || 
            _lastKnownStatus.getCrawlerState() == CrawlerStatus.CrawlerState.IDLE ||
            _lastKnownStatus.getCrawlerState() == CrawlerStatus.CrawlerState.PURGED ) { 
          LOG.info("Crawler:" + getHostname() + " desired state flushed, current state active,purged or idle. sending FLUSH");
          sendCrawlerCommand(CrawlerAction.ActionType.FLUSH,_activeCrawlNumber);
        }
      }
      break;

      case CrawlerStatus.CrawlerState.PURGED: { 
        if (_lastKnownStatus.getCrawlerState() == CrawlerStatus.CrawlerState.ACTIVE || _lastKnownStatus.getCrawlerState() == CrawlerStatus.CrawlerState.IDLE) {
          LOG.info("Crawler:" + getHostname() + " desired state purged, current state active, sending FLUSH");
          sendCrawlerCommand(CrawlerAction.ActionType.FLUSH,_activeCrawlNumber);
        }
        else if (_lastKnownStatus.getCrawlerState() == CrawlerStatus.CrawlerState.FLUSHED) {
          LOG.info("Crawler:" + getHostname() + " desired state purged, current state flushed, sending PURGE");
          sendCrawlerCommand(CrawlerAction.ActionType.PURGE,_activeCrawlNumber); 
        }
      }
      break;
    }
  }
  
  public void sendHeartbeat() { 
    if (!_heartbeatActive && !_commandActive) { 

    	_heartbeatActive = true;
    	
      try { 
        _crawlerService.queryStatus(new Callback<NullMessage,CrawlerStatus>() {
  
          public void requestComplete(AsyncRequest<NullMessage, CrawlerStatus> request) {
            
            boolean forceDisconnect  = false;
            if (request.getStatus() == Status.Success) { 
              processStatusResponse(request.getOutput());          
            }
            else { 
              CrawlDBServer.LOG.error("Heartbeat request to crawler: " + getHostname() + " failed with Status: " + request.getStatus().toString());
              forceDisconnect = true;
            }
            
            if (forceDisconnect) { 
              try {
                _channel.close();
              } catch (IOException e) {
                e.printStackTrace();
              }
            }
          	_heartbeatActive = false;
          } 
          
        });
      }
      catch (RPCException e ){ 
      	_heartbeatActive = false;
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
    CrawlDBServer.LOG.info("OnlineCrawlerState - Connected to Host:" + _hostName);
    crawlerOnline();
  }

  /** crawler online callback - triggered when a crawler comes online  **/
  private void crawlerOnline() {
    _crawlerOnline = true;
  }
  
  /** crawler offline callback - triggered when crawler has gone offline (socket disconnect etc.) **/
  private void crawlerOffline() { 
    _commandActive = false;
    _crawlerOnline = false;
    _heartbeatActive = false;
  }
  
  
  
  public boolean OutgoingChannelDisconnected(AsyncClientChannel channel) {
    // CrawlDBServer.LOG.info("Disconnect detected on OUTGOING Connection to Crawler: "+ _hostName);
    crawlerOffline();
    return true;
  }
}