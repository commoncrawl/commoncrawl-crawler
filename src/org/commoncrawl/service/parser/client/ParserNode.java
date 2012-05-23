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

package org.commoncrawl.service.parser.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.async.Timer;
import org.commoncrawl.rpc.base.internal.AsyncClientChannel;
import org.commoncrawl.rpc.base.internal.AsyncRequest;
import org.commoncrawl.rpc.base.internal.NullMessage;
import org.commoncrawl.rpc.base.internal.AsyncClientChannel.ConnectionCallback;
import org.commoncrawl.rpc.base.internal.AsyncRequest.Callback;
import org.commoncrawl.rpc.base.internal.AsyncRequest.Status;
import org.commoncrawl.rpc.base.shared.RPCException;
import org.commoncrawl.service.parser.ParseRequest;
import org.commoncrawl.service.parser.ParseResult;
import org.commoncrawl.service.parser.ParserServiceSlave;
import org.commoncrawl.service.parser.SlaveStatus;
import org.commoncrawl.util.CCStringUtils;


/**
 * A representation of an online parser node 
 * 
 * @author rana
 *
 */
public class ParserNode implements ConnectionCallback, Comparable<ParserNode> {

  
  String _nodeName;
  InetSocketAddress _nodeAddress;
  AsyncClientChannel  _outgoingChannel;
  ParserServiceSlave.AsyncStub _asyncStub;
  AtomicBoolean _online = new AtomicBoolean();
  EventLoop _eventLoop;
  Dispatcher _dispatcher;
  Timer _statusPollTimer;
  SlaveStatus _status = null;
  long _lastTouched = -1L;
  
  public static final Log LOG = LogFactory.getLog(ParserNode.class);
  private static final int TIMER_POLL_DELAY = 100;

  
  public ParserNode(Dispatcher dispatcher,EventLoop eventLoop,String nodeName,InetSocketAddress address) { 
    _eventLoop = eventLoop;
    _nodeName = nodeName;
    _nodeAddress = address;
    _dispatcher = dispatcher;
  }
    
  public String getNodeName() { 
    return _nodeName;
  }
  
  /** 
   * startup communications
   */
  public void startup() throws IOException { 
    
    _outgoingChannel 
      = new AsyncClientChannel(
          _eventLoop,
          new InetSocketAddress(0),
          _nodeAddress,this);
    LOG.info("Starting Communications with Node:" + _nodeName);
    _asyncStub = new ParserServiceSlave.AsyncStub(_outgoingChannel);
    _outgoingChannel.open();
    _statusPollTimer = new Timer(TIMER_POLL_DELAY,false,new Timer.Callback() {
      
      @Override
      public void timerFired(Timer timer) {
        if (_online.get()) {
          queryStatus();
        }
        else {
          _dispatcher.getQueueLock().lock();
          try { 
            _status = null;
          }
          finally { 
            _dispatcher.getQueueLock().unlock();
          }
          _dispatcher.nodeStatusChanged(ParserNode.this); 
        }
      }
    });
  }
  
  
  public void queryStatus(){ 
    try {
//      LOG.info("Querying Node:" + _nodeName + " for Status");
      _asyncStub.queryStatus(new Callback<NullMessage, SlaveStatus>() {

        @Override
        public void requestComplete(AsyncRequest<NullMessage, SlaveStatus> request) {
          
          _dispatcher.getQueueLock().lock();
          try { 
            if (request.getStatus()  == Status.Success) {
              _status = request.getOutput();
              LOG.info("Node:" + _nodeName +" Load:" + _status.getLoad() 
                  + " Queued:" + _status.getQueuedDocs() 
                  + " Active:" + _status.getActiveDocs());
            }
            else { 
              LOG.error("Failed to get Status from Node:" + _nodeName);
              _status = null;
            }
          }
          finally { 
            _dispatcher.getQueueLock().unlock();
          }
          _dispatcher.nodeStatusChanged(ParserNode.this);

          _eventLoop.setTimer(_statusPollTimer);
        }
      });
    } catch (RPCException e) {
      // reset the timer ... 
      _eventLoop.setTimer(_statusPollTimer);
    }
  }
  
  public ParseResult dispatchRequest(final ParseRequest request) throws IOException {
    final AtomicReference<ParseResult> result = new AtomicReference<ParseResult>();
    
    if (_online.get()) {
//      LOG.info("Dispatching Parse Request for URL:" + request.getDocURL() 
//          + " to Node:" + _nodeName);  
      final Semaphore requestSemaphore = new Semaphore(0);
      
      _eventLoop.queueAsyncCallback(new org.commoncrawl.async.Callback() {
        
        @Override
        public void execute() {
          try {
            _asyncStub.parseDocument(request, new Callback<ParseRequest, ParseResult>() {

              @Override
              public void requestComplete(
                  AsyncRequest<ParseRequest, ParseResult> request) {
                try { 
//                  LOG.info("Parse Request for URL:" + request.getInput().getDocURL() 
//                      + " recvd responseStatus:" + request.getStatus() 
//                      + " from Node:" + _nodeName); 
                  
                  if (request.getStatus() == Status.Success) { 
                    result.set(request.getOutput());
                  }
                }
                finally {
//                  LOG.info("Releasing Request Semaphore for URL:" + request.getInput().getDocURL());
                  requestSemaphore.release();
                }
              }
            });
          } catch (Exception e) {
            LOG.error(CCStringUtils.stringifyException(e));
            LOG.info("Releasing Request Semaphore for URL:" + request.getDocURL());
            requestSemaphore.release();
          }
        }
      });
      
//      LOG.info("Waiting on ParseReq Semaphore for URL:"+ request.getDocURL());
      requestSemaphore.acquireUninterruptibly();
//      LOG.info("ParseReq Semaphore signlaed for URL:"+ request.getDocURL());
    }
    return result.get();
  }
  
  public ParserServiceSlave.AsyncStub getStub() { 
    return _asyncStub;
  }
  
  
  /**
   * shutdown communications
   */
  public void shutdown() { 
    try {
      _outgoingChannel.close();
    } catch (IOException e) {
      LOG.error(CCStringUtils.stringifyException(e));
    }
    _asyncStub = null;
  }

  @Override
  public void OutgoingChannelConnected(AsyncClientChannel channel) {
    _online.set(true);
    queryStatus();
  }

  @Override
  public boolean OutgoingChannelDisconnected(AsyncClientChannel channel) {
    _eventLoop.cancelTimer(_statusPollTimer);
    _online.set(false);
    return false;
  }
  
  public boolean isOnline() { 
    return _online.get();
  }

  @Override
  public int compareTo(ParserNode o) {
    if (_status != null && o._status != null) {
      int result = Double.compare(Math.floor(_status.getLoad()), Math.floor(o._status.getLoad()));
      if (result == 0) { 
        result = (_status.getQueuedDocs() < o._status.getQueuedDocs()) ? -1 :
          (_status.getQueuedDocs() > o._status.getQueuedDocs()) ? 1 : 0;
        if (result == 0) { 
          result = (_status.getActiveDocs() < o._status.getActiveDocs()) ? -1 :
            (_status.getActiveDocs() > o._status.getActiveDocs()) ? 1 : 0;
          
          if (result == 0) { 
            result = (_lastTouched < o._lastTouched ) ? -1: 
              (_lastTouched > o._lastTouched ) ? 1 :0;            
          }
        }
        
      }
      return result;
    }
    else if (_status != null && o._status == null) { 
      return -1;
    }
    else if (_status == null && o._status != null) {
    }
    else { 
      return (_lastTouched < o._lastTouched ) ? -1: 
        (_lastTouched > o._lastTouched ) ? 1 :0;
    }
      
    return 0;
  }
  
  public void touch() {
    _dispatcher.getQueueLock().lock();
    try { 
      // mark last touch time 
      _lastTouched = System.currentTimeMillis();
      // invalidate status 
      _status = null;
    }
    finally { 
      _dispatcher.getQueueLock().unlock();
    }
  }
}
