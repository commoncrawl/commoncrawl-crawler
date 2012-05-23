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
package org.commoncrawl.service.directory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.rpc.base.internal.AsyncClientChannel;
import org.commoncrawl.rpc.base.internal.AsyncRequest;
import org.commoncrawl.rpc.base.internal.NullMessage;
import org.commoncrawl.rpc.base.shared.RPCException;
import org.commoncrawl.util.CCStringUtils;

public class DirectoryServiceListener implements AsyncClientChannel.ConnectionCallback {

  private AsyncClientChannel _sourceChannel;
  private DirectoryServiceServer _server;
  private InetSocketAddress _address;
  private AsyncClientChannel _channel;
  private DirectoryServiceCallback.AsyncStub _callbackStub;
  private long _connectionCookie;
  private Map<String,Pattern> _subscriptions = new TreeMap<String,Pattern> ();
  
  private static final Log LOG = LogFactory.getLog(DirectoryServiceListener.class);

  
  public DirectoryServiceListener(DirectoryServiceServer server,AsyncClientChannel sourceChannel, InetSocketAddress address,long connectionCookie) {
    _sourceChannel = sourceChannel;
    _server = server;
    _address = address;
    _connectionCookie = connectionCookie;
  }

  public String getName() { 
    return "To:" + _address.toString();
  }
  public void connect() throws IOException { 
    LOG.info("Opening Listener Channel to:" + _address.toString());
    _channel = new AsyncClientChannel(_server.getEventLoop(),_server.getServerAddress(),_address,this);
    _channel.open();
    _callbackStub = new DirectoryServiceCallback.AsyncStub(_channel);
  }
  
  public void disconnect() {
    LOG.info("Closing Listener Channel to:" + _address.toString());
    if (_channel != null) { 
      try {
        _channel.close();
      } catch (IOException e) {
      }
      _channel = null;
      if (_server != null) { 
        _server.removeListener(_sourceChannel,this);
        _server = null;
      }
    }
  }

  @Override
  public void OutgoingChannelConnected(AsyncClientChannel channel) {
    LOG.info("Connected to CallbackChannel:" + _address.toString());
    DirectoryServiceRegistrationInfo registrationInfo = new DirectoryServiceRegistrationInfo();
    
    registrationInfo.setRegistrationCookie(_connectionCookie);
    
    try {
      LOG.info("Calling Initialize on CallbackChannel:" + _address.toString());
      _callbackStub.initialize(registrationInfo,new AsyncRequest.Callback<org.commoncrawl.service.directory.DirectoryServiceRegistrationInfo,NullMessage>() {
  
        @Override
        public void requestComplete(AsyncRequest<DirectoryServiceRegistrationInfo, NullMessage> request) {
          if (request.getStatus() == AsyncRequest.Status.Success) { 
            if (_server != null) { 
              LOG.info("Initialize on CallbackChannel:" + _address.toString() + " Succeeded. Activating Listener");
              _server.activateListener(_sourceChannel,DirectoryServiceListener.this);
            }
          }
          else {
            LOG.error("Initialize on CallbackChannel:" + _address.toString() + "Failed! Closing Channel");
            disconnect();
          }
        } 
        
      });
    }
    catch (RPCException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      disconnect();
    }
  }

  @Override
  public boolean OutgoingChannelDisconnected(AsyncClientChannel channel) {
    disconnect();
    return false;
  }
  
  public AsyncClientChannel getSourceChannel() { 
    return _sourceChannel;
  }
  
  public void addSubscription(String itemPath) {
    Pattern pattern = Pattern.compile(itemPath);
    if (pattern != null) { 
      _subscriptions.put(itemPath,pattern);
    }
  }
  
  public void removeSubscription(String itemPath) {
    _subscriptions.remove(itemPath);
  }
  
  public Map<String,Pattern> getSubscriptions() { 
    return _subscriptions;
  }
  
  public void dispatchItemsChangedMessage(DirectoryServiceItemList items) { 
    if (_channel != null && _channel.isOpen()) {
      try { 
        LOG.info("Dispatching Item Changed to:" + _address.toString());
        _callbackStub.itemChanged(items,new AsyncRequest.Callback<org.commoncrawl.service.directory.DirectoryServiceItemList,NullMessage>() {
  
          @Override
          public void requestComplete(AsyncRequest<DirectoryServiceItemList, NullMessage> request) {
            
          } 
          
        });
      }
      catch (RPCException e) { 
        LOG.error(CCStringUtils.stringifyException(e));
      }
    }
  }
  
}
