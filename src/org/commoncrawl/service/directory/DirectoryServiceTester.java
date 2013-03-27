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
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.record.Buffer;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.async.Timer;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.rpc.base.internal.AsyncClientChannel;
import org.commoncrawl.rpc.base.internal.AsyncContext;
import org.commoncrawl.rpc.base.internal.AsyncRequest;
import org.commoncrawl.rpc.base.internal.AsyncServerChannel;
import org.commoncrawl.rpc.base.internal.NullMessage;
import org.commoncrawl.rpc.base.internal.Server;
import org.commoncrawl.rpc.base.shared.RPCException;
import org.commoncrawl.util.CCStringUtils;

public class DirectoryServiceTester extends Server implements DirectoryServiceCallback, AsyncServerChannel.ConnectionCallback, AsyncClientChannel.ConnectionCallback {

  private static final Log LOG = LogFactory.getLog(DirectoryServiceTester.class);  
  EventLoop _eventLoop = null;
  InetSocketAddress _address = null;
  AsyncClientChannel _channel;
  DirectoryService.AsyncStub _serviceStub;
  long _connectionCookie = 0;

  
  public static void main(String[] args) {
    InetSocketAddress socketAddress = new InetSocketAddress(args[0],Integer.parseInt(args[1]));
    
    if (socketAddress != null) { 
      DirectoryServiceTester tester = new DirectoryServiceTester(socketAddress);
      LOG.info("Waiting for Event Loop to Terminate");
      try {
        tester._eventLoop.getEventThread().join();
      } catch (InterruptedException e) {
      }
      LOG.info("Event Loop Terminated");
    }
    else { 
      LOG.error("Invalid Local Address Specified!");
    }
  }
  
  public DirectoryServiceTester(InetSocketAddress address) { 
  
     _eventLoop = new EventLoop();
     _address = address;
    
     _eventLoop.start();
        
     // create server channel ... 
     AsyncServerChannel channel = new AsyncServerChannel(this, _eventLoop,_address,this);
    
     // register RPC services it supports ... 
     registerService(channel,DirectoryServiceCallback.spec);
     
     try {
      start();
    } catch (IOException e) {
      LOG.error(CCStringUtils.stringifyException(e));
      _eventLoop.stop();
      return;
    }
     
     _eventLoop.setTimer(new Timer(0,false,new Timer.Callback() {

      @Override
      public void timerFired(Timer timer) {
        connect();
      } 
       
     }));
  }
  
  void connect() { 
    try {
      _channel = new AsyncClientChannel(_eventLoop,_address,new InetSocketAddress(_address.getAddress(),CrawlEnvironment.DIRECTORY_SERVICE_RPC_PORT),this);
      _channel.open();
      _serviceStub = new DirectoryService.AsyncStub(_channel);
    }
    catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  void queryItem(final String path)throws IOException { 
    DirectoryServiceQuery queryRequest = new DirectoryServiceQuery();
    queryRequest.setItemPath(path);
    
    
    _serviceStub.query(queryRequest, new AsyncRequest.Callback<DirectoryServiceQuery, DirectoryServiceItemList>(){

      @Override
      public void requestComplete(AsyncRequest<DirectoryServiceQuery, DirectoryServiceItemList> request) {
        dumpItemList(request.getOutput());

      } 
    });
  }
  
  void publishTestItem(final String path,String testBuffer)throws IOException { 
    byte bytes[] = testBuffer.getBytes(Charset.forName("UTF8"));
    
    DirectoryServiceItem item = new DirectoryServiceItem();
    item.setItemPath(path);
    item.setItemData(new Buffer(bytes));
    
    
    _serviceStub.publish(item,new AsyncRequest.Callback<DirectoryServiceItem, DirectoryServiceItem>() {

      @Override
      public void requestComplete(AsyncRequest<DirectoryServiceItem, DirectoryServiceItem> request) {
        LOG.info("Publish of Item:" + path + " Completed.");
      } 
    });
  }

  void startTest() { 
    LOG.info("Starting Tests");
    
    DirectoryServiceSubscriptionInfo subscription = new DirectoryServiceSubscriptionInfo();
    subscription.setSubscriptionPath("/test/zzz/.*");
    
    try { 
      LOG.info("Subscribing to .*");
      _serviceStub.subscribe(subscription,new AsyncRequest.Callback<DirectoryServiceSubscriptionInfo,DirectoryServiceItemList>() {
  
        @Override
        public void requestComplete(AsyncRequest<DirectoryServiceSubscriptionInfo, DirectoryServiceItemList> request) {
          dumpItemList(request.getOutput());
          
          try {
            
            long currentTime = System.currentTimeMillis();
            
            LOG.info("Subscription Successful. Publishing Items... Seed:" + currentTime);
            publishTestItem("/test/foo/bar","bar"+ currentTime);
            publishTestItem("/test/foo/bar2","bar2" + currentTime);
            publishTestItem("/test/foo/bar3","bar3" + currentTime);
            publishTestItem("/test/zzz/bar","zbar" + currentTime);
            publishTestItem("/test/zzz/bar2","zbar2" + currentTime);
            publishTestItem("/test/zzz/bar3","zbar3" + currentTime);
            
            LOG.info("Publish Successful. querying Items...");
            // queryItem("/test/foo/bar");
            // queryItem("/test/foo/bar2");
            // queryItem("/test/foo/bar3");
            // queryItem("/test/zzz/bar");
            // queryItem("/test/zzz/bar2");
            // queryItem("/test/zzz/bar3");
            
            LOG.info("Done...");
            
          } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        } 
        
      });
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
    }
  }
  
  void dumpItemList(DirectoryServiceItemList itemList) { 
    for (DirectoryServiceItem item: itemList.getItems()) { 
      String value = Charset.forName("UTF-8").decode(ByteBuffer.wrap(item.getItemData().getReadOnlyBytes())).toString();
      System.out.println("item:" + item.getItemPath() + " changed to version:" + item.getVersionNumber() + "Value:" + value);
    }
  }
  
  @Override
  public void itemChanged(AsyncContext<DirectoryServiceItemList, NullMessage> rpcContext) throws RPCException {
    dumpItemList(rpcContext.getInput());
    rpcContext.completeRequest();
  }

  @Override
  public void IncomingClientConnected(AsyncClientChannel channel) {
    
  }

  @Override
  public void IncomingClientDisconnected(AsyncClientChannel channel) {
    
  }

  @Override
  public void OutgoingChannelConnected(AsyncClientChannel channel) {
    LOG.info("Connected to Directory Server. Initiating Registration");
    DirectoryServiceRegistrationInfo registrationInfo = new DirectoryServiceRegistrationInfo();
    
    _connectionCookie = System.currentTimeMillis();
    
    registrationInfo.setConnectionString(_address.getAddress().getHostAddress() + ":" + _address.getPort());
    registrationInfo.setRegistrationCookie(_connectionCookie);
    
    try { 
      _serviceStub.register(registrationInfo,new AsyncRequest.Callback<DirectoryServiceRegistrationInfo,NullMessage>() {
  
        @Override
        public void requestComplete(AsyncRequest<DirectoryServiceRegistrationInfo, NullMessage> request) {
          
        } 
        
      });
    }
    catch (IOException e) {
      LOG.error(CCStringUtils.stringifyException(e));
      _eventLoop.stop();
    }
  }

  @Override
  public boolean OutgoingChannelDisconnected(AsyncClientChannel channel) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void initialize(AsyncContext<DirectoryServiceRegistrationInfo, NullMessage> rpcContext)throws RPCException {
    LOG.info("Received Initialization Request on Callback Channel");
    if (rpcContext.getInput().getRegistrationCookie() == _connectionCookie) {
      LOG.info("Callback Cookie is Valid. Starting Test");
      rpcContext.setStatus(AsyncRequest.Status.Success);
      rpcContext.completeRequest();
      startTest();
    }
    else { 
      LOG.error("Connection Cookies Don't Match!");
      _eventLoop.stop();
    }
  }
}
