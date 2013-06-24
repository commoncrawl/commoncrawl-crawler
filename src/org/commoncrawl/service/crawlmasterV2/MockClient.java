package org.commoncrawl.service.crawlmasterV2;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.async.Timer;
import org.commoncrawl.protocol.CrawlMaster;
import org.commoncrawl.protocol.SlaveHello;
import org.commoncrawl.protocol.SlaveRegistration;
import org.commoncrawl.rpc.base.internal.AsyncClientChannel;
import org.commoncrawl.rpc.base.internal.AsyncClientChannel.ConnectionCallback;
import org.commoncrawl.rpc.base.internal.AsyncRequest;
import org.commoncrawl.rpc.base.internal.AsyncRequest.Callback;
import org.commoncrawl.rpc.base.internal.AsyncRequest.Status;
import org.commoncrawl.rpc.base.internal.NullMessage;
import org.commoncrawl.rpc.base.shared.RPCException;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.IPAddressUtils;

public class MockClient implements ConnectionCallback, Timer.Callback {
  
  Timer.Callback cb;
  
  public static final Log LOG = LogFactory.getLog(MockClient.class);

  public static void main(String[] args) throws Exception {
    InetAddress rpcAddress = InetAddress.getByName(args[0]);
    int  rpcPort = Integer.parseInt(args[1]);
    InetAddress clientIP = InetAddress.getByName(args[2]);
    String serviceName = args[3];

    MockClient client = new MockClient(rpcAddress, rpcPort,clientIP,serviceName);
    client.join();
    
  }
  

  EventLoop eventLoop;
  AsyncClientChannel _masterChannel = null;
  CrawlMaster.AsyncStub _masterRPCStub;
  Timer _timer;
  SlaveRegistration _registration;
  InetAddress clientIP;
  String serviceName;
  
  
  public MockClient(InetAddress address,int port,InetAddress clientIP,String serviceName) throws IOException {
    eventLoop = new EventLoop();
    eventLoop.start();
    this.clientIP = clientIP;
    this.serviceName = serviceName;
    
    InetSocketAddress masterAddress = new InetSocketAddress(address, port);

    _masterChannel = new AsyncClientChannel(eventLoop,new InetSocketAddress(0),masterAddress,this);
    _masterRPCStub = new CrawlMaster.AsyncStub(_masterChannel);
    _masterChannel.open();
  }

  @Override
  public void OutgoingChannelConnected(AsyncClientChannel channel) {
    try { 
      SlaveHello hello = new SlaveHello();
      hello.setIpAddress(IPAddressUtils.IPV4AddressToInteger(clientIP.getAddress()));
      hello.setServiceName(serviceName);
      hello.setCookie(System.currentTimeMillis());
      
      _masterRPCStub.registerSlave(hello, new Callback<SlaveHello, SlaveRegistration>() {
  
        @Override
        public void requestComplete(AsyncRequest<SlaveHello, SlaveRegistration> request) {
          if (request.getStatus() == Status.Success) {
            LOG.info("Slave Index:" + request.getOutput().getInstanceId());
            _registration = request.getOutput();
            _timer  = new Timer(1000,false,MockClient.this);
            eventLoop.setTimer(_timer);
          }
          else {
            LOG.error("Registration Request Failed");
            eventLoop.stop();
          }
        }
      });
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
    }
  }

  @Override
  public boolean OutgoingChannelDisconnected(AsyncClientChannel channel) {
    return false;
  }
  
  void join() { 
    try {
      eventLoop.getEventThread().join();
    } catch (InterruptedException e) {
    }
  }

  int _fireCount = 0;
  
  @Override
  public void timerFired(Timer timer) {
    LOG.info("Timer Fired");
    if (++_fireCount < 10) { 
      try {
        LOG.info("Sending Extend Message");
        _masterRPCStub.extendRegistration(_registration, new Callback<SlaveRegistration, NullMessage>() {

          @Override
          public void requestComplete(AsyncRequest<SlaveRegistration, NullMessage> request) {
            LOG.info("Extension Successfull!");
            _timer = new Timer(1000,false,MockClient.this);
            eventLoop.setTimer(_timer);
          }
        });
      } catch (RPCException e) {
        e.printStackTrace();
        eventLoop.stop();
      }
    }
    else { 
      try {
        LOG.info("Sending Expire Message");
        _masterRPCStub.expireRegistration(_registration, new Callback<SlaveRegistration, NullMessage>() {

          @Override
          public void requestComplete(AsyncRequest<SlaveRegistration, NullMessage> request) {
            
            eventLoop.stop();
          } 
          
        });
      } catch (RPCException e) {
        e.printStackTrace();
        eventLoop.stop();
      }
    }
  }
}
