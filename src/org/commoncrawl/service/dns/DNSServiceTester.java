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

package org.commoncrawl.service.dns;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.async.Timer;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.io.NIODNSCache;
import org.commoncrawl.io.NIODNSLocalResolver;
import org.commoncrawl.rpc.base.internal.AsyncClientChannel;
import org.commoncrawl.rpc.base.internal.AsyncRequest;
import org.commoncrawl.rpc.base.internal.Server;
import org.commoncrawl.rpc.base.internal.AsyncRequest.Callback;
import org.commoncrawl.rpc.base.shared.RPCException;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.IPAddressUtils;
import org.commoncrawl.util.JVMStats;

/**
 * 
 * @author rana
 *
 */
public class DNSServiceTester extends Server implements AsyncClientChannel.ConnectionCallback {

  private static final Log LOG = LogFactory.getLog(DNSServiceTester.class);  
  EventLoop _eventLoop = null;
  InetSocketAddress _address = null;
  AsyncClientChannel _channel;
  DNSService.AsyncStub _serviceStub;
  long _connectionCookie = 0;
  Semaphore _blockingSemaphore = new Semaphore(0);
  long _testItemCount = 0;
  // long _itemsComplete = 0;
  AtomicLong _itemsComplete = new AtomicLong();
  File _testFile = null;
  String _testName = "";

  
  public static void main(String[] args) {
    
    InetSocketAddress socketAddress = new InetSocketAddress(args[0],CrawlEnvironment.DNS_SERVICE_RPC_PORT);
    // File testFileLocation = new File(args[1]);
    
    if (socketAddress != null && args[1].length() != 0) { 
      DNSServiceTester tester = new DNSServiceTester(socketAddress,args[1]);
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
  
  public DNSServiceTester(InetSocketAddress address,String queryName) { 
  
     _eventLoop = new EventLoop();
     _address = address;
     _testFile = null;
     _testName = queryName;
    
     _eventLoop.start();
        
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
      _channel = new AsyncClientChannel(_eventLoop,new InetSocketAddress(0),_address,this);
      _channel.open();
      _serviceStub = new DNSService.AsyncStub(_channel);
    }
    catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  void doSimpleQuery(String dnsName) { 
    LOG.info("Doing Simple Query for Name:" + dnsName);
    DNSQueryInfo queryInfo = new DNSQueryInfo();
    queryInfo.setHostName(dnsName);
    
    //TODO: PASS TIMEOUT VALUE TO THE DNS SERVER SERVICE 
    
    try { 
      _serviceStub.doQuery(queryInfo, new Callback<DNSQueryInfo,DNSQueryResponse>() {
  
        @Override
        public void requestComplete(AsyncRequest<DNSQueryInfo, DNSQueryResponse> request) {
          
          if (request.getStatus() == AsyncRequest.Status.Success) { 
            if (request.getOutput().getStatus() == DNSQueryResponse.Status.SUCCESS) { 
              String strIPAddress = null;
              String strCName = "";
              String strTTL = "";
              
              strIPAddress = IPAddressUtils.IntegerToIPAddressString(request.getOutput().getAddress());
              if (request.getOutput().isFieldDirty(DNSQueryResponse.Field_CNAME)) { 
                strCName = request.getOutput().getCname();
              }
              strTTL = Long.toString(request.getOutput().getTtl());
              
              LOG.info("DNS Query Succeeded. IP:" + strIPAddress + " CName:" + strCName + " TTL:" + strTTL);
            }
            else { 
              String strError = "UNKNOWN ERROR";
              switch (request.getOutput().getStatus()) { 
                case DNSQueryResponse.Status.RESOLVER_FAILURE: { 
                  strError = "RESOLVER FAILURE (";
                  strError += request.getOutput().getErrorDesc();
                  strError += ")";
                }
                default:    { 
                  strError = "SERVER FAILURE (";
                  strError += request.getOutput().getErrorDesc();
                  strError += ")";
                }
              }
              LOG.info("DNS Query Failed with Error:" + strError);
            }
          }
          else { 
            LOG.info("DNS Query Failed with RPC ERROR");
          }
          
          _eventLoop.stop();
        } 
        
      });
    }
    catch (RPCException e) { 
      LOG.error("RPC Exception:" + CCStringUtils.stringifyException(e));
      _eventLoop.stop();
    }
  }
  
  
  void startTest(File testFileLocation) { 
    LOG.info("Starting Test...");
    
    
    LOG.info("Loading DNS Test File:" + testFileLocation);
    JVMStats.dumpMemoryStats();
    
    FileReader reader = null;

    NIODNSCache cache = NIODNSLocalResolver.getDNSCache();
    
    
    try {

      reader = new FileReader(testFileLocation);

      BufferedReader lineReader = new BufferedReader(reader);
      
      String line = null;
      
      HashSet<String> hostSet = new HashSet<String>();
      
      while ((line = lineReader.readLine()) != null) { 
        
        int firstDelimiterIdx = line.indexOf(",/");
        int secondDelimiterIdx = -1;
        int thirdDelimiterIdx = -1;
        if (firstDelimiterIdx != -1) { 
          secondDelimiterIdx = line.indexOf(',',firstDelimiterIdx+2);
          thirdDelimiterIdx = line.indexOf(',',secondDelimiterIdx + 1);
        }
        
        if (firstDelimiterIdx != -1 && secondDelimiterIdx != -1) { 
          final String hostName = line.substring(0,firstDelimiterIdx);
          final String ipAddress = line.substring(firstDelimiterIdx + 2,secondDelimiterIdx);
          int ipAddressInteger = IPAddressUtils.IPV4AddressStrToInteger(ipAddress);
          
          LOG.info("Querying Service for Address for Host:" + hostName);
          DNSQueryInfo queryInfo = new DNSQueryInfo();
          
          queryInfo.setHostName(hostName);
          
          // increase item count 
          _testItemCount++;
          
          
          _serviceStub.doQuery(queryInfo, new Callback<DNSQueryInfo,DNSQueryResponse>() {

            @Override
            public void requestComplete(AsyncRequest<DNSQueryInfo, DNSQueryResponse> request) {
              
              if (request.getOutput().getStatus() == DNSQueryResponse.Status.SUCCESS) { 
                LOG.info("Request Succeeded for Host:" + hostName 
                    + " IPAddress:" + IPAddressUtils.IntegerToIPAddressString(request.getOutput().getAddress())
                    + " TTL:" + request.getOutput().getTtl() + "(" +(request.getOutput().getTtl() - System.currentTimeMillis())  +  ")Source:" + request.getOutput().getSourceServer());
              }
              else { 
                LOG.info("Request Failed for Host:" + hostName + " Reason:" + request.getOutput().getErrorDesc() + " Source:" + request.getOutput().getSourceServer());
              }

              LOG.info("Total:" + _testItemCount + " Complete:" + _itemsComplete.get());
              _itemsComplete.incrementAndGet();

              // release the semaphore 
              _blockingSemaphore.release();
            }
            
            
          });
          
        }
        
        if (_testItemCount - _itemsComplete.get() > 5000) { 
          LOG.info("Queued Count:" + (_testItemCount-_itemsComplete.get()) + ". Waiting for Queued Count to drop to 1000");
          while (_testItemCount - _itemsComplete.get() > 1000) {
            LOG.info("Waiting for Count to Go Below 1000 ("+ (_testItemCount - _itemsComplete.get()) + ")");
            try {
              _blockingSemaphore.acquire();
            } catch (InterruptedException e) {
            }
          }
        }
      }
      
      
      LOG.info("Done Processing Test File. Entry Count:" + _testItemCount);
      JVMStats.dumpMemoryStats();
      
    } catch (IOException e) {
      LOG.error(CCStringUtils.stringifyException(e));
    }
    finally { 
      try {
        reader.close();
      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
      }
    }
    
    LOG.info("Waiting on all items to complete...");

    while (_itemsComplete.get() != _testItemCount) { 
      try {
        _blockingSemaphore.acquire();
      } catch (InterruptedException e) {
      }
      if (_itemsComplete.get() % 1000 == 0) { 
        LOG.info("Completed Another 1000 Items");
      }
    }
    LOG.info("All items to completed...");
  }
  
 
  @Override
  public void OutgoingChannelConnected(AsyncClientChannel channel) {
    LOG.info("Connected to Service. Starting Test Thread");
    new Thread(new Runnable() {

      @Override
      public void run() {
        LOG.info("Test Thread Started...");
        doSimpleQuery(_testName);
      } 
      
    }).start();
  }

  @Override
  public boolean OutgoingChannelDisconnected(AsyncClientChannel channel) {
    LOG.error("Connection to DNS Service Severed. Exiting ... ");
    _eventLoop.stop();
    return false;
  }

}
