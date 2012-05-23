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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.record.Buffer;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.rpc.base.internal.AsyncClientChannel;
import org.commoncrawl.rpc.base.internal.AsyncRequest;

/**
 * 
 * @author rana
 *
 */
public class BlockingClient implements AsyncClientChannel.ConnectionCallback {
  
  EventLoop _eventLoop = null;
  AsyncClientChannel _channel;
  DirectoryService.AsyncStub _serviceStub;
  Semaphore _blockingCallSemaphore = null;
  Buffer _buffer = new Buffer();
  
  private static final Log LOG = LogFactory.getLog(BlockingClient.class);
  
  
  BlockingClient() { 
    _eventLoop = new EventLoop();
    _eventLoop.start();
  }
  
  public static BufferedReader createReaderFromPath(InetAddress directoryServiceServer,String itemPath) throws IOException {
    // load primary path via directory service client
    return new BufferedReader(new InputStreamReader(new ByteArrayInputStream(BlockingClient.loadDataFromPath(directoryServiceServer,itemPath)),Charset.forName("UTF8")));
  }
  

  public static byte[] loadDataFromPath(InetAddress directoryServiceServer,String itemPath) throws IOException { 

    final BlockingClient client = new BlockingClient();
    
    try { 
      
      client.connect(directoryServiceServer);

      DirectoryServiceQuery query = new DirectoryServiceQuery();
      query.setItemPath(itemPath);
      
      client._blockingCallSemaphore = new Semaphore(0);
      client._serviceStub.query(query, new AsyncRequest.Callback<DirectoryServiceQuery, DirectoryServiceItemList>() { 
        @Override
        public void requestComplete(AsyncRequest<DirectoryServiceQuery, DirectoryServiceItemList> request) {
          
          if (request.getStatus() == AsyncRequest.Status.Success) {
            
            DirectoryServiceItem item = request.getOutput().getItems().get(0);
            client._buffer = new Buffer(item.getItemData().getReadOnlyBytes());
          }
          else {
            LOG.error("Request:" + request.getInput().getItemPath() + " returned NULL result.");
          }
          if (client._blockingCallSemaphore != null) { 
            client._blockingCallSemaphore.release();
          }
        }      
      });
      client._blockingCallSemaphore.acquireUninterruptibly();
      client._blockingCallSemaphore = null;
      
      if (client._buffer.getCount() == 0) { 
        throw new IOException("Failed to retrieve item at path:" + itemPath);
      }
      else { 
        return client._buffer.get();
      }

    }
    catch (IOException e) { 
      throw e;
    }
  }

  void shutdown() { 
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
  
  void connect(InetAddress address) throws IOException { 
    try {
      System.out.println("Connecting to server at:" + address);
      _channel = new AsyncClientChannel(_eventLoop,null,new InetSocketAddress(address,CrawlEnvironment.DIRECTORY_SERVICE_RPC_PORT),this);
      _blockingCallSemaphore = new Semaphore(0);
      _channel.open();
      _serviceStub = new DirectoryService.AsyncStub(_channel);
      System.out.println("Waiting on Connect... ");
      _blockingCallSemaphore.acquireUninterruptibly();
      System.out.println("Connect Semaphore Released... ");
      _blockingCallSemaphore = null;
      
      if (!_channel.isOpen()) { 
        throw new IOException("Connection Failed!");
      }
    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  @Override
  public void OutgoingChannelConnected(AsyncClientChannel channel) {
    System.out.println("OutgoingChannelConnected... ");
    if (_blockingCallSemaphore != null) { 
      _blockingCallSemaphore.release();
    }
  }



  @Override
  public boolean OutgoingChannelDisconnected(AsyncClientChannel channel) {
    System.out.println("OutgoingChannelDisconnected... ");
    if (_blockingCallSemaphore != null) { 
      _blockingCallSemaphore.release();
    }
    return true;
  }
  
}
