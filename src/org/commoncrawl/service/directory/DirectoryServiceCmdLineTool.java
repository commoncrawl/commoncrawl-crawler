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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Semaphore;

import org.apache.hadoop.record.Buffer;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.rpc.base.internal.AsyncClientChannel;
import org.commoncrawl.rpc.base.internal.AsyncRequest;
import org.commoncrawl.util.CCStringUtils;
import org.mortbay.log.Log;

/**
 * 
 * @author rana
 *
 */
public class DirectoryServiceCmdLineTool implements AsyncClientChannel.ConnectionCallback {

  EventLoop _eventLoop = null;
  AsyncClientChannel _channel;
  DirectoryService.AsyncStub _serviceStub;
  Semaphore _blockingCallSemaphore = null;


  public DirectoryServiceCmdLineTool() { 
    _eventLoop = new EventLoop();
    _eventLoop.start();
  }
  
  public void doImport(String directoryPath, File importFileName)throws IOException {
    
    Log.info("Import: Dir Path:" + directoryPath + " Incoming FilePath:" + importFileName);
    
    FileInputStream inputStream = new FileInputStream(importFileName);
    byte[] data = new byte[(int)importFileName.length()];
    inputStream.read(data);
    
    DirectoryServiceItem item = new DirectoryServiceItem();
    item.setItemPath(directoryPath);
    item.setItemData(new Buffer(data));
    _blockingCallSemaphore = new Semaphore(0);
    _serviceStub.publish(item,new AsyncRequest.Callback<DirectoryServiceItem, DirectoryServiceItem>() { 
      @Override
      public void requestComplete(AsyncRequest<DirectoryServiceItem, DirectoryServiceItem> request) {
        if (request.getStatus() == AsyncRequest.Status.Success) { 
          System.out.println("Request Succeeded. Version Number is:" + request.getOutput().getVersionNumber());
        }
        else { 
          System.out.println("Request Failed"); 
        }
        if (_blockingCallSemaphore != null) { 
          _blockingCallSemaphore.release();
        }
      }      
    });
    _blockingCallSemaphore.acquireUninterruptibly();
    _blockingCallSemaphore = null;
  }

  public void doExport(String directoryPath,final File outputPath )throws IOException {
    DirectoryServiceQuery query = new DirectoryServiceQuery();
    query.setItemPath(directoryPath);
    
    _blockingCallSemaphore = new Semaphore(0);
    _serviceStub.query(query, new AsyncRequest.Callback<DirectoryServiceQuery, DirectoryServiceItemList>() { 
      @Override
      public void requestComplete(AsyncRequest<DirectoryServiceQuery, DirectoryServiceItemList> request) {
        if (request.getStatus() == AsyncRequest.Status.Success) {
          DirectoryServiceItem item = request.getOutput().getItems().get(0);
          System.out.println("Request Succeeded. Output Path:" + item.getItemPath() + " Version Number:" + item.getVersionNumber());
          
          File itemPath = new File(item.getItemPath());
          File output = outputPath;
          if (output.isDirectory()) { 
            output = new File(output,itemPath.getName());
          }
          FileOutputStream outputStream = null;
          try {
            Log.info("Writing Output to:" + output.getAbsolutePath());
            outputStream = new FileOutputStream(output);
            outputStream.write(item.getItemData().getReadOnlyBytes());
            //System.out.write(item.getItemData().getReadOnlyBytes());
          } catch (IOException e){ 
            System.out.println("Request Failed with Exception:" + CCStringUtils.stringifyException(e));
          }
          finally { 
            if (outputStream != null) {
              try {
                outputStream.close();
              } catch (IOException e) {
              }
            }
          }
        }
        else { 
          System.out.println("Request Failed"); 
        }
        if (_blockingCallSemaphore != null) { 
          _blockingCallSemaphore.release();
        }
      }      
    });
    _blockingCallSemaphore.acquireUninterruptibly();
    _blockingCallSemaphore = null;
  }

  public void shutdown() { 
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

  public static void main(String[] args) {
    
    System.out.println("Running ....");
    if (args.length < 3) { 
      System.out.println("usage: cmdtool [import/export] serviceIP directoryServicePath filePath");
      return;
    }
    if (args.length >= 3) { 
      
      String command = args[0];
      String serverIP = args[1];
      String directoryServicePath = args[2];
      
      InetAddress address = null;
      try {
        address = InetAddress.getByName(serverIP);
      } catch (UnknownHostException e) {
        System.out.println("Uknown Host:" + serverIP);
        return;
      }
      
      if (directoryServicePath.length() == 0 || !directoryServicePath.startsWith("/") || directoryServicePath.endsWith("/")) { 
        System.out.println(directoryServicePath + " is not a valid directory path.");
        return;
      }
      
      
      if (command.equals("import")) { 
        String fileInputOutputPath = args[3];
        File targetFile = new File(fileInputOutputPath);


        if (!targetFile.exists()) { 
          System.out.println("Target File for Import:" + targetFile + " does not exist.");
          return;
        }
        DirectoryServiceCmdLineTool cmdLineTool = new DirectoryServiceCmdLineTool();
        
        try {
          cmdLineTool.connect(address);
          cmdLineTool.doImport(directoryServicePath,targetFile);
        } catch (IOException e) {
          e.printStackTrace();
        }
        cmdLineTool.shutdown();
      }
      else if (command.equals("export")) { 
        File outputPath = new File(args[3]);
        DirectoryServiceCmdLineTool cmdLineTool = new DirectoryServiceCmdLineTool();
        
        try {
          cmdLineTool.connect(address);
          cmdLineTool.doExport(directoryServicePath,outputPath);
        } catch (IOException e) {
          e.printStackTrace();
        }
        cmdLineTool.shutdown();
      }
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
