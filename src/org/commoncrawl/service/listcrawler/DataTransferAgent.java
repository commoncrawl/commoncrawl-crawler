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

package org.commoncrawl.service.listcrawler;


import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.io.NIOBufferList;
import org.commoncrawl.io.NIOHttpConnection;
import org.commoncrawl.io.NIOHttpConnection.DataSource;
import org.commoncrawl.io.NIOHttpConnection.Listener;
import org.commoncrawl.io.NIOHttpConnection.State;
import org.commoncrawl.util.CCStringUtils;

import com.google.common.collect.ImmutableSet;

/** 
 * A daemon process that monitors the crawler logs and transfers them to a remote location
 * @author rana
 *
 */
public class DataTransferAgent {

  public static final String CACHE_DATA_PREFIX = "cacheData-";
  
  static final Log LOG = LogFactory.getLog(DataTransferAgent.class);
  static Pattern regEx = Pattern.compile("^cacheData-(.{4}).*$");
  static Pattern oobRegEx = Pattern.compile("^(.{4}).*$");
  static Pattern partFileRegEx = Pattern.compile("^part-(.{5}).*$");
  
  static File hdfsCacheFileToLogFileLocation(File baseDir,FileStatus hdfsFile)throws IOException { 
    Matcher m = regEx.matcher(hdfsFile.getPath().getName());
    if (m.matches()) { 
      String prefixId = m.group(1);
      File prefixDir = new File(baseDir,prefixId);
      prefixDir.mkdirs();
      File logFileLocation = new File(prefixDir,hdfsFile.getPath().getName());
      return logFileLocation;
    }
    else { 
      LOG.error("Failed to Match FileName"+hdfsFile.getPath());
      return null;
    }
  }
  
  static File outOfOrderFileToLogFileLocation(File baseDir,Path hdfsFile)throws IOException { 
    Matcher m = oobRegEx.matcher(hdfsFile.getParent().getName());
    if (m.matches()) { 
      String prefixId = m.group(1);
      File prefixDir = new File(baseDir,prefixId);
      prefixDir.mkdirs();
      Matcher m2 = partFileRegEx.matcher(hdfsFile.getName());
      if (m2.matches()) { 
        String prefixAndPart = hdfsFile.getParent().getName() + 
            "-" + m2.group(1);
        File logFileLocation = new File(prefixDir,prefixAndPart);
        return logFileLocation;
      }
    }
    LOG.error("Failed to Match FileName"+hdfsFile);
    return null;
  }
  
  public static class ProxyTransferItem {
    
    ProxyTransferItem() { 
      
    }
    
    ProxyTransferItem(Path hdfsPath,File logFilePath,String uploadName) { 
      this.hdfsFilePath = hdfsPath;
      this.logFilePath = logFilePath;
      this.uploadName = uploadName;
    }
    Path hdfsFilePath;
    File logFilePath;
    String uploadName;
  }
  
  private static void probeAndSetSize(boolean sendSize,int targetSize,int minSize,SocketChannel channel)throws IOException { 
    
    if (sendSize && channel.socket().getSendBufferSize() >= targetSize) {  
      //System.out.println("SendSize is Already:" + channel.socket().getSendBufferSize());
      return;
    }
    else if (!sendSize && channel.socket().getReceiveBufferSize() >= targetSize) { 
      //System.out.println("RcvSize is Already:" + channel.socket().getReceiveBufferSize());
      return;
    }
    
    do { 
      
      int sizeOut = 0;
      if (sendSize) { 
        channel.socket().setSendBufferSize(targetSize);
        sizeOut = channel.socket().getSendBufferSize();
      }
      else { 
        channel.socket().setReceiveBufferSize(targetSize);
        sizeOut = channel.socket().getReceiveBufferSize();
      }
      if (sizeOut == targetSize)
        break;
      targetSize >>= 1;
    }
    while (targetSize > minSize);
    
  }
  
  static class BufferStruct {
    ByteBuffer _buffer;
    int _id;

    BufferStruct(ByteBuffer b,int id) { 
      _buffer = b;
      _id = id;
    }
  }

  static class CCBridgeServerMapping {
    
    public CCBridgeServerMapping(String internalIP,String externalIP) { 
      _internalName = internalIP;
      _externalName = externalIP;
    }
    
    String _internalName;
    String _externalName;
  }
  
  static int uploadSingeFile(CCBridgeServerMapping mapping,FileSystem fs,Configuration conf,Path hdfsFilePath,String uploadName,EventLoop eventLoop)throws IOException {
    
    final FileStatus fileStatus = fs.getFileStatus(hdfsFilePath);
    LOG.info("Uploading:" + uploadName +" size:" + fileStatus.getLen() + " to:" + mapping._internalName);

    {
      // construct url 
      URL fePostURL = new URL("http://"+ mapping._externalName+":8090/");
      LOG.info("POST URL IS:" + fePostURL.toString());

      // open input stream 
      final FSDataInputStream is = fs.open(hdfsFilePath);
      final Semaphore blockingSemaphore = new Semaphore(0);
      NIOHttpConnection connection = null;
      try { 
        // create connection 
        connection = new NIOHttpConnection(fePostURL,eventLoop.getSelector(),eventLoop.getResolver(),null);
        // set listener 
        connection.setListener(new Listener() {

          @Override
          public void HttpConnectionStateChanged(NIOHttpConnection theConnection, State oldState, State state) {
            LOG.info("Connection State Changed to:" + state.toString());
            if (state == State.DONE || state == State.ERROR) {
              //LOG.info("Connection Transition to Done or Error");
              //LOG.info("Response Headers:" + theConnection.getResponseHeaders().toString());
              blockingSemaphore.release();
            }
          }

          @Override
          public void HttpContentAvailable(NIOHttpConnection theConnection,
              NIOBufferList contentBuffer) {
            // TODO Auto-generated method stub
            
          } 
        }
        );
        // set headers 
        connection.getRequestHeaders().reset();
        connection.getRequestHeaders().prepend("PUT /put?src="+uploadName+" HTTP/1.1",null);
        connection.getRequestHeaders().set("Host",mapping._internalName+":8090");
        connection.getRequestHeaders().set("Content-Length",Long.toString(fileStatus.getLen()));
        connection.getRequestHeaders().set("Connection", "keep-alive");
        connection.setPopulateDefaultHeaderItems(false);
        
        final LinkedBlockingDeque<BufferStruct> _loaderQueue = new LinkedBlockingDeque<BufferStruct>(20);
        final AtomicBoolean eof = new AtomicBoolean();
        final ByteBuffer sentinel = ByteBuffer.allocate(4096);
        sentinel.position(sentinel.position());
        final Thread loaderThread = new Thread(new Runnable() {

          int _id=0;
          @Override
          public void run() {
            int bytesRead;
            byte incomingBuffer[] = new byte[4096 * 10];
            try { 
              while ((bytesRead = is.read(incomingBuffer)) != -1) {
                ByteBuffer buffer = ByteBuffer.wrap(incomingBuffer, 0,bytesRead);
                buffer.position(bytesRead);
                
                //LOG.info("Loader Thread Read:"+ bytesRead + " Buffer:" + ++_id);
                try {
                  _loaderQueue.put(new BufferStruct(buffer,_id));
                } catch (InterruptedException e) {
                  LOG.error(CCStringUtils.stringifyException(e));
                  break;
                }
                incomingBuffer = new byte[4096 * 10];
              }
              try {
                _loaderQueue.put(new BufferStruct(sentinel,++_id));
              } catch (InterruptedException e) {
              }
            }
            catch (IOException e){
              LOG.error(CCStringUtils.stringifyException(e));
              return;
            }
          } 
          
        });
        
        loaderThread.start();
        
        // set data source ... 
        connection.setDataSource(new DataSource() {
  
          int bytesTransferred = 0;
          
          @Override
          public boolean read(NIOBufferList dataBuffer) throws IOException {
            if (eof.get()) 
              return true;
            //LOG.info("Connect read callback triggered");
            BufferStruct buffer = _loaderQueue.poll();
            if (buffer != null) { 
              if (buffer._buffer != sentinel) {
                //LOG.info("Got Buffer:"+ buffer._id);
                if (buffer._id == 1) { 
                  //LOG.info("Inital Buffer Bytes:" + new String(buffer._buffer.array(),0,10).toString());
                }
                dataBuffer.write(buffer._buffer);
                bytesTransferred += buffer._buffer.limit();
                //LOG.info("Read:" + buffer._buffer.limit() + " Transfered:" + bytesTransferred);
                return false;
              }
              else { 
                //LOG.info("EOF Condition");
                dataBuffer.write(sentinel);
                eof.set(true);
                return true;
              }
            }
            return false;
          } 
        });
        
        // open connection 
        connection.open();
        // wait for connection to complete ... 
        blockingSemaphore.acquireUninterruptibly();
        // kill loader thread 
        loaderThread.interrupt();
        try {
          LOG.info("Waiting for Loader Thread");
          loaderThread.join();
          LOG.info("Done Waiting for Loader Thread");
        } catch (InterruptedException e) {
        }
      }
      finally { 
        is.close();
        if (connection != null) {  
          connection.close();
          LOG.info("Response Code for File:" + uploadName + "to Host: " + mapping._internalName + " is:"  + connection.getResponseHeaders().getHttpResponseCode());
          return connection.getResponseHeaders().getHttpResponseCode();
          /*
          if (connection.getResponseHeaders().getHttpResponseCode() != 200) { 
            throw new IOException("Failed to upload file:" + dataFile.getName() + " responseCode:" + connection.getResponseHeaders().getHttpResponseCode());
          }
          */
        }
      }
    }
    // something went wrong ??? 
    LOG.error("Failed to upload file:" + uploadName + " unknown response code");
    return 500;
  }  
  
  static Thread startTransferThread(final int threadIndex,final CCBridgeServerMapping mapping,final File shutdownFile,final FileSystem fs,final Configuration conf,final LinkedBlockingDeque<ProxyTransferItem> itemQueue,final EventLoop eventLoop,final Semaphore shutdownSemaphore) { 
    Thread thread = new Thread(new Runnable() {

      @Override
      public void run() {
        try {
          while (true) {
            if (shutdownFile.exists()) { 
              LOG.info("Exiting due to shutdown file existense!");
              break;
            }
            ProxyTransferItem item = itemQueue.take();
            
            if (item.hdfsFilePath == null) { 
              LOG.info("Transfer Thread:" + Thread.currentThread().getId() + " Exiting");
            }
            else { 
              try {
                LOG.info("Transfer Thread:"+ threadIndex+" for Host:" + mapping._internalName + " Transferring File:" + item.hdfsFilePath);
                int result = uploadSingeFile(mapping,fs,conf,item.hdfsFilePath,item.uploadName,eventLoop);
                if (result == 200){ 
                  LOG.info("Transfer Thread:" + threadIndex + "for Host:" + mapping._internalName + " Done Transferring File:" + item.hdfsFilePath);
                  //item.logFilePath.createNewFile();
                }
                else if (result == 409) { 
                  LOG.info("Transfer Thread:" + threadIndex + "for Host:" + mapping._internalName + " File Already Exists for Path:" + item.hdfsFilePath);
                  //item.logFilePath.createNewFile();
                }
                else { 
                  LOG.error("Transfer Thread:" + threadIndex + "for Host:" + mapping._internalName + " File Transfer Failed with Error:" +result + " for Path:" + item.hdfsFilePath);
                  itemQueue.putFirst(item);
                }
              } catch (IOException e) {
                LOG.error("Transfer Failed for Thread:" + threadIndex+ "Host:" + mapping._internalName + " File: " + item.hdfsFilePath);
                LOG.fatal(CCStringUtils.stringifyException(e));
              }
            }
          }
        } catch (InterruptedException e) {
        }
        finally { 
          shutdownSemaphore.release();
        }
      } 
      
    });
    thread.start();
    return thread;
  }
  
  public static final int TRANSFER_THREADS_PER_HOST = 8;
  
  static ImmutableSet<CCBridgeServerMapping> mappingsTable = new ImmutableSet.Builder<CCBridgeServerMapping>()
      .add(new CCBridgeServerMapping("0.0.0.0","0.0.0.0"))
      .build();
  
  public static void main(String[] args) {

    Logger  logger = Logger.getLogger("org.commoncrawl");
    logger.setLevel(Level.INFO);
    BasicConfigurator.configure();
    
    Configuration conf = new Configuration();
    
    
    conf.addResource("core-site.xml");
    conf.addResource("hdfs-site.xml");
    
    // set a big io buffer size ... 
    conf.setInt("io.file.buffer.size", 4096 * 1024);
    
    final File transferLogDir     = new File("/home/rana/ccprod/data/proxy_xfr_log");
    final Path hdfsCacheDataPath  = new Path("crawl/proxy/cache/");
    final File shutdownFile     = new File("/home/rana/ccprod/data/shutdown_xfr");
    
    // create a deque .. 
    final LinkedBlockingDeque<ProxyTransferItem> itemQueue = new LinkedBlockingDeque<ProxyTransferItem>();
    
    final EventLoop eventLoop = new EventLoop();
    eventLoop.start();
    
    try {

      final DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(conf);
      Thread transferThreads[]  = new Thread[TRANSFER_THREADS_PER_HOST * mappingsTable.size()];
      Semaphore shutdownSemaphore = new Semaphore(0);
      int threadIndex = 0;
      for (int i=0;i<TRANSFER_THREADS_PER_HOST;++i){
        int serverIdx=0;
        for (CCBridgeServerMapping mapping : mappingsTable) { 
          transferThreads[(i * mappingsTable.size()) + serverIdx++] = startTransferThread(threadIndex++,mapping,shutdownFile,fs,conf,itemQueue,eventLoop,shutdownSemaphore);
        }
      }
      
      Thread scannerThread = new Thread(new Runnable()  { 
        
        long _lastScanId = -1;
        long _lastOutOfOrderDataDirId = -1L;
        
        static final int SCAN_INTERVAL_MS = 500;
        @Override
        public void run() {
          
            while (true) { 
  
              try {
              if (shutdownFile.exists()) { 
                LOG.info("Shutdown File Detected in ScanTimer Outer Loop. Exiting Scan Thread");
                return;
              }
              
              LOG.info("Scanning For Files based on filter. Last Known Scan Id is:" + _lastScanId);
              FileStatus fileList[] = fs.listStatus(hdfsCacheDataPath, new PathFilter() {
                
  
                @Override
                public boolean accept(Path path) {
                  try {
                    if (path.getName().startsWith("cacheData-")) { 
                      // extract file id ... 
                      long currentFileId = Long.parseLong(path.getName().substring("cacheData-".length()));
                      // figure out if we are going to process it ... 
                      if (_lastScanId == -1 || currentFileId > _lastScanId) {
                        return true;
                      }
                    }
                  }
                  catch (Exception e) { 
                    LOG.error("Caught Exception Processing Path Filter:" + CCStringUtils.stringifyException(e));
                  }
                  return false;
                }
              });
              LOG.info("Scan returned:" + fileList.length + " Number of Valid Files");
  
              long latestFileId = 0L;
              for (FileStatus file : fileList) {
                // extract file id ... 
                long currentFileId = Long.parseLong(file.getPath().getName().substring("cacheData-".length()));
                // figure out if we are going to process it ... 
                if (_lastScanId == -1 || currentFileId > _lastScanId) {
                  // cache max latest id ..
                  latestFileId = Math.max(latestFileId, currentFileId);
                  File logFile = hdfsCacheFileToLogFileLocation(transferLogDir,file);
                  if (logFile != null) { 
                    if (logFile.exists()) { 
                      LOG.info("Skipping:" + file.getPath().getName());
                    }
                    else { 
                      LOG.info("Queueing File:" + file.getPath().getName());
                      itemQueue.add(new ProxyTransferItem(file.getPath(),logFile,file.getPath().getName()));
                    }
                  }
                }
              }
              // ok update lastest file id 
              _lastScanId = Math.max(_lastScanId,latestFileId);
              
              FileStatus outofOrderDataDirs[] = fs.globStatus(new Path("crawl/proxy/dtAgentOutOfOrderTransfers/*"));
              
              for (FileStatus outOfOrderDataDir : outofOrderDataDirs) { 
                long dataDirId = Long.parseLong(outOfOrderDataDir.getPath().getName());
                if (dataDirId > _lastOutOfOrderDataDirId) { 
                  FileStatus candidates[] = fs.globStatus(new Path(outOfOrderDataDir.getPath(),"part-*"));
                  
                  for (FileStatus candidate : candidates) { 
                    File logFile = outOfOrderFileToLogFileLocation(transferLogDir,candidate.getPath());
                    if (logFile != null) { 
                      String candidateName = 
                          candidate.getPath().getParent().getName() 
                          + "-" 
                          + candidate.getPath().getName();
 
                      if (logFile.exists()) { 
                        LOG.info("Skipping OOB FILE:" + candidateName);
                            
                      }
                      else { 
                        LOG.info("Queueing OOB FILE:" + candidateName);
                        itemQueue.add(new ProxyTransferItem(candidate.getPath(),logFile,candidateName));
                      }
                    }                    
                  }
                  _lastOutOfOrderDataDirId = dataDirId;
                }
              }
              
              LOG.info("Finish Scan. Last Known Scan Id is now:" + _lastScanId);
              
              
            }
            catch (Exception e) { 
              LOG.error(CCStringUtils.stringifyException(e));
            }
            
            try {
              Thread.sleep(SCAN_INTERVAL_MS);
            } catch (InterruptedException e) {
            }
          }
        }
      });
      
      // start scanner thread ... 
      scannerThread.start();
      
      
      LOG.info("Waiting on Transfer Threads");
      shutdownSemaphore.acquireUninterruptibly(TRANSFER_THREADS_PER_HOST * mappingsTable.size());
      LOG.info("ALL Transfer Threads Dead.");
      // wait for scanner thread to die 
      LOG.info("Waiting for Scanner Thread to Die.");
      try {
        scannerThread.join();
      } catch (InterruptedException e) {
      }
      LOG.info("Killing Event Loop");
      eventLoop.stop();
      
    } catch (IOException e) {
      LOG.error(CCStringUtils.stringifyException(e));
    }
    
  }

}
