/**
 * Copyright 2008 - CommonCrawl Foundation
 * 
 * CommonCrawl licenses this file to you under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.commoncrawl.crawl.crawler.listcrawler;



import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DataOutputBuffer;
import org.commoncrawl.protocol.CacheItem;
import org.commoncrawl.util.shared.ByteStream;
import org.commoncrawl.util.shared.CCStringUtils;
import org.commoncrawl.util.shared.FlexBuffer;

/** 
 * thread that writes crawled content to disk / cache 
 * @author rana
 *
 */
final class CacheWriterThread implements Runnable {
  
  private class ThriftyGZIPOutputStream extends GZIPOutputStream {
    public ThriftyGZIPOutputStream(OutputStream os) throws IOException {
      super(os);
    }

    @Override
    public void finish() throws IOException {
      super.finish();
      def.end();
    }

    @Override
    public void close() throws IOException {
      super.finish();
      def.end();
    }
  }
  
  
  public static final Log LOG = LogFactory.getLog(CacheWriterThread.class);

  
  private CRC32 _crc32Out = new CRC32();
  private CacheManager _manager;
  private File _dataDirectory;
  private LinkedBlockingQueue<CacheWriteRequest> _writeRequestQueue;
  
  public CacheWriterThread(CacheManager cacheManager) {
    _manager = cacheManager;
    _dataDirectory     = cacheManager.getLocalDataDirectory();
    _writeRequestQueue = cacheManager.getWriteRequestQueue();
  }
  
  @Override
  public void run() {
    
    boolean shutdown = false;
    
    while(!shutdown) { 
      try {
        final CacheWriteRequest request = _writeRequestQueue.take();
        
        switch (request._requestType) { 
        
          case ExitThreadRequest: { 
            // shutdown condition ... 
            CacheManager.LOG.info("Disk Writer Thread Received Shutdown. Exiting!");
            shutdown = true;
          }
          break;
          
          case WriteRequest: { 

          	long timeStart = System.currentTimeMillis();
          	
            try { 
              // reset crc calculator (single thread so no worries on synchronization)
              _crc32Out.reset();
              
              // figure out if we need to compress the item ... 
              if ((request._item.getFlags() & CacheItem.Flags.Flag_IsCompressed) == 0 && request._item.getContent().getCount() != 0) { 
                LOG.info("Incoming Cache Request Content for:" + request._item.getUrl() + " is not compressed. Compressing...");
                ByteStream compressedBytesOut = new ByteStream(request._item.getContent().getCount());
                ThriftyGZIPOutputStream  gzipOutputStream = new ThriftyGZIPOutputStream(compressedBytesOut);
                gzipOutputStream.write(request._item.getContent().getReadOnlyBytes(),0,request._item.getContent().getCount());
                gzipOutputStream.finish();
                LOG.info("Finished Compressing Incoming Content for:" + request._item.getUrl() + 
                         " BytesIn:" + request._item.getContent().getCount() + 
                         " BytesOut:" + compressedBytesOut.size());
                // replace buffer
                
                request._item.setContent(new FlexBuffer(compressedBytesOut.getBuffer(),0,compressedBytesOut.size()));
                request._item.setFlags((request._item.getFlags() | CacheItem.Flags.Flag_IsCompressed));
              }

              // create streams ...
              ByteStream bufferOutputStream = new ByteStream(8192);
              
              CheckedOutputStream checkedStream = new CheckedOutputStream(bufferOutputStream,_crc32Out);
              DataOutputStream dataOutputStream = new DataOutputStream(checkedStream);
              
              // remember if this item has content ... 
              boolean hasContent = request._item.isFieldDirty(CacheItem.Field_CONTENT);
              // now mark the content field as clean, so that it will not be serialized in our current serialization attempt ... 
              request._item.setFieldClean(CacheItem.Field_CONTENT);
              // and go ahead and write out the data to the intermediate buffer while also computing partial checksum 
              request._item.write(dataOutputStream);

              request._item.setFieldDirty(CacheItem.Field_CONTENT);
               

              // ok, now ... write out file header ... 
              CacheItemHeader itemHeader = new CacheItemHeader(_manager.getLocalLogSyncBytes());
              
              itemHeader._status          = CacheItemHeader.STATUS_ALIVE;
              itemHeader._lastAccessTime  = System.currentTimeMillis();
              itemHeader._fingerprint     = request._itemFingerprint;
              // compute total length ... 
              
              // first the header bytes in the cacheItem 
              itemHeader._dataLength      =   bufferOutputStream.size();
              // next the content length (encoded - as in size + bytes) ... 
              itemHeader._dataLength      +=  4 + request._item.getContent().getCount();
              // lastly the crc value iteself ... 
              itemHeader._dataLength      += 8;
              // open the log file ... 
              DataOutputBuffer logStream = new DataOutputBuffer();

              // ok, go ahead and write the header 
              itemHeader.writeHeader(logStream);
              // ok now write out the item data minus content... 
              logStream.write(bufferOutputStream.getBuffer(),0,bufferOutputStream.size());
              // now create a checked stream for the content ... 
              CheckedOutputStream checkedStream2 = new CheckedOutputStream(logStream,checkedStream.getChecksum());
              
              dataOutputStream = new DataOutputStream(checkedStream2);
              
              // content size 
              dataOutputStream.writeInt(request._item.getContent().getCount());
              // now write out the content (via checked stream so that we can calc checksum on content)
              dataOutputStream.write(request._item.getContent().getReadOnlyBytes(),0,request._item.getContent().getCount());
              // ok ... lastly write out the checksum bytes ... 
              dataOutputStream.writeLong(checkedStream2.getChecksum().getValue());
              // and FINALLY, write out the total item bytes (so that we can seek in reverse to read last request log 
              logStream.writeInt(CacheItemHeader.SIZE + itemHeader._dataLength);
              
              // ok flush everyting to the memory stream 
              dataOutputStream.flush();
                

              //ok - time to acquire the log semaphore 
              //LOG.info("Acquiring Local Log Semaphore");
              _manager.getLocalLogAccessSemaphore().acquireUninterruptibly();
                            
              try { 
              	
              	// now time to acquire the write semaphore ... 
              	_manager.getLocalLogWriteAccessSemaphore().acquireUninterruptibly();
              	
                // get the current file position 
                long recordOffset = _manager.getLocalLogFilePos();
              	
              	try { 

	                long ioTimeStart = System.currentTimeMillis();
	                
	                RandomAccessFile logFile = new RandomAccessFile(_manager.getActiveLogFilePath(),"rw");
	                
	                try { 
	                	// seek to our known record offset 
	                	logFile.seek(recordOffset);
	                	// write out the data
	                	logFile.write(logStream.getData(),0,logStream.getLength());
	                }
	                finally { 
	                	logFile.close();
	                }
	                // now we need to update the file header 
	                _manager.updateLogFileHeader(_manager.getActiveLogFilePath(),1,CacheItemHeader.SIZE + itemHeader._dataLength + 4 /*trailing bytes*/);
	                
	                CacheManager.LOG.info("#### Wrote Cache Item in:" + (System.currentTimeMillis() - timeStart) 
	                		+ " iotime:" + (System.currentTimeMillis() - ioTimeStart) + " QueueSize:"  + _writeRequestQueue.size());
	                
              	}
	              finally {
	              	// release write semaphore quickly 
	              	_manager.getLocalLogWriteAccessSemaphore().release();
	              }
	              
                // now inform the manager of the completed request ... 
                _manager.writeRequestComplete(request,recordOffset);
              }
              finally { 
                //LOG.info("Releasing Local Log Semaphore");
                _manager.getLocalLogAccessSemaphore().release();
              }
            }
            catch (IOException e) { 
            	CacheManager.LOG.error("### FUC# BATMAN! - GONNA LOSE THIS REQUEST!!!!:" + CCStringUtils.stringifyException(e));
              _manager.writeRequestFailed(request,e);
            }
          }
          break;
        }
      } catch (InterruptedException e) {
        
      }
    }
  }
}