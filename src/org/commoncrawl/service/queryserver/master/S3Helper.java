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
package org.commoncrawl.service.queryserver.master;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.concurrent.Semaphore;
import java.util.zip.Deflater;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.io.NIOBufferList;
import org.commoncrawl.io.NIOHttpConnection;
import org.commoncrawl.protocol.ArchiveInfo;
import org.commoncrawl.protocol.shared.ArcFileItem;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.S3Downloader;
import org.commoncrawl.util.StreamingArcFileReader;

public class S3Helper {
  
  private static final Log LOG = LogFactory.getLog(S3Helper.class);
  
  private static SimpleDateFormat S3_TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy/MM/dd/");

  private static String hdfsNameToS3ArcFileName(long arcFileDate,int arcFilePartNo) {
    
    String arcFileName = Long.toString(arcFileDate) + "_" +  arcFilePartNo + ".arc.gz"; 
    
    synchronized (S3_TIMESTAMP_FORMAT) {
      return S3_TIMESTAMP_FORMAT.format(new Date(arcFileDate))  +arcFilePartNo + "/" +  arcFileName;
    }
  }
  
  public static ArcFileItem retrieveArcFileItem(ArchiveInfo archiveInfo,EventLoop eventLoop) throws IOException {  
    
  	// the default bucket id 
  	String bucketId = "commoncrawl-crawl-002";
  	
  	//ok, see if we need to switch buckets 
  	if (archiveInfo.getCrawlNumber() == 1) { 
  		bucketId = "commoncrawl";
  	}
  	
    S3Downloader downloader = new S3Downloader(bucketId,"","",false);
    
    // now activate the segment log ... 
    final Semaphore downloadCompleteSemaphore = new Semaphore(0);
    final StreamingArcFileReader arcFileReader = new StreamingArcFileReader(false);
    //arcFileReader.setArcFileHasHeaderItemFlag(false);
    
    // create a buffer list we will append incoming content into ... 
    final LinkedList<ByteBuffer> bufferList = new LinkedList<ByteBuffer>();
 
    downloader.initialize(new S3Downloader.Callback() {

      @Override
      public boolean contentAvailable(NIOHttpConnection connection,int itemId, String itemKey,NIOBufferList contentBuffer) {
        LOG.info("ContentQuery contentAvailable called for Item:" + itemKey + " totalBytesAvailable:" + contentBuffer.available());
        
        
        try { 
          while (contentBuffer.available() != 0) { 
            bufferList.add(contentBuffer.read());
          }
          return true;
        }
        catch (IOException e) {
          LOG.error(CCStringUtils.stringifyException(e));
          return false;
        }
      }

      @Override
      public void downloadComplete(int itemId, String itemKey) {
        LOG.info("S3 Download Complete for item:" + itemKey);
        downloadCompleteSemaphore.release();
      }

      @Override
      public void downloadFailed(int itemId, String itemKey, String errorCode) {
        LOG.info("S3 Download Failed for item:" + itemKey);
        downloadCompleteSemaphore.release();
      }

      @Override
      public boolean downloadStarting(int itemId, String itemKey,long contentLength) {
        LOG.info("ContentQuery DownloadStarting for Item:" + itemKey + " contentLength:" + contentLength);
        return true;
      } 
      
    },eventLoop);
    
    
    LOG.info("Starting request for Item:" + hdfsNameToS3ArcFileName(archiveInfo.getArcfileDate(),archiveInfo.getArcfileIndex()) + " Offset:" + archiveInfo.getArcfileOffset());
    
    int sizeToRetrieve = (archiveInfo.getCompressedSize() != 0) ? archiveInfo.getCompressedSize() : 30000;
    sizeToRetrieve += 10;
    
    downloader.fetchPartialItem(hdfsNameToS3ArcFileName(archiveInfo.getArcfileDate(),archiveInfo.getArcfileIndex()), archiveInfo.getArcfileOffset()-10, sizeToRetrieve);
    downloadCompleteSemaphore.acquireUninterruptibly();
    
    if (bufferList.size() == 0) { 
    	return null;
    }
    
    ByteBuffer firstBuffer = bufferList.getFirst();
    if (firstBuffer != null) { 
    	int offsetToGZIPHeader = scanForGZIPHeader(firstBuffer.duplicate());
    	if (offsetToGZIPHeader != -1) { 
    		firstBuffer.position(offsetToGZIPHeader);
    		LOG.info("*** Offset to GZIP Header:" + offsetToGZIPHeader);
    	}
    	else { 
    		LOG.error("*** Failed to find GZIP Header offset");
    	}
    }
    
    
    
    // now try to decode content if possible
    for (ByteBuffer buffer : bufferList) { 
      LOG.info("Adding Buffer of Size:" + buffer.remaining() + " Position:" + buffer.position() + " Limit:" + buffer.limit());
    	arcFileReader.available(buffer);
    }
    
    ArcFileItem item = arcFileReader.getNextItem();

    if (item != null) { 
      LOG.info("Request Returned item:" + item.getUri());
      LOG.info("Uncompressed Size:" + item.getContent().getCount());
    }
    return item;
  }
  
  static int scanForGZIPHeader(ByteBuffer byteBuffer) throws IOException { 
  	
  	LOG.info("*** SCANNING FOR GZIP MAGIC Bytes:" + Byte.toString((byte)StreamingArcFileReader.GZIP_MAGIC) + " " + Byte.toString((byte)(StreamingArcFileReader.GZIP_MAGIC >> 8)) + " BufferSize is:" + byteBuffer.limit() + " Remaining:" + byteBuffer.remaining());
  	int limit = byteBuffer.limit();
  	
  	while (byteBuffer.position() + 2 < limit) { 
  		//LOG.info("Reading Byte At:"+ byteBuffer.position());
      int b = byteBuffer.get();
      //LOG.info("First Byte is:"+ b);
      if (b == (byte)(StreamingArcFileReader.GZIP_MAGIC)) {
      	
      	byteBuffer.mark();
      	
      	byte b2 = byteBuffer.get();
      	//LOG.info("Second Byte is:"+ b2);
      	if (b2 == (byte)(StreamingArcFileReader.GZIP_MAGIC >> 8)) {
      		
      		byte b3 = byteBuffer.get();
      		if (b3 == Deflater.DEFLATED) { 
      			LOG.info("Found GZip Magic at:" + (byteBuffer.position() - 3));
      			return byteBuffer.position() - 3;
      		}
      	}
      	byteBuffer.reset();
      }
  	}
  	LOG.error("Failed to Find GZIP Magic!!");
  	//LOG.error(Arrays.toString(byteBuffer.array()));
  	return -1;
  }
  
  public static void main(String[] args) {
	 File arcFile = new File(args[0]);
	 long offset  = Long.parseLong(args[1]);
	 long contentSize  = Long.parseLong(args[2]);
	 
	 try {
	  RandomAccessFile fileHandle = new RandomAccessFile(arcFile, "r");
	  
	  fileHandle.seek(Math.max(offset - 10,0));
	  
	  byte data[] = new byte[(int)contentSize + 10];
	  fileHandle.readFully(data);
	  
	  ByteBuffer buffer = ByteBuffer.wrap(data);
	  buffer.position(0);
	  
	  int position = scanForGZIPHeader(buffer.slice());
	 
	  buffer.position(position);
	  
	  StreamingArcFileReader reader = new StreamingArcFileReader(false);
	  reader.available(buffer);
	  ArcFileItem nextItem =  reader.getNextItem();
	  System.out.println(nextItem.getUri());
	  
	  
  } catch (IOException e) {
	  // TODO Auto-generated catch block
	  e.printStackTrace();
  }
  }
}
