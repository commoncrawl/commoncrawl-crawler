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
package org.commoncrawl.service.pagerank.slave;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.zip.CRC32;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.commoncrawl.async.CallbackWithResult;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.async.Timer;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.protocol.CompressedOutlinkList;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.rpc.base.internal.AsyncClientChannel;
import org.commoncrawl.rpc.base.internal.AsyncContext;
import org.commoncrawl.rpc.base.internal.AsyncRequest;
import org.commoncrawl.rpc.base.internal.AsyncServerChannel;
import org.commoncrawl.rpc.base.internal.NullMessage;
import org.commoncrawl.rpc.base.internal.Server;
import org.commoncrawl.rpc.base.internal.AsyncRequest.Callback;
import org.commoncrawl.rpc.base.internal.AsyncRequest.Status;
import org.commoncrawl.rpc.base.shared.RPCException;
import org.commoncrawl.rpc.base.shared.RPCStruct;
import org.commoncrawl.service.crawler.filters.SuperDomainFilter;
import org.commoncrawl.service.crawler.filters.Filter.FilterResult;
import org.commoncrawl.service.pagerank.BaseConfig;
import org.commoncrawl.service.pagerank.BeginPageRankInfo;
import org.commoncrawl.service.pagerank.BlockTransfer;
import org.commoncrawl.service.pagerank.BlockTransferAck;
import org.commoncrawl.service.pagerank.CheckpointInfo;
import org.commoncrawl.service.pagerank.Constants;
import org.commoncrawl.service.pagerank.FileInfo;
import org.commoncrawl.service.pagerank.IterationInfo;
import org.commoncrawl.service.pagerank.PRRangeItem;
import org.commoncrawl.service.pagerank.PageRankSlave;
import org.commoncrawl.service.pagerank.SlaveStatus;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.FileUtils;
import org.commoncrawl.util.FlexBuffer;
import org.commoncrawl.util.JVMStats;
import org.commoncrawl.util.URLUtils;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.hadoop.compression.lzo.LzoCodec;


public class PageRankUtils {

  // TODO:HACK 
  public static final int           VALUES_PER_RANGE  = 10;

  public static final Log LOG = LogFactory.getLog(PageRankUtils.class);

  public  static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }    
    
  private static String outlinkValuesFilePrefix = "OutlinkPR";
  
  public static Path getCheckpointFilePath(Path jobPath,int iterationPhase,int iterationNumnber,int nodeIndex) { 
  	String fileName = IterationInfo.Phase.toString(iterationPhase) + "-CheckpointComplete-"+ NUMBER_FORMAT.format(iterationNumnber) + "-" + NUMBER_FORMAT.format(nodeIndex);
  	return new Path(jobPath,fileName);
  }
  
  public static String makeUniqueFileName(String fileNamePrefix,int iterationNumber,int nodeIndex) { 
    if (iterationNumber == 0) { 
      return fileNamePrefix + NUMBER_FORMAT.format(nodeIndex);
    }
    else { 
      return fileNamePrefix + NUMBER_FORMAT.format(iterationNumber) + "-" + NUMBER_FORMAT.format(nodeIndex);
    }
  }
  
  public static File makeIdsFilePath(File basePath, int nodeIndex) { 
    return new File(basePath,PageRankUtils.makeUniqueFileName(Constants.PR_IDS_FILE_PREFIX,0,nodeIndex));
  }

  public static Path makeRangeFilePath(File basePath, int nodeIndex) { 
    return new Path(basePath.getAbsolutePath(),PageRankUtils.makeUniqueFileName(Constants.PR_RANGE_FILE_PREFIX,0,nodeIndex));
  }
  
  public static String getOutlinksBaseName(int myNodeIdx,int iterationNumber) { 
    return outlinkValuesFilePrefix + "-" + NUMBER_FORMAT.format(iterationNumber) + "-" + NUMBER_FORMAT.format(myNodeIdx);
  }
  
  
  private static int readVIntFromByteBuffer(ByteBuffer source) { 
    return (int) readVLongFromByteBuffer(source);
  }
  
  private static long readVLongFromByteBuffer(ByteBuffer source) { 
    byte firstByte = source.get();
    int len = WritableUtils.decodeVIntSize(firstByte);
    if (len == 1) {
      return firstByte;
    }
    long i = 0;
    for (int idx = 0; idx < len-1; idx++) {
      byte b = source.get();
      i = i << 8;
      i = i | (b & 0xFF);
    }
    return (WritableUtils.isNegativeVInt(firstByte) ? (i ^ -1L) : i);
  }
  
  
  public static final class PRValueMap {
    
    private static final int RANGE_ITEM_SIZE = 20;
    private static final int RANGE_FP_OFFSET = 8;
    private static final int RANGE_POS_OFFSET = 16;
    
    private File rangeFilePath = null;
    private ByteBuffer valueFileBuffer = null;
    private ByteBuffer rangeFileBuffer = null;
    private int rangeItemCount = 0;
    
    public PRValueMap() { 
      
    }
    
    public void open(FileSystem fs,Path valueFilePath,Path rangeFilePath)throws IOException {
    	
    	
      LOG.info("OPENING PRValueMap - Available Memory:" + Runtime.getRuntime().freeMemory() + " TotalMemory:" + Runtime.getRuntime().totalMemory()) ;
      
      
      FileStatus valueFileStatus = fs.getFileStatus(valueFilePath);
      FileStatus rangeFileStatus = fs.getFileStatus(rangeFilePath);
      
      if (valueFileStatus == null) { 
      	LOG.error("Value File at Path:" + valueFilePath + " not Found!");
      	throw new FileNotFoundException();
      }
      if (rangeFileStatus == null) { 
      	LOG.error("Range File at Path:" + rangeFilePath + " not Found!");
      	throw new FileNotFoundException();
      }
      
      FSDataInputStream valueFile = null;
      FSDataInputStream rangeFile = null;
      
      try {
        LOG.info("Create R/W Random Access File for values Path:" + valueFilePath);
        valueFile = fs.open(valueFilePath);
        LOG.info("Create R-ONLY Random Access File for range Path:" + rangeFilePath);
        rangeFile = fs.open(rangeFilePath);
        LOG.info("Allocating R/W Buffer of Size:" + valueFileStatus.getLen()  + " for Value File" + " Available Memory:" + Runtime.getRuntime().freeMemory());
        JVMStats.dumpMemoryStats();
        byte [] valueMapData = new byte[(int)valueFileStatus.getLen()];
        //this.valueFileBuffer = ByteBuffer.allocate((int) valueFileStatus.getLen() );
        this.valueFileBuffer = ByteBuffer.wrap(valueMapData);
        LOG.info("Loading R/W Buffer From Value File");
        long loadStart = System.currentTimeMillis();
        for (int offset=0,totalRead=0;offset<valueFileBuffer.capacity();) { 
          int bytesToRead = Math.min(16384,valueFileBuffer.capacity() - totalRead);
          valueFile.read(valueFileBuffer.array(),offset,bytesToRead);
          offset+= bytesToRead;
          totalRead += bytesToRead;
        }
        LOG.info("Load of Value File Buffer Took:" + (System.currentTimeMillis() - loadStart) + " MS");
        
        LOG.info("Mapping R-ONLY Buffer of Size:" + rangeFileStatus.getLen() + " for Range File");
        this.rangeFileBuffer = ByteBuffer.allocate((int) rangeFileStatus.getLen() );
        LOG.info("Loading RangeFile Buffer From Range File");
        loadStart = System.currentTimeMillis();
        for (int offset=0,totalRead=0;offset<rangeFileBuffer.capacity();) { 
          int bytesToRead = Math.min(16384,rangeFileBuffer.capacity() - totalRead);
          rangeFile.read(rangeFileBuffer.array(),offset,bytesToRead);
          offset+= bytesToRead;
          totalRead += bytesToRead;
        }
        LOG.info("Load of Range File Buffer Took:" + (System.currentTimeMillis() - loadStart) + " MS");
        // calculate range item count
        rangeItemCount = (int)rangeFileStatus.getLen() / RANGE_ITEM_SIZE;
      }
      finally { 
        if (valueFile != null) 
          valueFile.close();
        if (rangeFile != null)
          rangeFile.close();
      }
    }
    
    void flush(OutputStream stream) throws IOException { 
      if (valueFileBuffer != null) { 
        LOG.info("Flushing valueBuffer");

        LOG.info("Accessing underlying ByteArray");
        valueFileBuffer.position(0);
        byte array[] = valueFileBuffer.array();
        long timeStart = System.currentTimeMillis();
        stream.write(array);
        long timeEnd = System.currentTimeMillis();
        LOG.info("ValueBuffer Flush took:" + (timeEnd-timeStart) + " Milliseconds - valueBufferSize:" + valueFileBuffer.limit());
      }
    }
    
    void close() throws IOException { 

      LOG.info("CLOSING PRValueMap");
      
      valueFileBuffer = null;
      rangeFileBuffer = null;
    }
    
    enum GetSetOPType { 
      GET,
      SET,
      ADD
    }
    
    public final float getPRValue(URLFPV2 urlItem) throws IOException { 
      return getSetPRValue(urlItem, GetSetOPType.GET, 0.0f);
    }
    
    public final void setPRValue(URLFPV2 urlItem, float value) throws IOException { 
      getSetPRValue(urlItem, GetSetOPType.SET, value);
    }

    public final void addPRValue(URLFPV2 urlItem, float value) throws IOException { 
      getSetPRValue(urlItem, GetSetOPType.ADD, value);
    }
    
    public void zeroValues()throws IOException {
      
      valueFileBuffer.position(0);
      while (valueFileBuffer.position() < valueFileBuffer.limit()) { 
        valueFileBuffer.getLong();
        //TODO: SWITCH TO INT FOR TEST
        // valueFileBuffer.putShort((short)0);
        valueFileBuffer.putFloat(0.0f);
      }
    }
    
    // TODO: SWITCH TO INT FOR TEST
    // static Map<Long,Short> debugMap = new TreeMap<Long,Short>();
    static Map<Long,Float> debugMap = new TreeMap<Long,Float>();
    
    public void finalizePageRank()throws IOException {
      valueFileBuffer.position(0);
      
      int itemCount = 0;
      
      while (valueFileBuffer.position() < valueFileBuffer.limit()) { 
        long fingerprint = valueFileBuffer.getLong();
        valueFileBuffer.mark();
        //TODO: SWITCH TO INT FOR TEST
        // int accumulatedRank = valueFileBuffer.getShort();
        float accumulatedRank = valueFileBuffer.getFloat();
        // TODO: hack use default pr formula for now ...
        float finalRank =  (.150f + (.85f * (float)accumulatedRank));
        
        valueFileBuffer.reset();
        valueFileBuffer.putFloat(finalRank);
      }
    }
    
    final float getSetPRValue(URLFPV2 urlItem,GetSetOPType opType,float valueIn) throws IOException{
      
    	//long timeStart = System.currentTimeMillis();
      int rangeIdx = findRangePosition(urlItem);
      //long timeEnd = System.currentTimeMillis();
      
      if (rangeIdx == -1) { 
        throw new IOException("Unable to locate PR Value for domain:" + urlItem.getDomainHash() + " fingerprint:" + urlItem.getUrlHash());
      }
      
      //DBG
      if (1 == 0) { 
	      URLFPV2 rangeFP = new URLFPV2();
	      populateFPForRange(rangeFileBuffer,rangeFP, rangeIdx);
	      //LOG.info("Range for Domain:" + urlItem.getDomainHash() + " FP:" + urlItem.getUrlHash() + " is Domain:" + rangeFP.getDomainHash() + " FP:" + rangeFP.getUrlHash() );
      }
      
      //get the search start positon via the range
      int rangeOffset = rangeFileBuffer.getInt(rangeIdx*RANGE_ITEM_SIZE + RANGE_POS_OFFSET);
      // now start walking items in range ... 
      //LOG.info("RangeOffset for domain:" + urlItem.getDomainHash() + " fingerprint:" + urlItem.getUrlHash() + " is:" + rangeOffset);
      
      // seek to range offset ... 
      valueFileBuffer.position(rangeOffset);
      
      //timeStart = System.currentTimeMillis();
      
      // walk up to max number of items in range ... 
      for (int itemIdx=0;itemIdx<VALUES_PER_RANGE;++itemIdx) { 
        // read the urlf fp ... 
        long urlFPValue = valueFileBuffer.getLong();

        if (urlItem.getUrlHash() == urlFPValue) { 
        	
        	//timeEnd = System.currentTimeMillis();
        	///LOG.info("Scan took:" + (timeEnd-timeStart));
          if (opType == GetSetOPType.SET) {
            valueFileBuffer.putFloat(valueIn);
            return 0;
          }
          else if (opType == GetSetOPType.GET) {
            return valueFileBuffer.getFloat();
          }
          else { // ADD
           valueFileBuffer.mark();
           float value = valueFileBuffer.getFloat();
           valueFileBuffer.reset();
           valueFileBuffer.putFloat((Math.min(value + valueIn,Float.MAX_VALUE)));
           return 0;
          }
        }
        // otherwise skip the value ... 
        else {
          valueFileBuffer.getFloat();
        }
        
        // if we reached trailing end of buffer ... we are done 
        if(valueFileBuffer.remaining() == 0) { 
          throw new IOException("Reached end of Value Buffer Looking for Value");
        }
      }
      //this is bad news... dump context info for debug purposes before throwing exception
      LOG.error("Reached End of Range looking for PRValue for FP:"+ urlItem.getUrlHash());
      URLFPV2 rangeFPDBG = new URLFPV2();
      populateFPForRange(rangeFileBuffer,rangeFPDBG, rangeIdx);
      LOG.error("Closest Range Was Index:" + rangeIdx + " DomainHash:" + rangeFPDBG.getDomainHash() + " URLHash:" + rangeFPDBG.getUrlHash());
      if (rangeIdx + 1 < this.rangeItemCount) { 
      	populateFPForRange(rangeFileBuffer,rangeFPDBG, rangeIdx + 1);
      	LOG.error("Range At Index:" + (rangeIdx + 1) + " DomainHash:" + rangeFPDBG.getDomainHash() + " URLHash:" + rangeFPDBG.getUrlHash());
      }
      LOG.error("Dumping Next 600 bytes at offset:" + rangeOffset);

      /*
      // re-seek to range offset ... 
      valueFileBuffer.position(rangeOffset);
      LOG.error("\n" + dumpAsHex(valueFileBuffer, Math.min(600,valueFileBuffer.remaining())));
      */
      
      LOG.error("Dumping Values:");
      // re-seek to range offset ... 
      valueFileBuffer.position(rangeOffset);
      
      
      // walk up to max number of items in range ... 
      for (int itemIdx=0;itemIdx<VALUES_PER_RANGE  && valueFileBuffer.remaining() != 0;++itemIdx) { 
        // read the urlf fp ... 
        long urlFPValue = valueFileBuffer.getLong();
        // and the value 
        float value = valueFileBuffer.getFloat();
        LOG.error("Item:" + itemIdx +" FP:" + urlFPValue + " Value:" + value);
      }
      LOG.error("Dump Complete");
        
      throw new IOException("Reached the End of Range looking for designated PRValue");
    }
    
    
    private static final int HEX_CHARS_PER_LINE  = 32;
    public String dumpAsHex(ByteBuffer data,int amount) {
      
        StringBuffer buf = new StringBuffer(amount << 1) ;
        int k = 0 ;
        int flen = amount;
        char hexBuffer[] = new char[HEX_CHARS_PER_LINE*2 + (HEX_CHARS_PER_LINE - 1)+ 2];
        char asciiBuffer[] = new char[HEX_CHARS_PER_LINE + 1];
        
        hexBuffer[hexBuffer.length-1]=0;
        asciiBuffer[asciiBuffer.length - 1]=0;

        for (int i = 0; i < flen ; i++) {
            int j = data.get() & 0xFF ;
            
            hexBuffer[k*3] = Character.forDigit((j >>> 4) , 16);
            hexBuffer[k*3+1] = Character.forDigit((j & 0x0F), 16);
            hexBuffer[k*3+2] = ' ';
            
            if (j<0x20)
              asciiBuffer[k] =  '.';
            else if (k < 0x78)
              asciiBuffer[k] = (char)j;
            else
              asciiBuffer[k] = '?';            
            k++ ;
            if (k % HEX_CHARS_PER_LINE == 0) {
                hexBuffer[hexBuffer.length-2] = 0;
                buf.append(hexBuffer);
                buf.append(" ");
                buf.append(asciiBuffer);
                buf.append('\n') ;
                k = 0 ;
            }
        }
        if (k  != 0) {
          hexBuffer[k*3 + 1] = 0;
          asciiBuffer[k] = 0;
          buf.append(hexBuffer);
          buf.append(" ");
          buf.append(asciiBuffer);
          buf.append('\n') ;
        }
        return buf.toString() ;
    }
    
    int getRangeOffsetFromRangeIndex(int rangeIndex) { 
      return rangeFileBuffer.getInt(rangeIndex*RANGE_ITEM_SIZE + RANGE_POS_OFFSET);
    }
    
    static final void populateFPForRange(ByteBuffer sourceBuffer, URLFPV2 placeHolder,int rangeIndex) { 
      placeHolder.setDomainHash(sourceBuffer.getLong(rangeIndex*RANGE_ITEM_SIZE));
      placeHolder.setUrlHash(sourceBuffer.getLong(rangeIndex*RANGE_ITEM_SIZE + RANGE_FP_OFFSET));
    }
    
    final int findRangePosition(URLFPV2 searchTerm) {
      
    	long searchDomainHash = searchTerm.getDomainHash();
    	long searchURLHash    = searchTerm.getUrlHash();
      
      int low = 0;
      int high = rangeItemCount - 1;
      while (low <= high) {
        
      	int mid = low + ((high - low) / 2);
        
      	long currentDomainHash = rangeFileBuffer.getLong(mid*RANGE_ITEM_SIZE);
      	
      	int result = (currentDomainHash<searchDomainHash ? -1 : (currentDomainHash==searchDomainHash ? 0 : 1));
      	if (result == 0) { 
      		long currentURLHash = rangeFileBuffer.getLong(mid*RANGE_ITEM_SIZE + RANGE_FP_OFFSET);
      		result = (currentURLHash<searchURLHash ? -1 : (currentURLHash==searchURLHash ? 0 : 1));
      	}
        
        //comparisonFP.setDomainHash(rangeFileBuffer.getLong(mid*RANGE_ITEM_SIZE));
        //comparisonFP.setUrlHash(rangeFileBuffer.getLong(mid*RANGE_ITEM_SIZE + RANGE_FP_OFFSET));
        // populateFPForRange(rangeFileBuffer, comparisonFP, mid);
        if (result > 0)
            high = mid - 1;
        else if (result < 0)
            low = mid + 1;
        else
            return mid; // found
      }
      if (high < rangeItemCount)
        return high;
      return -1; // not found
    }

    void dumpRangeItems() {
      RandomAccessFile rangeFileObj = null;
      PRRangeItem item = new PRRangeItem();
      try {
        rangeFileObj = new RandomAccessFile(rangeFilePath,"r");
        
        for (int i=0;i<rangeItemCount;++i) { 
          item.clear();
          item.readFields(rangeFileObj);
          LOG.info("Range Item:" + i + " Domain:" + item.getDomainStart() + " FPStart:" + item.getUrlFPStart() + " Offset:" + item.getStartPos());
        }
        rangeFileBuffer.position(0);
      } 
      catch (IOException e) { 
        LOG.error(CCStringUtils.stringifyException(e));
      }
      finally { 
        if (rangeFileObj != null) { 
          try {
            rangeFileObj.close();
          } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
      }
    }
  }

  static int findPos(int[] array,int searchTerm) { 
    
    int low = 0;
    int high = array.length -1;
    while (low <= high) {
      int mid = low + ((high - low) / 2);  // Note: not (low + high) / 2 !!
      if (array[mid] > searchTerm)
          high = mid - 1;
      else if (array[mid] < searchTerm)
          low = mid + 1;
      else
          return mid; // found
    }
    if (high < array.length)
      return high;
    return -1; // not found
  }
  
  private static interface PRValueOutputStream { 
    
    void writePRValue(URLFPV2 targetFP,URLFPV2 sourceFP,float prValue) throws IOException;
    void close(boolean deleteUnderlyingFile) throws IOException;
  }
 
  private static class PRSequenceFileOutputStream implements PRValueOutputStream {

    FileSystem          _fileSystem;
    Path                _path;
    SequenceFile.Writer _writer = null;
    DataOutputBuffer    _outputWriter = new DataOutputBuffer();
    FlexBuffer          _buffer = new FlexBuffer();
    
    public PRSequenceFileOutputStream(Configuration conf,FileSystem fs,Path path) throws IOException { 
      _fileSystem = fs;
      _path = path;
      _writer = SequenceFile.createWriter(
          _fileSystem, 
          conf, 
          path, 
          FlexBuffer.class, 
          NullWritable.class,
          fs.getConf().getInt("io.file.buffer.size", 4096 * 12),
          (short)1, fs.getDefaultBlockSize(),
          CompressionType.BLOCK, new DefaultCodec(), null, new Metadata());          
    }
    
    @Override
    public void close(boolean deleteUnderlyingFile) throws IOException {
      _writer.close();
      if (deleteUnderlyingFile) { 
        _fileSystem.delete(_path,false);
      }
    }

    @Override
    public void writePRValue(URLFPV2 target, URLFPV2 source, float prValue) throws IOException {
      
      _outputWriter.reset();
      _outputWriter.writeLong(target.getDomainHash());
      _outputWriter.writeLong(target.getUrlHash());
      _outputWriter.writeLong(source.getRootDomainHash());
      _outputWriter.writeLong(source.getDomainHash());
      _outputWriter.writeLong(source.getUrlHash());
      _outputWriter.writeFloat(prValue);
      
      _buffer.set(_outputWriter.getData(), 0, _outputWriter.getLength());
      
      _writer.append(_buffer,NullWritable.get());
    }
    
  }
  
  private static class PROldValueOutputStream implements PRValueOutputStream { 
    
    PROldValueOutputStream(FileSystem fs,Path path)throws IOException {
      _targetFS = fs;
      _path = path;
      _stream = fs.create(path);
    }
        
    public FileSystem _targetFS; 
    public Path _path;   // optional path if this is a remote file 
    public FSDataOutputStream _stream;
    
    @Override
    public void close(boolean deleteUnderlyingFile) throws IOException {
      if (_stream != null){ 
        _stream.flush();
        _stream.close();
        _stream = null;
      }
      if (deleteUnderlyingFile) { 
        _targetFS.delete(_path,false);
      }
    }

    @Override
    public void writePRValue(URLFPV2 target, URLFPV2 source, float prValue)throws IOException {
      _stream.writeLong(target.getDomainHash());
      _stream.writeLong(target.getUrlHash());
      _stream.writeLong(source.getRootDomainHash());
      _stream.writeLong(source.getDomainHash());
      _stream.writeLong(source.getUrlHash());
      _stream.writeFloat(prValue);
    }
  }
  
  public static void purgeNodeDistributionFilesForIteration(FileSystem remoteFS,String remoteOutputPath,int nodeIndex,int nodeCount,int iterationNumber)throws IOException { 
  	String fileNamePrefix = getOutlinksBaseName(nodeIndex,iterationNumber);
  	
  	for (int i=0;i<nodeCount;++i) {
      // create output filename 
      String fileName = fileNamePrefix + "-" + NUMBER_FORMAT.format(i);

  		Path remotePath = new Path(remoteOutputPath,fileName);
  		
  		LOG.info("Deleting:" + remotePath);
  		remoteFS.delete(remotePath,true);
  	}
  }
  
 /**
  * PRValueMultiplexer
  *  multiplexes page rank value distribution
  *  across a set of pre-defined nodes  
  * 
  * @author rana
  *
  */
  public static class PRValueMultiplexer { 
    EventLoop _eventLoop = null;
    Vector<InetSocketAddress> _slaveAddressList;
    
    LinkedList<PRValueBlockWriter> _activeWriters = new LinkedList<PRValueBlockWriter>();
    PRValueBlockWriter _failedWriter = null;
    
    int _myNodeIndex;
    Configuration _conf;
    long _jobId;
    int  _iterationNumber;
    boolean _failed = false;
    int  _completionCount = 0;
    int _nodeCount =0;
    
    /**
     * construct a PRValueMultiplexer 
     * 
     * @param conf
     * @param jobId
     * @param iterationNumber
     * @param slaveAddressList
     * @param myNodeIndex
     * @throws IOException
     */
    public PRValueMultiplexer(Configuration conf,long jobId,int iterationNumber,Vector<InetSocketAddress> slaveAddressList,int myNodeIndex) throws IOException {
      
      LOG.info("PRValueMultiplexer initialized. SlaveAddress List Size:" + slaveAddressList.size() + " JobID:" + jobId + " myNodeId:" + myNodeIndex );
      _slaveAddressList = slaveAddressList;
      _myNodeIndex = myNodeIndex;
      _conf = conf;
      _jobId = jobId;
      _iterationNumber = iterationNumber;
      _nodeCount = slaveAddressList.size();
      
      // start event loop ... 
      _eventLoop = new EventLoop();
      _eventLoop.start();
      
      try { 
        createWriters();
      }
      catch (IOException e) {
        _failed = true;
        LOG.error("Got Exception opening BlockWriters");
        closeAllWriters();
        throw e;
      }
    }
    
    /**
     * close the multiplexer, and optionally wait and flush all streams 
     * @param forced - if false, block for all streams to complete
     * @return true if failure condition 
     */
    public boolean close(boolean forced) {
      
      // if not a forced close ... and we are not in a failure condition ...  
      if (!forced && !_failed && _completionCount != _slaveAddressList.size()) {
        LOG.info("Setting up Poll Loop to monitor for clean shutdown");
        // create a semaphore to block on 
        final Semaphore blockingSemaphore = new Semaphore(0);
        
        // set up a poll loop to monitor writers ...
        _eventLoop.setTimer(new Timer(10,true,new Timer.Callback() {
          
          @Override
          public void timerFired(Timer timer) {
            if (_failed || _completionCount == _slaveAddressList.size()) {
              if (_failed) { 
                LOG.error("Poll loop detected Failure. Shutting Down");
              }
              else { 
                LOG.info("Poll loop detected completion. Shutting Down");
              }
              // release semaphore ... 
              blockingSemaphore.release();
              // cancel timer ... 
              _eventLoop.cancelTimer(timer);
            }
          }
        }));
        
        // ok now wait for completion ...  
        blockingSemaphore.acquireUninterruptibly();
      }
      // a forced close is explicit, meaning just teardown everything ... 
      if (forced) { 
        _failed = true;
      }
      // ok close all writes 
      closeAllWriters();
      
      // ok finally shutdown the event loop 
      _eventLoop.stop();
      
      return _failed;
    }
    
    
    /**
     * write a page rank value to the appropriate stream ... 
     *  this method could block ...  
     * @param target
     * @param source
     * @param prValue
     * @throws IOException
     */
    public void writePRValue(int targetNode,URLFPV2 targetFP,URLFPV2 sourceFP,float prValue)throws IOException {
      
      if (_failed) { 
        throw new IOException("Multiplexer in Failed State!");
      }
      // figure out which stream this entry belongs to ... 
      //int nodeIndex = (target.hashCode() & Integer.MAX_VALUE) % _nodeCount;
      
      // write directly to the proper block writer
      PRValueBlockWriter writer = null;
      synchronized (_activeWriters) {
        if (_activeWriters.size() != 0)
          writer = _activeWriters.get(targetNode);
      }
      if (writer != null) {  
        writer.writePRValue(targetFP, sourceFP, prValue);
      }
      else { 
        LOG.error("No Writer Found for nodexIndex:" + targetNode);
      }
    }
 
    
    /**
     * 
     * create the block writers   
     * @throws IOException
     */
    void createWriters()throws IOException { 
      int targetSlaveIndex = 0;
      for (InetSocketAddress targetSlaveAddress : _slaveAddressList) {
        LOG.info("Creating Writer for:" + targetSlaveAddress);
        PRValueBlockWriter prValueWriter = new PRValueBlockWriter(
            this, 
            _conf, 
            _jobId, 
            targetSlaveAddress, 
            targetSlaveIndex++, 
            _myNodeIndex, 
            _iterationNumber 
            );

        synchronized (_activeWriters) {
          _activeWriters.add(prValueWriter);
        }
      }
    }
    
    void writerFailed(final PRValueBlockWriter writer,final IOException reason) { 
      LOG.info("Writer Failed Callback for writer:" + writer._targetSlaveAddress);
      _failed = true;
      
      // fail this in the context of the async thread 
      _eventLoop.setTimer(new Timer(0,false,new Timer.Callback() {
        
        @Override
        public void timerFired(Timer timer) {
          LOG.error("Writer for Slave:"+ writer._targetSlaveAddress 
              + " failed with exception:" 
              + CCStringUtils.stringifyException(reason));
          
          _failedWriter = writer;
          
          closeAllWriters();
        }
        
      }));
    }
    
    void writerDone(final PRValueBlockWriter writer) {
      LOG.info("Writer:" + writer._targetSlaveAddress + " done");
      synchronized (this) {
        _completionCount++;
      }
      writer.close();
    }
    
    void closeAllWriters() {
      LOG.info("Multiplexer: Closing all Writers");
      ImmutableList<PRValueBlockWriter> writers = null;
      synchronized (_activeWriters) {
        writers = new ImmutableList.Builder().addAll(_activeWriters).build();
      }
      
      for (PRValueBlockWriter writer : writers) { 
        writer.close();
      }
      // clear list 
      synchronized (_activeWriters) {
        _activeWriters.clear();
      }
    }
  }
  
  /**
   * Individual Node PageRank Value Stream Writer
   * 
   * @author rana
   *
   */
  static class PRValueBlockWriter implements AsyncClientChannel.ConnectionCallback  {
    
    PRValueMultiplexer _multiplexer;
    ByteBuffer _outputBuffer = null;
    byte[] _outputArray = null;
    LinkedBlockingQueue<ByteBuffer> _packetQueue = new LinkedBlockingQueue<ByteBuffer>(MAX_PACKETS_ENQUEUED);
    CRC32 _crc32 = new CRC32();
    int _itemCount=0;
    LzoCodec _codec = new LzoCodec();
    InetSocketAddress _targetSlaveAddress;
    int _targetSlaveIndex;
    int _sourceSlaveIndex;
    int _iterationNumber;
    String _targetFileName;
    FileInfo                  _fileInfo = new FileInfo();
    long _lastBlockId = 0;
    // set when no more data is expected ..
    boolean _done = false;
    
    // slave communication related code ... 
    AsyncClientChannel        _channel;
    PageRankSlaveServer.AsyncStub  _asyncStub;
    Semaphore                 _blockingCallSemaphore = null;
    IOException               _lastIOException = null;
    String                    _logLinePrefix;
    

    private void log(boolean isError,String message) {
      if (isError)
        LOG.error(_logLinePrefix + message);
      else 
        LOG.info(_logLinePrefix + message);
    }
    
    public PRValueBlockWriter(PRValueMultiplexer multiplexer, Configuration conf,
        long jobId,
        InetSocketAddress targetSlaveAddress,
        int targetSlaveIndex,
        int sourceSlaveIndex,
        int iterationNumber)throws IOException {
      _multiplexer = multiplexer;
      _outputBuffer = allocateNewBuffer();
      _codec.setConf(conf);
      _targetSlaveAddress = targetSlaveAddress;
      _targetSlaveIndex = targetSlaveIndex;
      _sourceSlaveIndex = sourceSlaveIndex;
      _iterationNumber = iterationNumber; 
      _logLinePrefix = "[TGT:" + targetSlaveIndex + " Addr:" + _targetSlaveAddress + "]";
      
      _blockingCallSemaphore = new Semaphore(0);

      
      log(false,"Connecting to slave at index:" + _targetSlaveIndex 
          + " endPoint:"+  _targetSlaveAddress);
      _channel = new AsyncClientChannel(_multiplexer._eventLoop,null,_targetSlaveAddress,this);
      _channel.open();
      _asyncStub = new PageRankSlaveServer.AsyncStub(_channel);
      log(false,"Waiting on Connect... ");
      _blockingCallSemaphore.acquireUninterruptibly();
      log(false,"Connect Semaphore Released... ");
      
      
      if (!_channel.isOpen()) {
        log(true,"Connection Failed!");
        throw new IOException("Connection Failed!");
      }
      _targetFileName = getOutlinksBaseName(_sourceSlaveIndex,_iterationNumber) + "-" + NUMBER_FORMAT.format(_targetSlaveIndex);
      _fileInfo.setFileName(_targetFileName);
      _fileInfo.setJobId(jobId);
      log(false,"Sending Open File Command For Target:" + _targetSlaveAddress);
      sendOpenFileCommand();
    }

    /**
     * Enqueue a page-rank value into this stream  
     * 
     * @param target
     * @param source
     * @param prValue
     * @throws IOException
     */
    public void writePRValue(URLFPV2 target,URLFPV2 source,float prValue)throws IOException {
      
      _outputBuffer.putLong(target.getDomainHash());
      _outputBuffer.putLong(target.getUrlHash());
      _outputBuffer.putLong(source.getRootDomainHash());
      _outputBuffer.putLong(source.getDomainHash());
      _outputBuffer.putLong(source.getUrlHash());
      _outputBuffer.putFloat(prValue);
      
      if (++_itemCount == RECORDS_PER_BLOCK) { 
        // flush 
        flush();
      }
    }    
    
    /**
     * mark this stream as compelte  
     */
    public void done() { 
      // mark the stream as complete ... 
      _done = true;
      // the poll thread will 
    }
    
    
    void queuePollEvent() { 
      // start the poll timer ... 
      _multiplexer._eventLoop.setTimer(new Timer(10, false, new Timer.Callback() {
        
        @Override
        public void timerFired(Timer timer) {
          if (_lastIOException == null && _channel.isOpen()) { 
            ByteBuffer nextPacket = _packetQueue.poll();
            
            if (nextPacket != null) { 
              // ok we got a packet ... send it 
              log(false, "got packet via poll");
              
              BlockTransfer tranfserRequest = new BlockTransfer();
              
              tranfserRequest.setBlockData(new FlexBuffer(nextPacket.array(),0,nextPacket.limit()));
              tranfserRequest.setBlockId(_lastBlockId++);
              tranfserRequest.setFileId(_fileInfo.getFileId());
              
              try { 
                log(false,"Calling transferBlock RPC");
                _asyncStub.transferBlock(tranfserRequest,new Callback<BlockTransfer, BlockTransferAck>() {
  
                  @Override
                  public void requestComplete(
                      AsyncRequest<BlockTransfer, BlockTransferAck> request) {

                    log(false,"transferBlock RPC Returned with Status:" + request.getStatus());
                    
                    if (request.getStatus() == Status.Success) { 
                      // queue next poll event ... 
                      queuePollEvent();
                    }
                    else { 
                      log(true, "transferBlock Failed!");
                      failed(new IOException("Transfer Block Failed!"));
                    }
                  }
  
                });
              }
              catch (IOException e) { 
                log(true, CCStringUtils.stringifyException(e));
                // mark this stream as done ... 
                failed(e);
              }
            }
            else {
              // check to see if we are done 
              if (_done) {
                try { 
                  log(false,"Sending commitFile RPC");
                  _asyncStub.commitFile(_fileInfo, new Callback<FileInfo, NullMessage>() {
  
                    @Override
                    public void requestComplete(AsyncRequest<FileInfo, NullMessage> request) {
                      log(false,"commitFile RPC returned with Status:" + request.getStatus());
                      if (request.getStatus() == Status.Success) { 
                        _multiplexer.writerDone(PRValueBlockWriter.this);
                      }
                    }
                  });
                }
                catch (IOException e) { 
                  log(true,CCStringUtils.stringifyException(e));
                }
              }
              else { 
                queuePollEvent();
              }
            }
          }
          else {
            failed(null);
          }
        }
      }));
      
    }
    
    void sendOpenFileCommand()throws IOException {
      log(false,"sending createJobFile RPC");
      _asyncStub.createJobFile(_fileInfo, new Callback<FileInfo, FileInfo>() {

        @Override
        public void requestComplete(AsyncRequest<FileInfo, FileInfo> request) {
          log(false,"createJobFile RPC returned with Status:" + request.getStatus());
          if (request.getStatus() == Status.Success) { 
            log(false,"Create File Successful!!");
            _fileInfo.setFileId(request.getOutput().getFileId());
            // start polling 
            log(false,"Polling for Data Packets");
            queuePollEvent();            
          }
          else { 
            // indicate a failure condition ... 
            failed(new IOException("File Open Failed for Slave:" +
              _targetSlaveAddress ));
          }
        }
      });
      
    }
    
    /** 
     * indicate a failure condition 
     * @param e
     */
    private void failed(IOException e) {
      log(true,"failed called with Exception:" + CCStringUtils.stringifyException(e));
      if (e != null) { 
        _lastIOException = e;
      }
      // inform the multiplexer of the error ...
      _multiplexer.writerFailed(this,_lastIOException);
      
    }
    
    public void close() { 
      log(false,"close called channel is:" + _channel + " packetQueue size is:" + _packetQueue.size());
      if (_channel != null) { 
        try {
          _channel.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
        _channel = null;
      }
      // dump packets on the floor
      _packetQueue.clear();
    }
    
    
    private ByteBuffer allocateNewBuffer() { 
      return ByteBuffer.allocate(BLOCK_HEADER_SIZE + RECORD_BYTE_SIZE * RECORDS_PER_BLOCK + PADDING);
    }
    
    
    private static final int MAX_PACKETS_ENQUEUED = 5;
    private static final int RECORD_BYTE_SIZE = 48; // EACH RECORD IS 48 bytes long ...
    private static final int RECORDS_PER_BLOCK = (2 ^ 12); // 4096 records per block... 
    private static final int SYNC_ESCAPE = -1;      // "length" of sync entries
    private static final int SYNC_ESCAPE_SIZE = 4;      // "length" of sync entries
    private static final byte SYNC_BYTES[] = { 'S','Y','N','C','B','Y','T','E' };
    // sync bytes size ... 
    private static final int BLOCK_SYNC_BYTE_SIZE = SYNC_ESCAPE_SIZE +SYNC_BYTES.length; // escape + hash;
    // block CRC LENGTH 
    private static final int BLOCK_CRC_FIELD_SIZE = 8;
    // block LENGTH 
    private static final int BLOCK_COMPRESSED_LENGTH_FIELD_SIZE = 4;
    // block LENGTH 
    private static final int BLOCK_UNCOMPRESSED_LENGTH_FIELD_SIZE = 4;
    // PADDING FOR COMPRESSOR 
    private static final int PADDING = 2 ^ 8;
    
    
    
    // block header size ... 
    private static final int BLOCK_HEADER_SIZE 
      = BLOCK_SYNC_BYTE_SIZE 
      + BLOCK_CRC_FIELD_SIZE 
      + BLOCK_COMPRESSED_LENGTH_FIELD_SIZE
      + BLOCK_UNCOMPRESSED_LENGTH_FIELD_SIZE;
    
    
    
    void flush()throws IOException {
      log(false,"flush called");
      
      if (!_channel.isOpen() || _lastIOException != null) { 
        log(true,"Invalid State. Connection Already Closed!");
        throw new IOException("Connection Already Closed!");
      }
      
      // queue packet for send ... 
      if (_outputBuffer.position() != 0) { 
        // create compressed buffer object .. 
        ByteBuffer compressedBuffer = allocateNewBuffer();
        // skip header ... 
        compressedBuffer.position(BLOCK_HEADER_SIZE);
        // create output stream based on bytebuffer 
        OutputStream compressedDataOutputStream = newOutputStream(compressedBuffer);
        // ok ... now compress the block 
        CompressionOutputStream codecStream = _codec.createOutputStream(compressedDataOutputStream);
        // compress data .. 
        codecStream.write(_outputBuffer.array(), 0, _outputBuffer.position());
        // flush it ... 
        codecStream.close();
        // compute crc ... 
        _crc32.reset();
        _crc32.update(_outputBuffer.array(), 0, _outputBuffer.position());
        // remember compressed buffer size ..
        int compressedBufferSize = compressedBuffer.position() - BLOCK_HEADER_SIZE;
        // ok write out header ... 
        compressedBuffer.position(0);
        // write sync bytes into header ... 
        compressedBuffer.putInt(SYNC_ESCAPE);
        // and write sync bytes 
        compressedBuffer.put(SYNC_BYTES,0,SYNC_BYTES.length);
        // write crc ... 
        compressedBuffer.putLong(_crc32.getValue());
        // write compressed length and uncompressed length... 
        compressedBuffer.putInt(compressedBufferSize);
        compressedBuffer.putInt(_outputBuffer.position());
        // and put it in queue ...
        compressedBuffer.position(compressedBufferSize + BLOCK_HEADER_SIZE);
        // flip it .. 
        compressedBuffer.flip();
        
        log(false,"queueing packet. Item Count:" 
            + _itemCount 
            + " UncompressedSize:" + _outputBuffer.position()
            + " CompressedSize:" + compressedBuffer.limit());
        // add it to queue 
        try {
          _packetQueue.put(compressedBuffer);
        } catch (InterruptedException e) {
        }
        // get new output buffer ...  
        _outputBuffer.position(0);
        // reset item count 
        _itemCount = 0;
      }
    }
    
    private static OutputStream newOutputStream(final ByteBuffer buf) {
      return new OutputStream() {

        @Override
        public void write(int b) throws IOException {
          buf.put((byte) (b & 0xff));
        }

        public void write(byte src[], int off, int len) throws IOException {
          buf.put(src, off, len);
        }
      };
    }


    @Override
    public void OutgoingChannelConnected(AsyncClientChannel channel) {
      LOG.info("OutgoingChannelConnected... ");
      if (_blockingCallSemaphore != null) { 
        _blockingCallSemaphore.release();
      }
    }

    @Override
    public boolean OutgoingChannelDisconnected(AsyncClientChannel channel) {
      LOG.info("OutgoingChannelDisconnected... ");
      try {
        // explicitly close the channel!
        _channel.close();
      } catch (IOException e) {
      }
      
      _lastIOException = new IOException("Disconnected from slave");
      
      if (_blockingCallSemaphore != null) { 
        _blockingCallSemaphore.release();
      }
      else { 
        failed(_lastIOException);
      }
      return false;
    }
  }
  
  /**
   * Helper Class that encapsulates Block Receiving Logic for Slave Servers  
   * 
   * @author rana
   *
   */
  static class PRValueBlockFileReceiver { 
   
    // the active job id 
    private long _jobId;
    // the fully qualified job storage path ... 
    private File _jobFileLocalPath;
    // immediate shutdown flag ...
    private boolean _immediateShutdown = false;
    
    /**
     * 
     */
    public PRValueBlockFileReceiver(long jobId,File jobFileLocalPath) { 
      _jobId = jobId;
      _jobFileLocalPath = jobFileLocalPath;
     
      startBlockWriter();
    }
    
    /**
     * shutdown the block receiver
     *  either in an orderly manner or immediately
     * @param orderly in an orderly manner (complete queued requests) or immediately
     */
    public void shutdown(boolean orderly) throws IOException { 
      if (_blockWriter != null) { 
        // create a shutdown request ... 
        BlockRequest request = BlockRequest.shutdownRequest();
        // put in queue
        _blockRequestQueue.add(request);
        // if immediate, indicate so 
        _immediateShutdown = !orderly;
        // ok wait for thread to exit ...
        LOG.info("Waiting for BlockWriter Thread Shutdown");
        try {
          _blockWriter.join();
        } catch (InterruptedException e) {
        }
        LOG.info("BlockWriter Thread Exited");
        // ok reset state ...  
        _immediateShutdown = false;
        _blockWriter = null;
      }
    }
    
    File getActiveJobLocalPath() { 
      return _jobFileLocalPath;
    }
    
    long getJobId() { 
      return _jobId;
    }
    
    private static  class BlockRequest<DataType extends RPCStruct,ResultType> { 
      
      enum BlockRequestType { 
        FILE_CREATE,
        BLOCK_WRITE,
        FILE_COMMIT,
        PURGE,
        SHUTDOWN
      }
      
      AsyncContext _context;
      CallbackWithResult<BlockRequest<DataType,ResultType>> _callback;
      DataType _data;
      BlockRequestType _type;
      ResultType _result;
      
      
      public static BlockRequest<FileInfo,Long> createFileRequest(AsyncContext context,FileInfo fileInfo,CallbackWithResult<BlockRequest<FileInfo,Long>> callback)throws IOException {
        return new BlockRequest<FileInfo,Long>(context,BlockRequestType.FILE_CREATE,fileInfo,callback,0L);
      }

      public static BlockRequest<FileInfo,Boolean> commitFileRequest(AsyncContext context,FileInfo fileInfo,CallbackWithResult<BlockRequest<FileInfo,Boolean>> callback)throws IOException {
        return new BlockRequest<FileInfo,Boolean>(context,BlockRequestType.FILE_COMMIT,fileInfo,callback,false);
      }

      public static BlockRequest<BlockTransfer,Boolean> blockTransferRequest(AsyncContext context,BlockTransfer blockInfo,CallbackWithResult<BlockRequest<BlockTransfer,Boolean>> callback)throws IOException {
        return new BlockRequest<BlockTransfer,Boolean>(context,BlockRequestType.BLOCK_WRITE,blockInfo,callback,false);
      }

      public static BlockRequest<NullMessage,Boolean> purgeRequest(AsyncContext context,NullMessage nullMessage,CallbackWithResult<BlockRequest<NullMessage,Boolean>> callback)throws IOException {
        return new BlockRequest<NullMessage,Boolean>(context,BlockRequestType.PURGE,null,callback,false);
      }

      public static BlockRequest<NullMessage,Boolean> shutdownRequest()throws IOException {
        return new BlockRequest<NullMessage,Boolean>(null,BlockRequestType.SHUTDOWN,null,null,false);
      }
      
      public BlockRequest(AsyncContext context,BlockRequestType type, DataType data,CallbackWithResult<BlockRequest<DataType,ResultType>> callback,ResultType defaultResultValue) throws IOException {
        _context = context;
        _type = type;
        _data = data;
        _callback = callback;
        _result = defaultResultValue;
      }

    }
    
    Thread _blockWriter = null;
    LinkedBlockingQueue<BlockRequest> _blockRequestQueue 
        = new LinkedBlockingQueue<BlockRequest>();
    long _lastFileId = 0;
    
    static class ActiveFile {
      
      ActiveFile(File file,RandomAccessFile stream,long fileId) { 
        _file = file;
        _stream = stream;
        _fileId = fileId;
      }
      
      File _file;
      RandomAccessFile _stream;
      long _fileId;
    }
    TreeMap<Long,ActiveFile> 
                            _activeFilesMap = new TreeMap<Long,ActiveFile>();
    void startBlockWriter() { 
      
      _blockWriter = new Thread( new Runnable() {

        @SuppressWarnings("unchecked")
        @Override
        public void run() {
          LOG.info("BlockWriter Thread Running... ");
          try { 
            while (true) { 
              try {
                BlockRequest request = _blockRequestQueue.take();
                
                if (_immediateShutdown || request._type == BlockRequest.BlockRequestType.PURGE || 
                    request._type == BlockRequest.BlockRequestType.SHUTDOWN) {
                  LOG.info("Got Shutdown Or Purge Request... Closing existing connections");
                  purgeOpenFiles();
                
                  if (_immediateShutdown || request._type == BlockRequest.BlockRequestType.SHUTDOWN) { 
                    LOG.info("Received Shutdown Request. Existing Thread");
                    break;
                  }
                }
                else { 
                 
                  if (request._type == BlockRequest.BlockRequestType.FILE_CREATE) {
                    BlockRequest<FileInfo, Long> typedRequest = (BlockRequest<FileInfo, Long>)request;
                    LOG.info("Got Block File Create Request for Path:" + typedRequest._data.getFileName());
                    // create the actual file ... 
                    File basePath = getActiveJobLocalPath();
                    File path     = new File(basePath,typedRequest._data.getFileName());
                    // try to create a file from scratch ...
                    try {
                      RandomAccessFile stream = new RandomAccessFile(path, "rw");
                      ActiveFile activeFile = new ActiveFile(path,stream,++_lastFileId);
                      _activeFilesMap.put(activeFile._fileId, activeFile);
                      typedRequest._result = activeFile._fileId;
                      LOG.info("Created Block File at Path:" + path + " FileId:" + activeFile._fileId);
                      // ok return to caller 
                    } catch (IOException e) {
                      typedRequest._result = 0L;
                      LOG.error("Error Creating Block File:" + path + ":" + CCStringUtils.stringifyException(e));
                    }
                    finally {
                      // initiate callback 
                      typedRequest._callback.execute(typedRequest);
                    }
                  }
                  else if (request._type == BlockRequest.BlockRequestType.FILE_COMMIT) { 
                    BlockRequest<FileInfo, Boolean> typedRequest = (BlockRequest<FileInfo, Boolean>)request;
                    LOG.info("Got Commit Request for FileId::" + typedRequest._data.getFileId());
                    // expect failure 
                    typedRequest._result = false;
                    
                    // try to access the file 
                    try {
                      ActiveFile activeFile = _activeFilesMap.get(typedRequest._data.getFileId());
                      if (activeFile != null) { 
                        LOG.info("Committing File: " + activeFile._file + " Id:" + activeFile._fileId);
                        if (activeFile._stream != null) {
                          try { 
                            activeFile._stream.close();
                            typedRequest._result = true;
                          }
                          catch (IOException e) { 
                            LOG.error(CCStringUtils.stringifyException(e));
                          }
                        }
                        _activeFilesMap.remove(activeFile._fileId);
                      }
                      else { 
                        LOG.error("No Active File Found for Id:" + typedRequest._data.getFileId());
                      }
                    }
                    finally {
                      // initiate callback 
                      typedRequest._callback.execute(typedRequest);
                    }                
                  }
                  else if (request._type == BlockRequest.BlockRequestType.BLOCK_WRITE) {
                    BlockRequest<BlockTransfer, Boolean> typedRequest = (BlockRequest<BlockTransfer, Boolean>)request;
                    
                    LOG.info("Got Block Transfer Request for FileId:" 
                        + typedRequest._data.getFileId()
                        + " ByteCount:" + typedRequest._data.getBlockData().getCount());
                    
                    // expect failure 
                    typedRequest._result = false;
                    
                    // try to access the file 
                    try {
                      ActiveFile activeFile = _activeFilesMap.get(typedRequest._data.getFileId());
                      if (activeFile != null) { 
                        LOG.info("Writing: " + typedRequest._data.getBlockData().getCount() + " Bytes to File: " + activeFile._file + " Id:" + activeFile._fileId);
                        if (activeFile._stream != null) {
                          try { 
                            activeFile._stream.write(typedRequest._data.getBlockData().getReadOnlyBytes(),0,typedRequest._data.getBlockData().getCount());
                            typedRequest._result = true;
                          }
                          catch (IOException e) { 
                            LOG.error(CCStringUtils.stringifyException(e));
                          }
                        }
                        _activeFilesMap.remove(activeFile._fileId);
                      }
                      else { 
                        LOG.error("No Active File Found for Id:" + typedRequest._data.getFileId());
                      }
                    }
                    finally {
                      // initiate callback 
                      typedRequest._callback.execute(typedRequest);
                    }                 
                  }
                }
                
              } catch (InterruptedException e) {
              }
            }
          }
          finally { 
            LOG.info("Block Writer Thread Exiting");
          }
            
        }
      });
      
      _blockWriter.start();
    }
    
    void purgeOpenFiles() { 
      for (ActiveFile file : _activeFilesMap.values()) { 
        if (file._stream != null) { 
          try {
            file._stream.close();
          } catch (IOException e) {
            LOG.error(CCStringUtils.stringifyException(e));
          }
          file._file.delete();
        }
      }
      _activeFilesMap.clear();
    }
    
    
    public void createJobFile(final AsyncContext<FileInfo, FileInfo> rpcContext)
        throws RPCException {
      
      try { 
        if (getJobId() != rpcContext.getInput().getJobId()) { 
          throw new IOException ("Invalid Job Config or Invalid Job Id!");
        }
        
        LOG.info("Got createJobFile RPC. Path:" + rpcContext.getInput().getFileName());
        
        // default to failure status ... 
        rpcContext.setStatus(Status.Error_RequestFailed);

        try {
          BlockRequest request 
            = BlockRequest.createFileRequest(
                rpcContext,
                rpcContext.getInput(),
                new CallbackWithResult<BlockRequest<FileInfo,Long>>() {

                  @Override
                  public void execute(BlockRequest<FileInfo,Long> requestObject) {
                    try {
                      LOG.info("Received callback for createFile:" + requestObject._data.getFileName() + " Result:" + requestObject._result);
                      // ok request was successful ...
                      if (requestObject._result != 0L) { 
                        // write was successful ...
                        rpcContext.getOutput().setFileId(rpcContext.getInput().getFileId());
                        rpcContext.getOutput().setFileId(requestObject._result);
                        rpcContext.setStatus(Status.Success);
                      }
                    }
                    finally { 
                      try {
                        rpcContext.completeRequest();
                      } catch (RPCException e) {
                        LOG.error(CCStringUtils.stringifyException(e));
                      }
                    }
                  }
                });
          
          _blockRequestQueue.put(request);
          
        } catch (IOException e) {
          LOG.error(CCStringUtils.stringifyException(e));
          rpcContext.setStatus(Status.Error_RequestFailed);
          rpcContext.completeRequest();
        } catch (InterruptedException e) {
        }      
      }
      catch (IOException e) { 
        rpcContext.setErrorDesc(CCStringUtils.stringifyException(e));
        LOG.error(rpcContext.getErrorDesc());
        rpcContext.setStatus(Status.Error_RequestFailed);
      }
      finally { 
        rpcContext.completeRequest();
      }
      
    }  
    
    
    public void transferBlock(
        final AsyncContext<BlockTransfer, BlockTransferAck> rpcContext)
        throws RPCException {
      
      LOG.info("Got trasferBlock RPC. FileId:" 
          + rpcContext.getInput().getFileId()
          + " BufferSize:" + rpcContext.getInput().getBlockData().getCount());
      
      try {
        BlockRequest request 
          = BlockRequest.blockTransferRequest(
              rpcContext,
              rpcContext.getInput(),
              new CallbackWithResult<BlockRequest<BlockTransfer,Boolean>>() {

                @Override
                public void execute(BlockRequest<BlockTransfer, Boolean> requestObject) {
                  try {
                    // ok request was successful ...
                    if (requestObject._result == true) { 
                      // write was successful ...
                      rpcContext.getOutput().setFileId(rpcContext.getInput().getFileId());
                      rpcContext.getOutput().setBlockId(rpcContext.getInput().getBlockId());
                      rpcContext.setStatus(Status.Success);
                    }
                    else { 
                      rpcContext.setStatus(Status.Error_RequestFailed);
                    }
                  }
                  finally { 
                    try {
                      rpcContext.completeRequest();
                    } catch (RPCException e) {
                      LOG.error(CCStringUtils.stringifyException(e));
                    }
                  }
                }
              });
        
        _blockRequestQueue.put(request);
        
      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
        rpcContext.setStatus(Status.Error_RequestFailed);
        rpcContext.completeRequest();
      } catch (InterruptedException e) {
      }
    }
    
    public void commitFile(final AsyncContext<FileInfo, NullMessage> rpcContext)
    throws RPCException {
      
      LOG.info("Got commitFile RPC. FileId:" 
          + rpcContext.getInput().getFileId()
          );
      
      try {
        BlockRequest request 
          = BlockRequest.commitFileRequest(
              rpcContext,
              rpcContext.getInput(),
              new CallbackWithResult<BlockRequest<FileInfo,Boolean>>() {

                @Override
                public void execute(BlockRequest<FileInfo, Boolean> requestObject) {
                  
                  try { // ok request was successful ...
                    if (requestObject._result == true) { 
                      // write was successful ...
                      rpcContext.setStatus(Status.Success);
                    }
                    else { 
                      rpcContext.setStatus(Status.Error_RequestFailed);
                    }
                  }
                  finally { 
                    try {
                      rpcContext.completeRequest();
                    } catch (RPCException e) {
                      LOG.error(CCStringUtils.stringifyException(e));
                    }
                  }
                }
              });
        
        _blockRequestQueue.put(request);
        
      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
        rpcContext.setStatus(Status.Error_RequestFailed);
        rpcContext.completeRequest();
      } catch (InterruptedException e) {
      }
      
    }    
  }
  
  
  public static class PRValueBlockWriterAndReceiverTester 
    extends Server implements PageRankSlave ,AsyncServerChannel.ConnectionCallback {

    AsyncServerChannel _channel;
    EventLoop _eventLoop;
    PRValueBlockFileReceiver _receiver;
    File _jobLocalPath;
    
    PRValueBlockWriterAndReceiverTester(EventLoop eventLoop, int instanceId,int portToUse) throws IOException { 
      
      _eventLoop = eventLoop;
      
      _jobLocalPath = new File("/tmp/prvalue_receiver_test/" + instanceId);
      
      InetSocketAddress localAddress = new InetSocketAddress("localhost",0);

      InetSocketAddress address = new InetSocketAddress("localhost",portToUse);
      
      _channel = new AsyncServerChannel(this, _eventLoop, address,this);
      
      registerService(_channel,PageRankSlave.spec);
      
      FileUtils.recursivelyDeleteFile(_jobLocalPath);
      
      _jobLocalPath.mkdirs();
      
      start();
      
      // start the block receiver.... 
      _receiver = new PRValueBlockFileReceiver(1,_jobLocalPath);      
      
    }
    
    void shutdown() { 
      LOG.info("Doing orderly shutdown on receiver");
      try {
        _receiver.shutdown(true);
      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
      }
      LOG.info("Closing Channel");
      stop();
    }
    
    
    public static void runTest() {
      EventLoop eventLoop = new EventLoop();
      eventLoop.start();
      try {
        // instantiate tester ... 
        LOG.info("Starting Servers");
        PRValueBlockWriterAndReceiverTester tester1 = new PRValueBlockWriterAndReceiverTester(eventLoop,0,9000);
        PRValueBlockWriterAndReceiverTester tester2 = new PRValueBlockWriterAndReceiverTester(eventLoop,1,9001);
        PRValueBlockWriterAndReceiverTester tester3 = new PRValueBlockWriterAndReceiverTester(eventLoop,2,9002);
        
        Vector<InetSocketAddress> addressList = new Vector<InetSocketAddress>();
        
        addressList.add(new InetSocketAddress("127.0.0.1",9000));
        addressList.add(new InetSocketAddress("127.0.0.1",9001));
        addressList.add(new InetSocketAddress("127.0.0.1",9002));
        
        Configuration conf = new Configuration();
        
        conf.addResource("core-site.xml");
        conf.addResource("hdfs-site.xml");
        conf.addResource("mapred-site.xml");
        
        LOG.info("Creating Multiplexer");
        // instantiate block writer ... 
        PRValueMultiplexer multiplexer = new PRValueMultiplexer(conf, 1, 0, addressList, 0);
        
        URLFPV2 source = URLUtils.getURLFPV2FromURL("http://source.com/");
        URLFPV2 dest   = URLUtils.getURLFPV2FromURL("http://dest.com/");
        
        LOG.info("Writing Values");
        for (int i=0;i<10000;++i) { 
          multiplexer.writePRValue(i % 3, source, dest, 1.0f);
        }
        
        LOG.info("Waiting on Close");
        multiplexer.close(false);
        
        // shutdown writers
        LOG.info("Shutting Down Receiver 1");
        tester1.shutdown();
        LOG.info("Shutting Down Receiver 2");
        tester2.shutdown();
        LOG.info("Shutting Down Receiver 3");
        tester3.shutdown();
        
        eventLoop.stop();
        
      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
      }
    }
    
    @Override
    public void beginPageRank(
        AsyncContext<BeginPageRankInfo, SlaveStatus> rpcContext)
        throws RPCException {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void checkpoint(AsyncContext<CheckpointInfo, SlaveStatus> rpcContext)
        throws RPCException {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void commitFile(AsyncContext<FileInfo, NullMessage> rpcContext)
        throws RPCException {
      LOG.info("TestServer: Recevied commitFile Cmd");
      _receiver.commitFile(rpcContext);
      
    }

    @Override
    public void createJobFile(AsyncContext<FileInfo, FileInfo> rpcContext)
        throws RPCException {
      LOG.info("TestServer: Recevied createJobFile Cmd");
      _receiver.createJobFile(rpcContext);
      
    }

    @Override
    public void deleteFile(AsyncContext<FileInfo, NullMessage> rpcContext)
        throws RPCException {
      
      
    }

    @Override
    public void doIteration(AsyncContext<IterationInfo, SlaveStatus> rpcContext)
        throws RPCException {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void endPageRank(AsyncContext<NullMessage, SlaveStatus> rpcContext)
        throws RPCException {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void heartbeat(AsyncContext<NullMessage, SlaveStatus> rpcContext)
        throws RPCException {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void initialize(AsyncContext<BaseConfig, SlaveStatus> rpcContext)
        throws RPCException {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void transferBlock(
        AsyncContext<BlockTransfer, BlockTransferAck> rpcContext)
        throws RPCException {
      LOG.info("TestServer: Recevied transferBlock Cmd");
      _receiver.transferBlock(rpcContext);
    }

    @Override
    public void IncomingClientConnected(AsyncClientChannel channel) {
      LOG.info("TestServer IncomingClient Connected");
    }

    @Override
    public void IncomingClientDisconnected(AsyncClientChannel channel) {
      LOG.info("TestServer IncomingClient Disconnected");
    }     
  }
  
  
  private static FileSystem buildDistributionOutputStreamVector(boolean useSequenceFile,String fileNamePrefix,File localOutputPath,String remoteOutputPath, int myNodeIndex, int nodeCount,Vector<PRValueOutputStream> outputStreamVector) { 
    
    Configuration conf = new Configuration(CrawlEnvironment.getHadoopConfig());
    
    conf.setInt("dfs.socket.timeout",240000);
    conf.setInt("io.file.buffer.size", 4096 * 20);
    
    DistributedFileSystem hdfs = new DistributedFileSystem();
    
    try {
    	
      
      hdfs.initialize(FileSystem.getDefaultUri(conf), conf);
    	
      for (int i=0;i<nodeCount;++i) {
      	
        // create output filename 
        String fileName = fileNamePrefix + "-" + NUMBER_FORMAT.format(i);
        // create stream (local or remote stream, depending on i) 
        // remote path 
        Path remotePath = new Path(remoteOutputPath,fileName);
        // remove file
        CrawlEnvironment.getDefaultFileSystem().delete(remotePath,false);
        if (useSequenceFile) { 
          // recreate it ... 
          outputStreamVector.add(new PRSequenceFileOutputStream(conf,CrawlEnvironment.getDefaultFileSystem(),remotePath));
        }
        else { 
          // recreate it ... 
          outputStreamVector.add(new PROldValueOutputStream(CrawlEnvironment.getDefaultFileSystem(),remotePath));
        }
      }
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      for (PRValueOutputStream streamInfo : outputStreamVector) { 
        try { 
          if (streamInfo != null) {
            streamInfo.close(true);
          }
        }
        catch (IOException e2) { 
          LOG.error(CCStringUtils.stringifyException(e2));
        }
        outputStreamVector.clear();
      }
    }
    
    return hdfs;
  }
  
  
  public static Vector<Path> buildCalculationInputStreamVector(File localOutputPath,String remoteOutputPath, int myNodeIndex, int nodeCount, int iterationNumber) { 
    
    Vector<Path> vector = new Vector<Path>();
    
    for (int i=0;i<nodeCount;++i) { 
      // create output filename 
      String fileName = getOutlinksBaseName(i,iterationNumber) + "-" + NUMBER_FORMAT.format(myNodeIndex);
      // create stream (local or remote stream, depending on i) 
      // remote path 
      Path remotePath = new Path(remoteOutputPath,fileName);
      LOG.info("Adding Path:" + remotePath + " For Index:" + i);
      // 
      vector.add(remotePath);
    }
    return vector;
  }
  
  public static class SourceAndRank implements Comparable<SourceAndRank> { 
  	
  	SourceAndRank(URLFPV2 fingerprint,float prValue) { 
  		source.setDomainHash(fingerprint.getDomainHash());
  		source.setRootDomainHash(fingerprint.getRootDomainHash());
  		source.setUrlHash(fingerprint.getUrlHash());
  		rank  = prValue;
  	}
  	
  	URLFPV2 source = new URLFPV2();
  	float 	rank;
  	
		@Override
    public int compareTo(SourceAndRank o) {
			return source.compareTo(o.source);
		}
		
  }
  
  public static class DomainHashAndPRValue implements Comparable<DomainHashAndPRValue> {
  	
  	public DomainHashAndPRValue(long domainHash,float prValue) { 
  		_domainHash = domainHash;
  		_accumulator = prValue;
  		_inputs = 1;
  	}
  	
  	public void updatePRValue(float newPRValue) { 
  		_accumulator += newPRValue;
  		_inputs++;
  	}
  	
  	public float averageValue() { 
  		return _accumulator / (float)_inputs;
  	}
  	
  	
  	public long 	_domainHash;
  	public float 	_accumulator;
  	public int 		_inputs;

		@Override
    public int compareTo(DomainHashAndPRValue o) {
	    return ((Long)_domainHash).compareTo(o._domainHash);
    }
  	
  }
  
  public static class RootDomain { 
  	
  	public RootDomain() { 
  		
  	}
  	
  	public HashMap<Long,DomainHashAndPRValue> subDomains = new HashMap<Long,DomainHashAndPRValue>();
  }
 
  public static class TargetAndSources { 
  	URLFPV2 target = new URLFPV2();
  	HashMap<Long,RootDomain> sources = new HashMap<Long,RootDomain>();
  	
  }
  
  public static class TargetSourceAndRank {
  	
  	public boolean readFromStream(DataInputStream inputStream) throws IOException {
  		if (inputStream.available() != 0) { 
	      target.setDomainHash(inputStream.readLong());
	      target.setUrlHash(inputStream.readLong());
	      source.setRootDomainHash(inputStream.readLong());
	      source.setDomainHash(inputStream.readLong());
	      source.setUrlHash(inputStream.readLong());
	      prValue = inputStream.readFloat();
	      isValid = true;
  		}
  		else { 
  			isValid = false;
  		}
  		return isValid;
  	}
  	
  	@Override
  	public String toString() {
  		return "Target DomainHash:" + target.getDomainHash() + " FP:" + target.getUrlHash() + " Source DomainHash:" + source.getDomainHash() + " FP:" + source.getUrlHash();
  	}
  	
  	boolean isValid = false;
  	URLFPV2 target = new URLFPV2();
  	URLFPV2 source = new URLFPV2();
  	float   prValue;
  }
  
  static interface PRInputSource { 
    public TargetSourceAndRank next() throws IOException;
    public TargetSourceAndRank last();
    public void close() throws IOException;
    public long getSize() throws IOException;
  }
  
  
  static class PRSequenceFileInputSource implements PRInputSource {

    SequenceFile.Reader _reader;
    public Path _path;
    public TargetSourceAndRank      _currentValue = new TargetSourceAndRank();
    DataInputBuffer _inputStream = new DataInputBuffer();
    FlexBuffer _buffer = new FlexBuffer();
    long _totalLength = 0;
    
    
    public PRSequenceFileInputSource(Configuration conf,FileSystem fs,Path path,SortedPRInputReader reader)throws IOException { 
      _path = path;
      _reader = new SequenceFile.Reader(fs, path, conf);
      FileStatus fileStatus = fs.getFileStatus(_path);
      _totalLength = 0L;
      if (fileStatus != null) { 
        _totalLength = fileStatus.getLen();
      }
    }
    
    @Override
    public void close() throws IOException {
      if (_reader != null) { 
        _reader.close();
        _reader = null;
      }
    }

    @Override
    public TargetSourceAndRank last(){
      return _currentValue;
    }

    @Override
    public TargetSourceAndRank next() throws IOException {
      _currentValue = null;
      if (_reader.next(_buffer, NullWritable.get())) { 
        _inputStream.reset(_buffer.get(), _buffer.getCount());
        _currentValue = new TargetSourceAndRank();
        _currentValue.readFromStream(_inputStream);        
      }
      return _currentValue;
    }

    @Override
    public long getSize() throws IOException {
      return _totalLength;
    } 
   
    
  }
  static class PROldInputSource implements PRInputSource { 
  	
  	SortedPRInputReader _reader = null;
  	long _bytesTotal;
  	
  	public PROldInputSource(Path path,SortedPRInputReader reader) throws IOException { 
      _path = path;
      _istream = CrawlEnvironment.getDefaultFileSystem().open(_path);
      _bytesTotal = CrawlEnvironment.getDefaultFileSystem().getFileStatus(_path).getLen();

      // wrap the stream so that we can monitor progress ...
      _istream = new FilterInputStream(_istream) {

        @Override
        public int read() throws IOException {
          _reader._totalBytesRead += 1;
          return this.in.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
          int bytesRead = this.in.read(b, off, len);
          _reader._totalBytesRead += bytesRead;
          return bytesRead;
        }

        @Override
        public long skip(long n) throws IOException {
          long bytesSkipped = this.in.skip(n);
          _reader._totalBytesRead += bytesSkipped;
          return bytesSkipped;
        }
      };

      _stream = new DataInputStream(_istream);
      _reader = reader;
    }

  	@Override
  	public TargetSourceAndRank next() throws IOException {
  		
  		_currentValue = null;
  		if (_stream != null && _stream.available() != 0) {
  			_currentValue = new TargetSourceAndRank();
  			// reset bytes read counter
	  		_currentValue.readFromStream(_stream);
  		}
  		return _currentValue;
  	}
  	
  	@Override
  	public TargetSourceAndRank last() {
  	  return _currentValue;
  	}
  	
  	@Override
  	public void close() throws IOException { 
  		if (_istream != null) { 
  			_istream.close();
  			_istream = null;
  			_stream = null;
  		}
  	}
  	
  	public Path _path;
  	public InputStream _istream;
  	public DataInputStream _stream;
  	public TargetSourceAndRank		_currentValue = new TargetSourceAndRank();

    @Override
    public long getSize() throws IOException {
      return _bytesTotal;
    }
  }
  
  
  
  public static class SortedPRInputReader { 
  	
  	PRInputSource _inputs[] = null;
  	int _validStreams = 0;
  	long _totalBytesToRead = 0;
  	long _totalBytesRead 	 = 0;
  	
  	public SortedPRInputReader(Configuration conf,FileSystem fs,Vector<Path> streams,boolean useSequenceFile) throws IOException { 
  		
  		try { 
  			LOG.info("PRInputReader: Allocating Stream Array of Size:" + streams.size());
	  		//ok allocate an array up to stream vector size ... 
	  		_inputs = new PRInputSource[streams.size()];
	
	  		// now, open streams 
	  		for (Path streamInfo : streams) {
	  		  if (!useSequenceFile) { 
	  		    _inputs[_validStreams] = new PROldInputSource(streamInfo,this);
	  		  }
	  		  else { 
	  		    _inputs[_validStreams] = new PRSequenceFileInputSource(conf,fs,streamInfo,this);
	  		  }
	  			// advance to first item 
	  			if (_inputs[_validStreams].next() == null) {
	  				LOG.error("PRInputReader: Stream At Index:" + _validStreams + " contains zero entries!");
	  				_inputs[_validStreams].close();
	  			}
	  			else {
	  				LOG.info("PRInputReader: Stream :" + _validStreams + " First Item:" + _inputs[_validStreams].last().toString() );
	  				_totalBytesToRead += _inputs[_validStreams].getSize();
	  				_validStreams++;
	  			}
	  		}
	  		// lastly sort streams
	  		sortStreams();
	  		
	  		LOG.info("Sorted First Item:" + _inputs[0].last().toString());
  		}
  		catch (IOException e) { 
  			LOG.error(CCStringUtils.stringifyException(e));
  			close();
  			throw e;
  		}
  	}
  	
  	void close() { 
  		for (int i=0;i<_validStreams;++i) { 
  			try { 
  				_inputs[i].close();
  			}
  			catch (IOException e) { 
  				LOG.error(CCStringUtils.stringifyException(e));
  			}
  			_inputs[i] = null;
  		}
  		_validStreams = 0;
  	}
  	static final int MAX_ROOT_DOMAIN_SOURCES_PER_TARGET = 100000;
  	static final int MAX_SUBDOMAIN_SOURCES_PER_ROOTDOMAIN = 500;
  	static DomainHashAndPRValue addSourceToTarget(TargetAndSources tgtAndSources,TargetSourceAndRank source) {
  		
  		RootDomain rootDomain  = tgtAndSources.sources.get(source.source.getRootDomainHash());
  		if (rootDomain == null) {
  			if (tgtAndSources.sources.size() < MAX_ROOT_DOMAIN_SOURCES_PER_TARGET) { 
  				rootDomain = new RootDomain();
  				tgtAndSources.sources.put(source.source.getRootDomainHash(), rootDomain);
  			}
  		}
  		DomainHashAndPRValue hashAndPRValue = (rootDomain != null) ? rootDomain.subDomains.get(source.source.getDomainHash()) : null;
  		if (hashAndPRValue == null) { 
  			hashAndPRValue = new DomainHashAndPRValue(source.source.getDomainHash(), source.prValue);
    		if (rootDomain != null && rootDomain.subDomains.size() < MAX_SUBDOMAIN_SOURCES_PER_ROOTDOMAIN) { 
    			rootDomain.subDomains.put(source.source.getDomainHash(),hashAndPRValue);
    		}
  		}
  		else { 
  			hashAndPRValue.updatePRValue(source.prValue);
  		}
  		return hashAndPRValue;
  	}
  	
  	void sortStreams() { 
  		Arrays.sort(_inputs,0,_validStreams,new Comparator<PRInputSource>() {

				@Override
        public int compare(PRInputSource o1, PRInputSource o2) {
	        return o1.last().target.compareTo(o2.last().target);
        }
			});
  	}
  	
  	// collect next valid target and all related sources 
  	TargetAndSources readNextTarget() throws IOException {
  		
  		if (_validStreams != 0) { 
  			TargetAndSources target = new TargetAndSources();
  			
  			target.target.setDomainHash(_inputs[0].last().target.getDomainHash());
  			target.target.setUrlHash(_inputs[0].last().target.getUrlHash());
  			
  			//LOG.info("readNextTarget - target is:" + target.target.getDomainHash() + ":" + target.target.getUrlHash());
  			//LOG.info("readNextTarget - source is:" + _inputs[0].last().source.getDomainHash() + ":" + _inputs[0].last().source.getUrlHash());
  			DomainHashAndPRValue lastValue = addSourceToTarget(target,_inputs[0].last());
  			
  			// advance input zero 
  			_inputs[0].next();
  			
  			// ok enter a loop and collect all sources for current target ... 
  			for (int streamIdx=0;streamIdx<_validStreams;) { 
  				if (_inputs[streamIdx].last() == null || _inputs[streamIdx].last().target.compareTo(target.target) != 0) { 
  					streamIdx++;
  				}
  				else {
  					if (lastValue != null && lastValue._domainHash == _inputs[streamIdx].last().source.getDomainHash()) { 
  						lastValue.updatePRValue(_inputs[streamIdx].last().prValue);
  					}
  					else { 
  						lastValue = addSourceToTarget(target,_inputs[streamIdx].last());
  					}
  	  			// advance current stream ... 
  	  			_inputs[streamIdx].next();
  				}
  			}
  			
  			// ok now collect remaining valid streams 
  			int newValidStreamCount=0;
  			for (int currStreamIdx=0;currStreamIdx<_validStreams;++currStreamIdx) { 
  				if (_inputs[currStreamIdx].last() != null) { 
  					_inputs[newValidStreamCount++] = _inputs[currStreamIdx];
  				}
  				else {
  					// close the stream ... 
  					_inputs[currStreamIdx].close();
  					// null it out ... 
  					_inputs[currStreamIdx] = null;
  				}
  			}
  			// resset valid stream count 
  			_validStreams = newValidStreamCount;
  			
  			// ok now sort streams ...
  			if (_validStreams != 0) { 
  				sortStreams();
  			}
  			
  			return target;
  		}
  		else { 
  			return null;
  		}
  	}
  }
  
  public static class CalculateRankQueueItem {
  	
  	public CalculateRankQueueItem(TargetAndSources next) {
  		_e = null;
  		_next = next;
  	}
  	
  	public CalculateRankQueueItem(IOException e) {
  		_e = e;
  		_next = null;
  	}

  	public CalculateRankQueueItem() {
  		_e = null;
  		_next = null;
  	}
  	
  	
  	public TargetAndSources _next;
  	public IOException _e;
  }
  
  public static void calculateRank(final Configuration conf,final FileSystem fs,final PRValueMap valueMap, final File jobLocalDir,final String jobWorkPath,final int nodeIndex, final int slaveCount, final int iterationNumber,final SuperDomainFilter superDomainFilter,final ProgressAndCancelCheckCallback progressAndCancelCallback) throws IOException {
    
  	final LinkedBlockingQueue<CalculateRankQueueItem> readAheadQueue = new LinkedBlockingQueue<CalculateRankQueueItem>(20);

    // build stream vector ... 
    Vector<Path> streamVector = buildCalculationInputStreamVector(jobLocalDir,jobWorkPath,nodeIndex,slaveCount,iterationNumber);
    
    // construct a reader ... 
    final SortedPRInputReader reader = new SortedPRInputReader(conf,fs,streamVector,true);

  	Thread readerThread = new Thread(new Runnable() { 
  	
			@Override
      public void run() {
  		
		    IOException exceptionOut = null;
		    try {   
		    	
		    
		    	TargetAndSources target = null;
		    	
		    	while ((target = reader.readNextTarget()) != null) {
		    		try {
	            readAheadQueue.put(new CalculateRankQueueItem(target));
            } catch (InterruptedException e) {
            }
		    	}
		    }
		    catch (IOException e) { 
		    	LOG.error(CCStringUtils.stringifyException(e));
		    	exceptionOut = e;
		    }
		    finally  { 
		    	if (reader != null) { 
		    		reader.close();
		    	}
		    }
	    	try {
	    		readAheadQueue.put(new CalculateRankQueueItem(exceptionOut));
        } catch (InterruptedException e1) {
        }
		    
      }
  	});

  	readerThread.start();
  	
  	int failedUpdates = 0;
  	int totalUpdates = 0;
  	long iterationStart = System.currentTimeMillis();
  	boolean cancelled = false;
  	
  	while (!cancelled) { 
  		
  		CalculateRankQueueItem queueItem = null;
      try {
	      queueItem = readAheadQueue.take();
      } catch (InterruptedException e) {
      }
  		
  		if (queueItem._next != null) { 
				totalUpdates++;
				//LOG.info("Target: DomainHash:" + target.target.getDomainHash() + " URLHash:" + target.target.getUrlHash() + " ShardIdx:" + ((target.target.hashCode() & Integer.MAX_VALUE) % CrawlEnvironment.PR_NUMSLAVES)); 
		    // now accumulate rank from stream into value map 
		    if (!accumulateRank(valueMap,queueItem._next,superDomainFilter)) { 
		    	failedUpdates++;
		    	LOG.error("**TotalUpdates:" + totalUpdates + " Failed Updates:" + failedUpdates);
		    }
  		
		    if ((totalUpdates + failedUpdates) % 10000 == 0) {
		      
		    	float percentComplete = (float) reader._totalBytesRead / (float)reader._totalBytesToRead;
		    	if (progressAndCancelCallback  != null) { 
		    		cancelled = progressAndCancelCallback.updateProgress(percentComplete);
		    		if (cancelled) { 
		    			LOG.info("Cancel check callback returned true");		    			
		    		}
		    	}
		    	
		      long timeEnd = System.currentTimeMillis();
		      int milliseconds = (int)(timeEnd - iterationStart);
		      
		      //LOG.info("Accumulate PR for 10000 Items Took:" + milliseconds + " Milliseconds QueueSize:" + readAheadQueue.size());
		      
		      iterationStart = System.currentTimeMillis();
		    }
  		}
  		else { 
  			if (queueItem._e != null) { 
  				LOG.error(CCStringUtils.stringifyException(queueItem._e));
  				throw queueItem._e;
  			}
  			else { 
			    // now finally pagerank value in value map ... 
			    valueMap.finalizePageRank();
  			}
  			break;
  		}
  	}
  	try {
	    readerThread.join();
    } catch (InterruptedException e) {
    }
  }
  
  
  private static boolean accumulateRank(PRValueMap valueMap, TargetAndSources target,SuperDomainFilter superDomainFilter) throws IOException { 

    float rank = 0.0f;
    //LOG.info("Accumulating Rank for DomainHash:" + target.target.getDomainHash() + " URLFP:" + target.target.getUrlHash());
    for (Map.Entry<Long, RootDomain> entry : target.sources.entrySet()) {
    	
    	// ok first figure out if this is a super domain 
    	boolean rootIsSuperDomain = (superDomainFilter != null && superDomainFilter.filterItemByHashIdV2(entry.getKey()) == FilterResult.Filter_Accept);
    	
    	RootDomain rootDomain = entry.getValue();
    	
    	if (!rootIsSuperDomain) {
    		
	    	float accumulator = 0.0f;
	    	
    		/*
	    	if (rootDomain.subDomains.size() > 1) { 
    			LOG.info("Non-Super-Domain:" + entry.getKey() + " has " + rootDomain.subDomains.size() + " subdomains");
    		}
    		*/	    	
	    	int subDomainsIterated = 0;
    		for (DomainHashAndPRValue source : rootDomain.subDomains.values()) {
    			++subDomainsIterated;
	    		/*
    			if (rootDomain.subDomains.size() > 1) {
	    			LOG.info("Taking Max Between CurrentValue:" +  maxSourceValue + " and current SubDomain:" + source._prValue);
	    		}
	    		*/
	    		accumulator += source.averageValue();
	    		if (subDomainsIterated > 100) 
	    			break;
	    	}
    		if (subDomainsIterated != 0) { 
    			rank += accumulator / (float) subDomainsIterated;
    		}
    	}
    	else { 
    		
    		/*
    		if (rootDomain.subDomains.size() > 1) { 
    			LOG.info("Super-Domain:" + entry.getKey() + " has " + rootDomain.subDomains.size() + " subdomains");
    		}
    		*/
    		// ok walk items in collection (which are sorted by domain id)
    		for (DomainHashAndPRValue source : rootDomain.subDomains.values()) {
    			/*
    			if (rootDomain.subDomains.size() > 1) {
    				LOG.info("Adding SubDomain:" + source._domainHash + " value:" + source._prValue + " to existing value:" + rank);
    			}
    			*/
    			rank += source.averageValue();
				}
    	}
    }
    try { 
    	// update page rank for item in map 
    	valueMap.addPRValue(target.target, rank);
    	return true;
    }
    catch (IOException e) { 
    	return false;
    }
  }
  
  private static class OutlinkItem {
    
    public OutlinkItem() { 
    	targetFingerprint = new URLFPV2();
    	sourceFingerprint = new URLFPV2();
    }
    
    public OutlinkItem(IOException e) { 
      error = e;
    }
    
    public URLFPV2 targetFingerprint = null;
    public URLFPV2 sourceFingerprint = null;
    public int   urlCount = 0;
    public IOException error = null;
  }
  
  public interface ProgressAndCancelCheckCallback { 
    boolean updateProgress(float percentComplete);
  }
  
  
  public static void distributeRank(final PRValueMap valueMap,final Path outlinksFile,final boolean outlinksIsRemote,File localOutputDir,String remoteOutputDir,int thisNodeIdx,int nodeCount,int iterationNumber,final ProgressAndCancelCheckCallback progressCallback)throws IOException { 

    final Configuration conf = CrawlEnvironment.getHadoopConfig();
    
    Vector<PRValueOutputStream> outputStreamVector = new Vector<PRValueOutputStream>();

    // allocate a queue ... 
    final LinkedBlockingQueue<OutlinkItem> queue = new LinkedBlockingQueue<OutlinkItem>(20000);
    
    try { 
      
      // start the loader thread ... 
      Thread loaderThread = new Thread( new Runnable() {

      	final BytesWritable key= new BytesWritable();
      	final BytesWritable value = new BytesWritable();
      	
      	final DataInputBuffer keyStream = new DataInputBuffer();
      	final DataInputBuffer valueStream = new DataInputBuffer();
      	
        @Override
        public void run() {
          LOG.info("Opening Outlinks File at:" + outlinksFile);
          SequenceFile.Reader reader = null;
          try {
          	
          	
            FileSystem fsForOutlinksFile = null;
            if (outlinksIsRemote) { 
            	fsForOutlinksFile = CrawlEnvironment.getDefaultFileSystem();
            }
            else { 
            	fsForOutlinksFile = FileSystem.getLocal(conf);
            }

            FileStatus outlinksFileStatus = fsForOutlinksFile.getFileStatus(outlinksFile);
            long bytesToReadTotal = (outlinksFileStatus != null) ? outlinksFileStatus.getLen() : 0;
            
            reader = new SequenceFile.Reader(fsForOutlinksFile,outlinksFile,conf);
            OutlinkItem item = new OutlinkItem();
            int itemCount = 0;
            boolean isCancelled = false;
            while (!isCancelled && reader.next(key,value)) {
            	
            	keyStream.reset(key.getBytes(),0,key.getLength());
            	valueStream.reset(value.getBytes(),0,value.getLength());
            	
            	//populate item from data 
            	readURLFPFromStream(keyStream, item.targetFingerprint);
            	item.urlCount = readURLFPAndCountFromStream(valueStream, item.sourceFingerprint);
            	
              try {
                long blockTimeStart = System.currentTimeMillis();
                queue.put(item);
                long blockTimeEnd = System.currentTimeMillis();
              } catch (InterruptedException e) {
              }
              item = new OutlinkItem();
              
              if (itemCount++ %10000 == 0 && progressCallback != null) {
              	
              	float percentComplete = (float)reader.getPosition() / (float)bytesToReadTotal;
                if (progressCallback.updateProgress(percentComplete)) { 
                  LOG.info("Cancel check callback returned true.Cancelling outlink item load");
                  isCancelled = true;
                }
              }
            }
            item.sourceFingerprint = null;
            item.targetFingerprint = null;
            
            // add empty item 
            try {
              if (!isCancelled) { 
                queue.put(item);
              }
              else { 
                queue.put(new OutlinkItem(new IOException("Operation Cancelled")));
              }
            } catch (InterruptedException e) {
            }
            
          }
          catch (IOException e) {
            // add error item to queue.
            try {
              queue.put(new OutlinkItem(e));
            } catch (InterruptedException e1) {
            }
          }
          finally {
            if (reader != null)
              try {
                reader.close();
              } catch (IOException e) {
              }
          }
        } 
        
      });
      
      loaderThread.start();
      
      // first things first ... initialize output stream vector
      FileSystem fileSystem = buildDistributionOutputStreamVector(true,getOutlinksBaseName(thisNodeIdx,iterationNumber),localOutputDir,remoteOutputDir,thisNodeIdx,nodeCount,outputStreamVector);
      
      try { 
	      // open outlinks file .
	      LOG.info("Iterating Items in Outlinks File and Writing Test Value");
	      
	      int itemCount = 0;
	      int totalOutlinkCount = 0;
	      int iterationOutlinkCount = 0;
	      long iterationStart = System.currentTimeMillis();
	      long timeStart = iterationStart;
	      
	      boolean done = false;
	      
	      ArrayList<OutlinkItem> items = new ArrayList<OutlinkItem>();
	      // start iterating outlinks 
	      while(!done) {
	        
	        //OutlinkItem item = null;
	        
	        //try {
	          long waitTimeStart = System.currentTimeMillis();
	          queue.drainTo(items);
	          long waitTimeEnd = System.currentTimeMillis();
	        //} catch (InterruptedException e) {
	        //}
	        
	        for (OutlinkItem item : items) { 
		        if (item.error !=  null) {
		          LOG.info("Loader Thread Returned Error:" + CCStringUtils.stringifyException(item.error));
		          throw item.error;
		        }
		        else if (item.sourceFingerprint == null) { 
		          LOG.info("Loader Thread Indicated EOF via emtpy item");
		          done = true;
		        }
		        else { 
		          ++itemCount;
		          
		          /*
		          LOG.info("SourceFP-DomainHash:" + item.sourceFingerprint.getDomainHash() + " URLHash:" + item.sourceFingerprint.getUrlHash() 
		          		+ " PartitionIdx:" + ((item.sourceFingerprint.hashCode() & Integer.MAX_VALUE) % CrawlEnvironment.PR_NUMSLAVES) );
		          */
		          		
		          
		          // now get pr value for fingerprint (random seek in memory here!!!)
		          float prValue = valueMap.getPRValue(item.sourceFingerprint) / (float) Math.max(item.urlCount,1);
		          
		          // write value out 
		          int nodeIndex = (item.targetFingerprint.hashCode() & Integer.MAX_VALUE) % nodeCount;
		          outputStreamVector.get(nodeIndex).writePRValue(item.targetFingerprint,item.sourceFingerprint,prValue);
		          
		          if (itemCount % 10000 == 0) {
		            
		            long timeEnd = System.currentTimeMillis();
		            int milliseconds = (int)(timeEnd - iterationStart);
		            
		            LOG.info("Distribute PR for 10000 Items with:" + iterationOutlinkCount + " Outlinks Took:" + milliseconds + " Milliseconds" + " QueueCount:" + queue.size() );
		            
		            iterationStart = System.currentTimeMillis();
		            totalOutlinkCount += iterationOutlinkCount;
		            iterationOutlinkCount = 0;
		          }
		          
		        }
	        }
	        items.clear();
	      }
	      
	      totalOutlinkCount += iterationOutlinkCount;
	      
	      LOG.info("Distribute Finished for a total of:" + itemCount + " Items with:" + totalOutlinkCount + " Outlinks Took:" + (System.currentTimeMillis() - timeStart) + " Milliseconds" );
	      
	      LOG.info("Waiting for Loader Thread to Die");
	      try {
	        loaderThread.join();
	      } catch (InterruptedException e) {
	      }
	      LOG.info("Loader Thread Died - Moving on...");
      }
      finally {
      	
        for (PRValueOutputStream info : outputStreamVector) {
        	
        	if (info != null) { 
        	  info.close(false);
        	}
        }
      	
      	if (fileSystem != null) { 
      		fileSystem.close();
      	}
      }
    }
    catch (IOException e) { 
      LOG.error("Exception caught while distributing outlinks:" + CCStringUtils.stringifyException(e));
      throw e;
    }
  }
  
  @Test
  public void testname() throws Exception {
    int array[] = { 2,3,5,7,10 };
    System.out.println("searching for 1 returned:" + findPos(array,1));
    System.out.println("searching for 2 returned:" + findPos(array,2));
    System.out.println("searching for 11 returned:" + findPos(array,11));
    System.out.println("searching for 8 returned:" + findPos(array,8));
  }
  public static void main(String[] args) {

    LOG.info("Initializing Hadoop Config");

    Configuration conf = new Configuration();
    
    conf.addResource("nutch-default.xml");
    conf.addResource("nutch-site.xml");
    conf.addResource("core-site.xml");
    conf.addResource("hdfs-site.xml");
    conf.addResource("mapred-site.xml");
    
    CrawlEnvironment.setHadoopConfig(conf);
    CrawlEnvironment.setDefaultHadoopFSURI("hdfs://ccn02:9000/");
    
    if (args[0].equals("PRValueRW")) { 
      runPRValueReadWriteTest(args);
    }
    else if (args[0].equals("IDRead")) { 
      runIDReadBenchmark(args);
    }
    else if (args[0].equals("DRank")) { 
      runDistributeRankBenchmark(args);
    }
    else if (args[0].equals("ARank")) { 
      runAccumulateRankBechmark(args);
    }
    else if (args[0].equals("BlockFileRcv")) { 
      LOG.info("Running BlockFileReceiver test");
      runBlockFileReceiverTest();
    }
  }
  
  private static void runBlockFileReceiverTest() { 
    PRValueBlockWriterAndReceiverTester.runTest();
  }
  
  private static void runIDReadBenchmark(String[] args) { 
    File idsFile = new File(args[1]);
    
    URLFPV2 fingerPrint = new URLFPV2();
    
    LOG.info("Opening ID File at path:" + idsFile.getAbsolutePath());
    
    RandomAccessFile stream = null;
    
    try { 
      
      stream = new RandomAccessFile(idsFile,"r");
      long length = stream.length();
      
      int idCount = 0;
      long totalStartTime = System.currentTimeMillis();
      long snapshotTime   = System.currentTimeMillis();
      
      boolean error = false;
      
      while (!error) { 
        fingerPrint.readFields(stream);
        ++idCount;
        
        if (idCount % 10000 == 0) { 
          LOG.info("Read 10000 ids in:" + (System.currentTimeMillis() - snapshotTime) + " MS");
          snapshotTime = System.currentTimeMillis();
        }
      }
      
      LOG.info("Completed Reading a Total of:" + idCount + " IDs in:" + (System.currentTimeMillis() - totalStartTime) + " MS");
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
    }
    finally { 
      if (stream != null) { 
        try {
          stream.close();
        } catch (IOException e) {
        }
      }
    }
  }
  
  
  
  private static void runPRValueReadWriteTest(String[] args) { 
    
    Configuration conf = CrawlEnvironment.getHadoopConfig();
    
    File valueFile = new File(args[1]);
    File rangeFile = new File(args[2]);
    File outlinksFile = new File(args[3]);
    
    PRValueMap valueMap = new PRValueMap();
    
    try { 
      valueMap.open(FileSystem.getLocal(conf),new Path(valueFile.getAbsolutePath()), new Path(rangeFile.getAbsolutePath()));
      // valueMap.dumpRangeItems();
      
      LOG.info("Opening Outlinks File at:" + outlinksFile);
      SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.getLocal(conf),new Path(outlinksFile.getPath()),conf);
      LOG.info("Iterating Items in Outlinks File and Writing Test Value");
      
      URLFPV2 fingerprint = new URLFPV2();
      CompressedOutlinkList outlinkList = new CompressedOutlinkList();
      
      int itemCount = 0;
      long valueWriteStart = System.currentTimeMillis();
      long timeStart = valueWriteStart;
      
      while (reader.next(fingerprint,outlinkList)) { 

        ++itemCount;
        // LOG.info("Got Item with Domain Hash:" + fingerprint.getDomainHash() + " URLFP:" + fingerprint.getUrlHash());
        // get pr value for item ... 
        //TODO: SWITCH TO INT FOR TEST
        // valueMap.setPRValue(fingerprint,itemCount % Short.MAX_VALUE);
        valueMap.setPRValue(fingerprint,itemCount % Integer.MAX_VALUE);
        // LOG.info("Get PRValue returned:" + prValue);
                
        fingerprint.clear();
        outlinkList.clear();
        
        if (itemCount % 10000 == 0) {
          LOG.info("Wrote 10000 Items in:" + (System.currentTimeMillis() - timeStart) + " Milliseconds");
          timeStart = System.currentTimeMillis();
        }
      }
      
      LOG.info("Done Writing Values. Took:" + (System.currentTimeMillis() - valueWriteStart) + " Milliseconds");
      
      valueFile.delete();
      OutputStream stream = null;
      
      try { 
        stream = new FileOutputStream(valueFile);
        // flush stuff to disk 
        valueMap.flush(stream);
      }
      finally { 
        if (stream != null)
          stream.close();
      }
      
      
      LOG.info("Opening Outlinks File at:" + args[2]);
      reader = new SequenceFile.Reader(FileSystem.getLocal(conf),new Path(args[2]),conf);
      LOG.info("Iterating Items in Outlinks File and Reading Test Value");
      
      
      itemCount = 0;
      long valueReadStart = System.currentTimeMillis();
      timeStart = valueWriteStart;
      
      while (reader.next(fingerprint,outlinkList)) { 

        ++itemCount;
        // LOG.info("Got Item with Domain Hash:" + fingerprint.getDomainHash() + " URLFP:" + fingerprint.getUrlHash());
        // get pr value for item ... 
        float prValue = valueMap.getPRValue(fingerprint);
        // LOG.info("Get PRValue returned:" + prValue);
        
        //TODO: SWITCH TO INT FOR TEST
        //if (prValue != (itemCount % Short.MAX_VALUE)) {
        if (prValue != (itemCount % Integer.MAX_VALUE)) {
          //TODO: SWITCH TO INT FOR TEST
          // throw new IOException("PRValue did not match for item:" + itemCount + " Expected:" + (itemCount % Short.MAX_VALUE) + " Got:" + prValue);
          throw new IOException("PRValue did not match for item:" + itemCount + " Expected:" + (itemCount % Integer.MAX_VALUE) + " Got:" + prValue);
        }
        
        fingerprint.clear();
        outlinkList.clear();
        
        if (itemCount % 10000 == 0) {
          LOG.info("Read 10000 Items in:" + (System.currentTimeMillis() - timeStart) + " Milliseconds");
          timeStart = System.currentTimeMillis();
        }
      }
      
      LOG.info("Done Reading Values. Took:" + (System.currentTimeMillis() - valueReadStart) + " Milliseconds");
      
      
      valueMap.close();
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
    }
    
  }
  
  private static void runAccumulateRankBechmark(String args[]){ 
    LOG.info("Initializing Hadoop Config");

    Configuration conf = new Configuration();
    
    conf.addResource("nutch-default.xml");
    conf.addResource("nutch-site.xml");
    conf.addResource("mapred-site.xml");
    conf.addResource("hdfs-site.xml");
    conf.addResource("commoncrawl-default.xml");
    conf.addResource("commoncrawl-site.xml");
    
    CrawlEnvironment.setHadoopConfig(conf);
    CrawlEnvironment.setDefaultHadoopFSURI("hdfs://ccn01:9000/");
  	
  	try { 
    	
      Path valueFile 		= new Path(args[1]);
      Path rangeFile 		= new Path(args[2]);
      Path outlinksFile = new Path(args[3]);
      //Path outputDir 		= new Path(args[4]); 
      String remoteOutputDir = args[4]; 
      
      LOG.info("ValuesFile:" + valueFile);
      LOG.info("RangeFile:" + rangeFile);
      LOG.info("OutlinksFile:" + outlinksFile);
      LOG.info("RemoteOutputDir:" + remoteOutputDir);
      
      
    	LOG.info("Initializing SuperDomain Filter");
    	SuperDomainFilter superDomainFilter = new SuperDomainFilter();
    	superDomainFilter.loadFromPath(new InetSocketAddress("10.0.20.21",CrawlEnvironment.DIRECTORY_SERVICE_RPC_PORT).getAddress(),CrawlEnvironment.ROOT_SUPER_DOMAIN_PATH, false);
    	LOG.info("Loaded SuperDomain Filter");
      
      int    thisNodeIdx = 0;
      int    totalNodeCount = CrawlEnvironment.PR_NUMSLAVES;
      
      FileSystem fs = FileSystem.get(conf);
      
      PRValueMap valueMap = new PRValueMap();
      
      LOG.info("Initializing Value Map");
      valueMap.open(FileSystem.get(conf),valueFile, rangeFile);
      LOG.info("Initialized Value Map");
      
      LOG.info("Calculating Rank");
      long timeStart = System.currentTimeMillis();
      calculateRank(conf,fs,valueMap, null, remoteOutputDir, 0, totalNodeCount, 0, superDomainFilter, null);
      long timeEnd = System.currentTimeMillis();
      LOG.info("Done Calculating Rank. Took:" + (timeEnd-timeStart));
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
    }
  }
  
  public static final int readURLFPAndCountFromStream(DataInput input,URLFPV2 fpOut)throws IOException { 
		fpOut.setDomainHash(input.readLong());
		fpOut.setRootDomainHash(input.readLong());
		fpOut.setUrlHash(input.readLong());
		return WritableUtils.readVInt(input);  	
  }
  
  public static final void writeURLFPAndCountToStream(DataOutput stream,URLFPV2 key,int urlCount)throws IOException { 
  	stream.writeLong(key.getDomainHash());
  	stream.writeLong(key.getRootDomainHash());
  	stream.writeLong(key.getUrlHash());
  	WritableUtils.writeVInt(stream, urlCount);
  }
  
  public static final void readURLFPFromStream(DataInput input,URLFPV2 fpOut)throws IOException { 
		fpOut.setDomainHash(input.readLong());
		fpOut.setRootDomainHash(input.readLong());
		fpOut.setUrlHash(input.readLong());
  }
  
  public static final void writeURLFPToStream(DataOutput stream,URLFPV2 key)throws IOException { 
  	stream.writeLong(key.getDomainHash());
  	stream.writeLong(key.getRootDomainHash());
  	stream.writeLong(key.getUrlHash());
  }
  
  private static void runDistributeRankBenchmark(String args[]){ 

    LOG.info("Initializing Hadoop Config");

    Configuration conf = new Configuration();
    
    conf.addResource("nutch-default.xml");
    conf.addResource("nutch-site.xml");
    conf.addResource("mapred-site.xml");
    conf.addResource("hdfs-site.xml");
    conf.addResource("commoncrawl-default.xml");
    conf.addResource("commoncrawl-site.xml");
    
    CrawlEnvironment.setHadoopConfig(conf);
    CrawlEnvironment.setDefaultHadoopFSURI("hdfs://ccn01:9000/");
  	
  	try { 
    	
      Path valueFile 		= new Path(args[1]);
      Path rangeFile 		= new Path(args[2]);
      Path outlinksFile = new Path(args[3]);
      //Path outputDir 		= new Path(args[4]); 
      String remoteOutputDir = args[4]; 
      
      LOG.info("ValuesFile:" + valueFile);
      LOG.info("RangeFile:" + rangeFile);
      LOG.info("OutlinksFile:" + outlinksFile);
      LOG.info("RemoteOutputDir:" + remoteOutputDir);
      
      int    thisNodeIdx = 0;
      int    totalNodeCount = CrawlEnvironment.PR_NUMSLAVES;
      
      FileSystem fs = FileSystem.get(conf);
      
      PRValueMap valueMap = new PRValueMap();
      
      valueMap.open(FileSystem.get(conf),valueFile, rangeFile);
      
      fs.mkdirs(new Path(remoteOutputDir));
      fs.delete(new Path(remoteOutputDir,"*"),false);
      
      //File localOutputFile = new File(localOutputDir,getOutlinksBaseName(0,0) + "-" + NUMBER_FORMAT.format(0));
      //localOutputFile.delete();
      
      distributeRank(valueMap,outlinksFile,true, null,remoteOutputDir, thisNodeIdx, totalNodeCount,0,null);
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
    }
  }

  
}
