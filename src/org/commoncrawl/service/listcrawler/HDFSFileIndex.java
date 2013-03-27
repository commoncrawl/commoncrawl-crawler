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


import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.rmi.server.UID;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.Comparator;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.protocol.CacheItem;
import org.commoncrawl.util.RiceCoding;
import org.commoncrawl.util.URLFingerprint;
import org.commoncrawl.util.BloomFilter;
import org.commoncrawl.util.ByteStream;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.IntrusiveList.IntrusiveListElement;

/** 
 * An index of files contained in a HDFS SequenceFile
 * 
 * @author rana
 *
 */
public class HDFSFileIndex {  
  
  public static final Log LOG = LogFactory.getLog(HDFSFileIndex.class);

  
  public static final int INDEX_HINT_RECORD_INTERVAL= 100;
  public static final int INDEX_HINT_SIZE = 8 + 4 + 4;
  
  
  private File        _localIndexFilePath = null;
  private FileSystem  _remoteFileSystem = null;
  private Path        _remoteDataPath = null;
  private BloomFilter _bloomFilter = null;
  private ByteBuffer  _indexHints  = null;
  private int         _indexHintCount = -1;
  private int         _indexDataOffset = -1;
  private int         _indexDataSize   = -1;
     
  
  public HDFSFileIndex(FileSystem remoteFileSystem,Path remoteIndexFileLocation,Path remoteDataFileLocation,File localIndexDataDirectory) throws IOException {
    _remoteFileSystem = remoteFileSystem;
    _remoteDataPath = remoteDataFileLocation;
    // create a local index file for the index
    _localIndexFilePath = new File(localIndexDataDirectory,remoteIndexFileLocation.getName());
    _localIndexFilePath.delete();
    
    LOG.info("Copying Remote Index Location:" + remoteIndexFileLocation + " to Local File Location:" + _localIndexFilePath);
    // copy over the index data file 
    remoteFileSystem.copyToLocalFile(remoteIndexFileLocation, new Path(_localIndexFilePath.getAbsolutePath()));
    LOG.info("Done Copying Remote File. Loading Index");
    // load the index 
    loadIndexFromLocalFile();
  }

  
  public HDFSFileIndex(FileSystem remoteFileSystem,File localIndexFileLocation,Path remoteDataFileLocation) throws IOException {
    _remoteFileSystem = remoteFileSystem;
    _remoteDataPath = remoteDataFileLocation;
    _localIndexFilePath = localIndexFileLocation;
    loadIndexFromLocalFile();
  }

  
  public long getIndexTimestamp() {
    try {
      Matcher m = Pattern.compile(".*-([0-9]*)").matcher(_remoteDataPath.getName());
      if (m.matches()) { 
        return Long.parseLong(m.group(1));
      }
    }
    catch (Exception e) { 
      LOG.error(CCStringUtils.stringifyException(e));
    }
    return 0L;
  }
  
  public Path getIndexDataPath() { 
    return _remoteDataPath;
  }
  
  private void loadIndexFromLocalFile()throws IOException {
    LOG.info("Loading Index from Local File:" + _localIndexFilePath);
    // now open an input stream to the local file ...
    FileInputStream fileInputStream = new FileInputStream(_localIndexFilePath);
    DataInputStream dataStream = new DataInputStream(fileInputStream);
    
    try { 
      // deserialize bloom filter 
      _bloomFilter = BloomFilter.serializer().deserialize(dataStream);
      _indexHintCount = dataStream.readInt();
      
      int indexHintDataSize = _indexHintCount * INDEX_HINT_SIZE;
      // and deserialize index hints 
      _indexHints = ByteBuffer.allocate(indexHintDataSize);
      
      dataStream.readFully(_indexHints.array());
      
      // load index data buffer size 
      _indexDataSize = dataStream.readInt();
      // and capture offset information 
      _indexDataOffset = (int) fileInputStream.getChannel().position();
    }
    finally { 
      if (fileInputStream != null) { 
        fileInputStream.close();
      }
    }
    LOG.info("Successfully loaded Index");
  }
  
  public CacheItem findItem(long targetFingerprint,boolean checkOnly)throws IOException { 
    // check bloom filter first ... 
    if (_bloomFilter.isPresent(targetFingerprint)) { 
//      synchronized (this) { 
        // find best hint ... 
        HDFSFileIndex.IndexItem itemOut = _findBestIndexHintForFingerprint(targetFingerprint);

        // if non null result returned   
        if (itemOut != null) {
            
          // if no match, then this is the next lowest matching hint item ... 
          if (itemOut.fingerprint != targetFingerprint) {
            // demand load item data 
            HDFSFileIndex.IndexDataBlock dataBlock = demandLoadIndexDataBlock(itemOut.fingerprint,itemOut.indexDataOffset,itemOut.indexDataSize);
            // and search within it ...
            itemOut = dataBlock.searchBlockFor(targetFingerprint);
          }
          
          if (itemOut != null && checkOnly) { 
            CacheItem item = new CacheItem();
            item.setUrlFingerprint(targetFingerprint);
            return item;
          }
          
          if (itemOut != null) {
            LOG.info("Found Match in Index:" + _localIndexFilePath + " For FP:" + targetFingerprint + " Loading File:" + _remoteDataPath+ " at Offset:" + _indexDataOffset);
            // open sequence file ... 
            SequenceFile.Reader reader = new SequenceFile.Reader(_remoteFileSystem,_remoteDataPath,CrawlEnvironment.getHadoopConfig());
            
            try { 
              reader.seek(itemOut.dataOffset);
              
              Text url = new Text();
              CacheItem item = new CacheItem();

              LOG.info("Reading Item and Data");
              reader.next(url,item);
              String strURL = url.toString();
              
              LOG.info("Read returned url:" + strURL);
              item.setUrl(strURL);
              
              return item;
            }
            finally {
              if (reader != null) 
                reader.close();
            }
          }
        }
//      }
    }
    return null;
  }

  private HDFSFileIndex.IndexDataBlock demandLoadIndexDataBlock(long fingerprint,int itemDataOffset,int itemDataSize) throws IOException { 
    // ok time to load this block ...
    RandomAccessFile file = new RandomAccessFile(_localIndexFilePath,"r");
    
    try { 
      
      ByteBuffer bufferOut = ByteBuffer.allocate(itemDataSize);
      
      if (bufferOut != null) {
        file.seek(_indexDataOffset + itemDataOffset);
        file.readFully(bufferOut.array());
        HDFSFileIndex.IndexDataBlock dataBlock = new IndexDataBlock(fingerprint,0,bufferOut);
        return dataBlock;
      }
      else { 
        throw new IOException("Unable to allocate byte buffer!!!");
      }
    }
    finally { 
     if (file != null) { 
       file.close();
     }
    }
  }
  
  
  private HDFSFileIndex.IndexItem _findBestIndexHintForFingerprint(long targetFP) throws IOException { 
    int low = 0;
    int high = _indexHintCount - 1;
    
    while (low <= high) {
      int mid = low + ((high - low) / 2);
      _indexHints.position(mid * (INDEX_HINT_SIZE));
      long hintFP = _indexHints.getLong();
      // compare to target 
      long comparisonResult = (hintFP > targetFP) ? 1 : (hintFP < targetFP) ? -1 : 0; 
      
      if (comparisonResult > 0)
          high = mid - 1;
      else if (comparisonResult < 0)
          low = mid + 1;
      else {
        return new IndexItem(targetFP,_indexHints.getInt());
      }
    }
    
    if (high >= 0 && low < _indexHintCount) { 
      
      _indexHints.position(high * INDEX_HINT_SIZE);
      // create nearest match ... 
      HDFSFileIndex.IndexItem itemOut = new IndexItem(_indexHints.getLong(),_indexHints.getInt(),_indexHints.getInt(),-1);
      // figure out this items data block size ... 
      if (high < (_indexHintCount - 1)) {
        _indexHints.position(((high+1) * INDEX_HINT_SIZE) + 12);
        itemOut.indexDataSize =  _indexHints.getInt() - itemOut.indexDataOffset;
      }
      else { 
        itemOut.indexDataSize =  _indexDataSize - itemOut.indexDataOffset;
      }
      return itemOut;
    }
    return null;
  }
  
  static class IndexItem {
    
    public IndexItem(long fingerprint,int dataOffset) { 
      this.fingerprint = fingerprint;
      this.dataOffset = dataOffset;
      this.indexDataOffset = -1;
    }

    public IndexItem(long fingerprint,int dataOffset,int indexDataOffset,int indexDataSize) { 
      this.fingerprint = fingerprint;
      this.dataOffset = dataOffset;
      this.indexDataOffset = indexDataOffset;
      this.indexDataSize = indexDataSize;
    }
    
    
    public long fingerprint;
    public int  dataOffset;
    public int  indexDataOffset = -1;
    public int  indexDataSize = -1;
  }

  
  public static class IndexDataBlock extends IntrusiveListElement<HDFSFileIndex.IndexDataBlock> { 
    public IndexDataBlock(long baseFingerprint,int dataOffset,ByteBuffer data) { 
      _dataOffset = dataOffset;
      _buffer    = data;
      _lastUseTime = System.currentTimeMillis();
      _baseFingerprint = baseFingerprint;
    }
    
    HDFSFileIndex.IndexItem searchBlockFor(long targetFingerprint) { 
      // reset cursor ... 
      _buffer.position(_dataOffset);
      
      int fingerprintMValue = _buffer.get();
      int fingerprintBits   = (int)CacheManager.readVLongFromByteBuffer(_buffer);
      
      RiceCoding.RiceCodeReader fingerprintReader 
        = new RiceCoding.RiceCodeReader(
            (int)fingerprintMValue,
            (int)fingerprintBits,
            _buffer.array(),
            _buffer.position());
      
      // advance past fingerprint data to offset data 
      _buffer.position(_buffer.position() + ((fingerprintBits + 7) /8));
      
      // and create offset data reader 
      RiceCoding.RiceCodeReader offsetReader 
      = new RiceCoding.RiceCodeReader(
          (int)_buffer.get(),
          (int)CacheManager.readVLongFromByteBuffer(_buffer),
          _buffer.array(),
          _buffer.position());
      
      long fingerprintValue = _baseFingerprint;
      
      while (fingerprintReader.hasNext()) { 
        fingerprintValue += fingerprintReader.nextValue();
        // rice coded values are offset by one since rice coding cannot support zero values .... 
        fingerprintValue -= 1;
        int offsetValue  = (int)offsetReader.nextValue();
        // now compare to target 
        if (fingerprintValue == targetFingerprint) { 
          return new IndexItem(fingerprintValue,offsetValue - 1 /*rice coder doesn't like zeros and Offset COULD be zero, so we have to offset by one to be safe*/);
        }
      }
      return null;
    }
    
    public int          _dataOffset = -1;
    public ByteBuffer   _buffer    = null;
    public long         _lastUseTime = -1;
    public long         _baseFingerprint;
  }

  
  
  private static double lg(double value) { 
      return Math.log(value)/Math.log(2.0);
  }
  
  public static void writeIndex(Vector<FingerprintAndOffsetTuple> offsetInfo,DataOutput indexFileOut)throws IOException { 
    
    long firstFingerprint = offsetInfo.get(0)._fingerprint;
    
    BloomFilter bloomFilter = new BloomFilter(offsetInfo.size(),0.001201);
    
    // sort the offset list by fingerprint 
    Collections.sort(offsetInfo,new Comparator<FingerprintAndOffsetTuple>() {

      @Override
      public int compare(FingerprintAndOffsetTuple o1, FingerprintAndOffsetTuple o2) {
        return (o1._fingerprint < o2._fingerprint) ? -1 : o1._fingerprint > o2._fingerprint ? 1 :0; 
      } 
      
    });
    // now we need to write the index out

    // allocate working set buffers ...
    ByteBuffer indexDataBuffer    = ByteBuffer.allocate(offsetInfo.size() * 16);
    ByteBuffer indexHintsBuffer   = ByteBuffer.allocate(((((offsetInfo.size() + INDEX_HINT_RECORD_INTERVAL) / INDEX_HINT_RECORD_INTERVAL)+1) * INDEX_HINT_SIZE) + 4);
    
    // build index hints placeholder 
    Vector<HDFSFileIndex.IndexItem> hints = new Vector<HDFSFileIndex.IndexItem>();
    // 0 100 200 300 400 500
    for (int i=0;i<offsetInfo.size();++i) {
      
      if (i%INDEX_HINT_RECORD_INTERVAL == 0 || (i == (offsetInfo.size()-1))) {
        HDFSFileIndex.IndexItem hint = new IndexItem(offsetInfo.get(i)._fingerprint,(int)offsetInfo.get(i)._offset);
        hints.add(hint);
        // add fingerprint to bloom filter 
        bloomFilter.add(hint.fingerprint);
      }
    }
    // start off the index hints buffer with a hint of the index hint buffer size 
    indexHintsBuffer.putInt(hints.size());
    
    // track total bits used ... 
    int bitsUsedForHints = 0;
    int bitsUsedForFingerprints = 0;
    int bitsUsedForOffsets = 0;
    
    // now start populating index data ... 
    for (int hintIdx=0;hintIdx<hints.size();++hintIdx) {
      
      HDFSFileIndex.IndexItem hint = hints.get(hintIdx);
      
      LOG.info("IndexWriter FP:" + hint.fingerprint);
      indexHintsBuffer.putLong(hint.fingerprint);
      indexHintsBuffer.putInt(hint.dataOffset);
      indexHintsBuffer.putInt(indexDataBuffer.position());
      
      // update stats 
      bitsUsedForHints += INDEX_HINT_SIZE * 8;
      

      if (hintIdx < hints.size() - 1) { 
        // track cumulative delta and offset values (for average calc later)
        double cumulativeDelta = 0;
        long cumulativeOffset = 0;
        
        int  subIndexItemCount = 0;
        int  nonZeroDeltaCount = 0;
        
        Vector<HDFSFileIndex.IndexItem> subHints = new Vector<HDFSFileIndex.IndexItem>();
        
        // initialize last fingerprint to indexed value ... 
        long lastFingerprint = hint.fingerprint;
        
        // first collect values in between index hints
        for (int nonIndexItem=(hintIdx*INDEX_HINT_RECORD_INTERVAL)+1;nonIndexItem < ((hintIdx+1)*INDEX_HINT_RECORD_INTERVAL);++nonIndexItem) {
          if (nonIndexItem >= offsetInfo.size())
            break;
      
          
          // calculate fingerprint delta ...
          long fingerprintDelta = offsetInfo.get(nonIndexItem)._fingerprint - lastFingerprint;
          LOG.info("IndexWriter FP:" + offsetInfo.get(nonIndexItem)._fingerprint + " Delta:" + fingerprintDelta);
          // offset delta
  
          if (fingerprintDelta != 0) { 
            
            cumulativeDelta += (double) fingerprintDelta;
            LOG.info("Cumulative Delta is:" + cumulativeDelta);
            nonZeroDeltaCount++;
          }
          
          cumulativeOffset += offsetInfo.get(nonIndexItem)._offset;
          
          ++subIndexItemCount;
          
          // add to collection vector 
          subHints.add(new IndexItem(fingerprintDelta,(int)offsetInfo.get(nonIndexItem)._offset));
          
          // remember the last fingerprint ...
          lastFingerprint = offsetInfo.get(nonIndexItem)._fingerprint;
          
          // add item to bloom filter
          bloomFilter.add(lastFingerprint);
        }
          
        // calculate average delta value 
        double averageDeltaValue = (double)cumulativeDelta / (double)nonZeroDeltaCount;
        // calculate m for fingerprint deltas 
        int mForFingerprints =  (int) Math.floor(lg(averageDeltaValue));
        LOG.info("Average Delta Value is:" + averageDeltaValue + " m is:" + mForFingerprints);
        // calculate average offset value
        double averageOffsetValue = (double)cumulativeOffset/ (double)subIndexItemCount;
        // calculate m for offsets 
        int mForOffsets =  (int) Math.floor(lg(averageOffsetValue));
        
        
        // calculate rice codes
        RiceCoding riceCodeFP       = new RiceCoding(mForFingerprints);
        RiceCoding riceCodeOffsets  = new RiceCoding(mForOffsets);
        
        // populate bits 
        for (HDFSFileIndex.IndexItem subItemHint : subHints) {
          if (subItemHint.fingerprint == 0) { 
            LOG.warn("Zero Delta for Fingerprint Detected.There are two duplicate entries in log!");
          }
          riceCodeFP.addItem(subItemHint.fingerprint + 1);
          riceCodeOffsets.addItem(subItemHint.dataOffset+1);
        }
        // now track bits used ... 
        bitsUsedForFingerprints += riceCodeFP.getNumBits();
        bitsUsedForOffsets      += riceCodeOffsets.getNumBits();
        
        // write out metadata 
        
        // save the current position 
        int currentPosition = indexDataBuffer.position();
        
        // fingerprint data 
        indexDataBuffer.put((byte)mForFingerprints);
        CacheManager.writeVLongToByteBuffer(indexDataBuffer,riceCodeFP.getNumBits());
        indexDataBuffer.put(riceCodeFP.getBits(), 0, (riceCodeFP.getNumBits() + 7) /8);
        
        // offset data 
        indexDataBuffer.put((byte)mForOffsets);
        CacheManager.writeVLongToByteBuffer(indexDataBuffer,riceCodeOffsets.getNumBits());
        indexDataBuffer.put(riceCodeOffsets.getBits(), 0, (riceCodeOffsets.getNumBits() + 7) /8);
        
  
        System.out.println("Item Count:" + subIndexItemCount + "FP Bits:" + subIndexItemCount * 64 + " Compressed:" + riceCodeFP.getNumBits() + 
                            " Offset Bits:" + subIndexItemCount * 32 + " Compressed:" + riceCodeOffsets.getNumBits());
        
        LOG.info("Item Count:" + subIndexItemCount + "FP Bits:" + subIndexItemCount * 64 + " Compressed:" + riceCodeFP.getNumBits() + 
            " Offset Bits:" + subIndexItemCount * 32 + " Compressed:" + riceCodeOffsets.getNumBits());

        
        if ((subIndexItemCount * 64) < riceCodeFP.getNumBits()) { 
        	throw new RuntimeException("Compressed Size > UnCompressed Size!!!!");
        }
        
        validateIndexData(indexDataBuffer.array(),currentPosition,hint.fingerprint,subHints,bloomFilter);
      }
      
    }
    
    if (!bloomFilter.isPresent(firstFingerprint)) { 
      throw new RuntimeException("Test Failed!");
    }
    
    // serialize bloomfilter
    ByteStream baos = new ByteStream(1<< 12);
    BloomFilter.serializer().serialize(bloomFilter, new DataOutputStream(baos));
    
    // spit out final stats 
    System.out.println(
          " Bloomfilter Size:" + baos.size() + 
          " IndexHintBuffer Size:" + indexHintsBuffer.position() +
          " IndexDataBuffer Size:" + indexDataBuffer.position());
          
    
    // now write out the final index file ... 
    
    // bloom filter data ... 
    indexFileOut.write(baos.getBuffer(),0,baos.size());
    // write hint data  
    indexFileOut.write(indexHintsBuffer.array(),0,indexHintsBuffer.position());
    // write out rice code data size 
    indexFileOut.writeInt(indexDataBuffer.position());
    // finally rice coded sub-index data
    indexFileOut.write(indexDataBuffer.array(),0,indexDataBuffer.position());
  }
  
  public static void validateIndexData(byte[] data,int offset,long baseFingerprint,Vector<HDFSFileIndex.IndexItem> subItems,BloomFilter filter) { 
    HDFSFileIndex.IndexDataBlock dataBlock = new IndexDataBlock(baseFingerprint,offset,ByteBuffer.wrap(data));
    
    long fingerprintValue= baseFingerprint;
    int itemIndex = 0;
    for (HDFSFileIndex.IndexItem item : subItems) { 
      fingerprintValue += item.fingerprint;
      long timeStart= System.currentTimeMillis();
      if (dataBlock.searchBlockFor(fingerprintValue) == null) { 
        throw new RuntimeException("Unable to Find fingerprint in data block ! - Test Failed!");
      }
      if (!filter.isPresent(fingerprintValue)) { 
        throw new RuntimeException("Unable to Find fingerprint in bloom filter ! - Test Failed!");
      }
      
      CacheManager.LOG.info("Search for Item@" + itemIndex++ + " Took:" + (System.currentTimeMillis() - timeStart));
    }
  }
  
  public static void main(String[] args) {

    try { 
      ByteStream outputStream = new ByteStream(8192);
      Vector<FingerprintAndOffsetTuple> fpInfo = new Vector<FingerprintAndOffsetTuple>();
      
      // construct 10000 entries with randomin fingerprints 
      for (int i=0;i<10000;++i) { 
        MessageDigest digester;
        digester = MessageDigest.getInstance("MD5");
        long time = System.currentTimeMillis();
        digester.update((new UID()+"@"+time+":" + i).getBytes());
        FingerprintAndOffsetTuple offsetInfo = new FingerprintAndOffsetTuple(URLFingerprint.generate64BitURLFPrint(StringUtils.byteToHexString(digester.digest())),i*10000);
        fpInfo.add(offsetInfo);
      }
      // clone the vector 
      Vector<FingerprintAndOffsetTuple> fpInfoCloned = new Vector<FingerprintAndOffsetTuple>();
      fpInfoCloned.addAll(fpInfo);
      // now write out the index ... 
      writeIndex(fpInfoCloned, new DataOutputStream(outputStream));
      // spit out some basic stats 
      System.out.println("output buffer size is:" + outputStream.size());
      
    }
    catch (Exception e) { 
      CacheManager.LOG.error(CCStringUtils.stringifyException(e));
    }
  }
  
  
  
}