package org.commoncrawl.util;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Raw Key SequenceFileIndex
 * 
 * @author rana
 *
 * @param <KeyType>
 */
public class KeyBasedSequenceFileIndex<KeyType extends WritableComparable> {

  static final Log LOG = LogFactory.getLog(KeyBasedSequenceFileIndex.class);

  byte[]          _indexData;
  RawComparator<KeyType> _comparator;
  /** 
   * 
   * @param conf
   * @param indexFilePath
   * @param keyComparator
   * @throws IOException
   */
  public KeyBasedSequenceFileIndex(Configuration conf,Path indexFilePath,RawComparator<KeyType> keyComparator)throws IOException {
    _comparator = keyComparator;
    FileSystem fs = FileSystem.get(indexFilePath.toUri(),conf);
    FileStatus fileStatus = fs.getFileStatus(indexFilePath);
    if (fileStatus == null) { 
      throw new IOException("Null FileStats !");
    }
    _indexData = new byte[(int)fileStatus.getLen()];
    FSDataInputStream stream = fs.open(indexFilePath);
    try { 
      // read full stream ... 
      stream.readFully(_indexData);
    }
    finally { 
      stream.close();
    }
  }
  
  public static class IndexReader<KeyType extends WritableComparable> { 
    KeyBasedSequenceFileIndex<KeyType> _index;
    DataInputStream _reader = null;
    ByteBuffer      _buffer = null;
    IndexItemData   _itemData = new IndexItemData();
    long _keyDataOffset;
    int _itemCount;
    
    public IndexReader(KeyBasedSequenceFileIndex<KeyType> index)throws IOException { 
      _index = index;
      DataInputBuffer temp = new DataInputBuffer();
      temp.reset(index._indexData,0,index._indexData.length);
      // seek to very end ... 
      temp.skip(index._indexData.length-12);
      // read key data pos ... 
      _keyDataOffset = temp.readLong();
      // read item count ... 
      _itemCount = temp.readInt();
      // set byte buffer 
      _buffer = ByteBuffer.wrap(index._indexData,0,index._indexData.length - 12);
      // set reader ...  
      _reader = new DataInputStream(wrapBufferInStream(_buffer));
      
      // dumpIndex();
    }
    
    private void dumpIndex()throws IOException { 
      DataInputBuffer tempBuffer = new DataInputBuffer();
      TextBytes textBytes = new TextBytes();
      LOG.info("Index Item Count:"+ _itemCount);
      for (int i=0;i<_itemCount;++i) { 
        getIndexItemDataAtPos(i, _buffer,_reader,_itemData);
        tempBuffer.reset(_index._indexData,(int)(_keyDataOffset+_itemData.keyDataOffset),_itemData.keyDataLen);
        textBytes.setFromRawTextBytes(tempBuffer);
        LOG.info("Pos:" + i + " Key:" + textBytes.toString() + " SeqFilePos:" + _itemData.seqFilePos);
      }
    }
    
    private static InputStream wrapBufferInStream(final ByteBuffer buf) {
      return new InputStream() {
          public synchronized int read() throws IOException {
              if (!buf.hasRemaining()) {
                  return -1;
              }
              return buf.get() & 0xff;
          }

          public synchronized int read(byte[] bytes, int off, int len) throws IOException {
              // Read only what's left
              len = Math.min(len, buf.remaining());
              buf.get(bytes, off, len);
              return len;
          }
      };
    }
    
    private static class IndexItemData { 
      long seqFilePos;
      int  keyDataOffset;
      int  keyDataLen;
    }
    
    private static void getIndexItemDataAtPos(int pos,ByteBuffer buffer,DataInputStream reader,IndexItemData indexData) throws IOException { 
      buffer.position((pos * (IndexWriter.INDEX_ITEM_SIZE)));
      indexData.seqFilePos = reader.readLong();
      indexData.keyDataOffset = reader.readInt();
      indexData.keyDataLen   = reader.readInt();
    }
    
    /**
     * 
     * @param keyData
     * @param keyDataOffset
     * @param keyDataLen
     * @return
     * @throws IOException
     */
    public long findBestPositionForKey(byte[] keyData,int keyDataOffset,int keyDataLen)throws IOException {
      
      int low = 0;
      int high = _itemCount - 1;
      while (low <= high) {
        int mid = low + ((high - low) / 2);
        // get data at index ... 
        getIndexItemDataAtPos(mid, _buffer, _reader, _itemData);
        // compare 
        int comparisonResult 
          = _index._comparator.compare(
              _index._indexData, 
              (int) _keyDataOffset + _itemData.keyDataOffset, 
              _itemData.keyDataLen, 
              keyData, keyDataOffset, keyDataLen);
        
        if (comparisonResult > 0)
            high = mid - 1;
        else if (comparisonResult < 0)
            low = mid + 1;
        else {
          LOG.info("Match at IndexPos:" + mid + " SeqFilePos:" + _itemData.seqFilePos);
          return _itemData.seqFilePos;  
        }
      }
      if (high == -1)
        return -1L;
      else {
        getIndexItemDataAtPos(high, _buffer, _reader, _itemData);
        LOG.info("Nearest Match at:" + high + " SeqFilePos:" + _itemData.seqFilePos);
        return _itemData.seqFilePos;
      }
    }
  }
  
  
  public static class IndexWriter<KeyType extends WritableComparable, ValueType extends Writable> implements SequenceFileIndexWriter<KeyType, ValueType> {

    FSDataOutputStream _outputStream; 
    long  _lastIndexedPos = -1;
    int   _totalIndexKeys = 0;
    DataOutputBuffer _keyDataBuffer = new DataOutputBuffer();

    public static final int INDEX_ITEM_SIZE = 8 + 4 + 4;
    
    public IndexWriter(Configuration conf,Path indexFilePath)throws IOException {
      FileSystem fs = FileSystem.get(indexFilePath.toUri(),conf);
      fs.delete(indexFilePath, false);
      _outputStream = fs.create(indexFilePath);
    }
    
    @Override
    public void close() throws IOException {
      try { 
        long keyDataPos = _outputStream.getPos();
        _outputStream.write(_keyDataBuffer.getData(),0,_keyDataBuffer.getLength());
        _outputStream.writeLong(keyDataPos);
        _outputStream.writeInt(_totalIndexKeys);
        _outputStream.flush();
      }
      finally { 
        _outputStream.close();
      }
    }

    @Override
    public void indexItem(byte[] keyData, int keyOffset, int keyLength,
        byte[] valueData, int valueOffset, int valueLength, long writerPosition)
        throws IOException {

      // check to see if block position changed ... 
      if (writerPosition != _lastIndexedPos){
        // establish new start index
        _lastIndexedPos = writerPosition;
        // increment index key count... 
        _totalIndexKeys++;
        // write index item 
        // stream pos
        _outputStream.writeLong(_lastIndexedPos);
        // key data offset 
        _outputStream.writeInt(_keyDataBuffer.getLength());
        // key len ... 
        _outputStream.writeInt(keyLength);
        // key data 
        _keyDataBuffer.write(keyData,keyOffset,keyLength);
      }
    }
    
  }
}
