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
package org.commoncrawl.hadoop.mergeutils;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.commoncrawl.util.FlexBuffer;

/**
 * Used to Generate optimized representations from complex key/values pairs.
 * Basically a tradeoff between better sort performance at the expense of a
 * little bit of increased (per record) memory footprint.
 * 
 * @author rana
 * 
 * @param <KeyType>
 * @param <ValueType>
 */
public abstract class OptimizedKeyGeneratorAndComparator<KeyType extends Writable, ValueType extends Writable> {

  /**
   * OptimizedKey - used to encapsulate optimized key data generated via the
   * generator
   * 
   * @author rana
   * 
   */
  public static final class OptimizedKey {

    // key types ...

    // a key that has a long component
    public static final int KEY_TYPE_LONG = 1 << 0;
    // a key that has a buffer component
    public static final int KEY_TYPE_BUFFER = 1 << 1;
    // a key that has a long and buffer component
    public static final int KEY_TYPE_LONG_AND_BUFFER = KEY_TYPE_LONG | KEY_TYPE_BUFFER;

    public static int writeVLong(DataOutput stream, long i) throws IOException {
      int bytesUsed = 1;
      if (i >= -112 && i <= 127) {
        stream.writeByte((byte) i);
        return bytesUsed;
      }

      int len = -112;
      if (i < 0) {
        i ^= -1L; // take one's complement'
        len = -120;
      }

      long tmp = i;
      while (tmp != 0) {
        tmp = tmp >> 8;
        len--;
      }

      stream.writeByte((byte) len);
      bytesUsed++;

      len = (len < -120) ? -(len + 120) : -(len + 112);

      for (int idx = len; idx != 0; idx--) {
        int shiftbits = (idx - 1) * 8;
        long mask = 0xFFL << shiftbits;
        stream.writeByte((byte) ((i & mask) >> shiftbits));
        bytesUsed++;
      }
      return bytesUsed;
    }

    // key type
    private int _keyType;
    // header size (based on key type)
    private int _headerSize = 0;

    // long value if optimized key is a long
    private long _longKeyValue = 0;
    // serialized buffer size
    private int _dataBufferSize = 0;
    // serialized buffer offset
    private int _dataBufferOffset = 0;
    // data buffer
    private FlexBuffer _dataBuffer = new FlexBuffer();

    // data buffer used to generate complex keys
    private DataOutputBuffer _outputStream = new DataOutputBuffer() {
      @Override
      public void close() throws IOException {
        super.close();
        _dataBuffer.set(this.getData(), 0, this.getLength());
        _dataBufferSize = this.getLength();
      }
    };

    private DataInputBuffer _inputStream = new DataInputBuffer();

    public OptimizedKey(int keyType) {
      _keyType = keyType;
      _headerSize = 0;
      if ((_keyType & KEY_TYPE_LONG) != 0)
        _headerSize += 8;
      if ((_keyType & KEY_TYPE_BUFFER) != 0)
        _headerSize += 8;
    }

    /**
     * 
     * @return the buffer key value
     */
    public FlexBuffer getBufferKeyValue() {
      return _dataBuffer;
    }

    /**
     * 
     * @return a DataOutputStream - write into this stream and then close it to
     *         commit data
     * @throws IOException
     */
    public DataOutputStream getBufferKeyValueStream() throws IOException {
      _outputStream.reset();
      return _outputStream;
    }

    public int getDataBufferOffset() {
      return _dataBufferOffset;
    }

    /**
     * get optimized key size in bytes
     * 
     */
    public int getDataBufferSize() {
      return _dataBufferSize;
    }

    /**
     * get header bytes size
     * 
     */
    public int getHeaderSize() {
      return _headerSize;
    }

    /**
     * 
     * @return key type
     */
    public int getKeyType() {
      return _keyType;
    }

    /**
     * 
     * @return the long key value
     */
    public long getLongKeyValue() {
      return _longKeyValue;
    }

    /**
     * initialize the optimized key object from the passed in key/value data
     * 
     * @param data
     * @param offset
     * @param length
     * @return
     * @throws IOException
     */
    public int initFromKeyValuePair(byte[] keyBytes, int keyOffset, int keyLength, byte[] valueBytes, int valueOffset,
        int valueLength) throws IOException {

      _inputStream.reset(keyBytes, keyOffset, keyLength);

      readHeader(_inputStream);

      if ((_keyType & KEY_TYPE_BUFFER) != 0 && _dataBufferSize != 0) {
        // initialize data buffer ...
        _dataBuffer.set(valueBytes, valueOffset + valueLength - _dataBufferSize, _dataBufferSize);
      }

      return _dataBufferSize;
    }

    /**
     * 
     */
    public int readHeader(DataInputStream stream) throws IOException {
      // read header ...
      if ((_keyType & KEY_TYPE_LONG) != 0) {
        _longKeyValue = stream.readLong();
      }
      if ((_keyType & KEY_TYPE_BUFFER) != 0) {
        _dataBufferSize = stream.readInt();
        _dataBufferOffset = stream.readInt();
      }
      return _headerSize;
    }

    public void setDataBufferOffset(int dataBufferOffset) {
      _dataBufferOffset = dataBufferOffset;
    }

    /** 
     * 
     */
    public void setLongKeyValue(long value) {
      _longKeyValue = value;
    }

    /**
     * 
     * 
     * @param outputStream
     * @return
     * @throws IOException
     */
    public int writeBufferToStream(DataOutputStream outputStream) throws IOException {
      _dataBufferSize = 0;
      if ((_keyType & KEY_TYPE_BUFFER) != 0) {
        outputStream.write(_dataBuffer.get(), _dataBuffer.getOffset(), _dataBuffer.getCount());
        _dataBufferSize += _dataBuffer.getCount();
      }
      return _dataBufferSize;
    }

    public int writeHeaderToStream(DataOutputStream outputStream) throws IOException {
      if ((_keyType & KEY_TYPE_LONG) != 0) {
        outputStream.writeLong(_longKeyValue);
      }
      if ((_keyType & KEY_TYPE_BUFFER) != 0) {
        outputStream.writeInt(_dataBuffer.getCount());
        outputStream.writeInt(_dataBufferOffset);
      }
      return _headerSize;
    }

  }

  /**
   * compare two optimized key values (previously emitted as buffers by generate
   * method)
   * 
   * @param key1Data
   * @param key1Offset
   * @param key1Length
   * @param key2Data
   * @param key2Offset
   * @param key2Length
   * @return 0 if equal,-1 if lvalue is less than rvalue, and 1 if not
   * @throws IOException
   */
  public int compareOptimizedBufferKeys(byte[] key1Data, int key1Offset, int key1Length, byte[] key2Data,
      int key2Offset, int key2Length) throws IOException {
    // default throws exception
    throw new IOException("compare optimized buffers not implemented in base class!");
  }

  /**
   * Given a key object and a value object, produce an optimized key data
   * structure
   * 
   * @param key
   *          - the key value associated with this object
   * @param value
   *          - the value associated with this object
   * @param optimizedKeyOut
   *          - the optimized key value out
   * @throws IOException
   */
  public abstract void generateOptimizedKeyForPair(KeyType key, ValueType value, OptimizedKey optimizedKeyOut)
      throws IOException;

  /**
   * Generate optimized key data given raw key/value buffers
   * 
   * @param keyData
   * @param keyOffset
   * @param keyLength
   * @param valueData
   * @param valueOffset
   * @param valueLength
   * @return
   * @throws IOException
   */
  public long generateOptimizedKeyForRawPair(byte[] keyData, int keyOffset, int keyLength, byte[] valueData,
      int valueOffset, int valueLength, OptimizedKey optimizedKeyOut) throws IOException {
    throw new IOException("generate OptimizedKeyForRawPair not implemented in base class!");
  }

  /**
   * 
   * @return the key type produced by the generator
   */
  public abstract int getGeneratedKeyType();

}
