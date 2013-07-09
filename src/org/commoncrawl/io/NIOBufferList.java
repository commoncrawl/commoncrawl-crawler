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
package org.commoncrawl.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 
 * NIOBufferList - ByteBuffer container with separate read and write cursors (in
 * order to faciliate a producer / consumer io model )
 * 
 * @author rana
 */

public final class NIOBufferList {

  public enum CRLFReadState {
    NONE, GOT_CR, DONE
  }

  /**
   * Min / Max Buffer Size Constants
   */
  private static final int MIN_BUF_SIZE = 4096;

  /**
   * Data Members
   * 
   */

  private static final int MAX_BUF_SIZE = MIN_BUF_SIZE << 4;
  /** Buffer Queue */
  private LinkedList<ByteBuffer> _bufferList = new LinkedList<ByteBuffer>();
  /** size in bytes of the buffers in the buffer list */
  private int _bufferListBytes = 0;
  /** Active Read Buffer Pointer */
  private ByteBuffer _readBuffer = null;
  /** Active Write Buffer Pointer */
  private ByteBuffer _writeBuffer = null;
  /** Last Allocated ByteBuffer Size */
  private int _lastWriteBufSize = 0;
  /** Minimum / Maximum Buffer Sizes */
  private int _minBufferSize = MIN_BUF_SIZE;
  private int _maxBufferSize = MAX_BUF_SIZE;
  /** blocking consumer support **/
  private NIODataSink _consumer = null;
  /** read event **/
  private ReentrantLock _readLock = null;

  private Condition _readEvent = null;

  /** internal - get the next read buffer from the queue */
  private synchronized ByteBuffer _getNextReadBuf() throws IOException {

    while (_readBuffer == null || _readBuffer.hasRemaining() == false) {

      _readBuffer = null;

      // once we reach here... we can recover the next buffer in the list ...
      if (!_bufferList.isEmpty()) {
        _readBuffer = _bufferList.removeFirst();
        _bufferListBytes -= _readBuffer.remaining();
      }
      // now if the buffer is null break out ...
      if (_readBuffer == null) {
        break;
      }
    }

    if (_readBuffer != null && _readEvent != null) {
      _readLock.lock();

      // signal the read event, thus potentially waking up a sleeping writer ...
      _readEvent.signal();
      _readEvent = null;

      _readLock.unlock();
    }

    return _readBuffer;
  }

  /**
   * allocate a new byte buffer (or potentially retrieve a buffer from the pool
   * )
   */
  public final ByteBuffer allocateBuffer() throws IOException {

    int desiredAllocSize = Math.max(_minBufferSize, Math.min(_maxBufferSize, _lastWriteBufSize << 1));
    return ByteBuffer.allocate(desiredAllocSize);
  }

  /** Returns the number of readable bytes */
  public int available() {

    int size = 0;
    if (_readBuffer != null) {
      size += _readBuffer.remaining();
    }

    synchronized (this) {
      size += _bufferListBytes;
    }
    return size;
  }

  /**
   * flush any partial writes and add the resulting ByteBuffer to the read queue
   */
  public void flush() {

    if (_writeBuffer != null && _writeBuffer.position() != 0) {

      _lastWriteBufSize = Math.max(_minBufferSize, _writeBuffer.position());

      synchronized (this) {
        // get the buffer ready for a read ...
        _writeBuffer.flip();
        if (_readBuffer == _writeBuffer) {
          throw new RuntimeException("read and write buffer pointers identical !!!");
        }

        // now ... tricky ... if blocking consumer is specified ... delegate
        // buffer queuing to it ..
        if (getSink() != null) {
          // and pass on the buffer to the consumer ...
          getSink().available(_writeBuffer);
        } else {
          // increment queued bytes count ...
          _bufferListBytes += _writeBuffer.remaining();
          // and add to our internal list ...
          _bufferList.add(_writeBuffer);
        }

        // either way clear our reference to the buffer ...
        _writeBuffer = null;
      }
    }
  }

  /** get set consumer **/
  public NIODataSink getSink() {
    return _consumer;
  }

  /**
   * get a write buffer
   * 
   * @return a ByteBuffer the caller can write into. Note: NIOBufferList OWNS
   *         the buffer, and will flush it when appropriate. Caller only writes
   *         into it.
   * @throws IOException
   */
  public ByteBuffer getWriteBuf() throws IOException {

    if (_writeBuffer == null || _writeBuffer.hasRemaining() == false) {

      flush();

      _writeBuffer = allocateBuffer();

    }
    return _writeBuffer;
  }

  /** Returns true if there is data available (to be read) in the buffer */
  public boolean isDataAvailable() {
    if (_readBuffer != null && _readBuffer.remaining() != 0)
      return true;
    else {
      synchronized (this) {
        return _bufferListBytes != 0;
      }
    }
  }

  /* peek at ByteBuffer - don't get */
  public ByteBuffer peekAtWriteBuffer() {
    return _writeBuffer;
  }

  /** put back (at the top of the buffer queue) a previously read byte buffer */
  public synchronized void putBack(ByteBuffer existingReadBuffer) throws IOException {

    if (existingReadBuffer == null)
      throw new IOException("Invalid Call to putBack - incoming Buffer is null!");
    if (_readBuffer != null) {
      if (_readBuffer == existingReadBuffer) {
        throw new IOException("Invalid Call to putBack - Trying to put back current read buffer!");
      }
      if (_readBuffer == _writeBuffer) {
        throw new RuntimeException("Critical Error!! read and write buffer pointers identical !!!");
      }
      // update buffer list bytes ...
      _bufferListBytes += _readBuffer.remaining();
      // put back the next buffer ...
      _bufferList.addFirst(_readBuffer);
    }
    _readBuffer = existingReadBuffer;
    if (_readBuffer == _writeBuffer) {
      throw new RuntimeException("Critical Error!! read and write buffer pointers identical !!!");
    }
  }

  /** obtain the next readable ByteBuffer */
  public synchronized ByteBuffer read() throws IOException {
    _getNextReadBuf();
    ByteBuffer bufferOut = _readBuffer;
    _readBuffer = null;
    return bufferOut;
  }

  /** read into a pre-allocated byte buffer */
  public int read(byte[] buffer) throws IOException {
    return read(buffer, 0, buffer.length);
  }

  /**
   * read into a pre-allocated byte buffer
   * 
   * @param offset
   *          the offset (in the byte buffer) where data should be stored
   * @param count
   *          the maximum number of bytes to store in the byte buffer
   * @return
   * @throws IOException
   */
  public int read(byte[] buffer, int offset, int count) throws IOException {

    int bytesRead = 0;

    // retrieve the initial buffer ...
    ByteBuffer readBuffer = _getNextReadBuf();

    while (count > 0) {

      if (readBuffer == null) {
        break;
      }
      int bytesAvailable = Math.min(count, readBuffer.remaining());

      readBuffer.get(buffer, offset, bytesAvailable);

      bytesRead += bytesAvailable;

      offset += bytesAvailable;
      count -= bytesAvailable;

      readBuffer = _getNextReadBuf();
    }

    return (bytesRead != 0) ? bytesRead : -1;
  }

  /**
   * get the next ByteBuffer worth of data
   * 
   * @param desiredMinSize
   *          the minimum size of available data required in the ByteBuffer
   * @return
   * @throws IOException
   */
  public ByteBuffer read(int desiredMinSize) throws IOException {

    // get top buffer in queue
    ByteBuffer bufferOut = read();

    if (bufferOut != null) {

      int shortBy = Math.max(0, desiredMinSize - bufferOut.remaining());

      // if less than min size ...
      if (shortBy != 0) {

        // check to see if subsequent buffer has remainder ...
        ByteBuffer nextBuffer = _getNextReadBuf();

        if (nextBuffer != null) {
          // compact the existing buffer ...
          bufferOut.compact();
          // if next buffer has remainder ...
          // and source buffer can accept it ...
          if (bufferOut.capacity() - bufferOut.limit() >= shortBy && nextBuffer.remaining() >= shortBy) {
            // increase limit ...
            int originalLimit = bufferOut.limit();
            int newLimit = originalLimit + shortBy;
            bufferOut.limit(newLimit);

            for (; originalLimit < newLimit; ++originalLimit) {
              bufferOut.put(originalLimit, nextBuffer.get());
            }
          }
        }
      }
    }
    return bufferOut;
  }

  public CRLFReadState readCRLFLine(StringBuffer accumulator, int lineMax, CRLFReadState lastReadState)
      throws IOException {

    ByteBuffer currentBuffer = null;

    while (lastReadState != CRLFReadState.DONE && (currentBuffer = _getNextReadBuf()) != null) {

      // read up to the end of the buffer ...
      while (currentBuffer.position() < currentBuffer.limit()) {

        char currentChar = (char) currentBuffer.get();

        if ((lastReadState != CRLFReadState.GOT_CR && currentChar == '\r')) {
          lastReadState = CRLFReadState.GOT_CR;
        } else if (lastReadState == CRLFReadState.GOT_CR && currentChar == '\n') {
          return CRLFReadState.DONE;
        } else {
          lastReadState = CRLFReadState.NONE;
          // add it to the buffer ...
          accumulator.append(currentChar);

          if (accumulator.length() > lineMax) {
            throw new IOException("Line Size Limit Reached With No Terminator!");
          }
        }
      }
    }
    return lastReadState;
  }

  /** Reset State (Release buffers) */
  public void reset() {
    _bufferList.clear();
    _bufferListBytes = 0;
    _readBuffer = null;
    _writeBuffer = null;
    _lastWriteBufSize = 0;
  }

  public void setMaxBufferSize(int size) {
    _maxBufferSize = size;
  }

  /** Set Min/Max Buffer Size */
  public void setMinBufferSize(int size) {
    _minBufferSize = size;
  }

  public synchronized void setReadEvent(ReentrantLock lock, Condition readEvent) {
    _readLock = lock;
    _readEvent = readEvent;
  }

  public void setSink(NIODataSink consumer) {
    _consumer = consumer;
  }

  /* Skip ahead from the current Read Cursor position */
  public long skip(long skipAmount) throws IOException {

    long amountSkipped = 0;

    // retrieve the initial buffer ...
    ByteBuffer readBuffer = _getNextReadBuf();

    while (skipAmount != 0) {

      if (readBuffer == null) {
        break;
      }

      long bytesAvailable = Math.min(skipAmount, readBuffer.remaining());

      readBuffer.position(readBuffer.position() + (int) bytesAvailable);

      amountSkipped += bytesAvailable;
      skipAmount -= bytesAvailable;

      readBuffer = _getNextReadBuf();
    }
    return (amountSkipped != 0) ? amountSkipped : -1;
  }

  /*
   * copy the specified byte buffer into a ByteBuffer and add to the read queue
   * 
   * @param offset offset into the byte buffer
   * 
   * @param size size of the byte buffer
   */
  public void write(byte[] buffer, int offset, int size) throws IOException {

    while (size > 0) {

      ByteBuffer writeBuffer = getWriteBuf();

      int available = Math.min(writeBuffer.remaining(), size);

      writeBuffer.put(buffer, offset, available);

      offset += available;
      size -= available;
    }
  }

  /* add a previously written buffer to the tail of the read queue */
  public void write(ByteBuffer buffer) throws IOException {
    // NOTE: buffer should already be flipped for READ

    // flush existing open buffer ...
    if (_writeBuffer != buffer) {
      flush();
    }

    // now set this as the current write buffer
    _writeBuffer = buffer;
  }

  /* write a byte value */
  public void write(int value) throws IOException {

    ByteBuffer writeBuffer = getWriteBuf();
    writeBuffer.put((byte) value);
  }
  
  @Override
  public String toString() {
    int readBufferBytes = (_readBuffer != null) ? _readBuffer.remaining() : 0;
    int listBytes = 0;
    for (ByteBuffer readBuffer : _bufferList) {
      listBytes += readBuffer.remaining();
    }
    return 
        "BufferList Size:" + _bufferList.size() 
        +" BufferListBytes:" + _bufferListBytes 
        + " BufferListBytes(Computed):" + listBytes
        + " ReadBufferBytes:" + readBufferBytes;
  }

}
