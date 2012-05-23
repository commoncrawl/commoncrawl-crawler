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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class NIOBufferListInputStream extends InputStream {

  private NIOBufferList _source = null;
  private ByteBuffer _buffer = null;

  public NIOBufferListInputStream(NIOBufferList source) {
    _source = source;
  }

  // @Override
  @Override
  public int available() throws IOException {

    int available = 0;

    if (_buffer != null) {
      available += _buffer.remaining();
    }
    available += _source.available();

    return available;
  }

  @Override
  public void close() throws IOException {

    if (_buffer != null && _buffer.remaining() != 0) {
      if (_source != null) {
        _source.putBack(_buffer);
      }
    }

    super.close();
  }

  private void ensureBuffer() throws IOException {
    if (_buffer == null || _buffer.remaining() == 0) {
      getNext();
    }
  }

  private void getNext() throws IOException {
    _buffer = null;
    _buffer = _source.read();
  }

  // @Override
  @Override
  public int read() throws IOException {
    ensureBuffer();
    if (_buffer != null) {
      return (_buffer.get() & 0xff);
    }
    return -1;
  }

  // @Override
  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  // @Override
  @Override
  public int read(byte[] b, int off, int len) throws IOException {

    int sizeOut = 0;

    while (len != 0) {

      ensureBuffer();

      if (_buffer == null) {
        break;
      }

      final int sizeAvailable = _buffer.remaining();
      final int sizeToRead = Math.min(sizeAvailable, len);

      _buffer.get(b, off, sizeToRead);

      len -= sizeToRead;
      sizeOut += sizeToRead;
      off += sizeToRead;
    }
    return (sizeOut != 0) ? sizeOut : -1;
  }

  @Override
  public void reset() {
    _buffer = null;
  }

  // @Override
  @Override
  public long skip(long skipAmount) throws IOException {

    long skipAmountOut = 0;

    while (skipAmount != 0) {

      ensureBuffer();

      if (_buffer == null) {
        break;
      }

      final long sizeAvailable = _buffer.remaining();
      final int sizeToSkip = (int) Math.min(sizeAvailable, skipAmount);

      _buffer.position(_buffer.position() + sizeToSkip);

      skipAmount -= sizeToSkip;
      skipAmountOut += sizeToSkip;
    }
    return skipAmount;
  }

  /**
   * create and return a substream of a desired size - consume desired size
   * bytes from source stream.
   * 
   * 
   * @param desiredStreamSize
   *          - Desired Stream Size. If current stream contains less than
   *          disired size bytes an EOFException is thrown.
   * @return new NIOBufferListInputStream contstrained to desired stream size
   * @throws IOException
   */
  public NIOBufferListInputStream subStream(int desiredStreamSize) throws IOException {
    // throw EOF if we don't have enough bytes to service the request
    if (available() < desiredStreamSize) {
      throw new EOFException();
    }
    // otherwise ... allocate a new buffer list ...
    NIOBufferList bufferList = new NIOBufferList();

    int sizeOut = 0;
    int len = desiredStreamSize;

    // walk buffers from existing stream + source buffer list ...
    while (len != 0) {
      // grab new ByteBuffer from buffer list if necessary ...
      ensureBuffer();

      // if we could get another buffer from list. bail...
      if (_buffer == null) {
        break;
      }

      // calculate bytes available
      final int sizeAvailable = _buffer.remaining();
      // and bytes to read this iteration ...
      final int sizeToRead = Math.min(sizeAvailable, len);
      // if we can consume entire buffer ...
      if (sizeAvailable <= sizeToRead) {
        // slice the existing buffer ...
        ByteBuffer buffer = _buffer.slice();
        // position it to limit (bufferList.write flips it, so we must do this).
        buffer.position(buffer.limit());
        // add to buffer list via write
        bufferList.write(buffer);
        // null out this buffer as it has been fully consumed
        _buffer = null;
      } else {
        // slice the existing buffer
        ByteBuffer buffer = _buffer.slice();
        // reset limit on new buffer
        buffer.limit(buffer.position() + sizeToRead);
        // position the new buffer to limit (to facilitate flip in write call)
        buffer.position(buffer.limit());
        // add it to buffer list
        bufferList.write(buffer);
        // and increment position of source buffer
        _buffer.position(_buffer.position() + sizeToRead);
      }

      len -= sizeToRead;
      sizeOut += sizeToRead;
    }
    // flush any trailing write buffer in new list ...
    bufferList.flush();

    if (sizeOut != desiredStreamSize) {
      throw new EOFException();
    }

    return new NIOBufferListInputStream(bufferList);
  }

}
