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
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * 
 * @author rana
 *
 */
public class NIOBufferInputStream extends InputStream {

  private NIOBuffer _source = null;
  private ByteBuffer _buffer = null;

  public NIOBufferInputStream(NIOBuffer source) {
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

  private void ensureBuffer() throws IOException {
    if (_buffer == null || _buffer.remaining() == 0) {
      getNext();
    }
  }

  private void getNext() throws IOException {
    if (_buffer != null) {
      _buffer.clear();
      NIOBuffer.freeReadBuffer(_buffer);
      _buffer = null;
    }
    _buffer = _source.read();
  }

  // @Override
  @Override
  public int read() throws IOException {
    while (_buffer != null) {
      if (_buffer.remaining() != 0) {
        break;
      } else {
        getNext();
      }
    }

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
    if (_buffer != null) {
      NIOBuffer.freeReadBuffer(_buffer);
      _buffer = null;
    }
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

}
