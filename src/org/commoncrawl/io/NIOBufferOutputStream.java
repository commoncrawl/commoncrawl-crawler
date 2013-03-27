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
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 *  a simple wrapper to write data directly into byte buffers ... 
 * @author rana
 *
 */
public class NIOBufferOutputStream extends OutputStream {

  private NIOBuffer _target = null;
  private ByteBuffer _buffer = null;

  public NIOBufferOutputStream(NIOBuffer buffer) {
    _target = buffer;
  }

  private int capacity() {
    return (_buffer != null) ? _buffer.remaining() : 0;
  }

  // @Override
  @Override
  public void flush() throws IOException {
    if (_buffer != null && _buffer.position() != 0) {
      _target.write(_buffer);
      _buffer = null;
    }
    _target.flush();
  }

  private void grow() throws IOException {
    synchronized (this) {

      if (_buffer != null) {
        _target.write(_buffer);
      }
      _buffer = _target.getWriteBuffer();
    }
  }

  public void reset() {
    _buffer = null;
  }

  // @Override
  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  // @Override
  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    while (len != 0) {
      if (capacity() == 0)
        grow();

      // calculate size to write based on buffer capacity
      final int sizeAvailable = _buffer.remaining();
      final int sizeToWrite = Math.min(sizeAvailable, len);

      // write data ...
      _buffer.put(b, off, sizeToWrite);

      // and increment pointers ...
      off += sizeToWrite;
      len -= sizeToWrite;
    }
  }

  // @Override
  @Override
  public void write(int b) throws IOException {
    if (capacity() == 0)
      grow();
    _buffer.put((byte) b);
  }
}
