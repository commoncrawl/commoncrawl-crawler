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

package org.commoncrawl.util;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

/**
 * ByteBuffer based input stream that also implements Seekable and Positionable,
 * thus allowing it to emulate FSDataInputStream
 * 
 * @author rana
 * 
 */
public class FSByteBufferInputStream extends BufferedInputStream implements
    Seekable, PositionedReadable {

  ByteBuffer _source;

  public FSByteBufferInputStream(ByteBuffer in) {
    super(new ByteBufferInputStream(in), in.limit());
    _source = in;

  }

  public long getPos() throws IOException {
    return _source.position();
  }

  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }
    _source.position(_source.position() + (int) n);
    return n;
  }

  public void seek(long pos) throws IOException {
    if (pos < 0) {
      return;
    }
    // optimize: check if the pos is in the buffer
    long end = _source.position();
    long start = end - count;
    if (pos >= start && pos < end) {
      this.pos = (int) (pos - start);
      return;
    }

    // invalidate buffer
    this.pos = 0;
    this.count = 0;

    _source.position((int) pos);
  }

  public boolean seekToNewSource(long targetPos) throws IOException {
    pos = 0;
    count = 0;
    _source.position((int) targetPos);
    return true;
  }

  public int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
    _source.mark();
    _source.position((int) position);
    int bytesToRead = Math.min(length, _source.remaining());
    _source.get(buffer, offset, bytesToRead);
    _source.reset();
    return bytesToRead;
  }

  public void readFully(long position, byte[] buffer, int offset, int length)
      throws IOException {
    _source.mark();
    _source.position((int) position);
    _source.get(buffer, offset, length);
    _source.reset();
  }

  public void readFully(long position, byte[] buffer) throws IOException {
    _source.mark();
    _source.position((int) position);
    _source.get(buffer);
    _source.reset();
  }
}