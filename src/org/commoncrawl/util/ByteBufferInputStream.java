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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Helper class that wraps a ByteBuffer as an InputStream
 * 
 * @author rana
 * 
 */
public class ByteBufferInputStream extends InputStream {

  ByteBuffer _source;

  public ByteBufferInputStream(ByteBuffer source) {
    _source = source;
  }

  @Override
  public synchronized int read() throws IOException {
    if (!_source.hasRemaining()) {
      return -1;
    }
    return _source.get() & 0xff;
  }

  @Override
  public synchronized int read(byte[] bytes, int off, int len)
      throws IOException {
    // Read only what's left
    len = Math.min(len, _source.remaining());
    _source.get(bytes, off, len);
    return len;
  }
}
