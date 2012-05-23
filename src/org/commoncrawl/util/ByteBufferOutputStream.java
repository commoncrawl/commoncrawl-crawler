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
import java.io.OutputStream;

import org.apache.hadoop.record.Buffer;

/**
 * Helper class that wraps a ByteBuffer as an OutputStream
 * 
 * @author rana
 * 
 */
public class ByteBufferOutputStream extends OutputStream {
  public Buffer _buffer = new Buffer();
  public byte[] _byte   = new byte[1];

  @Override
  public void write(int b) throws IOException {
    _byte[0] = (byte) b;
    write(_byte, 0, 1);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (_buffer.getCapacity() < _buffer.getCount() + len) {
      _buffer.setCapacity(Math.max(_buffer.getCapacity() << 1, _buffer
          .getCount()
          + len));
    }
    _buffer.append(b, off, len);
  }
}
