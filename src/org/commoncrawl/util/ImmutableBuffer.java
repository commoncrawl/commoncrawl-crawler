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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.record.Buffer;

/**
 * Wrapper class used to expose a semantically read-only Buffer via the RPC API
 * (especially in generated classes) without actually doing a wasteful copy of
 * the underlying storage.
 * 
 * @author rana
 * 
 */
public class ImmutableBuffer {

  private FlexBuffer _sourceBuffer;

  public ImmutableBuffer(FlexBuffer buffer) {
    _sourceBuffer = buffer;
  }

  /**
   * Get an input stream from the specified buffer
   * 
   * @return The InputStream bound to the underlying buffer
   */
  public InputStream getBytes() {
    return new ByteArrayInputStream(_sourceBuffer.get(), _sourceBuffer
        .getOffset(), _sourceBuffer.getCount());
  }

  /**
   * Get read-only data from the Buffer.
   * 
   * Only read-only in name, the returned buffer is actually owned by the
   * underlying Buffer object, and should not be modified under any
   * circumstances
   * 
   * @return The data is only valid between 0 and getCount() - 1.
   */
  public byte[] getReadOnlyBytes() {
    return _sourceBuffer.get();
  }

  /**
   * offset
   */
  public int getOffset() {
    return _sourceBuffer.getOffset();
  }

  /**
   * Get the current count of the buffer.
   */
  public int getCount() {
    return _sourceBuffer.getCount();
  }

  /**
   * Get the capacity, which is the maximum count that could handled without
   * resizing the backing storage.
   * 
   * @return The number of bytes
   */
  public int getCapacity() {
    return _sourceBuffer.get().length;
  }

  // inherit javadoc
  public int hashCode() {
    return _sourceBuffer.hashCode();
  }

  /**
   * Define the sort order of the Buffer.
   * 
   * @param other
   *          The other buffer
   * @return Positive if this is bigger than other, 0 if they are equal, and
   *         negative if this is smaller than other.
   */
  public int compareTo(Object other) {
    FlexBuffer otherBuffer = null;

    if (other instanceof ImmutableBuffer) {
      otherBuffer = ((ImmutableBuffer) other)._sourceBuffer;
    } else {
      otherBuffer = (FlexBuffer) other;
    }

    return _sourceBuffer.compareTo(otherBuffer);
  }

  // inherit javadoc
  public boolean equals(Object other) {

    if (other instanceof ImmutableBuffer) {
      return _sourceBuffer.equals(((ImmutableBuffer) other)._sourceBuffer);
    } else if (other instanceof Buffer) {
      return _sourceBuffer.equals((Buffer) other);
    }
    return false;
  }

  // inheric javadoc
  public String toString() {
    return _sourceBuffer.toString();
  }

  /**
   * Convert the byte buffer to a string an specific character encoding
   * 
   * @param charsetName
   *          Valid Java Character Set Name
   */
  public String toString(String charsetName)
      throws UnsupportedEncodingException {
    return _sourceBuffer.toString(charsetName);
  }

  // inherit javadoc
  public Object clone() throws CloneNotSupportedException {
    FlexBuffer result = (FlexBuffer) _sourceBuffer.clone();
    return new ImmutableBuffer(result);
  }

}
