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

import java.io.ByteArrayOutputStream;

/**
 * Helper class to gain access to ByteArrayOutputStream's underlying storage
 * 
 * @author rana
 * 
 */
public final class ByteStream extends ByteArrayOutputStream {
  public ByteStream(int initialSize) {
    super(initialSize);
  }

  public byte[] getBuffer() {
    return buf;
  }
}