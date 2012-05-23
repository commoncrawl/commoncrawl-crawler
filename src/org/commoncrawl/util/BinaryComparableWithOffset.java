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

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * adds offset support to BinaryComparable
 * 
 * @author rana
 *
 */
public abstract class BinaryComparableWithOffset extends BinaryComparable implements WritableComparable<BinaryComparable> {

  /**
   * get the offset into the underlying byte array  
   * @return
   */
  public abstract int getOffset();
  
  /**
   * Compare bytes from {#getBytes()}.
   * @see org.apache.hadoop.io.WritableComparator#compareBytes(byte[],int,int,byte[],int,int)
   */
  public int compareTo(BinaryComparable other) {
    if (this == other)
      return 0;
    if (other instanceof BinaryComparableWithOffset) { 
      return WritableComparator.compareBytes(getBytes(), getOffset(), getLength(),
          other.getBytes(), ((BinaryComparableWithOffset)other).getOffset(), other.getLength());
    }
    else { 
      return WritableComparator.compareBytes(getBytes(), getOffset(), getLength(),
             other.getBytes(), 0, other.getLength());
    }
  }

  /**
   * Compare bytes from {#getBytes()} to those provided.
   */
  public int compareTo(byte[] other, int off, int len) {
    return WritableComparator.compareBytes(getBytes(), getOffset(), getLength(),
      other, off, len);
  }

}
