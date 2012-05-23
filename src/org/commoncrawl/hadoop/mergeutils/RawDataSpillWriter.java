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
package org.commoncrawl.hadoop.mergeutils;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Extends SpillWriter with the ability to handle RAW (as well as Typed) records
 * 
 * @author rana
 * 
 * @param <KeyType>
 * @param <ValueType>
 */
public interface RawDataSpillWriter<KeyType extends WritableComparable, ValueType extends Writable> extends
    SpillWriter<KeyType, ValueType> {

  /**
   * spill a key/value pair in raw format
   * 
   * @param keyData
   * @param keyOffset
   * @param keyLength
   * @param valueData
   * @param valueOffset
   * @param valueLength
   * @throws IOException
   */
  void spillRawRecord(byte[] keyData, int keyOffset, int keyLength, byte[] valueData, int valueOffset, int valueLength)
      throws IOException;

}
