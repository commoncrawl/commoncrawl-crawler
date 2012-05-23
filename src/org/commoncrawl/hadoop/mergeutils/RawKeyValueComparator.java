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
 * Extends KeyValuePairComparator by adding support for comparing RAW records
 * (as well as Typed records).
 * 
 * @author rana
 * 
 * @param <KeyType>
 * @param <ValueType>
 */
public interface RawKeyValueComparator<KeyType extends WritableComparable, ValueType extends Writable> extends
    KeyValuePairComparator<KeyType, ValueType> {

  /**
   * compare two key value pairs in raw buffer format
   * 
   * @param key1Data
   * @param key1Offset
   * @param key1Length
   * @param key2Data
   * @param key2Offset
   * @param key2Length
   * @param value1Data
   * @param value1Offset
   * @param value1Length
   * @param value2Data
   * @param value2Offset
   * @param value2Length
   * @return
   * @throws IOException
   */
  int compareRaw(byte[] key1Data, int key1Offset, int key1Length, byte[] key2Data, int key2Offset, int key2Length,
      byte[] value1Data, int value1Offset, int value1Length, byte[] value2Data, int value2Offset, int value2Length)
      throws IOException;;

}
