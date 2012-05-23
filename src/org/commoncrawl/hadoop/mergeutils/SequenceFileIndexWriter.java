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

@SuppressWarnings("unchecked")
public interface SequenceFileIndexWriter<KeyType extends WritableComparable, ValueType extends Writable> {

  /**
   * flush and close the index file
   * 
   * @throws IOException
   */
  void close() throws IOException;

  /**
   * index a given item
   * 
   * @param keyData
   *          key bytes
   * @param keyOffset
   *          key offset
   * @param keyLength
   *          key length
   * @param valueData
   *          value bytes
   * @param valueOffset
   *          value offset
   * @param valueLength
   *          value length
   * @param writerPosition
   *          the sequence writer file position for the current item
   */
  void indexItem(byte[] keyData, int keyOffset, int keyLength, byte[] valueData, int valueOffset, int valueLength,
      long writerPosition) throws IOException;

}
