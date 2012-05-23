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
package org.commoncrawl.hadoop.util;

import org.apache.hadoop.io.LongWritable;

/**
 * 
 * @author rana
 *
 */
public class LongWritableComparator extends LongWritable.Comparator {

  @Override
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    if (l1 == 0 && l2 != 0)
      return -1;
    else if (l1 != 0 && l2 == 0)
      return 1;
    else
      return super.compare(b1, s1, l1, b2, s2, l2);
  }

}