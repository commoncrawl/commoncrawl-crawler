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
package org.commoncrawl.crawl.database.crawlpipeline.generator.segmenter;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.commoncrawl.crawl.database.SegmentGeneratorBundleKey;

/**
 * 
 * @author rana
 *
 */
public class BundleKeyComparator extends WritableComparator {

  public BundleKeyComparator() {
    super(SegmentGeneratorBundleKey.class,true);
  } 

  @Override
  public int compare(WritableComparable a, WritableComparable b) {

    SegmentGeneratorBundleKey key1 = (SegmentGeneratorBundleKey)a;
    SegmentGeneratorBundleKey key2 = (SegmentGeneratorBundleKey)b;

    if (key1.getBundleId() < key2.getBundleId())
      return -1;
    else if (key1.getBundleId() > key2.getBundleId())
      return 1;
    else { 
      return (key1.getAvgPageRank() > key2.getAvgPageRank()) ? -1 : (key1.getAvgPageRank() < key2.getAvgPageRank()) ? 1 : 0;
    }

  }
}