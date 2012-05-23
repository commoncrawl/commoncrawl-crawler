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
package org.commoncrawl.mapred.segmenter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.crawl.database.SegmentGeneratorBundleKey;
import org.commoncrawl.crawl.database.SegmentGeneratorItemBundle;

/**
 * 
 * @author rana
 *
 */
public class BundleKeyPartitioner implements Partitioner<SegmentGeneratorBundleKey,SegmentGeneratorItemBundle> {

  static final Log LOG = LogFactory.getLog(BundleKeyPartitioner.class);
      
  public int getPartition(SegmentGeneratorBundleKey key,SegmentGeneratorItemBundle value, int numPartitions) {
    // calculate local index based host fp % number of buckets per crawler
    int localIndex = (int)(Math.random() * bucketsPerCrawler);
    // partition by crawler id + number buckets per crawler
    return (key.getCrawlerId() * bucketsPerCrawler) + localIndex;
  }

  int bucketsPerCrawler = -1;

  public void configure(JobConf job) {
    // get buckets per crawler ... 
    bucketsPerCrawler   = job.getInt(CrawlEnvironment.PROPERTY_NUM_BUCKETS_PER_CRAWLER, 8);
  } 

}