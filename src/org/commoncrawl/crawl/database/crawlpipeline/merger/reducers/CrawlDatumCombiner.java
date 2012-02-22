/**
 * Copyright 2008 - CommonCrawl Foundation
 * 
 * CommonCrawl licenses this file to you under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.commoncrawl.crawl.database.crawlpipeline.merger.reducers;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.nutch.crawl.CrawlDatum;
import org.commoncrawl.protocol.CrawlDatumAndMetadata;
import org.commoncrawl.protocol.URLFPV2;

/** 
 * Combiner used to reduce complexity of merge
 * 
 * @author rana
 *
 */
public class CrawlDatumCombiner implements
      Reducer<URLFPV2, CrawlDatumAndMetadata, URLFPV2, CrawlDatumAndMetadata> {

    @Override
    public void reduce(URLFPV2 key, Iterator<CrawlDatumAndMetadata> values,
        OutputCollector<URLFPV2, CrawlDatumAndMetadata> output,
        Reporter reporter) throws IOException {
      boolean sawLinkedDatumAlready = false;
      while (values.hasNext()) {
        CrawlDatumAndMetadata datumAndMetadata = values.next();

        if (datumAndMetadata.getStatus() == CrawlDatum.STATUS_LINKED) {
          if (!sawLinkedDatumAlready) {
            output.collect(key, datumAndMetadata);
            sawLinkedDatumAlready = true;
          }
        } else {
          output.collect(key, datumAndMetadata);
        }
      }
    }

    @Override
    public void configure(JobConf job) {
      // TODO Auto-generated method stub

    }

    @Override
    public void close() throws IOException {
      // TODO Auto-generated method stub

    }

  }