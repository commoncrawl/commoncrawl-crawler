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

package org.commoncrawl.crawl.database.crawlpipeline.merger.mappers;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.nutch.crawl.CrawlDatum;
import org.commoncrawl.protocol.CrawlDatumAndMetadata;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.internal.CrawlDatumHelper;
import org.commoncrawl.util.internal.URLUtils;

/**
 * Transform a CrawlDatum into a CrawlDatumAndMetadata
 * 
 * @author rana
 *
 */
public class CrawlDatumTransformer implements
      Mapper<Text, CrawlDatum, URLFPV2, CrawlDatumAndMetadata> {
	
	enum Counters { 
		BAD_URL_DETECTED
	}
	

    @Override
    public void map(Text key, CrawlDatum value,
        OutputCollector<URLFPV2, CrawlDatumAndMetadata> output,
        Reporter reporter) throws IOException {
      // ok we want to invert the data ...
      String url = key.toString();
      // first get a urlfpv2
      URLFPV2 fp = URLUtils.getURLFPV2FromURL(url);
      if (fp == null) {
        reporter.incrCounter(Counters.BAD_URL_DETECTED, 1);
      } else {
        // construct a composite object from the datum ...
        CrawlDatumAndMetadata metadata = CrawlDatumHelper
            .crawlDatumAndMetadataFromDatum(value);
        metadata.getUrlAsTextBytes().set(key);
        metadata.setFieldDirty(CrawlDatumAndMetadata.Field_URL);
        // output
        output.collect(fp, metadata);
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