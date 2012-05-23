/**
 * Copyright 2012 - CommonCrawl Foundation
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
package org.commoncrawl.mapred.pipelineV3.domainmeta.subdomaincounts;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.mapred.pipelineV3.domainmeta.crawlstats.CrawlStatsCollectorTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.linkstats.CountInLinksStep;
import org.commoncrawl.mapred.pipelineV3.domainmeta.quantcast.ImportQuantcastStep;
import org.commoncrawl.util.JoinValue;
import org.commoncrawl.util.TextBytes;

/**
 * 
 * @author rana
 *
 */
public class QuantcastJoiningReducer implements Reducer<TextBytes, JoinValue, TextBytes, TextBytes> {

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void configure(JobConf job) {
    // TODO Auto-generated method stub

  }

  @Override
  public void reduce(TextBytes key, Iterator<JoinValue> values, OutputCollector<TextBytes, TextBytes> output,
      Reporter reporter) throws IOException {
    int subDomainCountShard = 0;
    int quantcastRank = 0;
    int incomingDomainsCount = 0;
    String statsJSON = null;

    while (values.hasNext()) {
      JoinValue value = values.next();
      if (value.getTag().toString().equals(ImportQuantcastStep.OUTPUT_DIR_NAME)) {
        quantcastRank = (int) value.getLongValue();
      } else if (value.getTag().toString().equals(CountInLinksStep.OUTPUT_DIR_NAME)) {
        incomingDomainsCount = (int) value.getLongValue();
      } else if (value.getTag().toString().equals(CrawlStatsCollectorTask.OUTPUT_DIR_NAME)) {
        statsJSON = value.getTextValue().toString();
      } else {
        subDomainCountShard = (int) value.getLongValue();
      }
    }
    if (subDomainCountShard >= 10) {
      if (quantcastRank == 0) {
        output.collect(new TextBytes(key.toString() + " - " + subDomainCountShard + "-" + incomingDomainsCount + " "
            + statsJSON), new TextBytes());
      }
    }
  }

}
