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

package org.commoncrawl.mapred.pipelineV3.domainmeta.crawlstats;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.JoinMapper;
import org.commoncrawl.util.JoinValue;
import org.commoncrawl.util.TextBytes;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * 
 * @author rana
 *
 */
public class DNSAndCrawlStatsJoinStep extends CrawlPipelineStep implements
    Reducer<TextBytes, JoinValue, TextBytes, TextBytes> {

  enum Counters {
    GOT_CRAWL_STATS, GOT_DNS_STASTS
  }

  public static final String OUTPUT_DIR_NAME = "dnsCrawlStatsJoin";

  private static final Log LOG = LogFactory.getLog(DNSAndCrawlStatsJoinStep.class);

  JsonParser parser = new JsonParser();

  public DNSAndCrawlStatsJoinStep() {
    super(null, null, null);
  }

  public DNSAndCrawlStatsJoinStep(CrawlPipelineTask task) {
    super(task, "DNS to CrawlStats Join", OUTPUT_DIR_NAME);
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public void configure(JobConf job) {

  }

  @Override
  public Log getLogger() {
    return LOG;
  }

  @Override
  public void reduce(TextBytes key, Iterator<JoinValue> values, OutputCollector<TextBytes, TextBytes> output,
      Reporter reporter) throws IOException {
    JsonObject crawlStatsObject = null;
    JsonObject dnsStats = null;

    while (values.hasNext()) {

      JoinValue value = values.next();

      if (value.getTag().toString().equals(WWWPrefixStatsWriterStep.OUTPUT_DIR_NAME)) {
        reporter.incrCounter(Counters.GOT_CRAWL_STATS, 1);
        crawlStatsObject = parser.parse(value.getTextValue().toString()).getAsJsonObject();
      } else {
        reporter.incrCounter(Counters.GOT_DNS_STASTS, 1);
        JsonObject dnsStatsItem = parser.parse(value.getTextValue().toString()).getAsJsonObject();
        if (dnsStats == null) {
          dnsStats = new JsonObject();
        }

        String domain = dnsStatsItem.get("domain").getAsString();
        dnsStatsItem.remove("domain");
        dnsStats.add(domain, dnsStatsItem);
      }
    }
    if (crawlStatsObject == null) {
      crawlStatsObject = new JsonObject();
    }
    if (dnsStats != null) {
      crawlStatsObject.add("dnsStats", dnsStats);
    }

    output.collect(key, new TextBytes(crawlStatsObject.toString()));
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {
    ImmutableList<Path> inputs = new ImmutableList.Builder<Path>().add(
        getOutputDirForStep(WWWPrefixStatsWriterStep.class)).add(getOutputDirForStep(DNSFailuresCollectorStep.class))
        .build();

    JobConf job = new JobBuilder(getDescription(), getConf())

    .inputs(inputs).inputIsSeqFile().mapper(JoinMapper.class).mapperKeyValue(TextBytes.class, JoinValue.class).reducer(
        DNSAndCrawlStatsJoinStep.class, false).outputKeyValue(TextBytes.class, TextBytes.class).reducer(
        DNSAndCrawlStatsJoinStep.class, false).numReducers(CrawlEnvironment.NUM_DB_SHARDS / 2).outputIsSeqFile()
        .output(outputPathLocation)

        .build();

    JobClient.runJob(job);
  }

}
