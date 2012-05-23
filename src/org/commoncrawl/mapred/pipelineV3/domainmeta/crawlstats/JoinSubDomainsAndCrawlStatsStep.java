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
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * 
 * @author rana
 *
 */
public class JoinSubDomainsAndCrawlStatsStep extends CrawlPipelineStep implements
    Reducer<TextBytes, JoinValue, TextBytes, TextBytes> {

  enum Counters {
    GOT_SUBDOMAIN_DATA, GOT_CRAWLSTATS_DATA, GOT_CRAWLSTATS_AND_SUBDOMAIN_DATA
  }

  public static final String OUTPUT_DIR_NAME = "subDomainDataJoined";

  private static final Log LOG = LogFactory.getLog(JoinSubDomainsAndCrawlStatsStep.class);

  JsonParser parser = new JsonParser();

  public JoinSubDomainsAndCrawlStatsStep() {
    super(null, null, null);
  }

  public JoinSubDomainsAndCrawlStatsStep(CrawlPipelineTask task) {
    super(task, "Join Subdomains and CrawlStats", OUTPUT_DIR_NAME);
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void configure(JobConf job) {
    // TODO Auto-generated method stub

  }

  @Override
  public Log getLogger() {
    return LOG;
  }

  @Override
  public void reduce(TextBytes key, Iterator<JoinValue> values, OutputCollector<TextBytes, TextBytes> output,
      Reporter reporter) throws IOException {

    JsonObject crawlStats = null;
    JsonArray subDomains = null;

    while (values.hasNext()) {
      JoinValue currentValue = values.next();
      if (currentValue.getTag().toString().equals(RankAndCrawlStatsJoinStep.OUTPUT_DIR_NAME)) {
        crawlStats = parser.parse(currentValue.getTextValue().toString()).getAsJsonObject();
        reporter.incrCounter(Counters.GOT_CRAWLSTATS_DATA, 1);
      } else if (currentValue.getTag().toString().equals(NonSuperSubdomainCollectorStep.OUTPUT_DIR_NAME)) {
        subDomains = parser.parse(currentValue.getTextValue().toString()).getAsJsonArray();
        reporter.incrCounter(Counters.GOT_SUBDOMAIN_DATA, 1);
      }
    }

    if (crawlStats != null && subDomains != null) {
      reporter.incrCounter(Counters.GOT_CRAWLSTATS_AND_SUBDOMAIN_DATA, 1);
    }
    if (crawlStats == null) {
      crawlStats = new JsonObject();
    }
    if (subDomains != null) {
      crawlStats.add("subdomains", subDomains);
    }

    output.collect(key, new TextBytes(crawlStats.toString()));
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {

    ImmutableList<Path> paths = ImmutableList.of(getOutputDirForStep(RankAndCrawlStatsJoinStep.class),
        getOutputDirForStep(NonSuperSubdomainCollectorStep.class));

    JobConf job = new JobBuilder(getDescription(), getConf())

    .inputs(paths).inputIsSeqFile().mapper(JoinMapper.class).mapperKeyValue(TextBytes.class, JoinValue.class).reducer(
        JoinSubDomainsAndCrawlStatsStep.class, false).numReducers(CrawlEnvironment.NUM_DB_SHARDS / 2).outputKeyValue(
        TextBytes.class, TextBytes.class).output(outputPathLocation).outputIsSeqFile()

    .build();

    JobClient.runJob(job);
  }

}
