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
public class WWWPrefixStatsWriterStep extends CrawlPipelineStep {

  public static class JoinReducer implements Reducer<TextBytes, JoinValue, TextBytes, TextBytes> {

    enum Counters {
      GOT_CRAWL_STATS, GOT_WWW_PREFIX_STATS
    }

    JsonParser parser = new JsonParser();

    TextBytes valueOut = new TextBytes();

    @Override
    public void close() throws IOException {

    }

    @Override
    public void configure(JobConf job) {

    }

    @Override
    public void reduce(TextBytes key, Iterator<JoinValue> values, OutputCollector<TextBytes, TextBytes> output,
        Reporter reporter) throws IOException {

      JsonObject crawlStatsObject = null;
      JsonObject wwwPrefixStatsObject = null;

      while (values.hasNext()) {

        JoinValue value = values.next();

        if (value.getTag().toString().equals(CrawlDBRedirectStatsCollectorStep.OUTPUT_DIR_NAME)) {
          reporter.incrCounter(Counters.GOT_CRAWL_STATS, 1);
          crawlStatsObject = parser.parse(value.getTextValue().toString()).getAsJsonObject();
        } else {
          reporter.incrCounter(Counters.GOT_WWW_PREFIX_STATS, 1);
          wwwPrefixStatsObject = parser.parse(value.getTextValue().toString()).getAsJsonObject();
        }
      }

      if (crawlStatsObject != null) {
        if (wwwPrefixStatsObject != null) {
          crawlStatsObject.addProperty("www", wwwPrefixStatsObject.get("www").getAsInt());
          crawlStatsObject.addProperty("nonWWW", wwwPrefixStatsObject.get("nonWWW").getAsInt());
        }
      } else {
        crawlStatsObject = wwwPrefixStatsObject;
      }

      if (crawlStatsObject != null) {
        valueOut.set(crawlStatsObject.toString());
        output.collect(key, valueOut);
      }
    }

  }

  public static final String OUTPUT_DIR_NAME = "wwwPrefixStatsMerged";

  private static final Log LOG = LogFactory.getLog(WWWPrefixStatsWriterStep.class);

  public WWWPrefixStatsWriterStep(CrawlPipelineTask task) {
    super(task, "Merge WWW Prefix Stats", OUTPUT_DIR_NAME);
  }

  @Override
  public Log getLogger() {
    return LOG;
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {

    ImmutableList<Path> inputs = new ImmutableList.Builder<Path>().add(
        getOutputDirForStep(CrawlDBRedirectStatsCollectorStep.class)).add(
        getOutputDirForStep(WWWPrefixStatsCollectorStep.class)).build();

    JobConf job = new JobBuilder(getDescription(), getConf())

    .inputs(inputs).inputIsSeqFile().mapper(JoinMapper.class).mapperKeyValue(TextBytes.class, JoinValue.class).reducer(
        JoinReducer.class, false).outputKeyValue(TextBytes.class, TextBytes.class).reducer(JoinReducer.class, false)
        .numReducers(CrawlEnvironment.NUM_DB_SHARDS / 2).outputIsSeqFile().output(outputPathLocation)

        .build();

    JobClient.runJob(job);
  }
}
