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
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.rank.JoinQuantcastAndDomainRankStep;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.JoinByTextSortByTagMapper;
import org.commoncrawl.util.JoinMapper;
import org.commoncrawl.util.JoinValue;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * 
 * @author rana
 *
 */
public class RankAndCrawlStatsJoinStep extends CrawlPipelineStep {

  public static class Phase1Mapper implements Mapper<TextBytes, TextBytes, TextBytes, TextBytes> {

    JsonParser parser = new JsonParser();

    @Override
    public void close() throws IOException {
    }

    @Override
    public void configure(JobConf job) {
    }

    @Override
    public void map(TextBytes key, TextBytes value, OutputCollector<TextBytes, TextBytes> output, Reporter reporter)
        throws IOException {

      String domain = key.toString();
      String rootDomain = URLUtils.extractRootDomainName(domain);
      if (rootDomain != null) {
        JsonObject jsonObj = parser.parse(value.toString()).getAsJsonObject();
        jsonObj.addProperty("domain", domain);
        output.collect(new TextBytes(rootDomain), new TextBytes(jsonObj.toString()));
      }
    }
  }

  public static class Phase2Reducer implements Reducer<TextBytes, JoinValue, TextBytes, TextBytes> {

    enum Counters {
      GOT_RANKDATA, GOT_CRAWLSTATS_RECORD, GOT_CRAWLSTATS_WITHOUT_RANK_DATA, MIXED_IN_RANKDATA_INTO_CRAWLSTATS
    }

    TextBytes realKey = new TextBytes();

    TextBytes tagKey = new TextBytes();
    JsonObject activeRankData = null;

    String activeRankDomain = null;

    JsonParser parser = new JsonParser();

    @Override
    public void close() throws IOException {

    }

    @Override
    public void configure(JobConf job) {

    }

    @Override
    public void reduce(TextBytes key, Iterator<JoinValue> values, OutputCollector<TextBytes, TextBytes> output,
        Reporter reporter) throws IOException {
      JoinByTextSortByTagMapper.getKeyFromCompositeKey(key, realKey);
      JoinByTextSortByTagMapper.getTagFromCompositeKey(key, tagKey);

      if (tagKey.toString().equals(TAG_RANKDATA)) {
        reporter.incrCounter(Counters.GOT_RANKDATA, 1);
        activeRankData = parser.parse(values.next().getTextValue().toString()).getAsJsonObject();
        activeRankDomain = realKey.toString();
      } else if (tagKey.toString().equals(TAG_CRAWLSTATS)) {
        reporter.incrCounter(Counters.GOT_CRAWLSTATS_RECORD, 1);
        if (activeRankDomain != null) {
          if (!activeRankDomain.equals(realKey.toString())) {
            reporter.incrCounter(Counters.GOT_CRAWLSTATS_WITHOUT_RANK_DATA, 1);
            activeRankDomain = null;
            activeRankData = null;
          }
        }

        while (values.hasNext()) {
          JoinValue value = values.next();
          JsonObject crawlStats = parser.parse(value.getTextValue().toString()).getAsJsonObject();
          if (activeRankData != null) {
            crawlStats.addProperty("dR", activeRankData.get("dR").getAsDouble());
            crawlStats.addProperty("qR", activeRankData.get("qR").getAsInt());

            reporter.incrCounter(Counters.MIXED_IN_RANKDATA_INTO_CRAWLSTATS, 1);
          }

          String realDoamin = crawlStats.get("domain").getAsString();
          crawlStats.remove("domain");

          output.collect(new TextBytes(realDoamin), new TextBytes(crawlStats.toString()));
        }
      }
    }

  }

  private static final Log LOG = LogFactory.getLog(RankAndCrawlStatsJoinStep.class);

  public static final String OUTPUT_DIR_NAME = "rankCrawlStatsJoin";

  static final String TAG_RANKDATA = "1";
  static final String TAG_CRAWLSTATS = "2";

  public RankAndCrawlStatsJoinStep(CrawlPipelineTask task) {
    super(task, "Rank And CrawlStats Join", OUTPUT_DIR_NAME);
  }

  @Override
  public Log getLogger() {
    return LOG;
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {
    
    Path phase1Output = JobBuilder.tempDir(getConf(), OUTPUT_DIR_NAME + "-Phase1");

    // ok output crawl stats by root domain
    JobConf job = new JobBuilder(getDescription() + "-Phase1", getConf())

    //.input(getOutputDirForStep(DNSAndCrawlStatsJoinStep.class))
    .input(getOutputDirForStep(CrawlDBStatsCollectorStep.class))
    .inputIsSeqFile()
    .mapper(Phase1Mapper.class)
    .keyValue(TextBytes.class, TextBytes.class)
    .output(phase1Output).outputIsSeqFile()
    .numReducers(CrawlEnvironment.NUM_DB_SHARDS)
    .build();

    JobClient.runJob(job);

    ImmutableMap<Path, String> step2InputMapping 
      = new ImmutableMap.Builder<Path, String>()
        .put(phase1Output,TAG_CRAWLSTATS)
        .put(getOutputDirForStep(JoinQuantcastAndDomainRankStep.class), TAG_RANKDATA)
        .build();

    job = new JobBuilder(getDescription() + " - step 2", getConf())
      .inputIsSeqFile()
      .inputs(ImmutableList.copyOf(step2InputMapping.keySet()))
      .mapper(JoinByTextSortByTagMapper.class)
      .mapperKeyValue(TextBytes.class, JoinValue.class)
      .outputKeyValue(TextBytes.class, TextBytes.class)
      .reducer(Phase2Reducer.class, false)
      .partition(JoinByTextSortByTagMapper.Partitioner.class)
      .numReducers(CrawlEnvironment.NUM_DB_SHARDS).outputIsSeqFile()
      .output(outputPathLocation)
      .build();

    JoinMapper.setPathToTagMapping(step2InputMapping, job);

    JobClient.runJob(job);
  }
}
