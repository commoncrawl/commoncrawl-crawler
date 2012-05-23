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

package org.commoncrawl.mapred.pipelineV3.domainmeta.rank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLUtils;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

/**
 * 
 * @author rana
 *
 */
public class IdSuperDomainsStep extends CrawlPipelineStep {

  public static class Stage1Reducer implements Reducer<TextBytes, TextBytes, TextBytes, TextBytes> {

    enum Counters {
      INVALID_TARGET_DOMAIN, INVALID_SOURCE_DOMAIN, SKIPPING_INLINK_FROM_SAME_ROOT
    }

    JsonParser parser = new JsonParser();

    @Override
    public void close() throws IOException {
      // TODO Auto-generated method stub

    }

    @Override
    public void configure(JobConf job) {
      // TODO Auto-generated method stub

    }

    @Override
    public void reduce(TextBytes key, Iterator<TextBytes> values, OutputCollector<TextBytes, TextBytes> output,
        Reporter reporter) throws IOException {

      String targetRootDomain = URLUtils.extractRootDomainName(key.toString());

      if (targetRootDomain == null) {
        reporter.incrCounter(Counters.INVALID_TARGET_DOMAIN, 1);
      } else {
        int inlinking = 0;

        while (values.hasNext()) {
          JsonObject linkData = parser.parse(values.next().toString()).getAsJsonObject();
          if (linkData.has("from")) {
            String inlinkingDomain = linkData.get("from").getAsString();
            String inlinkingRootDomain = URLUtils.extractRootDomainName(inlinkingDomain);

            if (inlinkingRootDomain == null) {
              reporter.incrCounter(Counters.INVALID_SOURCE_DOMAIN, 1);
            } else {
              if (inlinkingRootDomain.equals(targetRootDomain)) {
                reporter.incrCounter(Counters.SKIPPING_INLINK_FROM_SAME_ROOT, 1);
              } else {
                inlinking++;
              }
            }
          }
        }
        JsonObject jsonObject = new JsonObject();

        jsonObject.addProperty("name", key.toString());
        jsonObject.addProperty("inlinks", inlinking);

        output.collect(new TextBytes(targetRootDomain), new TextBytes(jsonObject.toString()));
      }
    }

  }

  public static class Stage2Reducer implements Reducer<TextBytes, TextBytes, IntWritable, TextBytes> {
    public static final int MIN_SUBDOMAINS_TO_QUALIFY = 15;
    public static final double MIN_STD_DEVIATION_TO_QUALIFY = 3.0;
    public static final int MAX_SAMPLES = 5;
    String samples[] = new String[MAX_SAMPLES];
    JsonParser parser = new JsonParser();

    @Override
    public void close() throws IOException {

    }

    @Override
    public void configure(JobConf job) {

    }

    @Override
    public void reduce(TextBytes key, Iterator<TextBytes> values, OutputCollector<IntWritable, TextBytes> output,
        Reporter reporter) throws IOException {

      DescriptiveStatistics stats = new DescriptiveStatistics();

      int sampleCount = 0;
      String maxSample = null;
      int maxInlinkCount = 0;
      while (values.hasNext()) {
        JsonObject domainData = parser.parse(values.next().toString()).getAsJsonObject();

        String domainName = domainData.get("name").getAsString();
        int inlinkCount = domainData.get("inlinks").getAsInt();

        if (sampleCount < MAX_SAMPLES) {
          samples[sampleCount++] = domainName;
        }

        if (maxSample == null || maxInlinkCount < inlinkCount) {
          maxSample = domainName;
          maxInlinkCount = inlinkCount;
        }
        stats.addValue(inlinkCount);
      }

      if (stats.getN() >= MIN_SUBDOMAINS_TO_QUALIFY) {
        if (stats.getStandardDeviation() >= MIN_STD_DEVIATION_TO_QUALIFY) {
          JsonObject jsonObject = new JsonObject();

          jsonObject.addProperty("root", key.toString());
          jsonObject.addProperty("N", stats.getN());
          jsonObject.addProperty("Dev", stats.getStandardDeviation());
          jsonObject.addProperty("MaxDomain", maxSample);
          jsonObject.addProperty("MaxDomainInlinks", maxInlinkCount);
          JsonArray array = new JsonArray();
          for (int i = 0; i < sampleCount; ++i) {
            array.add(new JsonPrimitive(samples[i]));
          }

          jsonObject.add("samples", array);

          double rank = Math.ceil(Math.log10(stats.getN()));

          output.collect(new IntWritable((int) rank), new TextBytes(jsonObject.toString()));
        }
      }
    }
  }

  public static final String OUTPUT_DIR_NAME = "inlinkStdDevByRootDomain";

  private static final Log LOG = LogFactory.getLog(IdSuperDomainsStep.class);

  public IdSuperDomainsStep(CrawlPipelineTask parentTask) throws IOException {
    super(parentTask, "Calc Inlink Std-Dev by RootDomain", OUTPUT_DIR_NAME);
  }

  @Override
  public Log getLogger() {
    return LOG;
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {
    Path tempOutput = JobBuilder.tempDir(getConf(), OUTPUT_DIR_NAME + "-" + "phase1");

    JobConf job = new JobBuilder(getDescription() + " Phase 1", getConf())

        .input(
            makeUniqueOutputDirPath(_task.getOutputDirForStep(DedupedDomainLinksStep.OUTPUT_DIR_NAME),
                getTaskIdentityId())).inputIsSeqFile().mapperKeyValue(TextBytes.class, TextBytes.class).reducer(
            Stage1Reducer.class, false).outputKeyValue(TextBytes.class, TextBytes.class).numReducers(
            CrawlEnvironment.NUM_DB_SHARDS / 2).output(tempOutput).outputIsSeqFile().build();

    JobClient.runJob(job);

    Path tempOutput2 = JobBuilder.tempDir(getConf(), OUTPUT_DIR_NAME + "-" + "phase2");

    job = new JobBuilder(getDescription() + " Phase 2", getConf())

    .input(tempOutput).inputIsSeqFile().mapperKeyValue(TextBytes.class, TextBytes.class).reducer(Stage2Reducer.class,
        false).outputKeyValue(IntWritable.class, TextBytes.class).numReducers(CrawlEnvironment.NUM_DB_SHARDS / 2)
        .output(tempOutput2).outputIsSeqFile().build();

    JobClient.runJob(job);

    job = new JobBuilder(getDescription() + " Phase 3", getConf())

    .input(tempOutput2).inputIsSeqFile().keyValue(IntWritable.class, TextBytes.class).numReducers(1).output(
        outputPathLocation).outputIsSeqFile().jarByClass(IdSuperDomainsStep.class).build();

    JobClient.runJob(job);
  }

}
