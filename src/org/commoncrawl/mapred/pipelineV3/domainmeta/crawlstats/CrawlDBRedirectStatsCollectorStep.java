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
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.DomainMetadataTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.rank.GenSuperDomainListStep;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.GoogleURL;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.JoinMapper;
import org.commoncrawl.util.JoinValue;
import org.commoncrawl.util.SuperDomainList;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLUtils;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * 
 * @author rana
 *
 */
public class CrawlDBRedirectStatsCollectorStep extends CrawlPipelineStep {

  public static class EmitOnlyOneRedirectReducer implements Reducer<TextBytes, TextBytes, TextBytes, IntWritable> {

    enum Counters {
      BAD_ROOT_DOMAIN, EXCEPTION_IN_REDIRECT_REDUCER
    }

    Set<Long> superDomainIdSet;
    IntWritable oneValue = new IntWritable(1);

    TextBytes outputKey = new TextBytes();

    @Override
    public void close() throws IOException {
    }

    @Override
    public void configure(JobConf job) {
      Path superDomainIdFile = new Path(job.get(CrawlStatsCollectorTask.SUPER_DOMAIN_FILE_PATH));

      try {
        superDomainIdSet = SuperDomainList.loadSuperDomainIdList(job, superDomainIdFile);
      } catch (IOException e) {
        LOG.error(StringUtils.stringifyException(e));
        throw new RuntimeException(e);
      }
    }

    @Override
    public void reduce(TextBytes key, Iterator<TextBytes> values, OutputCollector<TextBytes, IntWritable> output,
        Reporter reporter) throws IOException {

      try {
        GoogleURL urlObject = new GoogleURL(key.toString());

        if (urlObject.isValid()) {

          URLFPV2 fp = URLUtils.getURLFPV2FromURLObject(urlObject);

          if (fp != null) {

            String rootDomain = URLUtils.extractRootDomainName(urlObject.getHost());
            if (rootDomain == null) {
              reporter.incrCounter(Counters.BAD_ROOT_DOMAIN, 1);
              return;
            }

            if (rootDomain != null) {
              long rootDomainFP = SuperDomainList.domainFingerprintGivenName(rootDomain);
              if (superDomainIdSet.contains(rootDomainFP)) {
                rootDomain = urlObject.getHost();
              }
              outputKey.set(rootDomain);
              output.collect(outputKey, oneValue);
            }
          }
        }
      } catch (Exception e) {
        reporter.incrCounter(Counters.EXCEPTION_IN_REDIRECT_REDUCER, 1);
      }
    }
  }

  public static class JoinReducer implements Reducer<TextBytes, JoinValue, TextBytes, TextBytes> {

    enum Counters {
      GOT_REDIRECT_WITHOUT_CRAWLSTATS, GOT_NULL_CRAWL_STATS
    }

    JsonParser parser = new JsonParser();

    @Override
    public void close() throws IOException {
      // TODO Auto-generated method stub

    }

    @Override
    public void configure(JobConf job) {

    }

    @Override
    public void reduce(TextBytes key, Iterator<JoinValue> values, OutputCollector<TextBytes, TextBytes> output,
        Reporter reporter) throws IOException {

      JsonObject crawlStatsObject = null;
      int redirectCount = 0;
      while (values.hasNext()) {

        JoinValue value = values.next();

        if (value.getTag().toString().equals(CrawlDBStatsCollectorStep.OUTPUT_DIR_NAME)) {
          crawlStatsObject = parser.parse(value.getTextValue().toString()).getAsJsonObject();
        } else {
          redirectCount++;
        }
      }

      if (redirectCount != 0) {
        if (crawlStatsObject == null) {
          reporter.incrCounter(Counters.GOT_REDIRECT_WITHOUT_CRAWLSTATS, 1);
          crawlStatsObject = new JsonObject();
        }

        crawlStatsObject.addProperty("redirects", redirectCount);
      }

      if (crawlStatsObject != null) {
        output.collect(key, new TextBytes(crawlStatsObject.toString()));
      } else {
        reporter.incrCounter(Counters.GOT_NULL_CRAWL_STATS, 1);
      }
    }

  }

  public static final String OUTPUT_DIR_NAME = "crawlDBRedirectStats";

  private static final Log LOG = LogFactory.getLog(CrawlDBRedirectStatsCollectorStep.class);

  public CrawlDBRedirectStatsCollectorStep(CrawlPipelineTask task) {
    super(task, "Redirect Stats Injector", OUTPUT_DIR_NAME);
  }

  @Override
  public Log getLogger() {
    return LOG;
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {
    Path phase1Output = JobBuilder.tempDir(getConf(), OUTPUT_DIR_NAME + "-Step1");

    DomainMetadataTask rootTask = findTaskOfType(DomainMetadataTask.class);

    Path superDomainListPath = new Path(getOutputDirForStep(GenSuperDomainListStep.class), "part-00000");

    JobConf job = new JobBuilder(getDescription() + " Phase 1", getConf())

    .inputs(rootTask.getRedirectDataPaths()).inputIsSeqFile().mapperKeyValue(TextBytes.class, TextBytes.class)
        .outputKeyValue(TextBytes.class, IntWritable.class).reducer(EmitOnlyOneRedirectReducer.class, false)
        .numReducers(CrawlEnvironment.NUM_DB_SHARDS / 2).output(phase1Output).outputIsSeqFile().set(
            CrawlStatsCollectorTask.SUPER_DOMAIN_FILE_PATH, superDomainListPath.toString()).build();

    JobClient.runJob(job);

    ImmutableList<Path> phase2Inputs = new ImmutableList.Builder<Path>().add(
        getOutputDirForStep(CrawlDBStatsCollectorStep.class)).add(phase1Output).build();

    job = new JobBuilder(getDescription() + " Phase 2", getConf())

    .inputs(phase2Inputs).inputIsSeqFile().mapper(JoinMapper.class).mapperKeyValue(TextBytes.class, JoinValue.class)
        .reducer(JoinReducer.class, false).outputKeyValue(TextBytes.class, TextBytes.class).reducer(JoinReducer.class,
            false).numReducers(CrawlEnvironment.NUM_DB_SHARDS / 2).outputIsSeqFile().output(outputPathLocation)

        .build();

    JobClient.runJob(job);

  }
}
