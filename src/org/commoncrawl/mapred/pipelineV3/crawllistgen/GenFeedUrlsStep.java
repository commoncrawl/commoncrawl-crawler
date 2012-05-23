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
package org.commoncrawl.mapred.pipelineV3.crawllistgen;

import java.io.IOException;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.DomainMetadataTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.blogs.feedurlid.FeedUrlIdStep;
import org.commoncrawl.mapred.pipelineV3.domainmeta.crawlstats.CrawlStatsCollectorTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.rank.GenSuperDomainListStep;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.SuperDomainList;
import org.commoncrawl.util.TextBytes;

import com.google.common.collect.ImmutableSet;

/**
 * 
 * @author rana
 *
 */
public class GenFeedUrlsStep extends CrawlPipelineStep implements Mapper<TextBytes, TextBytes, TextBytes, TextBytes> {

  enum Counters {
    FAILED_TO_GEN_PARITIONKEY_FOR_FEED_URL, FAILED_TO_GEN_PARITIONKEY_FOR_SECONDARY_URL

  }

  private static final Log LOG = LogFactory.getLog(GenFeedUrlsStep.class);

  public static final String OUTPUT_DIR_NAME = "feedURLs";

  TextBytes keyOut = new TextBytes();

  TextBytes urlOut = new TextBytes();

  Set<Long> superDomainIdSet;

  public GenFeedUrlsStep() {
    super(null, null, null);
  }

  public GenFeedUrlsStep(CrawlPipelineTask task) {
    super(task, "Generate Feed URLS", OUTPUT_DIR_NAME);
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub

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
  public Log getLogger() {
    return LOG;
  }

  @Override
  public void map(TextBytes key, TextBytes value, OutputCollector<TextBytes, TextBytes> output, Reporter reporter)
      throws IOException {
    if (PartitionUtils.generatePartitionKeyGivenURL(superDomainIdSet, value, CrawlListGeneratorTask.KEY_TYPE_FEED_URL,
        keyOut)) {

      output.collect(keyOut, value);

      // ok try to extract path from feed url ...
      String feedURL = value.toString();
      int indexOfPrevSlash = feedURL.lastIndexOf('/', feedURL.length() - 2);
      if (indexOfPrevSlash != -1) {
        String rootPath = feedURL.substring(0, indexOfPrevSlash + 1);
        urlOut.set(rootPath);
        if (PartitionUtils.generatePartitionKeyGivenURL(superDomainIdSet, urlOut,
            CrawlListGeneratorTask.KEY_TYPE_FEED_URL, keyOut)) {
          output.collect(keyOut, value);
        } else {
          reporter.incrCounter(Counters.FAILED_TO_GEN_PARITIONKEY_FOR_SECONDARY_URL, 1);
        }
      }
    } else {
      reporter.incrCounter(Counters.FAILED_TO_GEN_PARITIONKEY_FOR_FEED_URL, 1);
    }
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {
    Path superDomainListPath = new Path(getOutputDirForStep(GenSuperDomainListStep.class), "part-00000");
    LOG.info("Super Domain Id File Path:" + superDomainListPath);
    DomainMetadataTask metadataTask = findTaskOfType(DomainMetadataTask.class);
    LOG.info("merge DB Paths:" + metadataTask.getMergeDBDataPaths());

    JobConf job = new JobBuilder(getDescription(), getConf())

    .input(getOutputDirForStep(FeedUrlIdStep.class)).inputIsSeqFile().keyValue(TextBytes.class, TextBytes.class)
        .mapper(GenFeedUrlsStep.class).partition(PartitionUtils.PartitionKeyPartitioner.class).numReducers(
            CrawlListGeneratorTask.NUM_SHARDS).output(outputPathLocation).outputIsSeqFile().setAffinityNoBalancing(
            getOutputDirForStep(PartitionCrawlDBStep.class),
            ImmutableSet.of("ccd001.commoncrawl.org", "ccd006.commoncrawl.org")).set(
            CrawlStatsCollectorTask.SUPER_DOMAIN_FILE_PATH, superDomainListPath.toString()).build();

    JobClient.runJob(job);

  }

}
