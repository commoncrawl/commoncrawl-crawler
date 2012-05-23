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
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.crawlstats.CrawlStatsCollectorTask;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.TextBytes;

import com.google.common.collect.ImmutableSet;

/**
 * 
 * @author rana
 *
 */
public class PartitionCrawlStatsStep extends CrawlPipelineStep implements
    Mapper<TextBytes, TextBytes, TextBytes, TextBytes> {

  public static final String OUTPUT_DIR_NAME = "crawlStatsPartitioned";

  private static final Log LOG = LogFactory.getLog(PartitionCrawlStatsStep.class);

  TextBytes outputKey = new TextBytes();

  HashSet<Long> emptySet = new HashSet<Long>();

  public PartitionCrawlStatsStep() {
    super(null, null, null);
  }

  public PartitionCrawlStatsStep(CrawlPipelineTask task) {
    super(task, "Parition CrawlStats", OUTPUT_DIR_NAME);
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
  public void map(TextBytes key, TextBytes value, OutputCollector<TextBytes, TextBytes> output, Reporter reporter)
      throws IOException {

    PartitionUtils.generatePartitionKeyGivenDomain(emptySet, key.toString(),
        CrawlListGeneratorTask.KEY_TYPE_CRAWLSTATS, outputKey);
    output.collect(outputKey, value);
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {

    JobConf job = new JobBuilder(getDescription(), getConf())

    .input(getOutputDirForStep(CrawlStatsCollectorTask.class)).inputIsSeqFile().mapper(PartitionCrawlStatsStep.class)
        .keyValue(TextBytes.class, TextBytes.class).partition(PartitionUtils.PartitionKeyPartitioner.class)
        .numReducers(CrawlListGeneratorTask.NUM_SHARDS).output(outputPathLocation).outputIsSeqFile()
        .setAffinityNoBalancing(getOutputDirForStep(PartitionCrawlDBStep.class),
            ImmutableSet.of("ccd001.commoncrawl.org", "ccd006.commoncrawl.org"))

        .build();

    JobClient.runJob(job);
  }

}
