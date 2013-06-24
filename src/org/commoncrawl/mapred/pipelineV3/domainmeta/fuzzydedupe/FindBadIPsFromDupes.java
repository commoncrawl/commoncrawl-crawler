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
package org.commoncrawl.mapred.pipelineV3.domainmeta.fuzzydedupe;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.TextBytes;

import com.google.common.collect.ImmutableList;

/**
 * 
 * @author rana
 *
 */
public class FindBadIPsFromDupes extends CrawlPipelineStep {

  public static final String OUTPUT_DIR_NAME = "badIPViaDupe";

  private static final Log LOG = LogFactory.getLog(FindBadIPsFromDupes.class);

  public FindBadIPsFromDupes(CrawlPipelineTask task) throws IOException {
    super(task, "Find Bad IPs By Dupes", OUTPUT_DIR_NAME);
  }

  @Override
  public Log getLogger() {
    return LOG;
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {
    LOG.info("Task Identity Path is:" + getTaskIdentityPath());
    LOG.info("Temp Path is:" + outputPathLocation);

    ImmutableList<Path> paths = new ImmutableList.Builder<Path>().add(
        makeUniqueOutputDirPath(_task.getOutputDirForStep(CrossDomainDupes.OUTPUT_DIR_NAME), getTaskIdentityId()))
        .build();

    Path stage1Temp = JobBuilder.tempDir(getConf(), "badIPS1");

    JobConf job = new JobBuilder(getPipelineStepName() + "-Stage1", getConf())
      .inputIsSeqFile()
      .inputs(paths)
      .mapperKeyValue(TextBytes.class, TextBytes.class)
      .outputKeyValue(TextBytes.class, TextBytes.class)
      .reducer(FindBadIPsReducer.class, false)
      .numReducers(CrawlEnvironment.NUM_DB_SHARDS / 2)
      .outputIsSeqFile().output(stage1Temp)
      .build();

    LOG.info("Running " + getDescription() + "-Stage 1");
    JobClient.runJob(job);
    LOG.info("Done Running " + getDescription() + "-Stage 1");

    JobConf identityJob = new JobBuilder(getPipelineStepName() + "-Stage2", getConf())

    .inputIsSeqFile()
    .input(stage1Temp)
    .keyValue(TextBytes.class, TextBytes.class)
    .numReducers(CrawlEnvironment.NUM_DB_SHARDS / 2)
    .jarByClass(FindBadIPsFromDupes.class)
    .outputIsSeqFile().output(outputPathLocation)
    .build();

    LOG.info("Running " + getDescription() + "-Stage 2");
    JobClient.runJob(identityJob);
    LOG.info("Done Running " + getDescription() + "-Stage 2");

  }

}
