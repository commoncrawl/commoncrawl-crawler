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

package org.commoncrawl.mapred.pipelineV3.domainmeta.linkstats;

import java.io.IOException;
import java.text.NumberFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.DomainMetadataTask;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.TextBytes;

import com.google.common.collect.ImmutableList;

/**
 * 
 * @author rana
 *
 */
public class CountInLinksStep extends CrawlPipelineStep {

  public static final String OUTPUT_DIR_NAME = "inlinkCounts";

  private static final Log LOG = LogFactory.getLog(CountInLinksStep.class);

  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();

  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }

  public CountInLinksStep(CrawlPipelineTask task) throws IOException {
    super(task, "Count Inlinks Step", OUTPUT_DIR_NAME);
  }

  @Override
  public Log getLogger() {
    return LOG;
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {

    LOG.info("Task Identity Path is:" + getTaskIdentityPath());
    LOG.info("Temp Path is:" + outputPathLocation);
    ImmutableList<Path> paths = new ImmutableList.Builder<Path>().addAll(
        ((DomainMetadataTask) getTask()).getRestrictedMergeDBDataPaths()).build();

    JobConf job = new JobBuilder(getPipelineStepName() + " - Phase 1", getConf()).inputIsSeqFile().inputs(paths)
        .mapperKeyValue(TextBytes.class, TextBytes.class).outputKeyValue(TextBytes.class, TextBytes.class).mapper(
            LinkStatusToInlinkingDomainMapper.class).reducer(UniqueIncomingRootDomainCounter.class, false).numReducers(
            10).outputIsSeqFile().output(outputPathLocation).build();

    LOG.info("Running Step 1");
    JobClient.runJob(job);
    LOG.info("Done Running Step 1");
  }

}
