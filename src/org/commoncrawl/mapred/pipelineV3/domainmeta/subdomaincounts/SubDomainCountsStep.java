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

package org.commoncrawl.mapred.pipelineV3.domainmeta.subdomaincounts;

import java.io.IOException;
import java.text.NumberFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
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
public class SubDomainCountsStep extends CrawlPipelineStep {

  public static final String OUTPUT_DIR_NAME = "subDomainCounts";

  private static final Log LOG = LogFactory.getLog(SubDomainCountsStep.class);

  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }

  public SubDomainCountsStep(CrawlPipelineTask task) throws IOException {
    super(task, "SubDomain Counts", OUTPUT_DIR_NAME);
  }

  @Override
  public Log getLogger() {
    return LOG;
  }

  @Override
  public String getPipelineStepName() {
    return "SubDomain Counts";
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {
    LOG.info("Task Identity Path is:" + getTaskIdentityPath());
    LOG.info("Temp Path is:" + outputPathLocation);
    ImmutableList<Path> paths = new ImmutableList.Builder<Path>().addAll(
        ((DomainMetadataTask) getTask()).getRestrictedMergeDBDataPaths()).addAll(
        ((DomainMetadataTask) getTask()).getRestrictedRedirectPaths()).build();

    Path step1Temp = JobBuilder.tempDir(getConf(), getOutputDirName());

    JobConf job = new JobBuilder(getPipelineStepName() + " - Phase 1", getConf()).inputIsSeqFile().inputs(paths)
        .mapperKeyValue(TextBytes.class, TextBytes.class).outputKeyValue(TextBytes.class, IntWritable.class).mapper(
            SubDomainCountsMapper.class).reducer(SubDomainCountsReducer.class, false).numReducers(10).outputIsSeqFile()
        .output(step1Temp).build();

    LOG.info("Running Step 1");
    JobClient.runJob(job);
    LOG.info("Done Running Step 1");

    JobConf job2 = new JobBuilder(getPipelineStepName() + " - Phase 2", getConf()).inputIsSeqFile().input(step1Temp)
        .mapper(IdentityMapper.class).reducer(IdentityReducer.class, false)
        .keyValue(TextBytes.class, IntWritable.class).jarByClass(SubDomainCountsStep.class).numReducers(1)
        .outputIsSeqFile().output(outputPathLocation).build();

    LOG.info("Running Step 2");
    JobClient.runJob(job2);
    LOG.info("Done Running Step 2 ");
  }
}
