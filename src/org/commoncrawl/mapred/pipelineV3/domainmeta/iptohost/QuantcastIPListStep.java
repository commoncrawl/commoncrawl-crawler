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

package org.commoncrawl.mapred.pipelineV3.domainmeta.iptohost;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.InverseMapper;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.quantcast.ImportQuantcastStep;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.JoinMapper;
import org.commoncrawl.util.JoinValue;
import org.commoncrawl.util.TextBytes;

import com.google.common.collect.ImmutableList;

/**
 * 
 * @author rana
 *
 */
public class QuantcastIPListStep extends CrawlPipelineStep {

  public static final String OUTPUT_DIR_NAME = "quantcastIPMapping";

  private static final Log LOG = LogFactory.getLog(QuantcastIPListStep.class);

  public QuantcastIPListStep(CrawlPipelineTask task) throws IOException {
    super(task, "quantcast to ip mapping", OUTPUT_DIR_NAME);
  }

  @Override
  public Log getLogger() {
    return LOG;
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {
    LOG.info("Task Identity Path is:" + getTaskIdentityPath());
    LOG.info("Final Temp Path is:" + outputPathLocation);

    // step 1 .. invert ip to host list to host to ip ...
    Path step1TempDir = JobBuilder.tempDir(getConf(), OUTPUT_DIR_NAME + "-step1");

    ImmutableList<Path> step1Inputs = new ImmutableList.Builder<Path>().add(
        makeUniqueOutputDirPath(_task.getOutputDirForStep(IPAddressToHostMappingStep.OUTPUT_DIR_NAME),
            getTaskIdentityId())).build();

    JobConf job = new JobBuilder(getPipelineStepName() + " - step 1", getConf()).inputIsSeqFile().inputs(step1Inputs)
        .keyValue(TextBytes.class, TextBytes.class).mapper(InverseMapper.class).numReducers(
            CrawlEnvironment.NUM_DB_SHARDS / 2).outputIsSeqFile().output(step1TempDir).jarByClass(
            QuantcastIPListStep.class).build();

    LOG.info("Running Step 1");
    JobClient.runJob(job);
    LOG.info("Done Running Step 1");

    // step 2
    // join quantcast domain rank info with host to ip mapping and emit
    // whitelisted ip list

    ImmutableList<Path> step2Inputs = new ImmutableList.Builder<Path>().add(
        makeUniqueOutputDirPath(_task.getOutputDirForStep(ImportQuantcastStep.OUTPUT_DIR_NAME), getTaskIdentityId()))
        .add(step1TempDir)

        .build();

    job = new JobBuilder(getPipelineStepName() + " - step 2", getConf()).inputIsSeqFile().inputs(step2Inputs)
        .mapperKeyValue(TextBytes.class, JoinValue.class).outputKeyValue(TextBytes.class, TextBytes.class).mapper(
            JoinMapper.class).reducer(QuantcastWhitelistByIPReducer.class, false).numReducers(
            CrawlEnvironment.NUM_DB_SHARDS / 2).outputIsSeqFile().output(outputPathLocation).build();

    LOG.info("Running Step 2");
    JobClient.runJob(job);
    LOG.info("Done Running Step 2");

  }

}
