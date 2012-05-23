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
import org.commoncrawl.mapred.pipelineV3.domainmeta.iptohost.IPAddressToHostMappingStep;
import org.commoncrawl.mapred.pipelineV3.domainmeta.iptohost.QuantcastIPListStep;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.JoinByTextSortByTagMapper;
import org.commoncrawl.util.JoinMapper;
import org.commoncrawl.util.JoinValue;
import org.commoncrawl.util.TextBytes;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * 
 * @author rana
 *
 */
public class HostBlacklistByDupesStep extends CrawlPipelineStep {

  public static final String OUTPUT_DIR_NAME = "hostBlackListFromDupes";

  private static final Log LOG = LogFactory.getLog(HostBlacklistByDupesStep.class);

  static final String TAG_BAD_IP_MAPPING = "1";

  static final String TAG_QUANTCAST_MAPPING = "2";

  static final String TAG_IP_TO_HOST_MAPPING = "3";

  public HostBlacklistByDupesStep(CrawlPipelineTask task) throws IOException {
    super(task, "Generate Host BlackList From Dupes", OUTPUT_DIR_NAME);
  }

  @Override
  public Log getLogger() {
    return LOG;
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {

    LOG.info("Task Identity Path is:" + getTaskIdentityPath());
    LOG.info("Final Temp Path is:" + outputPathLocation);

    // join
    // ip to host mapping
    // ip to bad host mapping
    // ip to quantcast domain (whitelist)

    Path temp = JobBuilder.tempDir(getConf(), OUTPUT_DIR_NAME + " phase-1");

    ImmutableMap<Path, String> step1InputMapping = new ImmutableMap.Builder<Path, String>().put(
        makeUniqueFullyQualifiedOutputDirPath(getConf(), _task
            .getOutputDirForStep(IPAddressToHostMappingStep.OUTPUT_DIR_NAME), getTaskIdentityId()),
        TAG_IP_TO_HOST_MAPPING).put(
        makeUniqueFullyQualifiedOutputDirPath(getConf(),
            _task.getOutputDirForStep(FindBadIPsFromDupes.OUTPUT_DIR_NAME), getTaskIdentityId()), TAG_BAD_IP_MAPPING)
        .put(
            makeUniqueFullyQualifiedOutputDirPath(getConf(), _task
                .getOutputDirForStep(QuantcastIPListStep.OUTPUT_DIR_NAME), getTaskIdentityId()), TAG_QUANTCAST_MAPPING)
        .build();

    JobConf job = new JobBuilder(getPipelineStepName() + " - step 1", getConf()).inputIsSeqFile().inputs(
        ImmutableList.copyOf(step1InputMapping.keySet())).mapperKeyValue(TextBytes.class, JoinValue.class)
        .outputKeyValue(TextBytes.class, TextBytes.class).mapper(JoinByTextSortByTagMapper.class).reducer(
            HostBlacklistByIPReducer.class, false).partition(JoinByTextSortByTagMapper.Partitioner.class).numReducers(
            CrawlEnvironment.NUM_DB_SHARDS / 2).outputIsSeqFile().output(temp).jarByClass(
            HostBlacklistByDupesStep.class).build();

    JoinMapper.setPathToTagMapping(step1InputMapping, job);

    LOG.info("Running Step 1");
    JobClient.runJob(job);
    LOG.info("Done Running Step 1");

    // ok now reduce to a single file ...
    job = new JobBuilder(getPipelineStepName() + " - step 2", getConf()).inputIsSeqFile().input(temp).keyValue(
        TextBytes.class, TextBytes.class).numReducers(1).outputIsSeqFile().output(outputPathLocation).jarByClass(
        HostBlacklistByDupesStep.class).build();

    JobClient.runJob(job);

  }

}
