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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.crawlstats.CrawlStatsCollectorTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.linkstats.CountInLinksStep;
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
public class SubDomainToQuantcastJoinStep extends CrawlPipelineStep {

  public static final String OUTPUT_DIR = "urlQuantcastJoin";

  private static final Log LOG = LogFactory.getLog(SubDomainToQuantcastJoinStep.class);

  public SubDomainToQuantcastJoinStep(CrawlPipelineTask task) throws IOException {
    super(task, "URLCount to Quantcast Stats Join", OUTPUT_DIR);
  }

  @Override
  public Log getLogger() {
    return LOG;
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {
    ImmutableList<Path> inputs = new ImmutableList.Builder<Path>()
        .add(
            makeUniqueOutputDirPath(_task.getOutputDirForStep(ImportQuantcastStep.OUTPUT_DIR_NAME), getTaskIdentityId()))
        .add(
            makeUniqueOutputDirPath(_task.getOutputDirForStep(SubDomainCountsStep.OUTPUT_DIR_NAME), getTaskIdentityId()))
        .add(makeUniqueOutputDirPath(_task.getOutputDirForStep(CountInLinksStep.OUTPUT_DIR_NAME), getTaskIdentityId()))
        .add(
            makeUniqueOutputDirPath(_task.getOutputDirForStep(CrawlStatsCollectorTask.OUTPUT_DIR_NAME),
                getTaskIdentityId())).build();

    JobConf jobConf = new JobBuilder(getPipelineStepName(), getConf())

    .inputs(inputs).inputIsSeqFile().mapper(JoinMapper.class).mapperKeyValue(TextBytes.class, JoinValue.class).reducer(
        QuantcastJoiningReducer.class, false).numReducers(1).outputKeyValue(Text.class, Text.class).outputFormat(
        TextOutputFormat.class).output(outputPathLocation).compressType(CompressionType.NONE).build();

    JobClient.runJob(jobConf);
  }

}
