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
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.DomainMetadataTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.rank.GenSuperDomainListStep;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.SuperDomainList;
import org.commoncrawl.util.TextBytes;

import com.google.gson.JsonParser;

/**
 * 
 * @author rana
 *
 */
public class PartitionRedirectDataStep extends CrawlPipelineStep implements
    Mapper<TextBytes, TextBytes, TextBytes, IntWritable> {
  enum Counters {
    REJECTED_RSS_PATTERN, REJECTED_ROBOTS_TXT, REJECTED_INVALID_EXTENSION, REJECTED_INVALID_TYPEANDREL,
    BAD_ROOT_DOMAIN, EXCEPTION_IN_REDIRECT_REDUCER
  }

  private static final Log LOG = LogFactory.getLog(PartitionRedirectDataStep.class);

  public static final String OUTPUT_DIR_NAME = "redirectDataParitioned";

  public static final String SUPER_DOMAIN_FILE_PATH = "super-domain-list";

  TextBytes outputKey = new TextBytes();

  Pattern atomRSSPattern = Pattern.compile(".*(application/atom.xml|application/rss.xml).*");

  JsonParser parser = new JsonParser();

  IntWritable oneValue = new IntWritable(1);

  Set<Long> superDomainIdSet;

  public PartitionRedirectDataStep() {
    super(null, null, null);
  }

  public PartitionRedirectDataStep(CrawlPipelineTask task) {
    super(task, "Redirect Data Paritioner", OUTPUT_DIR_NAME);
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public void configure(JobConf job) {
    Path superDomainIdFile = new Path(job.get(SUPER_DOMAIN_FILE_PATH));

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
  public void map(TextBytes key, TextBytes value, OutputCollector<TextBytes, IntWritable> output, Reporter reporter)
      throws IOException {

    if (PartitionUtils.generatePartitionKeyGivenURL(superDomainIdSet, key,
        CrawlListGeneratorTask.KEY_TYPE_REDIRECT_RECORD, outputKey)) {
      output.collect(outputKey, oneValue);
    }
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {

    Path superDomainListPath = new Path(getOutputDirForStep(GenSuperDomainListStep.class), "part-00000");

    DomainMetadataTask metadataTask = findTaskOfType(DomainMetadataTask.class);

    JobConf job = new JobBuilder(getDescription(), getConf())

    .inputs(metadataTask.getRedirectDataPaths()).inputIsSeqFile().mapper(PartitionRedirectDataStep.class).partition(
        PartitionUtils.PartitionKeyPartitioner.class).keyValue(TextBytes.class, IntWritable.class).numReducers(
        CrawlListGeneratorTask.NUM_SHARDS).output(outputPathLocation).outputIsSeqFile().set(SUPER_DOMAIN_FILE_PATH,
        superDomainListPath.toString())

    .build();

    JobClient.runJob(job);
  }

}
