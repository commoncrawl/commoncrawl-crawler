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

package org.commoncrawl.mapred.pipelineV3.domainmeta.rank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.quantcast.ImportQuantcastStep;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.JoinMapper;
import org.commoncrawl.util.JoinValue;
import org.commoncrawl.util.TextBytes;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonObject;

/**
 * 
 * @author rana
 *
 */
public class JoinQuantcastAndDomainRankStep extends CrawlPipelineStep implements
    Reducer<TextBytes, JoinValue, TextBytes, TextBytes> {

  enum Counters {
    HAD_DOMAIN_RANK, HAD_Q_RANK
  }

  private static final Log LOG = LogFactory.getLog(JoinQuantcastAndDomainRankStep.class);

  public static final String OUTPUT_DIR_NAME = "QuantcastDomainRankJoin";

  JsonObject objectOut = new JsonObject();

  TextBytes rankValue = new TextBytes();

  public JoinQuantcastAndDomainRankStep() {
    super(null, null, null);
  }

  public JoinQuantcastAndDomainRankStep(CrawlPipelineTask task) {
    super(task, "QuantcastDomainRankJoin", OUTPUT_DIR_NAME);
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
  public void reduce(TextBytes key, Iterator<JoinValue> values, OutputCollector<TextBytes, TextBytes> output,
      Reporter reporter) throws IOException {
    double domainRank = 0.0;
    int qrank = 0;

    while (values.hasNext()) {
      JoinValue nextValue = values.next();
      if (nextValue.getTag().toString().equals(ImportQuantcastStep.OUTPUT_DIR_NAME)) {
        reporter.incrCounter(Counters.HAD_Q_RANK, 1);
        qrank = (int) nextValue.getLongValue();
      } else if (nextValue.getTag().toString().equals(GenDomainRankStep.OUTPUT_DIR_NAME)) {
        reporter.incrCounter(Counters.HAD_DOMAIN_RANK, 1);
        domainRank = nextValue.getDoubleValue();
        break;
      }
    }

    objectOut.addProperty("dR", domainRank);
    objectOut.addProperty("qR", qrank);

    rankValue.set(objectOut.toString());

    output.collect(key, rankValue);
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {

    ImmutableList<Path> pathList = new ImmutableList.Builder<Path>()
        .add(getOutputDirForStep(ImportQuantcastStep.class)).add(getOutputDirForStep(GenDomainRankStep.class)).build();

    JobConf job = new JobBuilder(getDescription(), getConf())

    .inputs(pathList).inputIsSeqFile().mapper(JoinMapper.class).mapperKeyValue(TextBytes.class, JoinValue.class)
        .reducer(JoinQuantcastAndDomainRankStep.class, false).numReducers(CrawlEnvironment.NUM_DB_SHARDS / 2)
        .outputKeyValue(TextBytes.class, TextBytes.class).outputIsSeqFile().output(outputPathLocation)

        .build();

    JobClient.runJob(job);
  }

}
