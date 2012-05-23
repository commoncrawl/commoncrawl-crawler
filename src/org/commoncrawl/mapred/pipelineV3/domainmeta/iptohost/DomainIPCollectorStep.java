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
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.InverseMapper;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.TextBytes;

import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;

/**
 * 
 * @author rana
 *
 */
public class DomainIPCollectorStep extends CrawlPipelineStep implements
    Reducer<TextBytes, TextBytes, TextBytes, TextBytes> {

  enum Counters {
    EXCEEDED_MAP_IP_ADDRESSES
  }

  public static final String OUTPUT_DIR_NAME = "domainIPCollector";
  private static final Log LOG = LogFactory.getLog(DomainIPCollectorStep.class);

  static final int MAX_IP_ADDRESSES = 10;

  public DomainIPCollectorStep() {
    super(null, null, null);
  }

  public DomainIPCollectorStep(CrawlPipelineTask task) {
    super(task, "Domain IP Collector", OUTPUT_DIR_NAME);
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
  public void reduce(TextBytes key, Iterator<TextBytes> values, OutputCollector<TextBytes, TextBytes> output,
      Reporter reporter) throws IOException {

    JsonArray array = new JsonArray();

    while (values.hasNext()) {
      array.add(new JsonPrimitive(values.next().toString()));
      if (array.size() >= MAX_IP_ADDRESSES) {
        reporter.incrCounter(Counters.EXCEEDED_MAP_IP_ADDRESSES, 1);
        break;
      }
    }
    output.collect(key, new TextBytes(array.toString()));
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {
    JobConf job = new JobBuilder(getDescription(), getConf())

    .input(getOutputDirForStep(IPAddressToHostMappingStep.class)).inputIsSeqFile().mapper(InverseMapper.class).reducer(
        DomainIPCollectorStep.class, false).numReducers(CrawlEnvironment.NUM_DB_SHARDS / 2).output(outputPathLocation)
        .outputIsSeqFile().keyValue(TextBytes.class, TextBytes.class).build();

    JobClient.runJob(job);
  }

}
