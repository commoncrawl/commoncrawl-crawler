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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.TextBytes;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * 
 * @author rana
 *
 */
public class GenSuperDomainListStep extends CrawlPipelineStep implements
    Reducer<IntWritable, TextBytes, TextBytes, NullWritable> {

  public static final String OUTPUT_DIR_NAME = "superDomainList";

  private static final Log LOG = LogFactory.getLog(GenSuperDomainListStep.class);

  JsonParser parser = new JsonParser();

  public GenSuperDomainListStep() {
    super(null, null, null);
  }

  public GenSuperDomainListStep(CrawlPipelineTask task) {
    super(task, "Generate Super Domain List", OUTPUT_DIR_NAME);
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
  public void reduce(IntWritable key, Iterator<TextBytes> values, OutputCollector<TextBytes, NullWritable> output,
      Reporter reporter) throws IOException {
    while (values.hasNext()) {
      JsonObject object = parser.parse(values.next().toString()).getAsJsonObject();
      output.collect(new TextBytes(object.get("root").getAsString()), NullWritable.get());
    }
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {

    JobConf job = new JobBuilder(getDescription(), getConf())

    .input(makeUniqueOutputDirPath(_task.getOutputDirForStep(IdSuperDomainsStep.OUTPUT_DIR_NAME), getTaskIdentityId()))
        .inputIsSeqFile().mapperKeyValue(IntWritable.class, TextBytes.class).reducer(GenSuperDomainListStep.class,
            false).outputKeyValue(TextBytes.class, NullWritable.class).numReducers(1).output(outputPathLocation)
        .outputIsSeqFile().build();

    JobClient.runJob(job);

  }

}
