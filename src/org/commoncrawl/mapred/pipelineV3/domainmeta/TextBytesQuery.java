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
package org.commoncrawl.mapred.pipelineV3.domainmeta;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.SequenceFileUtils;
import org.commoncrawl.util.TextBytes;

/**
 * 
 * @author rana
 *
 */
public class TextBytesQuery implements Mapper<TextBytes, Writable, TextBytes, Writable> {

  public static void main(String[] args) throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path inputPath = new Path(args[0]);
    Path outputPath = JobBuilder.tempDir(conf, "rootDomainQuery");

    Class valueType = SequenceFileUtils.sniffValueTypeFromSequenceFile(fs, conf, inputPath);

    JobConf job = new JobBuilder("Query", conf).inputIsSeqFile().input(inputPath).keyValue(TextBytes.class, valueType)
        .mapper(TextBytesQuery.class).numReducers(1).outputIsSeqFile().output(outputPath).build();

    job.set("query", args[1]);

    JobClient.runJob(job);

    SequenceFileUtils.printContents(fs, conf, new Path(outputPath, "part-00000"));
  }

  TextBytes query;

  Pattern queryRegEx = null;

  @Override
  public void close() throws IOException {
  }

  @Override
  public void configure(JobConf job) {
    queryRegEx = Pattern.compile(job.get("query"));
  }

  @Override
  public void map(TextBytes key, Writable value, OutputCollector<TextBytes, Writable> output, Reporter reporter)
      throws IOException {
    if (queryRegEx.matcher(key.toString()).matches()) {
      output.collect(key, value);
    }
  }
}
