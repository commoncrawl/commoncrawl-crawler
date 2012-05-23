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
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.TextBytes;

/**
 * 
 * @author rana
 *
 */
public class LinkDumper implements Mapper<TextBytes, TextBytes, TextBytes, TextBytes> {

  public static void main(String[] args) throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path path = new Path(args[0]);
    String regEx = args[1];
    Pattern patternCheck = Pattern.compile(regEx);

    Path temp = JobBuilder.tempDir(conf, "linkDumper");
    JobConf jobConf = new JobBuilder("Query Graph", conf)

    .input(path).inputIsSeqFile().output(temp).keyValue(TextBytes.class, TextBytes.class).mapper(LinkDumper.class)
        .numReducers(1).outputIsSeqFile().set("queryPattern", regEx).build();

    JobClient.runJob(jobConf);

    SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(temp, "part-00000"), jobConf);
    TextBytes key = new TextBytes();
    TextBytes value = new TextBytes();
    while (reader.next(key, value)) {
      System.out.println(key.toString() + "\t" + value.toString());
    }
    reader.close();
  }

  Pattern queryRegEx = null;

  public LinkDumper() {

  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public void configure(JobConf job) {
    queryRegEx = Pattern.compile(job.get("queryPattern"));
  }

  @Override
  public void map(TextBytes key, TextBytes value, OutputCollector<TextBytes, TextBytes> output, Reporter reporter)
      throws IOException {

    if (queryRegEx.matcher(key.toString()).matches()) {
      output.collect(key, value);
    }
  }
}
