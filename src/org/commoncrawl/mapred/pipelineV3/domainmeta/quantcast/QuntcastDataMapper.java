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

package org.commoncrawl.mapred.pipelineV3.domainmeta.quantcast;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLUtils;

/**
 * 
 * @author rana
 *
 */
public class QuntcastDataMapper implements Mapper<LongWritable, Text, TextBytes, IntWritable> {

  Pattern pattern = Pattern.compile("([0-9]*)\\s*(.*)");

  @Override
  public void close() throws IOException {
  }

  @Override
  public void configure(JobConf job) {
  }

  @Override
  public void map(LongWritable key, Text value, OutputCollector<TextBytes, IntWritable> output, Reporter reporter)
      throws IOException {
    Matcher matcher = pattern.matcher(value.toString());

    if (matcher.matches()) {
      String rootDomain = URLUtils.extractRootDomainName(matcher.group(2));
      if (rootDomain != null) {
        try {
          output.collect(new TextBytes(rootDomain), new IntWritable(Integer.parseInt(matcher.group(1))));
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
    }
  }
}
