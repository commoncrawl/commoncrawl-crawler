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
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.util.GoogleURL;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLUtils;

import com.google.common.collect.Iterators;

/**
 * 
 * @author rana
 *
 */
public class DomainURLCounter implements Mapper<TextBytes, TextBytes, TextBytes, IntWritable>,
    Reducer<TextBytes, IntWritable, TextBytes, IntWritable> {

  @Override
  public void close() throws IOException {
  }

  @Override
  public void configure(JobConf job) {
  }

  @Override
  public void map(TextBytes key, TextBytes value, OutputCollector<TextBytes, IntWritable> output, Reporter reporter)
      throws IOException {
    String url = key.toString();
    GoogleURL urlObject = new GoogleURL(url);
    String rootDomain = URLUtils.extractRootDomainName(urlObject.getHost());
    if (rootDomain != null) {
      if (urlObject.getQuery().length() == 0) {
        output.collect(new TextBytes(rootDomain), new IntWritable(1));
      }
    }
  }

  @Override
  public void reduce(TextBytes key, Iterator<IntWritable> values, OutputCollector<TextBytes, IntWritable> output,
      Reporter reporter) throws IOException {
    int count = Iterators.size(values);
    output.collect(key, new IntWritable(count));
  }

}
