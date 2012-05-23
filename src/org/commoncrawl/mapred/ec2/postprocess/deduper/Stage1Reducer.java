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

package org.commoncrawl.mapred.ec2.postprocess.deduper;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.mapred.ec2.postprocess.deduper.DeduperUtils.DeduperValue;
import org.commoncrawl.mapred.ec2.postprocess.deduper.DeduperUtils.SimhashMatcher;
import org.commoncrawl.util.TextBytes;

/**
 * Take all candidates and emit sets for any items for which  
 * the hamming-distance between their simhash values <= K (3)
 * 
 * @author rana
 *
 */
public class Stage1Reducer implements Reducer<LongWritable, DeduperValue,TextBytes,TextBytes> {

  // instantiate the matcher ... 
  SimhashMatcher matcher;
  
  @Override
  public void reduce(LongWritable key, Iterator<DeduperValue> values,OutputCollector<TextBytes, TextBytes> output, Reporter reporter)throws IOException {
    if (matcher == null) { 
      matcher = new SimhashMatcher();
    }
    // and emit matches 
    matcher.emitMatches(2,values,output,reporter);
  }

  @Override
  public void configure(JobConf job) {
  }

  @Override
  public void close() throws IOException {
  }

}
