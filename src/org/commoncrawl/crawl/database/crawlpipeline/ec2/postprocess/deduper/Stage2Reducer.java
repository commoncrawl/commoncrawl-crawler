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

package org.commoncrawl.crawl.database.crawlpipeline.ec2.postprocess.deduper;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.crawl.database.crawlpipeline.ec2.postprocess.deduper.DeduperUtils.SetUnionFinder;
import org.commoncrawl.util.shared.CCStringUtils;
import org.commoncrawl.util.shared.TextBytes;

public class Stage2Reducer implements Reducer<TextBytes,TextBytes,TextBytes,TextBytes> {

  static final Log LOG = LogFactory.getLog(Stage2Reducer.class);

  SetUnionFinder unionFinder = new SetUnionFinder();
  
  
  @Override
  public void reduce(TextBytes key, Iterator<TextBytes> values,OutputCollector<TextBytes, TextBytes> output, Reporter reporter)throws IOException {
    try { 
      unionFinder.union(values);
      unionFinder.emit(key, output, reporter);
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      reporter.incrCounter("", "exceptions", 1);
    }
    
  }

  @Override
  public void configure(JobConf job) {
    
  }

  @Override
  public void close() throws IOException {
    
  }

}
