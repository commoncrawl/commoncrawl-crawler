package org.commoncrawl.crawl.database.crawlpipeline.ec2.postprocess.deduper;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.crawl.database.crawlpipeline.ec2.postprocess.deduper.DeduperUtils.DeduperValue;
import org.commoncrawl.crawl.database.crawlpipeline.ec2.postprocess.deduper.DeduperUtils.SimhashMatcher;
import org.commoncrawl.util.shared.TextBytes;

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
