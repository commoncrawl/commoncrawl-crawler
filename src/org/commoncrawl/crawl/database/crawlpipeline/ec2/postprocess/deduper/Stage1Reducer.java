package org.commoncrawl.crawl.database.crawlpipeline.ec2.postprocess.deduper;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.crawl.database.crawlpipeline.ec2.postprocess.deduper.DeduperUtils.DeduperSetTuple;
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
public class Stage1Reducer implements Reducer<LongWritable, DeduperValue,TextBytes,DeduperSetTuple> {

  
  @Override
  public void reduce(LongWritable key, Iterator<DeduperValue> values,OutputCollector<TextBytes, DeduperSetTuple> output, Reporter reporter)throws IOException {
    // instantiate the matcher ... 
    SimhashMatcher matcher= new SimhashMatcher(values);
    // and emit matches 
    matcher.emitMatches(output);
  }

  @Override
  public void configure(JobConf job) {
  }

  @Override
  public void close() throws IOException {
  }

}
