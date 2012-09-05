package org.commoncrawl.mapred.ec2.parser;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapRunner;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.protocol.CrawlURL;
import org.commoncrawl.protocol.ParseOutput;

/** 
 * Custom MapRunner, primarily to trap a successful execution of a map task 
 * (so that we can forward this information via the Task Data Client). 
 * 
 * @author rana
 *
 */
public class ParserMapRunner extends MapRunner<Text,CrawlURL,Text,ParseOutput>{

  @Override
  public void run(RecordReader<Text, CrawlURL> input,
      OutputCollector<Text, ParseOutput> output, Reporter reporter)
      throws IOException {
    try {
      // allocate key & value instances that are re-used for all entries
      Text key = input.createKey();
      CrawlURL value = input.createValue();
      
      while (input.next(key, value)) {
        // update progress first ... 
        ((ParserMapper)getMapper()).updateProgressAndPosition(input.getProgress(),input.getPos());
        // next map pair to output
        getMapper().map(key, value, output, reporter);
        // ok see if mapper terminated early ... 
        if (((ParserMapper)getMapper()).wasTerminatedEarly()) { 
          // skip processing remaining stream ... 
          break;
        }
      }
      
      // ok .. if we reach here without any exceptions ...
      // inform the TDC that this was a successful (potentially partially completed)
      // mapper task
      ((ParserMapper)getMapper()).commitTask(reporter);

      
    } finally {
      getMapper().close();
    }
  }
}
