package org.commoncrawl.service.crawler.util;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.commoncrawl.protocol.CrawlURL;

public class DumpCrawlLog {

  private static final Log LOG = LogFactory.getLog(DumpCrawlLog.class);
  
  public static void main(String[] args)throws IOException {
    Configuration conf = new Configuration();

    conf.addResource("nutch-default.xml");
    conf.addResource("nutch-site.xml");
    conf.addResource("core-site.xml");
    conf.addResource("hdfs-site.xml");
    conf.addResource("mapred-site.xml");
    
    if (args.length != 0) { 
      
      FileSystem fs = FileSystem.get(conf);
        
      Path path = new Path(args[0]);
      
      LOG.info("Opening crawl log at:" + path);
      
      SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
      
      Text url = new Text();
      CrawlURL urlData = new CrawlURL();
      
      while (reader.next(url, urlData)) { 
        LOG.info(
            "URL:" + url.toString() 
            + " Result:" + urlData.getLastAttemptResult() 
            + " Crawl Time:" + urlData.getLastCrawlTime()
            + " Headers:" + urlData.getHeaders());
      }
      reader.close();
    }
  }
}
