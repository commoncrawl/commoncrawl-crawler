/**
 * Copyright 2008 - CommonCrawl Foundation
 * 
 * CommonCrawl licenses this file to you under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.commoncrawl.crawl.crawler.util;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.commoncrawl.protocol.CrawlURL;

/**
 * Utility used to dump contents of a CrawlLog
 * 
 * @author rana
 *
 */
public class DumpCrawlLog {

  private static final Log LOG = LogFactory.getLog(DumpCrawlLog.class);
  
  public static void main(String[] args)throws IOException {
    Configuration conf = new Configuration();

    conf.addResource("nutch-default.xml");
    conf.addResource("nutch-site.xml");
    
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
