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

package org.commoncrawl.crawl.database.crawlpipeline;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.util.LockUtil;
import org.commoncrawl.crawl.database.CrawlDBSegment;

/** 
 * Utility functions 
 * 
 * @author rana
 *
 */
public class CrawlDBUtils {

  public static void install(JobConf job, Path crawlDb) throws IOException {
    Path newCrawlDb = job.getOutputPath();
    FileSystem fs = new JobClient(job).getFs();
    Path old = new Path(crawlDb, "old-" + System.currentTimeMillis());
    Path current = new Path(crawlDb, CrawlDb.CURRENT_NAME);
    if (fs.exists(current)) {
      fs.rename(current, old);
    }
    fs.mkdirs(crawlDb);
    fs.rename(newCrawlDb, current);
    Path lock = new Path(crawlDb, CrawlDb.LOCK_NAME);
    LockUtil.removeLockFile(fs, lock);
  }
  
  public static boolean isSegmentInState(FileSystem fs,Path crawlDBSegmentPath,int segmentId,int state) throws IOException {
    String stateName = CrawlDBSegment.Status.toString(state);
    if (stateName.equals("")) 
      throw new IOException("Invalid State Name!");
    return fs.exists(new Path(crawlDBSegmentPath,Integer.toString(segmentId) +"/." + stateName));
  }
  
  public static void markSegmentInState(FileSystem fs,Path crawlDBSegmentPath,int segmentId,int state) throws IOException { 
    String stateName = CrawlDBSegment.Status.toString(state);
    if (stateName.equals("")) 
      throw new IOException("Invalid State Name!");

    fs.createNewFile(new Path(crawlDBSegmentPath,Integer.toString(segmentId) +"/." + stateName));
  }

  public static void clearSegmentState(FileSystem fs,Path crawlDBSegmentPath,int segmentId,int state) throws IOException { 
    String stateName = CrawlDBSegment.Status.toString(state);
    if (stateName.equals("")) 
      throw new IOException("Invalid State Name!");

    fs.delete(new Path(crawlDBSegmentPath,Integer.toString(segmentId) +"/." + stateName),false);
  }
  
}
