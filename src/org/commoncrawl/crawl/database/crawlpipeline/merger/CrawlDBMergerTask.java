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

package org.commoncrawl.crawl.database.crawlpipeline.merger;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.commoncrawl.crawl.database.crawlpipeline.CrawlPipelineTask;

/** 
 * Task that joins various pieces of data together to produce a crawl database
 * 
 * @author rana
 *
 */
public class CrawlDBMergerTask extends CrawlPipelineTask {

  public CrawlDBMergerTask(CrawlPipelineTask parentTask, String taskDescription) throws IOException {
    super(parentTask, taskDescription);
    
    addStep(new GenSegmentMetadataStep(this, "Generate Segment Metadata"));
    addStep(new GenArcFileMetadataStep(this, "Generate ArcFile Metadata"));
    addStep(new GenMergedDBStep(this, "Generate Merged DB"));
  }

  static final Log LOG = LogFactory.getLog(CrawlDBMergerTask.class);

  public static final String ARC_METADATA_STEP_NAME = "arcfileMetadata";
  public static final String SEGMENT_METADATA_STEP_NAME = "inputs";
  public static final String GEN_MERGED_DB_STEP_NAME = "merged";
  
  @Override
  public Path getTaskOutputBaseDir() {
    return new Path("crawl/" + getPipelineStepName());
  }

  @Override
  public Log getLogger() {
    return LOG;
  }

  @Override
  public String getPipelineStepName() {
    return "merge";
  }
  
  @Override
  public Path getBaseOutputDirForStep()throws IOException {
    return new Path("crawl/crawldb_new");
  }

  @Override
  protected void finalStepComplete() throws IOException {
    FileSystem fs = getFileSystem();
    
    Path finalMergeOutput = getBaseOutputDirForStep(GEN_MERGED_DB_STEP_NAME);
    getLogger().info(getDescription() + " Looking for final output at:" + finalMergeOutput);
    if (!fs.exists(finalMergeOutput)) { 
      getLogger().error(getDescription() + " Final Output not Found!");
      throw new IOException("Final Output not Found at:" + finalMergeOutput);
    }
    Path newCrawlDBPath = new Path(getBaseOutputDirForStep(),Long.toString(System.currentTimeMillis()));
    getLogger().info(getDescription() + " Promoting final output to:" + newCrawlDBPath);
    fs.rename(finalMergeOutput, newCrawlDBPath);
  }
  
  public static void main(String[] args)throws Exception {
    int res = ToolRunner.run(new Configuration(), new CrawlDBMergerTask(null,"Merge Task"), args);
    System.exit(res);    
  }
}
