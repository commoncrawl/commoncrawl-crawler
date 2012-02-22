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
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.util.shared.CCStringUtils;


/**
 * A Task, consisting of a set of map-reduce steps that are run in sequence 
 * 
 * @author rana
 *
 */
public abstract class CrawlPipelineTask extends CrawlPipelineStep implements Tool {

  private static final Log     LOG           = LogFactory.getLog(CrawlPipelineStep.class);
  Configuration                _conf;
  ArrayList<CrawlPipelineStep> _steps        = new ArrayList<CrawlPipelineStep>();
  String                       _args[];

  public static final String   CRAWL_DB_PATH = "crawl/crawldb_new";

  /**
   * constructor for a task running as a step in another task
   * 
   * @param parentTask
   * @param taskDescription
   * @throws IOException
   */
  public CrawlPipelineTask(CrawlPipelineTask parentTask, String taskDescription) throws IOException {
    super(parentTask, taskDescription);
  }
  
  @Override
  protected boolean isTask() { 
    return true;
  }

  /**
   * Constructor for a task running as a top level task
   * 
   * @param taskDescription
   * @throws IOException
   */
  public CrawlPipelineTask(String taskDescription) throws IOException {
    super(null, taskDescription);
  }

  public CrawlPipelineTask addStep(CrawlPipelineStep step) throws IOException {
    _steps.add(step);
    return this;
  }

  @Override
  public Configuration getConf() {
    return _conf;
  }

  public FileSystem getFileSystem() throws IOException {
    return CrawlEnvironment.getDefaultFileSystem();
  }

  @Override
  public void setConf(Configuration conf) {
    _conf = conf;
    CrawlEnvironment.setHadoopConfig(_conf);
  }

  public abstract Path getTaskOutputBaseDir();

  public Path getTaskIdentityBasePath() throws IOException {
    return new Path(CRAWL_DB_PATH);
  }

  public Path getTaskIdentityPath() throws IOException {
    return new Path(getTaskIdentityBasePath(),Long.toString(getLatestDatabaseTimestamp()));
  }
  
  public long getTaskIdentityId() throws IOException { 
    return getLatestDatabaseTimestamp();
  }

  public long getLatestDatabaseTimestamp() throws IOException {
    FileSystem fs = CrawlEnvironment.getDefaultFileSystem();

    FileStatus candidates[] = fs.globStatus(new Path(getTaskIdentityBasePath(), "*"));

    long candidateTimestamp = -1L;

    for (FileStatus candidate : candidates) {
      LOG.info("Found Seed Candidate:" + candidate.getPath());
      long timestamp = Long.parseLong(candidate.getPath().getName());
      if (candidateTimestamp == -1 || candidateTimestamp < timestamp) {
        candidateTimestamp = timestamp;
      }
    }
    LOG.info("Selected Candidate is:" + candidateTimestamp);
    return candidateTimestamp;
  }

  protected void parseArgs() throws IOException {

  }

  
  @Override
  public void run(Path unused) throws IOException {
    try {
      int result = run(_args);
      if (result != 0) {
        throw new IOException(getDescription() + " Failed With ErrorCode:" + result);
      }
    } catch (Exception e) {
      throw new IOException(getDescription() + " Failed With Exception:" + CCStringUtils.stringifyException(e));
    }
  }

  protected Path getOutputDirForStep(String stepName)throws IOException { 
    return new Path(getTaskOutputBaseDir(),stepName);
  }
  
  @Override
  public int run(String[] args) throws Exception {
    _args = args;
    try {
      getLogger().info(getDescription() + " - Parsing Arguements");
      parseArgs();
      
      getLogger().info(getDescription() + " - Iterating Steps");

      if (_steps.size() != 0) { 
        CrawlPipelineStep finalStep = _steps.get(_steps.size() - 1);
        
        for (CrawlPipelineStep step : _steps) {
          getLogger().info(getDescription() + " - Processing Step:" + step.getName());
          if (!step.isComplete()) {
            getLogger().info(getDescription() + " - Step:" + step.getName() + " needs running. Checking dependencies");
  
            if (!step.isRunnable()) {
              getLogger().info(getDescription() + " - Step:" + step.getName() + " is not runnable!");
              return 1;
            } else {
              getLogger().info(getDescription() + " - Running Step:" + step.getName());
              step.runStepInternal();
              getLogger().info(getDescription() + " - Finished Running Step:" + step.getName());
            }
          }
          if (step == finalStep) {
            getLogger().info(getDescription() + " Final Step Complete - Calling Finalize Task");
            finalStepComplete();
          }
        }
      }
    } catch (IOException e) {
      getLogger().error(getDescription() + " threw Exception:" + CCStringUtils.stringifyException(e));
      return 1;
    }
    return 0;
  }
  
  protected abstract void finalStepComplete()throws IOException; 
}
