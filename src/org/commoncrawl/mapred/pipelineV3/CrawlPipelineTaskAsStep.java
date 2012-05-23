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
package org.commoncrawl.mapred.pipelineV3;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;

/**
 * 
 * @author rana
 *
 */
public abstract class CrawlPipelineTaskAsStep extends CrawlPipelineTask {

  private static final Log LOG = LogFactory
                                   .getLog(CrawlPipelineTaskAsStep.class);

  public CrawlPipelineTaskAsStep(CrawlPipelineTask parentTask,
      String taskDescription, String outputDirName) throws IOException {
    super(parentTask, taskDescription, outputDirName);
  }

  @Override
  public Path getTaskIdentityBasePath() throws IOException {
    return super.getTask().getTaskIdentityBasePath();
  }

  @Override
  public Path getTaskOutputBaseDir() {
    return new Path(super.getTask().getTaskOutputBaseDir(), getOutputDirName());
  }

  @Override
  public Path getTempDirForStep(CrawlPipelineStep step) throws IOException {
    Path tempOutputDir = new Path(CrawlEnvironment.getHadoopConfig().get(
        "mapred.temp.dir", ".")
        + "/"
        + getOutputDirName()
        + "/"
        + step.getOutputDirName()
        + "-"
        + System.currentTimeMillis());

    return tempOutputDir;
  }

  @Override
  public void runStep(Path unused) throws IOException {
    try {
      runTask(new String[0]);
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new IOException(e);
    }
  }
}
