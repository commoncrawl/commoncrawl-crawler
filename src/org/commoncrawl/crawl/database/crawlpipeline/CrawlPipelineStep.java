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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;

/** 
 * A single map-reduce step in a sequence of steps encapsulated by a Task
 * 
 * @author rana
 *
 */
public abstract class CrawlPipelineStep {

	public CrawlPipelineTask _task;
	public String _name;

	private static final Log LOG = LogFactory.getLog(CrawlPipelineStep.class);

	protected boolean isTask() { 
	  return false;
	}
	
	public CrawlPipelineStep(CrawlPipelineTask task, String name)
			throws IOException {
		_task = task;
		_name = name;
	}

	public FileSystem getFileSystem() throws IOException {
		return _task.getFileSystem();
	}

	public String getName() {
		return _name;
	}

	public String getDescription() {
		if (_task != null) {
			return _task.getDescription() + " - Step(" + getName() + "):";
		} else {
			return getName();
		}
	}

	/**
	 * 
	 * @return list of dependcies requires for this task to run
	 * @throws IOException
	 */
	public Path[] getDependencies() throws IOException {
		return new Path[0];
	}

	public abstract Log getLogger();

	public boolean isRunnable() throws IOException {

	  getLogger().info("Checking dependencies for:" + getDescription());

		FileSystem fs = getFileSystem();

		for (Path path : getDependencies()) {
			if (!fs.exists(path)) {
				getLogger().info(
						"File:" + path + " does NOT exist." + getDescription()
						+ " is not Runnable");

				return false;
			}
		}
		getLogger().info("All dependencies for " + getDescription() + " met.");

		return true;
	}

	public boolean isComplete() throws IOException {
		FileSystem fs = getFileSystem();
		Path outputDir = getOutputDir();
		if (!fs.exists(outputDir)) {
			getLogger().info(
					"Output Dir at: " + outputDir + " not found for:"
							+ getDescription());
			return false;
		}
		getLogger().info(
				"Output Dir found at: " + outputDir + ". Step:"
						+ getDescription() + " is complete.");
		return true;
	}

	public abstract String getPipelineStepName();

	public Path getBaseOutputDirForStep()throws IOException {
    return _task.getOutputDirForStep(getPipelineStepName());
	}

	public Path getBaseOutputDirForStep(String stepName)throws IOException { 
	  return _task.getOutputDirForStep(stepName);
	}

	public Path getOutputDir() throws IOException {
		return makeUniqueOutputDirPath(getBaseOutputDirForStep(),
				_task.getLatestDatabaseTimestamp());
	}
	

  public Path getTaskIdentityPath() throws IOException {
    return _task.getTaskIdentityPath();
  }
  
  public long getTaskIdentityId() throws IOException { 
    return _task.getTaskIdentityId();
  }
  
	public static Path makeUniqueOutputDirPath(Path basePath, long databaseId)
			throws IOException {
		return new Path(basePath, Long.toString(databaseId));
	}

	public Path getTempDir() throws IOException {

		Path tempOutputDir = new Path(CrawlEnvironment.getHadoopConfig().get(
				"mapred.temp.dir", ".")
				+ "/"
				+ getPipelineStepName()
				+ "-"
				+ System.currentTimeMillis());

		return tempOutputDir;
	}

	void runStepInternal() throws IOException {

		Path tempPath = getTempDir();
		Path outputDir = getOutputDir();

		getLogger().info(
				"Running " + getDescription() + " temp:" + tempPath
				+ " finaloutput:" + outputDir);

		run(tempPath);

		getLogger().info(
				"Finished running: " + getDescription() + ".Promoting temp:"
						+ tempPath + " to:" + outputDir);
	}

	public abstract void run(Path outputPathLocation) throws IOException;
}
