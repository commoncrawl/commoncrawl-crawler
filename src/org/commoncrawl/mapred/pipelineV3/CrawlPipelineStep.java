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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * A single map-reduce step in a sequence of steps encapsulated by a Task
 * 
 * @author rana
 * 
 */
public abstract class CrawlPipelineStep {

  public static Path makeUniqueFullyQualifiedOutputDirPath(Configuration conf,
      Path basePath, long databaseId) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Path uniquePath = new Path(basePath, Long.toString(databaseId));
    return fs.getFileStatus(uniquePath).getPath();
  }

  public static Path makeUniqueOutputDirPath(Path basePath, long databaseId)
      throws IOException {
    return new Path(basePath, Long.toString(databaseId));
  }

  public CrawlPipelineTask _task;

  public String            _name;

  public String            _outputDirName;

  private static final Log LOG = LogFactory.getLog(CrawlPipelineStep.class);

  public CrawlPipelineStep(CrawlPipelineTask task, String name,
      String outputDirName) {
    _task = task;
    _name = name;
    _outputDirName = outputDirName;
  }

  public <Type extends CrawlPipelineTask> Type findTaskOfType(
      Class<? extends CrawlPipelineTask> classType) {
    CrawlPipelineTask current = getTask();
    while (current != null) {
      if (current.getClass() == classType)
        return (Type) current;
      else
        for (CrawlPipelineTask dependency : current.getTaskDependencies()) {
          if (dependency.getClass() == classType) {
            return (Type) dependency;
          } else {
            CrawlPipelineTask searchResult = dependency
                .findTaskOfType(classType);
            if (searchResult != null)
              return (Type) searchResult;
          }
        }
      current = current.getTask();
    }
    return null;
  }

  public Path getBaseOutputDirForStep() throws IOException {
    return _task.getOutputDirForStep(getOutputDirName());
  }

  public Configuration getConf() throws IOException {
    return _task.getConf();
  }

  /**
   * 
   * @return list of dependcies requires for this task to run
   * @throws IOException
   */
  public Path[] getDependencies() throws IOException {
    return new Path[0];
  }

  public String getDescription() {
    if (_task != null) {
      return _task.getDescription() + " - Step(" + getName() + "):";
    } else {
      return getName();
    }
  }

  public FileSystem getFileSystem() throws IOException {
    return _task.getFileSystem();
  }

  public abstract Log getLogger();

  public String getName() {
    return _name;
  }

  public Path getOutputDir() throws IOException {
    return makeUniqueOutputDirPath(getBaseOutputDirForStep(), _task
        .getLatestDatabaseTimestamp());
  }

  /**
   * get output dir for a given class type by walking the node graph from the
   * top
   * 
   * @param targetClass
   * @return
   * @throws IOException
   */
  public Path getOutputDirForStep(Class<? extends CrawlPipelineStep> targetClass)
      throws IOException {
    CrawlPipelineTask rootTask = getRootTask();
    return rootTask.getOutputDirForStep(targetClass);
  }

  public String getOutputDirName() {
    return _outputDirName;
  }

  public String getPipelineStepName() {
    return _name;
  }

  public CrawlPipelineTask getRootTask() {
    CrawlPipelineTask current = getTask();
    while (current.getTask() != null)
      current = current.getTask();
    return current;
  }

  public CrawlPipelineTask getTask() {
    return _task;
  }

  public long getTaskIdentityId() throws IOException {
    return _task.getTaskIdentityId();
  }

  public Path getTaskIdentityPath() throws IOException {
    return _task.getTaskIdentityPath();
  }

  public Path getTempDir() throws IOException {
    return _task.getTempDirForStep(this);
  }

  public boolean isComplete() throws IOException {
    FileSystem fs = getFileSystem();
    Path outputDir = getOutputDir();
    if (!fs.exists(outputDir)) {
      getLogger().info(
          "Output Dir at: " + outputDir + " not found for:" + getDescription());
      return false;
    }
    getLogger().info(
        "Output Dir found at: " + outputDir + ". Step:" + getDescription()
            + " is complete.");
    return true;
  }

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

  protected boolean isTask() {
    return false;
  }

  public abstract void runStep(Path outputPathLocation) throws IOException;

  void runStepInternal() throws IOException {

    Path tempPath = getTempDir();
    Path outputDir = getOutputDir();

    getLogger().info(
        "Running " + getDescription() + " temp:" + tempPath + " finaloutput:"
            + outputDir);

    getFileSystem().mkdirs(tempPath.getParent());

    runStep(tempPath);

    getLogger().info(
        "Finished running: " + getDescription() + ".Promoting temp:" + tempPath
            + " to:" + outputDir);

    getFileSystem().mkdirs(outputDir.getParent());
    getFileSystem().rename(tempPath, outputDir);
  }
}
