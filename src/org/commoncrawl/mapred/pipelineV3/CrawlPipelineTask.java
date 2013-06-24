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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;

import com.google.common.collect.ImmutableList;

/**
 * A Task, consisting of a set of map-reduce steps that are run in sequence
 * 
 * @author rana
 * 
 */
public abstract class CrawlPipelineTask extends CrawlPipelineStep implements
    Tool {

  private static final Log     LOG             = LogFactory
                                                   .getLog(CrawlPipelineStep.class);
  Configuration                _conf;
  ArrayList<CrawlPipelineStep> _steps          = new ArrayList<CrawlPipelineStep>();
  ArrayList<CrawlPipelineTask> _dependencyList = new ArrayList<CrawlPipelineTask>();
  protected String             _args[];
  protected Path _identityBasePath;
  protected Path _rootOutputDir;

  /**
   * constructor for top level task
   * 
   * @param conf
   * @param taskDescription
   * @throws IOException
   */
  public CrawlPipelineTask(Configuration conf,String taskDescription,String outputDirName) throws IOException {
    super(null, taskDescription, outputDirName);
    setConf(conf);
  }

  /**
   * constructor for a task running as a step in another task
   * 
   * @param parentTask
   * @param taskDescription
   * @throws IOException
   */
  public CrawlPipelineTask(CrawlPipelineTask parentTask,String taskDescription,String outputDirName) throws IOException {
    super(parentTask, taskDescription, outputDirName);
    setConf(parentTask.getConf());
    _identityBasePath = parentTask.getTaskIdentityBasePath();
  }

  public CrawlPipelineTask addStep(CrawlPipelineStep step) throws IOException {
    _steps.add(step);
    return this;
  }

  public void addTaskDependency(Class<? extends CrawlPipelineTask> taskClass)
      throws IOException {
    try {
      _dependencyList.add(taskClass.newInstance());
    } catch (Exception e) {
      LOG.error("Failed to create dependent task of type:" + taskClass
          + " Error:" + StringUtils.stringifyException(e));
      throw new IOException(e);
    }
  }

  protected void finalStepComplete(CrawlPipelineStep finalStep,
      Path finalStepOutputDir) throws IOException {

    if (promoteFinalStepOutput()) {
      FileSystem fs = getFileSystem();
      Path taskAsStepOutputDir = getOutputDir();
      fs.mkdirs(taskAsStepOutputDir);
      getLogger().info(
          "finalStepComplete callback triggered - promoting output from:"
              + finalStep.getDescription() + " to output dir:"
              + taskAsStepOutputDir);

      // copy everything from final step into task output ...
      FileStatus files[] = fs.globStatus(new Path(finalStepOutputDir, "*"));

      if (files.length != 0) {
        fs.delete(taskAsStepOutputDir, true);
        fs.mkdirs(taskAsStepOutputDir);
      }

      for (FileStatus file : files) {
        fs.rename(file.getPath(), new Path(taskAsStepOutputDir, file.getPath()
            .getName()));
      }
    }
  }

  @Override
  public Configuration getConf() {
    return _conf;
  }

  @Override
  public FileSystem getFileSystem() throws IOException {
    return FileSystem.get(getTaskIdentityBasePath().toUri(), _conf);
  }

  public long getLatestDatabaseTimestamp() throws IOException {
    FileSystem fs = FileSystem.get(getTaskIdentityBasePath().toUri(), _conf);

    LOG
        .info("Scanning for Database Candidates in:"
            + getTaskIdentityBasePath());

    FileStatus candidates[] = fs.globStatus(new Path(getTaskIdentityBasePath(),
        "*"));

    long candidateTimestamp = -1L;

    for (FileStatus candidate : candidates) {
      LOG.info("Found Seed Candidate:" + candidate.getPath());
      try {
        long timestamp = Long.parseLong(candidate.getPath().getName());
        if (candidateTimestamp == -1 || candidateTimestamp < timestamp) {
          candidateTimestamp = timestamp;
        }
      } catch (Exception e) {
        LOG.error("Skipping Path:" + candidate.getPath());
      }
    }
    LOG.info("Selected Candidate is:" + candidateTimestamp);
    return candidateTimestamp;
  }

  /**
   * Search Graph and return context based output path for given step
   * 
   * @param classId
   * @return
   */
  @Override
  public Path getOutputDirForStep(Class<? extends CrawlPipelineStep> targetClass)
      throws IOException {
    Path pathOut = null;
    for (CrawlPipelineStep step : _steps) {
      if (step.getClass() == targetClass) {
        pathOut = step.getOutputDir();
      } else if (step instanceof CrawlPipelineTask) {
        pathOut = ((CrawlPipelineTask) step).getOutputDirForStep(targetClass);
      }
      if (pathOut != null) {
        break;
      }
    }
    if (pathOut == null) {
      for (CrawlPipelineTask dependency : _dependencyList) {
        pathOut = dependency.getOutputDirForStep(targetClass);
        if (pathOut != null)
          break;
      }
    }
    return pathOut;
  }

  public Path getOutputDirForStep(String stepName) throws IOException {
    return new Path(getTaskOutputBaseDir(), stepName);
  }

  @Override
  public String getPipelineStepName() {
    return "Task:" + _name;
  }

  public List<CrawlPipelineStep> getSteps() {
    return ImmutableList.copyOf(_steps);
  }

  public List<CrawlPipelineTask> getTaskDependencies() {
    return _dependencyList;
  }


  public Path getTaskIdentityBasePath() {
    return _identityBasePath;
  }
  
  public void setTaskIdentityBasePath(Path path) { 
    _identityBasePath = path;
  }
  
  public Path getRootOutputDir() { 
    return _rootOutputDir;
  }
  
  public void setRootOutputDir(Path rootOutputDir) { 
    _rootOutputDir = rootOutputDir;
  }
  
  public Path getTaskOutputBaseDir() throws IOException {
    if (getTask() != null) { 
      return new Path(getTask().getTaskOutputBaseDir(), getOutputDirName());
    }
    else { 
      return new Path(_rootOutputDir,getOutputDirName());
    }
  }

  
  @Override
  public long getTaskIdentityId() throws IOException {
    return getLatestDatabaseTimestamp();
  }

  @Override
  public Path getTaskIdentityPath() throws IOException {
    return new Path(getTaskIdentityBasePath(), Long
        .toString(getLatestDatabaseTimestamp()));
  }


  public Path getTempDirForStep(CrawlPipelineStep step) throws IOException {
    Path tempOutputDir = new Path(CrawlEnvironment.getHadoopConfig().get(
        "mapred.temp.dir", ".")
        + "/" + step.getOutputDirName() + "-" + System.currentTimeMillis());

    return tempOutputDir;
  }

  @Override
  protected boolean isTask() {
    return true;
  }

  /** overload to parse arguements **/
  protected void parseArgs() throws IOException {

  }

  /** 
   * overload to initialize task after argument parsing ... 
   * 
   * @throws IOException
   */
  public void initTask(String[] args)throws IOException { 
    // save args ... 
    _args = args;
    // parse args .. 
    parseArgs();
  }

  protected boolean promoteFinalStepOutput() {
    return true;
  }

  @Override
  public int run(String[] args) throws Exception {
    // init task
    initTask(args);
    // run task next 
    return runTask();
  }
  
  

  @Override
  public void runStep(Path unused) throws IOException {
    try {
      int result = runTask();
      if (result != 0) {
        throw new IOException(getDescription() + " Failed With ErrorCode:"
            + result);
      }
    } catch (Exception e) {
      throw new IOException(getDescription() + " Failed With Exception:"
          + StringUtils.stringifyException(e));
    }
  }

  public CrawlPipelineStep getFinalStep() { 
    if (_steps.size() != 0) { 
      return _steps.get(_steps.size()-1);
    }
    return null;
  }
  
  public int runTask() throws Exception {

    for (CrawlPipelineTask dependency : _dependencyList) {
      getLogger().info(
          getDescription() + " - Running Dependency:"
              + dependency.getDescription());
      int result = dependency.run(_args);
      if (result != 0) {
        getLogger().error(
            "Dependency: " + dependency.getDescription()
                + " failed to complete successfully!");
        return result;
      }
    }

    try {
      getLogger().info(getDescription() + " - Iterating Steps");

      if (_steps.size() != 0) {
        CrawlPipelineStep finalStep = _steps.get(_steps.size() - 1);

        for (CrawlPipelineStep step : _steps) {
          // getLogger().info(getDescription() + " - Processing Step:" +
          // step.getName());
          if (!step.isComplete()) {
            // getLogger().info(getDescription() + " - Step:" + step.getName() +
            // " needs running. Checking dependencies");

            if (!step.isRunnable()) {
              getLogger().info(
                  getDescription() + " - Step:" + step.getName()
                      + " is not runnable!");
              return 1;
            } else {
              if (step.isTask()) { 
                ((CrawlPipelineTask)step).initTask(_args);
              }
              getLogger().info(
                  getDescription() + " - Running Step:" + step.getName());
              step.doStep();
              getLogger().info(
                  getDescription() + " - Finished Running Step:"
                      + step.getName());
            }
          }
          if (step == finalStep) {
            getLogger().info(
                getDescription() + " Final Step Complete - Calling Finalize");
            finalStepComplete(step, step.getOutputDir());
          }
        }
      }
    } catch (IOException e) {
      getLogger().error(
          getDescription() + " threw Exception:"
              + StringUtils.stringifyException(e));
      return 1;
    }
    return 0;
  }

  @Override
  public void setConf(Configuration conf) {
    _conf = conf;
    CrawlEnvironment.setHadoopConfig(_conf);
  }
  
  /** 
   * get any passed in args 
   * @return
   */
  public String[] getArgs() { 
    return _args;
  }
}
