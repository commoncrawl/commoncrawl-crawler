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


package org.commoncrawl.crawl.database.crawlpipeline.ec2.parser;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.util.StringUtils;

/**
 * A clone of portions of the FileOutputCommitter in Hadoop used to help 
 * isolate issues we were having with running the job on EMR using the S3N 
 * FileSystem.
 * 
 * @author rana
 *
 */
public class OutputCommitter extends FileOutputCommitter {


  @Override
  public void setupJob(org.apache.hadoop.mapred.JobContext context) throws IOException {
    LOG.info("Setup Job Called on Custom Committer");
    super.setupJob(context);
  }
  
  Path getTempTaskOutputPath(TaskAttemptContext taskContext) {
    JobConf conf = taskContext.getJobConf();
    Path outputPath = FileOutputFormat.getOutputPath(conf);
    if (outputPath != null) {
      Path p = new Path(outputPath,
          (FileOutputCommitter.TEMP_DIR_NAME + Path.SEPARATOR +
              "_" + taskContext.getTaskAttemptID().toString()));
      try {
        FileSystem fs = p.getFileSystem(conf);
        return p.makeQualified(fs);
      } catch (IOException ie) {
        LOG.warn(StringUtils .stringifyException(ie));
        return p;
      }
    }
    return null;
  }

  @Override
  public void commitTask(TaskAttemptContext context) 
  throws IOException {
    LOG.info("Commit Called on Task:" + context.getTaskAttemptID().toString());
    Path taskOutputPath = getTempTaskOutputPath(context);
    TaskAttemptID attemptId = context.getTaskAttemptID();
    JobConf job = context.getJobConf();
    if (taskOutputPath != null) {
      FileSystem fs = taskOutputPath.getFileSystem(job);
      LOG.info("FileSystem for commit for Task:"+ attemptId + " is:" + fs.getUri());
      context.getProgressible().progress();
      if (fs.exists(taskOutputPath)) {
        Path jobOutputPath = taskOutputPath.getParent().getParent();
        // Move the task outputs to their final place
        moveTaskOutputs(context, fs, jobOutputPath, taskOutputPath);
        // Delete the temporary task-specific output directory
        if (!fs.delete(taskOutputPath, true)) {
          LOG.info("Failed to delete the temporary output" + 
              " directory of task: " + attemptId + " - " + taskOutputPath);
        }
        LOG.info("Saved output of task '" + attemptId + "' to " +  jobOutputPath);
      }
    }
  }

  private void moveTaskOutputs(TaskAttemptContext context,
      FileSystem fs,
      Path jobOutputDir,
      Path taskOutput) 
  throws IOException {
    TaskAttemptID attemptId = context.getTaskAttemptID();
    context.getProgressible().progress();
    if (fs.isFile(taskOutput)) {
      Path finalOutputPath = getFinalPath(jobOutputDir, taskOutput, 
          getTempTaskOutputPath(context));
      LOG.info("Renaming:" + taskOutput + " to:" + finalOutputPath);
      if (!fs.rename(taskOutput, finalOutputPath)) {
        LOG.info("Rename Failed for:" + taskOutput + " to:" + finalOutputPath + " Trying Delete and then Rename");
        if (!fs.delete(finalOutputPath, true)) {
          throw new IOException("Failed to delete earlier output of task: " + 
              attemptId);
        }
        LOG.info("Renaming:" + taskOutput + " to: " + finalOutputPath);
        if (!fs.rename(taskOutput, finalOutputPath)) {
          throw new IOException("Failed to save output of task: " + 
              attemptId);
        }
      }
      LOG.info("Moved " + taskOutput + " to " + finalOutputPath);
    } else if(fs.getFileStatus(taskOutput).isDir()) {
      FileStatus[] paths = fs.listStatus(taskOutput);
      Path finalOutputPath = getFinalPath(jobOutputDir, taskOutput, 
          getTempTaskOutputPath(context));
      LOG.info("Moving " + taskOutput + " to " + finalOutputPath);
      fs.mkdirs(finalOutputPath);
      if (paths != null) {
        for (FileStatus path : paths) {
          LOG.info("Moving " + path.getPath());
          moveTaskOutputs(context, fs, jobOutputDir, path.getPath());
        }
      }
    }
  }

  private Path getFinalPath(Path jobOutputDir, Path taskOutput, 
      Path taskOutputPath) throws IOException {
    URI taskOutputUri = taskOutput.toUri();
    URI relativePath = taskOutputPath.toUri().relativize(taskOutputUri);
    if (taskOutputUri == relativePath) {//taskOutputPath is not a parent of taskOutput
      throw new IOException("Can not get the relative path: base = " + 
          taskOutputPath + " child = " + taskOutput);
    }
    if (relativePath.getPath().length() > 0) {
      return new Path(jobOutputDir, relativePath.getPath());
    } else {
      return jobOutputDir;
    }
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext context) 
  throws IOException {
    LOG.info("COMMITTER- Needs Commit Called on:" + context.getTaskAttemptID().toString());
    try {
      Path taskOutputPath = getTempTaskOutputPath(context);
      if (taskOutputPath != null) {
        context.getProgressible().progress();
        FileSystem fs = FileSystem.get(context.getJobConf());
        LOG.info("COMMITTER - Default FS is:" + fs.getUri());
        // Get the file-system for the task output directory
        FileSystem fsFromPath = taskOutputPath.getFileSystem(context.getJobConf());
        // since task output path is created on demand, 
        // if it exists, task needs a commit
        LOG.info("COMMITTER - Checking if outputPath Exists:" + taskOutputPath + " for task:" +context.getTaskAttemptID().toString() );
        if (fs.exists(taskOutputPath)) {
          LOG.info("Needs Commit Returning TRUE");
          return true;
        }
      }
    } catch (IOException  ioe) {
      throw ioe;
    }
    LOG.info("COMMITTER Needs Commit Returning FALSE");
    return false;
  }
  

}
