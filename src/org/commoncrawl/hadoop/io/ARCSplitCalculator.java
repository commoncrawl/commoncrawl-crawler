/**
 * Copyright 2008 - CommonCrawl Foundation
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
package org.commoncrawl.hadoop.io;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;

/**
 * Calculates splits based on the desired number of files per split and the
 * desired size of each split.
 * 
 * <p>
 * Concrete implementations should override {@link #getARCResources}.
 * 
 * @author Albert Chern
 */
public abstract class ARCSplitCalculator implements JobConfigurable {

  /**
   * <tt>arc.split.calculator.files.per.split</tt> - the property where the
   * number of files per input split is stored.
   * 
   * @see #setFilesPerSplit
   */
  public static final String P_FILES_PER_SPLIT = "arc.split.calculator.files.per.split";

  /**
   * <tt>arc.split.calculator.mb.per.split</tt> - the property where the desired
   * size in megabytes of the split is stored.
   * 
   * @see #setMegabytesPerSplit
   */
  public static final String P_MB_PER_SPLIT = "arc.split.calculator.mb.per.split";

  /**
   * Sets the desired number of files per input split.
   * 
   * <p>
   * Default is 1.
   * 
   * @param job
   *          the job to set the number of files per split for
   * @param filesPerSplit
   *          the desired number of ARC files per split
   * 
   * @see #P_FILES_PER_SPLIT
   */
  public static final void setFilesPerSplit(JobConf job, int filesPerSplit) {
    job.setInt(P_FILES_PER_SPLIT, filesPerSplit);
  }

  /**
   * Sets the desired number of megabytes per split.
   * 
   * <p>
   * New files will be added to a split until the total size of the split
   * exceeds this threshold. Default is no limit.
   * 
   * @param job
   *          the job to set the number of megabytes per split for
   * @param mbPerSplit
   *          the desired number of megabytes per split
   */
  public static final void setMegabytesPerSplit(JobConf job, int mbPerSplit) {
    job.setInt(P_MB_PER_SPLIT, mbPerSplit);
  }

  private int filesPerSplit;
  private long bytesPerSplit;

  private void addSplit(List<ARCSplit> splits, ARCResource[] resources, int size) {
    if (size > 0) {
      ARCResource[] copy = new ARCResource[size];
      System.arraycopy(resources, 0, copy, 0, size);
      splits.add(new ARCSplit(copy));
    }
  }

  /**
   * @inheritDoc
   */
  public final void configure(JobConf job) {
    filesPerSplit = job.getInt(P_FILES_PER_SPLIT, 1);
    bytesPerSplit = job.get(P_MB_PER_SPLIT) == null ? Long.MAX_VALUE
        : Long.parseLong(job.get(P_MB_PER_SPLIT)) * 1024 * 1024;
    configureImpl(job);
  }

  /**
   * Hook for subclass configuration.
   * 
   * @param job
   *          the {@link JobConf} of the job
   * 
   * @see JobConfigurable#configure
   */
  protected void configureImpl(JobConf job) {
  }

  /**
   * Given a job, returns the {@link ARCResource}s it should process.
   * 
   * @param job
   *          the job for which to get the {@link ARCResource}s
   * 
   * @return the {@link ARCResource}s to process
   * 
   * @throws IOException
   *           if an IO error occurs
   */
  protected abstract Collection<ARCResource> getARCResources(JobConf job) throws IOException;

  /**
   * @inheritDoc
   */
  public ARCSplit[] getARCSplits(JobConf job) throws IOException {

    List<ARCSplit> splits = new LinkedList<ARCSplit>();

    ARCResource[] resources = new ARCResource[filesPerSplit];
    int nResources = 0;
    long length = 0;

    for (ARCResource resource : getARCResources(job)) {
      resources[nResources++] = resource;
      length += resource.getSize();
      // When the split is too big, add it
      if (nResources >= filesPerSplit || length >= bytesPerSplit) {
        addSplit(splits, resources, nResources);
        nResources = 0;
        length = 0;
      }
    }

    // Add the final split
    addSplit(splits, resources, nResources);
    return splits.toArray(new ARCSplit[splits.size()]);
  }
}
