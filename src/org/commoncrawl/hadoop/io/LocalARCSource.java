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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.commoncrawl.util.EscapeUtils;

/**
 * An {@link ARCSource} for local files.
 * 
 * @author Albert Chern
 */
public class LocalARCSource extends ARCSplitCalculator implements ARCSource, JobConfigurable {

  /**
   * <tt>local.arc.source.inputs</tt> - the property where the list of inputs is
   * stored.
   * 
   * @see #setInputs
   * @see #getInputs
   */
  public static final String P_INPUTS = "local.arc.source.inputs";

  /**
   * Returns the list of inputs set by {@link setInputs}.
   * 
   * @param job
   *          the job to get the inputs from
   * 
   * @return the list of inputs, or <tt>null</tt> if not set
   */
  public static String[] getInputs(JobConf job) {
    String inputs = job.get(P_INPUTS);
    return inputs == null ? null : EscapeUtils.split(',', inputs);
  }

  /**
   * Sets the list of inputs that will be processed.
   * 
   * <p>
   * Paths to add should either be for gzipped ARC files, or directories
   * containing gzipped ARC files.
   * 
   * @param job
   *          the job to set the inputs for
   * @param paths
   *          the paths to set as inputs
   * 
   * @see #P_INPUTS
   */
  public static void setInputs(JobConf job, String... paths) {
    job.set(P_INPUTS, EscapeUtils.concatenate(',', paths));
  }

  /**
   * @inheritDoc
   */
  @Override
  protected Collection<ARCResource> getARCResources(JobConf job) throws IOException {

    String[] inputs = getInputs(job);
    if (inputs == null) {
      throw new IllegalArgumentException("No inputs set");
    }

    Map<String, ARCResource> resources = new HashMap<String, ARCResource>();
    for (String input : inputs) {
      File file = new File(input);
      File[] files = file.isDirectory() ? file.listFiles() : new File[] { file };
      for (File f : files) {
        String path = f.getCanonicalPath();
        resources.put(path, new ARCResource(path, f.length()));
      }
    }
    return resources.values();
  }

  /**
   * @inheritDoc
   */
  public InputStream getStream(String resource, long streamPosition, Throwable lastError, int previousFailures)
      throws Throwable {

    if (lastError != null || previousFailures > 0) {
      // Don't retry...local IO failures are not expected
      return null;
    }

    if (streamPosition != 0) {
      // This shouldn't happen, but we'll check just in case
      throw new RuntimeException("Non-zero position requested");
    }

    return new FileInputStream(resource);
  }
}
