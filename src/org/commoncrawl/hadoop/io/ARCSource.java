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
import java.io.InputStream;

import org.apache.hadoop.mapred.JobConf;

/**
 * An input source for gzipped ARC files.
 * 
 * <p>
 * This interface is a layer that allows gzipped ARC files to be read from
 * different sources, such as a Hadoop FileSystem, an S3 bucket, or an FTP
 * server.
 * 
 * @author Albert Chern (Netseer Corp.)
 */
public interface ARCSource {

  /**
   * Given a job, returns the {@link ARCSplit}s that it should process.
   * 
   * @param job
   *          the job that is being executed
   * 
   * @return an array with the {@link ARCSplit}s for this job
   * 
   * @throws IOException
   *           if an IO error occurs
   */
  public ARCSplit[] getARCSplits(JobConf job) throws IOException;

  /**
   * Returns a stream for the given resource (from an {@link ARCSplit}) at the
   * specified byte position, or <tt>null</tt> to signify that the
   * {@link ARCSplitReader} should give up at this point.
   * 
   * <p>
   * Different implementations are allowed to implement their own retry code
   * inside this method. The only supported method of recovery is to return a
   * new stream repositioned at the requested position. If this is not possible,
   * then the job cannot recover because it might have already processed records
   * read prior to the requested position.
   * 
   * @param resource
   *          the resource to read
   * @param streamPosition
   *          the byte position to start at
   * @param lastError
   *          the error that resulted in this call, or <tt>null</tt> if this is
   *          the first call
   * @param previousFailures
   *          the number of previous failures for this resource
   * 
   * @return an {@link InputStream} for the resource at the specified byte
   *         position, or <tt>null</tt> if the {@link ARCSplitReader} should
   *         give up at this point
   * 
   * @throws Throwable
   *           if an error occurs
   */
  public InputStream getStream(String resource, long streamPosition, Throwable lastError, int previousFailures)
      throws Throwable;
}
