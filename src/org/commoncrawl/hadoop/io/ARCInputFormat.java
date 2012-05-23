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
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.commoncrawl.util.ArcFileReader;

/**
 * A map reduce input format for gzipped ARC files.
 * 
 * @author Albert Chern (Netseer Corp.)
 */
public class ARCInputFormat implements InputFormat, JobConfigurable {

  /**
   * <tt>arc.input.format.io.block.size</tt> - the property where the number of
   * bytes at a time to read from each input stream is stored.
   * 
   * <p>
   * Defaults to 32 KB.
   * 
   * @see #setIOBlockSize
   */
  public static final String P_IO_BLOCK_SIZE = "arc.input.format.io.block.size";

  /**
   * <tt>arc.input.format.io.buffer.size.mb</tt> - the property where the number
   * of bytes to use for IO buffering is stored.
   * 
   * <p>
   * Defaults to 10 MB.
   * 
   * @see #setIOBufferSize
   */
  public static final String P_IO_BUFFER_SIZE = "arc.input.format.io.buffer.size";

  /**
   * <tt>arc.input.format.io.timeout.ms</tt> - the property where the number of
   * milliseconds to block on input before throwing an exception is stored.
   * 
   * <p>
   * Defaults to 1 minute.
   * 
   * @see #setIOTimeout
   */
  public static final String P_IO_TIMEOUT = "arc.input.format.io.timeout.ms";

  /**
   * <tt>arc.input.format.arc.source.class</tt> - the property where the class
   * name of the {@link ARCSource} implementation to use is stored.
   */
  public static final String P_ARC_SOURCE = "arc.input.format.arc.source.class";

  private static final Log LOG = LogFactory.getLog(ARCInputFormat.class);

  /**
   * Sets the {@link ARCSource} implementation to use.
   * 
   * @param job
   *          the job to set the {@link ARCSource} for
   * @param arcSource
   *          the {@link Class} of the implementation to use
   * 
   * @see #P_ARC_SOURCE
   */
  public static void setARCSourceClass(JobConf job, Class arcSource) {
    job.setClass(P_ARC_SOURCE, arcSource, ARCSource.class);
  }

  /**
   * Sets the number of bytes to read at a time from each input stream.
   * 
   * @param job
   *          the job to set the IO block size for
   * @param blockSize
   *          the IO block size to use
   * 
   * @see #P_IO_BLOCK_SIZE
   */
  public static void setIOBlockSize(JobConf job, int blockSize) {
    job.setInt(P_IO_BLOCK_SIZE, blockSize);
  }

  /**
   * Sets the number of bytes to use for IO buffering.
   * 
   * @param job
   *          the job to set the buffer size for
   * @param bufferSize
   *          the number of bytes to use for IO buffering
   * 
   * @see #P_IO_BUFFER_SIZE
   */
  public static void setIOBufferSize(JobConf job, int bufferSize) {
    job.setInt(P_IO_BUFFER_SIZE, bufferSize);
  }

  /**
   * Sets the number of milliseconds to block on input before throwing an
   * exception.
   * 
   * @param job
   *          the job to set the timeout for
   * @param milliseconds
   *          the number of milliseconds to block on input before throwing an
   *          exception
   * 
   * @see #P_IO_TIMEOUT
   */
  public static void setIOTimeout(JobConf job, long milliseconds) {
    job.setLong(P_IO_TIMEOUT, milliseconds);
  }

  private int blockSize;
  private ARCSource arcSource;

  /**
   * @inheritDoc
   */
  public void configure(JobConf job) {

    blockSize = job.getInt(P_IO_BLOCK_SIZE, 32 * 1024);
    int bufferSize = job.getInt(P_IO_BUFFER_SIZE, 10 * 1024 * 1024);
    int queueSize = Math.max(1, bufferSize / blockSize);
    int timeout = job.getInt(P_IO_TIMEOUT, 60 * 1000);

    ArcFileReader.setBlockSize(blockSize);
    ArcFileReader.setBufferQueueSize(queueSize);
    ArcFileReader.setIOTimeoutValue(timeout);

    LOG.info("Block Size: " + blockSize);
    LOG.info("Queue Size: " + queueSize);
    LOG.info("IO Timeout: " + timeout);

    Class archiveSourceClass = job.getClass(P_ARC_SOURCE, JetS3tARCSource.class, ARCSource.class);
    arcSource = (ARCSource) ReflectionUtils.newInstance(archiveSourceClass, job);
  }

  /**
   * @inheritDoc
   */
  public RecordReader getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
    return new ARCSplitReader(job, (ARCSplit) split, arcSource, blockSize);
  }

  /**
   * @inheritDoc
   */
  public InputSplit[] getSplits(JobConf job, int ignored) throws IOException {
    InputSplit[] splits = arcSource.getARCSplits(job);
    if (splits.length < 1) {
      throw new IOException("No input to process");
    }
    LOG.info("Processing splits: " + Arrays.toString(splits));
    return splits;
  }

  /**
   * Required method for pre-0.19 versions of Hadoop.
   */
  public void validateInput(JobConf job) throws IOException {
  }
}
