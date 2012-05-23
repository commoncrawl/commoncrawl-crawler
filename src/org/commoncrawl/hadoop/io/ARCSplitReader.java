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
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.StringUtils;
import org.commoncrawl.protocol.shared.ArcFileItem;
import org.commoncrawl.util.ArcFileReader;

/**
 * A Hadooop {@link RecordReader} for reading {@link ARCSplit}s.
 * 
 * @author Albert Chern
 */
public class ARCSplitReader implements RecordReader<Text, ArcFileItem> {

  private class IOThread extends Thread {

    @Override
    public void run() {

      for (int i = 0; i < readers.length; i++) {

        ARCResource resource = split.getResources()[i];
        ArcFileReader reader = readers[i];
        InputStream stream = null;
        long streamPosition = 0;
        int failures = 0;
        Throwable lastError = null;

        while (true) {

          try {
            stream = source.getStream(resource.getName(), streamPosition, lastError, failures);
            if (stream == null) {
              // The ARCSource is telling us to fail
              LOG.fatal("IOThread exiting", lastError);
              error = lastError;
              return;
            }
            ReadableByteChannel channel = Channels.newChannel(stream);

            while (true) {
              // Just keep passing buffers to the reader. The
              // reader is supposed to limit the number of buffers
              // it will accept and block when it is at the limit.
              ByteBuffer buffer = ByteBuffer.allocate(blockSize);
              int bytesRead = channel.read(buffer);

              if (bytesRead > 0) {
                streamPosition += bytesRead;
                totalBytesRead += bytesRead;
                buffer.flip();
                reader.available(buffer);
              } else if (bytesRead == -1) {
                // Create the next reader before closing this
                // one so that we don't get a race condition
                // where the other thread tries to access the
                // next reader before it is instantiated.
                if (i + 1 < readers.length) {
                  readers[i + 1] = new ArcFileReader();
                }
                reader.finished();
                break;
              }
            }

            break;

          } catch (Throwable t1) {
            lastError = t1;
            failures++;
          } finally {
            try {
              if (stream != null) {
                stream.close();
              }
            } catch (Throwable t2) {
            }
            stream = null;
          }
        }
      }
    }
  }

  private static final Log LOG = LogFactory.getLog(ARCSplitReader.class);

  public static final String SPLIT_DETAILS = "arc.reader.split.details";
  private ARCSplit split;
  private ARCSource source;
  private int blockSize;
  private ArcFileReader[] readers;
  private int readerIndex;
  private long totalBytesRead;

  private Throwable error;

  /**
   * Creates a new <tt>ARCSplitReader</tt>.
   * 
   * @param split
   *          the {@link ARCSplit} to read
   * @param source
   *          the {@link ARCSource} to open resources with
   * @param blockSize
   *          the number of bytes at a time to read from each input stream
   */
  public ARCSplitReader(JobConf job, ARCSplit split, ARCSource source, int blockSize) {
    this.split = split;
    // record split details in job config for debugging purposes ...
    job.set(SPLIT_DETAILS, split.toString());
    this.source = source;
    this.blockSize = blockSize;
    this.readers = new ArcFileReader[split.getResources().length];
    this.readers[0] = new ArcFileReader();
    this.readerIndex = 0;
    this.totalBytesRead = 0;
    this.error = null;
    new IOThread().start();
  }

  /**
   * @inheritDoc
   */
  public void close() throws IOException {
  }

  /**
   * @inheritDoc
   */
  public Text createKey() {
    return new Text();
  }

  /**
   * @inheritDoc
   */
  public ArcFileItem createValue() {
    return new ArcFileItem();
  }

  /**
   * @inheritDoc
   */
  public long getPos() throws IOException {
    return totalBytesRead;
  }

  /**
   * @inheritDoc
   */
  public float getProgress() throws IOException {
    return totalBytesRead / (float) split.getLength();
  }

  /**
   * Gets the next {@link ArcFileItem} from the split.
   * 
   * @param item
   *          the {@link ArcFileItem} to populate
   * 
   * @return <tt>true</tt> if a new item was read, or <tt>false</tt> if there
   *         are no more
   * 
   * @throws IOException
   *           if an IO error or timeout occurs
   */
  public boolean next(ArcFileItem item) throws IOException {

    while (readerIndex < readers.length) {
      if (readers[readerIndex].hasMoreItems()) {
        // populate arc file path in item
        item.setArcFileName(split.getResources()[readerIndex].getName());
        try {
          // and then delegate to reader instance
          readers[readerIndex].getNextItem(item);
        } catch (IOException e) {
          LOG.error("IOException in ARCSplitReader.next().ArcFile:" + item.getArcFileName() + "\nException:"
              + StringUtils.stringifyException(e));
          throw e;
        } catch (Exception e) {
          LOG.error("Unknown Exception thrown in ARCSplitReader.next().ArcFile:" + item.getArcFileName()
              + "\nException:" + StringUtils.stringifyException(e));
          throw new RuntimeException(e);
        }
        return true;
      } else {
        readers[readerIndex++] = null;
      }
    }

    return false;
  }

  /**
   * @inheritDoc
   */
  public boolean next(Text key, ArcFileItem value) throws IOException {

    if (next(value)) {
      // set uri from given key ...
      key.set(value.getUri());
      // TODO: we are going to clear the uri field's dirty flag since it is
      // redundant
      // this means the uri will not be serialized with the rest of the item's
      // data...
      value.setFieldClean(ArcFileItem.Field_URI);
      return true;
    }
    return false;
  }
}
