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

package org.commoncrawl.util;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.ValueBytes;
import org.apache.hadoop.mapred.FileSplit;

/**
 * 
 * @author rana
 *
 */
public class RawRecordReader {

  
  private SequenceFile.Reader in;
  private long start;
  private long end;
  private boolean more = true;
  protected Configuration conf;
  private ValueBytes valueBytesOut = null;

  public RawRecordReader(Configuration conf, FileSplit split) throws IOException {
    
  	Path path = split.getPath();
    FileSystem fs = path.getFileSystem(conf);
    this.in = new SequenceFile.Reader(fs, path, conf);
    this.valueBytesOut = in.createValueBytes();
    this.end = split.getStart() + split.getLength();
    this.conf = conf;

    if (split.getStart() > in.getPosition())
      in.sync(split.getStart());                  // sync to start

    this.start = in.getPosition();
    more = start < end;
  }
   
  public synchronized ValueBytes next(DataOutputBuffer keyOut) throws IOException {
    if (more) { 
	    long pos = in.getPosition();
	    boolean remaining = (in.nextRawKey(keyOut) != -1);
	    if (remaining) {
	      
	    	in.nextRawValue(valueBytesOut);
	    }
	    if (pos >= end && in.syncSeen()) {
	      more = false;
	    } else {
	      more = remaining;
	    }
	    if (more) { 
	    	return valueBytesOut;
	    }
    }
    return null;
  }
  
  /**
   * Return the progress within the input split
   * @return 0.0 to 1.0 of the input byte range
   */
  public float getProgress() throws IOException {
    if (end == start) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (in.getPosition() - start) / (float)(end - start));
    }
  }
  
  public synchronized long getPos() throws IOException {
    return in.getPosition();
  }
  
  protected synchronized void seek(long pos) throws IOException {
    in.seek(pos);
  }
  public synchronized void close() throws IOException { in.close(); }
  
	
}
