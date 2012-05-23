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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * The name and size in bytes of a gzipped ARC file.
 * 
 * @author Albert Chern
 */
public class ARCResource implements Writable {

  private String name;
  private long size;

  public ARCResource(String name, long size) {
    this.name = name;
    this.size = size;
  }

  /**
   * Returns the name of this resource.
   */
  public String getName() {
    return name;
  }

  /**
   * Returns the size in bytes of this resource.
   */
  public long getSize() {
    return size;
  }

  /**
   * @inheritDoc
   */
  public void readFields(DataInput in) throws IOException {
    name = Text.readString(in);
    size = in.readLong();
  }

  /**
   * @inheritDoc
   */
  @Override
  public String toString() {
    return name + " " + size;
  }

  /**
   * @inheritDoc
   */
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, name);
    out.writeLong(size);
  }
}
