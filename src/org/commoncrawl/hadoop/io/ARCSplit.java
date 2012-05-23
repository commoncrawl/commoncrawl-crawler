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
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;

/**
 * A map reduce input split for gzipped ARC files.
 * 
 * @author Albert Chern
 */
public class ARCSplit implements InputSplit {

  /**
   * The {@link ARCResource}s for this split.
   */
  private ARCResource[] resources;

  /**
   * The total size of this split in bytes.
   */
  private long size;

  /**
   * The hosts that the resources are located on.
   */
  private String[] hosts;

  /**
   * Default constructor for Hadoop.
   */
  public ARCSplit() {
  }

  /**
   * Constructs an <tt>ARCSplit</tt> for {@link ARCResources}.
   * 
   * @param resources
   *          the resource identifiers for this split's ARC files
   */
  public ARCSplit(ARCResource[] resources) {
    this(resources, new String[0]);
  }

  /**
   * Constructs an <tt>ARCSplit</tt> for {@link ARCResource}s whose locations
   * are known.
   * 
   * @param resources
   *          the {@link ARCResource}s for this split's ARC files
   * @param hosts
   *          the hosts that the resources are located on
   */
  public ARCSplit(ARCResource[] resources, String[] hosts) {
    this.resources = resources;
    this.hosts = hosts;
    for (ARCResource resource : resources) {
      size += resource.getSize();
    }
  }

  /**
   * @inheritDoc
   */
  public long getLength() throws IOException {
    return size;
  }

  /**
   * @inheritDoc
   */
  public String[] getLocations() throws IOException {
    return hosts;
  }

  /**
   * Returns the resources for this split.
   */
  public ARCResource[] getResources() {
    return resources;
  }

  /**
   * @inheritDoc
   */
  public void readFields(DataInput in) throws IOException {
    int nResources = in.readInt();
    resources = new ARCResource[nResources];
    for (int i = 0; i < nResources; i++) {
      resources[i] = new ARCResource(Text.readString(in), in.readLong());
    }
    size = in.readLong();
    hosts = null;
  }

  /**
   * @inheritDoc
   */
  @Override
  public String toString() {
    return "Resources: " + Arrays.toString(resources) + " Size: " + size + " Hosts: " + Arrays.toString(hosts);
  }

  /**
   * @inheritDoc
   */
  public void write(DataOutput out) throws IOException {
    out.writeInt(resources.length);
    for (ARCResource resource : resources) {
      resource.write(out);
    }
    out.writeLong(size);
    // The hosts are only used on the client side, so don't serialize them
  }
}
