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

package org.commoncrawl.rpc.base.shared;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 
 * @author rana
 *
 */
public abstract class RPCStruct implements Cloneable {

  static protected final String emptyString = new String();

  /** clone support **/
  @Override
  public abstract Object clone() throws CloneNotSupportedException;

  /**
   * Deserialize a message from the given stream using the given protocol
   * decoder
   * 
   * @param in
   *          input stream
   * @param decoder
   *          protocol decoder
   */
  abstract public void deserialize(DataInput in, BinaryProtocol decoder) throws IOException;

  /** equals support **/
  @Override
  public abstract boolean equals(final Object peer);

  /**
   * return the struct's key as a string value
   * **/
  public String getKey() {
    return null;
  }

  /** hash code support **/
  @Override
  public abstract int hashCode();

  /**
   * merge support *
   * 
   * @throws CloneNotSupportedException
   */
  public abstract void merge(Object peer) throws CloneNotSupportedException;

  /**
   * Serialize to the given output stream using the given protocol encoder
   * 
   * @param out
   *          output stream
   * @param encoder
   *          protocol encoder
   */
  abstract public void serialize(DataOutput out, BinaryProtocol encoder) throws IOException;

}
