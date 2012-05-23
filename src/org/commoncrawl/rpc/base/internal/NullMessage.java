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
package org.commoncrawl.rpc.base.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.commoncrawl.rpc.base.shared.BinaryProtocol;
import org.commoncrawl.rpc.base.shared.RPCStruct;

/**
 * 
 * @author rana
 *
 */
public class NullMessage extends RPCStruct {

  static NullMessage singleton = new NullMessage();

  public static NullMessage getSingleton() {
    return singleton;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {

    throw new CloneNotSupportedException();
  }

  @Override
  public void deserialize(DataInput in, BinaryProtocol decoder) throws IOException {
    // NOOP
  }

  @Override
  public boolean equals(Object peer) {
    return false;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public void merge(Object peer) throws CloneNotSupportedException {
    throw new CloneNotSupportedException();
  }

  @Override
  public void serialize(DataOutput out, BinaryProtocol encoder) throws IOException {
    // noop
  }
}
