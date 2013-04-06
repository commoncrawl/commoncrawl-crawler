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
package org.commoncrawl.io;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * 
 * @author rana
 *
 */
public abstract class NIOClientSocket implements NIOSocket {
  boolean _readsDisabled;
  
  /** connect to the given address and port  */
  public abstract void  connect(InetSocketAddress address) throws IOException;
  /** used to finalize the establishment of the connection once the underlying Socket has become connectable*/
  public abstract boolean finishConnect()throws IOException;
  
  public abstract InetSocketAddress getLocalSocketAddress() throws IOException;
  public abstract InetSocketAddress getSocketAddress()throws IOException;
  
  /** read some data from the socket */
  public abstract int   read(ByteBuffer dst)throws IOException;
  /** write some data to the socket */
  public abstract int   write(ByteBuffer dst) throws IOException ;
  
  /** return true if reads have been disabled **/
  synchronized public boolean readsDisabled() { 
    return _readsDisabled;
  }
  
  synchronized public void disableReads() { 
    _readsDisabled = true;
  }
  
  synchronized public void enableReads() { 
   _readsDisabled = false;
  }

}
