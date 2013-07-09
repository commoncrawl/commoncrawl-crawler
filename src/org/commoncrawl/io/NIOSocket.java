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

import java.nio.channels.spi.AbstractSelectableChannel;

/**
 * 
 * @author rana
 * 
 *         NIOSocket - Abstract interface used to represent an underlying
 *         asynchronous socket.
 * 
 */
public interface NIOSocket {

  static class IdFactory {

    static int _lastId = 0;

    synchronized int getNextId() {
      return ++_lastId;
    }
  }

  static IdFactory _idFactory = new IdFactory();

  /** closes (and disconnects) the socket */
  void close();

  /** get the NIO channel associated with this socket */
  AbstractSelectableChannel getChannel();

  /** get the socket listener for this object */
  NIOSocketListener getListener();

  /** get this socket's unique identifier **/
  int getSocketId();

  /** returns true if the socket is valid and connected */
  boolean isOpen();

  /** set the socket listener for this object */
  void setListener(NIOSocketListener listener);

  /** are reads disabled on this socket **/
  boolean readsDisabled();

}
