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

/**
 * 
 * @author rana
 *
 */
public interface NIOServerSocket extends NIOSocket {

  /** a new client has connected to the server **/
  public void acceptable();

  /** open (bind) a server socket to the specified port (+address if specified) **/
  public void open(InetSocketAddress address) throws IOException;
}
