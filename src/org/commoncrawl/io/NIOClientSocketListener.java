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

/**
 * NIOSocketListener - Callback interface used to propagate async Socket events
 * 
 * @author rana
 * 
 */
public interface NIOClientSocketListener extends NIOSocketListener {

  /** socket has connected event */
  void Connected(NIOClientSocket theSocket) throws IOException;

  /** socket is readable event - return number of bytes read ... */
  int Readable(NIOClientSocket theSocket) throws IOException;

  /** socket is writeable event - return true if additional writes pending ... */
  void Writeable(NIOClientSocket theSocket) throws IOException;
}
