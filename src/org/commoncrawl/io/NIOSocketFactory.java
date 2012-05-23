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
import java.net.URL;

/**
 * NIOSocketFactory - factory object used to construct NIOSocket objects from
 * URL objects
 * 
 * @author rana
 * 
 */
public class NIOSocketFactory {

  static public NIOClientSocket createClientSocket(String protocol, InetSocketAddress localBindAddress,
      NIOClientSocketListener socketListener) throws IOException {

    if (protocol.equalsIgnoreCase("http")) {
      return new NIOClientTCPSocket(localBindAddress, socketListener);
    }
    throw new IOException("Unsupported Protocol Type:" + protocol);
  }

  static public NIOClientSocket createClientSocket(URL forURL, InetSocketAddress localBindAddress,
      NIOClientSocketListener socketListener) throws IOException {
    return createClientSocket(forURL.getProtocol(), localBindAddress, socketListener);
  }

}
