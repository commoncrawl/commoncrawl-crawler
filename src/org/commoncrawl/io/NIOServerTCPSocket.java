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
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractSelectableChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.util.CCStringUtils;

/**
 * 
 * @author rana
 *
 */
public class NIOServerTCPSocket implements NIOServerSocket {

  private static void setClientSocketOptions(SocketChannel channel) throws IOException {
    channel.socket().setTcpNoDelay(true);
    channel.configureBlocking(false);
  }

  private static void setListenerSocketOptions(ServerSocketChannel channel) throws IOException {
    channel.socket().setReuseAddress(true);
    channel.configureBlocking(false);
  }

  int _socketId = NIOSocket._idFactory.getNextId();

  ServerSocketChannel _channel;

  NIOServerSocketListener _listener;

  public static final Log LOG = LogFactory.getLog(NIOServerTCPSocket.class);

  public NIOServerTCPSocket(NIOServerSocketListener listener) {

    _listener = listener;
  }

  // @Override
  public void acceptable() {
    if (_listener != null) {

      SocketChannel newClientChannel = null;
      try {
        newClientChannel = _channel.accept();
        // set socket options
        setClientSocketOptions(newClientChannel);
        // allocate a new NIOClientTCPSocket object ...
        NIOClientTCPSocket newSocketObj = new NIOClientTCPSocket(newClientChannel);
        // inform the listener of the event
        _listener.Accepted(newSocketObj);
      } catch (Exception e) {
        LOG.error("ERROR: ACCEPTING CONNECTION On Server Socket:" + _channel.toString() + " Exception:" +  CCStringUtils.stringifyException(e));
        if (newClientChannel != null) {
          try {
            newClientChannel.close();
          } catch (IOException e2) {
            LOG.error(CCStringUtils.stringifyException(e));
          }
        }
        throw new RuntimeException(e);
      }
    }

  }

  // @Override
  public void close() {
    try {
      if (_channel != null) {
        if (_channel.socket() != null)
          _channel.socket().close();
        _channel.close();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      _channel = null;
    }
  }

  // @Override
  public AbstractSelectableChannel getChannel() {
    return _channel;
  }

  // @Override
  public NIOSocketListener getListener() {
    return _listener;
  }

  // @Override
  public int getSocketId() {
    return _socketId;
  }

  // @Override
  public boolean isOpen() {
    return _channel != null;
  }

  // @Override
  public void open(InetSocketAddress address) throws IOException {
    if (_channel != null) {
      throw new IOException("Invalid State. Socket already bound");
    }
    LOG.info(this + " Binding to: " + address.getAddress().getHostAddress() + ":" + address.getPort());
    _channel = ServerSocketChannel.open();
    setListenerSocketOptions(_channel);
    _channel.socket().bind(address);
  }

  // @Override
  public void setListener(NIOSocketListener listener) {
    _listener = (NIOServerSocketListener) listener;
  }
}
