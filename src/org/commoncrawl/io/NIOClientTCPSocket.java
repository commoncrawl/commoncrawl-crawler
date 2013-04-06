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
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractSelectableChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * NIOTCPSocket - TCP version of NIOSocket
 * 
 * @author rana
 * 
 */
public class NIOClientTCPSocket extends NIOClientSocket {

  enum SocketType {
    /** outgoing socket, initiated by the client **/
    Outgoing,
    /** incoming socket, accepted by the server **/
    Incoming
  }

  public static final Log LOG = LogFactory.getLog(NIOClientTCPSocket.class);

  private static void probeAndSetSize(boolean sendSize, int targetSize, int minSize, SocketChannel channel)
      throws IOException {

    if (sendSize && channel.socket().getSendBufferSize() >= targetSize) {
      // System.out.println("SendSize is Already:" +
      // channel.socket().getSendBufferSize());
      return;
    } else if (!sendSize && channel.socket().getReceiveBufferSize() >= targetSize) {
      // System.out.println("RcvSize is Already:" +
      // channel.socket().getReceiveBufferSize());
      return;
    }

    do {

      int sizeOut = 0;
      if (sendSize) {
        channel.socket().setSendBufferSize(targetSize);
        sizeOut = channel.socket().getSendBufferSize();
      } else {
        channel.socket().setReceiveBufferSize(targetSize);
        sizeOut = channel.socket().getReceiveBufferSize();
      }
      if (sizeOut == targetSize)
        break;
      targetSize >>= 1;
    } while (targetSize > minSize);

  }

  private static void setClientSocketOptions(SocketChannel channel) throws IOException {
    channel.socket().setPerformancePreferences(0, 1, 3);
    channel.socket().setTcpNoDelay(true);
    channel.socket().setSoLinger(false, 0);
    channel.socket().setKeepAlive(true);
    probeAndSetSize(false, 2 << 16, 2 << 10, channel);
    probeAndSetSize(true, 2 << 15, 2 << 10, channel);
    channel.configureBlocking(false);
  }

  int _socketId = NIOSocket._idFactory.getNextId();
  SocketType _socketType;
  SocketChannel _channel;
  NIOClientSocketListener _listener;

  InetSocketAddress _localAddress;

  InetSocketAddress _address;

  public NIOClientTCPSocket(InetSocketAddress localAddress, NIOClientSocketListener socketListener) throws IOException {
    _socketType = SocketType.Outgoing;
    _channel = SocketChannel.open();
    if (localAddress != null) {
      InetSocketAddress modifiedLocalAddress = new InetSocketAddress(localAddress.getAddress(), 0);
      // System.out.println("Binding To Local Address:" +
      // modifiedLocalAddress.getAddress());
      // LOG.info(this + "Binding To Local Address:" +
      // modifiedLocalAddress.getAddress());
      _channel.socket().bind(modifiedLocalAddress);
    }
    _localAddress = (InetSocketAddress) _channel.socket().getLocalSocketAddress();
    setClientSocketOptions(_channel);
    _listener = socketListener;

  }

  /**
   * internal constructor used to create NIOSocket objects for incoming client
   * connections
   **/
  NIOClientTCPSocket(SocketChannel channel) throws IOException {
    _socketType = SocketType.Incoming;
    _channel = channel;
    _address = new InetSocketAddress(channel.socket().getInetAddress(), channel.socket().getPort());
  }

  // @Override
  public void close() {

    if (_channel != null) {
      try {
        _channel.close();
      } catch (IOException e) {
        System.out.println(e);
      }
      _channel = null;
    }
  }

  // @Override
  public void connect(InetSocketAddress address) throws IOException {
    // System.out.println("Connecting to:"+address.getHostAddress()+" at port:"+Integer.toString(port));
    if (_socketType == SocketType.Incoming) {
      throw new IOException("Invalid State-Connect called on an Incoming (server) Socket");
    }
    _address = address;

    if (_channel == null) {
      throw new IOException("Invalid State- Channel Not Open during connect call");
    }
    // LOG.info(this + "Connecting to: " + address.getAddress().getHostAddress()
    // + ":" + address.getPort() + " via Interface:" +
    // _channel.socket().getLocalAddress().getHostAddress());
    _channel.connect(address);
  }

  // @Override
  public boolean finishConnect() throws IOException {
    if (_socketType == SocketType.Incoming) {
      throw new IOException("Invalid State-Connect called on an Incoming (server) Socket");
    }

    if (_channel == null) {
      throw new IOException("Invalid State - finishConnect called on closed channel");
    }

    try {
      if (_channel.finishConnect()) {

        _channel.socket().setKeepAlive(true);

        // LOG.info(this + "Connected to: " +
        // _address.getAddress().getHostAddress() + ":" + _address.getPort() +
        // " via Interface:" +
        // _channel.socket().getLocalAddress().getHostAddress());

        return true;
      }
    } catch (IOException e) {
      // LOG.error("channel.finishConnect to address:" +
      // _address.getAddress().getHostAddress() +" port: " + _address.getPort()
      // + " threw exception:" + e.toString());
      throw e;
    }
    return false;
  }

  // @Override
  public AbstractSelectableChannel getChannel() {
    return _channel;
  }

  // @Override
  public NIOSocketListener getListener() {
    return _listener;
  }

  public InetSocketAddress getLocalSocketAddress() throws IOException {
    if (_channel != null && _channel.isOpen()) {
      return (InetSocketAddress) _channel.socket().getLocalSocketAddress();
    }
    return _localAddress;
  }

  // @Override
  public InetSocketAddress getSocketAddress() throws IOException {
    return _address;
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
  public int read(ByteBuffer dst) throws IOException {
    if (_channel == null) {
      throw new IOException("Invalid State - read called on closed channel");
    }
    return _channel.read(dst);
  }

  /** set the socket listener for this object */
  public void setListener(NIOSocketListener listener) {
    _listener = (NIOClientSocketListener) listener;
  }

  // @Override
  public int write(ByteBuffer src) throws IOException {
    if (_channel == null) {
      throw new IOException("Invalid State - read called on closed channel");
    }
    return _channel.write(src);
  }

}
