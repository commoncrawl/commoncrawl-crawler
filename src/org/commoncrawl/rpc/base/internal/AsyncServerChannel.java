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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;

import org.commoncrawl.async.EventLoop;
import org.commoncrawl.async.Timer;
import org.commoncrawl.io.NIOClientSocket;
import org.commoncrawl.io.NIOClientTCPSocket;
import org.commoncrawl.io.NIOServerSocketListener;
import org.commoncrawl.io.NIOServerTCPSocket;
import org.commoncrawl.io.NIOSocket;
import org.commoncrawl.rpc.base.shared.RPCException;

/**
 * 
 * @author rana
 *
 */
public class AsyncServerChannel implements NIOServerSocketListener {

  public static interface ConnectionCallback {

    void IncomingClientConnected(AsyncClientChannel channel);

    void IncomingClientDisconnected(AsyncClientChannel channel);

  }

  private enum State {
    OPEN, OPEN_CONNECTED, CLOSED
  }

  private static int INITIAL_RECONNECT_DELAY = 1000;
  InetSocketAddress _address;
  EventLoop _asyncDispatcher;
  NIOServerTCPSocket _socket;
  Timer _reconnectTimer = null;
  int _reconnectDelay = 0;
  Server _server;
  LinkedList<AsyncClientChannel> _activeClients = new LinkedList<AsyncClientChannel>();

  ConnectionCallback _callback;

  State _state = State.CLOSED;

  public AsyncServerChannel(Server server, EventLoop client, InetSocketAddress address, ConnectionCallback callback) {
    _address = address;
    _asyncDispatcher = client;
    _server = server;
    _callback = callback;
  }

  // @Override
  public void Accepted(NIOClientSocket newClientSocket) throws IOException {
    AsyncClientChannel newChannel = new AsyncClientChannel((NIOClientTCPSocket) newClientSocket, this);
    _activeClients.add(newChannel);
    if (_callback != null) {
      _callback.IncomingClientConnected(newChannel);
    }
  }

  void bind() throws IOException {
    // increate reconnect delay ...
    _reconnectDelay = _reconnectDelay * 2;
    _reconnectTimer = null;

    if (_socket == null) {
      _socket = new NIOServerTCPSocket(this);
      _socket.open(_address);
      _asyncDispatcher.getSelector().registerForAccept(_socket);
      // update state ...
      _state = State.OPEN_CONNECTED;
    }
  }

  void ClientChannelDisconnected(AsyncClientChannel channel) {
    if (_callback != null)
      _callback.IncomingClientDisconnected(channel);
    _activeClients.remove(channel);
  }

  public void close() {
    if (_state != State.CLOSED) {
      release();
    }
  }

  // @Override
  public void Disconnected(NIOSocket theSocket, Exception disconnectReason) throws IOException {

  }

  final void dispatchRequest(AsyncClientChannel source, Frame.IncomingFrame frame) throws RPCException {
    getServer().dispatchRequest(this, source, frame);
  }

  public void Excepted(NIOSocket socket, Exception e) {

  }

  Server getServer() {
    return _server;
  }

  public void open() throws IOException {
    if (_state == State.CLOSED) {
      rebind();
    }
  }

  void rebind() throws IOException {

    release();

    if (_reconnectDelay == 0) {
      _reconnectDelay = INITIAL_RECONNECT_DELAY;
      bind();
    } else {
      _reconnectTimer = new Timer(_reconnectDelay, false,

      new Timer.Callback() {

        // @Override
        public void timerFired(Timer timer) {
          try {
            bind();
          } catch (IOException e) {
            e.printStackTrace();
            try {
              rebind();
            } catch (IOException e2) {

            }

          }
        }
      });

      _asyncDispatcher.setTimer(_reconnectTimer);
    }
  }

  void release() {
    // stop accepting sockets on this host ...
    if (_reconnectTimer != null) {
      _asyncDispatcher.cancelTimer(_reconnectTimer);
      _reconnectTimer = null;
    }
    if (_socket != null) {
      _asyncDispatcher.getSelector().cancelRegistration(_socket);
      _socket.close();
      _socket = null;
    }

    for (AsyncClientChannel client : _activeClients) {
      try {
        client.close();
      } catch (IOException e) {

      }
    }
    _activeClients.clear();

    _state = State.CLOSED;
  }

  public void sendResponse(AsyncContext context) throws RPCException {
    context.getClientChannel().sendResponse(context);
  }
}
