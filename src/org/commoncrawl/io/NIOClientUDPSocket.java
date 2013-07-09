package org.commoncrawl.io;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.security.SecureRandom;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.util.CCStringUtils;

public class NIOClientUDPSocket extends NIOClientSocket {

  DatagramChannel _channel = null;
  int _socketId = NIOSocket._idFactory.getNextId();
  NIOSocketListener _listener;
  InetSocketAddress _remoteAddress;
  
  private static final int EPHEMERAL_START = 1024;
  private static final int EPHEMERAL_STOP  = 65535;
  private static final int EPHEMERAL_RANGE  = EPHEMERAL_STOP - EPHEMERAL_START;

  private static SecureRandom prng = new SecureRandom();

  public static final Log LOG = LogFactory.getLog(NIOClientUDPSocket.class);
  
  public NIOClientUDPSocket() throws IOException{ 
    _channel = DatagramChannel.open();
    _channel.configureBlocking(false);
  }
  
  @Override
  public void close() {
    if (_channel != null) { 
      try {
        _channel.close();
      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
      }
      finally { 
        _channel = null;
      }
    }
  }

  @Override
  public AbstractSelectableChannel getChannel() {
    return _channel;
  }

  @Override
  public NIOSocketListener getListener() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getSocketId() {
    return _socketId;
  }

  @Override
  public boolean isOpen() {
    return _channel != null;
  }

  @Override
  public void setListener(NIOSocketListener listener) {
    _listener = listener;
  }

  private boolean bind_random(InetSocketAddress addr) throws IOException
  {
    InetSocketAddress temp;

    for (int i = 0; i < 1024; i++) {
      try {
        int port = prng.nextInt(EPHEMERAL_RANGE) + EPHEMERAL_START;
        if (addr != null)
          temp = new InetSocketAddress(addr.getAddress(),port);
        else
          temp = new InetSocketAddress(port);
        _channel.socket().bind(temp);
        return true;
      }
      catch (SocketException e) {
      }
    }
    return false;
  }

  
  @Override
  public void connect(InetSocketAddress remoteAddress) throws IOException {
    if (!bind_random(null)) { 
      throw new IOException("Unable to bind to random port!");
    }
    _channel.socket().connect(remoteAddress);
  }

  @Override
  public boolean finishConnect() throws IOException {
    return true;
  }

  @Override
  public InetSocketAddress getLocalSocketAddress() throws IOException {
    return new InetSocketAddress(_channel.socket().getLocalAddress(),_channel.socket().getLocalPort());
  }

  @Override
  public InetSocketAddress getSocketAddress() throws IOException {
    return _remoteAddress;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    return _channel.read(dst);
  }

  @Override
  public int write(ByteBuffer dst) throws IOException {
    return _channel.write(dst);
  }

}
