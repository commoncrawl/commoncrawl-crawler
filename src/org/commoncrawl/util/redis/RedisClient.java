package org.commoncrawl.util.redis;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Deque;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.async.Timer;
import org.commoncrawl.io.NIOBufferList;
import org.commoncrawl.io.NIOBufferListOutputStream;
import org.commoncrawl.io.NIOClientSocket;
import org.commoncrawl.io.NIOClientSocketListener;
import org.commoncrawl.io.NIOClientTCPSocket;
import org.commoncrawl.io.NIOSocket;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.redis.RedisCmd.Commands;
import org.commoncrawl.util.redis.RedisResponse.Type;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;

/** 
 * Async Redis Client
 * 
 * @author rana
 *
 */
public class RedisClient implements NIOClientSocketListener {

  /** logging **/
  private static final Log LOG = LogFactory.getLog(RedisClient.class);

  /** some specific byte patterns we care about **/
  static final byte[] CRLF = "\r\n".getBytes(Charsets.US_ASCII);
  static final byte[] OK = "OK".getBytes(Charsets.US_ASCII);
  static final byte[] QUEUED = "QUEUED".getBytes(Charsets.US_ASCII);

  /** max amount of encoded command data we buffer for transmission **/
  private static final int MAX_QUEUED_WRITES_SIZE = 1 << 16; 
  /** default reconnect delay **/
  private static final int INITIAL_RECONNECT_DELAY = 1000;
  /** max reconnect delay **/
  private static final int MAX_RECONNECT_DELAY = 15000;
  
  /** canned OK response **/
  private static final RedisResponse okResponse = new RedisResponse(Type.Status,OK);
    
  /** commands queued for dispatch **/
  Deque<RedisCmd> _queuedCmds = Lists.newLinkedList();
  /** un-ack'd commands that have already been sent across the wire  **/
  Deque<RedisCmd> _inflightCmds = Lists.newLinkedList();
  /** we are a fully pipelined client .. i.e. we send commands down the wire 
   *  as soon as we can, accumulating encoded command up to  MAX_QUEUED_WRITES_SIZE
   *  in this write buffer   
   */
  NIOBufferList   _writeBuffer = new NIOBufferList();
  /** a stream that writes into the write buffer (used to encoded commands) **/
  NIOBufferListOutputStream _writeStream = new NIOBufferListOutputStream(_writeBuffer);
  /** we are a fully streaming, non-blocking client. so, 
   * we accumulate incoming bytes into this buffer list before processing them **/
  NIOBufferList _readBuffer = new NIOBufferList();
  /** state machine that interprets the redis-protocol in a streaming manner **/
  RedisResponseBuilder _responseBuilder = new RedisResponseBuilder();
  /** are we connected to the redis server ? **/
  boolean             _isConnected = false;
  /** the event loop (selector thread) we use to drive async io and message
   *  processing
   */
  EventLoop           _eventLoop;
  /** the redis server's address and port **/
  InetSocketAddress   _hostAddress;
  /** the tcp socket object we are using for IO **/
  NIOClientTCPSocket  _tcpSocket;
  /** the current reconnect delay. we step the delay up to MAX_RECONNECT_DELAY
   *  if we encounter connection failures ...  
   */
  int _reconnectDelay = 0;
  /** timer used to reconnect **/
  Timer _reconnectTimer;
  
  /** 
   * Construct a RedisClient 
   *  
   * @param eventLoop the event loop used for IO / event processing  
   * @param hostAddress the address / port of the target redis server
   * @throws IOException
   */
  public RedisClient(EventLoop eventLoop,InetSocketAddress hostAddress)throws IOException { 
    _eventLoop = eventLoop;
    _hostAddress = hostAddress;
    openSocket();
  }
  
  
  /** 
   * Queue a redis command for transmission 
   *  
   * @param cmd the command to send over the wire
   * @param callback
   * @throws IOException
   */
  public void send(RedisCmd cmd,RedisClientCallback callback)throws IOException {
    
    cmd.callback = callback;
    
    // if multi ... 
    if (cmd.isMulti()) {
      // check to see if last command is exec ... 
      RedisCmd lastCmd = cmd.nestedCommands.getLast();
      if (lastCmd.isExec()) { 
        // if so, remove it, as we automatically do an EXEC 
        // for a multi ... 
        cmd.nestedCommands.removeLast();
      }
    }
    
    // enqueue the coammnd 
    _queuedCmds.add(cmd);
    
    if (_isConnected) { 
      // set the appropriate socket event state depending on the status of the 
      // inflight queue
      if (_inflightCmds.size() != 0) { 
        _eventLoop.getSelector().registerForReadAndWrite(_tcpSocket);
      }
      else { 
        _eventLoop.getSelector().registerForWrite(_tcpSocket);
      }
    }
  }
  
  /** 
   * Queue a redis command for transmission 
   *  
   * @param cmd the command to send over the wire 
   * @throws IOException
   */
  public void send(RedisCmd cmd)throws IOException { 
    send(cmd,null);
  }
  
  /** 
   * Convenience method to use builder to send a command 
   * 
   * @param builder the builder holding a command
   * @param callback 
   * @throws IOException
   */
  public void send(RedisCmdBuilder builder,RedisClientCallback callback)throws IOException {
    send(builder.build(),callback);
  }

  /** 
   * Convenience method to use builder to send a command 
   * 
   * @param builder the builder holding a command
   * @throws IOException
   */
  public void send(RedisCmdBuilder builder)throws IOException {
    send(builder.build(),null);
  }
  
  /** 
   * internal helper used to reconnect to redis 
   * 
   * @throws IOException
   */
  void openSocket() throws IOException {
    if (_reconnectTimer != null) { 
      _eventLoop.cancelTimer(_reconnectTimer);
      _reconnectTimer = null;
    }
    
    if (_tcpSocket != null) { 
      _tcpSocket.close();
    }
    _tcpSocket = new NIOClientTCPSocket(new InetSocketAddress(0),this);
    _tcpSocket.connect(_hostAddress);
    _eventLoop.getSelector().registerForConnect(_tcpSocket);
  }

  
  @Override
  public String toString() {
    return _hostAddress.toString();
  }
  
  /** 
   * NIOSocketListener callback 
   */
  @Override
  public void Disconnected(NIOSocket theSocket, Exception optionalException) throws IOException {
    _isConnected = false;
    LOG.info("Disconected");
    cleanup();
    
    LOG.error("Redis Client:" + this + " Disconnected. Setting Reconnect Timer.");

    // either way, increase subsequent reconnect interval
    _reconnectDelay = (_reconnectDelay == 0) ? INITIAL_RECONNECT_DELAY : Math.min(MAX_RECONNECT_DELAY, _reconnectDelay * 2);

    _reconnectTimer = new Timer(_reconnectDelay, false,

    new Timer.Callback() {

      // @Override
      public void timerFired(Timer timer) {
        try {
            LOG.info("Redis Client:" + this +" attempting reconnection");
            openSocket();
        } catch (IOException e) {
          _reconnectDelay = Math.min(MAX_RECONNECT_DELAY, _reconnectDelay * 2);
          LOG.error("Reconnect threw exception:" + e.toString() + " Will retry in:" + _reconnectDelay +" MS");
          _eventLoop.setTimer(_reconnectTimer);
        }
      }
    });
    _eventLoop.setTimer(_reconnectTimer);
  }
  
  /** 
   * internal cleaup method 
   */
  void cleanup() { 
    // cleanup inflight commands ... 
    while (_inflightCmds.size() != 0) {
      RedisCmd inflightCmd = _inflightCmds.removeLast();
      inflightCmd.resetTransmissionState();
      _queuedCmds.addFirst(inflightCmd);
    }
    // reset buffers etc. 
    _writeBuffer.reset();
    _readBuffer.reset();
    // reset response builder state ... 
    _responseBuilder.reset();
  }

  /** 
   * NISocketListener callback
   */
  @Override
  public void Excepted(NIOSocket socket, Exception e) {
    LOG.error("RedisClient:" + this + " Socket Exception with Exception:" + CCStringUtils.stringifyException(e));
    if (_tcpSocket != null) { 
      _tcpSocket.close();
    }
    try {
      Disconnected(socket, e);
    } catch (IOException e1) {
      LOG.error(CCStringUtils.stringifyException(e1));
    }
  }

  /** 
   * NISocketListener callback
   */
  @Override
  public void Connected(NIOClientSocket theSocket) throws IOException {
    _isConnected = true;
    LOG.info("Connected");
    if (_queuedCmds.size() != 0) { 
      _eventLoop.getSelector().registerForWrite(theSocket);
    }
  }

  /** 
   * NISocketListener callback
   */
  @Override
  public int Readable(NIOClientSocket theSocket) throws IOException {
    LOG.info("Readable");
    int lastBytesRead = 0;
    // read data ... 
    do { 
      ByteBuffer readBuffer = _readBuffer.getWriteBuf();
      lastBytesRead = theSocket.read(readBuffer);
    }
    while (lastBytesRead > 0);
    _readBuffer.flush();
    // process data ... 
    while (_readBuffer.available() != 0) {  
      ByteBuffer buffer = _readBuffer.read();
      while (buffer.remaining() != 0) { 
        RedisResponse response = _responseBuilder.processBuffer(buffer);
        if (response != null) {
          processResponse(response);
        }
      }
      
    }
    return lastBytesRead;
  }

  /** 
   * process a parsed redis response ... 
   * 
   * @param response
   * @throws IOException
   */
  void processResponse(RedisResponse response) throws IOException { 
    // get the earliest inflight command 
    RedisCmd earliestCmd = _inflightCmds.getFirst();
    
    // if it is a multi ...  
    if (earliestCmd.isMulti()) {
      // we must handle the multi command in a special way
      // we get an ack for the multi cmd, and a QUEUED response 
      // for each subesequent command that is part of the transaction.
      // we need to track each response so that we can then know when 
      // to appropriately handle the EXEC command 
      
      // if waiting for OK from actual MULTI command 
      if (earliestCmd.waitingForMultiStartACK()) { 
        // we should never get a non-OK response for a multi command. if we get this something 
        // is wrong. bubble up an exception 
        if (!response.isResponseOK()) {           
          IOException e = new IOException("Receieved improper response for multi command! Response:" + response);
          LOG.error("Receieved non OK response for multi! Response:" + response);
          throw e;
        }
        else { 
          // otherwise .. increment counter used to track multi responses  
          earliestCmd.incMultiReadCursor();
        }
      }
      // if we have received all the intermediate responses for the multi cmd
      else if (earliestCmd.waitingForExecResponse()) { 
        // pop this command from the inflight queue ...  
        _inflightCmds.removeFirst();
        // explicitly set the response for the top level command to OK 
        // indicating successful transaction execution ... 
        earliestCmd.response = okResponse;
        
        // walk actual responses from the EXEC 
        if (response.isMulti()) { 
          if (response.values != null) {
            int redisCmdIndex = 0;
            // walk multi response values 
            for (RedisResponse value : response.values) { 
              //TODO: this is legacy code since OLD redis did not handle errors 
              // in multi blocks properly, but later versions of redis now queue errors
              // and actually FAIL the entire transaction at the end (in case of an actual error).
              for (;earliestCmd.nestedCommands.get(redisCmdIndex).response != null;redisCmdIndex++);
              // bind each response to its associated cmd ...  
              earliestCmd.nestedCommands.get(redisCmdIndex++).response = value;
            }
          }
          // ok, notify caller of result if callback was specified in multi cmd... 
          if (earliestCmd.callback != null) { 
            earliestCmd.callback.CmdComplete(this,earliestCmd, response);
          }
        }
        else {
          // this is probably an error  
          earliestCmd.response = response;

          if (earliestCmd.callback != null) {
            earliestCmd.callback.CmdFailed(this,earliestCmd, response);
          }
          //System.out.println(response);
        }
      }
      else {
        // every intermediate response from a multi should be QUEUED
        if (!response.isResponseQUEUED()) {
          earliestCmd.setFailedMultiChildResponse(response);
        }
        earliestCmd.incMultiReadCursor();
      }
    }
    // If not a multi cmd ... 
    else {
      // remove from inflight queue 
      _inflightCmds.removeFirst();
      // set the response object ... 
      earliestCmd.response = response;
      if (earliestCmd.callback != null) {
        if (response.type == Type.Error) { 
          earliestCmd.callback.CmdFailed(this,earliestCmd, response);
        }
        else { 
          earliestCmd.callback.CmdComplete(this,earliestCmd, response);
        }
      }
    }
  }

  /** 
   * NISocketListener callback
   */
  @Override
  public void Writeable(NIOClientSocket theSocket) throws IOException {
    LOG.info("Writable");
    encodeCommands();
    
    if (_writeBuffer.available() == 0)
      // flush partial buffers ...
        _writeStream.flush();
 
    if (_writeBuffer.available() != 0) { 
          
      ByteBuffer bufferToWrite = _writeBuffer.read();
      
      if (bufferToWrite != null) { 
        try {
          int preWritePos = bufferToWrite.position();
          int amountWritten = theSocket.write(bufferToWrite);
          if (amountWritten > 0) { 
            System.out.println("Wrote:" + new String(bufferToWrite.array(),bufferToWrite.arrayOffset() + preWritePos,amountWritten));
          }
          if (bufferToWrite.remaining() != 0) { 
            _writeBuffer.putBack(bufferToWrite);
          }
        }
        catch (IOException e) {
          LOG.error(CCStringUtils.stringifyException(e));
          throw e;
        }
      }
    }
    
    boolean isReadable = (_inflightCmds.size() != 0);
    boolean isWritable = (_writeBuffer.available() != 0 || _writeStream.buffered() != 0);
   
    if (isReadable || isWritable) { 
      if (isWritable) {
        if (isReadable)
          _eventLoop.getSelector().registerForReadAndWrite(theSocket);
        else 
          _eventLoop.getSelector().registerForWrite(theSocket);
      }
      else { 
        _eventLoop.getSelector().registerForRead(theSocket);
      }
    }
  }
  
  /** 
   * encode queued commands 
   */
  final void encodeCommands()throws IOException {
    while (!_queuedCmds.isEmpty() && _writeBuffer.available() < MAX_QUEUED_WRITES_SIZE) {
      
      RedisCmd nextCmd = _queuedCmds.removeFirst();
      // encode onto buffer stream ... 
      nextCmd.encode(_writeStream);
      // add to inflight queue 
      _inflightCmds.addLast(nextCmd);
      // if multi, break out ... 
      if (nextCmd.isMulti()) {
        while (!nextCmd.allChildrenEncoded()) { 
          nextCmd.encodeNextMultiChild(_writeStream);
        }
        nextCmd.commitMulti(_writeStream);
      }
    }
  }
  
  //TODO: temporary code to validate that the basics work
  public static void main(String[] args) throws IOException {
    Logger  logger = Logger.getLogger("org.commoncrawl");
    logger.setLevel(Level.INFO);
    ConsoleAppender console = new ConsoleAppender(); //create appender
    String PATTERN = "%d [%p|%c|%C{1}] %m%n";
    console.setLayout(new PatternLayout(PATTERN)); 
    console.setThreshold(Level.INFO);
    console.activateOptions();
    
    Logger.getRootLogger().addAppender(console);
    
    EventLoop eventLoop = new EventLoop();
    eventLoop.start();
    
    RedisClient client = new RedisClient(eventLoop, new InetSocketAddress(InetAddress.getLocalHost(),6379));
    
    RedisClientCallback callback = new RedisClientCallback() {
      
      @Override
      public void CmdFailed(RedisClient client,RedisCmd cmd, RedisResponse response) {
        System.out.println("Cmd (FAILED):");
        System.out.print(cmd.toString());
        System.out.println("Response:");
        System.out.print(response.toString());
      }
      
      @Override
      public void CmdComplete(RedisClient client,RedisCmd cmd, RedisResponse response) {
        System.out.println("Cmd (COMPLETED):");
        System.out.print(cmd.toString());
        System.out.println("Response:");
        System.out.print(response.toString());
      }
    };
    
    client.send(new RedisCmdBuilder()
      .mutli()
      .cmd(Commands.INCR,"TEST2")
      .cmd(Commands.INCR,"TEST")
      .cmd(Commands.PING)
      .cmd(Commands.KEYS,"*"),callback);
    client.send(new RedisCmdBuilder().cmd(Commands.INCR,"TEST"),callback);
    client.send(new RedisCmdBuilder().cmd(Commands.INCR,"TEST2", "1"),callback);
    client.send(new RedisCmdBuilder().cmd(Commands.SET,"HELLO", "NEW VALUE"),callback);
    client.send(new RedisCmdBuilder().cmd(Commands.GET,"HELLO"),callback);

    
    try {
      eventLoop.getEventThread().join();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  

  
}
