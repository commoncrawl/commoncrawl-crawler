package org.commoncrawl.io;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;
import org.commoncrawl.async.Callback;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.async.Timer;
import org.commoncrawl.io.NIODNSQueryClient.Status;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.IPAddressUtils;
import org.xbill.DNS.ARecord;
import org.xbill.DNS.CNAMERecord;
import org.xbill.DNS.DClass;
import org.xbill.DNS.Header;
import org.xbill.DNS.Message;
import org.xbill.DNS.Name;
import org.xbill.DNS.Rcode;
import org.xbill.DNS.Record;
import org.xbill.DNS.Section;
import org.xbill.DNS.Type;
import org.xbill.DNS.WireParseException;

import com.google.common.collect.Lists;

public class NIODNSAsyncResolver extends NIODNSResolver implements NIOClientSocketListener, Timer.Callback{

  static final Log  LOG = LogFactory.getLog(NIODNSAsyncResolver.class);

  private static final short DEFAULT_UDPSIZE = 512;
  
  ByteBuffer _outgoingPacketHeader = ByteBuffer.allocate(2);
  NIOBufferList _outgoingData = new NIOBufferList();
  NIOBufferList _incomingData = new NIOBufferList();
  NIOBufferListInputStream _bufferListInputStream = new NIOBufferListInputStream(_incomingData);
  DataInputStream _inputStream = new DataInputStream(_bufferListInputStream);
  
  public class ResolverRequest implements Future<NIODNSQueryResult>  { 
    
    String hostName;
    NIODNSQueryClient client;
    NIODNSQueryResult result;
    int timeoutValue;
    long expireTime;
    int qid;
    boolean cancelled = false;
    boolean useCache = true;

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return cancelled = true;
    }
    @Override
    public NIODNSQueryResult get() throws InterruptedException,ExecutionException {
      return result;
    }
    @Override
    public NIODNSQueryResult get(long timeout, TimeUnit unit)throws InterruptedException, ExecutionException, TimeoutException {
      return result;
    }
    @Override
    public boolean isCancelled() {
      return cancelled;
    }
    @Override
    public boolean isDone() {
      return result != null;
    }
    
    public void doQuery() throws IOException {
      try {
        if (useCache) {
            // LOG.info("Checking Cache for Host:" + hostName);
            NIODNSQueryResult result = checkCache(client, hostName);
            if (result != null) { 
              _cacheHits.incrementAndGet();
              if (result.success()) {
                client.AddressResolutionSuccess(result._source, result.getHostName(),
                    result.getCName(), result.getAddress(), result.getTTL());
              } else {
                client.AddressResolutionFailure(result._source, 
                    result.getHostName(), result.getStatus(), result.getErrorDescription());
              }
              client.done(result._source, this);
            }
        }
        
        // construct message payload ... 
        Name name = Name.fromString(hostName, Name.root);
        Record rec = Record.newRecord(name, Type.A, DClass.IN);
        Message query = Message.newQuery(rec);
        
        // set qid 
        qid = query.getHeader().getID();
        // serialize to byte stream ... 
        byte [] out = query.toWire(Message.MAXLENGTH);
        
        // wrap it 
        ByteBuffer outgoingPayload = ByteBuffer.wrap(out);
        
        // reset buffer list 
        _outgoingData.reset();
        // set header ... 
        _outgoingPacketHeader.clear();
        _outgoingPacketHeader.array()[0]  = (byte)(out.length >>> 8);
        _outgoingPacketHeader.array()[1]  = (byte)(out.length & 0xFF);
        _outgoingPacketHeader.position(2);
        _outgoingData.write(_outgoingPacketHeader);
        // add payload 
        outgoingPayload.position(out.length);
        _outgoingData.write(outgoingPayload);
        _outgoingData.flush();
        
        LOG.info("Total Datagram size:" + _outgoingData.available());
        
        // reset read state 
        _readState = ReadState.READING_HEADER;
        _incomingData.reset();
        _incomingPacketLen = -1;
        
        // register for a write event ... 
        _theEventLoop.getSelector().registerForWrite(_tcpSocket);
        
        expireTime = System.currentTimeMillis() + timeoutValue;
      }
      catch (Exception e) {
        if (_logger != null)
          _logger.logDNSException(hostName, StringUtils.stringifyException(e));
        failRequest(this, Status.RESOLVER_FAILURE, "Exception:" + e);
      }
    }
  }
  
  EventLoop _theEventLoop;
  Timer _pollTimer;
  NIOClientTCPSocket _tcpSocket;
  LinkedList<ResolverRequest> _requestQueue = Lists.newLinkedList();
  ResolverRequest _activeRequest;
  boolean _isConnected;
  String _dnsServer;
  enum ReadState { 
    READING_HEADER,
    READING_PAYLOAD,
    READY,
    FINISHED
  }
  
  ReadState _readState;
  int       _incomingPacketLen;
  
  
  
  public NIODNSAsyncResolver(EventLoop theEventLoop,String serverName) throws IOException {
    _theEventLoop = theEventLoop;
    _pollTimer = new Timer(100,true,this);
    _theEventLoop.setTimer(_pollTimer);
    _dnsServer = serverName;
    _dnsCache.enableIPAddressTracking();
  }
   
  void openSocket() throws IOException {
    if (_tcpSocket != null) { 
      _tcpSocket.close();
    }
    _tcpSocket = new NIOClientTCPSocket(new InetSocketAddress(0),this);
    _tcpSocket.connect(new InetSocketAddress(InetAddress.getByName(_dnsServer),53));
    _theEventLoop.getSelector().registerForConnect(_tcpSocket);
  }


  @Override
  public void Disconnected(NIOSocket theSocket, Exception optionalException)
      throws IOException {
    //LOG.info("Disconnected");
    _isConnected = false;
    _tcpSocket.close();
    _tcpSocket = null;
    if (_activeRequest != null) {
      ResolverRequest failedRequest = _activeRequest;
      _activeRequest = null;
      synchronized (_requestQueue) {
        _requestQueue.addFirst(failedRequest);
      }
    }
    queueNextRequest();
  }

  @Override
  public void Excepted(NIOSocket socket, Exception e) {
    LOG.error("Resolver Socket Excepted!",e);
    LOG.error(CCStringUtils.stringifyException(e));
  }

  @Override
  public void Connected(NIOClientSocket theSocket) throws IOException {
    //LOG.info("Connected");
    _isConnected = true;
    if (_activeRequest != null) { 
      _theEventLoop.getSelector().registerForWrite(theSocket);
    }
  }

  
  @Override
  public int Readable(NIOClientSocket theSocket) throws IOException {
    //LOG.info("Readable ExistingReadState:" + _readState);
    int totalBytesRead = 0;
    int lastReadResult = 0;
    do { 
      ByteBuffer readBuffer = _incomingData.getWriteBuf();
      lastReadResult = theSocket.read(readBuffer);
      totalBytesRead += Math.max(0, lastReadResult);
    }
    while (lastReadResult > 0);
    //LOG.info("Total Read This Iteration:" + totalBytesRead + " Last Read Result:" + lastReadResult);
    // flush trailing data ... 
    _incomingData.flush();
    
    // check state ... 
    if (_readState == ReadState.READING_HEADER) { 
      if (_incomingData.available() >= 2) { 
        _incomingPacketLen = _inputStream.readShort();
        _readState = ReadState.READING_PAYLOAD;
        //LOG.info("Transitioned to READING_PAYLOAD. Payload Len:" + _incomingPacketLen);
      }
    }
    if (_readState == ReadState.READING_PAYLOAD) { 
      //LOG.info("Incoming Data Available:" + _inputStream.available());
      if (_inputStream.available() >= _incomingPacketLen) {
        //LOG.info("Transitioned to DONE");
        _readState = ReadState.READY;
      }
    }

    if (_readState == ReadState.READY) {
      
      _readState = ReadState.FINISHED;
      
      byte[] payload = new byte[_incomingPacketLen];
      _inputStream.readFully(payload);
      processDNSResponsePacket(payload);
    }
    else if (_readState != ReadState.READY || _readState != ReadState.FINISHED && lastReadResult != -1) { 
      // read more data 
      _theEventLoop.getSelector().registerForRead(_tcpSocket);
    }
    
    return lastReadResult;
  }

  @Override
  public void Writeable(NIOClientSocket theSocket) throws IOException {
    int originalBytesAvailable = _outgoingData.available();
    //LOG.info("Writable - bytesAvailable:" + originalBytesAvailable);
    boolean moreDataToWrite = false;
    while (_outgoingData.available() != 0) { 
      ByteBuffer nextBuffer = _outgoingData.read();
      if (nextBuffer == null) { 
        LOG.fatal("read returned null when available said:" + _outgoingData.available() + " BufferDetails:" + _outgoingData);
        // TEMP HACK TILL WE FIND THE ROOT CAUSE OF THIS :-(
        break;
      }
      //LOG.info("Next Buffer Remaining:" + nextBuffer.remaining());
      theSocket.write(nextBuffer);
      //LOG.info("After Write - Next Buffer Remaining:" + nextBuffer.remaining());
      if (nextBuffer.remaining() !=0) { 
        _outgoingData.putBack(nextBuffer);
        moreDataToWrite = true;
      }
    }
    //LOG.info("Wrote:" + (originalBytesAvailable-_outgoingData.available() + " Bytes Out"));
    if (!moreDataToWrite) { 
      //LOG.info("Transitioning from Write mode to Read");
      _theEventLoop.getSelector().registerForRead(_tcpSocket);
    }
    else { 
      _theEventLoop.getSelector().registerForWrite(_tcpSocket);
    }
  }

  void queueNextRequest() { 
    if (_activeRequest != null) { 
      if (System.currentTimeMillis() >= _activeRequest.expireTime) {
        //LOG.info("Timing Out Request:" + _activeRequest);
        ResolverRequest failedRequest = _activeRequest;
        _activeRequest = null;
        failRequest(failedRequest,Status.RESOLVER_FAILURE, "Timeout");
        _tcpSocket.close();
        _tcpSocket = null;
      }
    }
    
    int requestQueueSize = 0;
    synchronized (_requestQueue) {
      requestQueueSize = _requestQueue.size();
    }
    if (_tcpSocket == null && requestQueueSize != 0) { 
      try {
        //LOG.info("Reconnecting to DNS Server");
        openSocket();
      }
      catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
      }
    }
    
    if (_tcpSocket != null && _tcpSocket.isOpen() && _isConnected && _activeRequest == null) {
      synchronized (_requestQueue) {
        while (_requestQueue.size() != 0 && _activeRequest == null) { 
          _activeRequest = _requestQueue.removeFirst();
          if (_activeRequest.isCancelled())
            _activeRequest = null;
        }
      }
      try {
        if (_activeRequest != null) {
          //LOG.info("Calling doQuery on new request");

          _activeRequest.doQuery();
        }
      }
      catch (IOException e) { 
        LOG.error(CCStringUtils.stringifyException(e));
        ResolverRequest failedRequest = _activeRequest;
        _activeRequest = null;
        failRequest(failedRequest,Status.RESOLVER_FAILURE, "IOException:" + e.toString());
      }
    }
  }
  
  @Override
  public Future<NIODNSQueryResult> resolve(NIODNSQueryClient client,String theHost, boolean noCache, boolean highPriorityRequest,int timeoutValue) throws IOException {
    ResolverRequest request = new ResolverRequest();
    request.client = client;
    request.hostName = theHost;
    request.timeoutValue = timeoutValue;
    request.useCache = !noCache;

    synchronized (_requestQueue) {
      //LOG.info("Adding Request to Queue");
      
      if (highPriorityRequest)
        _requestQueue.addFirst(request);
      else 
        _requestQueue.add(request);
      
      if (_activeRequest == null && _isConnected) { 
        if (Thread.currentThread() == _theEventLoop.getEventThread()) { 
          queueNextRequest();
        }
        else { 
          _theEventLoop.queueAsyncCallback(new Callback() {
            
            @Override
            public void execute() {
              queueNextRequest();
            }
          });
        }
      }
    }
    return request;
  }

  @Override
  public void timerFired(Timer timer) {
    queueNextRequest();
  }
  
  void processDNSResponsePacket(byte responsePacket[]){
    if (_activeRequest != null) {
      //LOG.info("Parsing DNS Response");
      try { 
        /*
         * Check that the response is long enough.
         */
        if (responsePacket.length < Header.LENGTH) {
          throw new WireParseException("invalid DNS header - " +
                     "too short");
        }
        /*
         * Check that the response ID matches the query ID.  We want
         * to check this before actually parsing the message, so that
         * if there's a malformed response that's not ours, it
         * doesn't confuse us.
         */
        int id = ((responsePacket[0] & 0xFF) << 8) + (responsePacket[1] & 0xFF);
        int qid = _activeRequest.qid;
        if (id != qid) {
          String error = "invalid message id: expected " + qid + "; got id " + id;
          throw new WireParseException(error);
        }
        Message response = parseMessage(responsePacket);
        if (response != null) { 
          processDNSResponseMessage(response);
        }
        else { 
          ResolverRequest completedRequest = _activeRequest;
          _activeRequest = null;
          failRequest(completedRequest, Status.RESOLVER_FAILURE, "NULL Response");        
        }

      }
      catch (Exception e) { 
        LOG.error("Caught Exception Parsing Response for Query:" + _activeRequest,e);
        LOG.error(CCStringUtils.stringifyException(e));
        ResolverRequest completedRequest = _activeRequest;
        _activeRequest = null;
        failRequest(completedRequest, Status.SERVER_FAILURE, e.toString());        
      }
      finally { 
        queueNextRequest();        
      }
    }
  }
  
  public static final int MIN_TTL_VALUE = 60 * 20 * 1000;
  
  void processDNSResponseMessage(Message response)throws IOException { 
    InetAddress address = null;
    String cname = null;
    long expireTime = -1;

    //LOG.info("Processing DNS Message with Response Code:" + response.getRcode());
    if (response.getRcode() == Rcode.NOERROR) {
      // get answer
      Record records[] = response.getSectionArray(Section.ANSWER);

      if (records != null) {

        // walk records ...
        for (Record record : records) {

          // store CName for later use ...
          if (record.getType() == Type.CNAME) {
            cname = ((CNAMERecord) record).getAlias().toString();
            if (cname != null && cname.endsWith(".")) {
              cname = cname.substring(0, cname.length() - 1);
            }
          }
          // otherwise look for A record
          else if (record.getType() == Type.A && address == null) {
            address = ((ARecord) record).getAddress();
            expireTime = Math.max(
                (System.currentTimeMillis() + (((ARecord) record)
                    .getTTL() * 1000)), System.currentTimeMillis()
                    + MIN_TTL_VALUE);
          }
        }
      }

      if (address != null) {
        // LOG.info("Caching DNS Entry for Host:" + hostName);
        // update dns cache ...
        
          _dnsCache.cacheIPAddressForHost(_activeRequest.hostName, IPAddressUtils
              .IPV4AddressToInteger(address.getAddress()), expireTime,
              cname);
             
      }
    }
    
    // create result object ...
    NIODNSQueryResult result = new NIODNSQueryResult(this, _activeRequest.client, _activeRequest.hostName);

    if (response.getRcode() != Rcode.NOERROR) {
      result.setStatus(Status.SERVER_FAILURE);
      result.setErrorDesc(Rcode.string(response.getRcode()));
      
      if (_logger != null)
        _logger
            .logDNSFailure(_activeRequest.hostName, Rcode.string(response.getRcode()));
      
    } else if (response.getRcode() == Rcode.NOERROR) {

      if (address != null) {

        result.setStatus(Status.SUCCESS);
        result.setAddress(address);
        result.setCName(cname);
        result.setTTL(expireTime);

        if (_logger != null) {
          _logger.logDNSQuery(_activeRequest.hostName, address, expireTime, cname);
        }

      } else {
        result.setStatus(Status.SERVER_FAILURE);
        result.setErrorDesc("UNKNOWN-NO A RECORD");
        if (_logger != null)
          _logger.logDNSFailure(_activeRequest.hostName, "NOERROR");
      }
    }
    
    ResolverRequest completedRequest = _activeRequest;
    _activeRequest = null;
    completedRequest.result = result;
    if (result.getStatus() == Status.SUCCESS) { 
      if (!completedRequest.isCancelled()) { 
        completedRequest.client.AddressResolutionSuccess(this,completedRequest.hostName,result.getCName(),result.getAddress(),result.getTTL());
        completedRequest.client.done(this, completedRequest);
      }
    }
    else {
      // if result is server failure ... and not via bad host cache ...
      if (result.getStatus() == Status.SERVER_FAILURE) {
        if (result.getErrorDescription().equals("NXDOMAIN") || result.getErrorDescription().equals("NOERROR")) {
          _badHostCache.cacheIPAddressForHost(completedRequest.hostName, 0, System.currentTimeMillis()+ NXDOMAIN_FAIL_BAD_HOST_LIFETIME, null);
        }
      }
      failRequest(completedRequest,result.getStatus(),result.getErrorDescription());
    }
  }
  
  private Message parseMessage(byte [] b) throws WireParseException {
    try {
      return (new Message(b));
    }
    catch (IOException e) {
      LOG.error(CCStringUtils.stringifyException(e));
      if (!(e instanceof WireParseException))
        e = new WireParseException("Error parsing message");
      throw (WireParseException) e;
    }
  }

  
  public static void main(final String[] args)throws IOException {
    final EventLoop theEventLoop = new EventLoop();
    theEventLoop.start();
    
    File file = new File(args[0]);
    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
    
    
    try {
      NIODNSAsyncResolver resolver = new NIODNSAsyncResolver(theEventLoop, "127.0.0.1");
      
      final Semaphore dnsRequestCompleteSem = new Semaphore(0);
      String line = null;
      
      while ((line = reader.readLine()) != null) {
        final String hostName = line;
        
        resolver.resolve(new NIODNSQueryClient() {
  
          @Override
          public void AddressResolutionFailure(NIODNSResolver source,String hostName, Status status, String errorDesc) {
            System.out.println("Address Resolution Failed with Status:" + status + " ErrorDesc:" + errorDesc);
            dnsRequestCompleteSem.release();
          }
  
          @Override
          public void AddressResolutionSuccess(NIODNSResolver source,String hostName, String cName, InetAddress address, long addressTTL) {
            System.out.println("Address Resolution Succeeded with CName:" + cName + " IpAddress:" + address.toString());
            dnsRequestCompleteSem.release();
          }
  
          @Override
          public void DNSResultsAvailable() {
            // TODO Auto-generated method stub
            
          }
  
          @Override
          public void done(NIODNSResolver source,
              Future<NIODNSQueryResult> task) {            
          } 
          
        }
        , hostName, true, true, 10000);
        
        dnsRequestCompleteSem.acquireUninterruptibly();
      }
      
    } catch (IOException e) {
      e.printStackTrace();
    }
    
    System.out.println("Bye");
    System.exit(0);
  }
  
  void failRequest(ResolverRequest request,Status status, String errorDesc) {
    if (!request.isCancelled()) { 
      request.client.AddressResolutionFailure(this, request.hostName, status, errorDesc);
      request.client.done(this,request);
    }
  }
}
