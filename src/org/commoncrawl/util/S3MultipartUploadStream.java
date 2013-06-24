package org.commoncrawl.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.io.IOUtils;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.async.Timer;
import org.commoncrawl.io.NIOBufferList;
import org.commoncrawl.io.NIOHttpConnection;
import org.commoncrawl.io.NIOHttpHeaders;
import org.commoncrawl.io.NIOHttpConnection.State;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.internal.ServiceUtils;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.util.BinaryUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;


public class S3MultipartUploadStream extends OutputStream implements NIOHttpConnection.Listener {

  /**
   * request URI
   */
  URI           _uri;

  /** 
   * request bucket / key 
   */
  String _bucket;
  String _key;
  
  
  /** 
   * AWS Credentials
   */
  String        _s3AccessKey;
  String        _s3Secret;
  /** 
   * S3 Client object 
   */
  AmazonS3Client _s3Client;
  /** 
   * Init Reponse object 
   */
  InitiateMultipartUploadResult _initResponse = null;
  
  /** 
   * constraints ... 
   */
  public static final int DEFAULT_PART_BUFFER_SIZE = 5 * 1024 * 1024;
  public static final int DEFAULT_UPLOADER_COUNT = 10;
  public static final int DEFAULT_MAX_QUEUED_COUNT = 20;
  public static final int DEFAULT_MAX_RETRIES_PER_PART = 3;
  
  int _maxUploaders = DEFAULT_UPLOADER_COUNT;
  int _maxQueuedParts = DEFAULT_MAX_QUEUED_COUNT;
  int _maxRetries = DEFAULT_MAX_RETRIES_PER_PART;
  int _flushThreshold = DEFAULT_PART_BUFFER_SIZE;
  
  /** 
   * async uploader slots 
   */
  UploadItem _uploadSlots[];
  
  /** 
   * metrics ... 
   */
  int _queuedPartCount = 0;
  int _inFlightCount = 0;
  
  /** 
   * cancel support 
   */
  AtomicBoolean _cancelled = new AtomicBoolean();
  
  /** 
   * failures
   */
  AtomicReference<IOException> _failureException = new AtomicReference<IOException>();
  boolean _failureExceptionThrown = false;
  
  /** default polling interval **/
  private static final int DEFAULT_POLL_INTERVAL = 500;

  
  /** logging **/
  private static final Log LOG = LogFactory.getLog(S3MultipartUploadStream.class);
  
  /** 
   * async support stuff 
   */
  EventLoop _theEventLoop;
  ExecutorService  _dnsThreadPool = null;
  Timer _timer;

  /** 
   * advanced wrap buffer option ... 
   */
  boolean _wrapBuffers=false;
  
  /** 
   * part offset tracking ... 
   */
  long _nextPartOffset = 0;
  
  /** 
   * the active upload item 
   */
  UploadItem _activeItem;
  
  /** 
   * closed flag 
   */
  boolean _closed = false;

  /** 
   * S3 calling format 
   */
  static S3Utils.CallingFormat _callingFormat = S3Utils.CallingFormat.getSubdomainCallingFormat();

  
  
  /** 
   * Detailed stream constructor
   * 
   * @param uri
   * @param s3AccessKey
   * @param s3Secret
   * @param deleteExisting
   * @param optionalACL
   * @param maxUploaders
   * @param partBufferSize
   * @param maxQueuedParts
   * @param maxRetries
   * @throws IOException
   */
  public S3MultipartUploadStream(URI uri,String s3AccessKey,String s3Secret, boolean deleteExisting, CannedAccessControlList optionalACL,int maxUploaders,int partBufferSize,int maxQueuedParts,int maxRetries)throws IOException {
    try { 
      _dnsThreadPool = Executors.newFixedThreadPool(1);
      _theEventLoop = new EventLoop(_dnsThreadPool);
      
      _theEventLoop.start();
      _maxUploaders = maxUploaders;
      _maxQueuedParts = maxQueuedParts;
      _maxRetries = maxRetries;
      _flushThreshold = partBufferSize;
      _uploadSlots = new UploadItem[maxUploaders];
      _s3AccessKey = s3AccessKey;
      _s3Secret = s3Secret;
      _s3Client = new AmazonS3Client(new BasicAWSCredentials(s3AccessKey,s3Secret));
      
      _bucket= uri.getHost();
      _key    = uri.getPath().substring(1);
      
      // if not delete existing ...
      boolean exists = false;
      if (!deleteExisting) {
        try { 
          if (_s3Client.getObjectMetadata(_bucket, _key) != null) { 
            exists = true;
          }
        }
        catch (AmazonServiceException e) { 
          if (e.getStatusCode() != 404) { 
            throw e;
          }
        }
      }
  
      if (exists && !deleteExisting) { 
        throw new FileAlreadyExistsException();
      }
      
      // Step 1: Initialize.
      InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(_bucket, _key);
      
      if (optionalACL != null) { 
        initRequest.setCannedACL(optionalACL);
      }
       
      try { 
        _initResponse = _s3Client.initiateMultipartUpload(initRequest);
        LOG.info("Upload Request for Bucket:"+ _bucket + " Key:" + _key + " has id:" + _initResponse.getUploadId());
        startUpload();
      }
      catch (AmazonClientException e) { 
        LOG.error("Initiate Upload for Key:" + _key + " in Bucket:" + _bucket + " failed with Exception:" + e.toString());
        LOG.error(CCStringUtils.stringifyException(e));
        throw e;
      }
    }
    catch (Exception e) { 
      abort();
      if (e instanceof IOException) 
        throw (IOException)e;
      else 
        throw new IOException(e);
    }
    
  }

  /** 
   * start the uploaders (once init has succeeded)
   */
  private void startUpload()throws IOException {
    _activeItem = new UploadItem();
    _timer = new Timer(DEFAULT_POLL_INTERVAL,true,new Timer.Callback() {

      public void timerFired(Timer timer) {
        if (!uploadFailed()) {
          checkForTimeouts();
          fillSlots();
        }
      }
    });
    _theEventLoop.setTimer(_timer);
  }

  /** 
   * fail the entire upload due to a particular failed part upload request
   * 
   * @param failedItem
   */
  private void failUpload(UploadItem failedItem) {
    LOG.error("Upload For Bucket:"+ _bucket + " Key:" + _key + " failed due to Item:" + failedItem);
     
    _failureException.set(new IOException("Upload Failed. Item that caused failure:" + failedItem + " AttemptCount:" + failedItem._attemptCount));

    // kill event loop first ... 
    if (_timer != null) { 
      _theEventLoop.cancelTimer(_timer);
      _timer = null;
    }
    // next, close open connections... 
    synchronized (_uploadSlots) {
      for (int i=0;i<_uploadSlots.length;++i) { 
        // if empty slot found ... 
        if (_uploadSlots[i] != null) {
          _uploadSlots[i].shutdownUpload();
          _uploadSlots[i] = null;
        }
      }
    }
    
    // hmm... is this necessary ? 
    synchronized (_uploadQueue) {
      _uploadQueue.clear();
      _completedItems.clear();
      _uploadQueue.notifyAll();
    }
  }
  
  /** 
   * has the upload failed 
   * 
   * @return
   */
  private boolean uploadFailed() { 
    return _failureException.get() != null || _cancelled.get();
  }
  
  private void checkForTimeouts() { 
    synchronized (_uploadSlots) {
      for (int i=0;i<_uploadSlots.length;++i) { 
        if (_uploadSlots[i] != null) {
          if (_uploadSlots[i]._connection != null) { 
            if (_uploadSlots[i]._connection.checkForTimeout() == true) { 
              LOG.error("Request:" + _uploadSlots[i] + " timed out");
              UploadItem item = _uploadSlots[i];
              _uploadSlots[i] = null;
              _inFlightCount--;
              item.shutdownUpload();
              item._attemptCount++;
              if (item._attemptCount > _maxRetries) { 
                failUpload(item);
              }
              else { 
                synchronized (_uploadQueue) {
                  LOG.info("Requeuing Timedout Item:" + item);
                  _uploadQueue.add(item);
                }
              }
            }
          }
        }
      }
    }
  }
  
  /**
   * fill any open upload slots ...
   */
  private void fillSlots() {
    synchronized (_uploadSlots) { 
      if (!uploadFailed()) {
        outerLoop:
        for (int i=0;i<_uploadSlots.length;++i) { 
          // if empty slot found ... 
          if (_uploadSlots[i] == null) { 
            UploadItem uploadItem = null;
            synchronized (_uploadQueue) {
              if (_uploadQueue.size() != 0) { 
                uploadItem = _uploadQueue.removeFirst();
                uploadItem._attemptCount++;
                if (uploadItem._attemptCount > _maxRetries) { 
                  failUpload(uploadItem);
                  break outerLoop;
                }
                else { 
                  _inFlightCount++;
                }
              }
            }
                    
            if (uploadItem != null) {
              LOG.info("Queuing: " + uploadItem + " for Upload");
              _uploadSlots[i] = uploadItem;
              _uploadSlots[i].setSlot(i);
              try { 
                _uploadSlots[i].startUpload();
              }
              catch (IOException e) { 
                LOG.error ("Upload for : " + uploadItem + " FAILED with Exception:" + CCStringUtils.stringifyException(e));
                synchronized (_uploadQueue) {
                  _inFlightCount--;
                  _uploadQueue.add(uploadItem);
                }
              }
            }
          }
        }
      }
    }
  }
  

  /** 
   * Outputstream overload 
   */
  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    try { 
      while (len != 0 && !uploadFailed()) {
        int bytesWritten = _activeItem.write(b, off, len);
        len -= bytesWritten;
        
        if (!uploadFailed() && len != 0 ) {
          _activeItem.flush();
        }
      }
      if (!uploadFailed() && _activeItem._size >= _flushThreshold) {
        _activeItem.flush();
      }
    }
    catch (Exception e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      if (_failureException.get() == null) { 
        if (e instanceof IOException) { 
          _failureException.set((IOException)e);
        }
        else { 
          _failureException.set(new IOException(e));
        }
      }
    }
    if (uploadFailed()) {
      if (!_failureExceptionThrown){
        _failureExceptionThrown = true;
        throw _failureException.get();
      }
    }
  }
  
  /**
   * UploadItem - encapsulates a single part of a multi-part upload 
   * 
   * @author rana
   *
   */
  class UploadItem implements NIOHttpConnection.DataSource{
    
    /** 
     * request details ... 
     */
    public UploadPartRequest _requestObject;
    /**
     * the incoming data buffer list 
     */
    public NIOBufferList _bufferList = new NIOBufferList();
    /** 
     * md5 digest calculator 
     */
    public MessageDigest _digest = null;
    /** 
     * the size of this upload request ...
     */
    public int _size;
    /** 
     * the list of ByteBuffers associated with this part ...  
     */
    ArrayList<ByteBuffer> _buffers = Lists.newArrayList();
    /** 
     * cursor position in active buffer being currently serviced ... 
     */
    int _bufferPos=0;
    /** 
     * slot occupied by this upload request (if active) 
     */
    int _slot;
    /** 
     * the async http connection object (if this request is object )
     */
    NIOHttpConnection _connection = null;
    /** 
     * unique id of the request object ... 
     */
    int _id;
    /** 
     * response etag if request was successful .
     */
    String _responseETag;
    
    /** 
     * track the number of attempts / retries
     */
    int _attemptCount = 0;

    
    /** 
     * get the slot this upload item is assigned to 
     * @return
     */
    public int getSlot() { return _slot; }
    /** 
     * set the slot assigned to this request ...
     * @param index
     */
    public void setSlot(int index) { _slot = index; }
    
    @Override
    public String toString() {
      return "Part:" + _requestObject.getPartNumber() + " Offset:" + _requestObject.getFileOffset() + " Size:" + _requestObject.getPartSize();
    };
    
    /** 
     * UploadItem constructor ...
     *  
     * @throws IOException
     */
    public UploadItem()throws IOException { 
      _requestObject = new UploadPartRequest()
      .withBucketName(_bucket).withKey(_key)
      .withUploadId(_initResponse.getUploadId()).withPartNumber(++_partCount)
      .withFileOffset(_nextPartOffset);
      
      try {
        _digest = MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException e) {
        throw new IOException(e);
      }
      _id++;
    }
    
    /** 
     * the OutputStream object delegates its write call to the active UploadItem object
     * 
     * @param b
     * @param off
     * @param len
     * @return
     * @throws IOException
     */
    public int write(byte[] b, int off, int len)throws IOException { 
      int bytesWritten = 0;
      if (!_wrapBuffers) {
        while (len != 0 ) { 
          ByteBuffer writeBuffer = _bufferList.getWriteBuf();
          int bytesToWrite = Math.min(len, writeBuffer.remaining());
          _digest.update(b, off, bytesToWrite);
          writeBuffer.put(b, off,bytesToWrite);
          _size += bytesToWrite;
          bytesWritten += bytesToWrite;
          if (writeBuffer.remaining() == 0) {
            _bufferList.flush();
            if (_size >= _flushThreshold) {
              break;
            }
          }
          len -= bytesToWrite;
          off += bytesToWrite;
        }
      }
      else {
        int bytesToWrite = Math.min(len, _flushThreshold);
        bytesWritten = bytesToWrite;
        _digest.update(b,off,bytesToWrite);
        _bufferList.write(ByteBuffer.wrap(b,off,bytesToWrite));        
        _bufferList.flush();
        _size += bytesToWrite;
      }
      return bytesWritten;
    }
    
    /** 
     * flush the active upload part 
     * 
     * @throws IOException
     */
    void flush()throws IOException {
      _bufferList.flush();
      _queuedPartCount++;
      _requestObject.setPartSize(_size);
      _requestObject.setMd5Digest(BinaryUtils.toBase64(_digest.digest()));
      _nextPartOffset += _size;
            
      ByteBuffer buffer = null;
      long totalSize = 0;
      while ((buffer = _bufferList.read()) != null) {
        totalSize += buffer.limit();
        _buffers.add(buffer);
      }
      if (totalSize != _requestObject.getPartSize()) { 
        throw new IOException("Part Size:" + totalSize+ " and Queued Size:" + _requestObject.getPartSize() +" don't match!");
      }
      _bufferPos = 0;
      _bufferList = null;
      
      boolean queued = false;
      while (!queued) { 
        synchronized (_uploadQueue) {
          if (_uploadQueue.size() + _inFlightCount >= _maxQueuedParts) { 
            try {
              LOG.info("Blocking On Upload Queue");
              _uploadQueue.wait();
              LOG.info("Awoke from Block on Upload Queue");
            } catch (InterruptedException e) {
            }
          }
          _uploadQueue.add(this);
          queued = true;
        }
      }
      _activeItem = new UploadItem();
      
      fillSlots();
    }
    
    /** 
     * rest buffer cursor (for a retry)...
     */
    void resetBufferCursor() { 
      // reset all buffer cursors 
      for (ByteBuffer buffer : _buffers) { 
        buffer.position(0);
      }
      // reset buffer item cursor 
      _bufferPos = 0;
    }
    
    /** 
     * get the next bytebuffer in case of an active upload
     * 
     * @return
     */
    ByteBuffer getNextBuffer() { 
      ByteBuffer bufferOut = null;
      if (_bufferPos < _buffers.size()) { 
        bufferOut = _buffers.get(_bufferPos++);
      }
      return bufferOut;
    }
    
    static final String DEFAULT_CONTENT_TYPE = "binary/octet-stream";
    
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void addToAmazonHeader(String key,String value,Map amazonHeaders) { 
      List<String> list  = (List<String>) amazonHeaders.get(key);
      if (list == null) { 
        list = new Vector<String>();
        amazonHeaders.put(key, list);
      }
      list.add(value);
    }
    
    /**
     * Generate an rfc822 date for use in the Date HTTP header.
     */
    private String httpDate() {
        final String DateFormat = "EEE, dd MMM yyyy HH:mm:ss ";
        SimpleDateFormat format = new SimpleDateFormat( DateFormat, Locale.US );
        format.setTimeZone( TimeZone.getTimeZone( "GMT" ) );
        return format.format( new Date() ) + "GMT";
    }
    
    /**
     * start upload the part representing by this item 
     * @throws IOException
     */
    void startUpload() throws IOException {
      
      // reset cursor 
      resetBufferCursor();

      HashMap<String, String> params = Maps.newHashMap();
      params.put("uploadId", _requestObject.getUploadId());
      params.put("partNumber", Integer.toString(_requestObject.getPartNumber()));
      
      // construct the s3 url ...
      URL theURL = _callingFormat.getURL(false, S3Utils.DEFAULT_HOST, S3Utils.INSECURE_PORT, _bucket, _key, params);
      // allocate an http connection 
      _connection = new NIOHttpConnection(theURL,_theEventLoop.getSelector(),_theEventLoop.getResolver(),null);
      _connection.setId(_id);
      // set the back pointer to us ... 
      _connection.setContext(this);
      // set rate limit policy ... 
      //_connection.setUploadRateLimiter(_rateLimiter);
      // specify that we will populate our own request headers ... 
      _connection.setPopulateDefaultHeaderItems(false);
      // set up the data source ... 
      _connection.setDataSource(this);
      // get at headers ... 
      NIOHttpHeaders headers = _connection.getRequestHeaders();

      // populate http request string 
      headers.prepend("PUT" + " " + theURL.getPath() +"?" + theURL.getQuery() + " HTTP/1.1", null);
      
      if (theURL.getPort() != -1 && theURL.getPort() != 80) { 
        headers.set("Host",theURL.getHost() +":"+String.valueOf(theURL.getPort()));
      }
      else { 
        headers.set("Host",theURL.getHost());
      }
      // create a tree map in parallel (to pass to canonicalization routine for s3 auth)
      Map<String,List<String>> amazonHeaders = Maps.newTreeMap();
      
      // add content length ... 
      headers.set("Content-Length", ((Long)_requestObject.getPartSize()).toString());

      // set mime type header entry ... 
      headers.set("Content-Type",DEFAULT_CONTENT_TYPE);
      // and add content type to amazon headers as well ..
      addToAmazonHeader("Content-Type", DEFAULT_CONTENT_TYPE,amazonHeaders);

      // add date ... 
      String theDate = httpDate();
      headers.set("Date", theDate);
      addToAmazonHeader("Date", theDate, amazonHeaders);

      headers.set("Content-MD5", _requestObject.getMd5Digest());
      addToAmazonHeader("Content-MD5", _requestObject.getMd5Digest(), amazonHeaders);
      
      String canonicalString =  S3Utils.makeCanonicalString("PUT", _bucket, _key,params,amazonHeaders );
      
      //LOG.info("Headers for Request:" + headers.toString());
      //LOG.info("Cannonica for Request:" + canonicalString);
      
      String encodedCanonical = S3Utils.encode(_s3Secret, canonicalString, false);
      
      // add auth string to headers ... 
      headers.set("Authorization","AWS " + _s3AccessKey + ":" + encodedCanonical);
      
      // add cache control pragmas ... 
      headers.set ("Connection", "close");
      headers.set("Cache-Control", "no-cache");
      headers.set("Pragma", "no-cache");
      
      // ready to roll ... 
      
      // set the listener ... 
      _connection.setListener(S3MultipartUploadStream.this);
      // and open the connection 
      _connection.open();
    }
    
    /** 
     * shutdown the upload of this item 
     */
    void shutdownUpload() { 
      if (_connection != null) { 
        _connection.close();
        _connection.setContext(null);
        _connection.setListener(null);
        _connection = null;
      }
    }
    
    /** 
     * read some data from the list of buffers associated with this item 
     */
    @Override
    public boolean read(NIOHttpConnection connection,NIOBufferList dataBuffer) throws IOException {
      ByteBuffer nextBuffer = getNextBuffer();
      if (nextBuffer != null) { 
        nextBuffer.position(nextBuffer.limit());
        dataBuffer.write(nextBuffer);
        dataBuffer.flush();
      }
      // return true if EOF
      return (nextBuffer == null);
    }

    @Override
    public void finsihedWriting(NIOHttpConnection connection,ByteBuffer thisBuffer) throws IOException {
      
    }
  }

  
  int _partCount =0;
  
  LinkedList<UploadItem> _uploadQueue = new LinkedList<UploadItem>();
  TreeMap<Integer,UploadItem> _completedItems = Maps.newTreeMap(); 
  
  
  static byte[] _singleByteBuf = new byte[1];
  @Override
  public void write(int b) throws IOException {
    byte [] buffer;
    if (_wrapBuffers) { 
      buffer = new byte[1];
    }
    else { 
      buffer = _singleByteBuf;;
    }
    buffer[0] = (byte) b;
    write(buffer,0,1);
  }
  
  /** 
   * cancel / abort the upload 
   * 
   * @throws IOException
   */
  public void abort() throws IOException { 
    internalClose(true);
  }
  
  
  @Override
  public void close() throws IOException {
    internalClose(false);
  }
  
  private void internalClose(boolean isAbort) throws IOException {  
    if (!_closed) {
      try { 
        if (!isAbort && !uploadFailed()) { 
          if (_activeItem != null && _activeItem._size != 0) { 
            _activeItem.flush();
          }
        }
        
        if (!isAbort) { 
          LOG.info("close called for Request Bucket:" + _bucket + " Key:" + _key);
          boolean doneUploading = false;
          while (!doneUploading && !uploadFailed()) { 
            synchronized (_uploadQueue) {
              doneUploading = _completedItems.size() == _queuedPartCount;
              if (!doneUploading) { 
                LOG.info("Waiting for Upload Completion for Request Bucket:" + _bucket + " Key:" + _key + " ... There are:" + _queuedPartCount + " queued items and " + _completedItems.size() + " completed items");
                try {
                  _uploadQueue.wait();
                } catch (InterruptedException e) {
                }
              }
            }
          }
        }
        
        if (!uploadFailed() && !isAbort) { 
          LOG.info("Uploads Complete for Request Bucket:" + _bucket + " Key:" + _key);
          ArrayList<PartETag> etags = Lists.newArrayList();
          
          for (Map.Entry<Integer,UploadItem> uploadEntry : _completedItems.entrySet()) { 
            etags.add(new PartETag(uploadEntry.getKey(), uploadEntry.getValue()._responseETag));
          }
          
          // Step 3: complete.
          CompleteMultipartUploadRequest compRequest = new 
                      CompleteMultipartUploadRequest(_bucket, 
                                                     _key, 
                                                     _initResponse.getUploadId(), 
                                                     etags);
      
          try {
            LOG.info("Sending Complete for Request Bucket:" + _bucket + " Key:" + _key);
            _s3Client.completeMultipartUpload(compRequest);
            LOG.info("Complete for Request Succeeded for Bucket:" + _bucket + " Key:" + _key);
          }
          catch (Exception e) { 
            LOG.error("Complete for Request Failed for Bucket:" + _bucket + " Key:" + _key);
            LOG.error(CCStringUtils.stringifyException(e));
    
            _failureException.set(new IOException(e));
          }
        }
        if (_theEventLoop != null) {
          LOG.info("Shutting Down Event Loop");
          _theEventLoop.stop();
          _dnsThreadPool.shutdown();
        }
      }
      finally { 
        _closed = true;
      }
    }
    
    if (isAbort || _failureException.get() != null) { 
      try { 
        if (_initResponse != null) { 
          LOG.info("Upload Failed. Aborting Multipart Upload");
          _s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(_bucket, _key, _initResponse.getUploadId()));
        }
      }
      catch ( Exception e2) { 
        LOG.error(CCStringUtils.stringifyException(e2));
      }
    }
    // throw failure exception if first occurunce ... 
    if (!isAbort && !_failureExceptionThrown && _failureException.get() != null) {
      _failureExceptionThrown = true;
      throw _failureException.get();
    }
  }
  
  @Override
  public void HttpConnectionStateChanged(NIOHttpConnection theConnection,State oldState, State state) {
    
    if (!uploadFailed()) { 
      // extract the reference to the uploader based on 
      UploadItem item = (UploadItem) theConnection.getContext();
      
      //LOG.info("HttpConnection for: " + item + " transitioned:" + oldState + " to " + state );
      
      // get the associated slot and context ...
      int slotIndex = item.getSlot();
      
      if (state == State.ERROR || state == State.DONE) { 
        
        try { 
          boolean failed = true;
          String errorString = "";
          
          if (state == State.DONE) { 
            
            //LOG.info("Resonse Headers:" + theConnection.getResponseHeaders());
            int resultCode = NIOHttpConnection.getHttpResponseCode(theConnection.getResponseHeaders());
            
            if (resultCode == 200) {
              failed = false;
              
              item._responseETag = theConnection.getResponseHeaders().findValue("ETag");
              failed = true;
              if (item._responseETag != null) {
                item._responseETag = ServiceUtils.removeQuotes(item._responseETag);
                byte[] clientSideHash = BinaryUtils.fromBase64(item._requestObject.getMd5Digest());
                byte[] serverSideHash = BinaryUtils.fromHex(item._responseETag);
  
                if (!Arrays.equals(clientSideHash, serverSideHash)) {
                  LOG.error("Unable to verify integrity of data upload.  " +
                          "Client calculated content hash" + Arrays.toString(clientSideHash)+  
                          "didn't match hash calculated by Amazon S3:" + Arrays.toString(serverSideHash));
                  
                }
                else {
                  failed = false;
                }
              }
              
              if (!failed) { 
                synchronized (_uploadQueue) {
                  _inFlightCount--;
                  _completedItems.put(item._requestObject.getPartNumber(),item);
                  _uploadQueue.notifyAll();
                }
              }
            }
            else {
              errorString = "Http Request for: " + item + " Failed with Headers:" + theConnection.getResponseHeaders()
                  + " Response:" + theConnection.getErrorResponse();
              LOG.error(errorString);
            }
          }
          
          // if the get failed ... 
          if (failed) { 
            // check to see if we have a cached exception ...
            IOException failureException = null; // item.getException();
            
            if (failureException == null) { 
              // if not ... construct one from the result (if present).... 
              if (theConnection.getContentBuffer().available() != 0) {
                if (errorString.length() ==0)
                  errorString = "HTTP Request for:" + item + " Failed";
                failureException = new IOException(errorString);
              }
            }
            
            LOG.error("uploadFailed for Item:" + item + " with Exception:" + failureException);
            synchronized (_uploadQueue) {
              _inFlightCount--;
              _uploadQueue.add(item);
            }
          }
        }
        catch (Exception e) { 
          LOG.error(CCStringUtils.stringifyException(e));
          if (_failureException.get() == null) { 
            if (e instanceof IOException)
              _failureException.set((IOException)e);
            else
              _failureException.set(new IOException(e));
          }
        }
        finally { 
          // shutdown the uploader ... 
          item.shutdownUpload();
  
          synchronized (_uploadSlots) {
            // empty the slot ... 
            _uploadSlots[slotIndex] = null;
          }
        }
  
        // and fill slots ...
        fillSlots();
      }
    }
  }


  @Override
  public void HttpContentAvailable(NIOHttpConnection theConnection,NIOBufferList contentBuffer) {
    
  }
  
  public static void main(String[] args) throws Exception {
    if (args[0].equalsIgnoreCase("upload")) { 
      File inputPath = new File(args[1]);
      String s3AccessKey = args[2];
      String s3Secret = args[3];
      URI uri = new URI(args[4]);
      
      FileInputStream inputStream = new FileInputStream(inputPath);
      try { 
        S3MultipartUploadStream uploadStream = new S3MultipartUploadStream(uri, s3AccessKey, s3Secret, false, null, 10, DEFAULT_PART_BUFFER_SIZE, 20, 3);
        try { 
          IOUtils.copyBytes(inputStream,uploadStream, 1 * 1024*1024);
        }
        finally { 
          uploadStream.close();
        }
      }
      finally { 
        inputStream.close();
      }
    }
    else if (args[0].equalsIgnoreCase("purge")){ 
      String s3AccessKey = args[1];
      String s3Secret = args[2];
      String s3Bucket = args[3];
      
      AmazonS3Client s3Client = new AmazonS3Client(new BasicAWSCredentials(s3AccessKey,s3Secret));
      
      ListMultipartUploadsRequest allMultpartUploadsRequest = 
          new ListMultipartUploadsRequest(s3Bucket);
      MultipartUploadListing multipartUploadListing = 
          s3Client.listMultipartUploads(allMultpartUploadsRequest);
      
      Date now = new Date();
      do {
        for (MultipartUpload upload : multipartUploadListing.getMultipartUploads()) {
            if (upload.getInitiated().compareTo(now) < 0) {
              LOG.info("Deleting Upload for Key:" + upload.getKey() + " UploadId:" + upload.getUploadId());
                s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(
                        s3Bucket, upload.getKey(), upload.getUploadId()));
            }
        }

        ListMultipartUploadsRequest request = new ListMultipartUploadsRequest(s3Bucket)
            .withUploadIdMarker(multipartUploadListing.getNextUploadIdMarker())
            .withKeyMarker(multipartUploadListing.getNextKeyMarker());
        multipartUploadListing = s3Client.listMultipartUploads(request);
      } while (multipartUploadListing.isTruncated());
    }
  }
}
