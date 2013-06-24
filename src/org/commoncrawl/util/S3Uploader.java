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
package org.commoncrawl.util;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.io.NIOBufferList;
import org.commoncrawl.io.NIOHttpConnection;
import org.commoncrawl.io.NIOHttpHeaders;
import org.commoncrawl.util.BandwidthUtils.BandwidthStats;
import org.commoncrawl.util.GZIPUtils.UnzipResult;


/**
 * 
 * @author rana
 *
 */
public class S3Uploader implements NIOHttpConnection.DataSource {

  /** logging **/
  static final Log LOG = LogFactory.getLog(S3Uploader.class);
  
  private static final int MAX_QUEUED_READ_SIZE = 10 * 1024 * 1024;

  EventLoop _eventLoop;
  FSDataInputStream _inputStream;
  FileSystem _fileSystem;
  Path    _uploadTarget;
  String  _s3Bucket;
  String  _s3Key;
  String _s3ACL;
  String  _contentType;
  long   _contentLength;
  String  _s3AccessId;
  String  _s3SecretKey;
  IOException _exception = null;
  AtomicReference<Thread> _loaderThread = new AtomicReference<Thread>();
  NIOBufferList _writeBuffer = new NIOBufferList();
  ReentrantLock _readLock = new ReentrantLock();
  Condition _readEvent = _readLock.newCondition();
  boolean _abort = false;
  boolean _loadComplete = false;
  NIOHttpConnection _connection = null;
  S3Utils.CallingFormat _callingFormat = S3Utils.CallingFormat.getSubdomainCallingFormat();
  int         _slot;
  int         _bandWidthLimit;
  int         _Id;
  long  _bytesUploaded = 0;
  
  static IOException failureExceptionFromContent(NIOHttpConnection theConnection) { 

    String errorDescription = null;
    
    if (theConnection.getContentBuffer().available() != 0) { 

      NIOBufferList contentBuffer = theConnection.getContentBuffer();
      
      try { 
        // now check headers to see if it is gzip encoded
        int keyIndex =  theConnection.getResponseHeaders().getKey("Content-Encoding");
        
        if (keyIndex != -1) { 

          String encoding = theConnection.getResponseHeaders().getValue(keyIndex);
        
          byte data[] = new byte[contentBuffer.available()]; 
          // and read it from the niobuffer 
          contentBuffer.read(data);
            
            if (encoding.equalsIgnoreCase("gzip")) { 
              UnzipResult result = GZIPUtils.unzipBestEffort(data,256000);
              if (result != null) { 
                contentBuffer.reset();
                contentBuffer.write(result.data.get(), 0, result.data.getCount());
                contentBuffer.flush();
              }
            }
        }
        
        byte data[] = new byte[contentBuffer.available()];
      
        contentBuffer.read(data);
        
        ByteBuffer bb = ByteBuffer.wrap(data);
        StringBuffer buf = new StringBuffer();
        buf.append(Charset.forName("ASCII").decode(bb));
        errorDescription = buf.toString();
      }
      catch (IOException e) { 
        LOG.error(CCStringUtils.stringifyException(e));
      }
    }
    if (errorDescription == null) { 
      errorDescription = "UNKNOWN ERROR";
    }
    return new IOException(errorDescription);
  }
  
  BandwidthUtils.RateLimiter _rateLimiter = null;
  
  public S3Uploader(int uploaderId,EventLoop eventLoop,FileSystem fileSystem,Path uploadTarget,int bandWidthLimit,String s3Bucket,String s3Key,String contentMimeType,String s3AccessId,String s3SecretKey,String acl) { 
    
    _Id = uploaderId;
    _eventLoop = eventLoop;
    _fileSystem = fileSystem;
    _uploadTarget = uploadTarget;
    _s3Bucket = s3Bucket;
    _s3Key = s3Key;
    _s3ACL = acl;
    _s3AccessId = s3AccessId;
    _s3SecretKey = s3SecretKey;
    _contentType = contentMimeType;
    _bandWidthLimit = bandWidthLimit;
    _rateLimiter = new BandwidthUtils.RateLimiter(_bandWidthLimit);
    _writeBuffer.setMinBufferSize(65536 * 2);
    
  }
 
  public int getSlot() { return _slot; }
  public void setSlot(int index) { _slot = index; }
  
  Object _context;
  
  public void setContext(Object o) { _context = o; } 
  public Object getContext() { return _context; }
 
  public Path getPath() { return _uploadTarget; }
  
  private void startLoader() throws IOException { 

    _contentLength  = _fileSystem.getFileStatus(_uploadTarget).getLen();
    
    LOG.info("startLoader - Content Size is:" + _contentLength);
    
    _inputStream = _fileSystem.open(_uploadTarget);
    _abort = false;
    _loaderThread.set(new Thread(new Runnable() {

      public void run() {
        
        try {
  
          while (!_abort && _inputStream.available() != 0) {
            // first see if have reached a read threshold ...
            if (_writeBuffer.available() >= MAX_QUEUED_READ_SIZE) {

              //LOG.info("Connection:[" + _connection.getId() + "] Load Thread for Path:" + _uploadTarget.getName() + " Detected Queue Full. Grabbing Read Lock");

              try {                 
                
                // acquire the read lock 
                _readLock.lock();
                // set up our read event ... 
                _writeBuffer.setReadEvent(_readLock,_readEvent);
  
                // LOG.info("Connection:[" + _connection.getId() + "] Load Thread for Path:" + _uploadTarget.getName() + " Waiting on Read Event");
                // and wait on read event ... 
                try {
                  _readEvent.await();
                } catch (InterruptedException e) {
                  //LOG.info("Connection:[" + _connection.getId() + "] Load Thread for Target:" + _uploadTarget.toString() + " Interrupted");
                  _abort = true;
                }
  
                //LOG.info("Connection:[" + _connection.getId() + "] Load Thread for Path:" + _uploadTarget.getName() + " Woke up from Wait on Read Event");
              }
              finally { 
                _readLock.unlock();
              }
            }
            
            if (!_abort) { 
              ByteBuffer buffer = _writeBuffer.getWriteBuf();
              
              if (buffer == null) { 
                _exception = new IOException("Out Of Memory Error");
                LOG.error(CCStringUtils.stringifyException(_exception));
                break;
              }
              int bytesRead = _inputStream.read(buffer.array(),buffer.position(),buffer.remaining());
              buffer.position(buffer.position() + bytesRead);
              
              // LOG.info("Connection:[" + _connection.getId() + "] loadThread for Path:" + _uploadTarget.getName() + " Read:" + bytesRead);
            }
          }
          
          // LOG.info("loadThread for Path:" + _uploadTarget.getName() +" Done");
          
          if (_abort && _exception == null) { 
            _exception = new IOException("Transfer Explicitly Aborted");
            LOG.error(CCStringUtils.stringifyException(_exception));
          }
        } catch (IOException e) {
          _exception = e;
          LOG.error(CCStringUtils.stringifyException(e));
        }
        finally { 
          try {
            if (_inputStream != null) { 
              _inputStream.close();
            }
            _inputStream = null;
            
          } catch (IOException e) {
            LOG.error(CCStringUtils.stringifyException(e));
          }
          _loaderThread.set(null);
        }
        
        if (!_abort && _exception == null) {
          synchronized (_writeBuffer) { 
            _writeBuffer.flush();
            // LOG.info("Reader Thread Bytes Available:" + _writeBuffer.available());
            _loadComplete = true;
          }
        }
        else if(_exception != null) {
          if (_connection != null)
            _connection.close();
        }
        
        //LOG.info("Connection:[" + _connection.getId() + "] Load Thread for Target:" + _uploadTarget.toString() + " EXITING");
      }
    }));
    
    _loaderThread.get().start();
  }
  
  public void shutdown() { 
    if (_connection != null) { 
      _connection.close();
      _connection.setContext(null);
      _connection.setListener(null);
      _connection = null;
    }
    _abort = true;
    // signal the read event incase the read thread is locked on it... 
    if (_readLock != null) { 
      _readLock.lock();
      if (_readEvent != null) { 
        _readEvent.signal();
      }
      _readLock.unlock();
    }
    while(_loaderThread.get() != null) { 
      try {
        // LOG.info("Waiting for Loader Thread for Target:" + _uploadTarget + " to Exit");
        Thread thread = _loaderThread.get();
        if (thread != null) { 
          thread.join(10);
        }
        // LOG.info("Returned from Wait on Loader Thread for Target:" + _uploadTarget);
      } catch (InterruptedException e) {
        LOG.error(CCStringUtils.stringifyException(e));
      }
    }
    _writeBuffer.reset();
  }
  
  /**
   * Generate an rfc822 date for use in the Date HTTP header.
   */
  private static String httpDate() {
      final String DateFormat = "EEE, dd MMM yyyy HH:mm:ss ";
      SimpleDateFormat format = new SimpleDateFormat( DateFormat, Locale.US );
      format.setTimeZone( TimeZone.getTimeZone( "GMT" ) );
      return format.format( new Date() ) + "GMT";
  }
 
  private static void addToAmazonHeader(String key,String value,Map amazonHeaders) { 
    List<String> list  = (List<String>) amazonHeaders.get(key);
    if (list == null) { 
      list = new Vector<String>();
      amazonHeaders.put(key, list);
    }
    list.add(value);
  }
  
  
  private static String normalizeACLString(String targetString) { 
    StringBuffer buffer =new StringBuffer();
    boolean lastCharWasWhitepsace = false;
    
    for( char c : targetString.toCharArray()) { 
      if (c == ' ' || c == '\t' || c == '\n') { 
//        if (!lastCharWasWhitepsace) { 
          buffer.append(' ');
//        }
        lastCharWasWhitepsace = true;
      }
      else {
        if (c == '<') 
          buffer.append("&lt;");
        else if (c == '>')
          buffer.append("&gt;");
        else
          buffer.append(c);
        lastCharWasWhitepsace = false;
      }
    }
    
    return buffer.toString();
  }
  
  
  public void startUpload(NIOHttpConnection.Listener listener) throws IOException {
     
    // start the file loader ... 
    startLoader();
    // construct the s3 url ...
    URL theURL = _callingFormat.getURL(false, S3Utils.DEFAULT_HOST, S3Utils.INSECURE_PORT, _s3Bucket, _s3Key, null);
    // allocate an http connection 
    _connection = new NIOHttpConnection(theURL,_eventLoop.getSelector(),_eventLoop.getResolver(),null);
    _connection.setId(_Id);
    LOG.info("Connection for Path:" + _uploadTarget.getName() + " is:" + _connection.getId());
    // set the back pointer to us ... 
    _connection.setContext(this);
    // set rate limit policy ... 
    _connection.setUploadRateLimiter(_rateLimiter);
    // specify that we will populate our own request headers ... 
    _connection.setPopulateDefaultHeaderItems(false);
    // set up the data source ... 
    _connection.setDataSource(this);
    // get at headers ... 
    NIOHttpHeaders headers = _connection.getRequestHeaders();

    // populate http request string 
    headers.prepend("PUT" + " " + theURL.getFile() +" "  + "HTTP/1.1", null);
    
    if (theURL.getPort() != -1 && theURL.getPort() != 80) { 
      headers.set("Host",theURL.getHost() +":"+String.valueOf(theURL.getPort()));
    }
    else { 
      headers.set("Host",theURL.getHost());
    }
    // create a tree map in parallel (to pass to canonicalization routine for s3 auth)
    Map amazonHeaders = new TreeMap();
    
    // set mime type header entry ... 
    headers.set("Content-Type", _contentType);
    // and add content type to amazon headers as well ..
    addToAmazonHeader("Content-Type", _contentType,amazonHeaders);
    
    
    // and add content length ... 
    headers.set("Content-Length", ((Long)_contentLength).toString());
    
    // add date ... 
    String theDate = httpDate();
    
    headers.set("Date", theDate);
    addToAmazonHeader("Date", theDate, amazonHeaders);
    
    // set reduced redundancy flag 
    headers.set("x-amz-storage-class", "REDUCED_REDUNDANCY");
    addToAmazonHeader("x-amz-storage-class", "REDUCED_REDUNDANCY", amazonHeaders);
    
    // specify reduced redundancy storage 
    // and if acl is specified... 
    if (_s3ACL != null) {
      
      String aclStringNormalized = normalizeACLString(_s3ACL);
      // add it to the list of headers 
      //headers.set("x-amz-acl", _s3ACL);
      // and to the list of headers used to canonacalize the url ... 
      //addToAmazonHeader("x-amz-acl", aclStringNormalized, amazonHeaders);
      

    }
    
    
    
    String canonicalString =  S3Utils.makeCanonicalString("PUT", _s3Bucket, _s3Key, null,amazonHeaders );
    
    //LOG.info("Headers for Request:" + headers.toString());
    //LOG.info("Cannonica for Request:" + canonicalString);
    
    String encodedCanonical = S3Utils.encode(_s3SecretKey, canonicalString, false);
    
    // add auth string to headers ... 
    headers.set("Authorization","AWS " + _s3AccessId + ":" + encodedCanonical);
    
    // add cache control pragmas ... 
    headers.set ("Connection", "close");
    headers.set("Cache-Control", "no-cache");
    headers.set("Pragma", "no-cache");
    
    // ready to roll ... 
    
    // set the listener ... 
    _connection.setListener(listener);
    // and open the connection 
    _connection.open();
  }
  
  public void abortUpload() { 
    _abort = true;
    _connection.close();
    
  }
  
  public IOException getException() { 
    return _exception;
  }


  public boolean read(NIOHttpConnection connection,NIOBufferList dataBuffer)throws IOException {
    
    ByteBuffer buffer = null;
    
    if ((buffer = _writeBuffer.read()) != null) {
       _bytesUploaded += buffer.remaining();
       BandwidthStats stats = new BandwidthStats();
       _rateLimiter.getStats(stats);
       System.out.println("[" + _slot + "]ID:" + _Id + " read Callback for S3Uploader for Path:" + _uploadTarget.getName() + " returned:" + buffer.remaining() + " Bytes TotalBytesRead:" + _bytesUploaded 
      		 +" Rate:" + stats.scaledBitsPerSecond + " " + stats.scaledBitsUnits );
       buffer.position(buffer.limit());
       dataBuffer.write(buffer);
       dataBuffer.flush();
    }
    
    boolean eof = false;
    
    synchronized(_writeBuffer) { 
      eof = _writeBuffer.available() == 0 && _loadComplete;
    }
    
    
    return eof;
  }

  @Override
  public void finsihedWriting(NIOHttpConnection connection,ByteBuffer thisBuffer) throws IOException {
    //NOOP
  }
}
