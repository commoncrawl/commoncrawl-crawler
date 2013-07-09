package org.commoncrawl.util;

/**
* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 **/


import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.async.Callback;
import org.commoncrawl.async.Timer;
import org.commoncrawl.io.NIOBufferList;
import org.commoncrawl.io.NIOBufferListInputStream;
import org.commoncrawl.io.NIOHttpConnection;

/** 
 * 
 * An InputStream that fetches data from S3 by using an
 * S3Downloader instance to fetch/buffer data in a background thread.
 *
 * @author rana
 *
 */
public class S3InputStream extends NIOBufferListInputStream implements S3Downloader.Callback, Timer.Callback  {

  /** logging **/
  private static final Log LOG = LogFactory.getLog(S3InputStream.class);
  
  URI uri;
  S3Downloader downloader = null;
  AtomicReference<Exception> _exception = new AtomicReference<Exception>();
  ReentrantLock _writeLock = new ReentrantLock();
  AtomicReference<Condition>     _writeEvent = new AtomicReference<Condition>(_writeLock.newCondition());
  long                           _waitStartTime = -1;
  boolean                        _inWaitState = false;
  AtomicBoolean _eofCondition = new AtomicBoolean();
  AtomicReference<NIOHttpConnection> pausedConnection = new AtomicReference<NIOHttpConnection>();
  AtomicReference<NIOHttpConnection> activeConnection = new AtomicReference<NIOHttpConnection>();
  int activeItemId = -1;
  String activeItemKey = null;
  int MAX_BUFFER_SIZE = 1048576 * 5;
  Timer timeoutTimer;

  
  /** 
   * Initiate the stream with specified s3/s3n uri. 
   * @param uri s3/s3n uri that points to an s3 object 
   * @param s3AccessKey  
   * @param s3Secret 
   * @param bufferSize set this to be at least 1MB or higher to ensure decent performance 
   * @throws IOException
   */
  public S3InputStream(URI uri,String s3AccessKey,String s3Secret,int bufferSize,long seekPos) throws IOException { 
    super(new NIOBufferList());
    
    this.uri = uri;
    
    downloader = new S3Downloader(uri.getHost(), s3AccessKey,s3Secret, false);
    // we are download a single stream ... 
    downloader.setMaxParallelStreams(1);
    // initialize the callback 
    downloader.initialize(this);
    // initiate the download 
    LOG.info("Fetching:" + uri.getPath() + " seekPos:" + seekPos);
    if (seekPos == 0) { 
      downloader.fetchItem(uri.getPath().substring(1));
    }
    else { 
      downloader.fetchPartialItem(uri.getPath().substring(1), seekPos,-1L);
    }
    timeoutTimer = new Timer(5000,true,this);
  }
  
  
  @Override
  protected void ensureBuffer() throws IOException {
    
    do {

      super.ensureBuffer();
      
      if (_activeBuf == null) {
        // ok, unpause the connection in case it is in a paused state before going into a wait state ... 
        unpauseConnection();
        //System.out.println("Read from Main Thread  for Path:" + uri + ". Checking for EOF or Error");
        _writeLock.lock();
        try { 
          if (_eofCondition.get()) {
            if (_exception.get() != null) { 
              LOG.error("Read from Main Thread for Path:" + uri + " detected Exception"); 
              throw new IOException(_exception.get());
            }
            else {
              LOG.info("Read from Main Thread for Path:" + uri + " detected EOF");
              return;
            }
          }
          else { 
            _writeEvent.set(_writeLock.newCondition());
            _inWaitState = true;
            _waitStartTime = System.currentTimeMillis();
            //long nanoTimeStart = System.nanoTime();
            //System.out.println("Read from Main Thread for Path:" + uri + " Waiting on Write");
            try {
              _writeEvent.get().await();
              _waitStartTime = -1L;
              //long nanoTimeEnd = System.nanoTime();
              //System.out.println("Read from Main Thread for Path:" + uri + " Returned from Wait Took:" + (nanoTimeEnd-nanoTimeStart));
            } catch (InterruptedException e) {
              LOG.error("Read from Main Thread for Path:" + uri + " was Interrupted. Exiting");
              throw new IOException(e);
            }
          }
        }
        finally {
          _inWaitState = false;
          _writeLock.unlock();
        }
      }
    }
    while(_activeBuf == null);
    
    if (_bufferQueue.available() < MAX_BUFFER_SIZE) {
      unpauseConnection();
    }
  }

  void unpauseConnection() {
    if (pausedConnection.get() != null) { 
      downloader.getEventLoop().queueAsyncCallback(new Callback() {
  
        @Override
        public void execute() {
          final NIOHttpConnection connection = pausedConnection.get();
          pausedConnection.set(null);
          if (connection != null) { 
            LOG.info("*** RESUMING DOWNLOADS FOR:" + connection.getURL() + "***");
            try {
              connection.enableReads();
            } catch (IOException e) {
              LOG.error(CCStringUtils.stringifyException(e));
            }
          }
        } 
      });
    }
  }
  
  @Override
  public void close() throws IOException {
    downloader.shutdown();
  }  
  
  @Override
  public boolean downloadStarting(NIOHttpConnection connection,int itemId, String itemKey,long contentLength) {
    activeConnection.set(connection);
    downloader.getEventLoop().setTimer(timeoutTimer);
    activeItemId = itemId;
    activeItemKey = itemKey;
    
    return true;
  }

  @Override
  public boolean contentAvailable(NIOHttpConnection theConnection,int itemId, String itemKey,NIOBufferList contentBuffer) {
    
    ByteBuffer buffer = null;
    IOException exception = null;
    //int receivedBytes = 0;
    try { 
      while ((buffer  = contentBuffer.read()) != null) {
        if (buffer.position() != 0) { 
          buffer = buffer.slice();
        }
        //receivedBytes += buffer.remaining();
        buffer.position(buffer.limit());
        _bufferQueue.write(buffer);
      }
      _bufferQueue.flush();
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      exception = e;
    }
    if (_bufferQueue.available() >= MAX_BUFFER_SIZE) {
      LOG.info("*** PAUSING DOWNLOADS FOR:" + theConnection.getURL());
      theConnection.disableReads();
      pausedConnection.set(theConnection);
    }
    //long nanoTimeStart = System.nanoTime();
    _writeLock.lock();
    //long nanoTimeEnd = System.nanoTime();
    //System.out.println("Received: " + receivedBytes + "for URI:" + uri + " Lock took:" + (nanoTimeEnd-nanoTimeStart));
    try { 
      Condition writeCondition = _writeEvent.getAndSet(null);
      if (exception != null) { 
        _eofCondition.set(true);
        _exception.set(exception);
      }
      if (writeCondition != null) { 
        writeCondition.signal();
      }
    }
    finally { 
      _writeLock.unlock();
    }
    return true;
  }

  @Override
  public void downloadFailed(NIOHttpConnection connection,int itemId, String itemKey, String errorCode) {
    LOG.error("Download Failed for URI:" + S3InputStream.this.uri);
    _writeLock.lock();
    try {
      _exception.set(new IOException(errorCode));
      _eofCondition.set(true);
      Condition writeCondition = _writeEvent.getAndSet(null);
      if (writeCondition != null) { 
        writeCondition.signal();
      }
    }
    finally { 
      _writeLock.unlock();
    }
    downloader.getEventLoop().cancelTimer(timeoutTimer);
    activeConnection.set(null);
  }

  @Override
  public void downloadComplete(NIOHttpConnection connection,int itemId, String itemKey) {
    LOG.info("Download Complete for URI:" + S3InputStream.this.uri);
    _writeLock.lock();
    try {
      _exception.set(null);
      _eofCondition.set(true);
      Condition writeCondition = _writeEvent.getAndSet(null);
      if (writeCondition != null) { 
        writeCondition.signal();
      }
    }
    finally { 
      _writeLock.unlock();
    }
    downloader.getEventLoop().cancelTimer(timeoutTimer);
    activeConnection.set(null);
  }

  private static final int WAIT_LOCK_TIMEOUT = 5 * 60000;
  @Override
  public void timerFired(Timer timer) {
    LOG.info("timeout timer fired");
    boolean timedOut = false;
    NIOHttpConnection connection = activeConnection.get();
    if (connection != null) { 
      if (pausedConnection.get() == null) { 
        if (connection.checkForTimeout()) { 
          LOG.info("*** TIMEOUT detected via HTTPConnection for stream:" + connection.getURL());
          timedOut = true;
        }
      }
    }
    
    if (!timedOut) { 
      _writeLock.lock();
      try { 
        if (_inWaitState) { 
          if (System.currentTimeMillis() - _waitStartTime >= WAIT_LOCK_TIMEOUT) { 
            LOG.info("*** TIMEOUT detected via LOCKWAIT time for stream:" + connection.getURL());
            timedOut = true;
          }
        }
      }
      finally { 
        _writeLock.unlock();
      }
    }
    
    if (timedOut) {
      downloader.shutdown();
      downloadFailed(activeConnection.get(), activeItemId, activeItemKey, "TIMEOUT");
    }
  }
}
