package org.commoncrawl.util;

import java.io.EOFException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.IOUtils;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;

/** 
 * A resilient blocking InputStream that reads data off of S3. Resilient - as in able to transparently recover from 
 * IO errors when reading data from S3. 
 *  
 * @author rana
 *
 */
public class S3SeekableResilientInputStream extends InputStream implements Seekable, PositionedReadable { 

  URI           _uri;
  String        _s3AccessKey;
  String        _s3Secret;
  S3InputStream _s3Stream;
  int           _bufferSize;
  // position of the file cursor 
  long          _cursorPos = 0;
  // position of the stream
  long          _streamPos = 0;
  long          _streamLength=-1;
  int           _retryCounts;
  int           _maxRetries;

  /** logging **/
  private static final Log LOG = LogFactory.getLog(S3SeekableResilientInputStream.class);

  
  
  public S3SeekableResilientInputStream(URI uri,String s3AccessKey,String s3Secret,int bufferSize,int maxRetries)throws IOException{ 
    _uri = uri;
    _bufferSize = bufferSize;
    _s3AccessKey = s3AccessKey;
    _s3Secret = s3Secret;
    _maxRetries = maxRetries;
    _streamLength = getFileLength(uri, s3AccessKey, s3Secret);
    
    restartStream();
  }
  
  private static long getFileLength(URI uri,String s3AccessKey,String s3Secret)throws IOException { 
    BasicAWSCredentials credentials 
    = new BasicAWSCredentials(
        s3AccessKey,
        s3Secret);
  
    AmazonS3Client s3Client = new AmazonS3Client(credentials);
    try { 
      ObjectMetadata metadata = s3Client.getObjectMetadata(uri.getHost(), uri.getPath().substring(1));
      return metadata.getContentLength();
    }
    finally { 
      s3Client.shutdown();
    }
    
  }
  
  private void restartStream()throws IOException {
    if (_s3Stream != null) { 
      _s3Stream.close();
      _s3Stream = null;
    }
    LOG.info("Restart Stream:" + _uri.toString() + " at Position:"+ _streamPos);
    _s3Stream = new S3InputStream(_uri, _s3AccessKey, _s3Secret,_bufferSize,_streamPos);
  }
  
  @Override
  public int read() throws IOException {
    IOException lastException = null;
    if (_streamPos != _cursorPos) { 
      _streamPos = _cursorPos;
      restartStream();
    }
      
    do {
      try { 
        int bytesRead = _s3Stream.read();
        if (bytesRead != -1) { 
          _streamPos++;
          _cursorPos++;
        }
        else if (_streamPos != _streamLength) { 
          throw new EOFException();
        }
        return bytesRead;
      }
      catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
        lastException = e;
      }
      if (++_retryCounts < _maxRetries) { 
        restartStream();
      }
      else { 
        LOG.error("Retry Count for Stream:" + _uri.toString() + " Exceeded!");
        break;
      }
    }
    while (true);
    
    throw lastException;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    
    if (_streamPos != _cursorPos) { 
      _streamPos = _cursorPos;
      restartStream();
    }
    int bytesRead = streamRead(b, off, len);
    if (bytesRead != -1) 
      _cursorPos += bytesRead;
    return bytesRead;
  }
  
  private int streamRead(byte[] b, int off, int len) throws IOException {
    IOException lastException = null;
    do {
      try { 
        int bytesRead = _s3Stream.read(b, off, len);
        if (bytesRead != -1) {  
          _streamPos += bytesRead;
          //LOG.info("Stream:" + _uri.toString() + " Pos:" + _streamPos);
        }
        else if (_streamPos != _streamLength) { 
          throw new EOFException();
        }
        return bytesRead;
      }
      catch (IOException e) { 
        LOG.error(CCStringUtils.stringifyException(e));
        lastException = e;
      }
      if (++_retryCounts < _maxRetries) { 
        restartStream();
      }
      else { 
        LOG.error("Retry Count for Stream:" + _uri.toString() + " Exceeded!");
        break;
      }
    }
    while (true);
    
    throw lastException;
  }
  
  @Override
  public void close() throws IOException {
    if (_s3Stream != null) { 
      _s3Stream.close();
      _s3Stream = null;
    }
  }

  @Override
  public void seek(long pos) throws IOException {
    _cursorPos = pos;
    if (_streamPos != _cursorPos) { 
      _streamPos = _cursorPos;
      restartStream();
    }
  }

  @Override
  public long getPos() throws IOException {
    return _cursorPos;
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    seek(targetPos);
    return true;
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length)throws IOException {
    LOG.info("PREAD pos:" + position + " originalPos:" + _streamPos + " URI:" + _uri.toString());
    if (_streamPos != position) { 
      _streamPos = position;
      restartStream();
    }
    return streamRead(buffer,offset,length);
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length)
      throws IOException {
    LOG.info("PREAD pos:" + position + " originalPos:" + _streamPos + " URI:" + _uri.toString());
    if (_streamPos != position) { 
      _streamPos = position;
      restartStream();
    }
    int toRead = length;
    while (toRead > 0) {
      int ret = streamRead(buffer, offset, toRead);
      if (ret < 0) {
        throw new IOException("Premature EOF from inputStream");
      }
      toRead -= ret;
      offset += ret;
    }
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    readFully(position, buffer, 0, buffer.length);
  }
  
  
}
