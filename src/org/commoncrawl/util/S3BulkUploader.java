package org.commoncrawl.util;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.async.Timer;
import org.commoncrawl.io.NIOBufferList;
import org.commoncrawl.io.NIOHttpConnection;
import org.commoncrawl.io.NIOHttpConnection.State;

public class S3BulkUploader implements NIOHttpConnection.Listener {

  EventLoop _theEventLoop;
  FileSystem _fileSystem;
  Path         _uploadCandidates[];
  int           _bandwidthPerUploader;
  S3Uploader _uploaders[];
  String       _s3Bucket;
  String       _s3AccessId;
  String       _s3SecretKey;
  Timer      _timer;
  S3BulkUploader.Callback _callback;
  int         _lastUploaderId = 0;

  
  /** logging **/
  static final Log LOG = LogFactory.getLog(S3BulkUploader.class);

  
  /** default polling interval **/
  private static final int DEFAULT_POLL_INTERVAL = 500;
  
  public static class UploadCandidate { 
    
    public UploadCandidate(Path path,String uploadName,String mimeType,String acl) { 
      _path = path;
      _uploadName = uploadName;
      _mimeType = mimeType;
      _acl = acl;
    }
    
    public Path     _path;
    public String   _uploadName;
    public String   _mimeType;
    public String   _acl;
  }

  public static interface Callback { 
    /** get next upload candidate **/
    public S3BulkUploader.UploadCandidate getNextUploadCandidate();
    /** the upload failed ... return true if we should retry the item **/
    public void  uploadFailed(Path path,IOException e);
    /** the upload succeeded for the specified item **/
    public void uploadComplete(Path path,String bandwidthStats);
  }
  
  public S3BulkUploader(EventLoop eventLoop,FileSystem fileSystem,S3BulkUploader.Callback callback,String s3Bucket,String s3AccessId, String s3SecretKey,int bandwidthPerUploader,int maxUploaders) { 
    _theEventLoop = eventLoop;
    _fileSystem = fileSystem;
    _bandwidthPerUploader = bandwidthPerUploader;
    _uploaders = new S3Uploader[maxUploaders];
    _s3Bucket = s3Bucket;
    _s3AccessId = s3AccessId;
    _s3SecretKey = s3SecretKey;
    _callback = callback;
  }
  
  public void startUpload() { 
    _timer = new Timer(DEFAULT_POLL_INTERVAL,true,new Timer.Callback() {

      public void timerFired(Timer timer) {
        fillSlots();
      }
    });
    _theEventLoop.setTimer(_timer);
  }
  
  private void fillSlots() { 
    for (int i=0;i<_uploaders.length;++i) { 
      // if empty slot found ... 
      if (_uploaders[i] == null) { 
        S3BulkUploader.UploadCandidate uploadCandidate = _callback.getNextUploadCandidate();
        
        if (uploadCandidate != null) {
          LOG.info("Queuing: " + uploadCandidate._path.toString() + " for Upload");
          _uploaders[i] = new S3Uploader(++_lastUploaderId,_theEventLoop,_fileSystem,uploadCandidate._path,
              _bandwidthPerUploader,_s3Bucket,uploadCandidate._uploadName,uploadCandidate._mimeType,_s3AccessId,_s3SecretKey,uploadCandidate._acl);
          _uploaders[i].setSlot(i);
          try { 
            _uploaders[i].startUpload(this);
          }
          catch (IOException e) { 
            
            LOG.error ("Upload for : " + uploadCandidate._path.toString() + " FAILED with Exception:" + CCStringUtils.stringifyException(e));
            // notify controller through callback ...
            _callback.uploadFailed(uploadCandidate._path, e);
          }
        }
      }
    }
  }
  
  public void HttpConnectionStateChanged(NIOHttpConnection theConnection,State oldState, State state) {
    
    // extract the reference to the uploader based on 
    S3Uploader uploader = (S3Uploader) theConnection.getContext();
    
    System.out.println("HttpConnection for: " + uploader.getPath() + " transitioned:" + oldState + " to " + state );
    
    // get the associated slot ... 
    int slotIndex = uploader.getSlot();
    
    if (state == State.ERROR || state == State.DONE) { 
      
      boolean failed = true;
      
      if (state == State.DONE) { 
        
        int resultCode = NIOHttpConnection.getHttpResponseCode(theConnection.getResponseHeaders());
        
        if (resultCode == 200) {
          failed = false;
          BandwidthUtils.BandwidthStats stats = new BandwidthUtils.BandwidthStats();
          uploader._rateLimiter.getStats(stats);
          
          _callback.uploadComplete(uploader.getPath(),"DownloadSpeed:" + stats.scaledBitsPerSecond + " " + stats.scaledBitsUnits);
        }
      }
      
      // if the get failed ... 
      if (failed) { 
        // check to see if we have a cached exception ...
        IOException failureException = uploader.getException();
        
        if (failureException == null) { 
          // if not ... construct one from the result (if present).... 
          if (theConnection.getContentBuffer().available() != 0) { 
            failureException = S3Uploader.failureExceptionFromContent(theConnection);
          }
        }
        
        _callback.uploadFailed(uploader.getPath(), failureException);
        LOG.info("Returned from uploadFailed");
      }
      
      if (failed)
        LOG.info("Calling uploader.shutdown");
      // shutdown the uploader ... 
      uploader.shutdown();
      
      if (failed)
        LOG.info("Post uploader.shutdown");
      
      // empty the slot ... 
      _uploaders[slotIndex] = null;

      if (failed)
        LOG.info("Calling fill Slots");
      
      // and fill slots ... 
      fillSlots();
      
      if (failed)
        LOG.info("Post fill Slots");
      
    }
  }

  public void HttpContentAvailable(NIOHttpConnection theConnection,NIOBufferList contentBuffer) {
    // TODO Auto-generated method stub
    
  } 
}