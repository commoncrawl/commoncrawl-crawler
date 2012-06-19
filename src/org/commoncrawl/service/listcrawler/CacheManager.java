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

package org.commoncrawl.service.listcrawler;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.MalformedURLException;
import java.nio.ByteBuffer;
import java.rmi.server.UID;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.record.Buffer;
import org.commoncrawl.async.ConcurrentTask;
import org.commoncrawl.async.ConcurrentTask.CompletionCallback;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.async.Timer;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.io.NIOHttpHeaders;
import org.commoncrawl.protocol.CacheItem;
import org.commoncrawl.protocol.CrawlURLMetadata;
import org.commoncrawl.service.listcrawler.HDFSFlusherThread.IndexDataFileTriple;
import org.commoncrawl.util.HttpHeaderInfoExtractor;
import org.commoncrawl.util.SessionIDURLNormalizer;
import org.commoncrawl.util.URLFingerprint;
import org.commoncrawl.util.URLUtils;
import org.commoncrawl.util.ArcFileItemUtils;
import org.commoncrawl.util.CCStringUtils;
import org.junit.Assert;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/** 
 * Class that encapsulates most of the serving side of the crawler
 * 
 * @author rana
 *
 */
public class CacheManager {
  
  private HashMultimap<Long,Long>               _fingerprintToLocalLogPos = HashMultimap.create();
  private LinkedList<CacheItem>                 _writeQueue = new LinkedList<CacheItem>() ;
  private LinkedBlockingQueue<CacheWriteRequest> _writeRequestQueue = new LinkedBlockingQueue<CacheWriteRequest>();
  private LinkedBlockingQueue<CacheFlushRequest> _hdfsFlushRequestQueue = new LinkedBlockingQueue<CacheFlushRequest>();
  private Vector<HDFSFileIndex>                  _hdfsIndexList = new Vector<HDFSFileIndex>();
  
  // private LinkedBlockingQueue<CacheLoadRequest> _loadRequestQueue = new LinkedBlockingQueue<CacheLoadRequest>();
  private static final int DEFAULT_DISK_READER_THREADS = 8 * 4;
  private static final int HDFS_READER_THREADS = 3*8*4;
  private static final int LOG_ACCESS_SEMAPHORE_COUNT = 100 + 1;
  static final int ITEM_RECORD_TRAILING_BYTES = 4; 
  // flush the local cache to hdfs once the local cache document counts exceeds this number 
  public static final int LOCAL_CACHE_FLUSH_THRESHOLD = 10000;
  // proxy cache location in hdfs 
  private static final String PROXY_CACHE_LOCATION = "crawl/proxy/cache";
  static final String PROXY_CACHE_FILE_DATA_PREFIX = "cacheData";
  static final String PROXY_CACHE_FILE_INDEX_PREFIX = "cacheIndex";
  
  private static final int CACHE_POLL_TIMER_INTERVAL = 10000;
  
  public static final Log LOG = LogFactory.getLog(CacheManager.class);
  
  /** log file header **/
  private LocalLogFileHeader _header = new LocalLogFileHeader();

  /** local data directory **/
  File _localDataDirectory;
  
  /** remote data directory **/
  Path _remoteDataDirectory;
  
  
  
  /** event loop **/
  EventLoop _eventLoop;
  
  /** reader thread pool **/
  ExecutorService _cacheLoadThreadPool = null;
  /** hdfs loader thread pool **/
  ExecutorService _hdfsLoaderPool = null;
  /** number of cache writer threads **/
  public static final int CACHE_WRITER_THREADS = 8;
  /** local cache writer thread(S) **/
  Thread _writerThreads[] = null;
  /** hdfs flusher thread **/ 
  Thread _hdfsFlusherThread = null;
  /** hdfs flusher active indicator **/
  boolean _hdfsFlusherActive = false;
  /** file system object **/
  FileSystem _remoteFileSystem = null;
  /** local log virtual offset **/
  long _localLogStartOffset = 0;
  /** local log access mutex **/
  Semaphore _localLogAccessSempahore = new Semaphore(LOG_ACCESS_SEMAPHORE_COUNT);
  /** local log write mutex **/
  Semaphore _localLogWriteAccessSemaphore = new Semaphore(1);
  /** session id normalizer **/
  SessionIDURLNormalizer _sessionIdNormalizer = new SessionIDURLNormalizer();
  // cache flush timer
  Timer _cacheFlushTimer;
  /** cache flush threshold **/
  int   _cacheFlushThreshold = LOCAL_CACHE_FLUSH_THRESHOLD;
  
  /** internal constructor for test purposes  
   * 
   */
  private CacheManager(EventLoop eventLoop) { 
    _localDataDirectory = new File("/tmp/proxy/localData");
    _localDataDirectory.mkdirs();
    _remoteDataDirectory = new Path("/tmp/proxy/remoteData");
    
    if (eventLoop == null) { 
      _eventLoop = new EventLoop();
      _eventLoop.start();
    }
    else { 
      _eventLoop = eventLoop;
    }
    Configuration conf = new Configuration();
    
    conf.addResource("nutch-default.xml");
    conf.addResource("nutch-site.xml");
    conf.addResource("hadoop-default.xml");
    conf.addResource("hadoop-site.xml");
    conf.addResource("commoncrawl-default.xml");
    conf.addResource("commoncrawl-site.xml");
    
    CrawlEnvironment.setHadoopConfig(conf);
    
    try { 
      _remoteFileSystem = FileSystem.getLocal(conf);
      _remoteFileSystem.mkdirs(_remoteDataDirectory);
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      throw new RuntimeException("Could not initialize hdfs connection");
    }
  }


  /** DocumentCache - proper constructor
   * 
   */
  public CacheManager(FileSystem remoteFileSystem,File dataDirectory,EventLoop eventLoop) {
    _localDataDirectory = dataDirectory;
    _eventLoop = eventLoop;
    _remoteFileSystem = remoteFileSystem;
    _remoteDataDirectory = new Path(PROXY_CACHE_LOCATION);
    
  }
  
  public static final int INIT_FLAG_SKIP_CACHE_WRITER_INIT = 1;
  public static final int INIT_FLAG_SKIP_HDFS_WRITER_INIT  = 2;
  public static final int INIT_FLAG_SKIP_INDEX_LOAD        = 4;
  
  /** 
   * set cache flush threshold
   *  best to call this before initialize
   */
  public void setCacheFlushThreshold(int cacheFlushThreshold) { 
    _cacheFlushThreshold = cacheFlushThreshold;
  }
  
  public int getCacheFlushThreshold() { 
  	return _cacheFlushThreshold;
  }
  
  
  /**
   * initialize the DocumentCache
   * 
   * @throws IOException
   */
  public void initialize(int initFlags)throws IOException { 
    initializeActiveLog();
    if ((initFlags & INIT_FLAG_SKIP_INDEX_LOAD) == 0) {
      loadHDFSIndexFiles();
    }
    if ((initFlags & INIT_FLAG_SKIP_CACHE_WRITER_INIT) == 0) { 
      startCacheWriterThread();
    }
    if ((initFlags & INIT_FLAG_SKIP_HDFS_WRITER_INIT) == 0) {
      startHDFSFlusherThread();
    }
    _cacheLoadThreadPool  = Executors.newFixedThreadPool(DEFAULT_DISK_READER_THREADS);
    _hdfsLoaderPool       = Executors.newFixedThreadPool(HDFS_READER_THREADS);
    _cacheFlushTimer = new Timer(CACHE_POLL_TIMER_INTERVAL,true,new Timer.Callback() {

      @Override
      public void timerFired(Timer timer) {
        if (!_hdfsFlusherActive) {
          synchronized(_header) { 
            // check status ...
            if (_header._itemCount >= _cacheFlushThreshold) { 
              LOG.info("Local Cache Item Count:" + _header._itemCount + " Exceeded Cache Flush Threshold. Scheduling HDFS Flush");
              CacheFlushRequest flushRequest = new CacheFlushRequest(_header._fileSize - LocalLogFileHeader.SIZE,(int)_header._itemCount);
              _hdfsFlushRequestQueue.add(flushRequest);
              _hdfsFlusherActive = true;
            }
          }
        }
      } 
      
    });
    _eventLoop.setTimer(_cacheFlushTimer);
    
    LOG.info("Initialization Complete");
  }
  
  /**
   * shutdown the DocuemntCache manager
   * 
   */
  public void shutdown() { 
    if (_cacheFlushTimer != null) { 
      _eventLoop.cancelTimer(_cacheFlushTimer);
      _cacheFlushTimer = null;
    }
    if (_writerThreads != null) { 
      LOG.info("Shuting down write threads");
      for (int i=0;i<CACHE_WRITER_THREADS;++i)
      	_writeRequestQueue.add(new CacheWriteRequest());
    	for (int i=0;i<CACHE_WRITER_THREADS;++i) {
        try {
    		_writerThreads[i].join();
        } catch (InterruptedException e1) {
        }
    	}
      _writerThreads = null;
    }
    if (_cacheLoadThreadPool != null) { 
      LOG.info("write thread terminated. shuting down reader threads");
      _cacheLoadThreadPool.shutdown();
      try {
        while (!_cacheLoadThreadPool.awaitTermination(5000, TimeUnit.MILLISECONDS)) { 
          LOG.info("waiting ... ");
        }
      } catch (InterruptedException e) {
      }
      LOG.info("reader thread terminated");
      _cacheLoadThreadPool = null;
    }
    
    if (_hdfsLoaderPool != null) { 
      LOG.info("shuting down hdfs loader threads");
      _hdfsLoaderPool.shutdown();
      try {
        while (!_hdfsLoaderPool.awaitTermination(5000, TimeUnit.MILLISECONDS)) { 
          LOG.info("waiting ... ");
        }
      } catch (InterruptedException e) {
      }
      LOG.info("hdfs loads threads terminated");
      _hdfsLoaderPool = null;
    }
  }

  /********************************************************************************************************/
  // CacheItemCheckCallback
  /********************************************************************************************************/

  public static interface CacheItemCheckCallback { 
    public void cacheItemAvailable(String url,CacheItem item);
    public void cacheItemNotFound(String url);
  }
  
  
  /**
   * check if this specified url is present in the cache
   * 
   * @param url
   * @param callback
   * @throws MalformedURLException
   */
  public void checkCacheForItem(final String url,final CacheItemCheckCallback callback) {
    try { 
      // get the normalized url 
      String normalizedURL = normalizeURL(url);
      // get the fingerprint 
      long urlfp = URLFingerprint.generate64BitURLFPrint(normalizedURL);
      // delegate to properly qualified method
      checkCacheForItem(normalizedURL,urlfp,callback);
    }
    catch (MalformedURLException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      queueCacheItemNotFoundCallback(callback, url);
    }
  }
  
  /**
   * check if this specified url is present in the cache
   * 
   * @param url
   * @param callback
   * @throws MalformedURLException
   */
  public CacheItem checkCacheForItemInWorkerThread(final String url) {
    try { 
      // get the normalized url 
      String normalizedURL = normalizeURL(url);
      // get the fingerprint 
      long urlfp = URLFingerprint.generate64BitURLFPrint(normalizedURL);
      // delegate to properly qualified method
      return checkCacheForItemInWorkerThread(normalizedURL,urlfp);
    }
    catch (MalformedURLException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      return null;
    }
  }

  private static long dateFromCacheItem(CacheItem cacheItem) { 
    NIOHttpHeaders headers = ArcFileItemUtils.buildHeaderFromArcFileItemHeaders(cacheItem.getHeaderItems());
    CrawlURLMetadata metadata = new CrawlURLMetadata();
    try {
      HttpHeaderInfoExtractor.parseHeaders(headers,metadata);
    } catch (IOException e) {
      LOG.error(CCStringUtils.stringifyException(e));
      return 0;
    }
    if (metadata.isFieldDirty(CrawlURLMetadata.Field_HTTPDATE)) { 
      return metadata.getHttpDate();
    }
    else if (metadata.isFieldDirty(CrawlURLMetadata.Field_LASTMODIFIEDTIME)) { 
      return metadata.getLastModifiedTime();
    }
    else if (metadata.isFieldDirty(CrawlURLMetadata.Field_EXPIRES)) {
      return metadata.getExpires();
    }
    return 0;
  }
  
  /** check cache via fingerprint - this call blocks and should not be used in an async context
   * 
   * @param urlFingerprint
   * @return true if a document with matching fingerprint exists in the cache ... 
   */
  public long checkCacheForFingerprint(long urlFingerprint,boolean returnDate) { 

     
    synchronized(this) { 
      for (CacheItem item : _writeQueue) { 
        if (item.getUrlFingerprint() == urlFingerprint) { 
          if (returnDate) { 
            long dateOut = dateFromCacheItem(item);
            // if no date found, use current date as an approximate...
            return (dateOut != 0) ? dateOut : System.currentTimeMillis();
          }
          else return 1;
        }
      }
    }
    
    synchronized (this) { 
      if (_fingerprintToLocalLogPos.get(urlFingerprint).size() != 0) {
        // assume recent date as an approximate 
        return System.currentTimeMillis();
      }
    }
    
    // now check hdfs indexes 
    ImmutableList<HDFSFileIndex> indexList = null;
    
    synchronized(CacheManager.this) {
      indexList = ImmutableList.copyOf(_hdfsIndexList);
    }

    long timeStart = System.currentTimeMillis();
    
    // first check local item cache ...
    TreeSet<Long> cachedItems = new TreeSet<Long>();
            
    for (HDFSFileIndex index : Lists.reverse(indexList)) { 
      try {
        CacheItem itemFound = index.findItem(urlFingerprint,!returnDate);
        if (itemFound != null) { 
          if (returnDate) {
            // get item date from headers .
            long itemDate = dateFromCacheItem(itemFound);
            if (itemDate == 0) { 
              itemDate = index.getIndexTimestamp();
              // if item date still 0, this is BAD !!!
              if (itemDate == 0) { 
                LOG.error("!!!!!!UNABLE TO PARSE INDEX TIMESTAMP:" + index.getIndexDataPath());
                itemDate = 1L;
              }
            }
            // ok add it to the map ... 
            cachedItems.add(itemDate);
          }
          else {
            return 1;
          }
        }
      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
      }
    }
    if (returnDate && cachedItems.size() != 0) { 
      return cachedItems.last();
    }
    return 0;
  }
  
  /**
   * check cache for url by fingerprint 
   * 
   * @param urlfingerprint
   * @param callback
   * @throws MalformedURLException
   */
  public void checkCacheForItem(final String normalizedURL,final long urlFingerprint,final CacheItemCheckCallback callback) {
  
    // first check local item cache ...
    CacheItem cachedItemOut = null;
     
    synchronized(this) { 
      for (CacheItem item : _writeQueue) { 
        if (item.getUrlFingerprint() == urlFingerprint) { 
          cachedItemOut = item;
          break;
        }
      }
    }
    // if found initiate immediate callback 
    if (cachedItemOut != null) { 
      queueCacheItemFoundCallback(callback,cachedItemOut);
    }
    else { 
      Long[] fpToItemCache = new Long[0];
      synchronized(this) { 
        // now check local cache first ... 
        fpToItemCache = _fingerprintToLocalLogPos.get(urlFingerprint).toArray(fpToItemCache);
      }
      
      if (fpToItemCache.length != 0) { 
        queueLocalCacheLoadRequest(new CacheLoadRequest(normalizedURL,fpToItemCache,callback));
      }
      else {
        // if not found ... check hdfs cache ... 
        queueHDFSCacheLoadRequest(new CacheLoadRequest(normalizedURL,urlFingerprint,callback));
      }
    }
  }
  
  /**
   * check cache for url by fingerprint 
   * 
   * @param urlfingerprint
   * @param callback
   * @throws MalformedURLException
   */
  public CacheItem checkCacheForItemInWorkerThread(final String normalizedURL,final long urlFingerprint) {
  
    // first check local item cache ...
    CacheItem cachedItemOut = null;
     
    synchronized(this) { 
      for (CacheItem item : _writeQueue) { 
        if (item.getUrlFingerprint() == urlFingerprint) { 
          cachedItemOut = item;
          break;
        }
      }
    }
    // if found initiate immediate callback 
    if (cachedItemOut != null) { 
      // callback.cacheItemAvailable(cachedItemOut.getUrl(), cachedItemOut);
      return cachedItemOut;
    }
    else { 
      Long[] fpToItemCache = new Long[0];
      synchronized(this) { 
        // now check local cache first ... 
        fpToItemCache = _fingerprintToLocalLogPos.get(urlFingerprint).toArray(fpToItemCache);
      }
      
      if (fpToItemCache.length != 0) { 
        return queueLocalCacheLoadRequestInWorkerThread(new CacheLoadRequest(normalizedURL,fpToItemCache,null));
      }
      else {
        // if not found ... check hdfs cache ... 
        return queueHDFSCacheLoadRequestInWorkerThread(new CacheLoadRequest(normalizedURL,urlFingerprint,null));
      }
    }
  }  
  
  /** 
   * cache this item
   * 
   * @param item - the item to cache 
   * @param optionalSemaphore an optional semaphore that will be passed back in completion callback when io operations complete
   */
  public void cacheItem(CacheItem item,Semaphore optionalSemaphore) {
    synchronized (this) {
      _writeQueue.add(item);
      _writeRequestQueue.add(new CacheWriteRequest(item.getUrlFingerprint(),item,optionalSemaphore));
    }
  }
  
  
  EventLoop getEventLoop() {
    return _eventLoop;
  }

  long getLocalLogFilePos() { 
    long filePosOut = 0;
    synchronized (_header) { 
      filePosOut = _header._fileSize;
    }
    return filePosOut;
  }
  
  byte[] getLocalLogSyncBytes() { 
    return _header._sync;
  }

  LinkedBlockingQueue<CacheWriteRequest> getWriteRequestQueue() {
    return _writeRequestQueue; 
  }

  File getLocalDataDirectory() {
    return _localDataDirectory;
  }
  
  Path getRemoteDataDirectory() { 
    return _remoteDataDirectory;
  }
  
  
  /** startCacheWriterThread
   * 
   */
  private void startCacheWriterThread() { 
    
  	_writerThreads = new Thread[CACHE_WRITER_THREADS];
  	for (int i=0;i<CACHE_WRITER_THREADS;++i) { 
  		_writerThreads[i] = new Thread(new CacheWriterThread(this));
  		_writerThreads[i].start();
  	}
  }
  
  
  /** startHDFSFlusherThread
   * 
   */
  private void startHDFSFlusherThread() { 
    
    _hdfsFlusherThread = new Thread(new HDFSFlusherThread(this));
    _hdfsFlusherThread.start();    
  }
  
  
  /**  load hdfs indexes 
   * 
   */
  private synchronized void loadHDFSIndexFiles() throws IOException { 
    //scan remote file system for index files ... 
    FileStatus indexFiles[] = getRemoteFileSystem().globStatus(new Path(getRemoteDataDirectory(),PROXY_CACHE_FILE_INDEX_PREFIX+"*"));
    // iterate files 
    for (FileStatus indexFile : indexFiles) { 
      LOG.info("Found Remote Index File:" + indexFile.getPath() + " Scanning for valid local copy");
      File localPath = new File(getLocalDataDirectory(),indexFile.getPath().getName());
      if (!localPath.exists() || localPath.length() != indexFile.getLen()) { 
        LOG.info("Local Index File:" + localPath.getAbsolutePath() + " Not Found. Copying...");
        getRemoteFileSystem().copyToLocalFile(indexFile.getPath(), new Path(localPath.getAbsolutePath()));
        LOG.info("Remote Index File:" + indexFile.getPath()  + " copied to:" + localPath.getAbsolutePath());
      }
      // extract timestamp from index name 
      long indexTimestamp = Long.parseLong(indexFile.getPath().getName().substring(PROXY_CACHE_FILE_INDEX_PREFIX.length() + 1));
      // construct data file path 
      Path remoteDataPath = new Path(getRemoteDataDirectory(),PROXY_CACHE_FILE_DATA_PREFIX +"-"+indexTimestamp);
      // now load the index ...
      LOG.info("Loading Index from:" + localPath.getAbsolutePath() + " Data Path:" + remoteDataPath);
      HDFSFileIndex indexObject = new HDFSFileIndex(getRemoteFileSystem(),localPath,remoteDataPath);
      LOG.info("Loaded Index from:" + localPath.getAbsolutePath());
      _hdfsIndexList.add(indexObject);
    }
  }
  
  
  // shrink the log file by the desired amount and update the header 
  private final void flushLocalLog(final long bytesToRemove,final int itemsToRemove,final List<FingerprintAndOffsetTuple> flushedTupleList,final ArrayList<IndexDataFileTriple> tempFileTriples) {
    
    
    LOG.info("Acquiring Log Access Semaphores");
    // first boost this thread's priority ... 
    int originalThreadPriority = Thread.currentThread().getPriority();
    Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
    // next acquire all permits to the local access log ... block until we get there ... 
    getLocalLogAccessSemaphore().acquireUninterruptibly(LOG_ACCESS_SEMAPHORE_COUNT);
    // now that we have all the semaphores we need, reduce the thread's priority to normal
    Thread.currentThread().setPriority(originalThreadPriority);
    LOG.info("Acquired ALL Log Access Semaphores");
    
    long timeStart = System.currentTimeMillis();
    
    // now we have exclusive access to the local transaction log ... 
    File activeLogFilePath = getActiveLogFilePath();
    File checkpointLogFilePath = getCheckpointLogFilePath();
    try {
      // delete checkpoint file if it existed ... 
      checkpointLogFilePath.delete();
      // now rename activelog to checkpoint path 
      activeLogFilePath.renameTo(checkpointLogFilePath);
      
      long logFileConsolidationStartTime = System.currentTimeMillis();
      // now trap for exceptions in case something fails 
      try { 
        // fix up the header ... 
        _header._fileSize -= bytesToRemove;
        _header._itemCount -= itemsToRemove;
        
        // open a old file and new file 
        RandomAccessFile newFile = new RandomAccessFile(activeLogFilePath,"rw");
        RandomAccessFile oldFile = new RandomAccessFile(checkpointLogFilePath,"r");
        
        LOG.info("Opened new and old files. New Header FileSize is:" + _header._fileSize + " ItemCount:" + _header._itemCount);
        try { 
          // write out header ...
          long bytesRemainingInLogFile = _header._fileSize;
          
          LOG.info("Writing Header to New File. Bytes Remaining for Data are:" + bytesRemainingInLogFile);
          // write header to new file ... 
          _header.writeHeader(newFile);
          // decrement bytes available ... 
          bytesRemainingInLogFile -= LocalLogFileHeader.SIZE;
          
          if (bytesRemainingInLogFile != 0) {
            byte transferBuffer[] = new byte[(1 << 20) * 16];
            LOG.info("Seeking old file past flushed data (pos:" + LocalLogFileHeader.SIZE + bytesToRemove + ")");
            // seek past old data ... 
            oldFile.seek(LocalLogFileHeader.SIZE + bytesToRemove);
            // and copy across remaining data 
            while (bytesRemainingInLogFile != 0) {
              int bytesToReadWriteThisIteration = Math.min((int)bytesRemainingInLogFile,transferBuffer.length);
              oldFile.read(transferBuffer, 0, bytesToReadWriteThisIteration);
              newFile.write(transferBuffer,0,bytesToReadWriteThisIteration);
              LOG.info("Copied " + bytesToReadWriteThisIteration + " from Old to New");
              bytesRemainingInLogFile -= bytesToReadWriteThisIteration;
            }
          }
        }
        finally {
          if (newFile != null) { 
            newFile.close();
          }
          if (oldFile != null) { 
            oldFile.close();
          }
        }
        // if we reached here then checkpoint was successfull ... 
        LOG.info("Checkpoint - Log Consolidation Successfull! TOOK:" + (System.currentTimeMillis() - logFileConsolidationStartTime));
        
        LOG.info("Loading Index Files");
        for (IndexDataFileTriple triple : tempFileTriples) { 
	        LOG.info("Loading Index File:" + triple._localIndexFilePath);
        	final HDFSFileIndex fileIndex = new HDFSFileIndex(_remoteFileSystem,triple._localIndexFilePath,triple._dataFilePath);
	        LOG.info("Loaded Index File");
	        // update hdfs index list ... 
	        synchronized (CacheManager.this) {
	          LOG.info("Adding HDFS Index to list");
	          _hdfsIndexList.addElement(fileIndex);
	        }
        }
        
        // create a semaphore to wait on 
        final Semaphore semaphore = new Semaphore(0);
        
        LOG.info("Scheduling Async Event");
        // now we need to schedule an async call to main thread to update data structures safely ... 
        _eventLoop.setTimer(new Timer(0,false,new Timer.Callback() {

          @Override
          public void timerFired(Timer timer) {
            LOG.info("Cleaning Map");
            
            synchronized (CacheManager.this) { 
              // walk tuples 
              for (FingerprintAndOffsetTuple tuple : flushedTupleList) {
                //TODO: HACK!
                // remove from collection ... 
                _fingerprintToLocalLogPos.removeAll(tuple._fingerprint);
              }
            }
            LOG.info("Increment Offset Info");
            // finally increment locallog offset by bytes removed ... 
            _localLogStartOffset += bytesToRemove;
            
            LOG.info("Releasing Wait Semaphore");
            //release wait sempahore 
            semaphore.release();
          } 
        }));
        
        LOG.info("Waiting for Async Event to Complete");
        //wait for async operation to complete ...
        semaphore.acquireUninterruptibly();
        
        LOG.info("Async Event to Completed");
      }
      catch (IOException e){ 
        LOG.error("Checkpoint Failed with Exception:" + CCStringUtils.stringifyException(e));
        // delete new file ... 
        activeLogFilePath.delete();
        // and rename checkpoint file to active file ... 
        checkpointLogFilePath.renameTo(activeLogFilePath);
      }
    }
    finally { 
      LOG.info("Releasing ALL Log Access Semaphores. HELD FOR:" + (System.currentTimeMillis() - timeStart));
      getLocalLogAccessSemaphore().release(LOG_ACCESS_SEMAPHORE_COUNT);
    }
  }
  
  /** called by hdfs log flusher thread when a cache flush is complete 
   * 
   * @param request
   * @param tupleListOut
   * @param localIndexFileName
   * @param remoteDataFileName
   * @throws IOException
   */
  void hdfsCacheFlushRequestComplete(CacheFlushRequest request,List<FingerprintAndOffsetTuple> tupleListOut,ArrayList<IndexDataFileTriple> tempFileList)throws IOException {
    // ok we have been called from the hdfs worker thread, and it is in the middle of a flush transaction
    // and it wants us to atomically update our cache based on its recent successful flush operation
    flushLocalLog(request._bytesToFlush,request._itemsToFlush,tupleListOut,tempFileList);
    
    _eventLoop.setTimer(new Timer(0,false,new Timer.Callback() {

      @Override
      public void timerFired(Timer timer) {
        // reset hdfs flusher thread variable 
        _hdfsFlusherActive = false;
      }
    }));
    
  }
  
  /** called by hdfs log flusher thread if a cache flush request fails
   * 
   * @param request
   */ 
  public void hdfsCacheFlushRequestFailed(CacheFlushRequest request) {

    LOG.error("HDFS Cache Flush Failed");

    _eventLoop.setTimer(new Timer(0,false,new Timer.Callback() {

      @Override
      public void timerFired(Timer timer) {
        // reset hdfs flusher thread variable 
        _hdfsFlusherActive = false;
      }
    }));
  }
  

  Semaphore getLocalLogAccessSemaphore() { 
    return _localLogAccessSempahore;
  }
  
  Semaphore getLocalLogWriteAccessSemaphore() { 
    return _localLogWriteAccessSemaphore;
  }
  
  FileSystem getRemoteFileSystem() {
    return _remoteFileSystem;
  }
  
  LinkedBlockingQueue<CacheFlushRequest> getHDFSFlushRequestQueue() {
    return _hdfsFlushRequestQueue;
  }

  void writeRequestFailed(final CacheWriteRequest request,final IOException e) {
    LOG.error("Failed to complete write request for Item:+ "+ request._item.getUrl()  + " with Exception:" + CCStringUtils.stringifyException(e));
    synchronized (CacheManager.this) { 
      // ok time to find this item in the write queue and move it to the long term position queue ...
      _writeQueue.remove(request._item);
    }
    // ok finally, if completion semaphore is set... release it  
    if (request._optionalSemaphore != null) { 
    	request._optionalSemaphore.release();
    }
  }

  void writeRequestComplete(final CacheWriteRequest request,final long absoluteFilePosition) {
    synchronized (CacheManager.this) { 
      // ok time to find this item in the write queue and move it to the long term position queue ...
      _writeQueue.remove(request._item);
      // now ...  push it into long term lookup map ... 
      _fingerprintToLocalLogPos.put(request._itemFingerprint,_localLogStartOffset + absoluteFilePosition);
    }
    // ok finally, if completion semaphore is set... release it  
    if (request._optionalSemaphore != null) { 
    	request._optionalSemaphore.release();
    }
  }
  
  
  /**
   * queueCacheItemFoundCallback - helper method used to dispatch async callback
   * 
   * @param callback
   * @param itemFound
   */
  private void queueCacheItemFoundCallback(final CacheItemCheckCallback callback,final CacheItem itemFound) { 
    _eventLoop.setTimer(new Timer(0,false,new Timer.Callback() {

      @Override
      public void timerFired(Timer timer) {
        callback.cacheItemAvailable(itemFound.getUrl(), itemFound);
      } 
    }));
  }

  /**
   * queueCacheItemNotFoundCallback - helper method used to dispatch async callback
   * 
   * @param callback
   * @param itemFound
   */
  
  private void queueCacheItemNotFoundCallback(final CacheItemCheckCallback callback,final String url) { 
    _eventLoop.setTimer(new Timer(0,false,new Timer.Callback() {

      @Override
      public void timerFired(Timer timer) {
        callback.cacheItemNotFound(url);
      } 
    }));
  }
  
  
  /**
   * loadCacheItemFromDisk - load a single cache item from disk 
   * 
   * @param file
   * @param optTargetURL
   * @param location
   * @return
   * @throws IOException
   */    
  private CacheItem loadCacheItemFromDisk(FileInputStream file,String optTargetURL,long location) throws IOException {
    
    long timeStart = System.currentTimeMillis();
    
    // and read out the Item Header ...  
    CacheItemHeader itemHeader= new CacheItemHeader();
    itemHeader.readHeader(new DataInputStream(file));
    // see if it is valid ... 
    if (!Arrays.equals(itemHeader._sync, _header._sync)) { 
      LOG.error("### Item Lookup for URL:" + optTargetURL + " Record at:" + location + " failed - corrupt sync bytes detected!!!");
    }
    else { 
      CRC32 crc32 = new CRC32();
      // ok deserialize the bytes ... 
      CacheItem item = new CacheItem();
      CheckedInputStream checkedStream = new CheckedInputStream(file,crc32);
      DataInputStream itemStream = new DataInputStream(checkedStream);
      item.readFields(itemStream);
      // read the content buffer length 
      int contentBufferLen = itemStream.readInt();
      if (contentBufferLen != 0) { 
        byte data[] = new byte[contentBufferLen];
        itemStream.read(data);
        item.setContent(new Buffer(data));
      }
      
      // cache crc 
      long crcValueComputed = crc32.getValue();
      // read disk crc 
      long crcValueOnDisk = itemStream.readLong();
      // validate 
      if (crcValueComputed == crcValueOnDisk) { 
        String canonicalURL = URLUtils.canonicalizeURL(item.getUrl(),true);
        if (optTargetURL.length() == 0 || optTargetURL.equals(canonicalURL)) {
          if (isValidCacheItem(item)) { 
            LOG.info("### Item Lookup for URL:" + optTargetURL + " Record at:" + location + " completed in:" + (System.currentTimeMillis()-timeStart));
            return item;
          }
          else { 
            LOG.info("### Item Lookup for URL:" + optTargetURL + " Record at:" + location + " failed with invalid result code");
          }
          
        }
        else { 
          LOG.info("### Item Lookup for URL:" + optTargetURL + " Record at:" + location+ " failed with url mismatch. record url:" + item.getUrl());
        }
      }
      else { 
        LOG.error("### Item Lookup for URL:" + optTargetURL + " Record at:" + location + " failed - crc mismatch!!!");
      }
    }
    return null;
  }
  
  /** is this a valid cache item for servicing a query request 
   * 
   */
  private static boolean isValidCacheItem(CacheItem item) {
    
    if ((item.getFlags() & (CacheItem.Flags.Flag_IsPermanentRedirect|CacheItem.Flags.Flag_IsTemporaryRedirect)) != 0) { 
      return true;
    }
    // parse response code in headers ... 
    CrawlURLMetadata metadata = new CrawlURLMetadata();
    
    HttpHeaderInfoExtractor.parseStatusLine(item.getHeaderItems().get(0).getItemValue(), metadata);
    
    if (metadata.isFieldDirty(CrawlURLMetadata.Field_HTTPRESULTCODE)) {
      if (metadata.getHttpResultCode() == 200 /*|| (metadata.getHttpResultCode()>=400 && metadata.getHttpResultCode() <500)*/) {  
        return true;
      }
      else { 
        LOG.info("Rejecting Cache Item:" + item.getUrl() + " With Invalid Result Code:" + metadata.getHttpResultCode() 
            + " ActualValue:" + item.getHeaderItems().get(0).getItemValue());
      }
    }
    return false;
  }
  
  /** queue up an hdfs cache load request
   * 
   */
  private void queueHDFSCacheLoadRequest(final CacheLoadRequest loadRequest) { 
    
    _hdfsLoaderPool.submit(new ConcurrentTask<CacheItem>(_eventLoop,new Callable<CacheItem>() {

      @Override
      public CacheItem call() throws Exception {
        
        LOG.info("Executing HDFS Index Search Thread for URL:" + loadRequest._targetURL);
        
        ImmutableList<HDFSFileIndex> indexList = null;
        
        synchronized(CacheManager.this) {
          long timeStart = System.currentTimeMillis();
          indexList = ImmutableList.copyOf(_hdfsIndexList);
          long timeEnd = System.currentTimeMillis();
          // LOG.info("#### TIMER - indexList Copy Took:" + (timeEnd-timeStart));
        }
        long timeStart = System.currentTimeMillis();
        LOG.info("Starting Search of:" + indexList.size() + " hdfs indexes for fp:" + loadRequest._fingerprint);        
        for (HDFSFileIndex index : Lists.reverse(indexList)) { 
          CacheItem item = index.findItem(loadRequest._fingerprint,false);
          
          if (item != null) { 
            LOG.info("Found Hit for fingerprint:" + loadRequest._fingerprint + " URL:" + item.getUrl() + " IN:" + (System.currentTimeMillis() - timeStart));
            return item;
          }
        }
        LOG.info("FAILED TO FIND Hit during for fingerprint:" + loadRequest._fingerprint + " IN:" + (System.currentTimeMillis() - timeStart));
        return null;
      } 
      
    },
    new CompletionCallback<CacheItem>() {

      @Override
      public void taskComplete(CacheItem loadResult) {
        if (loadResult != null && isValidCacheItem(loadResult)) {
          LOG.info("### Item Load Request for URL:" + loadRequest._targetURL + " Succeeded. Initiating Callback");
          // reset pending to zero so no other load requests satisfy callback 
          loadRequest._callback.cacheItemAvailable(loadRequest._targetURL,loadResult);
        }
        else{ 
          LOG.info("### Item Load Request for URL:" + loadRequest._targetURL + " Failed with No-Item-Found");
          // if pending zero ... initiate failure callback 
          loadRequest._callback.cacheItemNotFound(loadRequest._targetURL);
        }        
      }

      @Override
      public void taskFailed(Exception e) {
        LOG.info("### Item Load Request for URL:" + loadRequest._targetURL + " Failed with FAILURE Reason:" + CCStringUtils.stringifyException(e));
        // if pending zero ... initiate failure callback 
        loadRequest._callback.cacheItemNotFound(loadRequest._targetURL);
      }
      
    }));
  }
  
  /** queue up an hdfs cache load request
   * 
   */
  private CacheItem queueHDFSCacheLoadRequestInWorkerThread(final CacheLoadRequest loadRequest) { 
    
    // LOG.info("#### ENTERING queueHDFSCacheLoadRequestInWorkerThread");
    
    try { 
      CacheItem loadResult = null;
          
      // LOG.info("Executing HDFS Index Search Thread for URL:" + loadRequest._targetURL);
      
      ImmutableList<HDFSFileIndex> indexList = null;
      
      synchronized(CacheManager.this) {
        indexList = ImmutableList.copyOf(_hdfsIndexList);
      }
      long timeStart = System.currentTimeMillis();
  
      // LOG.info("Starting Search of:" + indexList.size() + " hdfs indexes for fp:" + loadRequest._fingerprint);        
      for (HDFSFileIndex index : Lists.reverse(indexList)) { 
        CacheItem item = index.findItem(loadRequest._fingerprint,false);
        
        if (item != null) { 
          LOG.info("Found Hit for fingerprint:" + loadRequest._fingerprint + " URL:" + item.getUrl() + " IN:" + (System.currentTimeMillis() - timeStart));
          loadResult = item;
          break;
        }
      }
      LOG.info("FAILED TO FIND Hit during for fingerprint:" + loadRequest._fingerprint + " IN:" + (System.currentTimeMillis() - timeStart));
      
      if (loadResult != null && isValidCacheItem(loadResult)) {
        // LOG.info("### Item Load Request for URL:" + loadRequest._targetURL + " Succeeded. Initiating Callback");
        // reset pending to zero so no other load requests satisfy callback 
        // loadRequest._callback.cacheItemAvailable(loadRequest._targetURL,loadResult);
        return loadResult;
      }
      else{ 
        // LOG.info("### Item Load Request for URL:" + loadRequest._targetURL + " Failed with No-Item-Found");
        // if pending zero ... initiate failure callback 
        //loadRequest._callback.cacheItemNotFound(loadRequest._targetURL);
      }
    }
    catch (Exception e) {
      LOG.info("### Item Load Request for URL:" + loadRequest._targetURL + " Failed with FAILURE Reason:" + CCStringUtils.stringifyException(e));
      // if pending zero ... initiate failure callback 
      // loadRequest._callback.cacheItemNotFound(loadRequest._targetURL);
    }
    finally { 
      //LOG.info("#### EXITING queueHDFSCacheLoadRequestInWorkerThread");
    }
    return null;
  }
    
  
  /**
   * queue a cache load request via a background thread 
   * 
   * @param loadRequest
   */
  private void queueLocalCacheLoadRequest(final CacheLoadRequest loadRequest) { 
    // queue up requests into the thread pool executor (for now)
    for (final Long location : loadRequest._loacations) { 
      
      _cacheLoadThreadPool.submit(new ConcurrentTask<CacheItem>(_eventLoop, new Callable<CacheItem>() {

        @Override
        public CacheItem call() throws Exception {

          //LOG.info("### Local Cache Loader Called. Acquiring Semaphore");
          getLocalLogAccessSemaphore().acquireUninterruptibly();
          //LOG.info("### Local Cache Loader Called. Acquired Semaphore");
          
          // now set up and exception handler block to ensure that we release semaphore
          try { 
            //LOG.info("### Item Loading Item for URL:" + loadRequest._targetURL + " at Pos:" + location.longValue());
            
            // now that we have acquire the semaphore ... validate position against current log file offset ...
            if (location < _localLogStartOffset) {
              LOG.error("### Load Request for Potentially Flushed Item. Location Request:" + location + " Current LogStartOffset:" + _localLogStartOffset);
            }
            else { 
              long timeStart = System.currentTimeMillis();
              
              // we got a location ... initiate a disk read to fetch the serialized CacheItem
              FileInputStream file = new FileInputStream(getActiveLogFilePath());
              
              try { 
                // seek to item location ...
                file.skip(location.longValue() - _localLogStartOffset);
                return loadCacheItemFromDisk(file,loadRequest._targetURL,location.longValue());
              }
              catch (IOException e) { 
                LOG.error(CCStringUtils.stringifyException(e));
              }
              finally {
                // file.getFD().sync();
                file.close();
              }
            }
            return null;
          }
          finally { 
            //LOG.info("### Local Cache Loader Releasing Semaphore");
            getLocalLogAccessSemaphore().release();
          }
        } 
        
      },new CompletionCallback<CacheItem>() {

        @Override
        public void taskComplete(CacheItem loadResult) {
          if (loadResult != null) {
            // LOG.info("### Item Load Request for URL:" + loadRequest._targetURL + " Succeeded. Initiating Callback");
            // reset pending to zero so no other load requests satisfy callback 
            loadRequest._pendingItemCount = 0;
            loadRequest._callback.cacheItemAvailable(loadRequest._targetURL,loadResult);
          }
          else{ 
            // on failure reduce pending count ... 
            loadRequest._pendingItemCount--;
            
            if (loadRequest._pendingItemCount == 0) {
              // LOG.info("### Item Load Request for URL:" + loadRequest._targetURL + " Failed with No-Item-Found");
              // if pending zero ... initiate failure callback 
              loadRequest._callback.cacheItemNotFound(loadRequest._targetURL);
            }
          }
        }

        @Override
        public void taskFailed(Exception e) {
          // on failure reduce pending count ... 
          loadRequest._pendingItemCount--;
          
          if (loadRequest._pendingItemCount == 0) {
            // LOG.info("### Item Load Request for URL:" + loadRequest._targetURL + " Failed with FAILURE Reason:" + CCStringUtils.stringifyException(e));
            // if pending zero ... initiate failure callback 
            loadRequest._callback.cacheItemNotFound(loadRequest._targetURL);
          }
        }
        
      }));
          
          
    }
  }

  /**
   * queue a cache load request via a background thread 
   * 
   * @param loadRequest
   */
  private CacheItem queueLocalCacheLoadRequestInWorkerThread(final CacheLoadRequest loadRequest) {
    // LOG.info("#### ENTERING queueLocalCacheLoadRequestInWorkerThread");
    try { 
      CacheItem loadResult = null;
      
      // queue up requests into the thread pool executor (for now)
      for (final Long location : loadRequest._loacations) { 
        
          LOG.info("### Local Cache Loader Called. Acquiring Semaphore");
          getLocalLogAccessSemaphore().acquireUninterruptibly();
          LOG.info("### Local Cache Loader Called. Acquired Semaphore");
            
          // now set up and exception handler block to ensure that we release semaphore
          try { 
            // LOG.info("### Item Loading Item for URL:" + loadRequest._targetURL + " at Pos:" + location.longValue());
            
            // now that we have acquire the semaphore ... validate position against current log file offset ...
            if (location < _localLogStartOffset) {
              LOG.error("### Load Request for Potentially Flushed Item. Location Request:" + location + " Current LogStartOffset:" + _localLogStartOffset);
            }
            else { 
              long timeStart = System.currentTimeMillis();
              
              // we got a location ... initiate a disk read to fetch the serialized CacheItem
              FileInputStream file = new FileInputStream(getActiveLogFilePath());
              
              try { 
                // seek to item location ...
                file.skip(location.longValue() - _localLogStartOffset);
                loadResult = loadCacheItemFromDisk(file,loadRequest._targetURL,location.longValue());
                if (loadResult != null) { 
                  break;
                }
              }
              catch (IOException e) { 
                LOG.error(CCStringUtils.stringifyException(e));
              }
              finally {
                // file.getFD().sync();
                file.close();
              }
            }
          }
          finally { 
            LOG.info("### Local Cache Loader Releasing Semaphore");
            getLocalLogAccessSemaphore().release();
          }
        }
      
        if (loadResult != null) {
          LOG.info("### Item Load Request for URL:" + loadRequest._targetURL + " Succeeded. Initiating Callback");
          // reset pending to zero so no other load requests satisfy callback 
          loadRequest._pendingItemCount = 0;
          // loadRequest._callback.cacheItemAvailable(loadRequest._targetURL,loadResult);
          return loadResult;
        }
        else{ 
          // on failure reduce pending count ... 
          loadRequest._pendingItemCount--;
          
          if (loadRequest._pendingItemCount == 0) {
            LOG.info("### Item Load Request for URL:" + loadRequest._targetURL + " Failed with No-Item-Found");
            // if pending zero ... initiate failure callback 
            // loadRequest._callback.cacheItemNotFound(loadRequest._targetURL);
            return null;
          }
        }
    }
    catch (Exception e) { 
      LOG.info("### Item Load Request for URL:" + loadRequest._targetURL + " Failed with FAILURE Reason:" + CCStringUtils.stringifyException(e));
      // if pending zero ... initiate failure callback 
      //loadRequest._callback.cacheItemNotFound(loadRequest._targetURL);
    }
    finally { 
      //LOG.info("#### EXITING queueLocalCacheLoadRequestInWorkerThread");
    }
    return null;
  }
  
  /** 
   * normalize a url to be canonical 
   * 
   * @param url
   * @return
   * @throws MalformedURLException
   */
  public static String normalizeURL(String url) throws MalformedURLException { 
    return URLUtils.canonicalizeURL(url,true);
  }
  
  
  /** 
   * get fingerprint given url 
   */
  public static long getFingerprintGivenURL(String url) throws MalformedURLException { 
    // get the normalized url 
    String normalizedURL = normalizeURL(url);
    // get the fingerprint 
    return URLFingerprint.generate64BitURLFPrint(normalizedURL);
  }

  

  

  /** 
   * getLogFilePath - get active log file path 
   * 
   * @param directoryRoot
   * @return
   */
  File getActiveLogFilePath() { 
    return new File(_localDataDirectory,"ActiveLog"); 
  }
  
  File getCheckpointLogFilePath() { 
    return new File(_localDataDirectory,"Checkpoint"); 
  }
  
  
  /**
   * updateLogFileHeader - update the log file header 
   *    called via the log file writer thread ...
   * @throws IOException
   */
  void updateLogFileHeader(File logFileName, long newlyAddedItemsCount, long newItemsFileSize)throws IOException {
    
    RandomAccessFile file = new RandomAccessFile(logFileName,"rw");
    
    try { 
      
      synchronized(_header) { 
        // update cached header ... 
        _header._fileSize += newItemsFileSize;
        _header._itemCount += newlyAddedItemsCount;
        // set the position at zero .. 
        file.seek(0);
        // and write header to disk ... 
        _header.writeHeader(file);
      }
    }
    finally {
      // major bottle neck.. 
      // file.getFD().sync();
      file.close();
    }
  }
  
  /**
   * initializeEmptyLogFile - init an empty log file header
   * 
   * @param stream
   * @return
   * @throws IOException
   */
  private static LocalLogFileHeader initializeEmptyLogFile(DataOutput stream) throws IOException { 
    
    LocalLogFileHeader header = new LocalLogFileHeader();
    header.writeHeader(stream);
    
    return header;
  }
  
  /**
   * readLogFileHeader - from File
   * 
   * @param logFileName
   * @return
   * @throws IOException
   */
  private static LocalLogFileHeader readLogFileHeader(File logFileName) throws IOException {
    
    LocalLogFileHeader headerOut = new LocalLogFileHeader();
    RandomAccessFile file = new RandomAccessFile(logFileName,"r");
    try { 
      headerOut = readLogFileHeader(file);
    }
    finally { 
      file.close();
    }
    return headerOut;
  }
  
  /**
   * readLogFileHeader - from Stream
   * 
   * @param reader
   * @return
   * @throws IOException
   */
  private static LocalLogFileHeader readLogFileHeader(DataInput reader)throws IOException { 
    
    LocalLogFileHeader headerOut = new LocalLogFileHeader();
    
    headerOut.readHeader(reader);

    return headerOut;
  }  
  
  /** initiailizeActiveLog - init local cache log 
   * 
   * 
   * **/
  private void initializeActiveLog()throws IOException { 
    
    File activeLogPath = getActiveLogFilePath();
    
    if (!activeLogPath.exists()) { 
      DataOutputStream outputStream = new DataOutputStream(new FileOutputStream(activeLogPath));
      try {
        _header = initializeEmptyLogFile(outputStream);
      }
      finally { 
        outputStream.close();
      }
    }
    else { 
      _header = new LocalLogFileHeader();

      DataInputStream inputStream = new DataInputStream(new FileInputStream(activeLogPath));
      
      try { 
        _header.readHeader(inputStream);
      }
      finally { 
        inputStream.close();
      }
      
      if (_header._itemCount != 0) { 
        loadCache(activeLogPath,_header);
      }
    }
  }
  
  /**
   * loadCache - load local cache from disk 
   * @param activeLogPath
   * @param logFileHeader
   * @throws IOException
   */
  private synchronized void loadCache(File activeLogPath,LocalLogFileHeader logFileHeader) throws IOException { 
    
    RandomAccessFile file = new RandomAccessFile(getActiveLogFilePath(),"rw");
    
    byte [] syncCheck = new byte[_header._sync.length];
    
    
    try {

      long lastValidPos = LocalLogFileHeader.SIZE; 
      long currentPos   = lastValidPos; 
      long endPos     = file.length();
      
      
      CacheItemHeader itemHeader = new CacheItemHeader();
      
      // start read 
      while (currentPos < endPos) {

        if ((endPos - currentPos) < LocalLogFileHeader.SYNC_BYTES_SIZE) 
          break;
        
        // seek to current position ... 
        file.seek(currentPos);
        
        boolean headerLoadFailed = false;
        
        try { 
          // read the item header ... assuming things are good so far ... 
          itemHeader.readHeader(file);
        }
        catch (IOException e) { 
          LOG.error("### Item Header Load Failed With Exception:" + CCStringUtils.stringifyException(e));
          headerLoadFailed = true;
        }
        
        if (headerLoadFailed) {
          LOG.error("### Item File Corrupt at position:" + currentPos +" Seeking Next Sync Point");
          currentPos += LocalLogFileHeader.SYNC_BYTES_SIZE;
        }
        
        // if header sync bytes don't match .. then seek to next sync position ... 
        if (headerLoadFailed || !Arrays.equals(itemHeader._sync, _header._sync)) {

          LOG.error("### Item File Corrupt at position:" + currentPos +" Seeking Next Sync Point");
          
          // reseek to current pos 
          file.seek(currentPos);
          // read in a sync.length buffer amount 
          file.readFully(syncCheck);
        
          int syncLen = _header._sync.length;
        
          // start scan for next sync position ...
          for (int i = 0; file.getFilePointer() < endPos; i++) {
            int j = 0;
            for (; j < syncLen; j++) {
              if (_header._sync[j] != syncCheck[(i+j)%syncLen])
                break;
            }
            if (j == syncLen) {
              file.seek(file.getFilePointer() - LocalLogFileHeader.SYNC_BYTES_SIZE);     // position before sync
              break;
            }
            syncCheck[i%syncLen] = file.readByte();
          }
          // whatever, happened file pointer is at current pos 
          currentPos = file.getFilePointer();
          
          if (currentPos < endPos) { 
            LOG.info("### Item Loader Found another sync point at:" + currentPos);
          }
          else { 
            LOG.error("### No more sync points found!");
          }
        }
        else { 
          // ok figure out next steps based on header ... 
          // for now, just add item to our list ... 
          _fingerprintToLocalLogPos.put(itemHeader._fingerprint,_localLogStartOffset + currentPos);
          // now seek past data
          currentPos += CacheItemHeader.SIZE + itemHeader._dataLength + ITEM_RECORD_TRAILING_BYTES; 
        }
      }
    }
    finally { 
      if (file != null) { 
        file.close();
      }
    }
  }
  
  
  public static long readVLongFromByteBuffer(ByteBuffer source) { 
    byte firstByte = source.get();
    int len = WritableUtils.decodeVIntSize(firstByte);
    if (len == 1) {
      return firstByte;
    }
    long i = 0;
    for (int idx = 0; idx < len-1; idx++) {
      byte b = source.get();
      i = i << 8;
      i = i | (b & 0xFF);
    }
    return (WritableUtils.isNegativeVInt(firstByte) ? (i ^ -1L) : i);
  }
  
  public static void writeVLongToByteBuffer(ByteBuffer stream, long i) throws IOException {
    if (i >= -112 && i <= 127) {
      stream.put((byte)i);
      return;
    }
      
    int len = -112;
    if (i < 0) {
      i ^= -1L; // take one's complement'
      len = -120;
    }
      
    long tmp = i;
    while (tmp != 0) {
      tmp = tmp >> 8;
      len--;
    }
      
    stream.put((byte)len);
      
    len = (len < -120) ? -(len + 120) : -(len + 112);
      
    for (int idx = len; idx != 0; idx--) {
      int shiftbits = (idx - 1) * 8;
      long mask = 0xFFL << shiftbits;
      stream.put((byte)((i & mask) >> shiftbits));
    }
  }  
  
  /********************************************************************************************************/
  // TEST CODE 
  /********************************************************************************************************/
  
  
  
  
    
  public static void main(String[] args) {    

    final EventLoop eventLoop = new EventLoop();
    eventLoop.start();
    
      
    final CacheManager manager = new CacheManager(eventLoop);
    // delete active log if it exists ... 
    manager.getActiveLogFilePath().delete();
    try {
      manager.initialize(INIT_FLAG_SKIP_CACHE_WRITER_INIT | INIT_FLAG_SKIP_HDFS_WRITER_INIT);
    } catch (IOException e1) {
      LOG.error(CCStringUtils.stringifyException(e1));
      return;
    }
    
    
    MessageDigest digester;
    try {
      digester = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e1) {
      LOG.error(CCStringUtils.stringifyException(e1));
      return;
    }
    
    final byte [] randomBytes = new byte[1 << 15];
    LOG.info("Building Random Digest");
    for (int i=0;i<randomBytes.length;i+=16) { 
      long time = System.nanoTime();
      digester.update((new UID()+"@"+time).getBytes());
      System.arraycopy(digester.digest(),0,randomBytes,i,16);
    }
    
      
    final Semaphore semaphore = new Semaphore(0);

    if (args[0].equals("populate")) {
      
      manager.startCacheWriterThread();
      manager.startHDFSFlusherThread();
      
      try { 

        
        LOG.info("Done Building Random Digest");
        
        LOG.info("Writing Items To Disk");
        for (int i=0;i<1000000;++i) { 
          
          if (i % 1000 == 0) { 
            LOG.info("Wrote:" + i + " entries");
          }
          
          final CacheItem item1 = new CacheItem();
          item1.setUrl(manager.normalizeURL("http://www.domain.com/foobar/" + i));
          item1.setContent(new Buffer(randomBytes));
          item1.setUrlFingerprint(URLFingerprint.generate64BitURLFPrint(item1.getUrl()));
          manager.cacheItem(item1,null);
          Thread.sleep(1);
          
          if (i != 0 && i % 10000 == 0) { 
            LOG.info("Hit 10000 items.. sleeping for 20 seconds");
            Thread.sleep(20 * 1000);
          }
        }
    
        Thread.sleep(30000);
    
        for (int i=0;i<1000000;++i) { 
          
          final String url = new String("http://www.domain.com/foobar/"+i);
          manager.checkCacheForItem(url, new CacheItemCheckCallback() {
      
            @Override
            public void cacheItemAvailable(String url, CacheItem item) {
              Assert.assertTrue(item.getUrl().equals(url));
              String itemIndex = url.substring("http://www.domain.com/foobar/".length());
              int itemNumber = Integer.parseInt(itemIndex);
              if (itemNumber == 999999) { 
                semaphore.release();
              }
            }
      
            @Override
            public void cacheItemNotFound(String url) {
              Assert.assertTrue(false);
            } 
          });
        }
      }
      catch (IOException e) { 
        LOG.error(CCStringUtils.stringifyException(e));
      }
      catch (InterruptedException e2) { 
        
      }
    }
    else if (args[0].equals("read")) { 
      
      try { 
        final CacheItem item1 = new CacheItem();
        item1.setUrl(manager.normalizeURL("http://www.domain.com/barz/"));
        item1.setUrlFingerprint(URLFingerprint.generate64BitURLFPrint(item1.getUrl()));
        item1.setContent(new Buffer(randomBytes));
        manager.cacheItem(item1,null);      
        
        // queue up cache load requests .... 
        for (int i=0;i<10000;++i) {
          
          final String url = new String("http://www.domain.com/foobar/"+i);

          eventLoop.setTimer(new Timer(1,false,new Timer.Callback() {

            @Override
            public void timerFired(Timer timer) {
              manager.checkCacheForItem(url, new CacheItemCheckCallback() {

                @Override
                public void cacheItemAvailable(String url, CacheItem item) {
                  LOG.info("FOUND Item for URL:" + url + " ContentSize:" + item.getContent().getCount());
                }

                @Override
                public void cacheItemNotFound(String url) {
                  LOG.info("DIDNOT Find Item for URL:" + url);
                } 
                
              });
            } 
          }));
        }
        
        eventLoop.setTimer(new Timer(1,false,new Timer.Callback() {

          @Override
          public void timerFired(Timer timer) {
            manager.checkCacheForItem(item1.getUrl(), new CacheItemCheckCallback() {

              @Override
              public void cacheItemAvailable(String url, CacheItem item) {
                LOG.info("FOUND Item for URL:" + url + " ContentSize:" + item.getContent().getCount());
              }

              @Override
              public void cacheItemNotFound(String url) {
                LOG.info("DIDNOT Find Item for URL:" + url);
              } 
              
            });
          } 
          
        }));
      }
      catch (IOException e) { 
        LOG.error(CCStringUtils.stringifyException(e));
      }
    }
    semaphore.acquireUninterruptibly();

  }
  
  
  
}
