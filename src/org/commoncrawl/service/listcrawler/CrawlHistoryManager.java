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


import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.SequenceFile.ValueBytes;
import org.apache.hadoop.record.Buffer;
import org.apache.log4j.BasicConfigurator;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.mapred.ProxyCrawlHistoryItem;
import org.commoncrawl.protocol.CrawlURL;
import org.commoncrawl.protocol.URLFP;
import org.commoncrawl.rpc.base.shared.BinaryProtocol;
import org.commoncrawl.service.crawler.util.URLFPBloomFilter;
import org.commoncrawl.service.listcrawler.CrawlListMetadata;
import org.commoncrawl.util.URLUtils;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.CRC16;
import org.commoncrawl.util.FileUtils;
import org.junit.Assert;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/** 
 * Class that encapsulates the state necessary to manage long term crawl history
 * 
 * @author rana
 *
 */
public class CrawlHistoryManager implements CrawlHistoryStorage {

  public static final Log LOG = LogFactory.getLog(CrawlHistoryManager.class);

  private static class QueueItem<Type> {

    public QueueItem(Type item) {
      _item = item;
    }

    public Type _item;
  }

  public static interface ItemUpdater {
    public void updateItemState(URLFP fingerprint, ProxyCrawlHistoryItem item)
        throws IOException;
  }

  /** path to log file directory **/
  File                                      _localLogFileDir;
  /** remote data directory **/
  Path                                      _remoteDataDirectory;
  /** event loop **/
  EventLoop                                 _eventLoop;
  /** file system object **/
  FileSystem                                _remoteFileSystem                       = null;
  /** queue of pending crawl history updates **/
  LinkedBlockingQueue<HistoryUpdateRequest> _historyUpdateQueue                     = new LinkedBlockingQueue<HistoryUpdateRequest>();
  /** list loader queue **/
  LinkedBlockingQueue<QueueItem<CrawlList>> _listLoaderQueue                        = new LinkedBlockingQueue<QueueItem<CrawlList>>();
  /** crawl queue **/
  LinkedBlockingQueue<QueueItem<CrawlList>> _queueLoaderQueue                       = new LinkedBlockingQueue<QueueItem<CrawlList>>();
  /** shutdown flag **/
  boolean                                   _shutdown                               = false;
  /** queue loader shutdown flag **/
  boolean                                   _queueLoaderShutdown                    = false;

  /** cache writer thread **/
  Thread                                    _writerThread;
  /** list loader thread **/
  Thread                                    _listLoaderThread;
  /** queue loader thread **/
  Thread                                    _queueLoaderThread;
  /** checkpoint access semaphore **/
  Semaphore                                 _checkpointSemaphore                    = new Semaphore(
                                                                                        1);
  /** last checkpoint time **/
  long                                      _lastCheckpointTime                     = -1;
  /** crc16 - used to calculate individual payload crcs **/
  private CRC16                             _crc16in                                = new CRC16();
  /** buffer used to store sync byte data during payload scan **/
  private byte                              _syncByteBuffer[]                       = new byte[LocalLogFileHeader.SYNC_BYTES_SIZE];
  /** payload Buffer object used to accumulate payload data for writes **/
  Buffer                                    _payloadBuffer                          = new Buffer();
  /** data input buffer reused to read payload data **/
  DataInputBuffer                           _payloadInputStream                     = new DataInputBuffer();
  /** lists **/
  TreeMap<Long, CrawlList>                  _crawlLists                             = new TreeMap<Long, CrawlList>();

  public static final String                CRAWL_HISTORY_HDFS_LOGFILE_PREFIX       = "historyData-";
  public static final String                CRAWL_HISTORY_HDFS_BLOOMFILTER_PREFIX   = "historyBloomFilter-";

  /** log file header **/
  private LocalLogFileHeader                _header                                 = new LocalLogFileHeader();
  /** local log item map **/
  TreeMap<URLFP, ProxyCrawlHistoryItem>     _localLogItems                          = new TreeMap<URLFP, ProxyCrawlHistoryItem>();

  public static final int                   INIT_FLAG_SKIP_ACTIVE_LOG_FILE_INIT     = 1;
  public static final int                   INIT_FLAG_SKIP_LOAD_EXISTING_LISTS      = 2;
  public static final int                   INIT_FLAG_SKIP_LOG_WRITER_THREAD_INIT   = 4;
  public static final int                   INIT_FLAG_SKIP_LIST_LOADER_THREAD_INIT  = 8;
  public static final int                   INIT_FLAG_DISABLE_CHECKPOINTS           = 16;

  private static final int                  LOG_ITEM_HEADER_SIZE                    = LocalLogFileHeader.SYNC_BYTES_SIZE + 4 + 2;
  private static final int                  POLL_WAIT_TIME                          = 5000;
  public static final int                   DEFAULT_LOCAL_ITEM_CHECKPOINT_THRESHOLD = 100000;

  private static int                        _checkpointThreshold                    = DEFAULT_LOCAL_ITEM_CHECKPOINT_THRESHOLD;

  /**
   * constuctor
   * 
   * @param logFileDir
   *          the path to the local log file directory
   * @throws IOException
   */
  public CrawlHistoryManager(FileSystem remoteFileSystem,
      Path remoteLogFileDir, File localLogFileDir, EventLoop eventLoop,
      int initFlags) throws IOException {

    this._eventLoop = eventLoop;
    this._remoteFileSystem = remoteFileSystem;
    this._remoteDataDirectory = remoteLogFileDir;
    this._localLogFileDir = localLogFileDir;

    LOG.info("*** LOCAL DATA DIR:" + _localLogFileDir);

    initialize(initFlags);
  }

  private void initialize(int initFlags) throws IOException {

    // initialize the local log file ...
    if ((initFlags & INIT_FLAG_SKIP_ACTIVE_LOG_FILE_INIT) == 0) {
      initializeActiveLog();
    }

    // load pre-existing lists
    if ((initFlags & INIT_FLAG_SKIP_LOAD_EXISTING_LISTS) == 0) {
      loadExistingLists();
    }

    // start log writer thread ...
    if ((initFlags & INIT_FLAG_SKIP_LOG_WRITER_THREAD_INIT) == 0) {
      startLogWriterThread(initFlags);
    }

    // start list loader thread ...
    if ((initFlags & INIT_FLAG_SKIP_LIST_LOADER_THREAD_INIT) == 0) {
      startListLoaderThread();
    }
  }

  /**
   * shutdown the log properly
   */
  public void shutdown() {
    _shutdown = true;

    stopQueueLoaderThread();

    _historyUpdateQueue.add(new HistoryUpdateRequest());
    _listLoaderQueue.add(new QueueItem(null));
    try {
      if (_writerThread != null) {
        _writerThread.join();
      }
      if (_listLoaderThread != null) {
        _listLoaderThread.join();
      }
    } catch (InterruptedException e1) {
    }
    _writerThread = null;
    _listLoaderThread = null;

    _historyUpdateQueue.clear();
    _listLoaderQueue.clear();

    _shutdown = false;

  }

  /**
   * 
   * @return the local data directory
   */
  public File getLocalDataDir() {
    return _localLogFileDir;
  }

  /**
   * add a new url list to the queue
   * 
   * @param dataFilePath
   *          - the path to the file containing a list of urls
   * @return a unique list id that can be used to identify the list
   * @throws IOException
   */
  public long loadList(File dataFilePath,int refreshInterval) throws IOException {
    
    long listId = System.currentTimeMillis();
    // create a placeholder list
    CrawlList list = CrawlList.createListLoadingInLoadingState(this, listId,
        dataFilePath,refreshInterval);
    // add it to the map
    synchronized (_crawlLists) {
      _crawlLists.put(listId, list);
    }
    // add to to the loader queue .
    _listLoaderQueue.add(new QueueItem<CrawlList>(list));

    return listId;
  }

  /**
   * retrieve the list object associated with the given id
   * 
   * @param listId
   * @return
   */
  public CrawlList getList(long listId) {
    synchronized (_crawlLists) {
      return _crawlLists.get(listId);
    }
  }

  /**
   * 
   * @param matchCriteria
   *          - set of url fingerprints to match against
   * @param updater
   *          - the action to perform with each match
   */
  @Override
  public void syncList(final long listId, TreeSet<URLFP> matchCriteria,
      ItemUpdater targetList) throws IOException {
    // first grab last update time ...
    long lastUpdateTimePreScan = -1;

    Set<Long> processedItems = new HashSet<Long>();

    LOG.info("LIST:" + listId
        + " iterateCrawlHistoryLog - iterating hdfs log files");

    boolean exitLoop = false;
    do {
      synchronized (this) {
        lastUpdateTimePreScan = _lastCheckpointTime;
      }

      // ok now start to iterate item in checkpoint directory
      Path wildcardPattern = new Path(_remoteDataDirectory,
          CRAWL_HISTORY_HDFS_LOGFILE_PREFIX + "*");
      FileStatus candidates[] = _remoteFileSystem.globStatus(wildcardPattern);

      for (FileStatus candidate : candidates) {
        Path candidatePath = candidate.getPath();
        String candidateName = candidatePath.getName();
        long candidateTimestamp = Long.parseLong(candidateName
            .substring(CRAWL_HISTORY_HDFS_LOGFILE_PREFIX.length()));
        if (candidateTimestamp <= lastUpdateTimePreScan
            || lastUpdateTimePreScan == -1) {
          if (!processedItems.contains(candidateTimestamp)) {

            LOG.info("LIST:" + listId
                + " iterateCrawlHistoryLog - iterating hdfs file:"
                + candidateName);
            // go ahead and process this candidate ...
            iterateHDFSCrawlHistoryLog(listId, candidateTimestamp,
                matchCriteria, targetList);
            // add to set
            processedItems.add(candidateTimestamp);
          }
        }
      }

      // now acquire checkpoint semaphore
      LOG.info("LIST:" + listId
          + " iterateCrawlHistoryLog - acquiring semaphore");
      _checkpointSemaphore.acquireUninterruptibly();
      try {
        // check to see if checkpoint time has not changed
        if (_lastCheckpointTime == lastUpdateTimePreScan) {
          // ok checkpoint time has not changed since our previous attempt to
          // check it
          exitLoop = true;

          if (_localLogItems.size() != 0) {
            // go ahead and process any in memory items against the criteria ...
            for (URLFP candidate : matchCriteria) {
              ProxyCrawlHistoryItem item = _localLogItems.get(candidate);
              // if found call match action
              if (item != null) {
                targetList.updateItemState(candidate, item);
              }
            }
          }
        }
      } finally {
        _checkpointSemaphore.release();
      }
    } while (!exitLoop);
  }

  // take a remote crawl history log file and cache it locally
  private void cacheCrawlHistoryLog(File localCacheDir, long timestamp)
      throws IOException {

    SequenceFile.Reader reader = null;
    Path mapFilePath = new Path(_remoteDataDirectory,
        CRAWL_HISTORY_HDFS_LOGFILE_PREFIX + timestamp);
    Path indexFilePath = new Path(mapFilePath, "index");
    Path dataFilePath = new Path(mapFilePath, "data");
    File cacheFilePath = new File(localCacheDir,
        CRAWL_HISTORY_HDFS_LOGFILE_PREFIX + timestamp);

    SequenceFile.Reader indexReader = new SequenceFile.Reader(
        _remoteFileSystem, dataFilePath, CrawlEnvironment.getHadoopConfig());

    ValueBytes valueBytes = indexReader.createValueBytes();
    DataOutputBuffer keyBytes = new DataOutputBuffer();
    DataInputBuffer keyBuffer = new DataInputBuffer();
    DataOutputBuffer finalOutputStream = new DataOutputBuffer();
    DataOutputBuffer uncompressedValueBytes = new DataOutputBuffer();
    URLFP fp = new URLFP();

    try {
      while (indexReader.nextRaw(keyBytes, valueBytes) != -1) {

        keyBuffer.reset(keyBytes.getData(), 0, keyBytes.getLength());
        // read fingerprint ...
        fp.readFields(keyBuffer);
        // write hash only
        finalOutputStream.writeLong(fp.getUrlHash());
        uncompressedValueBytes.reset();
        // write value bytes to intermediate buffer ...
        valueBytes.writeUncompressedBytes(uncompressedValueBytes);
        // write out uncompressed length
        WritableUtils.writeVInt(finalOutputStream, uncompressedValueBytes
            .getLength());
        // write out bytes
        finalOutputStream.write(uncompressedValueBytes.getData(), 0,
            uncompressedValueBytes.getLength());
      }
      // delete existing ...
      cacheFilePath.delete();
      // compute crc ...
      CRC32 crc = new CRC32();
      crc.update(finalOutputStream.getData(), 0, finalOutputStream.getLength());
      // open final output stream
      DataOutputStream fileOutputStream = new DataOutputStream(
          new BufferedOutputStream(new FileOutputStream(cacheFilePath)));

      try {
        fileOutputStream.writeLong(crc.getValue());
        fileOutputStream.write(finalOutputStream.getData(), 0,
            finalOutputStream.getLength());
        fileOutputStream.flush();
      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
        fileOutputStream.close();
        fileOutputStream = null;
        cacheFilePath.delete();
        throw e;
      } finally {
        if (fileOutputStream != null) {
          fileOutputStream.close();
        }
      }
    } finally {
      if (indexReader != null) {
        indexReader.close();
      }
    }
  }

  private void iterateHDFSCrawlHistoryLog(long listId, long timestamp,
      TreeSet<URLFP> criteria, ItemUpdater targetList) throws IOException {

    // ok copy stuff locally if possible ...
    File localIndexPath = new File(getLocalDataDir(),
        CRAWL_HISTORY_HDFS_LOGFILE_PREFIX + timestamp + ".index");
    File localDataPath = new File(getLocalDataDir(),
        CRAWL_HISTORY_HDFS_LOGFILE_PREFIX + timestamp + ".data");
    File localBloomFilterPath = new File(getLocalDataDir(),
        CRAWL_HISTORY_HDFS_LOGFILE_PREFIX + timestamp + ".bloom");

    SequenceFile.Reader reader = null;
    Path mapFilePath = new Path(_remoteDataDirectory,
        CRAWL_HISTORY_HDFS_LOGFILE_PREFIX + timestamp);
    Path indexFilePath = new Path(mapFilePath, "index");
    Path dataFilePath = new Path(mapFilePath, "data");
    Path bloomFilePath = new Path(_remoteDataDirectory,
        CRAWL_HISTORY_HDFS_BLOOMFILTER_PREFIX + timestamp);

    // ok copy local first
    if (!localIndexPath.exists()) {
      LOG.info("LIST:" + listId + " Copying Index File:" + indexFilePath
          + " to Local:" + localIndexPath.getAbsolutePath());
      try {
        _remoteFileSystem.copyToLocalFile(indexFilePath, new Path(
            localIndexPath.getAbsolutePath()));
      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
        localIndexPath.delete();
        throw e;
      }
    }
    if (!localDataPath.exists()) {
      LOG.info("LIST:" + listId + " Copying Data File:" + dataFilePath
          + " to Local:" + localDataPath.getAbsolutePath());
      try {
        _remoteFileSystem.copyToLocalFile(dataFilePath, new Path(localDataPath
            .getAbsolutePath()));
      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
        localDataPath.delete();
        throw e;
      }

    }
    if (!localBloomFilterPath.exists()) {
      LOG.info("LIST:" + listId + " Copying Bloom File:" + bloomFilePath
          + " to Local:" + localBloomFilterPath.getAbsolutePath());
      try {
        _remoteFileSystem.copyToLocalFile(bloomFilePath, new Path(
            localBloomFilterPath.getAbsolutePath()));
      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
        localBloomFilterPath.delete();
        throw e;
      }

    }

    // ok open local
    FileSystem localFileSystem = FileSystem.getLocal(CrawlEnvironment
        .getHadoopConfig());

    SequenceFile.Reader indexReader = new SequenceFile.Reader(localFileSystem,
        new Path(localIndexPath.getAbsolutePath()), CrawlEnvironment
            .getHadoopConfig());

    try {
      URLFP firstIndexKey = null;
      URLFP lastIndexKey = new URLFP();
      LongWritable position = new LongWritable();
      while (indexReader.next(lastIndexKey, position)) {
        if (firstIndexKey == null) {
          try {
            firstIndexKey = (URLFP) lastIndexKey.clone();
          } catch (CloneNotSupportedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
      }

      LOG.info("LIST:" + listId + " ### Index First Domain:"
          + firstIndexKey.getDomainHash() + " URLHash:"
          + firstIndexKey.getUrlHash() + " Last Domain:"
          + lastIndexKey.getDomainHash() + " URLHash:"
          + lastIndexKey.getUrlHash());

      URLFP criteriaFirstKey = criteria.first();
      URLFP criteriaLastKey = criteria.last();

      if (firstIndexKey.compareTo(criteriaLastKey) > 0
          || lastIndexKey.compareTo(criteriaFirstKey) < 0) {
        LOG.info("LIST:" + listId + " Entire Index is Out of Range. Skipping!");
        LOG.info("LIST:" + listId + " ### Criteria First Domain:"
            + criteriaFirstKey.getDomainHash() + " URLHash:"
            + criteriaFirstKey.getUrlHash() + " Last Domain:"
            + criteriaLastKey.getDomainHash() + " URLHash:"
            + criteriaLastKey.getUrlHash());
        return;
      }
    } finally {
      indexReader.close();
    }

    LOG.info("LIST:" + listId + " ### Index:" + timestamp
        + " Passed Test. Doing Full Scan");
    // load bloom filter
    FSDataInputStream bloomFilterStream = localFileSystem.open(new Path(
        localBloomFilterPath.getAbsolutePath()));

    int hitCount = 0;

    try {
      URLFPBloomFilter filter = URLFPBloomFilter.load(bloomFilterStream);

      URLFP fpOut = new URLFP();
      ProxyCrawlHistoryItem itemOut = new ProxyCrawlHistoryItem();
      DataOutputBuffer valueBytesUncompressed = new DataOutputBuffer();
      ValueBytes valueBytes = null;
      DataInputBuffer valueReader = new DataInputBuffer();
      DataOutputBuffer keyBytes = new DataOutputBuffer();
      DataInputBuffer keyReader = new DataInputBuffer();

      URLFP lastFP = null;

      outerLoop:
      // now iterate each item in the criteria
      for (URLFP targetFP : criteria) {
        // if fingerprint is present in filter ...
        if (filter.isPresent(targetFP)) {
          // check to see if reader is initialized ...
          if (reader == null) {
            LOG.info("LIST:" + listId
                + " BloomFilter First Hit. Initializing Reader for file at:"
                + localDataPath.getAbsolutePath());
            reader = new SequenceFile.Reader(localFileSystem, new Path(
                localDataPath.getAbsolutePath()), CrawlEnvironment
                .getHadoopConfig());
            LOG.info("LIST:" + listId
                + " BloomFilter First Hit. Initialized Reader for file at:"
                + localDataPath.getAbsolutePath());
            valueBytes = reader.createValueBytes();
          }

          // if last read fingerprint was not null ...
          if (lastFP != null) {
            // does it match the current item
            if (lastFP.compareTo(targetFP) == 0) {
              // decompress value bytes ...
              valueBytesUncompressed.reset();
              valueBytes.writeUncompressedBytes(valueBytesUncompressed);
              // init valueReader
              valueReader.reset(valueBytesUncompressed.getData(),
                  valueBytesUncompressed.getLength());
              itemOut.readFields(valueReader);
              LOG.info("LIST:" + listId + " GOT HISTORY ITEM HIT. URL:"
                  + +lastFP.getUrlHash() + " File:" + dataFilePath);
              // if so, null out last fp
              lastFP = null;
              // and update item state ...
              targetList.updateItemState(targetFP, itemOut);

              hitCount++;

              continue;
            }
          }

          // ok at this point .. read the next item in the list ...
          lastFP = null;

          while (reader.nextRaw(keyBytes, valueBytes) != -1) {
            // init reader ...
            keyReader.reset(keyBytes.getData(), keyBytes.getLength());
            // read key
            fpOut.readFields(keyReader);
            // reset output buffer
            keyBytes.reset();

            // LOG.info("LIST:" + listId +" nextRaw Returned DH:" +
            // fpOut.getDomainHash() + " UH:" + fpOut.getUrlHash() + " TDH:" +
            // targetFP.getDomainHash() + " TUH:" + targetFP.getUrlHash());
            // compare it to target ...
            int result = fpOut.compareTo(targetFP);
            // ok does it match .. ?
            if (result == 0) {
              // decompress value bytes ...
              valueBytesUncompressed.reset();
              valueBytes.writeUncompressedBytes(valueBytesUncompressed);
              // init valueReader
              valueReader.reset(valueBytesUncompressed.getData(),
                  valueBytesUncompressed.getLength());
              itemOut.readFields(valueReader);

              LOG.info("LIST:" + listId + " GOT HISTORY ITEM HIT. URL:"
                  + fpOut.getUrlHash() + " File:" + dataFilePath);
              // update item state ...
              targetList.updateItemState(targetFP, itemOut);

              hitCount++;
              // and break to outer loop
              continue outerLoop;
            } else if (result == 1) {
              // LOG.info("LIST:" + listId +
              // " FP Comparison Returned 1. Going to OuterLoop");
              // update last FP
              lastFP = fpOut;
              // continue outer loop
              continue outerLoop;
            } else {
              // otherwise skip
            }
          }
          // ok if we got here .. we are done reading the sequence file and did
          // not find a trailing match
          LOG
              .warn("LIST:"
                  + listId
                  + " ### Reached End Of File Searching for item in MapFile while BloomFilter returned positive result (DomainHash:"
                  + targetFP.getDomainHash() + "FP:" + targetFP.getUrlHash()
                  + ")");
          // break out of outer loop

          break;
        }
      }
    } finally {
      bloomFilterStream.close();

      if (reader != null) {
        reader.close();
      }

      LOG.info("LIST:" + listId + " File:" + dataFilePath + " DONE. HitCount:"
          + hitCount);
    }
  }

  // Path mapOutputPath = new
  // Path(_remoteDataDirectory,CRAWL_HISTORY_HDFS_LOGFILE_PREFIX +
  // checkpointTimestamp);
  // Path filterOutputPath = new
  // Path(_remoteDataDirectory,CRAWL_HISTORY_HDFS_BLOOMFILTER_PREFIX +
  // checkpointTimestamp);

  /**
   * callback initiated by proxy server on a per url basis
   * 
   * @param connection
   * @param url
   * @param optTargetObj
   * @param successOrFailure
   */
  public void crawlComplete(CrawlURL url) {

    ProxyCrawlHistoryItem historyItem;

    if ((url.getFlags() & CrawlURL.Flags.IsRedirected) != 0) {
      historyItem = new ProxyCrawlHistoryItem();
      historyItem.setCrawlStatus(0);
      historyItem.setLastModifiedTime(System.currentTimeMillis());
      historyItem.setOriginalURL(url.getUrl());
      historyItem.setHttpResultCode(url.getOriginalResultCode());
      historyItem.setRedirectURL(url.getRedirectURL());
      if (url.isFieldDirty(CrawlURL.Field_RESULTCODE))
        historyItem.setRedirectHttpResult(url.getResultCode());
      historyItem
          .setRedirectStatus((url.getLastAttemptResult() == CrawlURL.CrawlResult.SUCCESS) ? 0
              : url.getLastAttemptFailureReason());

      // add the original url pointing to the final url to the queue
      _historyUpdateQueue.add(new HistoryUpdateRequest(historyItem));
      // ok, now create an entry for the redirected url
      historyItem = new ProxyCrawlHistoryItem();
      historyItem
          .setCrawlStatus((url.getLastAttemptResult() == CrawlURL.CrawlResult.SUCCESS) ? 0
              : url.getLastAttemptFailureReason());
      historyItem.setLastModifiedTime(System.currentTimeMillis());
      historyItem.setOriginalURL(url.getRedirectURL());
      if (url.isFieldDirty(CrawlURL.Field_RESULTCODE))
        historyItem.setHttpResultCode(url.getResultCode());
      // and add it to the queue
      _historyUpdateQueue.add(new HistoryUpdateRequest(historyItem));
    } else {
      // if not redirected ... create an entry for the redirected url
      historyItem = new ProxyCrawlHistoryItem();
      historyItem
          .setCrawlStatus((url.getLastAttemptResult() == CrawlURL.CrawlResult.SUCCESS) ? 0
              : url.getLastAttemptFailureReason());
      historyItem.setLastModifiedTime(System.currentTimeMillis());
      historyItem.setOriginalURL(url.getUrl());
      if (url.isFieldDirty(CrawlURL.Field_RESULTCODE))
        historyItem.setHttpResultCode(url.getResultCode());
      // and add it to the queue
      _historyUpdateQueue.add(new HistoryUpdateRequest(historyItem));
    }
  }

  /**
   * startCacheWriterThread
   * 
   */
  private void startLogWriterThread(int initFlags) {

    _writerThread = new Thread(new LogWriterThread(initFlags));
    _writerThread.start();
  }

  /**
   * getLogFilePath - get active log file path
   * 
   * @param directoryRoot
   * @return
   */
  private File getActiveLogFilePath() {
    return new File(_localLogFileDir, "ActiveLog");
  }

  /**
   * initializeEmptyLogFile - init an empty log file header
   * 
   * @param stream
   * @return
   * @throws IOException
   */
  private static LocalLogFileHeader initializeEmptyLogFile(DataOutput stream)
      throws IOException {

    LocalLogFileHeader header = new LocalLogFileHeader();
    header.writeHeader(stream);

    return header;
  }

  /**
   * initializeActiveLog - init local cache log
   * 
   * 
   * **/
  private void initializeActiveLog() throws IOException {

    File activeLogPath = getActiveLogFilePath();

    if (!activeLogPath.exists()) {
      DataOutputStream outputStream = new DataOutputStream(
          new FileOutputStream(activeLogPath));
      try {
        _header = initializeEmptyLogFile(outputStream);
      } finally {
        outputStream.close();
      }
    } else {
      _header = new LocalLogFileHeader();

      DataInputStream inputStream = new DataInputStream(new FileInputStream(
          activeLogPath));

      try {
        _header.readHeader(inputStream);
      } finally {
        inputStream.close();
      }

      if (_header._itemCount != 0) {
        _localLogItems = loadLocalLogItemMap();
      }
    }
  }

  /**
   * updateLogFileHeader - update the log file header called via the log file
   * writer thread ...
   * 
   * @throws IOException
   */
  void updateLogFileHeader(File logFileName, long newlyAddedItemsCount,
      long newItemsFileSize) throws IOException {

    RandomAccessFile file = new RandomAccessFile(logFileName, "rw");

    try {

      synchronized (_header) {
        // update cached header ...
        _header._fileSize += newItemsFileSize;
        _header._itemCount += newlyAddedItemsCount;
        // set the position at zero ..
        file.seek(0);
        // and write header to disk ...
        _header.writeHeader(file);
      }
    } finally {
      // major bottle neck..
      // file.getFD().sync();
      file.close();
    }
  }

  /**
   * get local log position according to cached header
   * 
   * @return
   */
  long getLocalLogFilePos() {
    long filePosOut = 0;
    synchronized (_header) {
      filePosOut = _header._fileSize;
    }
    return filePosOut;
  }

  /**
   * return the header sync bytes
   * 
   * @return header sync bytes
   */
  byte[] getLocalLogSyncBytes() {
    return _header._sync;
  }

  /**
   * 
   * @return a sorted map of urlfp to item
   * @throws IOException
   */
  TreeMap<URLFP, ProxyCrawlHistoryItem> loadLocalLogItemMap()
      throws IOException {

    TreeMap<URLFP, ProxyCrawlHistoryItem> itemMap = new TreeMap<URLFP, ProxyCrawlHistoryItem>();

    LOG.info("Reading Local Log File");
    RandomAccessFile file = new RandomAccessFile(getActiveLogFilePath(), "rw");

    // valid length indicator ...
    long validLength = 0;

    try {
      // skip header ...
      file.seek(LocalLogFileHeader.SIZE);
      validLength = file.getFilePointer();
      // ok walk n items ...
      for (int itemIdx = 0; itemIdx < _header._itemCount
          && file.getChannel().position() <= _header._fileSize; ++itemIdx) {
        try {
          ProxyCrawlHistoryItem item = readItem(file);
          // update valid length ...
          validLength = file.getFilePointer();
          // ok compute fingerprint for item ...
          URLFP fingerprintObject = URLUtils.getURLFPFromURL(item
              .getOriginalURL(), true);
          if (fingerprintObject == null) {
            LOG.error("Could not compute fingerprint for URL:"
                + item.getOriginalURL());
          } else {
            itemMap.put(fingerprintObject, item);
          }
        } catch (IOException e) {
          LOG.error(CCStringUtils.stringifyException(e));
          try {
            if (!seekToNextSyncBytesPos(file)) {
              LOG.error("Hit EOF While Seeking for next SyncByte Sequence!");
              break;
            } else {
              LOG.info("Seek to Next SyncByte Succeeded! Continuing Load");
            }
          } catch (IOException e2) {
            LOG.error(CCStringUtils.stringifyException(e2));
            LOG.error("Got IO Exception Reading SyncBytes - Bailing!");
            break;
          }
        }
      }
    } finally {
      if (file.length() > validLength) {
        LOG.warn("File Length is:" + file.length() + " Truncating Length to:"
            + validLength);
        file.setLength(validLength);
      }

      file.close();
    }
    LOG.info("Done Reading Local Log File");

    return itemMap;
  }

  private ProxyCrawlHistoryItem readItem(RandomAccessFile fileStream)
      throws IOException {

    try {
      // read sync bytes ...
      fileStream.read(_syncByteBuffer);
      // validate ...
      if (!Arrays.equals(_header._sync, _syncByteBuffer)) {
        throw new IOException("Error Reading Sync Bytes for Item In Checkpoint");
      }
      int checksum = fileStream.readInt();
      int payloadSize = fileStream.readShort();

      if (payloadSize == 0) {
        throw new IOException("Invalid Payload Size Reading Item In Checkpoint");
      }
      // read the payload
      _payloadBuffer.setCapacity(payloadSize);

      fileStream.read(_payloadBuffer.get(), 0, payloadSize);

      _crc16in.reset();
      _crc16in.update(_payloadBuffer.get(), 0, payloadSize);

      // if computed checksum does not match file checksum !!!
      if (_crc16in.getValue() != (long) checksum) {
        throw new IOException("Checksum Mismatch Expected:" + checksum
            + " got:" + _crc16in.getValue() + " while Reading Item");
      }
      _payloadInputStream.reset(_payloadBuffer.get(), 0, payloadSize);

      ProxyCrawlHistoryItem itemOut = new ProxyCrawlHistoryItem();

      itemOut.deserialize(_payloadInputStream, new BinaryProtocol());

      return itemOut;
    } catch (Exception e) {
      LOG.error(CCStringUtils.stringifyException(e));
      throw new IOException(e);
    }
  }

  /**
   * seek out next instance of sync bytes in the file input stream
   * 
   * @param file
   * @throws IOException
   */
  private boolean seekToNextSyncBytesPos(RandomAccessFile file)
      throws IOException {
    // read in a sync.length buffer amount
    file.read(_syncByteBuffer);

    int syncLen = _header._sync.length;

    // start scan for next sync position ...
    for (int i = 0; file.getFilePointer() < _header._fileSize; i++) {
      int j = 0;
      for (; j < syncLen; j++) {
        if (_header._sync[j] != _syncByteBuffer[(i + j) % syncLen])
          break;
      }
      if (j == syncLen) {
        // found matching sync bytes - reset file pos to before sync bytes
        file.seek(file.getFilePointer() - LocalLogFileHeader.SYNC_BYTES_SIZE); // position
                                                                               // before
                                                                               // sync
        return true;
      }
      _syncByteBuffer[i % syncLen] = file.readByte();
    }
    return false;
  }

  void doCheckpoint() {

    LOG.info("Starting Checkpoint Process");
    try {

      LOG.info("Writing HDFS Log File");

      // write local file contents to hdfs
      // we don't need to lock the map here because only the local log thread
      // (current thread) modified the map ...
      long checkpointTimestamp = writeMapFileToHDFS(_localLogItems);

      // ok that worked .. delete local log
      getActiveLogFilePath().delete();

      // ok now we DO NEED TO lock the map
      synchronized (_localLogItems) {
        _localLogItems.clear();
      }

      LOG.info("Regenerating Local Log");
      // and regenerate the file
      initializeActiveLog();

      synchronized (CrawlHistoryManager.this) {
        // ok now update checkpoint timestamp variable
        _lastCheckpointTime = checkpointTimestamp;
      }

      LOG.info("Checkpoint Done");

    } catch (IOException e) {
      LOG.error("CrawlHistoryLog Checkpoint Failed with Exception:"
          + CCStringUtils.stringifyException(e));
    }
  }

  long writeMapFileToHDFS(TreeMap<URLFP, ProxyCrawlHistoryItem> itemMap)
      throws IOException {

    long checkpointTimestamp = System.currentTimeMillis();

    Path mapOutputPath = new Path(_remoteDataDirectory,
        CRAWL_HISTORY_HDFS_LOGFILE_PREFIX + checkpointTimestamp);
    Path filterOutputPath = new Path(_remoteDataDirectory,
        CRAWL_HISTORY_HDFS_BLOOMFILTER_PREFIX + checkpointTimestamp);

    writeCheckpoint(itemMap, CrawlEnvironment.getHadoopConfig(),
        _remoteFileSystem, mapOutputPath, filterOutputPath);

    return checkpointTimestamp;
  }

  public static void writeCheckpoint(
      TreeMap<URLFP, ProxyCrawlHistoryItem> itemMap, Configuration conf,
      FileSystem remoteFileSystem, Path mapOutputPath, Path filterOutputPath)
      throws IOException {

    try {

      LOG.info("Generating Map File at Location:" + mapOutputPath
          + " Filter At:" + filterOutputPath);

      // open a temporary hdfs streams ...
      MapFile.Writer writer = new MapFile.Writer(conf, remoteFileSystem,
          mapOutputPath.toString(), URLFP.class, ProxyCrawlHistoryItem.class);

      // create a bloom filter
      URLFPBloomFilter filter = new URLFPBloomFilter(_checkpointThreshold * 2,
          10, 11);

      try {

        for (Map.Entry<URLFP, ProxyCrawlHistoryItem> entry : itemMap.entrySet()) {
          LOG.info("Writing Key to Map. DomainHash:"
              + entry.getKey().getDomainHash() + " URLHash:"
              + entry.getKey().getUrlHash());
          filter.add(entry.getKey());
          writer.append(entry.getKey(), entry.getValue());
        }
      } finally {
        writer.close();
      }
      LOG.info("Done generating Map File");

      LOG.info("Writing Bloom Filter Data");
      // ok now also flush the bloom filter
      FSDataOutputStream bloomFilterOutputStream = remoteFileSystem
          .create(filterOutputPath);
      try {
        filter.serialize(bloomFilterOutputStream);
      } finally {
        bloomFilterOutputStream.flush();
        bloomFilterOutputStream.close();
      }
    } catch (IOException e) {

      // delete all relevant files ...
      remoteFileSystem.delete(mapOutputPath, true);
      remoteFileSystem.delete(filterOutputPath, false);

      // throw exception back out ...
      throw e;
    }
  }

  DataOutputBuffer _outputBuffer = new DataOutputBuffer();
  private CRC16    _crc16Out     = new CRC16();

  /**
   * append a ProxyCrawlHistoryItem to the active log
   * 
   * @param item
   * @throws IOException
   */
  void appendItemToLog(ProxyCrawlHistoryItem item) throws IOException {

    try {
      // open the log file ...
      DataOutputStream logStream = new DataOutputStream(new FileOutputStream(
          getActiveLogFilePath(), true));

      try {
        // reset crc calculator (single thread so no worries on synchronization)
        _crc16Out.reset();
        // reset output stream
        _outputBuffer.reset();
        // create checked stream
        CheckedOutputStream checkedStream = new CheckedOutputStream(
            _outputBuffer, _crc16Out);
        DataOutputStream dataOutputStream = new DataOutputStream(checkedStream);
        // write out item
        item.serialize(dataOutputStream, new BinaryProtocol());
        dataOutputStream.flush();

        // ok now write out sync,crc,length then data
        logStream.write(getLocalLogSyncBytes());
        logStream.writeInt((int) checkedStream.getChecksum().getValue());
        logStream.writeShort((short) _outputBuffer.getLength());
        logStream.write(_outputBuffer.getData(), 0, _outputBuffer.getLength());

        logStream.flush();
        logStream.close();
        logStream = null;

        // now we need to update the file header
        updateLogFileHeader(getActiveLogFilePath(), 1, LOG_ITEM_HEADER_SIZE
            + _outputBuffer.getLength());

        URLFP fingerprint = URLUtils.getURLFPFromURL(item.getOriginalURL(),
            true);
        // update local log
        synchronized (_localLogItems) {
          if (fingerprint != null) {
            _localLogItems.put(fingerprint, item);
          }
        }

        ImmutableSet<CrawlList> lists = null;
        // and now walk lists updating them as necessary
        synchronized (_crawlLists) {
          lists = new ImmutableSet.Builder<CrawlList>().addAll(
              _crawlLists.values()).build();
        }
        for (CrawlList list : lists) {
          try {
            list.updateItemState(fingerprint, item);
          } catch (Exception e) {
            // ok, IF an error occurs updating the list metadata.. we need to
            // continue along.
            // it is critical for this thread to not die in such a circumstance
            LOG.fatal("Error Updating List(" + list.getListId() + "):"
                + CCStringUtils.stringifyException(e));
            System.out.println("Exception in List Update(" + list.getListId()
                + "):" + CCStringUtils.stringifyException(e));
          }
        }

      } finally {
        if (logStream != null) {
          logStream.close();
        }
      }
    } finally {

    }
  }

  class LogWriterThread implements Runnable {

    int _initFlags;

    public LogWriterThread(int initFlags) {
      _initFlags = initFlags;
    }

    @Override
    public void run() {

      boolean shutdown = false;

      while (!shutdown) {
        try {

          final HistoryUpdateRequest request = _historyUpdateQueue.poll(
              POLL_WAIT_TIME, TimeUnit.MILLISECONDS);

          if (request != null) {
            switch (request._requestType) {

              case ExitThreadRequest: {
                // shutdown condition ...
                LOG.info("Log Writer Thread Received Shutdown. Exiting!");
                shutdown = true;
              }
                break;

              case UpdateRequest: {

                try {
                  appendItemToLog(request._item);
                } catch (IOException e) {
                  LOG.error(CCStringUtils.stringifyException(e));
                }
              }
                break;
            }
          }

          // now check if we can perform a checkpoint
          long localItemCount = 0;
          synchronized (_header) {
            localItemCount = _header._itemCount;
          }

          // LOG.info("$$$$ LOCAL ITEM COUNT IS:" + localItemCount);
          if (localItemCount >= _checkpointThreshold) {

            // if checkpoints were not disabled during initialization ...
            if ((_initFlags & INIT_FLAG_DISABLE_CHECKPOINTS) == 0) {
              LOG.info("$$$$ LOCAL ITEM COUNT EXCEEDS THRESHOLD:"
                  + _checkpointThreshold);
              // see if can start a checkpoint ..
              if (_checkpointSemaphore.tryAcquire(100, TimeUnit.MILLISECONDS)) {
                try {
                  // ok we can exclusively touch the local log file
                  doCheckpoint();
                } finally {
                  _checkpointSemaphore.release();
                }
              } else {
                LOG.warn("$$$$ FAILED TO ACQUIRE SEMAPHORE FOR CHECKPOINT!");
              }
            } else {
              LOG
                  .warn("$$$$ CHECKPOINTS DISABLED. SKIPPING POTENTIAL CHECKPOINT");
            }
          }

        } catch (InterruptedException e) {

        }
      }
    }
  }

  static class HistoryUpdateRequest {
    public enum RequestType {
      UpdateRequest, ExitThreadRequest
    }

    public HistoryUpdateRequest(ProxyCrawlHistoryItem item) {
      _item = item;
      _requestType = RequestType.UpdateRequest;
    }

    public HistoryUpdateRequest() {
      _requestType = RequestType.ExitThreadRequest;
    }

    public ProxyCrawlHistoryItem _item = null;
    public RequestType           _requestType;
  }

  private void loadExistingLists() throws IOException {
    // scan data directory for list id pattern
    FileSystem localFileSystem = FileSystem.getLocal(CrawlEnvironment
        .getHadoopConfig());

    FileStatus loadTargets[] = localFileSystem.globStatus(new Path(
        _localLogFileDir.getAbsolutePath(), CrawlList.LIST_URL_DATA_PREFIX
            + "*"));

    // sort list so that we load newer lists first ...
    Arrays.sort(loadTargets, new Comparator<FileStatus>() {

      @Override
      public int compare(FileStatus o1, FileStatus o2) {
        return ((Long) o2.getModificationTime()).compareTo(o1
            .getModificationTime());
      }

    });

    for (FileStatus loadTarget : loadTargets) {
      // extract timestamp ...
      long listId = Long.parseLong(loadTarget.getPath().getName().substring(
          CrawlList.LIST_URL_DATA_PREFIX.length()));
      LOG.info("Found List Data for List:" + listId);
      // validate
      if (CrawlList.allFilesPresent(_localLogFileDir, listId)) {
        LOG.info("List looks valid. Loading");
        try {
          CrawlList list = new CrawlList(this, listId);
          synchronized (_crawlLists) {
            CrawlList oldList = _crawlLists.get(listId);
            if (oldList != null) {
              list.setEventListener(oldList.getEventListener());
            }
            _crawlLists.put(listId, list);
          }
          LOG.info("Loaded List:" + listId + " Scheduling for Queueing");
          _queueLoaderQueue.add(new QueueItem<CrawlList>(list));
        } catch (IOException e) {
          LOG.error("Failed to load list:" + listId + " Exception:"
              + CCStringUtils.stringifyException(e));
          synchronized (_crawlLists) {
            _crawlLists.put(listId, CrawlList.createListWithLoadErrorState(
                this, listId, e));
          }
        }
      }
    }
  }

  private void startListLoaderThread() {
    _listLoaderThread = new Thread(new Runnable() {

      @Override
      public void run() {
        LOG.info("Starting List Loader Thread");
        while (true) {
          try {
            QueueItem<CrawlList> listItem = _listLoaderQueue.take();
            if (listItem._item == null || _shutdown) {
              break;
            } else {
              try {
                // mark the ui list as really loading ...
                listItem._item.markListAsReallyLoading();

                LOG.info("Attempting to load List:"
                    + listItem._item.getListId());
                CrawlList listToLoad = new CrawlList(CrawlHistoryManager.this,
                    listItem._item.getListId(), listItem._item
                        .getListURLDataFile(),listItem._item.getMetadata().getRefreshInterval());

                LOG.info("Successfully loaded List:"
                    + listItem._item.getListId() + " Sending to QueueLoader");
                synchronized (_crawlLists) {
                  CrawlList oldList = _crawlLists.get(listToLoad.getListId());
                  if (oldList != null) {
                    listToLoad.setEventListener(oldList.getEventListener());
                  }
                  _crawlLists.put(listToLoad.getListId(), listToLoad);
                }
                // add to queue loader ...
                if (!_shutdown) {
                  _queueLoaderQueue.add(new QueueItem<CrawlList>(listToLoad));
                }
              } catch (Exception e) {
                LOG.error("Failed to load List:" + listItem._item.getListId()
                    + " with Exception:" + CCStringUtils.stringifyException(e));
                synchronized (_crawlLists) {
                  _crawlLists.put(listItem._item.getListId(), CrawlList
                      .createListWithLoadErrorState(CrawlHistoryManager.this,
                          listItem._item.getListId(), e));
                }
              }
            }
          } catch (InterruptedException e) {
          }
        }
        LOG.info("Exiting List Loader Thread");
      }

    });
    _listLoaderThread.start();

  }

  /**
   * start the queue loader thread
   * 
   * @param loader
   *          - the passed in queue loader callback
   */
  public void startQueueLoaderThread(final CrawlQueueLoader loader) {
    _queueLoaderThread = new Thread(new Runnable() {

      @Override
      public void run() {
        LOG.info("Starting Queue Loader Thread");
        try { 
          while (true) {
            try {
              QueueItem<CrawlList> listItem = _queueLoaderQueue.take();
              if (listItem._item == null || _queueLoaderShutdown) {
                break;
              } else {
                try {
                  LOG.info("Attempting to queue List:"
                      + listItem._item.getListId());
                  listItem._item.queueUnCrawledItems(loader);
                  LOG
                      .info("Finished queueing List:"
                          + listItem._item.getListId());
                } catch (Exception e) {
                  LOG.error("Failed to queue List:" + listItem._item.getListId()
                      + " with Exception:" + CCStringUtils.stringifyException(e));
                }
              }
            } catch (InterruptedException e) {
            }
          }
        }
        finally { 
          LOG.info("Exiting Queue Loader Thread");
        }
      }

    });
    _queueLoaderThread.start();
  }

  public void stopQueueLoaderThread() {
    if (_queueLoaderThread != null) {
      _queueLoaderShutdown = true;
      _queueLoaderQueue.add(new QueueItem(null));
      try {
        _queueLoaderThread.join();
      } catch (InterruptedException e) {
      }
      _queueLoaderThread = null;
      _queueLoaderShutdown = false;
    }
  }

  private static CrawlURL proxyCrawlHitoryItemToCrawlURL(
      ProxyCrawlHistoryItem item) {
    CrawlURL url = new CrawlURL();

    url.setUrl(item.getOriginalURL());
    if (item.isFieldDirty(ProxyCrawlHistoryItem.Field_CRAWLSTATUS)) {
      if (item.getCrawlStatus() == 0) {
        url.setLastAttemptResult((byte) CrawlURL.CrawlResult.SUCCESS);
      } else {
        url.setLastAttemptResult((byte) CrawlURL.CrawlResult.FAILURE);
        url.setLastAttemptFailureReason((byte) item.getCrawlStatus());
      }
    }
    if (item.isFieldDirty(ProxyCrawlHistoryItem.Field_HTTPRESULTCODE)) {
      url.setResultCode(item.getHttpResultCode());
    }

    if (item.isFieldDirty(ProxyCrawlHistoryItem.Field_REDIRECTURL)) {
      // move original result code over to appropriate location in CrawlURL
      url.setOriginalResultCode(url.getResultCode());
      url.setFieldClean(CrawlURL.Field_RESULTCODE);
      url.setFlags(CrawlURL.Flags.IsRedirected);
      url.setRedirectURL(item.getRedirectURL());
      if (item.getRedirectStatus() == 0) {
        url.setLastAttemptResult((byte) CrawlURL.CrawlResult.SUCCESS);
      } else {
        url.setLastAttemptResult((byte) CrawlURL.CrawlResult.FAILURE);
        url.setLastAttemptFailureReason((byte) item.getRedirectStatus());
      }

      if (item.isFieldDirty(ProxyCrawlHistoryItem.Field_REDIRECTHTTPRESULT)) {
        url.setResultCode(item.getRedirectHttpResult());
      }
    }
    return url;
  }

  public Map<Long, CrawlListMetadata> collectListMetadata(Set<Long> predicate) {

    TreeMap<Long, CrawlListMetadata> metadataOut = new TreeMap<Long, CrawlListMetadata>();

    int normalPriority = Thread.currentThread().getPriority();
    try {
      LOG.info("### BOOSTING THREAD PRIORITY");
      Thread.currentThread().setPriority(Thread.MAX_PRIORITY);

      LOG.info("### ATTEMPTING LOCK");
      synchronized (_crawlLists) {

        LOG.info("### GOT LOCK");
        for (CrawlList list : _crawlLists.values()) {
          if (predicate.contains(list.getListId())) {
            metadataOut.put(list.getListId(), list.getMetadata());
          }
        }
      }
      LOG.info("### RELEASING LOCK");
    } finally {
      Thread.currentThread().setPriority(normalPriority);
    }

    return metadataOut;
  }

  /**********************************************************************************************/
  // TEST CODE
  /**********************************************************************************************/

  private static void generateTestURLFile(File outputFile, String[] urlList)
      throws IOException {
    PrintWriter writer = new PrintWriter(outputFile, "UTF-8");

    for (String url : urlList) {
      writer.println(url);
    }

    writer.flush();
    writer.close();

  }

  static final String[] urlList1 = { "http://www.google.com/1",
      "http://www.someotherdomain.com/1", "http://www.google.com/2",
      "http://www.google.com/3",
      "http://www.someotherdomain.com/triggerSemaphore" };

  static final String[] urlList2 = { "http://www.google.com/1",
      "http://www.google.com/2", "http://www.someotherdomain.com/2",
      "http://www.google.com/4", "http://www.google.com/5", };

  public static void main(String[] args) {
    // initialize ...
    Configuration conf = new Configuration();

    conf.addResource("nutch-default.xml");
    conf.addResource("nutch-site.xml");
    conf.addResource("core-site.xml");
    conf.addResource("hdfs-site.xml");
    conf.addResource("mapred-site.xml");

    BasicConfigurator.configure();

    conf.set("mapred.output.compression.codec",
        "org.apache.hadoop.io.compress.GzipCodec");
    conf.set("mapred.map.output.compression.codec",
        "org.apache.hadoop.io.compress.GzipCodec");
    conf.set("io.seqfile.compression.type", "NONE");

    CrawlEnvironment.setHadoopConfig(conf);
    CrawlEnvironment.setDefaultHadoopFSURI("file:///");

    EventLoop eventLoop = new EventLoop();

    eventLoop.start();

    testWriteMapFileToHDFS(eventLoop);

    // launchInTestMode();
  }

  private static void testWriteMapFileToHDFS(EventLoop eventLoop) {
    try {
      // initialize log manager
      CrawlHistoryManager logManager = initializeTestLogManager(eventLoop, true);

      // initialize item list
      TreeMap<URLFP, ProxyCrawlHistoryItem> items = buildTestList(urlList1);
      final TreeMap<String, URLFP> urlToURLFPMap = new TreeMap<String, URLFP>();

      for (Map.Entry<URLFP, ProxyCrawlHistoryItem> item : items.entrySet()) {
        urlToURLFPMap.put(item.getValue().getOriginalURL(), item.getKey());
      }

      // add to local item map in log manager
      for (ProxyCrawlHistoryItem item : items.values()) {
        logManager.appendItemToLog(item);
      }
      // ok shutdown log manager ...
      logManager.shutdown();

      // restart - reload log file ...
      logManager = initializeTestLogManager(eventLoop, false);

      // write to 'hdfs'
      logManager.doCheckpoint();

      syncAndValidateItems(items, logManager);

      logManager.shutdown();

      // restart
      logManager = initializeTestLogManager(eventLoop, false);

      // tweak original items
      updateTestItemStates(items);

      // ok append items
      for (ProxyCrawlHistoryItem item : items.values()) {
        logManager.appendItemToLog(item);
      }

      syncAndValidateItems(items, logManager);

      // ok now checkpoint the items
      logManager.doCheckpoint();

      // ok now validate one last time
      syncAndValidateItems(items, logManager);

      // shutdown
      logManager.shutdown();

      logManager = null;

      {
        // start from scratch ...
        final CrawlHistoryManager logManagerTest = initializeTestLogManager(
            eventLoop, true);

        // create a final version of the tree map reference
        final TreeMap<URLFP, ProxyCrawlHistoryItem> itemList = items;
        // create filename
        File urlInputFile = new File(logManagerTest.getLocalDataDir(),
            "testURLS-" + System.currentTimeMillis());
        // ok create a crawl list from urls
        CrawlList.generateTestURLFile(urlInputFile, urlList1);
        long listId = logManagerTest.loadList(urlInputFile,0);

        CrawlList listObject = logManagerTest.getList(listId);

        final Semaphore listCompletionSemaphore = new Semaphore(-(itemList
            .size() - 1));

        listObject.setEventListener(new CrawlList.CrawlListEvents() {

          @Override
          public void itemUpdated(URLFP itemFingerprint) {
            // TODO Auto-generated method stub
            listCompletionSemaphore.release();
          }
        });

        // ok start the appropriate threads
        logManagerTest.startLogWriterThread(0);
        logManagerTest.startListLoaderThread();
        logManagerTest.startQueueLoaderThread(new CrawlQueueLoader() {

          @Override
          public void queueURL(URLFP urlfp, String url) {
            logManagerTest
                .crawlComplete(proxyCrawlHitoryItemToCrawlURL(itemList
                    .get(urlToURLFPMap.get(url))));
          }
          @Override
          public void flush() {
            // TODO Auto-generated method stub
            
          }
        });

        LOG.info("Waiting for Release");

        // and wait for the finish
        listCompletionSemaphore.acquireUninterruptibly();

        LOG.info("Got Here");

      }

    } catch (IOException e) {
      LOG.error(CCStringUtils.stringifyException(e));
    }
  }

  private static void syncAndValidateItems(
      TreeMap<URLFP, ProxyCrawlHistoryItem> items,
      CrawlHistoryManager logManager) throws IOException {
    // ok now sync the list
    final TreeMap<URLFP, ProxyCrawlHistoryItem> syncedItemList = new TreeMap<URLFP, ProxyCrawlHistoryItem>();

    try {
      logManager.syncList(0L, Sets.newTreeSet(items.keySet()),
          new ItemUpdater() {

            @Override
            public void updateItemState(URLFP fingerprint,
                ProxyCrawlHistoryItem item) throws IOException {
              try {
                syncedItemList.put((URLFP) fingerprint.clone(),
                    (ProxyCrawlHistoryItem) item.clone());
              } catch (CloneNotSupportedException e) {
                e.printStackTrace();
              }
            }

          });
    } catch (IOException e) {
      LOG.error(CCStringUtils.stringifyException(e));
      Assert.assertTrue(false);
    }

    // assert that the key set is equal
    Assert.assertEquals(items.keySet(), syncedItemList.keySet());
    // ok now validate that the values are equal
    for (Map.Entry<URLFP, ProxyCrawlHistoryItem> item : items.entrySet()) {
      ProxyCrawlHistoryItem other = syncedItemList.get(item.getKey());
      Assert.assertEquals(item.getValue(), other);
    }
  }

  private static CrawlHistoryManager initializeTestLogManager(
      EventLoop eventLoop, boolean fromScratch) throws IOException {
    File baseTestDir = new File("/tmp/logManagerTest");
    File remoteDir = new File(baseTestDir, "remote");
    File localDir = new File(baseTestDir, "local");

    if (fromScratch) {
      FileUtils.recursivelyDeleteFile(baseTestDir);
      baseTestDir.mkdir();
      remoteDir.mkdir();
      localDir.mkdir();
    }

    FileSystem localFileSystem = FileSystem.getLocal(CrawlEnvironment
        .getHadoopConfig());

    int initFlags = INIT_FLAG_SKIP_LOAD_EXISTING_LISTS
        | INIT_FLAG_SKIP_LOG_WRITER_THREAD_INIT
        | INIT_FLAG_SKIP_LIST_LOADER_THREAD_INIT;

    return new CrawlHistoryManager(localFileSystem, new Path(remoteDir
        .getAbsolutePath()), localDir, eventLoop, initFlags);
  }

  private static TreeMap<URLFP, ProxyCrawlHistoryItem> buildTestList(
      String... urls) {

    TreeMap<URLFP, ProxyCrawlHistoryItem> mapOut = new TreeMap<URLFP, ProxyCrawlHistoryItem>();

    for (String url : urls) {
      URLFP fp = URLUtils.getURLFPFromURL(url, true);
      if (fp != null) {

        ProxyCrawlHistoryItem item = new ProxyCrawlHistoryItem();

        item.setCrawlStatus(0);
        item.setOriginalURL(url);
        item.setHttpResultCode(200);

        mapOut.put(fp, item);
      }
    }
    return mapOut;
  }

  private static void updateTestItemStates(
      TreeMap<URLFP, ProxyCrawlHistoryItem> items) {

    for (ProxyCrawlHistoryItem item : items.values()) {
      item.setHttpResultCode(301);
      item.setRedirectURL(item.getOriginalURL() + "/redirect");
      item.setRedirectStatus(0);
      item.setRedirectHttpResult(200);
    }
  }

  private static void launchInTestMode() {

    File baseTestDir = new File("/tmp/logManagerTest");
    FileUtils.recursivelyDeleteFile(baseTestDir);
    baseTestDir.mkdir();
    File remoteDir = new File(baseTestDir, "remote");
    File localDir = new File(baseTestDir, "local");
    remoteDir.mkdir();
    localDir.mkdir();

    final TreeMap<String, URLFP> urlToFPMap = new TreeMap<String, URLFP>();
    final TreeMap<URLFP, String> urlFPToString = new TreeMap<URLFP, String>();

    Set<String> list1 = Sets.newHashSet(urlList1);
    Set<String> list2 = Sets.newHashSet(urlList2);
    final Set<String> combined = Sets.union(list1, list2);
    Set<String> difference = Sets.difference(list1, list2);
    final Set<String> completedURLS = new HashSet<String>();
    for (String url : combined) {
      URLFP fingerprint = URLUtils.getURLFPFromURL(url, true);
      urlToFPMap.put(url, fingerprint);
      urlFPToString.put(fingerprint, url);
    }

    File testInputFile1 = new File(localDir, "INPUT_LIST-"
        + System.currentTimeMillis());
    File testInputFile2 = new File(localDir, "INPUT_LIST-"
        + (System.currentTimeMillis() + 1));

    try {

      generateTestURLFile(testInputFile1, urlList1);
      generateTestURLFile(testInputFile2, urlList2);

      FileSystem localFileSystem = FileSystem.getLocal(CrawlEnvironment
          .getHadoopConfig());

      EventLoop eventLoop = new EventLoop();
      eventLoop.start();

      final CrawlHistoryManager logManager = new CrawlHistoryManager(
          localFileSystem, new Path(remoteDir.getAbsolutePath()), localDir,
          eventLoop, 0);

      final LinkedBlockingQueue<ProxyCrawlHistoryItem> queue = new LinkedBlockingQueue<ProxyCrawlHistoryItem>();

      final Semaphore initialListComplete = new Semaphore(0);

      logManager.startQueueLoaderThread(new CrawlQueueLoader() {

        @Override
        public void queueURL(URLFP urlfp, String url) {
          ProxyCrawlHistoryItem item = new ProxyCrawlHistoryItem();
          item.setOriginalURL(url);
          queue.add(item);
        }
        
        @Override
        public void flush() {
          // TODO Auto-generated method stub

        }
      });

      Thread queueTestThread = new Thread(new Runnable() {

        @Override
        public void run() {
          while (true) {
            try {
              ProxyCrawlHistoryItem item = queue.take();

              if (item.getOriginalURL().length() == 0) {
                break;
              } else {

                System.out.println("Got:" + item.getOriginalURL());

                CrawlURL urlObject = new CrawlURL();

                Assert.assertTrue(!completedURLS
                    .contains(item.getOriginalURL()));
                completedURLS.add(item.getOriginalURL());

                urlObject
                    .setLastAttemptResult((byte) CrawlURL.CrawlResult.SUCCESS);
                urlObject.setUrl(item.getOriginalURL());
                urlObject.setResultCode(200);

                logManager.crawlComplete(urlObject);

                if (completedURLS.equals(combined)) {
                  System.out
                      .println("Hit Trigger URL. Releasing InitialListComplete Semaphore");
                  initialListComplete.release(1);
                }
              }

            } catch (InterruptedException e) {
            }
          }
        }

      });

      queueTestThread.start();

      logManager.loadList(testInputFile1,0);
      logManager.loadList(testInputFile2,0);
      System.out.println("Waiting for Initial List to Complete");
      initialListComplete.acquireUninterruptibly();
      System.out.println("Woke Up");

      try {
        eventLoop.getEventThread().join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

}
