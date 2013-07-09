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
 */

package org.commoncrawl.service.crawler;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.CRC32;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.commoncrawl.async.ConcurrentTask;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.async.Timer;
import org.commoncrawl.async.ConcurrentTask.CompletionCallback;
import org.commoncrawl.common.Environment;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.protocol.CrawlSegmentDetail;
import org.commoncrawl.protocol.CrawlSegmentHost;
import org.commoncrawl.protocol.CrawlSegmentURL;
import org.commoncrawl.protocol.CrawlURL;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.FPGenerator;
import org.commoncrawl.util.FileUtils;
import org.commoncrawl.util.FlexBuffer;
import org.commoncrawl.util.MovingAverage;
import org.commoncrawl.util.RuntimeStatsCollector;
import org.commoncrawl.util.SmoothedAverage;
import org.mortbay.jetty.security.Credential.MD5;

import com.google.common.collect.Iterators;

public final class CrawlLog {

  public static final Log LOG = LogFactory.getLog(CrawlLog.class);

  public static final int DEFAULT_LOG_FLUSH_INTERVAL = 30000;

  public static final int DEFAULT_LOG_CHECKPOINT_INTERVAL = 60000 * 5;

  public static final int DEFAULT_LOG_FILE_CHECKPOINT_ITEM_COUNT_THRESHOLD = 100000;
  
  public static final long DEFAULT_LOG_FILE_SIZE_CHECKPOINT_THRESHOLD = 1073741824 * 4; 

  /** log file header **/
  LogFileHeader _header = new LogFileHeader();

  /** node name **/
  String _nodeName;

  /** data directory **/
  File _rootDirectory;

  /** event loop **/
  EventLoop _eventLoop;

  /** thread pool **/
  ExecutorService _threadPool;

  /** crawler engine pointer **/
  CrawlerEngine _engine;

  /** robots segment logger **/
  CrawlSegmentLog _robotsSegment = new CrawlSegmentLog(null, -1, -1, null);

  /** individual crawl segment loggers **/
  Map<Long, CrawlSegmentLog> _loggers = new HashMap<Long, CrawlSegmentLog>();

  /** checkpoint completion callback **/
  CheckpointCompletionCallback _checkpointCompletionCallback = null;

  /** checkpoint id - analogous to parse segment id **/
  long _checkpointId;

  /** flush in progress flag **/
  boolean _flushInProgress = false;

  /** a shutdown operation is in progress **/
  boolean _shutdownInProgress = false;

  /** log flusher timer **/
  Timer _logFlusherTimer = null;

  /** last checkpoint time **/
  long _lastCheckpointTime = -1;

  MovingAverage _flushTimeAVG = new MovingAverage(10);
  SmoothedAverage _flushTimeSmoothed = new SmoothedAverage(.8);
  long _lastFlushTime = 0;

  /** get active log file path **/
  public static File getActivePath(File directoryRoot) {
    // and construct a path to the local crawl segment directory ...
    File crawlDataDir = new File(directoryRoot, CrawlEnvironment.getCrawlerLocalOutputPath());
    // append the segment id to the path ...
    return new File(crawlDataDir, CrawlEnvironment.ActiveCrawlLog);
  }

  /** get active log file path **/
  public static File getCheckpointPath(File directoryRoot) {
    // and construct a path to the local crawl segment directory ...
    File crawlDataDir = new File(directoryRoot, CrawlEnvironment.getCrawlerLocalOutputPath());
    // append the segment id to the path ...
    return new File(crawlDataDir, CrawlEnvironment.CheckpointCrawlLog);
  }

  public static void ensureDataDirectory(File directoryRoot) {
    // and construct a path to the local crawl segment directory ...
    File crawlDataDir = new File(directoryRoot, CrawlEnvironment.getCrawlerLocalOutputPath());

    if (!crawlDataDir.exists()) {
      crawlDataDir.mkdir();
    }
  }

  /** purge local data directory **/
  public static void purgeDataDirectory(File directoryRoot) {
    // get crawl output path ...
    File crawlDataDir = new File(directoryRoot, CrawlEnvironment.getCrawlerLocalOutputPath());
    // delete entire directory and all contents underneath it
    FileUtils.recursivelyDeleteFile(crawlDataDir);
    // recreate directory
    crawlDataDir.mkdirs();
  }

  /** unit test constructor **/
  public CrawlLog() throws IOException {
    _rootDirectory = new File(CrawlEnvironment.DEFAULT_DATA_DIR, "crawlLog_unittest");
    if (!_rootDirectory.exists())
      _rootDirectory.mkdir();
    _eventLoop = new EventLoop();
    _nodeName = "test";
    _eventLoop.start();
    _threadPool = Executors.newFixedThreadPool(1);

    initialize();

  }

  public CrawlLog(CrawlerEngine engine) throws IOException {

    _engine = engine;
    _rootDirectory = engine.getServer().getDataDirectory();
    _nodeName = engine.getServer().getHostName();
    _eventLoop = engine.getEventLoop();
    _threadPool = engine.getServer().getDefaultThreadPool();

    initialize();
  }

  private void initialize() throws IOException {

    // create data directory if necessary ...
    ensureDataDirectory(_rootDirectory);

    File checkpointLogPath = getCheckpointPath(_rootDirectory);
    File activeLogPath = getActivePath(_rootDirectory);

    // check if it exists ...
    if (checkpointLogPath.exists()) {
      // log it ...
      LOG.warn("####Checkpoint Crawl Log Found - Possible Crash Recovery");
      // rename it as the active log ...
      checkpointLogPath.renameTo(activeLogPath);
    }

    LOG.info("Crawl Log Initializing Active Log");
    // either way call initialize active log ...
    _header = initializeActiveLog(_rootDirectory);

    LOG.info("Crawl Log Initialize returned " + _header._itemCount + " Entries in Active Log");

  }

  /** initialize log (file) **/
  private static LogFileHeader initializeActiveLog(File rootDirectory) throws IOException {
    File activeLogPath = getActivePath(rootDirectory);
    return initializeLogFileHeaderFromLogFile(activeLogPath);
  }

  private static LogFileHeader initializeLogFileHeaderFromLogFile(File logFilePath) throws IOException {

    LogFileHeader headerOut = null;
    if (!logFilePath.exists()) {
      DataOutputStream outputStream = new DataOutputStream(new FileOutputStream(logFilePath));
      try {
        headerOut = initializeEmptyLogFile(outputStream);
      } finally {
        outputStream.close();
      }
    } else {
      headerOut = new LogFileHeader();

      DataInputStream inputStream = new DataInputStream(new FileInputStream(logFilePath));

      try {
        headerOut.readHeader(inputStream);
      } finally {
        inputStream.close();
      }
    }

    return headerOut;
  }

  /** get the host name **/
  public String getNodeName() {
    return _nodeName;
  }

  /** make packed log id from list id and segment log id **/
  public static long makeSegmentLogId(int listId, int segmentId) {
    return (((long) listId) << 32) | (long) segmentId;
  }

  /** get segment log id from packed id **/
  public static int getSegmentIdFromLogId(long logId) {
    return (int) (logId & 0xFFFFFFFF);
  }

  /** get list id from packed id **/
  public static int getListIdFromLogId(long logId) {
    return (int) ((logId >> 32) & 0xFFFFFFFF);
  }

  /** add a segment log given segment id **/
  public void addSegmentLog(CrawlSegmentLog log) {
    if (_loggers.get(log.getSegmentId()) != null) {
      LOG.error("Attempt to Activate an Already Active Segment Log. Segment Id:" + log.getSegmentId());
      throw new RuntimeException("Attempt to Activate an Already Active Segment Log. Segment Id:" + log.getSegmentId());
    }
    _loggers.put(makeSegmentLogId(log.getListId(), log.getSegmentId()), log);
  }

  /** get the special robots crawl segment **/
  public CrawlSegmentLog getRobotsSegment() {
    return _robotsSegment;
  }

  /** get a segment log given segment id **/
  public CrawlSegmentLog getLogForSegment(int listId, int segmentId) {
    return _loggers.get(makeSegmentLogId(listId, segmentId));
  }

  /** remove segment log **/
  public CrawlSegmentLog removeSegmentLog(int listId, int segmentId) {
    return _loggers.remove(makeSegmentLogId(listId, segmentId));
  }

  private static class LogFileHeader {

    public static final int LogFileHeaderBytes = 0xCC00CC00;
    public static final int LogFileVersion = 1;

    public LogFileHeader() {
      _fileSize = 0;
      _itemCount = 0;
    }

    public long _fileSize;
    public long _itemCount;

    public void writeHeader(DataOutput stream) throws IOException {
      stream.writeInt(LogFileHeaderBytes);
      stream.writeInt(LogFileVersion);
      stream.writeLong(_fileSize);
      stream.writeLong(_itemCount);
    }

    public void readHeader(DataInput stream) throws IOException {
      int headerBytes = stream.readInt();
      int version = stream.readInt();

      if (headerBytes != LogFileHeaderBytes && version != LogFileVersion) {
        throw new IOException("Invalid CrawlLog File Header Detected!");
      }
      _fileSize = stream.readLong();
      _itemCount = stream.readLong();
    }
  }

  private static void updateLogFileHeader(File logFileName, LogFileHeader header, long addedRecordCount)
      throws IOException {

    RandomAccessFile file = new RandomAccessFile(logFileName, "rw");

    try {

      // update cached header ...
      header._fileSize = file.getChannel().size();
      header._itemCount += addedRecordCount;
      // set the position at zero ..
      file.seek(0);
      // and write header to disk ...
      header.writeHeader(file);
    } finally {
      // major bottle neck..
      // file.getFD().sync();
      file.close();
    }
  }

  private static LogFileHeader initializeEmptyLogFile(DataOutput stream) throws IOException {

    LogFileHeader header = new LogFileHeader();
    header.writeHeader(stream);

    return header;
  }

  public static LogFileHeader readLogFileHeader(File logFileName) throws IOException {

    LogFileHeader headerOut = new LogFileHeader();
    RandomAccessFile file = new RandomAccessFile(logFileName, "r");
    try {
      headerOut = readLogFileHeader(file);
    } finally {
      file.close();
    }
    return headerOut;
  }

  private static LogFileHeader readLogFileHeader(DataInput reader) throws IOException {

    LogFileHeader headerOut = new LogFileHeader();

    headerOut.readHeader(reader);

    return headerOut;
  }

  private boolean isCheckpointInProgress() {
    return _checkpointCompletionCallback != null;
  }

  private boolean isFlushInProgress() {
    return _flushInProgress == true;
  }

  private void setFlushInProgress(boolean value) {
    _flushInProgress = value;

    if (value == false) {
      // since we are in the async thread at this point, check to see if a
      // checkpoint is in progress
      if (isCheckpointInProgress()) {
        // if so, it was deferred, because of the flush in progress... so we
        // need to actually kick off the checkpoint progress
        // now that the flush is complete
        doCheckpoint();
      }
    }
  }

  public static interface CheckpointCompletionCallback {

    public void checkpointComplete(long checkpointId, Vector<Long> completedSegmentList);

    public void checkpointFailed(long checkpointId, Exception e);

  }

  public static interface FlushCompletionCallback {
    public void flushComplete();

    public void flushFailed(Exception e);
  }

  /** essentially swap crawl logs **/
  private void checkpointLocalCrawlLog() throws IOException {
    File activeCrawlLog = getActivePath(_rootDirectory);
    File checkpointCrawlLog = getCheckpointPath(_rootDirectory);

    LOG.info("MOVING ACTIVE:" + activeCrawlLog + "TO:" + checkpointCrawlLog);
    // delete any existing checkpoint log ...
    checkpointCrawlLog.delete();
    // rename active log to check point log
    activeCrawlLog.renameTo(checkpointCrawlLog);
    // and create a new active crawlLog ...
    _header = initializeActiveLog(_rootDirectory);
  }

  public void checkpoint(long checkpointStartTime, CheckpointCompletionCallback callback, long checkpointId) {

    // first check to see if checkpoint is already in progress ...
    if (isCheckpointInProgress()) {
      // immediately fail call
      callback.checkpointFailed(checkpointId, new Exception("Invalid State. Checkpoint already in progress!"));
    }

    _lastCheckpointTime = checkpointStartTime;

    // otherwise transition to a checkpoint in progress state
    _checkpointCompletionCallback = callback;
    _checkpointId = checkpointId;

    // now check to see if we are not in the middle of a flush ...
    if (!isFlushInProgress()) {
      // if not we can directly start the actual checkpoint process ...
      doCheckpoint();
    }
    // otherwise wait for the flush to finish (and thus trigger the checkpoint
    // process)
  }

  public void finalizeCheckpoint() {
    File checkpointLogFile = getCheckpointPath(_rootDirectory);
    checkpointLogFile.delete();
  }

  public void abortCheckpoint() {
    File activeLogFile = getActivePath(_rootDirectory);
    File checkpointLogFile = getCheckpointPath(_rootDirectory);
    LOG.info("###ABORTING CHECKPOINT! RENAMING:" + checkpointLogFile + " TO:" + activeLogFile);
    checkpointLogFile.renameTo(activeLogFile);
  }

  public void purgeActiveLog() throws IOException {
    File activeLogFilePath = getActivePath(_rootDirectory);

    if (activeLogFilePath.exists())
      activeLogFilePath.delete();

    _header = initializeActiveLog(_rootDirectory);
  }

  private static class CorruptCrawlLogException extends IOException {

    public CorruptCrawlLogException(String description) {
      super(description);
    }
  }

  /**
   * seek out next instance of sync bytes in the file input stream
   * 
   * @param file
   * @throws IOException
   */
  private static boolean seekToNextSyncBytesPos(byte[] syncBytesBuffer, RandomAccessFile file, long maxFileSize)
      throws IOException {

    while (file.getFilePointer() < maxFileSize) {
      try {
        // read in a sync.length buffer amount
        file.read(syncBytesBuffer);

        int syncLen = SYNC_BYTES_SIZE;

        // start scan for next sync position ...
        for (int i = 0; file.getFilePointer() < maxFileSize; i++) {
          int j = 0;
          for (; j < syncLen; j++) {
            if (_sync[j] != syncBytesBuffer[(i + j) % syncLen])
              break;
          }
          if (j == syncLen) {
            // found matching sync bytes - reset file pos to before sync bytes
            file.seek(file.getFilePointer() - SYNC_BYTES_SIZE); // position
            // before
            // sync
            return true;
          }
          syncBytesBuffer[i % syncLen] = file.readByte();
        }
      } catch (IOException e) {
        LOG.warn("IOException at:" + file.getFilePointer() + " Exception:" + CCStringUtils.stringifyException(e));
        LOG.warn("Skipping to:" + file.getFilePointer() + 4096);
        file.seek(file.getFilePointer() + 4096);
      }
    }
    return false;
  }

  private static interface HDFSCrawlURLWriter {
    public void writeCrawlURLItem(Text url, CrawlURL urlObject) throws IOException;

    public void close() throws IOException;

    public List<Path> getFilenames();
  }

  private static class SequenceFileCrawlURLWriter implements HDFSCrawlURLWriter {

    private static final long FLUSH_THRESHOLD = 1073741824L;
    
    FileSystem _fs;
    Configuration _conf;
    Path _stagingDirectory;
    String _nodeName;
    long _currentFileRecordCount = 0;
    ArrayList<Path> _outputPaths = new ArrayList<Path>();

    long _nextFileId = -1L;
    Path currentFilePath = null;
    SequenceFile.Writer writer = null;
    long _prevPos;

    public SequenceFileCrawlURLWriter(Configuration conf, FileSystem fs, Path path, String nodeName, long checkpointId)
        throws IOException {
      _conf = conf;
      _fs = fs;
      _stagingDirectory = path;
      _nodeName = nodeName;
      _nextFileId = checkpointId;

      flushFile(true);
    }

    private void flushFile(boolean openNew) throws IOException {
      if (writer != null) {
        writer.close();
        if (_currentFileRecordCount != 0) {
          LOG.info("Flushed Temp Checkpoint File:" + currentFilePath);
          _outputPaths.add(currentFilePath);
        } else {
          LOG.info("Deleting Emtpy Checkpoint File:" + currentFilePath);
          _fs.delete(currentFilePath, false);
        }
        writer = null;
        _currentFileRecordCount = 0;
        currentFilePath = null;
      }

      if (openNew) {
        // allocate a new filename
        currentFilePath = new Path(_stagingDirectory, CrawlEnvironment.buildCrawlLogCheckpointName(_nodeName,
            _nextFileId++));
        LOG.info("Allocating new Checkpoint File:" + currentFilePath);
        // delete it
        if (_fs.exists(currentFilePath)) {
          LOG.warn("Existing Checkpoint TempFile found at:" + currentFilePath + " - Deleting");
          _fs.delete(currentFilePath, false);
        }
        // open a sequence file writer at the temp file location ...
        writer = SequenceFile.createWriter(_fs, _conf, currentFilePath, Text.class, CrawlURL.class,
            CompressionType.BLOCK, new SnappyCodec());
        // reset record count ...
        _currentFileRecordCount = 0;
      }
    }

    @Override
    public void writeCrawlURLItem(Text url, CrawlURL urlObject) throws IOException {
      writer.append(url, urlObject);
      ++_currentFileRecordCount;
      long pos = writer.getLength();
      if (pos != _prevPos) {
        _prevPos = pos;
        if (pos >= FLUSH_THRESHOLD) {
          flushFile(true);
        }
      }
    }

    public void close() throws IOException {
      flushFile(false);
    }

    public List<Path> getFilenames() {
      return _outputPaths;
    }
  };

  private static class URLWriterException extends IOException {
    public URLWriterException() {

    }
  }

  private static void transferLocalCheckpointLog(File crawlLogPath, HDFSCrawlURLWriter writer, long checkpointId)
      throws IOException {

    // and open the crawl log file ...
    RandomAccessFile inputStream = null;

    IOException exception = null;

    CRC32 crc = new CRC32();
    CustomByteArrayOutputStream buffer = new CustomByteArrayOutputStream(1 << 17);
    byte[] syncBytesBuffer = new byte[SYNC_BYTES_SIZE];

    // save position for potential debug output.
    long lastReadPosition = 0;

    try {
      inputStream = new RandomAccessFile(crawlLogPath, "rw");
      // and a data input stream ...
      RandomAccessFile reader = inputStream;
      // seek to zero
      reader.seek(0L);

      // read the header ...
      LogFileHeader header = readLogFileHeader(reader);

      // read a crawl url from the stream...

      while (inputStream.getFilePointer() < header._fileSize) {

        if (seekToNextSyncBytesPos(syncBytesBuffer, reader, header._fileSize)) {

          try {
            lastReadPosition = inputStream.getFilePointer();

            // skip sync
            inputStream.skipBytes(SYNC_BYTES_SIZE);

            // read length ...
            int urlDataLen = reader.readInt();
            long urlDataCRC = reader.readLong();

            if (urlDataLen > buffer.getBuffer().length) {
              buffer = new CustomByteArrayOutputStream(((urlDataLen / 65536) + 1) * 65536);
            }
            reader.read(buffer.getBuffer(), 0, urlDataLen);
            crc.reset();
            crc.update(buffer.getBuffer(), 0, urlDataLen);

            long computedValue = crc.getValue();

            // validate crc values ...
            if (computedValue != urlDataCRC) {
              LOG.error("CRC Mismatch Detected during HDFS transfer in CrawlLog:" + crawlLogPath.getAbsolutePath()
                  + " Checkpoint Id:" + checkpointId + " FilePosition:" + lastReadPosition);
              inputStream.seek(lastReadPosition + 1);
            } else {
              // allocate a crawl url data structure
              CrawlURL url = new CrawlURL();
              DataInputStream bufferReader = new DataInputStream(new ByteArrayInputStream(buffer.getBuffer(), 0,
                  urlDataLen));
              // populate it from the (in memory) data stream
              url.readFields(bufferReader);
              try {
                // and write out appropriate sequence file entries ...
                writer.writeCrawlURLItem(new Text(url.getUrl()), url);
              } catch (IOException e) {
                LOG.error("Failed to write CrawlURL to SequenceFileWriter with Exception:"
                    + CCStringUtils.stringifyException(e));
                throw new URLWriterException();
              }
            }
          } catch (URLWriterException e) {
            LOG.error("Caught URLRewriter Exception! - Throwing to outer layer!");
            throw e;
          } catch (Exception e) {
            LOG.error("Ignoring Error Processing CrawlLog Entry at Position:" + lastReadPosition + " Exception:"
                + CCStringUtils.stringifyException(e));
          }
        } else {
          break;
        }
      }
    } catch (EOFException e) {
      LOG.error("Caught EOF Exception during read of local CrawlLog:" + crawlLogPath.getAbsolutePath()
          + " Checkpoint Id:" + checkpointId + " FilePosition:" + lastReadPosition);
    } catch (IOException e) {
      LOG.error(CCStringUtils.stringifyException(e));
      exception = e;
      throw e;
    } finally {
      if (inputStream != null)
        inputStream.close();
    }
  }


  private Path transferLocalSegmentLog(FileSystem hdfs, File localSegmentLogFile, long checkpointId, int listId,
      int segmentId) throws IOException {

    if (localSegmentLogFile.exists()) {

      // determine the file's size ...
      // if > header size (in other words it has data ... )
      if (localSegmentLogFile.length() > CrawlSegmentLog.getHeaderSize()) {
        // construct a target path (where we are going to store the checkpointed
        // crawl log )
        Path remoteLogFileName = CrawlEnvironment.getRemoteCrawlSegmentLogCheckpointPath(new Path(CrawlEnvironment.getCrawlSegmentLogsDirectory()),getNodeName(), checkpointId, listId, segmentId);

        // replace if existing ... 
        hdfs.delete(remoteLogFileName,false);

        Path localPath = new Path(localSegmentLogFile.getAbsolutePath());
        
        hdfs.mkdirs(remoteLogFileName.getParent());
        
        LOG.info("Copying CrawlSegmentLog for List:" + listId + " Segment:" + segmentId + " from:" + localPath  + " to fs:" + hdfs + " path:" + remoteLogFileName);
        hdfs.copyFromLocalFile(localPath, remoteLogFileName);

        return remoteLogFileName;
      }
    }
    return null;
  }

  private void purgeHDFSSegmentLogs(FileSystem hdfs, int listId, int segmentId) throws IOException {

    Path listLogDirectory = new Path(CrawlEnvironment.getCrawlSegmentLogsDirectory(), ((Integer) listId).toString());
    Path segmentLogDirectory = new Path(listLogDirectory, ((Integer) segmentId).toString());
    Path completionLogFilePath = new Path(segmentLogDirectory, CrawlEnvironment
        .buildCrawlSegmentCompletionLogFileName(getNodeName()));

    if (!hdfs.exists(completionLogFilePath)) {
      // create a zero length completion log file on hdfs ...
      hdfs.createNewFile(completionLogFilePath);
    }

    // skip this step as history servers now manage segment logs
    /*
     * // and now ... delete all logs Path segmentLogWildcardPath = new
     * Path(segmentLogDirectory
     * ,CrawlEnvironment.buildCrawlSegmentLogCheckpointWildcardString
     * (getNodeName())); FileStatus paths[] =
     * hdfs.globStatus(segmentLogWildcardPath); if (paths != null) { for
     * (FileStatus path : paths) { // hdfs.delete(path.getPath()); } }
     */
  }

  /** perform the actual checkpoint work here ... **/
  private void doCheckpoint() {
    // at this point, we should be in the async thread, and all flusher
    // activities are blocked ...
    LOG.info("CrawlLog Checkpoint - Starting ");
    // collect all necessary information from thread-unsafe data structure now
    // (in async thread context)
    final Set<Long> activeSegments = new HashSet<Long>();

    try {
      // add all active segment ids to our key set ...
      activeSegments.addAll(_loggers.keySet());
      LOG.info("CrawlLog Checkpoint - Preparing CrawlLog Files");
      // checkpoint crawl log ...
      checkpointLocalCrawlLog();
      LOG.info("CrawlLog Checkpoint - Preparing Segment Log Files");
      // next checkpoint all active segment logs ...
      for (CrawlSegmentLog segmentLog : _loggers.values()) {
        segmentLog.checkpointLocalLog();
      }
      LOG.info("CrawlLog Checkpoint - Ready for HDFS Transfer");
    } catch (IOException e) {
      LOG.error("Checkpoint failed with Exception:" + CCStringUtils.stringifyException(e));
    }

    // spawn a thread to do most of the blocking io ...
    _threadPool.submit(new ConcurrentTask<Boolean>(_eventLoop,

    new Callable<Boolean>() {

      public Boolean call() throws Exception {

        // we need to track these in case of failure ...
        Vector<Path> segmentLogFinalPaths = new Vector<Path>();

        // get the log file system
        final FileSystem crawlLogsFS  = CrawlEnvironment.getDefaultFileSystem();

        try {

          LOG.info("CrawlLog Checkpoint - Transferring CrawlLog to HDFS");

          // construct a target path (where we are going to store the
          // checkpointed crawl log )
          //Path stagingDirectory = new Path(CrawlEnvironment.getCheckpointStagingDirectory());
          Path checkpointDirectory = CrawlerServer.getServer().getCrawlContentPath();
          LOG.info("***Checkpoint Dir is:" + checkpointDirectory);
          if (checkpointDirectory == null) { 
            throw new IOException("Checkpoint Failed. Null Checkpoint Directory!");
          }

          FileSystem crawlDataFS = FileSystem.get(checkpointDirectory.toUri(),CrawlEnvironment.getHadoopConfig());
          LOG.info("***Checkpoint Content FS is:" + crawlDataFS);
          SequenceFileCrawlURLWriter hdfsWriter = new SequenceFileCrawlURLWriter(CrawlEnvironment.getHadoopConfig(),
              crawlDataFS, checkpointDirectory, getNodeName(), _checkpointId);

          try {
            // write out crawl log to hdfs ...
            transferLocalCheckpointLog(getCheckpointPath(_rootDirectory), hdfsWriter, _checkpointId);
          } catch (Exception e) {
            LOG.error("HDFS Write of CrawlLog failed. Deleting tempFiles:" + hdfsWriter.getFilenames() + " Exception:"
                + CCStringUtils.stringifyException(e));

            // close writer
            hdfsWriter.close();
            // delete any hdfs output ...
            for (Path path : hdfsWriter.getFilenames()) {
              LOG.info("Deleting temp (HDFS) checkpoint file:" + path);
              crawlDataFS.delete(path, false);
            }
            throw e;
          } finally {
            hdfsWriter.close();
          }

          LOG.info("CrawlLog Checkpoint - Transferring CrawlSegment Logs");
          // and next for every segment
          for (long packedLogId : activeSegments) {

            File segmentLogPath = CrawlSegmentLog.buildCheckpointPath(_rootDirectory, getListIdFromLogId(packedLogId),
                getSegmentIdFromLogId(packedLogId));

            LOG.info("CrawlLog Checkpoint - Transferring CrawlSegment Log for List:" + getListIdFromLogId(packedLogId) + " Segment:"+ getSegmentIdFromLogId(packedLogId));
            // copy the segment log ...
            Path remoteLogFilePath 
                = transferLocalSegmentLog(crawlLogsFS, segmentLogPath, _checkpointId,getListIdFromLogId(packedLogId), getSegmentIdFromLogId(packedLogId));
            
            // if path is not null (data was copied) ...
            if (remoteLogFilePath != null) {
              // add it to vector ...
              segmentLogFinalPaths.add(remoteLogFilePath);
            }
          }
          LOG.info("CrawlLog Checkpoint - Finished Transferring CrawlSegment Logs");

          // now if we got here ... all hdfs transfers succeeded ...
          // go ahead and move checkpoint log from staging to final data
          // directory ...
          /* 
          Path checkpointDirectory = new Path(CrawlEnvironment.getCheckpointDataDirectory());

          // if no checkpoint data directory ... create one ...
          if (!hdfs.exists(checkpointDirectory))
            hdfs.mkdirs(checkpointDirectory);

          for (Path checkpointTempFilePath : hdfsWriter.getFilenames()) {
            Path checkpointFinalPath = new Path(checkpointDirectory, checkpointTempFilePath.getName());
            LOG.info("Promoting Checking File From:" + checkpointTempFilePath + " to:" + checkpointFinalPath);
            // and essentially move the crawl log file from staging to data
            // directory ..
            boolean success = hdfs.rename(checkpointTempFilePath, checkpointFinalPath);
            if (!success) {
              throw new IOException("Failed to Rename Checkpoint Temp:" + checkpointTempFilePath + " to:"
                  + checkpointFinalPath);
            }
          }
          */
          // if we got here checkpoint was successfull...
          return true;
        } catch (Exception e) {
          LOG.error("Checkpoint:" + _checkpointId + " FAILED with exception:" + CCStringUtils.stringifyException(e));
          for (Path segmentPath : segmentLogFinalPaths) {
            crawlLogsFS.delete(segmentPath,false);
          }
          throw e;
        }
      }
    },

    new CompletionCallback<Boolean>() {

      public void taskComplete(Boolean updateResult) {

        Vector<Long> completedSegmentList = new Vector<Long>();

        LOG.info("CrawlLog Checkpoint - Finalizing CrawlLog Checkpoint");
        // delete the local checkpoint log ...
        finalizeCheckpoint();

        LOG.info("CrawlLog Checkpoint - Finalizing CrawlSegmentLogs");
        for (CrawlSegmentLog segmentLog : _loggers.values()) {
          // LOG.info("CrawlLog Checkpoint - Finalizing CrawlSegmentLog for Segment:"
          // + segmentLog.getSegmentId());
          // finalize the checkpoint on the segment log ...
          segmentLog.finalizeCheckpoint();
          // and check to see if the segment has been completed ...
          if (segmentLog.isSegmentComplete()) {
            // if so, add it our completed segments list ...
            completedSegmentList.add(makeSegmentLogId(segmentLog.getListId(), segmentLog.getSegmentId()));
          }
        }

        // now for all completed segments ... purge hdfs logs ...
        for (long packedSegmentId : completedSegmentList) {
          try {
            LOG.info("CrawlLog Checkpoint - Purging HDFS CrawlSegmentLogs from Completed Segment. List:"
                + getListIdFromLogId(packedSegmentId) + " Segment:" + getSegmentIdFromLogId(packedSegmentId));
            // purge hdfs files (and create a completion log file)
            //purgeHDFSSegmentLogs(CrawlEnvironment.getDefaultFileSystem(), getListIdFromLogId(packedSegmentId),getSegmentIdFromLogId(packedSegmentId));
            LOG.info("CrawlLog Checkpoint - Purging Local CrawlSegmentLogs from Completed Segment. List:"
                + getListIdFromLogId(packedSegmentId) + " Segment:" + getSegmentIdFromLogId(packedSegmentId));
            // and purge local files as well ...
            _loggers.get(packedSegmentId).purgeLocalFiles();
          } catch (IOException e) {
            LOG.error("Purge SegmentLog for Segment List:" + getListIdFromLogId(packedSegmentId) + " Segment:"
                + getSegmentIdFromLogId(packedSegmentId) + " threw IOException:" + CCStringUtils.stringifyException(e));
          }
          LOG.info("CrawlLog Checkpoint - DeRegistering Segment List:" + getListIdFromLogId(packedSegmentId)
              + " Segment:" + getSegmentIdFromLogId(packedSegmentId) + " From CrawlLog");
          // no matter what ... unload the segment ...
          _loggers.remove(packedSegmentId);
        }

        CheckpointCompletionCallback callback = _checkpointCompletionCallback;
        long checkpointId = _checkpointId;

        // otherwise transition to a checkpoint in progress state
        _checkpointCompletionCallback = null;
        _checkpointId = -1;

        LOG.info("CrawlLog Checkpoint - Checkpoint Complete - Initiating Callback");

        // and complete transaction ...
        callback.checkpointComplete(checkpointId, completedSegmentList);

      }

      public void taskFailed(Exception e) {

        // all failures are critical in this particular task ...
        LOG.error("Crawl Log FLUSH Threw Exception:" + CCStringUtils.stringifyException(e));

        // revert checkpoint logs ...
        abortCheckpoint();

        for (CrawlSegmentLog segmentLog : _loggers.values()) {
          segmentLog.abortCheckpoint();
        }

        CheckpointCompletionCallback callback = _checkpointCompletionCallback;
        long checkpointId = _checkpointId;

        // otherwise transition to a checkpoint in progress state
        _checkpointCompletionCallback = null;
        _checkpointId = -1;

        // now check to see if this was corrupt crawl log exception
        if (e.getCause() instanceof CorruptCrawlLogException) {
          // ACK!!!
          LOG.fatal("Corrupt CrawlLog detected with Exception:" + CCStringUtils.stringifyException(e));

          try {
            // this is a serious error ... time to purge the crawl log directory
            // altogether ...
            purgeActiveLog();

            // and all active segment logs as well...
            for (CrawlSegmentLog segmentLog : _loggers.values()) {
              segmentLog.purgeActiveLog();
            }
          } catch (IOException e2) {
            LOG.error("IOException during Segment Log PURGE:" + CCStringUtils.stringifyException(e2));
          }

          // time to die hard ...
          throw new RuntimeException(e);
        }

        // and complete transaction ...
        callback.checkpointFailed(checkpointId, e);
      }
    }));
  }

  private static final class CustomByteArrayOutputStream extends ByteArrayOutputStream {
    public CustomByteArrayOutputStream(int initialSize) {
      super(initialSize);
    }

    public byte[] getBuffer() {
      return buf;
    }
  }

  private void logCrawlLogWrite(CrawlURL url, int bufferSizeOut) {
    StringBuffer sb = new StringBuffer();

    sb.append(String.format("%1$20.20s ", CCStringUtils.dateStringFromTimeValue(System.currentTimeMillis())));
    sb.append(String.format("%1$4.4s ", url.getResultCode()));
    sb.append(String.format("%1$10.10s ", url.getContentRaw().getCount()));
    if ((url.getFlags() & CrawlURL.Flags.IsRedirected) != 0) {
      sb.append(url.getRedirectURL());
      sb.append(" ");
    }
    sb.append(url.getUrl());
    _engine.getCrawlLogLog().info(sb.toString());
  }

  static byte[] _sync; // 16 random bytes
  static final int SYNC_BYTES_SIZE = 16;
  static {
    try {
      MessageDigest digester = MessageDigest.getInstance("MD5");
      digester.update("SOME RANDOM BYTES".getBytes());
      _sync = digester.digest();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static class SyncedCrawlURLLogWriter {

    boolean _injectErrors = false;
    boolean _corruptThisEntry = false;

    public SyncedCrawlURLLogWriter(boolean injectErrors) {
      _injectErrors = injectErrors;
    }

    public SyncedCrawlURLLogWriter() {

    }

    private CustomByteArrayOutputStream bufferOutputStream = new CustomByteArrayOutputStream(1 << 17);
    private DataOutputStream dataOutputStream = new DataOutputStream(bufferOutputStream);
    private CRC32 crc = new CRC32();

    public void writeItem(DataOutputStream crawlLogStream, CrawlURL url) throws IOException {

      bufferOutputStream.reset();
      // write to intermediate stream ...
      url.write(dataOutputStream);
      // and crc the data ...
      crc.reset();
      crc.update(bufferOutputStream.getBuffer(), 0, bufferOutputStream.size());
      // write out sync bytes first
      crawlLogStream.write(_sync);
      // write out length
      crawlLogStream.writeInt(bufferOutputStream.size());
      // crc next
      long computedValue = crc.getValue();
      if (_injectErrors) {
        _corruptThisEntry = !_corruptThisEntry;
        if (_corruptThisEntry) {
          LOG.info("Intentionally Corrupting URL:" + url.getUrl());
          computedValue += 12;
        }
      }
      crawlLogStream.writeLong(computedValue);
      // and then the data
      crawlLogStream.write(bufferOutputStream.getBuffer(), 0, bufferOutputStream.size());
    }
  }

  private void flushLog(final FlushCompletionCallback completionCallback) {
    if (Environment.detailLogEnabled())
      LOG.info("LOG_FLUSH:Collecting Entries....");
    // set flush in progress indicator ...
    setFlushInProgress(true);
    // and collect buffers in async thread context (thus not requiring
    // synchronization)
    final LinkedList<CrawlSegmentLog.LogItemBuffer> collector = new LinkedList<CrawlSegmentLog.LogItemBuffer>();
    // flush robots log
    _robotsSegment.flushLog(collector);
    // walk segments collecting log items ....
    for (CrawlSegmentLog logger : _loggers.values()) {
      // flush any log items into the collector
      logger.flushLog(collector);
    }
    if (Environment.detailLogEnabled())
      LOG.info("LOG_FLUSH:Collection Returned " + collector.size() + " Buffers");

    // walk collector list identifying the list of unique segment ids
    final Set<Long> packedSegmentIdSet = new HashSet<Long>();

    int urlItemCount = 0;

    for (CrawlSegmentLog.LogItemBuffer buffer : collector) {
      if (buffer.getListId() != -1 && buffer.getSegmentId() != -1) {
        packedSegmentIdSet.add(makeSegmentLogId(buffer.getListId(), buffer.getSegmentId()));
      }
      urlItemCount += buffer.getItemCount();
    }

    if (Environment.detailLogEnabled())
      LOG.info("LOG_FLUSH:There are  " + urlItemCount + " Items in Flush Buffer Associated With "
          + packedSegmentIdSet.size() + " Segments");

    final File crawlLogFile = getActivePath(_rootDirectory);

    // now check to see if there is anything to do ...
    if (collector.size() != 0) {
      if (Environment.detailLogEnabled())
        LOG.info("LOG_FLUSH: Collector Size is NOT Zero... Starting Log Flusher Thread");
      // ok ... time to spawn a thread to do the blocking flush io
      _threadPool.submit(new ConcurrentTask<Boolean>(_eventLoop,

      new Callable<Boolean>() {

        public Boolean call() throws Exception {

          if (Environment.detailLogEnabled())
            LOG.info("LOG_FLUSH: Log Flusher Thread Started");
          long startTime = System.currentTimeMillis();

          Map<Long, DataOutputStream> streamsMapByPackedId = new HashMap<Long, DataOutputStream>();
          Map<Long, Integer> recordCountsByPackedId = new HashMap<Long, Integer>();

          long crawlLogRecordCount = 0;

          // open the actual crawler log file ...
          final DataOutputStream crawlLogStream = new DataOutputStream(new FileOutputStream(crawlLogFile, true));

          try {
            if (Environment.detailLogEnabled())
              LOG.info("LOG_FLUSH: Log Flusher Thread Opening Streams for Segments in Buffer");
            // now open a set of file descriptors related to the identified
            // segments
            for (long packedSegmentId : packedSegmentIdSet) {
              // construct the unique filename for the given log file...
              File activeSegmentLog = CrawlSegmentLog.buildActivePath(_rootDirectory,
                  getListIdFromLogId(packedSegmentId), getSegmentIdFromLogId(packedSegmentId));
              // initialize the segment log ...
              CrawlSegmentLog.initializeLogFile(activeSegmentLog);
              // initialize record counts per stream ...
              recordCountsByPackedId.put(packedSegmentId, CrawlSegmentLog.readerHeader(activeSegmentLog));
              // and open an output stream for the specified log file ...
              streamsMapByPackedId.put(packedSegmentId, new DataOutputStream(new FileOutputStream(activeSegmentLog,
                  true)));
            }

            if (Environment.detailLogEnabled())
              LOG.info("LOG_FLUSH: Log Flusher Thread Walking Items in Buffer");

            // initialize a total item count variable
            int totalItemCount = 0;

            // crawl history stream
            DataOutputBuffer historyStream = new DataOutputBuffer();

            // and now walk log buffers ...
            for (CrawlSegmentLog.LogItemBuffer buffer : collector) {
              if (Environment.detailLogEnabled())
                LOG.info("LOG_FLUSH: Log Flusher Thread Writing " + buffer.getItemCount() + " Entries for Segment:"
                    + buffer.getSegmentId());

              // output stream
              DataOutputStream segmentLogStream = null;

              if (buffer.getListId() != -1 && buffer.getSegmentId() != -1) {
                // update segment count first ...
                recordCountsByPackedId.put(makeSegmentLogId(buffer.getListId(), buffer.getSegmentId()),
                    recordCountsByPackedId.get(makeSegmentLogId(buffer.getListId(), buffer.getSegmentId()))
                        + buffer.getItemCount());
                // get output stream associated with segment id
                segmentLogStream = streamsMapByPackedId
                    .get(makeSegmentLogId(buffer.getListId(), buffer.getSegmentId()));
              }

              // and our local record counter ...
              crawlLogRecordCount += buffer.getItemCount();

              // and next do the actual disk flush ...
              totalItemCount += buffer.flushToDisk(totalItemCount,

              new CrawlSegmentLog.LogItemBuffer.CrawlURLWriter() {

                SyncedCrawlURLLogWriter syncedLogWriter = new SyncedCrawlURLLogWriter();

                public void writeItem(CrawlURL url) throws IOException {
                  // log it
                  logCrawlLogWrite(url, url.getContentSize());
                  // write it
                  syncedLogWriter.writeItem(crawlLogStream, url);
                }

                public void writeItemCount(int entryCount) throws IOException {
                }

              }, segmentLogStream, historyStream);
            }

            if (Environment.detailLogEnabled())
              LOG.info("LOG_FLUSH: Log Flusher Finished Writing Entries To Disk");
            collector.clear();

          } catch (IOException e) {
            LOG.error("Critical Exception during Crawl Log Flush:" + CCStringUtils.stringifyException(e));
            throw e;
          } finally {
            if (crawlLogStream != null) {
              crawlLogStream.flush();
              crawlLogStream.close();
            }

            for (DataOutputStream stream : streamsMapByPackedId.values()) {
              if (stream != null)
                stream.flush();
              stream.close();
            }
          }
          // at this point... update the crawl log header ...
          try {
            if (Environment.detailLogEnabled())
              LOG.info("LOG_FLUSH: Updating Log File Headers");
            // update the log file header
            updateLogFileHeader(crawlLogFile, _header, crawlLogRecordCount);
            // and update each completion log header ...
            for (long packedSegmentId : recordCountsByPackedId.keySet()) {
              File activeSegmentLogPath = CrawlSegmentLog.buildActivePath(_rootDirectory,
                  getListIdFromLogId(packedSegmentId), getSegmentIdFromLogId(packedSegmentId));
              CrawlSegmentLog.writeHeader(activeSegmentLogPath, recordCountsByPackedId.get(packedSegmentId));
            }
          } catch (IOException e) {
            LOG.error("Criticial Exception during Crawl Log Fluhs:" + CCStringUtils.stringifyException(e));
            throw e;
          } finally {

          }

          long endTime = System.currentTimeMillis();

          _flushTimeAVG.addSample((double) endTime - startTime);
          _flushTimeSmoothed.addSample((double) endTime - startTime);
          _lastFlushTime = endTime - startTime;

          LOG.info("LOG_FLUSH: Log Flusher Flushed Successfully");
          return true;
        }
      },

      new CompletionCallback<Boolean>() {

        public void taskComplete(Boolean updateResult) {
          setFlushInProgress(false);
          if (completionCallback != null) {
            completionCallback.flushComplete();
          }
        }

        public void taskFailed(Exception e) {

          setFlushInProgress(false);

          if (completionCallback != null) {
            completionCallback.flushFailed(e);
          }

          // all failures are critical in this particular task ...
          LOG.fatal("Crawl Log FLUSH Threw Exception:" + CCStringUtils.stringifyException(e));

          // no matter ... it is time to CORE the server ...
          throw new RuntimeException("CRITICAL FAILURE: Crawl Log FLUSH Threw Exception:"
              + CCStringUtils.stringifyException(e));

        }
      }));
    } else {
      setFlushInProgress(false);
      if (completionCallback != null) {
        completionCallback.flushComplete();
      }
    }
  }

  public boolean isForcedCheckpointPossible() {
    // now one more check to see if we have enough items to do a checkpoint ...
    if (_header._itemCount != 0) {
      return true;
    }
    return false;
  }

  public boolean isCheckpointPossible(long currentTime) {

    if (_lastCheckpointTime == -1 || currentTime - _lastCheckpointTime >= CrawlerServer.getServer().getCrawlLogCheckpointInterval()) {

      // now one more check to see if we have enough items to do a checkpoint
      // ...
      if (_header._itemCount >= CrawlerServer.getServer().getCrawlLogCheckpointItemThreshold()
          || _header._fileSize >= CrawlerServer.getServer().getCrawlLogCheckpointLogSizeThreshold()) {
        return true;
      }
    }
    return false;
  }

  public void forceFlushAndCheckpointLog(final CheckpointCompletionCallback outerCallback) {
    if (isCheckpointInProgress() || isFlushInProgress()) {
      throw new RuntimeException("forceFlush called while active Checkpoint or Flush In Progress!!");
    }

    flushLog(new FlushCompletionCallback() {

      @Override
      public void flushComplete() {

        long currentTime = System.currentTimeMillis();

        LOG.info("LOG_FLUSH Flush Complete... Checking to see if Checkpoint Possilbe");
        if (isForcedCheckpointPossible()) {
          // yes .. go ahead and checkpoint log
          LOG.info("Checkpointing Logs to HDFS");
          // start the checkpoint ...
          checkpoint(currentTime, new CheckpointCompletionCallback() {

            public void checkpointComplete(long checkpointId, Vector<Long> completedSegmentList) {
              LOG.info("CrawlLog Checkpoint:" + checkpointId + " completed");

              if (completedSegmentList != null) {
                // walk completed segments ... updating their crawl state ...
                if (_engine != null) {
                  for (long packedSegmentId : completedSegmentList) {
                    // notify crawler engine of status change ...
                    _engine.crawlSegmentComplete(packedSegmentId);
                  }
                }
              }
              // ok initiate outer callback
              outerCallback.checkpointComplete(checkpointId, null);
            }

            public void checkpointFailed(long checkpointId, Exception e) {
              LOG.error("Checkpoint Failed for Checkpoint:" + checkpointId + " With Exception:"
                  + CCStringUtils.stringifyException(e));
              outerCallback.checkpointFailed(checkpointId, e);
            }

          }, currentTime);
        } else {
          if (Environment.detailLogEnabled())
            LOG.info("Checkpoint Skipped. Nothing to checkpoint");
          outerCallback.checkpointComplete(0, null);
        }
      }

      @Override
      public void flushFailed(Exception e) {
        // log error and bail ...
        LOG.error(CCStringUtils.stringifyException(e));
        // initiate callback
        outerCallback.checkpointFailed(0, e);
      }

    });
  }

  public void startLogFlusher() {

    _logFlusherTimer = new Timer(CrawlerServer.getServer().getCrawlLogFlushInterval(), true, new Timer.Callback() {

      public void timerFired(Timer timer) {
        // if checkpoint is NOT in progress ...
        if (!isCheckpointInProgress() && !isFlushInProgress()) {

          LOG.info("LOG_FLUSH Starting ...");

          flushLog(

          new FlushCompletionCallback() {

            public void flushComplete() {
              // flush is complete ... check to see if we want to do a
              // checkpoint ...
              long currentTime = System.currentTimeMillis();

              LOG.info("LOG_FLUSH Flush Complete... Checking to see if Checkpoint Possilbe");
              if (isCheckpointPossible(currentTime)) {

                LOG.info("Checkpointing Logs to HDFS");

                // pause fetcher to prevent race condition where log flush takes
                // a long time and causes the fetcher to consume all avaliable
                // memory with content buffers
                _engine.pauseFetch();

                // start the checkpoint ...
                checkpoint(currentTime, new CheckpointCompletionCallback() {

                  public void checkpointComplete(long checkpointId, Vector<Long> completedSegmentList) {
                    LOG.info("CrawlLog Checkpoint:" + checkpointId + " completed");

                    _engine.resumeFetch();

                    if (completedSegmentList != null) {
                      // walk completed segments ... updating their crawl state
                      // ...
                      if (_engine != null) {
                        for (long packedSegmentId : completedSegmentList) {
                          // notify crawler engine of status change ...
                          _engine.crawlSegmentComplete(packedSegmentId);
                        }
                      }
                    }
                  }

                  public void checkpointFailed(long checkpointId, Exception e) {

                    _engine.resumeFetch();

                    LOG.error("Checkpoint Failed for Checkpoint:" + checkpointId + " With Exception:"
                        + CCStringUtils.stringifyException(e));
                  }

                }, currentTime);
              }
            }

            public void flushFailed(Exception e) {
              LOG.error("Flush Failed with Exception:" + CCStringUtils.stringifyException(e));
            }

          }

          );

          _engine.resumeFetch();
        }

        // now
      }
    });

    _eventLoop.setTimer(_logFlusherTimer);
  }

  public interface LogFlusherStopActionCallback {
    public void stopComplete();
  }

  public void stopLogFlusher(final LogFlusherStopActionCallback completionCallback) {

    // indicate that a shutdown is in progress ...
    _shutdownInProgress = true;

    // stop the log flusher timer ...
    if (_logFlusherTimer != null) {
      _eventLoop.cancelTimer(_logFlusherTimer);
    }

    // create a polling timer ...
    final Timer waitTimer = new Timer(1000, true, new Timer.Callback() {

      public void timerFired(Timer timer) {

        // check to see if we are done flushing or checkpointing ...
        if (!isFlushInProgress() && !isCheckpointInProgress()) {
          LOG.info("CrawlLog - stopLog Timer - No Flush or Checkpoint in Progress... Initiating CrawlLog Shutdown");
          // good to go ... cancel timer first ...
          _eventLoop.cancelTimer(timer);
          // and cleanup ...
          _logFlusherTimer = null;
          _shutdownInProgress = false;
          // initiate callback ...
          completionCallback.stopComplete();
        } else {
          LOG.info("CrawlLog - stopLog Timer - Flush or Checkpoint in Progress... Waiting ... ");
        }
      }
    });
    // and start the timer ...
    _eventLoop.setTimer(waitTimer);
  }

  public void collectStats(RuntimeStatsCollector collector) {

    collector.setDoubleValue(CrawlerEngineStats.ID, CrawlerEngineStats.Name.CrawlLog_FlushTimeAVG, _flushTimeAVG
        .getAverage());
    collector.setDoubleValue(CrawlerEngineStats.ID, CrawlerEngineStats.Name.CrawlLog_FlushTimeSmoothed,
        _flushTimeSmoothed.getAverage());
    collector.setLongValue(CrawlerEngineStats.ID, CrawlerEngineStats.Name.CrawlLog_FlushTimeLast, _lastFlushTime);
  }

  private static CrawlSegmentHost createHost(String hostName) {
    CrawlSegmentHost host = new CrawlSegmentHost();
    host.setHostName(hostName);
    byte[] hostNameAsBytes = host.getHostName().getBytes();
    host.setHostFP(FPGenerator.std64.fp(hostNameAsBytes, 0, hostNameAsBytes.length));
    return host;
  }

  private static CrawlSegmentURL createSegmentURL(URL url) {
    CrawlSegmentURL segmentURL = new CrawlSegmentURL();
    segmentURL.setUrl(url.toString());
    byte[] urlAsBytes = segmentURL.getUrl().getBytes();
    segmentURL.setUrlFP(FPGenerator.std64.fp(urlAsBytes, 0, urlAsBytes.length));
    return segmentURL;
  }

  private static CrawlSegmentDetail loadCrawlSegment(String fileName) throws IOException {

    TreeMap<String, CrawlSegmentHost> hosts = new TreeMap<String, CrawlSegmentHost>();

    URL resourceURL = CrawlEnvironment.getHadoopConfig().getResource(fileName);

    if (resourceURL == null) {
      throw new FileNotFoundException();
    }
    InputStream stream = resourceURL.openStream();
    BufferedReader reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(stream)));

    String line = null;

    do {
      line = reader.readLine();
      if (line != null) {
        if (Environment.detailLogEnabled())
          LOG.info(line);
        try {
          URL theURL = new URL(line);

          CrawlSegmentHost host = hosts.get(theURL.getHost());
          if (host == null) {

            host = createHost(theURL.getHost());

            hosts.put(theURL.getHost(), host);
          }
          CrawlSegmentURL segmentURL = createSegmentURL(theURL);
          host.getUrlTargets().add(segmentURL);
        } catch (MalformedURLException e) {
          LOG.error("SKIPPING Malformed URL::" + line);
        }
      }
    } while (line != null);

    CrawlSegmentDetail crawlSegmentDetail = new CrawlSegmentDetail();

    int urlCount = 0;
    crawlSegmentDetail.setSegmentId(1);
    for (CrawlSegmentHost host : hosts.values()) {
      crawlSegmentDetail.getHosts().add(host);
      urlCount += host.getUrlTargets().size();
    }

    crawlSegmentDetail.setUrlCount(urlCount);

    // finally, sort by host (as will be the case in a proper map reduce
    // produced segment ...
    Collections.sort(crawlSegmentDetail.getHosts());

    return crawlSegmentDetail;

  }

  public Vector<Long> getActiveSegmentIdList() {

    Vector<Long> segmentIdList = new Vector<Long>();
    segmentIdList.addAll(_loggers.keySet());
    return segmentIdList;
  }

  static void validateInputOutputCrawlURLArrays(ArrayList<CrawlURL> input, ArrayList<CrawlURL> output)
      throws IOException {
    Assert.assertTrue(input.size() == output.size());
    for (int i = 0; i < input.size(); ++i) {
      CrawlURL left = input.get(i);
      CrawlURL right = input.get(i);
      Assert.assertTrue(left.getUrl().equals(right.getUrl()));
      Assert.assertTrue(left.getContentRaw().getReadOnlyBytes().equals(right.getContentRaw().getReadOnlyBytes()));
    }
  }

  static void validateLogFlusherCode(final File localDirPath, final Path remotePath, boolean injectErrors)
      throws IOException {

    final Configuration conf = new Configuration();

    final FileSystem fs = FileSystem.get(conf);

    fs.mkdirs(remotePath);

    // ok create a crawlLog test file
    File localFile = File.createTempFile("crawlLog", "test", localDirPath);
    localFile.delete();

    LOG.info("Initializing Temp File:" + localFile);
    // initialize
    LogFileHeader fileHeader = initializeLogFileHeaderFromLogFile(localFile);

    LOG.info("Creating SyncedCrawl URL Writer");
    // create synced url writer ...
    SyncedCrawlURLLogWriter crawlURLWriter = new SyncedCrawlURLLogWriter(injectErrors);

    ArrayList<CrawlURL> urlObjects = new ArrayList<CrawlURL>();
    // write a couple of url objects
    for (int i = 0; i < 100; ++i) {
      CrawlURL url = new CrawlURL();
      url.setUrl("http://someurl.com/" + i);
      byte bytes[] = MD5.digest("Some Random:" + Math.random() + " Number").getBytes();
      url.setContentRaw(new FlexBuffer(bytes));
      final DataOutputStream crawlLogStream = new DataOutputStream(new FileOutputStream(localFile, true));
      try {
        LOG.info("Appending object to log");
        crawlURLWriter.writeItem(crawlLogStream, url);
      } finally {
        LOG.info("Flushing Log");
        crawlLogStream.flush();
        crawlLogStream.close();
      }
      LOG.info("Updating Header");
      updateLogFileHeader(localFile, fileHeader, 1);

      if (!injectErrors || i % 2 == 0) {
        urlObjects.add(url);
      } else {
        // drop odd entry
        LOG.info("Dropping Odd Entry:" + url.getUrl());
      }
    }

    final ArrayList<CrawlURL> urlObjectsOut = new ArrayList<CrawlURL>();

    HDFSCrawlURLWriter stubWriter = new HDFSCrawlURLWriter() {

      SequenceFileCrawlURLWriter innerWriter = new SequenceFileCrawlURLWriter(conf, fs, remotePath, "testNode", 1L);

      @Override
      public void writeCrawlURLItem(Text url, CrawlURL urlObject) throws IOException {
        LOG.info("Got URL:" + url.toString());
        urlObjectsOut.add(urlObject);
        innerWriter.writeCrawlURLItem(url, urlObject);
      }

      @Override
      public void close() throws IOException {
        innerWriter.close();
      }

      public List<Path> getFilenames() {
        return innerWriter.getFilenames();
      }
    };

    try {
      LOG.info("Transferring from Local to Remote");
      transferLocalCheckpointLog(localFile, stubWriter, 1L);
    } finally {
      stubWriter.close();
    }
    LOG.info("Validating Input/Output");
    validateInputOutputCrawlURLArrays(urlObjects, urlObjectsOut);
    // read via sequenceFile
    urlObjectsOut.clear();
    Path firstFile = Iterators.getNext(stubWriter.getFilenames().iterator(), null);
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, firstFile, conf);
    Text key = new Text();
    CrawlURL value = new CrawlURL();
    while (reader.next(key, value)) {
      LOG.info("Got:" + key.toString());
      urlObjectsOut.add(value);
      value = new CrawlURL();
    }
    reader.close();
    LOG.info("Validating Input/Output");
    validateInputOutputCrawlURLArrays(urlObjects, urlObjectsOut);

    LOG.info("Done!");
  }

  public static void walkCrawlLogFile(File crawlLogPath, long startOffset) throws IOException {

    // and open the crawl log file ...
    RandomAccessFile inputStream = null;

    IOException exception = null;

    CRC32 crc = new CRC32();
    CustomByteArrayOutputStream buffer = new CustomByteArrayOutputStream(1 << 17);
    byte[] syncBytesBuffer = new byte[SYNC_BYTES_SIZE];

    // save position for potential debug output.
    long lastReadPosition = 0;

    try {
      inputStream = new RandomAccessFile(crawlLogPath, "rw");

      // and a data input stream ...
      RandomAccessFile reader = inputStream;
      // seek to zero
      reader.seek(0L);

      // read the header ...
      LogFileHeader header = readLogFileHeader(reader);

      System.out.println("Header ItemCount:" + header._itemCount + " FileSize:" + header._fileSize);

      if (startOffset != 0L) {
        System.out.println("Preseeking to:" + startOffset);
        reader.seek(startOffset);
      }

      Configuration conf = new Configuration();

      // read a crawl url from the stream...

      long recordCount = 0;
      while (inputStream.getFilePointer() < header._fileSize) {

        // System.out.println("PRE-SYNC SeekPos:"+
        // inputStream.getFilePointer());
        if (seekToNextSyncBytesPos(syncBytesBuffer, reader, header._fileSize)) {

          // System.out.println("POST-SYNC SeekPos:"+
          // inputStream.getFilePointer());

          lastReadPosition = inputStream.getFilePointer();

          // skip sync
          inputStream.skipBytes(SYNC_BYTES_SIZE);

          // read length ...
          int urlDataLen = reader.readInt();
          long urlDataCRC = reader.readLong();

          if (urlDataLen > buffer.getBuffer().length) {
            buffer = new CustomByteArrayOutputStream(((urlDataLen / 65536) + 1) * 65536);
          }
          reader.read(buffer.getBuffer(), 0, urlDataLen);
          crc.reset();
          crc.update(buffer.getBuffer(), 0, urlDataLen);

          long computedValue = crc.getValue();

          // validate crc values ...
          if (computedValue != urlDataCRC) {
            LOG.error("CRC Mismatch Detected during HDFS transfer in CrawlLog:" + crawlLogPath.getAbsolutePath()
                + " FilePosition:" + lastReadPosition);
            inputStream.seek(lastReadPosition + 1);
          } else {
            if (recordCount++ % 10000 == 0) {
              // allocate a crawl url data structure
              CrawlURL url = new CrawlURL();
              DataInputStream bufferReader = new DataInputStream(new ByteArrayInputStream(buffer.getBuffer(), 0,
                  urlDataLen));
              // populate it from the (in memory) data stream
              url.readFields(bufferReader);

              System.out.println("Record:" + recordCount + " At:" + lastReadPosition + " URL:" + url.getUrl()
                  + " BuffSize:" + urlDataLen + " ContentLen:" + url.getContentRaw().getCount() + " LastModified:"
                  + new Date(url.getLastAttemptTime()).toString());
            }
          }
        } else {
          break;
        }
      }
    } catch (EOFException e) {
      LOG.error("Caught EOF Exception during read of local CrawlLog:" + crawlLogPath.getAbsolutePath()
          + " FilePosition:" + lastReadPosition);
    } catch (IOException e) {
      LOG.error(CCStringUtils.stringifyException(e));
      exception = e;
      throw e;
    } finally {
      if (inputStream != null)
        inputStream.close();
    }
  }
}
