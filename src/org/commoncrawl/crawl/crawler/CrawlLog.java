/**
 * Copyright 2008 - CommonCrawl Foundation
 * 
 * CommonCrawl licenses this file to you under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.commoncrawl.crawl.crawler;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
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
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.zip.CRC32;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.commoncrawl.async.ConcurrentTask;
import org.commoncrawl.async.ConcurrentTask.CompletionCallback;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.async.Timer;
import org.commoncrawl.common.Environment;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.protocol.CrawlSegmentDetail;
import org.commoncrawl.protocol.CrawlSegmentHost;
import org.commoncrawl.protocol.CrawlSegmentURL;
import org.commoncrawl.protocol.CrawlURL;
import org.commoncrawl.util.internal.RuntimeStatsCollector;
import org.commoncrawl.util.shared.CCStringUtils;
import org.commoncrawl.util.shared.FPGenerator;
import org.commoncrawl.util.shared.FileUtils;
import org.commoncrawl.util.shared.MovingAverage;
import org.commoncrawl.util.shared.SmoothedAverage;
import org.junit.Test;

/**
 * CrawlLog - A write ahead log that stores all crawler state up to a checkpoint
 * 
 * @author rana
 *
 */
public final class CrawlLog {

  public static final Log LOG = LogFactory.getLog(CrawlLog.class);
  
  private static final int LOG_FLUSH_INTERVAL = 30000;
  
  private static final int LOG_CHECKPOINT_INTERVAL = 60000 * 5;
  
  private static final int LOG_FILE_CHECKPOINT_ITEM_COUNT_THRESHOLD = 100000;
 
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
  CrawlSegmentLog _robotsSegment = new CrawlSegmentLog(null,-1,-1,null);
  
  /** individual crawl segment loggers **/
  Map<Long,CrawlSegmentLog> _loggers =new HashMap<Long,CrawlSegmentLog>();
  
  /** checkpoint completion callback **/
  CheckpointCompletionCallback _checkpointCompletionCallback = null;
  
  /** checkpoint id - analogous to parse segment id **/
  long   _checkpointId;
  
  /** flush in progress flag **/
  boolean _flushInProgress = false;
  
  /** a shutdown operation is in progress **/
  boolean _shutdownInProgress =false;
  
  /** log flusher timer **/
  Timer _logFlusherTimer = null;
  
  /** last checkpoint time **/
  long  _lastCheckpointTime = -1;
  
  MovingAverage         _flushTimeAVG = new MovingAverage(10);
  SmoothedAverage    _flushTimeSmoothed = new SmoothedAverage(.8);
  long                       _lastFlushTime  =0;
  
  /** get active log file path **/
  public static File getActivePath(File directoryRoot) { 
    // and construct a path to the local crawl segment directory ... 
    File crawlDataDir = new File(directoryRoot,CrawlEnvironment.getCrawlerLocalOutputPath());
    // append the segment id to the path ... 
    return new File(crawlDataDir,CrawlEnvironment.ActiveCrawlLog);
  }

  /** get active log file path **/
  public static File getCheckpointPath(File directoryRoot) { 
    // and construct a path to the local crawl segment directory ... 
    File crawlDataDir = new File(directoryRoot,CrawlEnvironment.getCrawlerLocalOutputPath());
    // append the segment id to the path ... 
    return new File(crawlDataDir,CrawlEnvironment.CheckpointCrawlLog);
  }
  
  public static void ensureDataDirectory(File directoryRoot) { 
    // and construct a path to the local crawl segment directory ... 
    File crawlDataDir = new File(directoryRoot,CrawlEnvironment.getCrawlerLocalOutputPath());
    
    if (!crawlDataDir.exists()) { 
      crawlDataDir.mkdir();
    }
  }
  
  /** purge local data directory **/
  public static void purgeDataDirectory(File directoryRoot) { 
    // get crawl output path ... 
    File crawlDataDir = new File(directoryRoot,CrawlEnvironment.getCrawlerLocalOutputPath());
    // delete entire directory and all contents underneath it 
    FileUtils.recursivelyDeleteFile(crawlDataDir);
    // recreate directory 
    crawlDataDir.mkdirs();
  }
  
  /** unit test constructor **/
  public CrawlLog() throws IOException { 
    _rootDirectory = new File(CrawlEnvironment.DEFAULT_DATA_DIR,"crawlLog_unittest");
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
    _nodeName    = engine.getServer().getHostName();
    _eventLoop = engine.getEventLoop();
    _threadPool = engine.getServer().getDefaultThreadPool();
    
    initialize();
  }
  
  private void initialize() throws IOException { 

    // create data directory if necessary ... 
    ensureDataDirectory(_rootDirectory);
    
    File checkpointLogPath = getCheckpointPath(_rootDirectory);
    File activeLogPath        = getActivePath(_rootDirectory);
    
    // check if it exists ... 
    if (checkpointLogPath.exists()){
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
  private static LogFileHeader initializeActiveLog(File rootDirectory)throws IOException { 
    File activeLogPath = getActivePath(rootDirectory);
    LogFileHeader headerOut = null;
    if (!activeLogPath.exists()) { 
      DataOutputStream outputStream = new DataOutputStream(new FileOutputStream(activeLogPath));
      try {
        headerOut = initializeEmptyLogFile(outputStream);
      }
      finally { 
        outputStream.close();
      }
    }
    else { 
      headerOut = new LogFileHeader();

      DataInputStream inputStream = new DataInputStream(new FileInputStream(activeLogPath));
      
      try { 
        headerOut.readHeader(inputStream);
      }
      finally { 
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
  public static long makeSegmentLogId(int listId,int segmentId) { 
    return (((long)listId) << 32) | (long)segmentId;
  }

  /** get segment log id from packed id**/
  public static int getSegmentIdFromLogId(long logId) { 
    return (int) (logId & 0xFFFFFFFF);
  }
  /** get list id from packed id**/
  public static int getListIdFromLogId(long logId) { 
    return (int) ((logId >> 32) & 0xFFFFFFFF);
  }

  
  
  /** add a segment log given segment id **/
  public void addSegmentLog(CrawlSegmentLog log) { 
    if (_loggers.get(log.getSegmentId()) != null) { 
      LOG.error("Attempt to Activate an Already Active Segment Log. Segment Id:" + log.getSegmentId());
      throw new RuntimeException("Attempt to Activate an Already Active Segment Log. Segment Id:" + log.getSegmentId());
    }
    _loggers.put(makeSegmentLogId(log.getListId(),log.getSegmentId()), log);
  }

  /** get the special robots crawl segment **/
  public CrawlSegmentLog getRobotsSegment() { 
    return _robotsSegment; 
  }
  
  /** get a segment log given segment id **/
  public CrawlSegmentLog getLogForSegment(int listId,int segmentId) { 
    return _loggers.get(makeSegmentLogId(listId,segmentId));
  }
  
  /** remove segment log **/
  public CrawlSegmentLog removeSegmentLog(int listId,int segmentId) { 
    return _loggers.remove(makeSegmentLogId(listId,segmentId));
  }
  
  private static class LogFileHeader {
    
    public static final int LogFileHeaderBytes = 0xCC00CC00;
    public static final int LogFileVersion         = 1;
    
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
      int version         = stream.readInt();
      
      if (headerBytes != LogFileHeaderBytes && version !=LogFileVersion) { 
        throw new IOException("Invalid CrawlLog File Header Detected!");
      }
      _fileSize = stream.readLong();
      _itemCount = stream.readLong();
    }
  }
  
  private void updateLogFileHeader(File logFileName, long addedRecordCount)throws IOException {
    
    RandomAccessFile file = new RandomAccessFile(logFileName,"rw");
    
    try { 
      
      // update cached header ... 
      _header._fileSize = file.getChannel().size();
      _header._itemCount += addedRecordCount;
      // set the position at zero .. 
      file.seek(0);
      // and write header to disk ... 
      _header.writeHeader(file);
    }
    finally {
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
    RandomAccessFile file = new RandomAccessFile(logFileName,"r");
    try { 
      headerOut = readLogFileHeader(file);
    }
    finally { 
      file.close();
    }
    return headerOut;
  }
  
  private static LogFileHeader readLogFileHeader(DataInput reader)throws IOException { 
    
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
      // since we are in the async thread at this point, check to see if a checkpoint is in progress
      if (isCheckpointInProgress()) { 
        // if so, it was deferred, because of the flush in progress... so we need to actually kick off the checkpoint progress
        // now that the flush is complete 
        doCheckpoint();
      }
    }
  }
  
  public static interface CheckpointCompletionCallback { 
    
    public void checkpointComplete(long checkpointId,Vector<Long> completedSegmentList);
    public void checkpointFailed(long checkpointId,Exception e);

  }
  
  public static interface FlushCompletionCallback { 
    public void flushComplete();
    public void flushFailed(Exception e);
  }
  
  /** essentially swap crawl logs **/
  private void checkpointLocalCrawlLog() throws IOException { 
    File activeCrawlLog        = getActivePath(_rootDirectory);
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
      //immediately fail call 
      callback.checkpointFailed(checkpointId,new Exception("Invalid State. Checkpoint already in progress!"));
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
    // otherwise wait for the flush to finish (and thus trigger the checkpoint process)
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
  
  public void purgeActiveLog()throws IOException { 
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
  
  private Path transferLocalCheckpointLog(FileSystem hdfs,File crawlLogPath, long checkpointId) throws IOException { 
    
    // construct a target path (where we are going to store the checkpointed crawl log )
    Path stagingDirectory      = new Path(CrawlEnvironment.getCheckpointStagingDirectory());
    Path tempFileName       = new Path(stagingDirectory,CrawlEnvironment.buildCrawlLogCheckpointName(getNodeName(),checkpointId));
    
    // delete the temp file ... 
    hdfs.delete(tempFileName);
    
    // open a sequence file writer at the temp file location ...
    SequenceFile.Writer writer = new SequenceFile.Writer(hdfs,CrawlEnvironment.getHadoopConfig(),tempFileName,Text.class,CrawlURL.class);
    // and open the crawl log file ...
    FileInputStream inputStream = null;
    
    IOException exception = null;
    
    CRC32 crc = new CRC32();
    CustomByteArrayOutputStream buffer = new CustomByteArrayOutputStream(1 << 17);
    
    try { 
      inputStream = new FileInputStream(crawlLogPath);
      // and a data input stream ...
      DataInputStream reader = new DataInputStream(inputStream);
      
      // read the header ... 
      LogFileHeader header =  readLogFileHeader(reader);
      
      // we can only read up to file size specified ... get the channel to keep track of position 
      FileChannel channel = inputStream.getChannel();

      // read a crawl url from the stream... 
      
      while (channel.position() < header._fileSize) { 
        
        // save position for potential debug output.
        long readPosition = channel.position();
        // read length ... 
        int urlDataLen = reader.readInt();
        long urlDataCRC = reader.readLong();
        
        if (urlDataLen > buffer.getBuffer().length) { 
          buffer = new  CustomByteArrayOutputStream( ((urlDataLen / 65536) + 1) * 65536 );
        }
        reader.read(buffer.getBuffer(), 0, urlDataLen);
        crc.reset();
        crc.update(buffer.getBuffer(), 0, urlDataLen);
        
        long computedValue = crc.getValue();
        
        // validate crc values ... 
        if (computedValue != urlDataCRC) { 
          throw new CorruptCrawlLogException("CRC Mismatch Detected during HDFS transfer in CrawlLog:" + crawlLogPath.getAbsolutePath() + " Checkpoint Id:" + checkpointId + " FilePosition:" + readPosition);
        }
        else { 
          // allocate a crawl url data structure 
          CrawlURL url = new CrawlURL();
          DataInputStream bufferReader = new DataInputStream(new ByteArrayInputStream(buffer.getBuffer(),0,urlDataLen));
          //populate it from the (in memory) data stream
          url.readFields(bufferReader);
          // and write out appropriate sequence file entries ...
          writer.append(new Text(url.getUrl()),url);
        }
      }
    }
    catch (IOException e) {
    	LOG.error(CCStringUtils.stringifyException(e));
      exception = e;
      throw e;
    }
    finally {
    	if (inputStream != null) 
    		inputStream.close();
    	if (writer != null) 
    		writer.close();

      // if there was an exception ... 
      if (exception != null) { 
        LOG.error("HDFS Write of CrawlLog failed with Exception:" + CCStringUtils.stringifyException(exception));
        // delete any hdfs output ... 
        hdfs.delete(tempFileName);
      }
    }
    return tempFileName;
  }
  
  private Path getFinalSegmentLogPath(FileSystem hdfs,long checkpointId,int listId, int segmentId) throws IOException {
    Path listLogDirectory         = new Path(CrawlEnvironment.getCrawlSegmentDataDirectory(),((Integer)listId).toString());
    Path segmentLogDirectory      = new Path(listLogDirectory,((Integer)segmentId).toString());
    Path completionLogFilePath    = new Path(segmentLogDirectory,CrawlEnvironment.buildCrawlSegmentLogCheckpointFileName(getNodeName(),checkpointId));
    
    return completionLogFilePath;
  }
  
  private Path transferLocalSegmentLog(FileSystem hdfs,File localSegmentLogFile, long checkpointId,int listId, int segmentId) throws IOException { 

    if (localSegmentLogFile.exists()) { 
      
      // determine the file's size ... 
      // if > header size (in other words it has data ... )
      if (localSegmentLogFile.length() > CrawlSegmentLog.getHeaderSize()) {  
        // construct a target path (where we are going to store the checkpointed crawl log )
        Path remoteLogFileName = new Path(CrawlEnvironment.getCheckpointStagingDirectory(),CrawlEnvironment.buildCrawlSegmentLogCheckpointFileName(getNodeName(),checkpointId) +"_"+ Integer.toString(listId) + "_" + Integer.toString(segmentId));
        
        hdfs.copyFromLocalFile(new Path(localSegmentLogFile.getAbsolutePath()),remoteLogFileName);
        
        return remoteLogFileName;
      }
    }
    return null;
  }
  
  private void purgeHDFSSegmentLogs(FileSystem hdfs, int listId,int segmentId) throws IOException { 
    
    Path listLogDirectory         = new Path(CrawlEnvironment.getCrawlSegmentDataDirectory(),((Integer)listId).toString());
    Path segmentLogDirectory      = new Path(listLogDirectory,((Integer)segmentId).toString());
    Path completionLogFilePath    = new Path(segmentLogDirectory,CrawlEnvironment.buildCrawlSegmentCompletionLogFileName(getNodeName()));
    
    if (!hdfs.exists(completionLogFilePath)) { 
      // create a zero length completion log file on hdfs ... 
      hdfs.createNewFile(completionLogFilePath);
    }
    
    // skip this step as history servers now manage segment logs 
    /*
    // and now ... delete all logs 
    Path segmentLogWildcardPath = new Path(segmentLogDirectory,CrawlEnvironment.buildCrawlSegmentLogCheckpointWildcardString(getNodeName()));
    FileStatus paths[] = hdfs.globStatus(segmentLogWildcardPath);
    if (paths != null) { 
      for (FileStatus path : paths) { 
        // hdfs.delete(path.getPath());
      }
    }
    */
  }
  
    
  /**  perform the actual checkpoint work here ... **/
  private void doCheckpoint() { 
    // at this point, we should be in the async thread, and all flusher activities are blocked ...
    LOG.info("CrawlLog Checkpoint - Starting ");
    //collect all necessary information from thread-unsafe data structure now (in async thread context)
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
    }
    catch (IOException e) { 
      LOG.error("Checkpoint failed with Exception:" + CCStringUtils.stringifyException(e));
    }
    
    // spawn a thread to do most of the blocking io ... 
    _threadPool.submit(new ConcurrentTask<Boolean>(_eventLoop,
        
        new Callable<Boolean>() {

          public Boolean call() throws Exception {

            // we need to track these in case of failure ... 
            Path checkpointTempFileName = null;
            Vector<Path> segmentLogStagingPaths = new Vector<Path>();
            Vector<Path> segmentLogFinalPaths = new Vector<Path>();

            // get the file system 
            FileSystem hdfs = CrawlEnvironment.getDefaultFileSystem();
            
            try { 
              
              LOG.info("CrawlLog Checkpoint - Transferring CrawlLog to HDFS");
              // write out crawl log to hdfs ... 
              checkpointTempFileName = transferLocalCheckpointLog(hdfs,getCheckpointPath(_rootDirectory),_checkpointId);
              
              LOG.info("CrawlLog Checkpoint - Transferring CrawlSegment Logs");
              // and next for every segment 
              for (long packedLogId : activeSegments) {
                
                File segmentLogPath = CrawlSegmentLog.buildCheckpointPath(_rootDirectory,getListIdFromLogId(packedLogId),getSegmentIdFromLogId(packedLogId));
                
                // LOG.info("CrawlLog Checkpoint - Transferring CrawlSegment Log for Segment:" + segmentId);
                // copy the segment log ... 
                Path remoteLogFilePath = transferLocalSegmentLog(hdfs,segmentLogPath,_checkpointId,getListIdFromLogId(packedLogId),getSegmentIdFromLogId(packedLogId));
                // if path is not null (data was copied) ... 
                if (remoteLogFilePath != null) { 
                  // add it to vector ... 
                  segmentLogStagingPaths.add(remoteLogFilePath);
                  // and add final path to vector while we are at it ... 
                  segmentLogFinalPaths.add(getFinalSegmentLogPath(hdfs, _checkpointId, getListIdFromLogId(packedLogId),getSegmentIdFromLogId(packedLogId)));
                }
              }
              LOG.info("CrawlLog Checkpoint - Finished Transferring CrawlSegment Logs");
              
              // now if we got here ... all hdfs transfers succeeded ... 
              // go ahead and move checkpoint log from staging to final data directory ... 
              Path checkpointDirectory      = new Path(CrawlEnvironment.getCheckpointDataDirectory());
              Path checkpointFileName       = new Path(checkpointDirectory,CrawlEnvironment.buildCrawlLogCheckpointName(getNodeName(),_checkpointId));
              
              // if no checkpoint data directory ... create one ... 
              if (!hdfs.exists(checkpointDirectory))
                  hdfs.mkdirs(checkpointDirectory);
              // and essentially move the crawl log file from staging to data directory ..
              hdfs.rename(checkpointTempFileName,checkpointFileName);
              // and now do the same thing for each segment log files
              for (int i=0;i<segmentLogStagingPaths.size();++i) {
                hdfs.rename(segmentLogStagingPaths.get(i), segmentLogFinalPaths.get(i));
              }
              
              // if we got here checkpoint was successfull... 
              return true;
            }
            catch (Exception e) { 
              LOG.error("Checkpoint:" + _checkpointId +" FAILED with exception:" + CCStringUtils.stringifyException(e));
              // in case of error ... we need to undo the checkpoint (as best as possible) ... 
              if (checkpointTempFileName != null) { 
                hdfs.delete(checkpointTempFileName);
              }
              for (Path segmentPath : segmentLogStagingPaths) {
                hdfs.delete(segmentPath);
              }
              for (Path segmentPath : segmentLogFinalPaths) {
                hdfs.delete(segmentPath);
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
                // LOG.info("CrawlLog Checkpoint - Finalizing CrawlSegmentLog for Segment:" + segmentLog.getSegmentId());
                // finalize the checkpoint on the segment log ... 
                segmentLog.finalizeCheckpoint();
                // and check to see if the segment has been completed ... 
                if (segmentLog.isSegmentComplete()) { 
                  // if so, add it our completed segments list ... 
                  completedSegmentList.add(makeSegmentLogId(segmentLog.getListId(), segmentLog.getSegmentId()));
                }
              }
              
              // now for all completed segments ... purge hdfs logs ... 
              for (long packedSegmentId : completedSegmentList ) { 
                try { 
                  LOG.info("CrawlLog Checkpoint - Purging HDFS CrawlSegmentLogs from Completed Segment. List:" + getListIdFromLogId( packedSegmentId) + " Segment:" + getSegmentIdFromLogId(packedSegmentId));
                  // purge hdfs files (and create a completion log file)
                  purgeHDFSSegmentLogs(CrawlEnvironment.getDefaultFileSystem(),getListIdFromLogId( packedSegmentId),getSegmentIdFromLogId(packedSegmentId));
                  LOG.info("CrawlLog Checkpoint - Purging Local CrawlSegmentLogs from Completed Segment. List:" + getListIdFromLogId( packedSegmentId) + " Segment:" + getSegmentIdFromLogId(packedSegmentId));
                  // and purge local files as well ... 
                  _loggers.get(packedSegmentId).purgeLocalFiles();
                }
                catch (IOException e) { 
                  LOG.error("Purge SegmentLog for Segment List:" + getListIdFromLogId( packedSegmentId) + " Segment:" + getSegmentIdFromLogId(packedSegmentId) + " threw IOException:" + CCStringUtils.stringifyException(e));
                }
                LOG.info("CrawlLog Checkpoint - DeRegistering Segment List:" + getListIdFromLogId( packedSegmentId) + " Segment:" + getSegmentIdFromLogId(packedSegmentId) + " From CrawlLog");
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
              callback.checkpointComplete(checkpointId,completedSegmentList);
              
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
                    // this is a serious error ... time to purge the crawl log directory altogether ... 
                    purgeActiveLog();
                    
                    // and all active segment logs as well... 
                    for (CrawlSegmentLog segmentLog : _loggers.values()) { 
                      segmentLog.purgeActiveLog();
                    }
                  }
                  catch (IOException e2) { 
                    LOG.error("IOException during Segment Log PURGE:" + CCStringUtils.stringifyException(e2));
                  }
                                    
                  // time to die hard ... 
                  throw new RuntimeException(e);
              }
              
              // and complete transaction ... 
              callback.checkpointFailed(checkpointId, e);
            }
          }
          ));
  }
  
  private static final class CustomByteArrayOutputStream extends ByteArrayOutputStream { 
    public CustomByteArrayOutputStream(int initialSize) { 
      super(initialSize);
    }
    public byte[] getBuffer() { return buf; }
  }
  
  private void logCrawlLogWrite(CrawlURL url,int bufferSizeOut) { 
    StringBuffer sb = new StringBuffer();
    
    sb.append(String.format("%1$20.20s ",CCStringUtils.dateStringFromTimeValue(System.currentTimeMillis())));
    sb.append(String.format("%1$4.4s ",  url.getResultCode()));
    sb.append(String.format("%1$10.10s ",url.getContentRaw().getCount()));
    if ((url.getFlags() & CrawlURL.Flags.IsRedirected) != 0) { 
      sb.append(url.getRedirectURL());
      sb.append(" ");
    }
    sb.append(url.getUrl());
    _engine.getCrawlLogLog().info(sb.toString());
  }
  
  private void flushLog(final FlushCompletionCallback completionCallback) { 
    if (Environment.detailLogEnabled())
      LOG.info("LOG_FLUSH:Collecting Entries....");
    // set flush in progress indicator ... 
    setFlushInProgress(true);
    // and collect buffers in async thread context (thus not requiring synchronization)
    final LinkedList<CrawlSegmentLog.LogItemBuffer> collector = new LinkedList<CrawlSegmentLog.LogItemBuffer>();
    // flush robots log 
    _robotsSegment.flushLog(collector);
    // walk segments collecting log items .... 
    for (CrawlSegmentLog logger : _loggers.values()) { 
      // flush any log items into the collector   
      logger.flushLog(collector);
    }
    if (Environment.detailLogEnabled())
      LOG.info("LOG_FLUSH:Collection Returned "+collector.size() + " Buffers");
    
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
      LOG.info("LOG_FLUSH:There are  "+urlItemCount + " Items in Flush Buffer Associated With " + packedSegmentIdSet.size() + " Segments");

    final File crawlLogFile   = getActivePath(_rootDirectory);

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
              
              Map<Long,DataOutputStream> streamsMapByPackedId = new HashMap<Long,DataOutputStream> ();
              Map<Long,Integer> recordCountsByPackedId= new HashMap<Long,Integer> ();
              
              long crawlLogRecordCount =0;

              // open the actual crawler log file ... 
              final DataOutputStream crawlLogStream = new DataOutputStream(new FileOutputStream(crawlLogFile,true));

              try { 
                if (Environment.detailLogEnabled())
                  LOG.info("LOG_FLUSH: Log Flusher Thread Opening Streams for Segments in Buffer");
                // now open a set of file descriptors related to the identified segments 
                for (long packedSegmentId : packedSegmentIdSet) {
                  // construct the unique filename for the given log file... 
                  File activeSegmentLog = CrawlSegmentLog.buildActivePath(_rootDirectory,getListIdFromLogId(packedSegmentId),getSegmentIdFromLogId(packedSegmentId));
                  // initialize the segment log ...
                  CrawlSegmentLog.initializeLogFile(activeSegmentLog);
                  // initialize record counts per stream ... 
                  recordCountsByPackedId.put(packedSegmentId,CrawlSegmentLog.readerHeader(activeSegmentLog));
                  // and open an output stream for the specified log file ... 
                  streamsMapByPackedId.put(packedSegmentId, new DataOutputStream(new FileOutputStream(activeSegmentLog,true)));
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
                    LOG.info("LOG_FLUSH: Log Flusher Thread Writing " + buffer.getItemCount() + " Entries for Segment:" + buffer.getSegmentId());

                  // output stream
                  DataOutputStream segmentLogStream = null;

                  if (buffer.getListId() != -1 && buffer.getSegmentId() != -1) { 
                    // update segment count first ... 
                    recordCountsByPackedId.put(makeSegmentLogId(buffer.getListId(),buffer.getSegmentId()), recordCountsByPackedId.get(makeSegmentLogId(buffer.getListId(),buffer.getSegmentId())) + buffer.getItemCount());
                    // get output stream associated with segment id   
                    segmentLogStream = streamsMapByPackedId.get(makeSegmentLogId(buffer.getListId(),buffer.getSegmentId()));                  
                  }
                  
                  // and our local record counter ... 
                  crawlLogRecordCount += buffer.getItemCount();
                  
                  // and next do the actual disk flush ... 
                  totalItemCount += buffer.flushToDisk(totalItemCount, 
                      
                      new CrawlSegmentLog.LogItemBuffer.CrawlURLWriter() {

                        private CustomByteArrayOutputStream bufferOutputStream = new CustomByteArrayOutputStream(1 << 17);
                        private DataOutputStream dataOutputStream = new DataOutputStream(bufferOutputStream);
                        private CRC32 crc = new CRC32();
                        
                        
                        public void writeItem(CrawlURL url) throws IOException {

                          bufferOutputStream.reset();
                          // write to intermediate stream ... 
                          url.write(dataOutputStream);
                          // log it 
                          logCrawlLogWrite(url,dataOutputStream.size());
                          // and crc the data ... 
                          crc.reset();
                          crc.update(bufferOutputStream.getBuffer(),0,bufferOutputStream.size());
                          // write out length first 
                          crawlLogStream.writeInt(bufferOutputStream.size());
                          //crc next
                          long computedValue = crc.getValue();
                          //TODO: waste of space - write 32 bit values as long because having problems with java sign promotion rules during read...
                          crawlLogStream.writeLong(computedValue);
                          // and then the data 
                          crawlLogStream.write(bufferOutputStream.getBuffer(),0,bufferOutputStream.size());
                        }
    
                        public void writeItemCount(int entryCount)throws IOException {
                        } 
                    
                  },segmentLogStream,historyStream);
                }
                
                if (Environment.detailLogEnabled())
                  LOG.info("LOG_FLUSH: Log Flusher Finished Writing Entries To Disk");
                collector.clear();
                
              }
              catch (IOException e) { 
                LOG.error("Critical Exception during Crawl Log Flush:" + CCStringUtils.stringifyException(e));
                throw e;
              }
              finally { 
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
                updateLogFileHeader(crawlLogFile,crawlLogRecordCount);
                // and update each completion log header ... 
                for (long packedSegmentId : recordCountsByPackedId.keySet()) { 
                  File activeSegmentLogPath = CrawlSegmentLog.buildActivePath(_rootDirectory,getListIdFromLogId(packedSegmentId), getSegmentIdFromLogId(packedSegmentId));
                  CrawlSegmentLog.writeHeader(activeSegmentLogPath, recordCountsByPackedId.get(packedSegmentId));
                }
              }
              catch (IOException e) { 
                LOG.error("Criticial Exception during Crawl Log Fluhs:" + CCStringUtils.stringifyException(e));
                throw e;
              }
              finally { 

              }
              
              long endTime = System.currentTimeMillis();
              
              _flushTimeAVG.addSample((double)endTime - startTime);
              _flushTimeSmoothed.addSample((double)endTime - startTime);
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
              throw new RuntimeException("CRITICAL FAILURE: Crawl Log FLUSH Threw Exception:" + CCStringUtils.stringifyException(e));
              
            }
          }
          ));
    }
    else { 
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
    
    if (_lastCheckpointTime == -1 || currentTime - _lastCheckpointTime >= LOG_CHECKPOINT_INTERVAL ) {
      
      // now one more check to see if we have enough items to do a checkpoint ... 
      if (_header._itemCount >=  LOG_FILE_CHECKPOINT_ITEM_COUNT_THRESHOLD) {
        return true;
      }
    }
    return false;
  }
  
  public void forceFlushAndCheckpointLog(final CheckpointCompletionCallback outerCallback) { 
    if (isCheckpointInProgress() || isFlushInProgress()) {
      throw new RuntimeException("forceFlush called while active Checkpoint or Flush In Progress!!");
    }
    
    flushLog(
        new FlushCompletionCallback() {

          @Override
          public void flushComplete() {
            
            long currentTime = System.currentTimeMillis();

            LOG.info("LOG_FLUSH Flush Complete... Checking to see if Checkpoint Possilbe");
            if (isForcedCheckpointPossible()) {
              // yes .. go ahead and checkpoint log
              LOG.info("Checkpointing Logs to HDFS");
              // start the checkpoint ... 
              checkpoint(currentTime,new CheckpointCompletionCallback() {

                public void checkpointComplete(long checkpointId,Vector<Long> completedSegmentList) {
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

                public void checkpointFailed(long checkpointId,Exception e) {
                  LOG.error("Checkpoint Failed for Checkpoint:" + checkpointId + " With Exception:" + CCStringUtils.stringifyException(e));
                  outerCallback.checkpointFailed(checkpointId, e);
                } 
                
              }, currentTime);
            }
            else { 
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
    
    _logFlusherTimer = new Timer(LOG_FLUSH_INTERVAL,true, new Timer.Callback() {

      public void timerFired(Timer timer) {
        // if checkpoint is NOT in progress ... 
        if (!isCheckpointInProgress() && !isFlushInProgress()) {
          
          LOG.info("LOG_FLUSH Starting ...");
          
          flushLog( 
          
              new FlushCompletionCallback() {

                public void flushComplete() {
                  // flush is complete ... check to see if we want to do a checkpoint ... 
                  long currentTime = System.currentTimeMillis();
                  
                  LOG.info("LOG_FLUSH Flush Complete... Checking to see if Checkpoint Possilbe");
                  if (isCheckpointPossible(currentTime)) { 
                  
                    LOG.info("Checkpointing Logs to HDFS");

                    // pause fetcher to prevent race condition where log flush takes a long time and causes the fetcher to consume all avaliable memory with content buffers 
                    _engine.pauseFetch();  
                    
                      // start the checkpoint ... 
                      checkpoint(currentTime,new CheckpointCompletionCallback() {

                        public void checkpointComplete(long checkpointId,Vector<Long> completedSegmentList) {
                          LOG.info("CrawlLog Checkpoint:" + checkpointId + " completed");

                          _engine.resumeFetch();  
                          
                          if (completedSegmentList != null) { 
                            // walk completed segments ... updating their crawl state ... 
                            if (_engine != null) { 
                              for (long packedSegmentId : completedSegmentList) { 
                                // notify crawler engine of status change ... 
                                _engine.crawlSegmentComplete(packedSegmentId);
                              }
                            }
                          }
                        }

                        public void checkpointFailed(long checkpointId,Exception e) {
                          
                          _engine.resumeFetch();
                          
                          LOG.error("Checkpoint Failed for Checkpoint:" + checkpointId + " With Exception:" + CCStringUtils.stringifyException(e));
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
    final Timer waitTimer = new Timer(1000,true,new Timer.Callback() {

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
        }
        else { 
          LOG.info("CrawlLog - stopLog Timer - Flush or Checkpoint in Progress... Waiting ... ");
        }
      }
    });
    // and start the timer ... 
    _eventLoop.setTimer(waitTimer);
  }
  
  public void collectStats(RuntimeStatsCollector collector) { 

    collector.setDoubleValue(CrawlerEngineStats.ID, CrawlerEngineStats.Name.CrawlLog_FlushTimeAVG,_flushTimeAVG.getAverage());
    collector.setDoubleValue(CrawlerEngineStats.ID, CrawlerEngineStats.Name.CrawlLog_FlushTimeSmoothed,_flushTimeSmoothed.getAverage());
    collector.setLongValue(CrawlerEngineStats.ID, CrawlerEngineStats.Name.CrawlLog_FlushTimeLast,_lastFlushTime);
  }
  
    
  @Test
  public void testCrawlLog() throws Exception {
    // initialize ...
    Configuration conf = new Configuration();
    
    conf.addResource("nutch-default.xml");
    conf.addResource("nutch-site.xml");
    conf.addResource("hadoop-default.xml");
    conf.addResource("hadoop-site.xml");
    conf.addResource("commoncrawl-default.xml");
    conf.addResource("commoncrawl-site.xml");
    
    CrawlEnvironment.setHadoopConfig(conf);
    CrawlEnvironment.setDefaultHadoopFSURI("file:///");
    CrawlEnvironment.setParseSegmentDataDirectory(_rootDirectory.getAbsolutePath() + "/parse_segments");
    
    
    // delete any existing log entry ... 
    CrawlSegmentLog.buildActivePath(_rootDirectory, 1,1).delete();
    
    // create a crawl segment ... 
    CrawlSegmentDetail detail = loadCrawlSegment("bloggerGETs_1.txt");
    // create a log instance 
    CrawlSegmentLog segmentLog = new CrawlSegmentLog(_rootDirectory,1,1,getNodeName());
    // add it to the crawl log instance 
    addSegmentLog(segmentLog);
    // get it back ... 
    segmentLog  = getLogForSegment(1,1);

    // compute our test targets ... 
    int totalItemCount = detail.getUrlCount();
    int desiredItemCount = totalItemCount - 100;
    
    // populate the logs 

    int curItemCount = 0;
    int flushPendingCount = 0;
    int flushCount = 0;
    // create a semaphore ... 
    final Semaphore semaphore = new Semaphore(0);
    
    for (CrawlSegmentHost host : detail.getHosts()) { 
      for (CrawlSegmentURL url : host.getUrlTargets()) { 
        //populate a crawl url data ... 
        CrawlURL crawlURLData = new CrawlURL();
        
        
      	String urlStr = url.getUrl();
        crawlURLData.setUrl(urlStr);
        crawlURLData.setFingerprint(url.getUrlFP());
        crawlURLData.setHostFP(host.getHostFP());
        // and add it the log ... 
        segmentLog.completeItem(crawlURLData);
        ++flushPendingCount;
        ++curItemCount;
        
        if (flushPendingCount % 1000 == 0 || curItemCount == desiredItemCount) {
          // count the number of flushes ... 
          ++flushCount;
          flushLog( new FlushCompletionCallback() {

            public void flushComplete() {
              semaphore.release();
            }

            public void flushFailed(Exception e) {
              semaphore.release();
            } 
            
          }
          
          );
        }
        if (curItemCount == desiredItemCount)
          break;
      }
      if (curItemCount == desiredItemCount)
        break;
    }
    
    // wait for all flushes to complete ...
    while (flushCount-- != 0) { 
      semaphore.acquire();
    }
    
    // next checkpoint the log ... 
    checkpoint(System.currentTimeMillis(),new CheckpointCompletionCallback() {

      public void checkpointComplete(long checkpointId,Vector<Long> completedSegmentIds) {
        semaphore.release();
      }

      public void checkpointFailed(long checkpointId, Exception e) {
        semaphore.release();
      } 
      
      }, 1);

    /*
     * TODO: WE NEED TO REWORK THIS SECTION >....
    // recreate a crawl segment ... 
    detail = loadCrawlSegment("bloggerGETs_1.txt");
    // create a log instance 
    segmentLog = new CrawlSegmentLog(_rootDirectory,1,getNodeName());
    // add it to the crawl log instance 
    addSegmentLog(segmentLog);
    // get it back ... 
    segmentLog  = getLogForSegment(1);
    // if exists, then just reconcile against the full segment against the log 
    segmentLog.syncToLog(detail);
    // validate .. 
    assert detail.getUrlsComplete() == desiredItemCount && detail.getUrlCount() == totalItemCount;
    // finally delete both log files after second run ... 
    CrawlSegmentLog.getActivePathGivenSegmentId(_rootDirectory, 1).delete();
    getActivePath(_rootDirectory).delete();
  */
    
    
  }
  
  
  private static CrawlSegmentHost createHost(String hostName) { 
    CrawlSegmentHost host = new CrawlSegmentHost();
    host.setHostName(hostName);
    byte[] hostNameAsBytes = host.getHostName().getBytes();
    host.setHostFP(FPGenerator.std64.fp(hostNameAsBytes,0,hostNameAsBytes.length));
    return host;
  }
  
  private static CrawlSegmentURL createSegmentURL(URL url) { 
    CrawlSegmentURL segmentURL = new CrawlSegmentURL();
    segmentURL.setUrl(url.toString());
    byte[] urlAsBytes = segmentURL.getUrl().getBytes();
    segmentURL.setUrlFP(FPGenerator.std64.fp(urlAsBytes,0,urlAsBytes.length));
    return segmentURL;
  }
  
  private static CrawlSegmentDetail loadCrawlSegment(String fileName) throws IOException { 
    
    TreeMap<String,CrawlSegmentHost> hosts = new TreeMap<String,CrawlSegmentHost>();
    
    URL resourceURL = CrawlEnvironment.getHadoopConfig().getResource(fileName);
    
    if (resourceURL == null) {
      throw new FileNotFoundException();
    }
    InputStream stream = resourceURL.openStream();
    BufferedReader reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(stream)));
    
    String line = null;
    
    do { 
      line = reader.readLine();
      if (line != null){ 
        if (Environment.detailLogEnabled())
          LOG.info(line);
        try { 
          URL theURL = new URL(line);
  
          CrawlSegmentHost host = hosts.get(theURL.getHost());
          if (host == null) { 
            
            host = createHost(theURL.getHost());
            
            hosts.put(theURL.getHost(),host);
          }
          CrawlSegmentURL segmentURL = createSegmentURL(theURL);
          host.getUrlTargets().add(segmentURL);
        }
        catch (MalformedURLException e) { 
          LOG.error("SKIPPING Malformed URL::"+line);
        }
      }
    } while(line != null);
    
    CrawlSegmentDetail crawlSegmentDetail = new CrawlSegmentDetail();
    
    int urlCount = 0;
    crawlSegmentDetail.setSegmentId(1);
    for (CrawlSegmentHost host : hosts.values()) { 
      crawlSegmentDetail.getHosts().add(host);
      urlCount += host.getUrlTargets().size();
    }

    crawlSegmentDetail.setUrlCount(urlCount);
    
    // finally, sort by host (as will be the case in a proper map reduce produced segment ... 
    Collections.sort(crawlSegmentDetail.getHosts());
    
    return crawlSegmentDetail;
    
  }

  public Vector<Long> getActiveSegmentIdList() {
    
    Vector<Long> segmentIdList = new Vector<Long>();
    segmentIdList.addAll(_loggers.keySet());
    return segmentIdList;
  }
}
