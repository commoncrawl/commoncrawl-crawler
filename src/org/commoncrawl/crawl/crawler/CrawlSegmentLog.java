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


package org.commoncrawl.crawl.crawler;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.record.Buffer;
import org.commoncrawl.common.Environment;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.protocol.BulkItemHistoryQuery;
import org.commoncrawl.protocol.BulkItemHistoryQueryResponse;
import org.commoncrawl.protocol.CrawlSegmentURLFP;
import org.commoncrawl.protocol.CrawlURL;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.rpc.base.internal.AsyncRequest;
import org.commoncrawl.rpc.base.internal.AsyncRequest.Callback;
import org.commoncrawl.rpc.base.internal.AsyncRequest.Status;
import org.commoncrawl.util.internal.URLFPBloomFilter;
import org.commoncrawl.util.internal.URLUtils;
import org.commoncrawl.util.shared.BloomCalculations;
import org.commoncrawl.util.shared.ImmutableBuffer;
import org.commoncrawl.util.shared.BitUtils.BitStream;
import org.commoncrawl.util.shared.BitUtils.BitStreamReader;

/**
 * A transaction log that tracks crawl progress within a single crawl segemnt
 * 
 * @author rana
 *
 */
public final class CrawlSegmentLog {

  private static final int DEFAULT_LOGITEM_LIST_SIZE = 100;
  
  public static final Log LOG = LogFactory.getLog(CrawlSegmentLog.class);
  
  
  public static class CrawlSegmentFPMap { 
    public int _urlCount =0;
    public int _urlsComplete = 0;
    private byte[] _urlfpBuffer = null;
    private int    _urlfpBufferSize = 0;
    private URLFPBloomFilter _validFingerprintsBloomFilter = null;
    private URLFPBloomFilter _crawledItemsBloomFilter = null;
    
    
    public void setURLFPBuffer(int segmentURLCount,byte[] data,int length)throws IOException {
      _urlCount = segmentURLCount;
      _urlfpBuffer = data;
      _urlfpBufferSize = length;
      // initialize the bloom filters 
      _validFingerprintsBloomFilter = new URLFPBloomFilter(segmentURLCount*2, BloomCalculations.computeBestK(11), 11);
      _crawledItemsBloomFilter      = new URLFPBloomFilter(segmentURLCount*2, BloomCalculations.computeBestK(11), 11);
      // populate valid items filter ... 
      DataInputBuffer inputBuffer = getURLFPAsStream();
      URLFPV2 urlfp = new URLFPV2();
      while (inputBuffer.available() != 0) { 
        urlfp.setDomainHash(WritableUtils.readVLong(inputBuffer));
        urlfp.setUrlHash(WritableUtils.readVLong(inputBuffer));
        _validFingerprintsBloomFilter.add(urlfp);
      }
    }
    
    public DataInputBuffer getURLFPAsStream()throws IOException { 
      if (_urlfpBuffer != null && _urlfpBufferSize != 0) { 
        DataInputBuffer dataInputBuffer = new DataInputBuffer();
        dataInputBuffer.reset(_urlfpBuffer, _urlfpBufferSize);
        return dataInputBuffer;
      }
      else { 
        throw new IOException("URLFPBuffer Not Initialized!");
      }
    }

    public Buffer getURLFPAsBuffer()throws IOException { 
      if (_urlfpBuffer != null && _urlfpBufferSize != 0) { 
        return new Buffer(_urlfpBuffer,0, _urlfpBufferSize);
      }
      else { 
        throw new IOException("URLFPBuffer Not Initialized!");
      }
    }

    public boolean wasCrawled(URLFPV2 urlfp) { 
      return _crawledItemsBloomFilter.isPresent(urlfp);
    }
    
    public void setCrawled(URLFPV2 urlfp) { 
      _crawledItemsBloomFilter.add(urlfp);
    }
    
    public boolean isValidSegmentURL(URLFPV2 urlfp) { 
      return _validFingerprintsBloomFilter.isPresent(urlfp);
    }
  }
  
  
  File             _rootDataDir;
  int              _listId;
  int              _segmentId;
  int              _localLogItemCount;
  int              _checkpointItemCount;
  int              _remainingURLS;
  String           _nodeName;
  boolean          _segmentComplete;
  boolean          _urlCountValid;
  
  LinkedList<LogItemBuffer> _buffers = new LinkedList<LogItemBuffer>();
  
  public CrawlSegmentLog(File rootDataDirectory,int listId,int segmentId,String nodeName) { 
    
    _rootDataDir = rootDataDirectory;
    _listId = listId;
    _segmentId = segmentId;
    _remainingURLS = 0;
    _localLogItemCount = 0;
    _checkpointItemCount = 0;
    _nodeName = nodeName;
    _segmentComplete = false;
    _urlCountValid = false;
  }
  
  /** get the host name **/
  public String getNodeName() { 
    return _nodeName;
  }
  
  /** get the list this segment log is associated with **/
  public int getListId() { 
    return _listId;
  }
  
  /** check and see if this segment is complete **/
  public synchronized boolean isSegmentComplete() { 
    return _segmentComplete;
  }
  
  public synchronized boolean isURLCountValid() { 
    return _urlCountValid;
  }
  
  public static void insetFPIntoArray(ArrayList<CrawlSegmentURLFP> vector,CrawlSegmentURLFP targetfp) { 
    int insertionPos = findInsertionPosForFP(vector,targetfp.getUrlFP());
    if (insertionPos == -1){
      vector.add(0, targetfp);
    }
    else { 
      if (vector.get(insertionPos).getUrlFP() != targetfp.getUrlFP()) { 
        vector.add(insertionPos+1,targetfp);
      }
    }
  }
  
  public static int findInsertionPosForFP(ArrayList<CrawlSegmentURLFP> vector,long targetfp) {

    int low  = 0;
    int high = vector.size() - 1;
    
    while (low <= high) {
     int mid = low + ((high - low) / 2);

     CrawlSegmentURLFP urlfp = vector.get(mid);

     
     int compareResult = (urlfp.getUrlFP() < targetfp) ? -1 : (urlfp.getUrlFP() > targetfp) ? 1 : 0;
        
     if (compareResult > 0) {  
         high = mid - 1;
     }
     else if (compareResult < 0) { 
         low = mid + 1;
     }
     else { 
       return mid;
     }
       
   }
   return high;
  }
  
  private static void updateFPMapFromBulkQueryResponse(CrawlSegmentFPMap segmentDetail,BulkItemHistoryQueryResponse queryResponse) throws IOException { 
    
    BitStream bitStream = new BitStream(queryResponse.getResponseList().getReadOnlyBytes(),queryResponse.getResponseList().getCount()*8);
    BitStreamReader reader = new BitStreamReader(bitStream);
    
    int updatedItemCount = 0;
    int processedItemCount = 0;
    
    // ok walk entire urlfp stream (prepopulated from crawl segment)
    DataInputBuffer inputBuffer = segmentDetail.getURLFPAsStream();
    URLFPV2 urlfp = new URLFPV2();
    
    while (inputBuffer.available() != 0) {
      
      urlfp.setDomainHash(WritableUtils.readVLong(inputBuffer));
      urlfp.setUrlHash(WritableUtils.readVLong(inputBuffer));
      
      processedItemCount++;
    
      // check to see what history server says about the item ... 
      if (reader.getbit() == 1) {
        // if it indicates this item was crawled, update the bloom filter ... 
        segmentDetail.setCrawled(urlfp);
        updatedItemCount++;
        // and update urls complete ... 
        segmentDetail._urlsComplete++;
      }
      else {
        // otherwise, tricky, but check local bloom filter to see if it was crawled prior to checkpoint with history server 
        if (segmentDetail.wasCrawled(urlfp)) { 
          // if so, update urls complete 
          segmentDetail._urlsComplete++;
        }
      }
    }
    
    // if (Environment.detailLogEnabled())
      LOG.info("###SYNC: Reconciled FPMap with Query Response. " 
          + " URLCount:" + segmentDetail._urlCount  
          + " Complete:" + segmentDetail._urlsComplete
          + " Items Changed:" + updatedItemCount);
  }
    
  private static BulkItemHistoryQuery buildHistoryQueryBufferFromMap(CrawlSegmentFPMap segmentDetail) throws IOException { 
    // create a bulk item query message ... 
    BulkItemHistoryQuery query = new BulkItemHistoryQuery();
    // get the entire urlfp stream from segmentFPMap and set it in the message 
    query.setFingerprintList(segmentDetail.getURLFPAsBuffer());
    
    return query;
  }
    
  
  /** sync the incoming segment against the local crawl log and then send it up to the history server **/
  public int syncToLog(CrawlSegmentFPMap segmentDetail) throws IOException { 
    if (Environment.detailLogEnabled())
      LOG.info("### SYNC: List:"+ _listId + " Segment:" + _segmentId +" Syncing Progress Log");
    
    int itemsProcessed = 0;
    
    // and construct a path to the local crawl segment directory ... 
    File  activeLogPath = buildActivePath(_rootDataDir,_listId,_segmentId);
    File  checkpointLogPath = buildCheckpointPath(_rootDataDir,_listId, _segmentId);
    
    // check if it exists ... 
    if (checkpointLogPath.exists()){
      // log it ... 
      if (Environment.detailLogEnabled())
        LOG.info("### SYNC: List:"+ _listId + " Segment:" + _segmentId +" Checkpoint Log Found");
      // rename it as the active log ... 
      checkpointLogPath.renameTo(activeLogPath);
    }
    
    if (activeLogPath.exists()) {
      // reconcile against active log (if it exists) ...
      _localLogItemCount = reconcileLogFile(FileSystem.getLocal(CrawlEnvironment.getHadoopConfig()),new Path(activeLogPath.getAbsolutePath()),_listId,_segmentId,segmentDetail,null);
      if (Environment.detailLogEnabled())
        LOG.info("### SYNC: List:"+ _listId + " Segment:" + _segmentId +" Reconciled Local Log File with ProcessedItemCount:" + _localLogItemCount);
      itemsProcessed += _localLogItemCount;
    }

    FileSystem hdfs = CrawlEnvironment.getDefaultFileSystem();
    
    // first things first ... check to see if special completion log file exists in hdfs 
    Path hdfsSegmentCompletionLogPath = 
      new Path(CrawlEnvironment.getCrawlSegmentDataDirectory() + "/" + getListId() + "/"
                    + getSegmentId() + "/" 
                    + CrawlEnvironment.buildCrawlSegmentCompletionLogFileName(getNodeName()));
    
    if (hdfs.exists(hdfsSegmentCompletionLogPath)) {
      if (Environment.detailLogEnabled())
        LOG.info("### SYNC: List:"+ _listId + " Segment:" + _segmentId +" Completion File Found. Marking Segment Complete");
      // if the file exists then this segment has been crawled and uploaded already ... 
      // if active log file exists ... delete it ... 
      if (activeLogPath.exists()) 
        activeLogPath.delete();
      //reset local log item count ... 
      _localLogItemCount = 0;
      itemsProcessed = -1;
      
      // remove all hosts from segment
      segmentDetail._urlsComplete = segmentDetail._urlCount;
    }
    else {
      
      if (segmentDetail != null) { 
        if (Environment.detailLogEnabled())
          LOG.info("### SYNC: Building BulkItem History Query for List:"+ _listId + " Segment:" + _segmentId);
        BulkItemHistoryQuery query = buildHistoryQueryBufferFromMap(segmentDetail);
        
        if (query != null) {
          // create blocking semaphore ... 
          final Semaphore semaphore = new Semaphore(1);
          semaphore.acquireUninterruptibly();
          if (Environment.detailLogEnabled())
            LOG.info("### SYNC: Dispatching query to history server");
          //create an outer response object we can pass aysnc response to ... 
          final BulkItemHistoryQueryResponse outerResponse = new BulkItemHistoryQueryResponse();
          
          CrawlerServer.getServer().getHistoryServiceStub().bulkItemQuery(query, new Callback<BulkItemHistoryQuery, BulkItemHistoryQueryResponse>() {

            @Override
            public void requestComplete(final AsyncRequest<BulkItemHistoryQuery, BulkItemHistoryQueryResponse> request) {
              // response returns in async thread context ... 
              if (request.getStatus() == Status.Success) {
                if (Environment.detailLogEnabled())
                  LOG.info("###SYNC: bulk Query to history server succeeded. setting out resposne");
                ImmutableBuffer buffer = request.getOutput().getResponseList();
                outerResponse.setResponseList(new Buffer(buffer.getReadOnlyBytes(),0,buffer.getCount()));
              }
              else { 
                LOG.error("###SYNC: bulk Query to history server failed.");
                
              }
              // release semaphore
              semaphore.release();
            }
          });
          LOG.info("###SYNC: Loader thread blocked waiting for bulk query response");
          semaphore.acquireUninterruptibly();
          LOG.info("###SYNC: Loader thread received response from history server");
          
          if (outerResponse.getResponseList().getCount() == 0) { 
            LOG.error("###SYNC: History Server Bulk Query Returned NULL!!! for List:" +  _listId + " Segment:" + _segmentId);
          }
          else { 
            // ok time to process the response and integrate the results into the fp list 
            updateFPMapFromBulkQueryResponse(segmentDetail,outerResponse);
          }
        }
        else { 
          if (Environment.detailLogEnabled())
            LOG.warn("### SYNC: No fingerprints found when processing segment detail for List:"+ _listId + " Segment:" + _segmentId);
          segmentDetail._urlsComplete = segmentDetail._urlCount;          
        }
      }
      /*
      // and now walk hdfs looking for any checkpointed logs ...
      // scan based on checkpoint filename ... 
      FileStatus[] remoteCheckpointFiles = hdfs.globStatus(new Path(CrawlEnvironment.getCrawlSegmentDataDirectory() + "/" + getListId() + "/"
          + getSegmentId() + "/" + CrawlEnvironment.buildCrawlSegmentLogCheckpointWildcardString(getNodeName())));
      
      if (remoteCheckpointFiles != null) {

        LOG.info("### SYNC: List:"+ _listId + " Segment:" + _segmentId +" Found Remote Checkpoint Files");
        
        // create a temp file to hold the reconciled log ... 
        File consolidatedLogFile = null;
        
        if (remoteCheckpointFiles.length > 1) { 
          // create temp log file ... 
          consolidatedLogFile = File.createTempFile("SegmentLog", Long.toString(System.currentTimeMillis()));
          // write out header ... 
          CrawlSegmentLog.writeHeader(consolidatedLogFile,0);
        }
        // walk the files 
        for(FileStatus checkpointFilePath : remoteCheckpointFiles) {
          // and reconcile them against segment ... 
          itemsProcessed += reconcileLogFile(hdfs,checkpointFilePath.getPath(),getListId(),getSegmentId(),segmentDetail,consolidatedLogFile);
          LOG.info("### SYNC: List:"+ _listId + " Segment:" + _segmentId +" Processed Checkpoint File:" + checkpointFilePath.getPath() + " Items Processed:" + itemsProcessed);          
        }
        
        // finally ... if consolidatedLogFile is not null 
        if (consolidatedLogFile != null) { 
          // build a new hdfs file name ... 
          Path consolidatedHDFSPath = new Path(CrawlEnvironment.getCrawlSegmentDataDirectory() + "/" + getListId() + "/" + getSegmentId() + "/" + CrawlEnvironment.buildCrawlSegmentLogCheckpointFileName(getNodeName(), System.currentTimeMillis()));
          LOG.info("### SYNC: List:"+ _listId + " Segment:" + _segmentId +" Writing Consolidated Log File:" + consolidatedHDFSPath + " to HDFS");         
          // and copy local file to log ... 
          hdfs.copyFromLocalFile(new Path(consolidatedLogFile.getAbsolutePath()),consolidatedHDFSPath);
          // and delete all previous log file entries ... 
          for (FileStatus oldCheckPointFile : remoteCheckpointFiles) { 
            hdfs.delete(oldCheckPointFile.getPath());
          }
          consolidatedLogFile.delete();
        }
      }
      */
    }
    
    if (segmentDetail != null) { 
      _remainingURLS += (segmentDetail._urlCount - segmentDetail._urlsComplete);
      // mark url count as valid now ...
      _urlCountValid = true;

      // now if remaining url count is zero ... then mark the segment as complete ... 
      if(_remainingURLS == 0 && _localLogItemCount == 0) { 
        _segmentComplete = true;
      }
    }
    if (Environment.detailLogEnabled())
      LOG.info("### SYNC: List:"+ _listId + " Segment:" + _segmentId +" Done Syncing Progress Log TotalURLS:" + segmentDetail._urlCount +" RemainingURLS:" + _remainingURLS + " LocalLogItemCount:" + _localLogItemCount);
    
    return itemsProcessed;
  }
  
  /** append a CrawlURL item to the log **/
  public void completeItem(CrawlURL urlItem) {
    
    LogItem item = new LogItem();
    
    item._hostFP = urlItem.getHostFP();
    item._itemFP = urlItem.getFingerprint();
    item._urlData = urlItem;
    
    getAvailableBuffer().appendItem(item);
    
    if ((item._urlData.getFlags() & CrawlURL.Flags.IsRobotsURL) == 0) { 
      // now check to see if item was redirected ... 
      if ((item._urlData.getFlags() & CrawlURL.Flags.IsRedirected) != 0) { 
        // if so, check last attempt reason 
        if (item._urlData.getLastAttemptResult() == CrawlURL.CrawlResult.SUCCESS && item._urlData.isFieldDirty(CrawlURL.Field_REDIRECTURL)) {
          
          String redirectURL = item._urlData.getRedirectURL();
          
          // attempt to generate a fingerprint for the the redirected url ... 
          URLFPV2 fingerprint = URLUtils.getURLFPV2FromURL(redirectURL);
                  
          if (fingerprint != null) { 
            // append a redirect item 
            item = new LogItem();
            
            item._hostFP = fingerprint.getDomainHash();
            item._itemFP = fingerprint.getUrlHash();
            item._urlData = urlItem;
            item._writeToCrawLog = false;
            
            getAvailableBuffer().appendItem(item);
          }
                 
        }
      }
    }
    
    // reduce remaining url count 
    --_remainingURLS;
    
    // and increment local log item count ... 
    ++_localLogItemCount;
  }
  
  public void purgeLocalFiles() throws IOException { 
    File activePath       = buildActivePath(_rootDataDir,_listId,getSegmentId());
    File checkpointPath   = buildCheckpointPath(_rootDataDir,_listId,getSegmentId());

    if (activePath.exists())
      activePath.delete();
    if (checkpointPath.exists())
      checkpointPath.delete();
  }
  
  /** checkpoint log file **/
  public void checkpointLocalLog() throws IOException { 
    
    File activePath       = buildActivePath(_rootDataDir,_listId,getSegmentId());
    File checkpointPath   = buildCheckpointPath(_rootDataDir,_listId,getSegmentId());
    
    // capture local log item count ... 
    _checkpointItemCount = _localLogItemCount;
    
    checkpointPath.delete();
    // rename active path to check point path ... 
    activePath.renameTo(checkpointPath);
    // and recreate log .. 
    initializeLogFile(activePath);    
  }

  void finalizeCheckpoint() { 
    
    File checkpointLogFile= buildCheckpointPath(_rootDataDir,_listId, _segmentId);
    // delete local checkpoint log file ...
    checkpointLogFile.delete();
    // and reduce local log item count by checkpoint amount ...
    _localLogItemCount -= _checkpointItemCount;
    //reset checkpoint item count ...
    _checkpointItemCount = 0;

    if (isURLCountValid()) {
      LOG.info("finalizeCheckpoint for Segment:" + _segmentId + " List: " + _listId +  " Remaining:" + _remainingURLS + " LocalLogItemCount:" + _localLogItemCount);

      // now finally ... if  remaining url count is zero and local log item count is zero as well... 
      if (_remainingURLS == 0 && _localLogItemCount == 0) {

        LOG.info("CrawlSegment ListId:" + _listId + " Segment:" + _segmentId + " Marked as Complete During CrawlSegmentLog Checkpoint");
        
        // then mark the segment as complete ... 
        _segmentComplete = true;

      }
    }
  }
  
  void abortCheckpoint() { 
    File activeLogFile      = buildActivePath(_rootDataDir,_listId, _segmentId);
    File checkpointLogFile  = buildCheckpointPath(_rootDataDir,_listId, _segmentId);
    checkpointLogFile.renameTo(activeLogFile);
    //reset checkpoint item count ...
    _checkpointItemCount = 0;
  }  
  
  /** ensure paths **/
  private static void ensurePaths(File rootDirectory) { 
    File crawlDataDir = new File(rootDirectory,CrawlEnvironment.getCrawlerLocalOutputPath());
    if (!crawlDataDir.exists()) { 
      crawlDataDir.mkdir();
    }
  }
  
  public static void initializeLogFile(File activeLogFilePath) throws IOException { 
    if (!activeLogFilePath.exists()) { 
      writeHeader(activeLogFilePath,0);
    }
  }
  
  public void purgeActiveLog()throws IOException { 
    File activeLogFilePath = buildActivePath(_rootDataDir,_listId, _segmentId);
    
    if (activeLogFilePath.exists())
      activeLogFilePath.delete();
    
    initializeLogFile(activeLogFilePath);
  }
 
  /** get list root crawl segment directory given list id **/
  public static Path buildHDFSCrawlSegmentSearchPathForListId(int listId,String hostName) { 
    Path pathOut = new Path(CrawlEnvironment.getCrawlSegmentDataDirectory(),Integer.toString(listId));
    pathOut      = new Path(pathOut,"*/" + hostName);
    return pathOut;
    
  }
  
  /** get active log file path given segment id **/
   public static File buildActivePath(File rootDirectory,int listId,int segmentId) { 
     // and construct a path to the local crawl segment directory ... 
     File crawlDataDir = new File(rootDirectory,CrawlEnvironment.getCrawlerLocalOutputPath());
     // list directory ... 
     File listDir = new File(crawlDataDir,Integer.toString(listId));
     if (!listDir.exists()) { 
       listDir.mkdirs();
     }
     // append the segment id to the path ... 
     return new File(listDir,((Integer)segmentId).toString() + "_" + CrawlEnvironment.ActiveSegmentLog);
   }

   /** get active log file path given segment id **/
   public static File buildCheckpointPath(File rootDirectory,int listId,int segmentId) { 
     // and construct a path to the local crawl segment directory ... 
     File crawlDataDir = new File(rootDirectory,CrawlEnvironment.getCrawlerLocalOutputPath());
     // list directory ... 
     File listDir = new File(crawlDataDir,Integer.toString(listId));
     if (!listDir.exists()) { 
       listDir.mkdirs();
     }
     // append the segment id to the path ... 
     return new File(listDir,((Integer)segmentId).toString() + "_" + CrawlEnvironment.CheckpointSegmentLog);
   }
  
  
  /** get segment id of associated segment **/
  public int getSegmentId() { 
    return _segmentId;
  }
  
  /** flush and add all pending buffers into the passed in list **/ 
  public void flushLog(LinkedList<LogItemBuffer> collector) { 
    for (LogItemBuffer buffer : _buffers) {
      if (buffer.getItemCount() != 0 ) { 
        collector.addLast(buffer);
      }
    }
    _buffers.clear();
    _buffers.addFirst(new LogItemBuffer(getListId(),getSegmentId()));
  }
  
  private LogItemBuffer  getAvailableBuffer() { 
    if (_buffers.isEmpty() || !_buffers.getFirst().spaceAvailable()) { 
      _buffers.addFirst(new LogItemBuffer(getListId(),getSegmentId()));
    }
    return _buffers.getFirst();
  }
  
  static class LogItem implements Comparable<LogItem>  {
    
    public static final int ItemSize_Bytes = 20; // hostFP(long) + itemFP(long) + position(int)

    // Comparable Implementation
    public int compareTo(LogItem otherItem) {
      if (_hostFP < otherItem._hostFP )
        return -1;
      else if (_hostFP > otherItem._hostFP)
        return 1;
      else { 
        if (_itemFP < otherItem._itemFP)
          return -1;
        else if (_itemFP > otherItem._itemFP)
          return 1;
        else 
          return 0;
      }
    }    
    
    public boolean        _writeToCrawLog = true;
    public long           _hostFP;
    public long           _itemFP;
    public CrawlURL       _urlData;  
  }
  
  static class LogItemBuffer { 
    
    private int       _listId;
    private int       _segmentId;
    private LogItem[] _itemsArray = null;
    private int       _itemCount;
    
    
    public LogItemBuffer(int listId,int segmentId) {
      _listId = listId;
      _segmentId = segmentId;
      _itemCount = 0;
      _itemsArray = new LogItem[DEFAULT_LOGITEM_LIST_SIZE];
    }
       
    public int  getListId() { return _listId; }
    public int  getSegmentId() { return _segmentId; }
    public LogItem[] getItems() { return _itemsArray; }
    public int getItemCount() { return _itemCount; } 
    
    public void appendItem(LogItem item) { 
      if (_itemsArray ==null || _itemCount == _itemsArray.length) { 
        throw new RuntimeException("Invalid call to append item");
      }
      _itemsArray[_itemCount++] = item;
    }
    
    public boolean spaceAvailable() { 
      return (_itemsArray != null && _itemCount < _itemsArray.length);
    }
    
    public static interface CrawlURLWriter { 
      
      void writeItemCount(int entryCount) throws IOException ;
      void writeItem(CrawlURL url) throws IOException ;
    }
    
    public int flushToDisk(int startingItemPosition,CrawlURLWriter urlWriter,DataOutputStream segmentLogStream,DataOutputStream historyLog) throws IOException { 
      
      // write out entry count first ... 
      urlWriter.writeItemCount(_itemCount);
      for (int i=0;i<_itemCount;++i) { 
        if (_itemsArray[i]._writeToCrawLog) { 
          // write url data ...
          urlWriter.writeItem(_itemsArray[i]._urlData);
        }
        CrawlURL urlObject = _itemsArray[i]._urlData; 
        // if now crawl directives ... 
        if ((urlObject.getFlags() & CrawlURL.Flags.InParseQueue) == 0) { 
          if (segmentLogStream != null) { 
            // and write out segment log info ...
            segmentLogStream.writeLong(_itemsArray[i]._hostFP);
            segmentLogStream.writeLong(_itemsArray[i]._itemFP);
            segmentLogStream.writeInt(startingItemPosition + i);
          }
          if (historyLog != null) { 
            URLFPV2 fp = URLUtils.getURLFPV2FromURL(urlObject.getUrl());
            if (fp != null) { 
              // write original url to history log ... 
              fp.write(historyLog);
            }
            // if redirected ... 
            if ((_itemsArray[i]._urlData.getFlags() & CrawlURL.Flags.IsRedirected) != 0) { 
              // calc fingerprint for url ... 
              fp = URLUtils.getURLFPV2FromURL(urlObject.getRedirectURL());
              if (fp != null) { 
                // write redirect fingerprint to history log ... 
                fp.write(historyLog);
              }
            }
          }
        }
        
        _itemsArray[i]._urlData.clear();
        _itemsArray[i]._urlData = null;
        _itemsArray[i] = null;
      }
      
      return _itemCount;
    }
    
    public void loadFromStream(byte[] readBuffer, int itemCount) { 
      _itemCount = itemCount;
      if (_itemsArray == null || _itemsArray.length < itemCount) { 
        // reallocate array ...
        _itemsArray = new LogItem[_itemCount];
      }
      
      int bytePosition = 0;
      
      ByteArrayInputStream inputStream = new ByteArrayInputStream(readBuffer);
      DataInputStream dataInputStream = new DataInputStream(inputStream); 
      
      for (int i=0;i<_itemCount;++i) {
        
        LogItem item = new LogItem();
        
        item._hostFP = (((long)readBuffer[bytePosition++] << 56) +
                               ((long)(readBuffer[bytePosition++] & 255) << 48) +
                               ((long)(readBuffer[bytePosition++] & 255) << 40) +
                               ((long)(readBuffer[bytePosition++] & 255) << 32) +
                               ((long)(readBuffer[bytePosition++] & 255) << 24) +
                               ((readBuffer[bytePosition++] & 255) << 16) +
                               ((readBuffer[bytePosition++] & 255) <<  8) +
                               ((readBuffer[bytePosition++] & 255) <<  0));

        
        item._itemFP = (((long)readBuffer[bytePosition++] << 56) +
            ((long)(readBuffer[bytePosition++] & 255) << 48) +
            ((long)(readBuffer[bytePosition++] & 255) << 40) +
            ((long)(readBuffer[bytePosition++] & 255) << 32) +
            ((long)(readBuffer[bytePosition++] & 255) << 24) +
            ((readBuffer[bytePosition++] & 255) << 16) +
            ((readBuffer[bytePosition++] & 255) <<  8) +
            ((readBuffer[bytePosition++] & 255) <<  0));
        
        // skip position hint...
        bytePosition += 4;
        _itemsArray[i] = item;
        
      }
    }
    
  }
  
  public static int getHeaderSize() { 
    return 8;
  }
  
  public static int readerHeader(File logFilePath) throws IOException { 
    int recordCount = 0;
    FileInputStream stream = new FileInputStream(logFilePath);
 
    try { 
      DataInputStream reader = new DataInputStream(stream);
      recordCount = readHeader(reader);
    }
    finally {
      stream.close();
    }
    
    return recordCount;
  }
  
  public static int readHeader(DataInputStream reader) throws IOException { 
    reader.skipBytes(4);
    return reader.readInt();
  }

  public static final int LogFileHeaderBytes = 0xCC00CC00;

  public static void writeHeader(File logFilePath,int recordCount) throws IOException { 
    RandomAccessFile stream = new RandomAccessFile(logFilePath,"rw");
    try { 
      stream.seek(0);
      stream.writeInt(LogFileHeaderBytes);
      stream.writeInt(recordCount);
    }
    finally { 
      // stream.getFD().sync();
      stream.close();
    }
  }
  
  public static int reconcileLogFile(FileSystem fs,Path logFilePath,int listId,int segmentId,CrawlSegmentFPMap segment,File consolidationFile)throws IOException { 
    
    RandomAccessFile consolidationStream = null;

    int consolidationFileItemCount = 0;
    
    if (consolidationFile != null) { 
      consolidationStream = new RandomAccessFile(consolidationFile,"rw");
      consolidationFileItemCount = readerHeader(consolidationFile);
      consolidationStream.seek(consolidationStream.length());
    }
    
    int processedItemCount = 0;
    

    FSDataInputStream hdfsInputStream = null;
    
    try {
      
      // get the file size on disk 
      long fileSize = fs.getFileStatus(logFilePath).getLen();
  
      // allocate an array that can hold up to the list size of items ...
      byte[] buffer = new byte[DEFAULT_LOGITEM_LIST_SIZE * LogItem.ItemSize_Bytes];
      
      // calcuate item count 
      int totalItemCount = (int)( (fileSize - getHeaderSize()) / LogItem.ItemSize_Bytes);
          
      // get a reader ... 
      
      hdfsInputStream = fs.open(logFilePath);
  
      int headerItemCount = readHeader(hdfsInputStream);
      
      if (headerItemCount != totalItemCount) { 
        LOG.warn("CrawlSegmentLog - header item count for log file:" + logFilePath.toString() + " is:"+ headerItemCount + " file size indicates:" + totalItemCount);
        totalItemCount = headerItemCount;
      }
      
      int remainingItemCount = totalItemCount;
      
      
      LogItemBuffer itemList = new LogItemBuffer(listId,segmentId);
  
      while (remainingItemCount != 0) {
        
        int blockItemCount = Math.min(remainingItemCount,DEFAULT_LOGITEM_LIST_SIZE);
        
        // and read the data 
        hdfsInputStream.read(buffer,0,(int)blockItemCount * LogItem.ItemSize_Bytes);
        // and if consolidation stream is valid ... 
        if (consolidationStream != null) { 
          // add entries to that stream ... 
          consolidationStream.write(buffer,0,(int)blockItemCount * LogItem.ItemSize_Bytes);
        }
        
        // if not a dry run... 
        if (segment != null) { 
          // populate the item list   
          itemList.loadFromStream(buffer,blockItemCount);
          // reconcile the list against the segment 
          processedItemCount += reconcileItemList(itemList,segment);
        }
        // reduce item count 
        remainingItemCount -= blockItemCount;
      }
      
      // finally if consolidation stream is valid ... 
      if (consolidationStream != null) { 
        // update the file's header .. 
        writeHeader(consolidationFile, consolidationFileItemCount +totalItemCount );
      }
    }
    finally { 
      if (consolidationStream != null)  {
        consolidationStream.close();
      }
      if (hdfsInputStream != null) { 
        hdfsInputStream.close();
      }
    }
    return processedItemCount;
  }
  
  public static interface LogFileItemCallback { 
    public void processItem(long domainHash,long urlFingerprint);
  }
  
  public static void walkFingerprintsInLogFile(FileSystem fs,Path logFilePath,LogFileItemCallback callback)throws IOException { 
    
    FSDataInputStream hdfsInputStream = null;
    
    try {
      
      // get the file size on disk 
      long fileSize = fs.getFileStatus(logFilePath).getLen();
  
      // allocate an array that can hold up to the list size of items ...
      byte[] buffer = new byte[DEFAULT_LOGITEM_LIST_SIZE * LogItem.ItemSize_Bytes];
      
      // calcuate item count 
      int totalItemCount = (int)( (fileSize - getHeaderSize()) / LogItem.ItemSize_Bytes);
          
      // get a reader ... 
      
      hdfsInputStream = fs.open(logFilePath);
  
      int headerItemCount = readHeader(hdfsInputStream);
      
      if (headerItemCount != totalItemCount) { 
        LOG.warn("CrawlSegmentLog - header item count for log file:" + logFilePath.toString() + " is:"+ headerItemCount + " file size indicates:" + totalItemCount);
        totalItemCount = headerItemCount;
      }
      
      int remainingItemCount = totalItemCount;
      
      
      LogItemBuffer itemList = new LogItemBuffer(0,0);
  
      while (remainingItemCount != 0) {
        
        int blockItemCount = Math.min(remainingItemCount,DEFAULT_LOGITEM_LIST_SIZE);
        
        // and read the data 
        hdfsInputStream.read(buffer,0,(int)blockItemCount * LogItem.ItemSize_Bytes);
        
        // populate the item list   
        itemList.loadFromStream(buffer,blockItemCount);
        
        // for walk items in list 
        for (int i=0;i<itemList.getItemCount();++i) { 
          LogItem item = itemList.getItems()[i];
          callback.processItem(item._hostFP,item._itemFP);
        }
        // reduce item count 
        remainingItemCount -= blockItemCount;
      }
    }
    finally { 
      if (hdfsInputStream != null) { 
        hdfsInputStream.close();
      }
    }
  }  
  
  public static int reconcileItemList(LogItemBuffer itemList,CrawlSegmentFPMap segment) { 
   
    int processedItemCount = 0;

    URLFPV2 urlfp = new URLFPV2();
    
    // and now walk segment and list consolidating segment as we go along ... 
    for (int i=0;i<itemList.getItemCount();++i) {
      
      LogItem item  = itemList.getItems()[i];
      
      urlfp.setDomainHash(item._hostFP);
      urlfp.setUrlHash(item._itemFP);
      
      if (segment.isValidSegmentURL(urlfp)) { 
        //update local bloom filter ... 
        segment.setCrawled(urlfp); 
        // increment processed item count 
        processedItemCount++;
      }
    }
    return processedItemCount;
  }
}
