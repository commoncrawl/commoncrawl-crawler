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

package org.commoncrawl.service.crawlhistory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.record.Buffer;
import org.commoncrawl.async.Timer;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.db.RecordStore;
import org.commoncrawl.db.RecordStore.RecordStoreException;
import org.commoncrawl.protocol.BulkItemHistoryQuery;
import org.commoncrawl.protocol.BulkItemHistoryQueryResponse;
import org.commoncrawl.protocol.BulkUpdateData;
import org.commoncrawl.protocol.CrawlHistoryStatus;
import org.commoncrawl.protocol.CrawlerHistoryService;
import org.commoncrawl.protocol.SingleItemHistoryQueryResponse;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.protocol.CrawlHistoryStatus.CheckpointState;
import org.commoncrawl.rpc.base.internal.AsyncClientChannel;
import org.commoncrawl.rpc.base.internal.AsyncContext;
import org.commoncrawl.rpc.base.internal.AsyncServerChannel;
import org.commoncrawl.rpc.base.internal.NullMessage;
import org.commoncrawl.rpc.base.internal.AsyncRequest.Status;
import org.commoncrawl.rpc.base.shared.RPCException;
import org.commoncrawl.server.CommonCrawlServer;
import org.commoncrawl.service.crawler.CrawlSegmentLog;
import org.commoncrawl.service.crawlhistory.HistoryServerState;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.ImmutableBuffer;
import org.commoncrawl.util.URLFPBloomFilter;
import org.commoncrawl.util.BitUtils.BitStream;
import org.commoncrawl.util.time.Hour;

/**
 * 
 * 
 * @author rana
 *
 */
public class CrawlHistoryServer extends CommonCrawlServer
 implements CrawlerHistoryService, AsyncServerChannel.ConnectionCallback{

  
  private int _numElements = -1;
  private int _numHashFunctions = -1;
  private int _bitsPerElement = -1;
  private int _crawlNumber = -1;
  private URLFPBloomFilter _bloomFilter = null;
  /** primary crawler database **/
  RecordStore   _recordStore = new RecordStore();
  /** server state record key **/
  String HistoryServerStateKey = "HistoryServerState";
  /** server state object **/
  HistoryServerState _state;



  private static final Log LOG = LogFactory.getLog(CrawlHistoryServer.class);

  
  @Override
  protected String getDefaultDataDir() {
    return CrawlEnvironment.DEFAULT_DATA_DIR;
  }

  @Override
  protected String getDefaultHttpInterface() {
    return CrawlEnvironment.DEFAULT_HTTP_INTERFACE;
  }

  @Override
  protected int getDefaultHttpPort() {
    return CrawlEnvironment.DEFAULT_CRAWLER_HISTORY_HTTP_PORT;
  }

  @Override
  protected String getDefaultLogFileName() {
    return "historyserver.log";
  }

  @Override
  protected String getDefaultRPCInterface() {
    return CrawlEnvironment.DEFAULT_RPC_INTERFACE;
  }

  @Override
  protected int getDefaultRPCPort() {
    return CrawlEnvironment.DEFAULT_CRAWLER_HISTORY_RPC_PORT;
  }

  @Override
  protected String getWebAppName() {
    return CrawlEnvironment.CRAWLER_HISTORY_WEBAPP_NAME;
  }

  @Override
  protected boolean initServer() {
    File dataPath = getDataDirectory();
    File dbPath   = new File(dataPath,CrawlEnvironment.CRAWLER_HISTORY_DB);
    // now initialize the recorstore ... 
    try {
      // initialize database
      _recordStore.initialize(dbPath, null);
      
      _state = new HistoryServerState();
      _state.setCurrentCheckpointState(CheckpointState.ACTIVE);
      _state.setCurrentCrawlNumber(_crawlNumber);
      
      updateState();
      
      // load bloom filter from disk if possible 
      loadBloomFilter();
      
      // create server channel ... 
      AsyncServerChannel channel = new AsyncServerChannel(this, this.getEventLoop(), this.getServerAddress(),this);
      
      // register RPC services it supports ... 
      registerService(channel,CrawlerHistoryService.spec);
      
      // start the checkpoint thread ...
      startCheckpointThread(CrawlEnvironment.getDefaultFileSystem());
      
    } catch (IOException e) {
      LOG.error(CCStringUtils.stringifyException(e));
    }
    return true;
  }
  
  /** do a clean shutdown (if possible) **/
  @Override
  public void stop() {
    
    // ok, wait to grab the checkpoint thread semaphore 
    LOG.info("Server Shutdown Detected. Waiting on checkpoint thread");
    _shutdownFlag = true;
    _checkpointThreadSemaphore.acquireUninterruptibly();
    LOG.info("Checkpoint thread semaphore acquired. Joining checkpoint thread ... ");
    if (_checkpointThread != null) { 
      try {
        _checkpointThread.join();
      } catch (Exception e) {
        LOG.error("Exception while waiting for Checkpoint Thread shutdown:" + CCStringUtils.stringifyException(e));
      }
    }
    // ok safe to call super now ... 
    super.stop();
  }
    
  /** update and persist state data structure **/
  private void updateState() throws RecordStoreException {
    _recordStore.beginTransaction();
    _recordStore.updateRecordByKey(HistoryServerStateKey, _state);
    _recordStore.commitTransaction();
  }
  
  
  @Override
  protected boolean parseArguments(String[] argv) {
    
    for(int i=0; i < argv.length;++i) {
      if (argv[i].equalsIgnoreCase("--numElements")) { 
        if (i+1 < argv.length) { 
          _numElements = Integer.parseInt(argv[++i]);
        }
      }
      else if (argv[i].equalsIgnoreCase("--numHashFunctions")) { 
        if (i+1 < argv.length) { 
          _numHashFunctions = Integer.parseInt(argv[++i]);
        }
      }
      else if (argv[i].equalsIgnoreCase("--numBitsPerElement")) { 
        if (i+1 < argv.length) { 
          _bitsPerElement = Integer.parseInt(argv[++i]);
        }
      }
      else if (argv[i].equalsIgnoreCase("--crawlNumber")) {
        _crawlNumber = Integer.parseInt(argv[++i]);
      }
    }
    
    return (_numElements != -1 && _numHashFunctions != -1 && _bitsPerElement != -1 && _crawlNumber != -1);
  }

  @Override
  protected void printUsage() {
    
  }

  @Override
  protected boolean startDaemons() {
    return true;
  }

  @Override
  protected void stopDaemons() {
    
  }

  @Override
  public void checkpoint(AsyncContext<CrawlHistoryStatus, CrawlHistoryStatus> rpcContext)throws RPCException {
    LOG.info("Received Checkpoint Command with CrawlerNumber:" + rpcContext.getInput().getActiveCrawlNumber());
    if (_state.getCurrentCrawlNumber() < rpcContext.getInput().getActiveCrawlNumber() || _bloomFilter ==   null) {
      rpcContext.setStatus(Status.Error_RequestFailed);
      rpcContext.setErrorDesc("Incoming Version:" + rpcContext.getInput().getActiveCrawlNumber() + " Expected:" + _state.getCurrentCrawlNumber());
      rpcContext.completeRequest();
    }
    else {
      
      if (_state.getCurrentCheckpointState()  == CrawlHistoryStatus.CheckpointState.ACTIVE && rpcContext.getInput().getCheckpointState() == CrawlHistoryStatus.CheckpointState.TRANSITIONING) { 
        LOG.info("Moving to Transitioning State");
        moveToTransitioningState(rpcContext);
      }
      else { 
        LOG.info("Current Crawl Number equals Checkpoint Command Crawl Number. Ignoring Checkpoint Command");
        //NOOP ... just ignore the request ...and echo current crawl number ... 
        rpcContext.getOutput().setActiveCrawlNumber(_state.getCurrentCrawlNumber());
        rpcContext.getOutput().setCheckpointState(_state.getCurrentCheckpointState());
        rpcContext.completeRequest();
      }
    }
  }

  @Override
  public void bulkItemQuery(AsyncContext<BulkItemHistoryQuery, BulkItemHistoryQueryResponse> rpcContext)throws RPCException {
    LOG.info("Received BulkItemQueryRequest");
    ImmutableBuffer inputBuffer = rpcContext.getInput().getFingerprintList();
    
    if (inputBuffer.getCount() != 0) {  
      try { 
        
        if (_bloomFilter == null) { 
          throw new IOException("BloomFilter Not Initilized. Invalid Server State!");
        }
        
        DataInputStream inputStream = new DataInputStream(
                                        new ByteArrayInputStream(inputBuffer.getReadOnlyBytes(),0,inputBuffer.getCount()));
  
        BitStream bitStreamOut = new BitStream();
        
        URLFPV2 fingerprint = new URLFPV2();
        
        int itemsPresent = 0;
        while (inputStream.available() != 0) { 
          fingerprint.setDomainHash(WritableUtils.readVLong(inputStream));
          fingerprint.setUrlHash(WritableUtils.readVLong(inputStream)); 
          if (_bloomFilter.isPresent(fingerprint)) { 
            bitStreamOut.addbit(1);
            ++itemsPresent;
          }
          else { 
            bitStreamOut.addbit(0);
          }
        }
        
        LOG.info("Received BulkItemQueryRequest Completed with " + itemsPresent + " items found");
        
        rpcContext.getOutput().setResponseList(new Buffer(bitStreamOut.bits,0,(bitStreamOut.nbits + 7) / 8));
      }
      catch (IOException e) { 
        LOG.error(CCStringUtils.stringifyException(e));
        rpcContext.setStatus(Status.Error_RequestFailed);
        rpcContext.setErrorDesc(CCStringUtils.stringifyException(e));
      }
      rpcContext.completeRequest();
    }
      
  }
  
  @Override
  public void singleItemQuery(AsyncContext<URLFPV2, SingleItemHistoryQueryResponse> rpcContext)throws RPCException {
    try { 
      if (_bloomFilter == null) { 
        throw new IOException("BloomFilter Not Initilized. Invalid Server State!");
      }
      rpcContext.getOutput().setWasCrawled(_bloomFilter.isPresent(rpcContext.getInput()));
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      rpcContext.setStatus(Status.Error_RequestFailed);
      rpcContext.setErrorDesc(CCStringUtils.stringifyException(e));
    }
    rpcContext.completeRequest();
  }

  @Override
  public void updateHistory(AsyncContext<URLFPV2, NullMessage> rpcContext)throws RPCException {
    try { 
      if (_bloomFilter == null) { 
        throw new IOException("BloomFilter Not Initilized. Invalid Server State!");
      }
      _bloomFilter.add(rpcContext.getInput());
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      rpcContext.setStatus(Status.Error_RequestFailed);
      rpcContext.setErrorDesc(CCStringUtils.stringifyException(e));
    }
    rpcContext.completeRequest();
  }

  
  private Path getDataFileBasePath() {
    return new Path(CrawlEnvironment.HDFS_HistoryServerBase,getHostName());
  }
  
  private Path getDataFileFinalPath() {
    return new Path(CrawlEnvironment.HDFS_HistoryServerBase,getHostName()+".data");
  }

  private Path getDataFileCheckpointPath() {
    return new Path(CrawlEnvironment.HDFS_HistoryServerBase,getHostName()+".checkpoint");
  }
  
  private Path getCheckpointMutexPath() {
    Hour hour = new Hour(new Date());
    return new Path(CrawlEnvironment.HDFS_HistoryServerBase+CrawlEnvironment.HDFS_HistoryServerCheckpointMutex+"."+hour.getFirstMillisecond());
  }
  
  private List<Path> reloadActiveHistory()throws IOException {
    ArrayList<Path> paths = new ArrayList<Path>();
    FileSystem fs = CrawlEnvironment.getDefaultFileSystem();
    
    // create scan pattern 
    Path hdfsScanPath = 
      new Path(   CrawlEnvironment.getCrawlSegmentDataDirectory() 
                + "/" 
                + _state.getCurrentCrawlNumber() 
                + "/*/"
                + CrawlEnvironment.buildCrawlSegmentLogCheckpointWildcardString(getHostName()));
    
    // scan hdfs for log files
    FileStatus candidates[];
    
    LOG.info("Scanning For Cadnidates in:" + hdfsScanPath);
    candidates = fs.globStatus(hdfsScanPath);

    // iterate candidates 
    for(FileStatus candidate : candidates) { 
      
      // ok found a candidate we can work on 
      LOG.info("Found Candidate:" + candidate.getPath());
      final URLFPV2 placeHolderFP = new URLFPV2();
      CrawlSegmentLog.walkFingerprintsInLogFile(fs,candidate.getPath(),new CrawlSegmentLog.LogFileItemCallback() {
        
        @Override
        public void processItem(long domainHash, long urlFingerprint) {
          placeHolderFP.setDomainHash(domainHash);
          placeHolderFP.setUrlHash(urlFingerprint);
          // add item for bloom filter 
          _bloomFilter.add(placeHolderFP);
        }
      });
      LOG.info("Finished Processing Candidate:" + candidate.getPath());
      
      paths.add(candidate.getPath());
    }
    
    return paths;
  }
  
  private void reloadLaggingHistory(int previousCrawlNumber)throws IOException {
    FileSystem fs = CrawlEnvironment.getDefaultFileSystem();
    
    // create scan pattern 
    Path hdfsScanPath = 
      new Path(   CrawlEnvironment.getCrawlSegmentDataDirectory() 
                + "/" 
                + previousCrawlNumber 
                + "/*/"
                + CrawlEnvironment.buildCrawlSegmentLogCheckpointWildcardString(getHostName()));
    
    // scan hdfs for log files
    FileStatus candidates[];
    
    LOG.info("Scanning For Cadnidates in:" + hdfsScanPath);
    candidates = fs.globStatus(hdfsScanPath);

    // iterate candidates 
    for(FileStatus candidate : candidates) { 
      
      // ok found a candidate we can work on 
      LOG.info("Found Candidate:" + candidate.getPath());
      final URLFPV2 placeHolderFP = new URLFPV2();
      CrawlSegmentLog.walkFingerprintsInLogFile(fs,candidate.getPath(),new CrawlSegmentLog.LogFileItemCallback() {
        
        @Override
        public void processItem(long domainHash, long urlFingerprint) {
          placeHolderFP.setDomainHash(domainHash);
          placeHolderFP.setUrlHash(urlFingerprint);
          // add item for bloom filter 
          _bloomFilter.add(placeHolderFP);
        }
      });
      LOG.info("Finished Processing Candidate:" + candidate.getPath());
    }
  }
  
  private void moveToTransitioningState(final AsyncContext<CrawlHistoryStatus, CrawlHistoryStatus> rpcContext) {
    
    // start a thread an wait for checkpoint thread to purge all log files ... 
    new Thread(new Runnable() {

      @Override
      public void run() {
        // create scan pattern 
        Path hdfsScanPath = 
          new Path(   CrawlEnvironment.getCrawlSegmentDataDirectory() 
                    + "/" 
                    + _state.getCurrentCrawlNumber() 
                    + "/*/"
                    + CrawlEnvironment.buildCrawlSegmentLogCheckpointWildcardString(getHostName()));
        

        LOG.info("Scanning Log Directory at Path:" + hdfsScanPath + " for Log Files");
        // scan hdfs for log files
        while (true) {
          try {
            
            FileSystem fs = CrawlEnvironment.getDefaultFileSystem();
            
            if (fs.globStatus(hdfsScanPath).length != 0) {           
              LOG.info("Waiting for CheckpointThread to Purge All Existing Log Files for Crawl Number:" + _state.getCurrentCrawlNumber());
              try {
                Thread.sleep(5000);
              } catch (InterruptedException e) {
                
              }
            }
            else { 
              break;
            }
          } catch (IOException e) {
            LOG.error(CCStringUtils.stringifyException(e));
          }
        }
        
        LOG.info("Acquiring Checkpoint Thread Semaphore");
        _checkpointThreadSemaphore.acquireUninterruptibly();
        LOG.info("Acquired Checkpoint Thread Semaphore - Scheduling Async Callback");
        // ok now we can safely reset state, shift back to async thread ... 
        getEventLoop().setTimer(new Timer(0,false,new Timer.Callback() {
          
          @Override
          public void timerFired(Timer timer) {
            try {
              LOG.info("Updating State to Transitioning");
              // set server to appropriate state 
              _state.setCurrentCheckpointState(CrawlHistoryStatus.CheckpointState.TRANSITIONING);
              LOG.info("Serializing Database State");
              updateState();
              rpcContext.getOutput().setActiveCrawlNumber(_state.getCurrentCrawlNumber());
              rpcContext.getOutput().setCheckpointState(_state.getCurrentCheckpointState());
              
            } catch (IOException e) {
              LOG.error(CCStringUtils.stringifyException(e));
              rpcContext.setStatus(Status.Error_RequestFailed);
              rpcContext.setErrorDesc(CCStringUtils.stringifyException(e));
            }
            
            // complete the request ... 
            try {
              rpcContext.completeRequest();
            } catch (RPCException e) {
              LOG.error(CCStringUtils.stringifyException(e));
            }
            finally { 
              _checkpointThreadSemaphore.release();
            }
          }
        }
         ));

      }
      
    }).start();
    
  }
  Thread _resetThread = null; 
  private void resetBloomFilter(final AsyncContext<CrawlHistoryStatus, CrawlHistoryStatus> rpcContext){ 
    LOG.info("Got Reset BloomFilter RPC");
    if (_resetThread != null) { 
      rpcContext.setErrorDesc("Reset Already In Progress!");
      rpcContext.setStatus(Status.Error_RequestFailed);
      try {
        rpcContext.completeRequest();
      } catch (RPCException e) {
      }
    }
    else { 
      // ok, first we need to stop the checkpoint thread ... 
      // so, start a new thread and have it block on the checkpoint semaphore ... 
      _resetThread = new Thread(new Runnable(){
  
        @Override
        public void run() {
          LOG.info("Waiting for Checkpoint Thread to IDLE");
          // ok, now in the thread's context, safely block to acquire the checkpoint semaphore 
          _checkpointThreadSemaphore.acquireUninterruptibly();
          LOG.info("Checkpoint Thread to IDLE - Proceeding with BloomFilter Reset");
          // ok now we can safely reset state, shift back to async thread ... 
          getEventLoop().setTimer(new Timer(0,false,new Timer.Callback() {
            
            @Override
            public void timerFired(Timer timer) {
              try { 
                // ok now we are back in the async thread context  ...
                try { 
                  LOG.info("Deleting Existing Checkpoint Files");
                  // safely delete checkpoint files ... 
                  FileSystem fs = CrawlEnvironment.getDefaultFileSystem();
                  fs.delete(getDataFileCheckpointPath(),false);
                  fs.delete(getDataFileFinalPath(),false);
                  
                  LOG.info("Reseting BloomFilter");
                  // safely reset bloom filter 
                  _bloomFilter = null;
                  _bloomFilter = new URLFPBloomFilter(_numElements,_numHashFunctions,_bitsPerElement);
                  
                  // ok reload any lagging history ...
                  LOG.info("Reloading Lagging History");
                  reloadLaggingHistory(rpcContext.getInput().getActiveCrawlNumber() - 1);
                  
                  // and write out bloom filter
                  Path finalPath      = getDataFileFinalPath();
                  
                  LOG.info("Writing BloomFilter Data");
                  // serialize the filter ... 
                  serializeBloomFilter(finalPath);
  
                  LOG.info("Update Disk State");
                  _state.setCurrentCrawlNumber(rpcContext.getInput().getActiveCrawlNumber());
                  _state.setCurrentCheckpointState(CrawlHistoryStatus.CheckpointState.ACTIVE);
                  // write state to disk ... 
                  updateState();
  
                  LOG.info("Transition to new CrawlNumber:" + rpcContext.getInput().getActiveCrawlNumber() + " complete");
                  // update status 
                  rpcContext.getOutput().setActiveCrawlNumber(_state.getCurrentCrawlNumber());
                  rpcContext.getOutput().setCheckpointState(_state.getCurrentCheckpointState());
                }
                catch(IOException e) { 
                  LOG.error(CCStringUtils.stringifyException(e));
                  rpcContext.setStatus(Status.Error_RequestFailed);
                  rpcContext.setErrorDesc(CCStringUtils.stringifyException(e));
                }
                // complete the request ... 
                try {
                  rpcContext.completeRequest();
                } catch (RPCException e) {
                  LOG.error(CCStringUtils.stringifyException(e));
                }
              }
              finally { 
                // reset scan variables
                _lastCheckpointScanTime = -1; 
                _lastCheckpointFlushTime = 1;
                _checkpointThreadSemaphore.release();
                _resetThread = null;
              }
              
            }
          }));
        }
      });
      _resetThread.start();
    }
  }
  
  private void serializeBloomFilter(Path checkpointPath) throws IOException { 

    FileSystem fs = CrawlEnvironment.getDefaultFileSystem();
    
    // delete existing ... 
    fs.delete(checkpointPath,false);
    
    FSDataOutputStream outputStream = fs.create(checkpointPath);
    
    try { 
      DataOutputStream dataOut = new DataOutputStream(outputStream);
      
      dataOut.writeInt(0); // version 
      dataOut.writeInt(_state.getCurrentCrawlNumber()); // crawl number ... 
      
      // serialize bloom filter contents ... 
      _bloomFilter.serialize(outputStream);
    }
    finally { 
      if (outputStream != null) { 
        outputStream.flush();
        outputStream.close();
      }
    }
  }
  
  private void deSerializeBloomFilter(Path checkpointPath) throws IOException { 

    FileSystem fs = CrawlEnvironment.getDefaultFileSystem();
    
    FSDataInputStream stream = fs.open(checkpointPath);
    
    try { 
      
      stream.readInt(); // version  
      stream.readInt(); // crawl number ... 
      
      // serialize bloom filter contents ... 
      _bloomFilter = URLFPBloomFilter.load(stream);
    }
    finally {
      stream.close();
    }
  }
  
  
  private boolean validateOnDiskVersion() throws IOException { 
    FileSystem fs = CrawlEnvironment.getDefaultFileSystem();
    Path dataFilePath = getDataFileFinalPath();
    LOG.info("Loading BloomFilter From Disk at Path:" + dataFilePath);
    if (fs.exists(dataFilePath)) { 
      FSDataInputStream stream = null;
      try { 
        stream = fs.open(dataFilePath);
        DataInputStream dataInput = new DataInputStream(stream);
        // skip version
        dataInput.readInt();
        // read crawl version ... 
        int serializedCrawlVersion = dataInput.readInt();
        LOG.info("BloomFilter From On Disk has CrawlVersion:" + serializedCrawlVersion);
        if (serializedCrawlVersion < _state.getCurrentCrawlNumber()) {
          LOG.error("skipping load because serial crawl number is less than current crawl");
          stream.close();
          stream = null;
          fs.rename(dataFilePath, new Path(dataFilePath.getParent(),dataFilePath.getName()+"-V-"+serializedCrawlVersion));
          return false;
        }
        return true;
      }
      finally { 
        if (stream != null) 
          stream.close();
      }
    }
    return false;
  }
  
  private void loadBloomFilter() throws IOException { 
    FileSystem fs = CrawlEnvironment.getDefaultFileSystem();
    Path dataFilePath = getDataFileFinalPath();
    LOG.info("Potentially Loading BloomFilter From Disk at Path:" + dataFilePath);
    if (!validateOnDiskVersion()) { 
      LOG.info("On Disk Verison Not Valid. Allocating New BloomFilter at Path:" + dataFilePath);
      LOG.info("Allocating NEW BloomFilter");
      _bloomFilter = new URLFPBloomFilter(_numElements,_numHashFunctions,_bitsPerElement);
    }
    else {
      LOG.info("Loading BloomFilter From Disk");
      deSerializeBloomFilter(dataFilePath);
    }
    List<Path> paths = reloadActiveHistory();
    if (paths.size() != 0) { 
      LOG.info("Loaded Some History Via Log Files - Writing Back to Disk");
      serializeBloomFilter(dataFilePath);
      
      for (Path historyFile : paths) { 
        fs.delete(historyFile,false);
      }
    }
  }
  
  private Thread _checkpointThread = null;
  private Semaphore _checkpointThreadSemaphore = new Semaphore(1);
  private boolean _shutdownFlag = false;
  /** checkpoint paths **/
  private TreeSet<Path> _processedPaths = new TreeSet<Path>();
  /** last checkpoint time **/
  private long   _lastCheckpointScanTime = -1;
  private long   _lastCheckpointFlushTime = -1;
  private static final int CHECKPOINT_MUTEX_ACQUISITON_DELAY = 60000 * 2;
  /** urls process since last checkpoint **/
  private AtomicInteger    _urlsProcessedSinceCheckpoint = new AtomicInteger();
  /** checkpoint scan interval **/
  private static final int CHECKPOINT_SCAN_INTERVAL = 60000; // every minute 
  /** checkpoint flush interval **/
  private static final int CHECKPOINT_FLUSH_INTERVAL = 15 * 60 * 1000; // 15 minutes 
  
  
  
  private void startCheckpointThread(final FileSystem fs) { 
    
    _checkpointThread = new Thread(new Runnable() {

      @Override
      public void run() {
        
        
        // ok check point thread run in perpetuty
        while (!_shutdownFlag) {
          
          if (_lastCheckpointScanTime == -1 
              || _lastCheckpointFlushTime == -1
               || (System.currentTimeMillis() - _lastCheckpointScanTime) >=  CHECKPOINT_SCAN_INTERVAL
                || (System.currentTimeMillis() - _lastCheckpointFlushTime) >=  CHECKPOINT_FLUSH_INTERVAL) { 
          
            //LOG.info("Checkpoint Thread Grabbing Semaphore");
            // grab checkpoint thread semaphore 
            _checkpointThreadSemaphore.acquireUninterruptibly();
            //LOG.info("Checkpoint Thread Grabbed Semaphore");
  
            try { 
              // create scan pattern 
              Path hdfsScanPath = 
                new Path(   CrawlEnvironment.getCrawlSegmentDataDirectory() 
                          + "/" 
                          + _state.getCurrentCrawlNumber() 
                          + "/*/"
                          + CrawlEnvironment.buildCrawlSegmentLogCheckpointWildcardString(getHostName()));
              
              // scan hdfs for log files
              FileStatus candidates[];
              try {
                LOG.info("Checkpoint Thread Scanning For Cadnidates in:" + hdfsScanPath);
                candidates = fs.globStatus(hdfsScanPath);
    
                // iterate candidates 
                for(FileStatus candidate : candidates) { 
                  
                  // check candidate against processed path list ... 
                  if (!_processedPaths.contains(candidate.getPath())){
                    int urlCountBeforeProcessing = _urlsProcessedSinceCheckpoint.get();
                    // ok found a candidate we can work on 
                    LOG.info("Checkpoint Thread Found Candidate:" + candidate.getPath());
                    final URLFPV2 placeHolderFP = new URLFPV2();
                    CrawlSegmentLog.walkFingerprintsInLogFile(fs,candidate.getPath(),new CrawlSegmentLog.LogFileItemCallback() {
                      
                      @Override
                      public void processItem(long domainHash, long urlFingerprint) {
                        placeHolderFP.setDomainHash(domainHash);
                        placeHolderFP.setUrlHash(urlFingerprint);
                        // add item for bloom filter 
                        _bloomFilter.add(placeHolderFP);
                        // inrement urls processed count ...
                        _urlsProcessedSinceCheckpoint.addAndGet(1);
                      }
                    });
                    _processedPaths.add(candidate.getPath());
                    LOG.info("Finished Processing Candidate:" + candidate.getPath());
                  }
                }
                
                // update scan time ... 
                _lastCheckpointScanTime = System.currentTimeMillis();

                // see if can do a full checkpoint ... 
                if (_lastCheckpointFlushTime == -1 || System.currentTimeMillis() - _lastCheckpointFlushTime >= CHECKPOINT_FLUSH_INTERVAL) {
                      
                  int approximateItemsToFlush = _urlsProcessedSinceCheckpoint.get();
                  // ok at this point we are read to initialize a checkpoint 
                  if (approximateItemsToFlush != 0) {
                    
                    Path checkpointMutexPath = getCheckpointMutexPath();
                    
                    if (fs.createNewFile(checkpointMutexPath)) {
                      try { 
                        LOG.info("Checkpoint Thread Starting Checkpoint");
                    
                        // get the checkpoint path ... 
                        Path checkpointPath = getDataFileCheckpointPath();
                        Path finalPath      = getDataFileFinalPath();
                        
                        LOG.info("Checkpoint Thread Writing BloomFilter Data");
                        // serialize the filter ... 
                        serializeBloomFilter(checkpointPath);
                        
                        LOG.info("Checkpoint Thread Deleting Old Checkpoint Data");
                        // ok now everything seems to have gone fine ... delete existing data file 
                        fs.delete(finalPath,false);
                        LOG.info("Checkpoint Thread ReWriting New Checkpoint Data");
                        // rename checkpoint to final ... 
                        fs.rename(checkpointPath, finalPath);
                        
                        if (_state.getCurrentCheckpointState() != CrawlHistoryStatus.CheckpointState.TRANSITIONING) { 
                          LOG.info("Checkpoint Thread Deleting Processed Files");
                          // ok safely delete all processed files
                          for (Path processedFilePath: _processedPaths){ 
                            fs.delete(processedFilePath,false);
                          }
                          _processedPaths.clear();
                        }
                        else { 
                          LOG.info("Skipping Processed Files Purge because we are in Transitioning State");
                        }
                        _urlsProcessedSinceCheckpoint.addAndGet(-approximateItemsToFlush);
                      }
                      finally { 
                        LOG.info("Checkpoint Thread Releasing Mutex:" + checkpointMutexPath);
                        fs.delete(checkpointMutexPath, false);
                      }
                    }
                    else { 
                      int delay = (int)(Math.random() * CHECKPOINT_MUTEX_ACQUISITON_DELAY);
                      LOG.info("Checkpoint thread failed to acquire Mutex:" + checkpointMutexPath + " Waiting " + delay + "(MS) before retry");
                      try {
                        Thread.sleep(delay);
                      } catch (InterruptedException e) {
                      }
                    }
                  }
                  // update last checkpoint no time no matter what ...
                  _lastCheckpointFlushTime = System.currentTimeMillis();
                }
                
              } catch (IOException e) {
                LOG.error("Checkpoint Thread Bloom Filter Checkpoint Failed with Exception:" + CCStringUtils.stringifyException(e));
                try {
                  Thread.sleep(60000);
                } catch (InterruptedException e1) {
                }
              }
            }
            finally { 
              LOG.info("Checkpoint Thread Releasing Checkpoint Semaphore");
              _checkpointThreadSemaphore.release();
            }
          }
          else { 
            try {
              //LOG.info("Checkpoint Thread IDLE");
              Thread.sleep(100);
            } catch (InterruptedException e) {
            }
          }
        }
        
      } 
      
    });
    _checkpointThread.start();
  }

  @Override
  public void IncomingClientConnected(AsyncClientChannel channel) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void IncomingClientDisconnected(AsyncClientChannel channel) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void queryStatus(AsyncContext<NullMessage, CrawlHistoryStatus> rpcContext)throws RPCException {
    rpcContext.getOutput().setActiveCrawlNumber(_state.getCurrentCrawlNumber());
    rpcContext.getOutput().setCheckpointState(_state.getCurrentCheckpointState());
    rpcContext.completeRequest();
  }

  @Override
  public void sync(final AsyncContext<CrawlHistoryStatus, NullMessage> rpcContext) throws RPCException {
    LOG.info("Received Sync From Crawler");
    // validate crawl number 
    if (_state.getCurrentCrawlNumber() == rpcContext.getInput().getActiveCrawlNumber()) { 
      // snapshot current time 
      final long startTime = System.currentTimeMillis();
      
      // ok reset resync variable on checkpoint thread 
      _lastCheckpointScanTime = -1;
      // now set a timer to poll periodically for resync to complete
      getEventLoop().setTimer(new Timer(100,true,new Timer.Callback() {
        
        @Override
        public void timerFired(Timer timer) {
          // ok check to see if resync happened ... 
          if (_lastCheckpointScanTime >= startTime) { 
            getEventLoop().cancelTimer(timer);
            try {
              rpcContext.completeRequest();
            } catch (RPCException e) {
              LOG.error(CCStringUtils.stringifyException(e));
            }
          }
        }
      }));
    }
    else { 
      LOG.error("Crawler CrawlNumber and HistoryServer CrawlNumber don't match! - Aborting Sync");
      rpcContext.setStatus(Status.Error_RequestFailed);
      rpcContext.setErrorDesc("Crawler CrawlNumber and HistoryServer CrawlNumber don't match! - Aborting Sync");
      rpcContext.completeRequest();
    }
  }

  @Override
  public void bulkUpdateHistory(AsyncContext<BulkUpdateData, NullMessage> rpcContext) throws RPCException {

    LOG.info("Received BulkUpdate Request");
    ImmutableBuffer inputBuffer = rpcContext.getInput().getFingerprintList();
    
    if (inputBuffer.getCount() != 0) {  
      try { 
        
        if (_bloomFilter == null) { 
          throw new IOException("BloomFilter Not Initilized. Invalid Server State!");
        }
        
        DataInputStream inputStream = new DataInputStream(
                                        new ByteArrayInputStream(inputBuffer.getReadOnlyBytes(),0,inputBuffer.getCount()));
  
        URLFPV2 fingerprint = new URLFPV2();
        
        int itemsAdded = 0;
        while (inputStream.available() != 0) { 
          fingerprint.setDomainHash(WritableUtils.readVLong(inputStream));
          fingerprint.setUrlHash(WritableUtils.readVLong(inputStream)); 
          _bloomFilter.add(fingerprint); 
          ++itemsAdded;
        }
        _urlsProcessedSinceCheckpoint.addAndGet(itemsAdded);
        
        LOG.info("Finished Processed BulkUpdate Request. " + itemsAdded + " items processed." );
      }
      catch (IOException e) { 
        LOG.error(CCStringUtils.stringifyException(e));
        rpcContext.setStatus(Status.Error_RequestFailed);
        rpcContext.setErrorDesc(CCStringUtils.stringifyException(e));
      }
      rpcContext.completeRequest();
    }
  }

  
}
