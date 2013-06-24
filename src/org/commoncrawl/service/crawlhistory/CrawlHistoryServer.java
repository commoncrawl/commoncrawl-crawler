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
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
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
import org.commoncrawl.protocol.BulkItemHistoryQuery;
import org.commoncrawl.protocol.BulkItemHistoryQueryResponse;
import org.commoncrawl.protocol.BulkUpdateData;
import org.commoncrawl.protocol.CrawlHistoryStatus;
import org.commoncrawl.protocol.CrawlMaster;
import org.commoncrawl.protocol.CrawlerHistoryService;
import org.commoncrawl.protocol.SingleItemHistoryQueryResponse;
import org.commoncrawl.protocol.SlaveHello;
import org.commoncrawl.protocol.SlaveRegistration;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.protocol.CrawlHistoryStatus.CheckpointState;
import org.commoncrawl.rpc.base.internal.AsyncClientChannel;
import org.commoncrawl.rpc.base.internal.AsyncContext;
import org.commoncrawl.rpc.base.internal.AsyncRequest;
import org.commoncrawl.rpc.base.internal.AsyncServerChannel;
import org.commoncrawl.rpc.base.internal.NullMessage;
import org.commoncrawl.rpc.base.internal.AsyncRequest.Status;
import org.commoncrawl.rpc.base.shared.RPCException;
import org.commoncrawl.server.CommonCrawlServer;
import org.commoncrawl.service.crawler.CrawlSegmentLog;
import org.commoncrawl.service.crawlhistory.HistoryServerState;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.IPAddressUtils;
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
 implements CrawlerHistoryService, AsyncServerChannel.ConnectionCallback, AsyncClientChannel.ConnectionCallback,Timer.Callback  {

  
  InetAddress _masterIP;
  int         _masterPort = -1;
  
  private int _numElements = -1;
  private int _numHashFunctions = -1;
  private int _bitsPerElement = -1;
  private int _crawlNumber = 0;
  private URLFPBloomFilter _bloomFilter = null;
  /** server state record key **/
  String HistoryServerStateKey = "HistoryServerState";
  /** server state object **/
  HistoryServerState _state;
  /** checkpoint file system **/
  FileSystem _checkpointFS;
  /** segment log fs **/
  FileSystem _segmentLogFS;
  /** base storage path **/
  Path _baseStoragePath = new Path(CrawlEnvironment.HDFS_HistoryServerBase);
  /** segment storage dir **/
  Path _segmentLogsStoragePath = new Path(CrawlEnvironment.getCrawlSegmentLogsDirectory());
  /** timers **/
  Timer _handshakeTimer;
  
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
  /** default checkpoint scan interval **/
  private static final int DEFAULT_CHECKPOINT_SCAN_INTERVAL = 60000; // every minute 
  /** default checkpoint flush interval **/
  private static final int DEFAULT_CHECKPOINT_FLUSH_INTERVAL = 10 * 60 * 1000; // 10 minutes 
  
  private int _checkpointScanInterval = DEFAULT_CHECKPOINT_SCAN_INTERVAL;
  private int _checkpointFlushInterval = DEFAULT_CHECKPOINT_FLUSH_INTERVAL;

  enum HandshakeState { 
    NOT_INITIATED,
    INITIATING,
    IDLE,
    RENEWING
  }
  
  HandshakeState _handshakeState = HandshakeState.NOT_INITIATED;
  boolean _connectedToMaster = false;
  SlaveRegistration _registration = null;
  
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
  
  /** 
   * 
   * @return the instance id based name for this host 
   * @throws IOException
   */
  String getHostId() throws IOException { 
    if (_registration != null) { 
      return CrawlEnvironment.getCrawlerNameGivenId(_registration.getInstanceId());
    }
    throw new IOException("Invalid State. No Established Instance Id!");
  }

  AsyncClientChannel _masterChannel = null;
  CrawlMaster.AsyncStub _masterRPCStub;
  AsyncServerChannel _serverChannel  = null;
  
  @Override
  protected boolean initServer() {

    try { 
      InetSocketAddress masterAddress = new InetSocketAddress(_masterIP,_masterPort);
  
      InetSocketAddress serverAddress = new InetSocketAddress(_serverAddress.getAddress(),0);
          
      _masterChannel = new AsyncClientChannel(getEventLoop(),serverAddress,masterAddress,this);
      _masterRPCStub = new CrawlMaster.AsyncStub(_masterChannel);
      _masterChannel.open();
      
      // create server channel ... 
      _serverChannel = new AsyncServerChannel(this, this.getEventLoop(), this.getServerAddress(),this);
      
      // register RPC services it supports ... 
      registerService(_serverChannel,CrawlerHistoryService.spec);
      
      
      _handshakeTimer = new Timer(1000,true,this);
      getEventLoop().setTimer(_handshakeTimer);
      
      return true;

    }
    catch (Exception e) { 
      LOG.error(CCStringUtils.stringifyException(e));
    }
    return false;
    
  }
  
  void startServices() {
    
    _shutdownFlag = false;

    // now initialize the recorstore ... 
    try {
      
      _state = new HistoryServerState();
      _state.setCurrentCheckpointState(CheckpointState.ACTIVE);
      _state.setCurrentCrawlNumber(_crawlNumber);
      
      updateState();
      
      // load bloom filter from disk if possible 
      loadBloomFilter();
            
      _serverChannel.open();
      
      // start the checkpoint thread ...
      startCheckpointThread();
      
    } catch (IOException e) {
      LOG.error(CCStringUtils.stringifyException(e));
    }
  }

  void shutdownServices() {
    _serverChannel.close();
    _registration = null;
    _handshakeState = HandshakeState.NOT_INITIATED;
    // ok, wait to grab the checkpoint thread semaphore 
    LOG.info("Server Shutdown Detected. Waiting on checkpoint thread");
    _shutdownFlag = true;
    try { 
      _checkpointThreadSemaphore.acquireUninterruptibly();
      LOG.info("Checkpoint thread semaphore acquired. Joining checkpoint thread ... ");
      if (_checkpointThread != null) { 
        try {
          _checkpointThread.join();
        } catch (Exception e) {
          LOG.error("Exception while waiting for Checkpoint Thread shutdown:" + CCStringUtils.stringifyException(e));
        }
      }
    }
    finally { 
      _checkpointThreadSemaphore.release();
    }
  }
  
  /** do a clean shutdown (if possible) **/
  @Override
  public void stop() {
    shutdownServices();
    super.stop();
  }
    
  /** update and persist state data structure **/
  private void updateState(){
    //NOOP now
  }
  
  
  @Override
  protected boolean parseArguements(String[] argv) {
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
      else if (argv[i].equalsIgnoreCase("--checkpointFS")) { 
        try {
          _checkpointFS = FileSystem.get(new URI(argv[++i]),getConfig());
        } catch (Exception e) {
          LOG.error(CCStringUtils.stringifyException(e));
        }
      }
      else if (argv[i].equalsIgnoreCase("--masterIP")) {
        try {
          _masterIP = InetAddress.getByName(argv[++i]);
        } catch (UnknownHostException e) {
          LOG.error(CCStringUtils.stringifyException(e));
        }
      }
      else if (argv[i].equalsIgnoreCase("--masterPort")) {
        _masterPort = Integer.parseInt(argv[++i]);
      }
      else if (argv[i].equalsIgnoreCase("--storageBase")) {
        _baseStoragePath = new Path(argv[++i]);
      }
      else if (argv[i].equalsIgnoreCase("--segmentLogsDir")) {
        _segmentLogsStoragePath = new Path(argv[++i]);
        try {
          _segmentLogFS = FileSystem.get(_segmentLogsStoragePath.toUri(),getConfig());
        } catch (IOException e) {
          LOG.error(CCStringUtils.stringifyException(e));
          return false;
        }
      }
      else if (argv[i].equalsIgnoreCase("--checkpointScanInterval")) {
        _checkpointScanInterval = Integer.parseInt(argv[++i]);
      }
      else if (argv[i].equalsIgnoreCase("--checkpointFlushInterval")) {
        _checkpointFlushInterval = Integer.parseInt(argv[++i]);
      }
      
    }

    if (_segmentLogFS == null) { 
      try {
        _segmentLogFS = FileSystem.get(_segmentLogsStoragePath.toUri(),getConfig());
      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
        return false;
      }
    }
    
    if (
        _numElements != -1 
        && _numHashFunctions != -1 
        && _bitsPerElement != -1 
        && _crawlNumber != -1 
        && _checkpointFS != null
        && _masterIP != null 
        && _masterPort != -1
        ) { 
      return true;
    }
    else { 
      LOG.error("Some Command Line Parameters Were Missing:");
      
      LOG.error("Parameter:_numElements Value:" + _numElements);
      LOG.error("Parameter:_numHashFunctions Value:" + _numHashFunctions);
      LOG.error("Parameter:_bitsPerElement Value:" + _bitsPerElement);
      LOG.error("Parameter:_crawlNumber Value:" + _crawlNumber);
      LOG.error("Parameter:_checkpointFS Value:" + _checkpointFS);
      LOG.error("Parameter:_segmentLogFS Value:" + _segmentLogFS);
      LOG.error("Parameter:_masterIP Value:" + _masterIP);
      LOG.error("Parameter:_masterPort Value:" + _masterPort);
      
      return false;
    }
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
    //NOOP
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
    finally { 
      rpcContext.completeRequest();
    }
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
    finally { 
      rpcContext.completeRequest();
    }
  }

  
  private final Path getDataFileBasePath() throws IOException { 
    return new Path(_baseStoragePath,getHostId());
  }
  
  private final Path getDataFileFinalPath()throws IOException { 
    return new Path(_baseStoragePath,getHostId()+".data");
  }

  private final Path getDataFileCheckpointPath()throws IOException { 
    return new Path(_baseStoragePath,getHostId()+".checkpoint");
  }
  
  private final Path getCheckpointMutexPath()throws IOException { 
    Hour hour = new Hour(new Date());
    Path checkpointPath = new Path(_baseStoragePath,CrawlEnvironment.HDFS_HistoryServerCheckpointMutex+"."+ getHostId() +"."+hour.getFirstMillisecond());
    return checkpointPath;
  }
  
  private List<Path> reloadActiveHistory()throws IOException {
    ArrayList<Path> paths = new ArrayList<Path>();
    FileSystem fs = _segmentLogFS;
    
    // create scan pattern 
    Path hdfsScanPath = CrawlEnvironment.getRemoteCrawlSegmentLogWildcardPath(_segmentLogsStoragePath,getHostId()); 
    
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
  

  private void serializeBloomFilter(Path checkpointPath) throws IOException { 

    FileSystem fs = _checkpointFS;
    
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

    FileSystem fs = _checkpointFS;
    
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
    FileSystem fs = _checkpointFS;
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
    FileSystem fs = _checkpointFS;
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
  
  
  
  private void startCheckpointThread() { 
    
    _checkpointThread = new Thread(new Runnable() {

      @Override
      public void run() {
        
        
        // ok check point thread run in perpetuty
        while (!_shutdownFlag) {
          
          if (_lastCheckpointScanTime == -1 
              || _lastCheckpointFlushTime == -1
               || (System.currentTimeMillis() - _lastCheckpointScanTime) >=  _checkpointScanInterval
                || (System.currentTimeMillis() - _lastCheckpointFlushTime) >=  _checkpointFlushInterval) { 
          
            //LOG.info("Checkpoint Thread Grabbing Semaphore");
            // grab checkpoint thread semaphore 
            _checkpointThreadSemaphore.acquireUninterruptibly();
            LOG.info("Checkpoint Thread Grabbed Semaphore");
  
            try { 

              try {
                // create scan pattern 
                Path hdfsScanPath = CrawlEnvironment.getRemoteCrawlSegmentLogWildcardPath(_segmentLogsStoragePath, getHostId());
                
                // scan hdfs for log files
                FileStatus candidates[];
                
                LOG.info("Scanning for logs in:" + _segmentLogsStoragePath + " using wildcard:" + hdfsScanPath + " FS is:" + _segmentLogFS);
                candidates = _segmentLogFS.globStatus(hdfsScanPath);

                // iterate candidates 
                for(FileStatus candidate : candidates) { 
                  
                  // check candidate against processed path list ... 
                  if (!_processedPaths.contains(candidate.getPath())){
                    int urlCountBeforeProcessing = _urlsProcessedSinceCheckpoint.get();
                    // ok found a candidate we can work on 
                    LOG.info("Checkpoint Thread Found Candidate:" + candidate.getPath());
                    final URLFPV2 placeHolderFP = new URLFPV2();
                    CrawlSegmentLog.walkFingerprintsInLogFile(_segmentLogFS,candidate.getPath(),new CrawlSegmentLog.LogFileItemCallback() {
                      
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
                if (_lastCheckpointFlushTime == -1 || System.currentTimeMillis() - _lastCheckpointFlushTime >= _checkpointFlushInterval) {
                      
                  int approximateItemsToFlush = _urlsProcessedSinceCheckpoint.get();
                  // ok at this point we are read to initialize a checkpoint 
                  if (approximateItemsToFlush != 0) {
                    
                    Path checkpointMutexPath = getCheckpointMutexPath();
                    
                    if (_checkpointFS.createNewFile(checkpointMutexPath)) {
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
                        _checkpointFS.delete(finalPath,false);
                        LOG.info("Checkpoint Thread ReWriting New Checkpoint Data");
                        // rename checkpoint to final ... 
                        _checkpointFS.rename(checkpointPath, finalPath);
                        
                        if (_state.getCurrentCheckpointState() != CrawlHistoryStatus.CheckpointState.TRANSITIONING) { 
                          LOG.info("Checkpoint Thread Deleting Processed Files");
                          // ok safely delete all processed files
                          for (Path processedFilePath: _processedPaths){ 
                            _segmentLogFS.delete(processedFilePath,false);
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
                        _checkpointFS.delete(checkpointMutexPath, false);
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
        LOG.info("Checkpoint Thread Exiting!");
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
    try {
      
      rpcContext.getOutput().setActiveCrawlNumber(_state.getCurrentCrawlNumber());
      rpcContext.getOutput().setCheckpointState(_state.getCurrentCheckpointState());
      rpcContext.setStatus(Status.Success);
    }
    finally { 
      rpcContext.completeRequest();
    }
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
          LOG.info("Timer Fired. Last Checkpoint Scan Time:" + _lastCheckpointScanTime + " Desired Threshold Time:" + startTime);
          if (_lastCheckpointScanTime >= startTime) { 
            getEventLoop().cancelTimer(timer);
            try {
              rpcContext.setStatus(Status.Success);
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
    
    try { 
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
      }
    }
    finally { 
      rpcContext.completeRequest();
    }
  }


  
  @Override
  public void OutgoingChannelConnected(AsyncClientChannel channel) {
    _connectedToMaster = true;
    initiateHandshake();
  }

  public void initiateHandshake() { 
    _handshakeState = HandshakeState.INITIATING;
    LOG.info("Connected to Master. Initiating Handshake");
    SlaveHello slaveHello = new SlaveHello();
    
    slaveHello.setIpAddress(IPAddressUtils.IPV4AddressToInteger(_serverAddress.getAddress().getAddress()));
    slaveHello.setCookie(System.currentTimeMillis());
    slaveHello.setServiceName("history");
    
    try { 
      _masterRPCStub.registerSlave(slaveHello,new AsyncRequest.Callback<SlaveHello, SlaveRegistration>() {
  
        @Override
        public void requestComplete(AsyncRequest<SlaveHello, SlaveRegistration> request) {
          if (request.getStatus() == Status.Success) {
            // LOG.info("Master Handshake Successfull");
            _registration = request.getOutput();
            _registration.setLastTimestamp(System.currentTimeMillis());
            _handshakeState = HandshakeState.IDLE;
            
            startServices();
          }
          else { 
            LOG.error("Handshake to Master Failed");
            _handshakeState = HandshakeState.NOT_INITIATED;
          }
        }
      });
    }
    catch (Exception e) { 
      LOG.error(CCStringUtils.stringifyException(e));
    }
    
  }
  
  @Override
  public boolean OutgoingChannelDisconnected(AsyncClientChannel channel) {
    LOG.error("Disconnected From Master. Shutting Down Services");
    _connectedToMaster = false;
    shutdownServices();

    return false;
  }

  @Override
  public void timerFired(Timer timer) {
    if (timer == _handshakeTimer) { 
      if (_handshakeState == HandshakeState.NOT_INITIATED) { 
        initiateHandshake();
      }
      else if (_handshakeState == HandshakeState.IDLE) { 
        if (_registration != null) { 
          if (System.currentTimeMillis() - _registration.getLastTimestamp() >= 1000) { 
            //LOG.info("Renewing Lease with Master");
            _handshakeState = HandshakeState.RENEWING;
            try {
              _masterRPCStub.extendRegistration(_registration, new AsyncRequest.Callback<SlaveRegistration, NullMessage>() {

                @Override
                public void requestComplete(AsyncRequest<SlaveRegistration, NullMessage> request) {
                  if (request.getStatus() == Status.Success) { 
                    //LOG.info("Extended Registration");
                    _registration.setLastTimestamp(System.currentTimeMillis());
                    _handshakeState = HandshakeState.IDLE;
                  }
                  else { 
                    LOG.error("Handshake Extension Failed!");
                    shutdownServices();
                  }
                } 
                
              });
            } catch (RPCException e) {
              LOG.error(CCStringUtils.stringifyException(e));
              shutdownServices();
            }
          }
        }
      }
    }
  }

  
}
