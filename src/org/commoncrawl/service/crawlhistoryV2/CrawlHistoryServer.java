package org.commoncrawl.service.crawlhistoryV2;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.record.Buffer;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.db.RecordStore;
import org.commoncrawl.protocol.BulkItemHistoryQuery;
import org.commoncrawl.protocol.BulkItemHistoryQueryResponse;
import org.commoncrawl.protocol.BulkUpdateData;
import org.commoncrawl.protocol.CrawlerHistoryService;
import org.commoncrawl.protocol.CrawlerHistoryServiceV2;
import org.commoncrawl.protocol.SingleItemHistoryQueryResponse;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.rpc.base.internal.AsyncClientChannel;
import org.commoncrawl.rpc.base.internal.AsyncContext;
import org.commoncrawl.rpc.base.internal.AsyncServerChannel;
import org.commoncrawl.rpc.base.internal.NullMessage;
import org.commoncrawl.rpc.base.internal.AsyncRequest.Status;
import org.commoncrawl.rpc.base.shared.RPCException;
import org.commoncrawl.server.CommonCrawlServer;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.ImmutableBuffer;
import org.commoncrawl.util.URLFPBloomFilter;
import org.commoncrawl.util.URLFPUtils;
import org.commoncrawl.util.BitUtils.BitStream;

public class CrawlHistoryServer extends CommonCrawlServer
 implements CrawlerHistoryServiceV2, AsyncServerChannel.ConnectionCallback{

  // ok we are rushed for time, so hard code a bunch of stuff for now 
  static Path hdfsBasePath = new Path("crawl/historyV2");
  static Path checkpointBasePath = new Path(hdfsBasePath,"checkpoints");
  static Path checkpointStagingPath = new Path(hdfsBasePath,"staging");
  static Path tlogPath = new Path(hdfsBasePath,"tlog");
  static final String CHECKPOINT_PREFIX = "CHECKPOINT-";
  static final String TLOG_PREFIX = "TLOG-";
  
  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  
  private static final int NUM_HASH_FUNCTIONS = 10;
  private static final int NUM_BITS = 11;
  private static final int NUM_ELEMENTS = 55 * 1 << 20; // 55 million elements per filter 
  
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }    

    
  // up to 
  private URLFPBloomFilter _bloomFilters[] = new URLFPBloomFilter[CrawlEnvironment.NUM_DB_SHARDS];
  
  /** primary crawler database **/
  RecordStore   _recordStore = new RecordStore();
  /** server state record key **/
  String HistoryServerStateKey = "HistoryServerState";


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
      // load state (if any)
      loadState();
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
  
  private void loadCheckpointState() throws IOException { 
    // ok find latest checkpoint dir
    //   if found, load bloom filters from checkpoint dir 
    //   else initialize empty bloom filters
    // for each log buffer 
    //   if log buffer timestamp > last checkpoint timestamp 
    //     mutate bloom filter based on log buffers 
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
  
  /** load state **/
  private void loadState() throws IOException { 
    
  }
    
  
  @Override
  protected boolean parseArguments(String[] argv) {
    return true;
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
  public void bulkItemQuery(AsyncContext<BulkItemHistoryQuery, BulkItemHistoryQueryResponse> rpcContext)throws RPCException {
    LOG.info("Received BulkItemQueryRequest");
    ImmutableBuffer inputBuffer = rpcContext.getInput().getFingerprintList();
    
    if (inputBuffer.getCount() != 0) {  
      try { 
        
        if (_bloomFilters == null) { 
          throw new IOException("BloomFilter Not Initialized. Invalid Server State!");
        }
        
        DataInputStream inputStream = new DataInputStream(
                                        new ByteArrayInputStream(inputBuffer.getReadOnlyBytes(),0,inputBuffer.getCount()));
  
        BitStream bitStreamOut = new BitStream();
        
        URLFPV2 fingerprint = new URLFPV2();
        
        int itemsPresent = 0;
        while (inputStream.available() != 0) { 
          fingerprint.setDomainHash(WritableUtils.readVLong(inputStream));
          fingerprint.setUrlHash(WritableUtils.readVLong(inputStream));
          
          int partition = URLFPUtils.getPartitionGivenFP(fingerprint);
          
          if (_bloomFilters[partition] == null) { 
            throw new IOException("BloomFilter not Loaded");
          }
          if (_bloomFilters[partition].isPresent(fingerprint)) { 
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
      if (_bloomFilters == null) { 
        throw new IOException("BloomFilter Not Initialized. Invalid Server State!");
      }
      int partition = URLFPUtils.getPartitionGivenFP(rpcContext.getInput());
      if (_bloomFilters[partition] == null) { 
        throw new IOException("BloomFilter for Part:" + partition + " Not Loaded!");
      }
      rpcContext.getOutput().setWasCrawled(_bloomFilters[partition].isPresent(rpcContext.getInput()));
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
      if (_bloomFilters == null) { 
        throw new IOException("BloomFilter Not Initialized. Invalid Server State!");
      }
      int partition = URLFPUtils.getPartitionGivenFP(rpcContext.getInput());
      if (_bloomFilters[partition] == null) { 
        throw new IOException("BloomFilter for Part:" + partition + " Not Loaded!");
      }
      _bloomFilters[partition].add(rpcContext.getInput());
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
    
  private void loadBloomFilter() throws IOException { 
    FileSystem fs = CrawlEnvironment.getDefaultFileSystem();
    Path dataFilePath = getDataFileFinalPath();
    
    FileStatus fileStatus = fs.getFileStatus(dataFilePath);
    if (fileStatus == null || !fileStatus.isDir()) {
      IOException e = new IOException("Data File Path:" + dataFilePath + " is not a valid directory!");
      LOG.error(e.getMessage());
      throw e;
    }
    
    // check how many files are present ... 
    FileStatus files[] = fs.globStatus(new Path(dataFilePath,"part-*"));
    
    // ok files exist... read them in 
    if (files.length != 0) { 
      if (files.length != CrawlEnvironment.NUM_DB_SHARDS) { 
        IOException e = new IOException("Invalid Number of Shards found at:" + dataFilePath);
        LOG.error(e.getMessage());
        throw e;
      }
      
      for (FileStatus file : files) { 
        // get part file index ...
        int part = Integer.parseInt(file.getPath().getName().substring("part-".length()));
        if (part < CrawlEnvironment.NUM_DB_SHARDS) { 
          FSDataInputStream inputStream = fs.open(file.getPath());
          try {
            LOG.info("Loading Part:" + file.getPath().getName());
            _bloomFilters[part] = URLFPBloomFilter.load(inputStream);
          }
          finally { 
            inputStream.close();
          }
        }
        else { 
          IOException e = new IOException("Invalid part file found during load:" + file.getPath());
        }
      }
    }
    // ok no, they don't. initialize empty bloom filters
    else { 
      LOG.info("No Existing Filters Found. Allocating New Filters");
      for (int i=0;i<CrawlEnvironment.NUM_DB_SHARDS;++i){
        LOG.info("Allocating Part:" + i);
        _bloomFilters[i] = new URLFPBloomFilter(NUM_ELEMENTS, NUM_HASH_FUNCTIONS, NUM_BITS);
      }
    }
  }
  
  private Thread _checkpointThread = null;
  private Semaphore _checkpointThreadSemaphore = new Semaphore(1);
  private boolean _shutdownFlag = false;
  /** checkpoint flush interval **/
  private static final int CHECKPOINT_FLUSH_INTERVAL = 15 * 60 * 1000; // 15 minutes 
  
  private void startCheckpointThread(final FileSystem fs) { 
    
    _checkpointThread = new Thread(new Runnable() {

      @Override
      public void run() {
        
        
        // ok check point thread will run indefinitely until shutdown event is triggered 
        while (!_shutdownFlag) {
          // get a cas lock on the server 
          // rotate checkpoint log
          // release lock 
          // start checkpoint
            // generate timestamp
            // create hdfs checkpoint temp 
            // write each partition to disk 
            // promote checkpoint to checkpoint dir 
            // delete old checkpoint
            // delete log buffers with timestamp < checkpoint timestamp 
              
        }
      } 
      
    });
    _checkpointThread.start();
  }

  @Override
  public void IncomingClientConnected(AsyncClientChannel channel) {
    
  }

  @Override
  public void IncomingClientDisconnected(AsyncClientChannel channel) {
    
  }


  @Override
  public void bulkUpdateHistory(AsyncContext<BulkUpdateData, NullMessage> rpcContext) throws RPCException {

    LOG.info("Received BulkUpdate Request");
    ImmutableBuffer inputBuffer = rpcContext.getInput().getFingerprintList();
    
    if (inputBuffer.getCount() != 0) {  
      try { 
        
        if (_bloomFilters == null) { 
          throw new IOException("BloomFilter Not Initialized. Invalid Server State!");
        }
        
        DataInputStream inputStream = new DataInputStream(
                                        new ByteArrayInputStream(inputBuffer.getReadOnlyBytes(),0,inputBuffer.getCount()));
  
        URLFPV2 fingerprint = new URLFPV2();
        
        int itemsAdded = 0;
        while (inputStream.available() != 0) { 
          fingerprint.setDomainHash(WritableUtils.readVLong(inputStream));
          fingerprint.setUrlHash(WritableUtils.readVLong(inputStream));
          // get partition ... 
          int partition = URLFPUtils.getPartitionGivenFP(fingerprint);
          if (_bloomFilters[partition] == null) { 
            throw new IOException("BloomFilter for part:" + partition + " not initialized!");
          }
          // write to filter ... 
          _bloomFilters[partition].add(fingerprint); 
          ++itemsAdded;
        }
        
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


  public static final long getLatestCheckpointId(FileSystem remoteFS)throws IOException {
    FileStatus candidates[] = remoteFS.globStatus(new Path(checkpointBasePath,CHECKPOINT_PREFIX+"*"));
    
    long bestCandidateId = -1;
    
    for (FileStatus candidate : candidates) { 
      String candidateName = candidate.getPath().getName();
      try { 
        long candidateId = Long.parseLong(candidateName.substring(CHECKPOINT_PREFIX.length()));
        
        if (bestCandidateId == -1 || candidateId > bestCandidateId) { 
          if (candidate.isDir()) { 
            bestCandidateId = candidateId;
          }
          else { 
            LOG.error("Skipping Candidate:" + candidateName);
          }
        }
      }
      catch (NumberFormatException e) { 
        LOG.error(CCStringUtils.stringifyException(e));
      }
    }
    return bestCandidateId;
  }

}
