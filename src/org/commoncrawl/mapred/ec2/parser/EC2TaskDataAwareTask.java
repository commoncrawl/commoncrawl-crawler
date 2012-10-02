package org.commoncrawl.mapred.ec2.parser;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.protocol.CrawlDBService;
import org.commoncrawl.protocol.LongQueryParam;
import org.commoncrawl.protocol.MapReduceTaskIdAndData;
import org.commoncrawl.protocol.SimpleByteResult;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.rpc.base.internal.AsyncContext;
import org.commoncrawl.rpc.base.internal.AsyncServerChannel;
import org.commoncrawl.rpc.base.internal.NullMessage;
import org.commoncrawl.rpc.base.internal.Server;
import org.commoncrawl.rpc.base.internal.AsyncRequest.Status;
import org.commoncrawl.rpc.base.shared.RPCException;
import org.commoncrawl.util.TaskDataUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

public class EC2TaskDataAwareTask extends Server implements CrawlDBService, Constants {

  public static final Log LOG = LogFactory.getLog(EC2TaskDataAwareTask.class);
  
  static final int    TASK_DATA_PORT = 9200;

  
  private EventLoop _eventLoop = new EventLoop();
  private static InetAddress _serverAddress = null;
  
  public EC2TaskDataAwareTask(Configuration conf)throws IOException {
    // start async event loop thread 
    _eventLoop.start();
    
    // look for our ip address 
    // ok get our ip address ... 
    _serverAddress = getMasterIPAddress("eth0");
    
    // set the address and port for the task data server (if available) 
    if (_serverAddress == null) {
      throw new IOException("Unable to determine Master IP Address!");
    }
    else { 
      LOG.info("Task Data IP is:" + _serverAddress.getHostAddress() + " and Port is:" + TASK_DATA_PORT);
      
      // ok establish the rpc server channel ... 
      InetSocketAddress taskDataServerAddress = new InetSocketAddress(_serverAddress.getHostAddress(), TASK_DATA_PORT);
      AsyncServerChannel channel = new AsyncServerChannel(this, _eventLoop, taskDataServerAddress, null);
      // register the task data service 
      registerService(channel, CrawlDBService.spec);
      // and start processing rpc requests for it ... 
      start();      
    }
  }
  
  public void shutdown()throws IOException { 
    _eventLoop.stop();
  }
  
  /** 
   * get ip address for master 
   */
  protected static InetAddress getMasterIPAddress(String intfc)throws IOException { 
    NetworkInterface netIF = NetworkInterface.getByName(intfc);

    if (netIF != null) {
      Enumeration<InetAddress> e = netIF.getInetAddresses();

      while (e.hasMoreElements()) {

        InetAddress address = e.nextElement();
        // only allow ipv4 addresses for now ...
        if (address.getAddress().length == 4) {
          LOG.info("IP for Master on interface:"+ intfc  + " is:" + address.getHostAddress());
          return address;
        }
      }
    }
    return null;
  }
  
  protected static void initializeTaskDataAwareJob(JobConf jobConf,long segmentId)throws IOException { 
    // initialize task data client info 
    TaskDataUtils.initializeTaskDataJobConfig(jobConf, segmentId, new InetSocketAddress(_serverAddress, TASK_DATA_PORT));
  }
  
  protected static void finalizeJob(FileSystem fs,Configuration conf,JobConf jobConf,long segmentId)throws IOException { 
    writeSplitsManifest(fs,conf,jobConf,segmentId);
    writeTrailingSplitsFile(fs,conf,jobConf,segmentId);
    
    // purge maps 
    synchronized (_badTaskIdMap) {
      _badTaskIdMap.removeAll(Long.toString(segmentId));
    }
    synchronized (_goodTaskIdMap) {
      _goodTaskIdMap.removeAll(Long.toString(segmentId));
    }
    
  }
  
  
  protected static void writeTrailingSplitsFile(FileSystem fs, Configuration conf,
      JobConf jobConf, long segmentTimestamp) throws IOException {
  
    ImmutableList.Builder<String> listBuilder = new ImmutableList.Builder<String>();
    // ok bad splits map ... 
    synchronized (_badTaskIdMap) {
      for (String badTaskEntry : _badTaskIdMap.get(Long.toString(segmentTimestamp))) { 
        listBuilder.add(badTaskEntry);
      }
    }
    String validSegmentPathPrefix = conf.get(VALID_SEGMENTS_PATH_PROPERTY);

    listToTextFile(listBuilder.build(), fs, new Path(validSegmentPathPrefix+Long.toString(segmentTimestamp)+"/"+TRAILING_SPLITS_MANIFEST_FILE));
  }
  
  protected static void writeSplitsManifest(FileSystem fs, Configuration conf,
      JobConf jobConf, long segmentTimestamp) throws IOException {
    // calculate splits ...
    InputSplit[] splits = jobConf.getInputFormat().getSplits(jobConf,
        jobConf.getNumMapTasks());

    LOG.info("Writing Splits Manifest for Segment: " + segmentTimestamp
        + " splitCount:" + splits.length);
    ImmutableList.Builder<String> allListBuilder = new ImmutableList.Builder<String>();
    ImmutableList.Builder<String> failedListBuilder = new ImmutableList.Builder<String>();

    // (taken from hadoop code to replicate split order and generate proper
    // task id to split mapping)
    // sort the splits into order based on size, so that the biggest
    // go first
    Arrays.sort(splits,
        new Comparator<org.apache.hadoop.mapred.InputSplit>() {
      public int compare(org.apache.hadoop.mapred.InputSplit a,
          org.apache.hadoop.mapred.InputSplit b) {
        try {
          long left = a.getLength();
          long right = b.getLength();
          if (left == right) {
            return 0;
          } else if (left < right) {
            return 1;
          } else {
            return -1;
          }
        } catch (IOException ie) {
          throw new RuntimeException(
              "Problem getting input split size", ie);
        }
      }
    });

    String segmentIdStr = Long.toString(segmentTimestamp);
    
    int splitIndex = 0;
    for (InputSplit sortedSplit : splits) {
      allListBuilder.add(sortedSplit.toString());
      synchronized (_goodTaskIdMap) {
        // check to see of it the task data "good task" map contains the specified split ... 
        if (!_goodTaskIdMap.containsEntry(segmentIdStr, Integer.toString(splitIndex))) { 
          // if not, add it the failed list ... 
          failedListBuilder.add(Integer.toString(splitIndex)+","+sortedSplit.toString());
        }
      }
      ++splitIndex;
    }
    
    String validSegmentPathPrefix = conf.get(VALID_SEGMENTS_PATH_PROPERTY);
    
    // emit ALL splits file 
    listToTextFile(allListBuilder.build(), fs, new Path(validSegmentPathPrefix+Long.toString(segmentTimestamp)+"/"+SPLITS_MANIFEST_FILE));
    // emit FAILED splits file (subset of all)
    listToTextFile(failedListBuilder.build(), fs, new Path(validSegmentPathPrefix+Long.toString(segmentTimestamp)+"/"+FAILED_SPLITS_MANIFEST_FILE));
  }
    
  
  protected  static List<String> textFileToList(FileSystem fs,Path path)throws IOException { 
    
    ImmutableList.Builder<String> builder = new ImmutableList.Builder<String>(); 
    
    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path),Charset.forName("UTF-8")));
    try { 
      String line;
      while ((line = reader.readLine()) != null) { 
        if (line.length() != 0 && !line.startsWith("#")) 
          builder.add(line);
      }
    }
    finally { 
      reader.close();
    }
    return builder.build();
  }
  
  protected static void listToTextFile(List<? extends Object> objects,FileSystem fs,Path path)throws IOException { 
    Writer writer = new OutputStreamWriter(fs.create(path), Charset.forName("UTF-8"));
    try { 
      for (Object obj : objects) { 
        writer.write(obj.toString());
        writer.append("\n");
      }
      writer.flush();
    }
    finally { 
      writer.close();
    }
  }
    
  
  /** 
   * Task Data RPC Spec Implementation  
   */

  static Multimap<String,String> _badTaskIdMap = TreeMultimap.create(
      String.CASE_INSENSITIVE_ORDER,new Comparator<String>() {

        @Override
        public int compare(String o1, String o2) {
          String tid1 = o1.substring(0,o1.indexOf(","));
          String tid2 = o2.substring(0,o2.indexOf(","));
          return tid1.compareToIgnoreCase(tid2);
        }
      });

  static Multimap<String,String> _goodTaskIdMap = TreeMultimap.create(
      String.CASE_INSENSITIVE_ORDER,String.CASE_INSENSITIVE_ORDER);
  
  
  @Override
  public void updateMapReduceTaskValue(
      AsyncContext<MapReduceTaskIdAndData, NullMessage> rpcContext)
      throws RPCException {

    rpcContext.setStatus(Status.Error_RequestFailed);
    // one big hack .. if we get the "bad" task data key, add the job/task to the bad task id map 
    if (rpcContext.getInput().getDataKey().equalsIgnoreCase(ParserMapper.BAD_TASK_TASKDATA_KEY)) {
      synchronized (_badTaskIdMap) {
        _badTaskIdMap.put(rpcContext.getInput().getJobId(),rpcContext.getInput().getTaskId()+","+rpcContext.getInput().getDataValue());
      }
      rpcContext.setStatus(Status.Success);
    }
    else if (rpcContext.getInput().getDataKey().equalsIgnoreCase(ParserMapper.GOOD_TASK_TASKDATA_KEY)) { 
      synchronized (_goodTaskIdMap) { 
        _goodTaskIdMap.put(rpcContext.getInput().getJobId(),rpcContext.getInput().getTaskId());
      }
      rpcContext.setStatus(Status.Success);
    }
    rpcContext.completeRequest();
  }

  @Override
  public void queryMapReduceTaskValue(
      AsyncContext<MapReduceTaskIdAndData, MapReduceTaskIdAndData> rpcContext)
      throws RPCException {
    
    rpcContext.setStatus(Status.Error_RequestFailed);
    
    // similarly if we ask for the "bad" task data key, check to see if the job/task 
    // is in the map, and if so, return 1 
    if (rpcContext.getInput().getDataKey().equalsIgnoreCase(ParserMapper.BAD_TASK_TASKDATA_KEY)) { 
      
      try {
        rpcContext.getOutput().merge(rpcContext.getInput());
      } catch (CloneNotSupportedException e) {
      }
      
      synchronized (_badTaskIdMap) {
        if (_badTaskIdMap.containsEntry(
            rpcContext.getInput().getJobId(),
            rpcContext.getInput().getTaskId()+",")) { 
          
          // hack ... caller doesn't need split info ... 
          rpcContext.getOutput().setDataValue("1");
        }
        rpcContext.setStatus(Status.Success);
      }
    }
    rpcContext.completeRequest();
  }

  @Override
  public void purgeMapReduceTaskValue(
      AsyncContext<MapReduceTaskIdAndData, NullMessage> rpcContext)
      throws RPCException {
    //NOOP - NOT SUPPORTED
    rpcContext.setStatus(Status.Error_RequestFailed);
    rpcContext.completeRequest();
  }

  @Override
  public void queryDuplicateStatus(
      AsyncContext<URLFPV2, SimpleByteResult> rpcContext) throws RPCException {
    //NOOP - NOT SUPPORTED
    rpcContext.setStatus(Status.Error_RequestFailed);
    rpcContext.completeRequest();
  }

  @Override
  public void queryLongValue(
      AsyncContext<LongQueryParam, LongQueryParam> rpcContext)
      throws RPCException {
    //NOOP - NOT SUPPORTED
    rpcContext.setStatus(Status.Error_RequestFailed);
    rpcContext.completeRequest();
  }

  @Override
  public void queryFingerprintStatus(
      AsyncContext<URLFPV2, SimpleByteResult> rpcContext) throws RPCException {
    //NOOP - NOT SUPPORTED
    rpcContext.setStatus(Status.Error_RequestFailed);
    rpcContext.completeRequest();
  } 
    
}
