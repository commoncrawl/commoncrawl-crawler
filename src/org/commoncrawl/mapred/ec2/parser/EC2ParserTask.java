/**
 * Copyright 2012 - CommonCrawl Foundation
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

package org.commoncrawl.mapred.ec2.parser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.protocol.CrawlDBService;
import org.commoncrawl.protocol.LongQueryParam;
import org.commoncrawl.protocol.MapReduceTaskIdAndData;
import org.commoncrawl.protocol.ParseOutput;
import org.commoncrawl.protocol.SimpleByteResult;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.rpc.base.internal.AsyncContext;
import org.commoncrawl.rpc.base.internal.AsyncRequest.Status;
import org.commoncrawl.rpc.base.internal.AsyncServerChannel;
import org.commoncrawl.rpc.base.internal.NullMessage;
import org.commoncrawl.rpc.base.internal.RPCTestService;
import org.commoncrawl.rpc.base.internal.Server;
import org.commoncrawl.rpc.base.shared.RPCException;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.TaskDataUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

/**
 * EC2 ParserTask 
 * 
 * Spawns the EMR Job that processes CrawlLogs.
 * 
 * First in a sequence of jobs that are part of the migration of data processing
 * from the internal cluster to EC2. This job is designed to run on EMR. It
 * utilizes spot instances to help reduce costs, and thus only currently uses
 * Mappers with (0) Reducers to help make the job more resilient to machine 
 * failures as well as dynamic (spot) task tracker availablity.
 * 
 * 
 * @author rana
 *
 */
@SuppressWarnings("static-access")
public class EC2ParserTask extends Server implements CrawlDBService{
  
  public static final Log LOG = LogFactory.getLog(EC2ParserTask.class);
  
  static final String S3N_BUCKET_PREFIX = "s3n://aws-publicdatasets";
  static final String CRAWL_LOG_INTERMEDIATE_PATH = "/common-crawl/crawl-intermediate/";
  
  static final String VALID_SEGMENTS_PATH = "/common-crawl/parse-output/valid_segments2/";
  static final String TEST_VALID_SEGMENTS_PATH = "/common-crawl/parse-output-test/valid_segments/";
  static final String VALID_SEGMENTS_PATH_PROPERTY = "cc.valid.segments.path";
  
  static final String SEGMENTS_PATH = "/common-crawl/parse-output/segment/";
  static final String TEST_SEGMENTS_PATH = "/common-crawl/parse-output-test/segment/";
  static final String SEGMENT_PATH_PROPERTY = "cc.segment.path";
  
  static final String JOB_LOGS_PATH = "/common-crawl/job-logs/";
  static final String TEST_JOB_LOGS_PATH = "/common-crawl/test-job-logs/";
  static final String JOB_LOGS_PATH_PROPERTY = "cc.job.log.path";

  
  static final String SEGMENT_MANIFEST_FILE = "manfiest.txt";
  static final String SPLITS_MANIFEST_FILE = "splits.txt";
  static final String TRAILING_SPLITS_MANIFEST_FILE = "trailing_splits.txt";
  static final String FAILED_SPLITS_MANIFEST_FILE = "failed_splits.txt";
  static final int    LOGS_PER_ITERATION = 1000;
  static final Pattern CRAWL_LOG_REG_EXP = Pattern.compile("CrawlLog_ccc[0-9]{2}-[0-9]{2}_([0-9]*)");
  static final int MAX_SIMULTANEOUS_JOBS = 100;
  
  static final int    TASK_DATA_PORT = 9200;
  
  public static final String CONF_PARAM_TEST_MODE = "EC2ParserTask.TestMode";
  
  LinkedBlockingQueue<QueueItem> _queue = new LinkedBlockingQueue<QueueItem>();
  Semaphore jobThreadSemaphore = null;
  int maxSimultaneousJobs = MAX_SIMULTANEOUS_JOBS;

  
  static Options options = new Options();
  static { 
    
    options.addOption(
        OptionBuilder.withArgName("testMode").hasArg(false).withDescription("Test Mode").create("testMode"));
    
  }
  
  private EventLoop _eventLoop = new EventLoop();
  private static InetAddress _serverAddress = null;
  
  public EC2ParserTask(Configuration conf)throws Exception {
  
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
    
    if (!conf.getBoolean(CONF_PARAM_TEST_MODE, false)) { 
     conf.set(VALID_SEGMENTS_PATH_PROPERTY,VALID_SEGMENTS_PATH);
     conf.set(SEGMENT_PATH_PROPERTY,SEGMENTS_PATH);
     conf.set(JOB_LOGS_PATH_PROPERTY, JOB_LOGS_PATH);
     jobThreadSemaphore = new Semaphore(-(MAX_SIMULTANEOUS_JOBS-1));
     
    }
    else { 
     conf.set(VALID_SEGMENTS_PATH_PROPERTY,TEST_VALID_SEGMENTS_PATH);
     conf.set(SEGMENT_PATH_PROPERTY,TEST_SEGMENTS_PATH);
     conf.set(JOB_LOGS_PATH_PROPERTY, TEST_JOB_LOGS_PATH);
     jobThreadSemaphore = new Semaphore(0);
     maxSimultaneousJobs = 1;
    }
    
    FileSystem fs = FileSystem.get(new URI("s3n://aws-publicdatasets"),conf);
    LOG.info("FileSystem is:" + fs.getUri() +" Scanning for candidates at path:" + CRAWL_LOG_INTERMEDIATE_PATH);
    TreeSet<Path> candidateSet = buildCandidateList(fs, new Path(CRAWL_LOG_INTERMEDIATE_PATH));
    LOG.info("Scanning for completed segments"); 
    List<Path> processedLogs = scanForCompletedSegments(fs,conf);
    LOG.info("Found " + processedLogs.size() + " processed logs");
    // remove processed from candidate set ... 
    candidateSet.removeAll(processedLogs);
    // ok we are ready to go .. 
    LOG.info("There are: " + candidateSet.size() + " logs in need of parsing");
    while (candidateSet.size() != 0) { 
      ImmutableList.Builder<Path> pathBuilder = new ImmutableList.Builder<Path>();
      Iterator<Path> iterator = Iterators.limit(candidateSet.iterator(),LOGS_PER_ITERATION);
      while (iterator.hasNext()) { 
        pathBuilder.add(iterator.next());
        iterator.remove();
      }
      LOG.info("Queueing Parse");
      queue(fs,conf,pathBuilder.build());
      LOG.info("Queued Parse");
      
      // in test mode, queue only a single segment's worth of data 
      if (conf.getBoolean(CONF_PARAM_TEST_MODE, false)) {
        LOG.info("Test Mode - Queueing only a single Item");
        break;
      }
    }

    // queue shutdown items 
    for (int i=0;i<maxSimultaneousJobs;++i) { 
      _queue.put(new QueueItem());
    }
  }
  
  void run() { 
    LOG.info("Starting Threads");
    // startup threads .. 
    for (int i=0;i<maxSimultaneousJobs;++i) { 
      Thread thread = new Thread(new QueueTask());
      thread.start();
    }
    
    
    // ok wait for them to die
    LOG.info("Waiting for Queue Threads to Die");
    jobThreadSemaphore.acquireUninterruptibly();
    LOG.info("Queue Threads Dead. Exiting");
    // stop event loop 
    _eventLoop.stop();
  }
  
  static class QueueItem {
    QueueItem() { 
      pathList = null;
    }
    
    QueueItem(FileSystem fs,Configuration conf,ImmutableList<Path> pathList) { 
      this.conf = conf;
      this.fs = fs;
      this.pathList = pathList;
    }
    
    public Configuration conf;
    public FileSystem fs;
    public ImmutableList<Path> pathList;
  }
  
  private void queue(FileSystem fs,Configuration conf,ImmutableList<Path> paths) { 
    try {
      _queue.put(new QueueItem(fs,conf,paths));
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  

  
  class QueueTask implements Runnable {


    @Override
    public void run() {
      while (true) {
        LOG.info("Queue Thread:" + Thread.currentThread().getId() + " Running");
        try {
          QueueItem item = _queue.take();
          
          
          if (item.pathList != null) { 
            LOG.info("Queue Thread:" + Thread.currentThread().getId() + " got item with Paths:" + item.pathList);
            LOG.info("Queue Thread:" + Thread.currentThread().getId() + " Starting Job");
            try {
              parse(item.fs,item.conf,item.pathList);
            } catch (IOException e) {
              LOG.error("Queue Thread:" + Thread.currentThread().getId() + " threw exception:" + CCStringUtils.stringifyException(e));
            }
          }
          else { 
            LOG.info("Queue Thread:" + Thread.currentThread().getId() + " Got Shutdown Queue Item - EXITING");
            break;
          }
        } catch (InterruptedException e) {
        }
      }
      
      LOG.info("Queue Thread:" + Thread.currentThread().getId() + " Released Semaphore");
      jobThreadSemaphore.release();
    } 
  }
  
  public static void main(String[] args)throws Exception {
    Configuration conf = new Configuration();
    
    conf.addResource(new Path("/home/hadoop/conf/core-site.xml"));
    conf.addResource(new Path("/home/hadoop/conf/mapred-site.xml"));

    CommandLineParser parser = new GnuParser();

    try {
      // parse the command line arguments
      CommandLine line = parser.parse( options, args );    
      
      boolean testMode = line.hasOption("testMode"); 
      if (testMode) { 
        LOG.info("Running in Test Mode");
        conf.setBoolean(CONF_PARAM_TEST_MODE,true);
      }
      else { 
        LOG.info("Running in Prod Mode");
      }
      
      EC2ParserTask task = new EC2ParserTask(conf);
      task.run();
      System.exit(0);
    }
    catch (Exception e) { 
      LOG.error(CCStringUtils.stringifyException(e));
    }
    System.exit(1);

  }
  
  
  private static void writeTrailingSplitsFile(FileSystem fs, Configuration conf,
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
  
  
  private static void writeSplitsManifest(FileSystem fs, Configuration conf,
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
  
  private static void parse(FileSystem fs,Configuration conf,ImmutableList<Path> paths)throws IOException { 
    LOG.info("Need to Parse:" + paths.toString());
    // create output path 
    long segmentId = System.currentTimeMillis();
    
    String segmentPathPrefix = conf.get(SEGMENT_PATH_PROPERTY);
    
    Path outputPath = new Path(S3N_BUCKET_PREFIX + segmentPathPrefix + Long.toString(segmentId));
    LOG.info("Starting Map-Reduce Job. SegmentId:" + segmentId + " OutputPath:" + outputPath);
    // run job...
    JobConf jobConf = new JobBuilder("parse job",conf)
      
      .inputs(paths)
      .inputFormat(SequenceFileInputFormat.class)
      .keyValue(Text.class, ParseOutput.class)
      .mapRunner(ParserMapRunner.class)
      .mapper(ParserMapper.class)
      .maxMapAttempts(3)
      .maxMapTaskFailures(1000)
      .speculativeExecution(true)
      .numReducers(0)
      .outputFormat(ParserOutputFormat.class)
      .output(outputPath)
      .minSplitSize(134217728*4)
      .reuseJVM(1000)
      .build();
    
    Path jobLogsPath = new Path(S3N_BUCKET_PREFIX + conf.get(JOB_LOGS_PATH_PROPERTY) + Long.toString(segmentId));
    
    jobConf.set("hadoop.job.history.user.location", jobLogsPath.toString());
    jobConf.set("fs.default.name", S3N_BUCKET_PREFIX);    
    jobConf.setLong("cc.segmet.id", segmentId);
    // set task timeout to 20 minutes 
    jobConf.setInt("mapred.task.timeout", 20 * 60 * 1000);
    // set mapper runtime to max 20 minutes ...  
    jobConf.setLong(ParserMapper.MAX_MAPPER_RUNTIME_PROPERTY, 40 * 60  * 1000);
    
    jobConf.setOutputCommitter(OutputCommitter.class);
    // allow lots of failures per tracker per job 
    jobConf.setMaxTaskFailuresPerTracker(1000);
    
    // initialize task data client info 
    TaskDataUtils.initializeTaskDataJobConfig(jobConf, segmentId, new InetSocketAddress(_serverAddress, TASK_DATA_PORT));
    
    JobClient.runJob(jobConf);
    
    LOG.info("Job Finished. Writing Segments Manifest Files");
    writeSegmentManifestFile(fs,conf,segmentId,paths);
    writeSplitsManifest(fs,conf,jobConf,segmentId);
    writeTrailingSplitsFile(fs,conf,jobConf,segmentId);
  }
  
  private static List<Path> scanForCompletedSegments(FileSystem fs,Configuration conf) throws IOException { 
    ImmutableList.Builder<Path> pathListBuilder = new ImmutableList.Builder<Path>();
    
    String validSegmentPathPrefix = conf.get(VALID_SEGMENTS_PATH_PROPERTY);
    
    for (FileStatus fileStatus : fs.globStatus(new Path(validSegmentPathPrefix+"[0-9]*"))) { 
      pathListBuilder.addAll(scanSegmentManifestFile(fs,fileStatus.getPath()));
    }
    return pathListBuilder.build();
  }
  
  private static List<Path> scanSegmentManifestFile(FileSystem fs,Path segmentPath)throws IOException {
    LOG.info("Scanning Segment Manifest for segment at path:" + segmentPath);
    Path manifestPath = new Path(segmentPath,SEGMENT_MANIFEST_FILE);
    ImmutableList.Builder<Path> pathListBuilder = new ImmutableList.Builder<Path>(); 
    for (String pathStr : textFileToList(fs, manifestPath)) { 
      pathListBuilder.add(new Path(pathStr));
    }
    return pathListBuilder.build();
  }
  
  private static void writeSegmentManifestFile(FileSystem fs,Configuration conf,long segmentTimestamp,List<Path> logsInSegment) throws IOException {
    LOG.info("Writing Segment Manifest for Segment: " + segmentTimestamp + " itemCount:" + logsInSegment.size());
    ImmutableList.Builder<String> listBuilder = new ImmutableList.Builder<String>();
    
    String validSegmentPathPrefix = conf.get(VALID_SEGMENTS_PATH_PROPERTY);

    for (Path logPath : logsInSegment) { 
      listBuilder.add(logPath.toString().substring(S3N_BUCKET_PREFIX.length()));
    }
    listToTextFile(listBuilder.build(), fs, new Path(validSegmentPathPrefix+Long.toString(segmentTimestamp)+"/"+SEGMENT_MANIFEST_FILE));
  }
  
  
  
  /** build a list of parse candidates sorted by timestamp 
   * 
   * @param fs
   * @param logFilePath
   * @return a Set of Candidates
   * @throws IOException
   */
  private static TreeSet<Path> buildCandidateList(FileSystem fs,Path logFilePath)throws IOException {
    
    TreeSet<Path> candidateList = new TreeSet<Path>(new Comparator<Path>() {

      @Override
      public int compare(Path p1, Path p2) {
        String n1 = p1.getName();
        String n2 = p2.getName();
        Matcher m1 = CRAWL_LOG_REG_EXP.matcher(n1);
        Matcher m2 = CRAWL_LOG_REG_EXP.matcher(n2);
        m1.matches();
        m2.matches();
        Long   v1 = Long.parseLong(m1.group(1));
        Long   v2 = Long.parseLong(m2.group(1));

        return v1.compareTo(v2); 
      }
     
     });
    
    LOG.info("Scanning for Log Files at:" + logFilePath);
    FileStatus candidateItems[] = fs.globStatus(new Path(logFilePath,"CrawlLog*"));
    for (FileStatus candidate : candidateItems) { 
      candidateList.add(candidate.getPath());
    }
    
    return candidateList;
  }
  
  private static List<String> textFileToList(FileSystem fs,Path path)throws IOException { 
    
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
  
  private static void listToTextFile(List<String> lines,FileSystem fs,Path path)throws IOException { 
    Writer writer = new OutputStreamWriter(fs.create(path), Charset.forName("UTF-8"));
    try { 
      for (String line : lines) { 
        writer.write(line);
        writer.append("\n");
      }
      writer.flush();
    }
    finally { 
      writer.close();
    }
  }
  
  
  static void printUsage() { 
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( "EC2Launcher", options );
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
  
  /** 
   * get ip address for master 
   */
  private static InetAddress getMasterIPAddress(String intfc)throws IOException { 
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
}
