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

import java.io.IOException;
import java.net.URI;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
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
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.commoncrawl.protocol.ParseOutput;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.JobBuilder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

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
public class EC2ParserTask extends EC2TaskDataAwareTask {
  
  public static final Log LOG = LogFactory.getLog(EC2ParserTask.class);
  

  static final int    LOGS_PER_ITERATION = 1000;
  static final Pattern CRAWL_LOG_REG_EXP = Pattern.compile("CrawlLog_ccc[0-9]{2}-[0-9]{2}_([0-9]*)");
  static final int MAX_SIMULTANEOUS_JOBS = 100;
  
  
  
  
  LinkedBlockingQueue<QueueItem> _queue = new LinkedBlockingQueue<QueueItem>();
  Semaphore jobThreadSemaphore = null;
  int maxSimultaneousJobs = MAX_SIMULTANEOUS_JOBS;

  
  static Options options = new Options();
  static { 
    
    options.addOption(
        OptionBuilder.withArgName("testMode").hasArg(false).withDescription("Test Mode").create("testMode"));

    options.addOption(
        OptionBuilder.withArgName("checkpoint").hasArg(false).withDescription("Create Checkpoint").create("checkpoint"));
    
  }
  
  
  public EC2ParserTask(Configuration conf)throws Exception {
  
    super(conf);
    
    if (!conf.getBoolean(CONF_PARAM_TEST_MODE, false)) { 
     conf.set(VALID_SEGMENTS_PATH_PROPERTY,VALID_SEGMENTS_PATH);
     conf.set(SEGMENT_PATH_PROPERTY,SEGMENTS_PATH);
     conf.set(JOB_LOGS_PATH_PROPERTY, JOB_LOGS_PATH);
     conf.set(CHECKPOIINTS_PATH_PROPERTY,CHECKPOINTS_PATH);
     
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
      task.shutdown();
      System.exit(0);
    }
    catch (Exception e) { 
      LOG.error(CCStringUtils.stringifyException(e));
    }
    System.exit(1);

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
      // allow two attempts to process the split 
      // after that, we will pick it up in post processing step 
      .maxMapAttempts(2)
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
    // set mapper runtime to max 45 minutes .....  
    jobConf.setLong(ParserMapper.MAX_MAPPER_RUNTIME_PROPERTY, 45 * 60  * 1000);
    
    jobConf.setOutputCommitter(OutputCommitter.class);
    // allow lots of failures per tracker per job 
    jobConf.setMaxTaskFailuresPerTracker(Integer.MAX_VALUE);
    
    initializeTaskDataAwareJob(jobConf,segmentId);
    
    JobClient.runJob(jobConf);
    
    LOG.info("Job Finished. Writing Segments Manifest Files");
    writeSegmentManifestFile(fs,conf,segmentId,paths);
    
    finalizeJob(fs,conf,jobConf,segmentId);
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
  

  
  static void printUsage() { 
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( "EC2Launcher", options );
  }


}
