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

package org.commoncrawl.mapred.ec2.crawlStats;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.mapred.ec2.postprocess.linkCollector.LinkKey.LinkKeyComparator;
import org.commoncrawl.mapred.ec2.postprocess.linkCollector.LinkKey.LinkKeyPartitioner;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.TextBytes;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * 
 * @author rana
 *
 */
public class EC2StatsCollectorJob {

  static final Log LOG = LogFactory.getLog(EC2StatsCollectorJob.class);
  
  static final Path internalEC2SegmentPath = new Path("crawl/ec2Import/segment");
  static final Path internalMergedSegmentPath = new Path("crawl/ec2Import/mergedSegment"); 
  
  static final String S3N_BUCKET_PREFIX = "s3n://aws-publicdatasets";
  static final String SEGMENTS_PATH = "/common-crawl/parse-output/segment/";
  static final String STATS_INTERMEDIATE_OUTPUT_PATH = "/common-crawl/stats-output/intermediate/";
  static final String VALID_SEGMENTS_PATH = "/common-crawl/parse-output/valid_segments/";
  static final String JOB_SUCCESS_FILE = "_SUCCESS";
  
  static final int MAX_SIMULTANEOUS_JOBS = 1;
  
  public static final String ARC_TO_BAD_URL_NAMED_OUTPUT = "arcRecords";
  public static final String CRAWL_RECORD_NAMED_OUTPUT = "crawlRecords";
  
  LinkedBlockingQueue<QueueItem> _queue = new LinkedBlockingQueue<QueueItem>();
  Semaphore jobThreadSemaphore = new Semaphore(-(MAX_SIMULTANEOUS_JOBS-1));


  
  public static void processSegmentEC2(FileSystem s3fs,Configuration conf,long segmentId)throws IOException { 
    Path outputPath = new Path(S3N_BUCKET_PREFIX + STATS_INTERMEDIATE_OUTPUT_PATH+Long.toString(segmentId));
    LOG.info("Starting Intermedaite Merge of Segment:" + segmentId + " Output path is:" + outputPath);
    
    if (s3fs.exists(outputPath)) { 
      LOG.warn("Output Path Already Exists for Segment:" + segmentId +".Deleting!");
      s3fs.delete(outputPath,true);
    }
    
    // ok collect merge files
    ArrayList<Path> pathList = new ArrayList<Path>();
    for (FileStatus metadataFile : s3fs.globStatus(new Path(SEGMENTS_PATH,segmentId + "/metadata-*"))) { 
      pathList.add(metadataFile.getPath().makeQualified(s3fs));
    }
    LOG.info("Input Paths for Segment:" + segmentId + " are:" + pathList);
    
    JobConf jobConf = new JobBuilder("Intermediate merge for:" + segmentId, conf)
      .inputs(pathList)
      .inputFormat(SequenceFileInputFormat.class)
      .keyValue(Text.class, Text.class)
      .mapper(CrawlStatsMapper.class)
      .reducer(CrawlStatsReducer.class, false)
      .maxMapAttempts(7)
      .maxReduceAttempts(7)
      .maxMapTaskFailures(1000)
      .numReducers(500)
      .speculativeExecution(true)
      .output(outputPath)
      .outputFormat(SequenceFileOutputFormat.class)
      .compressMapOutput(true)
      .compressor(CompressionType.BLOCK, SnappyCodec.class)
      .build();
    
    // slow start the reducers  
    jobConf.setFloat("mapred.reduce.slowstart.completed.maps",1.0f);
    // set up multi output streams 
    MultipleOutputs.addNamedOutput(jobConf, ARC_TO_BAD_URL_NAMED_OUTPUT, SequenceFileOutputFormat.class, Text.class, Text.class);
    MultipleOutputs.addNamedOutput(jobConf, CRAWL_RECORD_NAMED_OUTPUT, SequenceFileOutputFormat.class, Text.class, Text.class);
        
    JobClient.runJob(jobConf);
  }
  
  
  
  private static SortedSet<Long> scanForValidSegments(FileSystem fs) throws IOException { 
    SortedSet<Long> completeSegmentIds = Sets.newTreeSet(); 
    
    for (FileStatus fileStatus : fs.globStatus(new Path(VALID_SEGMENTS_PATH+"[0-9]*"))) { 
      completeSegmentIds.add(Long.parseLong(fileStatus.getPath().getName()));
    }
    return completeSegmentIds;
  }

  private static SortedSet<Long> scanForMergedSegments(FileSystem fs) throws IOException { 
    SortedSet<Long> completeSegmentIds = Sets.newTreeSet(); 

    for (FileStatus fileStatus : fs.globStatus(new Path(STATS_INTERMEDIATE_OUTPUT_PATH+"[0-9]*"))) { 
      // ok look for the SUCCESS file
      Path successPath = new Path(fileStatus.getPath(),JOB_SUCCESS_FILE);
      if (fs.exists(successPath)) { 
        completeSegmentIds.add(Long.parseLong(fileStatus.getPath().getName()));
      }
    }
    return completeSegmentIds;
  }

  
  public EC2StatsCollectorJob(Configuration conf)throws Exception {
    FileSystem fs = FileSystem.get(new URI("s3n://aws-publicdatasets"),conf);
    LOG.info("FileSystem is:" + fs.getUri() +" Scanning for valid segments");
    SortedSet<Long> validSegments = scanForValidSegments(fs);
    LOG.info("There are: " + validSegments.size() + " valid segments. Scanning for Merged Segments");
    SortedSet<Long> mergedSegments = scanForMergedSegments(fs);
    LOG.info("There are: " + mergedSegments.size() + " merged Segments");
    // calculate difference 
    Set<Long> segmentsToProcess = Sets.difference(validSegments, mergedSegments);
    LOG.info("There are: " + segmentsToProcess.size() + " Segments that need to be merged");
    // ok we are ready to go .. 
    for (long segmentId : segmentsToProcess) {  
      LOG.info("Queueing Segment:" + segmentId +" for Merge");
      queue(fs,conf,segmentId);
    }
    // queue shutdown items 
    for (int i=0;i<MAX_SIMULTANEOUS_JOBS;++i) { 
      _queue.put(new QueueItem());
    }
  }
  
  void run() { 
    LOG.info("Starting Threads");
    // startup threads .. 
    for (int i=0;i<MAX_SIMULTANEOUS_JOBS;++i) { 
      Thread thread = new Thread(new QueueTask());
      thread.start();
    }
    
    
    // ok wait for them to die
    LOG.info("Waiting for Queue Threads to Die");
    jobThreadSemaphore.acquireUninterruptibly();
    LOG.info("Queue Threads Dead. Exiting");
  }  
  
  public static void main(String[] args)throws Exception {
    
    if (args.length != 0 && args[0].equalsIgnoreCase("--runOnEC2")) { 
      Configuration conf = new Configuration();
      conf.addResource(new Path("/home/hadoop/conf/core-site.xml"));
      conf.addResource(new Path("/home/hadoop/conf/mapred-site.xml"));
    
      EC2StatsCollectorJob task = new EC2StatsCollectorJob(conf);
      task.run();
    }
  }
  
  
  static class QueueItem {
    QueueItem() { 
      segmentId = -1L;
    }
    
    QueueItem(FileSystem fs,Configuration conf,long segmentId) { 
      this.conf = conf;
      this.fs = fs;
      this.segmentId = segmentId;
    }
    
    public Configuration conf;
    public FileSystem fs;
    public long segmentId;
  }
  
  class QueueTask implements Runnable {


    @Override
    public void run() {
      while (true) {
        LOG.info("Queue Thread:" + Thread.currentThread().getId() + " Running");
        try {
          QueueItem item = _queue.take();
          
          
          if (item.segmentId != -1L) { 
            LOG.info("Queue Thread:" + Thread.currentThread().getId() + " got segment:" + item.segmentId);
            LOG.info("Queue Thread:" + Thread.currentThread().getId() + " Starting Job");
            try {
              processSegmentEC2(item.fs,item.conf,item.segmentId);
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
  
  private void queue(FileSystem fs,Configuration conf,long segmentId) { 
    try {
      _queue.put(new QueueItem(fs,conf,segmentId));
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }   
}
