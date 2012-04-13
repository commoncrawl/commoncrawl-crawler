package org.commoncrawl.crawl.database.crawlpipeline.ec2.postprocess.linkCollector;

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
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.crawl.database.crawlpipeline.JobConfig;
import org.commoncrawl.crawl.database.crawlpipeline.ec2.postprocess.linkCollector.LinkKey.LinkKeyComparator;
import org.commoncrawl.crawl.database.crawlpipeline.ec2.postprocess.linkCollector.LinkKey.LinkKeyPartitioner;
import org.commoncrawl.util.shared.CCStringUtils;
import org.commoncrawl.util.shared.TextBytes;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class LinkCollectorJob {

  static final Log LOG = LogFactory.getLog(LinkCollectorJob.class);
  
  static final Path internalEC2SegmentPath = new Path("crawl/ec2Import/segment");
  static final Path internalMergedSegmentPath = new Path("crawl/ec2Import/mergedSegment"); 
  
  static final String S3N_BUCKET_PREFIX = "s3n://aws-publicdatasets";
  static final String SEGMENTS_PATH = "/common-crawl/parse-output/segment/";
  static final String MERGE_INTERMEDIATE_OUTPUT_PATH = "/common-crawl/merge-output/intermediate/";
  static final String VALID_SEGMENTS_PATH = "/common-crawl/parse-output/valid_segments/";
  static final String JOB_SUCCESS_FILE = "_SUCCESS";
  
  static final int MAX_SIMULTANEOUS_JOBS = 3;
  
  LinkedBlockingQueue<QueueItem> _queue = new LinkedBlockingQueue<QueueItem>();
  Semaphore jobThreadSemaphore = new Semaphore(-(MAX_SIMULTANEOUS_JOBS-1));


  
  public static void mergeSegmentEC2(FileSystem s3fs,Configuration conf,long segmentId)throws IOException { 
    Path outputPath = new Path(S3N_BUCKET_PREFIX + MERGE_INTERMEDIATE_OUTPUT_PATH+Long.toString(segmentId));
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
    
    JobConf jobConf = new JobConfig("Intermediate merge for:" + segmentId, conf)
      .inputs(pathList)
      .inputFormat(SequenceFileInputFormat.class)
      .keyValue(TextBytes.class, TextBytes.class)
      .mapper(LinkDataResharder.class)
      .maxMapAttempts(7)
      .maxReduceAttempts(7)
      .maxMapTaskFailures(100)
      .reducer(LinkDataResharder.class, true)
      .partition(LinkKeyPartitioner.class)
      .sort(LinkKeyComparator.class)
      .numReducers(5000)
      .speculativeExecution(true)

      .output(outputPath)
      .outputFormat(SequenceFileOutputFormat.class)

      .compressMapOutput(true)
      .compressor(CompressionType.BLOCK, SnappyCodec.class)
      
      .delayReducersUntil(1.0f)
      
      .build();
        
    JobClient.runJob(jobConf);
  }
  
  
  public static void mergeSegmentInternal(FileSystem fs,Configuration conf,long segmentId,Path affinityPath)throws IOException { 
    Path outputPath = JobConfig.tempDir(conf, Long.toString(segmentId));
    LOG.info("Starting Intermedaite Merge of Segment:" + segmentId + " Temp Output path is:" + outputPath);
    fs.delete(outputPath,true);    
    
    Path inputPath = new Path(internalEC2SegmentPath,Long.toString(segmentId));
    LOG.info("Input Path for Segment:" + segmentId + " is:" + inputPath);
    
    JobConf jobConf = new JobConfig("Intermediate merge for:" + segmentId, conf)
      .input(inputPath)
      .inputFormat(SequenceFileInputFormat.class)
      .keyValue(TextBytes.class, TextBytes.class)
      .mapper(LinkDataResharder.class)
      .maxMapTaskFailures(100)
      .reducer(LinkDataResharder.class, true)
      .partition(LinkKeyPartitioner.class)
      .sort(LinkKeyComparator.class)
      .numReducers(CrawlEnvironment.NUM_DB_SHARDS)
      .setAffinity(affinityPath,ImmutableSet.of("ccd001.commoncrawl.org"))
      .speculativeMapExecution()

      .output(outputPath)
      .outputFormat(SequenceFileOutputFormat.class)

      .compressMapOutput(true)
      .compressor(CompressionType.BLOCK, SnappyCodec.class)
            
      .build();
        
    JobClient.runJob(jobConf);
    
    Path finalOutputPath = new Path(internalMergedSegmentPath,Long.toString(segmentId));
    LOG.info("Renaming tempoutput:" + outputPath + " to:" + finalOutputPath);
    fs.rename(outputPath, finalOutputPath); 
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

    for (FileStatus fileStatus : fs.globStatus(new Path(MERGE_INTERMEDIATE_OUTPUT_PATH+"[0-9]*"))) { 
      // ok look for the SUCCESS file
      Path successPath = new Path(fileStatus.getPath(),JOB_SUCCESS_FILE);
      if (fs.exists(successPath)) { 
        completeSegmentIds.add(Long.parseLong(fileStatus.getPath().getName()));
      }
    }
    return completeSegmentIds;
  }

  
  public LinkCollectorJob(Configuration conf)throws Exception {
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
    int iteration = 0;
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
    
      LinkCollectorJob task = new LinkCollectorJob(conf);
      task.run();
    }
    else { 
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(conf);
      
      FileStatus segments[] = fs.globStatus(new Path(internalEC2SegmentPath,"[0-9]*"));
      
      // first find an affinity segment 
      Path affinityTarget = null;
      for (FileStatus segment : segments) { 
        long segmentId = Long.parseLong(segment.getPath().getName());
        Path stage1Path = new Path(internalMergedSegmentPath,Long.toString(segmentId));
        if (fs.exists(stage1Path)) {
          LOG.info("Found existing segment to build affinity against");
          affinityTarget = stage1Path;
        }
      }
      
      for (FileStatus segment : segments) { 
        long segmentId = Long.parseLong(segment.getPath().getName());
        LOG.info("Segment Id:"+ segmentId);
        Path stage1Path = new Path(internalMergedSegmentPath,Long.toString(segmentId));
        
        if (!fs.exists(stage1Path)) { 
          LOG.info("Need to run stage 1 for Segment:" + segmentId);
          try { 
            mergeSegmentInternal(fs,conf,segmentId,affinityTarget);
            if (affinityTarget == null) { 
              LOG.info("Adopting Successfully create merge output as affinity segment");
              affinityTarget = stage1Path;
            }
          }
          catch (IOException e) { 
            LOG.error("stage 1 for Segment:" + segmentId +" Failed with Exception:" + CCStringUtils.stringifyException(e));
          }
        }
      }      
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
              mergeSegmentEC2(item.fs,item.conf,item.segmentId);
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
