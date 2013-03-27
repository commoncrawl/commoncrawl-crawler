package org.commoncrawl.mapred.ec2.postprocess.crawldb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

import javax.annotation.Nullable;

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
import org.commoncrawl.mapred.ec2.postprocess.crawldb.CrawlDBKey.CrawlDBKeyPartitioner;
import org.commoncrawl.mapred.ec2.postprocess.crawldb.CrawlDBKey.LinkKeyComparator;
import org.commoncrawl.mapred.ec2.postprocess.crawldb.LinkGraphDataEmitterJob.QueueItem;
import org.commoncrawl.mapred.ec2.postprocess.crawldb.LinkGraphDataEmitterJob.QueueTask;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.TextBytes;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class CrawlDBMergeJob {
  
  static final Log LOG = LogFactory.getLog(CrawlDBMergeJob.class);

  ///////////////////////////////////////////////////////////////////////////
  // CONSTANTS  
  ///////////////////////////////////////////////////////////////////////////
  
  // EC2 PATHS 
  static final String S3N_BUCKET_PREFIX = "s3n://aws-publicdatasets";
  static final String GRAPH_DATA_OUTPUT_PATH = "/common-crawl/crawl-db/intermediate/";
  static final String INTERMEDIATE_MERGE_PATH = "/common-crawl/crawl-db/merge/intermediate";
  static final String FULL_MERGE_PATH = "/common-crawl/crawl-db/merge/full";
  static final String MULTIPART_SEGMENT_FILE = "MULTIPART.txt";
  
  // Default Max Paritions per Run 
  static final int DEFAULT_MAX_PARTITIONS_PER_RUN = 5;
  
  // max simultaneous intermediate merge jobs ... 
  static final int MAX_SIMULTANEOUS_JOBS = 1;
  
  static LinkedBlockingQueue<QueueItem> jobQueue = new LinkedBlockingQueue<QueueItem>();

  
  static class QueueItem {
    QueueItem() { 
      segmentIds = null;
    }
    
    QueueItem(FileSystem fs,Configuration conf,List<Long> segmentIds) { 
      this.conf = conf;
      this.fs = fs;
      this.segmentIds = segmentIds;
    }
    
    public Configuration conf;
    public FileSystem fs;
    public List<Long> segmentIds;
    public boolean    finalMergeJob;
  }
  
  static class QueueTask implements Runnable {

    Semaphore jobTaskSemaphore = null;
    
    public QueueTask(Semaphore jobTaskSemaphore) { 
      this.jobTaskSemaphore = jobTaskSemaphore;
    }
    
    @Override
    public void run() {
      while (true) {
        LOG.info("Queue Thread:" + Thread.currentThread().getId() + " Running");
        try {
          QueueItem item = jobQueue.take();
          
          
          if (item.segmentIds != null) { 
            LOG.info("Queue Thread:" + Thread.currentThread().getId() + " got segments:" + item.segmentIds);
            LOG.info("Queue Thread:" + Thread.currentThread().getId() + " Starting Job");
            try {
              runIntermediateMerge(item.fs,item.conf,item.segmentIds);
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
      jobTaskSemaphore.release();
    } 
  }  
  
  public static void main(String[] args) throws Exception  {

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(new URI(S3N_BUCKET_PREFIX),conf);
    
    //TODO: Read Parition Size from Command Line
    int partitionSize = DEFAULT_MAX_PARTITIONS_PER_RUN;
    
    
    // find latest intermediate merge timestamp ...
    long latestFullMergeTS = findLatestTimestamp(fs, conf,FULL_MERGE_PATH);
    LOG.info("Latest Full Merge Timestamp is:" + latestFullMergeTS);
    
    // find list of completed merge candidates ... 
    SortedSet<Long> completedCandidateList = filterAndSortRawInputs(fs, conf, INTERMEDIATE_MERGE_PATH, latestFullMergeTS);
        
    // find list of raw merge candidates ... 
    SortedSet<Long> rawCandidateList = filterAndSortRawInputs(fs, conf, GRAPH_DATA_OUTPUT_PATH, latestFullMergeTS);

    // find list of raw candiates that still need an intermediate merge ... 
    Set<Long> rawUnmergedSet = Sets.difference(rawCandidateList, completedCandidateList);
    
    // convert to list 
    List<Long> unmergedList = Lists.newArrayList(rawUnmergedSet);
    // unecessary sort ?? 
    Collections.sort(unmergedList);
    // partition into groups
    List<List<Long>> partitions = Lists.partition(unmergedList, partitionSize);
    
    if (partitions.size() != 0) { 
      // create completion semaphore ... 
      Semaphore mergeCompletionSemaphore = new Semaphore(-(partitions.size() - 1));
      // ok queue intermediate merges ...
      for (List<Long> partitionIds : partitions) { 
        jobQueue.put(new QueueItem(fs,conf,partitionIds));
      }
      // queue shutdown items 
      for (int i=0;i<MAX_SIMULTANEOUS_JOBS;++i) { 
        jobQueue.put(new QueueItem());
      }

      // start threads 
      LOG.info("Starting Threads");
      // startup threads .. 
      for (int i=0;i<MAX_SIMULTANEOUS_JOBS;++i) { 
        Thread thread = new Thread(new QueueTask(mergeCompletionSemaphore));
        thread.start();
      }
      
      // wait for completion ... 
      LOG.info("Waiting for intermediate merge completion");
      mergeCompletionSemaphore.acquireUninterruptibly();
    }
    
    // find list of final merge candidates 
    Set<Long> finalMergeCandidates = filterAndSortRawInputs(fs, conf, INTERMEDIATE_MERGE_PATH, latestFullMergeTS);
    
    if (finalMergeCandidates.size() != 0) {
      LOG.info("Starting Potential Final Merge");
      // run final merge ... (if necessary)
      runFinalMerge(fs,conf,Lists.newArrayList(finalMergeCandidates),latestFullMergeTS);
    }
  }
  
  /** 
   * run the crawldb merge on a list of graph data segments  
   * @param fs
   * @param conf
   * @param partitionIds
   * @throws IOException
   */
  static void runIntermediateMerge(FileSystem fs,Configuration conf,List<Long> partitionIds)throws IOException  { 
    long maxTimestamp = Iterators.getLast(partitionIds.iterator(), (long) -1);
    
    if (maxTimestamp == -1) { 
      throw new IOException("No Valid Partitions Found in List:" + partitionIds);
    }
    
    // construct a final output path ...   
    Path finalOutputPath = new Path(S3N_BUCKET_PREFIX + INTERMEDIATE_MERGE_PATH,Long.toString(maxTimestamp));
    
    if (fs.exists(finalOutputPath)) {
      LOG.info("Deleting Existing Output at:" + finalOutputPath);
      fs.delete(finalOutputPath, true);
    }
    
    LOG.info("Starting Intermeidate Merge for Paritions:" + partitionIds + " OutputPath is:" + finalOutputPath);

    // construct input paths ...  
    ArrayList<Path> inputPaths = new ArrayList<Path>();
    
    for (long segmentId : partitionIds) { 
      inputPaths.add(new Path(S3N_BUCKET_PREFIX + GRAPH_DATA_OUTPUT_PATH,Long.toString(segmentId)));
    }
    
    JobConf jobConf = new JobBuilder("Intermediate Merge for Segments:" + partitionIds, conf)
    .inputs(inputPaths)
    .inputFormat(SequenceFileInputFormat.class)
    .mapperKeyValue(TextBytes.class, TextBytes.class)
    .outputKeyValue(TextBytes.class, TextBytes.class)
    .outputFormat(SequenceFileOutputFormat.class)
    .reducer(CrawlDBWriter.class,true)
    .partition(CrawlDBKeyPartitioner.class)
    .sort(LinkKeyComparator.class)
    .numReducers(CrawlDBCommon.NUM_SHARDS)
    .speculativeExecution(true)
    .output(finalOutputPath)
    .compressMapOutput(true)
    .maxMapAttempts(4)
    .maxReduceAttempts(3)
    .maxMapTaskFailures(1)
    .compressor(CompressionType.BLOCK, SnappyCodec.class)
    .build();
            
    LOG.info("Starting JOB:" + jobConf);
    try { 
      JobClient.runJob(jobConf);
      LOG.info("Finished JOB:" + jobConf);
      // write multipart candidate list 
      if (partitionIds.size() != 1) { 
        LOG.info("Writing Multipart Segment List for OutputSegment:"+ maxTimestamp);
        listToTextFile(partitionIds,fs,new Path(finalOutputPath,MULTIPART_SEGMENT_FILE));
        LOG.info("Successfully Wrote Multipart Segment List for OutputSegment:"+ maxTimestamp);
      }
    }
    catch (IOException e) { 
      LOG.error("Failed to Execute JOB:" + jobConf + " Exception:\n" + CCStringUtils.stringifyException(e));
       fs.delete(finalOutputPath, true);
    }
  }
  
  /** 
   * run the final crawldb merge on a list intermediate merge candidates   
   * @param fs
   * @param conf
   * @param partitionIds
   * @throws IOException
   */
  static void runFinalMerge(FileSystem fs,Configuration conf,List<Long> partitionIds, long latestFinalMergeTS)throws IOException  { 
    long maxTimestamp = Iterators.getLast(partitionIds.iterator(), (long) -1);
    
    if (maxTimestamp == -1) { 
      throw new IOException("No Valid Partitions Found in List:" + partitionIds);
    }
    
    // construct a final output path ...   
    Path finalOutputPath = new Path(S3N_BUCKET_PREFIX + FULL_MERGE_PATH,Long.toString(maxTimestamp));
    
    LOG.info("Starting Final Merge for Paritions:" + partitionIds + " and FinalMerge TS:" + latestFinalMergeTS + " OutputPath is:" + finalOutputPath);

    // construct input paths ...  
    ArrayList<Path> inputPaths = new ArrayList<Path>();
    
    for (long segmentId : partitionIds) { 
      inputPaths.add(new Path(S3N_BUCKET_PREFIX + INTERMEDIATE_MERGE_PATH,Long.toString(segmentId)));
    }
    if (latestFinalMergeTS != -1) { 
      inputPaths.add(new Path(S3N_BUCKET_PREFIX + FULL_MERGE_PATH,Long.toString(latestFinalMergeTS)));
    }
    
    JobConf jobConf = new JobBuilder("Final Merge for Segments:" + partitionIds, conf)
    .inputs(inputPaths)
    .inputFormat(SequenceFileInputFormat.class)
    .mapperKeyValue(TextBytes.class, TextBytes.class)
    .outputKeyValue(TextBytes.class, TextBytes.class)
    .outputFormat(SequenceFileOutputFormat.class)
    .reducer(CrawlDBWriter.class,true)
    .partition(CrawlDBKeyPartitioner.class)
    .sort(LinkKeyComparator.class)
    .numReducers(CrawlDBCommon.NUM_SHARDS)
    .speculativeExecution(true)
    .output(finalOutputPath)
    .compressMapOutput(true)
    .compressor(CompressionType.BLOCK, SnappyCodec.class)
    .build();
            
    LOG.info("Starting JOB:" + jobConf);
    try { 
      JobClient.runJob(jobConf);
      LOG.info("Finished JOB:" + jobConf);
    }
    catch (IOException e) { 
      LOG.error("Failed to Execute JOB:" + jobConf + " Exception:\n" + CCStringUtils.stringifyException(e));
       fs.delete(finalOutputPath, true);
    }
  }
  
  /** 
   * scan the given prefix path and find the latest output
   * 
   * @param fs
   * @param conf
   * @return
   * @throws IOException
   */
  static long findLatestTimestamp(FileSystem fs,Configuration conf,String searchPath)throws IOException {
    long timestampOut = -1L;
    
    FileStatus files[] = fs.globStatus(new Path(S3N_BUCKET_PREFIX + searchPath,"[0-9]*"));
    
    for (FileStatus candidate : files) { 
      Path successPath = new Path(candidate.getPath(),"_SUCCESS");
      if (fs.exists(successPath)) { 
        long timestamp = Long.parseLong(candidate.getPath().getName());
        timestampOut = Math.max(timestamp, timestampOut);
      }
    }
    return timestampOut;
  }
  
  
  static List<String> textFileToList(FileSystem fs,Path path)throws IOException { 
    
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
  
  static void listToTextFile(List<? extends Object> objects,FileSystem fs,Path path)throws IOException { 
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
   * 
   */
  static List<Long> scanForMultiPartList(FileSystem fs,Configuration conf,Path rootSegmentPath)throws IOException { 
    Path multiPartFilePath = new Path(rootSegmentPath,MULTIPART_SEGMENT_FILE);
    if (fs.exists(multiPartFilePath)) { 
      return Lists.transform(textFileToList(fs,multiPartFilePath), new Function<String,Long>() {

        @Override
        @Nullable
        public Long apply(@Nullable String arg0) {
          return Long.parseLong(arg0);
        } 
      });
    }
    else { 
      return null;
    }
  }
  
  /** 
   * iterate the intermediate link graph data and extract unmerged set ... 
   * 
   * @param fs
   * @param conf
   * @param latestMergeDBTimestamp
   * @return
   * @throws IOException
   */
  static SortedSet<Long> filterAndSortRawInputs(FileSystem fs,Configuration conf,String searchPath, long latestMergeDBTimestamp )throws IOException { 
    TreeSet<Long> set = new TreeSet<Long>();

    FileStatus candidates[] = fs.globStatus(new Path(S3N_BUCKET_PREFIX + searchPath,"[0-9]*"));
    
    for (FileStatus candidate : candidates) {
      LOG.info("Found Merge Candidate:" + candidate.getPath());
      long candidateTimestamp = Long.parseLong(candidate.getPath().getName());
      if (candidateTimestamp > latestMergeDBTimestamp) { 
        Path successPath = new Path(candidate.getPath(),"_SUCCESS");
        if (fs.exists(successPath)) {
          // scan for a multipart result 
          List<Long> multipartResult = scanForMultiPartList(fs,conf,candidate.getPath());
          if (multipartResult != null) {
            LOG.info("Merge Candidate Completed with Multipart Result:" + multipartResult);
            set.addAll(multipartResult);
          }
          else { 
            set.add(candidateTimestamp);
          }
        }
        else { 
          LOG.info("Rejected Merge Candidate:" + candidate.getPath());
        }
      }
    }
    return set;
  }  
  
}
