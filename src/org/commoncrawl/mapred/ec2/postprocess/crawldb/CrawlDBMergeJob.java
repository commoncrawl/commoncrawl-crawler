package org.commoncrawl.mapred.ec2.postprocess.crawldb;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.TextBytes;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

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
  
  // Default Max Paritions per Run 
  static final int DEFAULT_MAX_PARTITIONS_PER_RUN = 10;
  
  
  
  public static void main(String[] args) throws Exception  {

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(new URI(S3N_BUCKET_PREFIX),conf);
    
    //TODO: Read Parition Size from Command Line
    int partitionSize = DEFAULT_MAX_PARTITIONS_PER_RUN;
    
    
    // find latest intermediate merge timestamp ...
    long latestIntermediateMergeTS = findLatestTimestamp(fs, conf,INTERMEDIATE_MERGE_PATH);
    // final latest final merge 
    LOG.info("Latest Intermediate Merge Timestamp is:" + latestIntermediateMergeTS);
    // find latest intermediate merge timestamp ...
    long latestFullMergeTS = findLatestTimestamp(fs, conf,FULL_MERGE_PATH);
    LOG.info("Latest Full Merge Timestamp is:" + latestFullMergeTS);
    // pick max timestamp 
    long latestMergeTimestamp = Math.max(latestIntermediateMergeTS,latestFullMergeTS);
    
    // find list of merge candidates ... 
    List<Long> candidateList = filterAndSortRawInputs(fs, conf, GRAPH_DATA_OUTPUT_PATH, latestMergeTimestamp);

    // partition into groups
    List<List<Long>> partitions = Lists.partition(candidateList, partitionSize);
    
    // ok run intermediate merges ...
    for (List<Long> partitionIds : partitions) { 
      runIntermediateMerge(fs,conf,partitionIds);
    }
    
    // find list of final merge candidates 
    List<Long> finalMergeCandidates = filterAndSortRawInputs(fs, conf, INTERMEDIATE_MERGE_PATH, latestMergeTimestamp);
    
    // run final merge ... (if necessary)
    runFinalMerge(fs,conf,finalMergeCandidates,latestFullMergeTS);    
  }
  
  /** 
   * run the crawldb merge on a list of graph data segments  
   * @param fs
   * @param conf
   * @param partitionIds
   * @throws IOException
   */
  static void runIntermediateMerge(FileSystem fs,Configuration conf,List<Long> partitionIds)throws IOException  { 
    long maxTimestamp = Iterators.getLast(partitionIds.iterator(),(long)-1).longValue();
    
    if (maxTimestamp == -1) { 
      throw new IOException("No Valid Partitions Found in List:" + partitionIds);
    }
    
    // construct a final output path ...   
    Path finalOutputPath = new Path(S3N_BUCKET_PREFIX + INTERMEDIATE_MERGE_PATH,Long.toString(maxTimestamp));
    
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
   * run the final crawldb merge on a list intermediate merge candidates   
   * @param fs
   * @param conf
   * @param partitionIds
   * @throws IOException
   */
  static void runFinalMerge(FileSystem fs,Configuration conf,List<Long> partitionIds, long latestFinalMergeTS)throws IOException  { 
    long maxTimestamp = Iterators.getLast(partitionIds.iterator(),(long)-1).longValue();
    
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
  
  /** 
   * iterate the intermediate link graph data and extract unmerged set ... 
   * 
   * @param fs
   * @param conf
   * @param latestMergeDBTimestamp
   * @return
   * @throws IOException
   */
  static List<Long> filterAndSortRawInputs(FileSystem fs,Configuration conf,String searchPath, long latestMergeDBTimestamp )throws IOException { 
    ArrayList<Long> list = new ArrayList<Long>();
    FileStatus candidates[] = fs.globStatus(new Path(S3N_BUCKET_PREFIX + searchPath,"[0-9]*"));
    
    for (FileStatus candidate : candidates) {
      LOG.info("Found Merge Candidate:" + candidate.getPath());
      long candidateTimestamp = Long.parseLong(candidate.getPath().getName());
      if (candidateTimestamp > latestMergeDBTimestamp) { 
        Path successPath = new Path(candidate.getPath(),"_SUCCESS");
        if (fs.exists(successPath)) { 
          list.add(candidateTimestamp);
        }
        else { 
          LOG.info("Rejected Merge Candidate:" + candidate.getPath());
        }
      }
    }
    
    // sort the list ... 
    Collections.sort(list);
    
    return list;
  }  
  
}
