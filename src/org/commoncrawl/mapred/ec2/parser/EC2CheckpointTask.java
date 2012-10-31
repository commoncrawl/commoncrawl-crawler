package org.commoncrawl.mapred.ec2.parser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.commoncrawl.protocol.ParseOutput;
import org.commoncrawl.util.ArcFileWriter;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.Tuples.Pair;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;
/** 
 * 
 * The CC EC2 workflow involves running the EC2ParserTask, which ingests RAW crawl logs (data) and produces  
 * ARC, metadata and text files for crawled content. The parse jobs run on EC2 in an EMR/Spot Instance context, 
 * so smooth job performance is important to prevent lagging mappers from reducing cluster utilization and thus 
 * wasting expensive compute resources. To resolve this requirement, the parse job will fail-fast mappers that
 * either take too long or those that create too many failures. A scheme has been put into place whereby failed 
 * splits are tracked. This task (the Checkpoint Task) is run after a Parse run has completed and its job is to 
 * collect all the failed splits, group them into a unit called a 'Checkpoint' and then run them in a modified
 * (potentially less expensive, longer running) job context, to try and achieve as close to 100% coverage of the 
 * raw crawl data. This task creates a staged checkpoint directory, under which it creates a set of segments, each
 * of which contains a set of failed splits from previous parse runs. It subsequently runs map-reduce jobs to parse 
 * these segments. Once all segments have been parsed (as best as possible), the 'staged' checkpoint is promoted to a 
 * real checkpoint by having it move from the staged_checkpoint directory to the checkpoint directory. In the process,
 * all segments that were processed within the context of the checkpoint are promoted to be 'real segments', and are 
 * thus added to the valid_segments list and are made visible to all consumers of the data.     
 * 
 * @author rana
 *
 */
public class EC2CheckpointTask extends EC2TaskDataAwareTask { 
  
  public EC2CheckpointTask(Configuration conf) throws IOException {
    
    super(conf);
    
    if (conf.getBoolean(CONF_PARAM_TEST_MODE, false))  {
      maxSimultaneousJobs = 1;
    }
    
    jobThreadSemaphore = new Semaphore(-(maxSimultaneousJobs-1));
    
  }

  public static final Log LOG = LogFactory.getLog(EC2CheckpointTask.class);
  
  static Options options = new Options();
  static { 
    
    options.addOption(
        OptionBuilder.withArgName("testMode").hasArg(false).withDescription("Test Mode").create("testMode"));
  }

  
  public static void main(String[] args)throws IOException {

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
        conf.setBoolean(Constants.CONF_PARAM_TEST_MODE,true);
      }
      else { 
        LOG.info("Running in Prod Mode");
      }
      
      FileSystem fs;
      try {
        fs = FileSystem.get(new URI("s3n://aws-publicdatasets"),conf);
      } catch (URISyntaxException e) {
        throw new IOException(e.toString());
      }
      LOG.info("FileSystem is:" + fs.getUri() +" Scanning for valid checkpoint id");
      long latestCheckpointId = findLastValidCheckpointId(fs,conf);
      LOG.info("Latest Checkpoint Id is:"+ latestCheckpointId);
      
      EC2CheckpointTask task = new EC2CheckpointTask(conf);
      
      LOG.info("Performing checkpoint");
      task.doCheckpoint(fs, conf);
      LOG.info("checkpoint complete");
      task.shutdown();
    
      System.exit(0);
    }
    catch (Exception e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      System.exit(1);
    }
  }
  
  /**
   * return the last valid checkpoint id or -1 if no checkpoints exist
   * 
   * @param fs
   * @param conf
   * @return
   * @throws IOException
   */
  static long findLastValidCheckpointId(FileSystem fs, Configuration conf)throws IOException {
    long lastCheckpointId = -1L;
    for (FileStatus dirStats : fs.globStatus(new Path(S3N_BUCKET_PREFIX + CHECKPOINTS_PATH,"[0-9]*"))) {  
      lastCheckpointId = Math.max(lastCheckpointId,Long.parseLong(dirStats.getPath().getName()));
    }
    return lastCheckpointId;
  }
  
  /** 
   * return the currently active checkpoint's id or -1 if no active checkpoint 
   * @param fs
   * @param conf
   * @return
   * @throws IOException
   */
  static long findStagedCheckpointId(FileSystem fs,Configuration conf)throws IOException { 
    FileStatus[] intermediateCheckpoints = fs.globStatus(new Path(S3N_BUCKET_PREFIX + CHECKPOINT_STAGING_PATH,"[0-9]*"));
    if (intermediateCheckpoints.length > 1) { 
      throw new IOException("More than one Staged Checkpoint Found!:" + intermediateCheckpoints);
    }
    else if (intermediateCheckpoints.length == 1) { 
      return Long.parseLong(intermediateCheckpoints[0].getPath().getName()); 
    }
    return -1L;
  }
  
  static Pattern arcFileNamePattern = Pattern.compile("^([0-9]*)_([0-9]*).arc.gz$");
  
  static Multimap<Integer,Long> getArcFilesSizesSegment(FileSystem fs,long segmentId) throws IOException  {
    
    Multimap<Integer,Long> splitToSizeMap = TreeMultimap.create();
    
    for (FileStatus arcCandidate : fs.globStatus(new Path(S3N_BUCKET_PREFIX + SEGMENTS_PATH + segmentId,"*.arc.gz"))) { 
      Matcher m = arcFileNamePattern.matcher(arcCandidate.getPath().getName());
      if (m.matches() && m.groupCount() == 2) { 
        int splitId = Integer.parseInt(m.group(2));
        splitToSizeMap.put(splitId,arcCandidate.getLen());
      }
    }
    return splitToSizeMap;
  }
    
  
  /** 
   * Given a list of Splits from a set of previosuly completed segments, construct a set of checkpoint segments 
   * and distribute splits amongst them 
   * 
   * @param fs
   * @param segmentOutputPath
   * @param splitDetails
   * @param baseSegmentId
   * @param defaultSplitSize
   * @param idealSplitsPerSegment
   * @throws IOException
   */
  static void buildSplitsForCheckpoint(FileSystem fs,Configuration conf,Path segmentOutputPath,List<SegmentSplitDetail> splitDetails,long baseSegmentId, int defaultSplitSize, int idealSplitsPerSegment) throws IOException {
    
    if (conf.getBoolean(CONF_PARAM_TEST_MODE, false)) { 
      idealSplitsPerSegment = 10;
      LOG.info("In Test Mode. Setting idealSplitsPerSegment to:" + idealSplitsPerSegment);
    }
    
    LOG.info("Attempting to split:" + splitDetails.size() + " splits using split size:" + defaultSplitSize + " desired splits per seg:" + idealSplitsPerSegment);
    ArrayList<FileSplit> splits = new ArrayList<FileSplit>();
    
    for (SegmentSplitDetail splitDetail : splitDetails) { 
      SplitInfo splitItem = null;
      if (splitDetail.isPartialSplit()) { 
        splitItem = splitDetail.partialSplit; 
      }
      else { 
        splitItem = splitDetail.originalSplit;
      }
      
      long splitBytes = splitItem.length;
      long splitOffset = splitItem.offset;
      int splitCount = (int) (splitBytes / defaultSplitSize);
      // if split bytes is less than default split size or the trailing bytes in the last 
      // split is >= 1/2 split size then add an additional split item to the list ... 
      if (splitCount == 0 || splitBytes % defaultSplitSize >= defaultSplitSize / 2) { 
        splitCount ++;
      }
      // now ... emit splits ... 
      for (int i=0;i<splitCount;++i) {
        long splitSize = defaultSplitSize;
        
        // gobble up all remaining bytes for trailing split ... 
        if (i == splitCount -1) { 
          splitSize = splitBytes;
        }
        // create split ... 
        FileSplit split = new FileSplit(new Path(splitItem.sourceFilePath),splitOffset, splitSize, (String[])null);
        // add split 
        splits.add(split);
        // increment counters 
        splitBytes -= splitSize;
        splitOffset += splitSize;
      }
    }
    
    // ok, now collect segments .. doing basic partitioning ... 
    List<List<FileSplit>> segments = Lists.partition(splits, idealSplitsPerSegment);
    
    LOG.info("Partitioned splits into:" + segments.size() + " segements");

    long segmentId = baseSegmentId;
    
    // now emit the segments ...
    for (List<FileSplit> segmentSplits : segments) { 
      // establish split path ... 
      Path splitManifestPath = new Path(segmentOutputPath +"/" + Long.toString(segmentId) +"/" + SPLITS_MANIFEST_FILE);
      // write manifest ... 
      listToTextFile(segmentSplits,fs,splitManifestPath);
      LOG.info("Wrote "+ segmentSplits.size() +" splits for segment:" + segmentId + " to:" + splitManifestPath);
      segmentId ++;
    }
  }
  
  /** 
   * 
   *  load checkpoint state from either a partially completed checkpoint, or a newly constructed checkpoint
   *  
   * @param fs
   * @param conf
   * @return
   * @throws IOException
   */
  static Pair<Long,Multimap<Long,SplitInfo>> findOrCreateCheckpoint(FileSystem fs,Configuration conf) throws IOException { 
    
    Multimap<Long,SplitInfo> segmentsAndSplits = null;
    LOG.info("Looking for Stagesd Checkpoint.");
    long stagedCheckpointId = findStagedCheckpointId(fs, conf);
    if (stagedCheckpointId == -1) {
      // create a base segment id ... 
      long baseSegmentId = System.currentTimeMillis();
      // and create a new checkpoint id ...
      stagedCheckpointId = baseSegmentId + 100000;
      LOG.info("No Staged Checkpoint Found. Creating New Checkpoint:" + stagedCheckpointId);
      
      // get last valid checkpoint id ... 
      long lastValidCheckpointId = findLastValidCheckpointId(fs, conf);
      LOG.info("Last Valid Checkpoint Id is:" + lastValidCheckpointId);
      
      LOG.info("Iterating Available Segments and collecting Splits");
      // iterate available segments (past last checkpoint date), collecting a list of partial of failed splits ... 
      List<SegmentSplitDetail> splitDetails = iterateAvailableSegmentsCollectSplits(fs,conf,lastValidCheckpointId);
      
      if (splitDetails.size() != 0) {
        try { 
          Path checkpointSplitsPath = new Path(S3N_BUCKET_PREFIX + CHECKPOINT_STAGING_PATH +Long.toString(stagedCheckpointId)+"/"+SPLITS_MANIFEST_FILE);
          
          LOG.info("Writing Splits Manifest (for checkpoint) to:" + checkpointSplitsPath);
          // write source split details to disk ... 
          listToTextFile(splitDetails,fs,checkpointSplitsPath);
          
          LOG.info("Assigning Splits to Checkpoint Segments");
          // given the list of failed/partial splits, ditribute them to a set of segments ...  
          buildSplitsForCheckpoint(fs,conf,new Path(S3N_BUCKET_PREFIX + CHECKPOINT_STAGING_PATH + "/" + Long.toString(stagedCheckpointId)+"/"),splitDetails,baseSegmentId,DEFAULT_PARSER_CHECKPOINT_JOB_SPLIT_SIZE,DEFAULT_PARSER_CHECKPOINT_SPLITS_PER_JOB);
        }
        catch (Exception e) { 
          LOG.error("Failed to create checkpoint segment:" + stagedCheckpointId + " Exception:"+ CCStringUtils.stringifyException(e));
          fs.delete(new Path(S3N_BUCKET_PREFIX + CHECKPOINT_STAGING_PATH + Long.toString(stagedCheckpointId)+"/"), true);
        }
      }
      else { 
        throw new IOException("No Valid Splits Found for Checkpoint!");
      }
    }
    
    // ok read in the splits ... 
    segmentsAndSplits = TreeMultimap.create();
    
    LOG.info("Scanning checkpoint staging dir for segments");
    // load the segments and splits from disk ...
    for (FileStatus stagedSegment : fs.globStatus(new Path(S3N_BUCKET_PREFIX + CHECKPOINT_STAGING_PATH + Long.toString(stagedCheckpointId)+"/"+ "[0-9]*"))) {
      long segmentId = Long.parseLong(stagedSegment.getPath().getName());
      for (SegmentSplitDetail splitDetail :   getSplitDetailsFromFile(fs,segmentId,new Path(stagedSegment.getPath(),SPLITS_MANIFEST_FILE),SPLITS_MANIFEST_FILE)) {
        segmentsAndSplits.put(segmentId, splitDetail.originalSplit);
      }
      LOG.info("Found Segment:" + segmentId + " with: " + segmentsAndSplits.size() + " splits");
    }
    return new Pair<Long,Multimap<Long,SplitInfo>>(stagedCheckpointId,segmentsAndSplits);
  }
  
  /** 
   * filter out incomplete segments given a set of checkpoint segments
   * 
   * @param fs
   * @param conf
   * @param checkpointDetail
   * @return
   * @throws IOException
   */
  static private List<Pair<Long,Collection<SplitInfo>>> filterCompletedSegments(FileSystem fs,Configuration conf,Pair<Long,Multimap<Long,SplitInfo>> checkpointDetail)throws IOException { 
    ArrayList<Pair<Long,Collection<SplitInfo>>> segmentListOut= new ArrayList<Pair<Long,Collection<SplitInfo>>>();
    // iterate segments ...
    for (long segmentId : checkpointDetail.e1.keySet()) { 
      // establish path ...
      Path segmentPath = new Path(S3N_BUCKET_PREFIX + CHECKPOINT_STAGING_PATH + checkpointDetail.e0 +"/" + segmentId+"/");
      // establish success file path 
      Path successFile = new Path(segmentPath,JOB_SUCCESS_FILE);
      // and output path 
      Path outputPath = new Path(segmentPath,CHECKPOINT_JOB_OUTPUT_PATH);
      
      LOG.info("Checking for existence of Success File:" + successFile + " for segment:" + segmentId);
      // check to see if job already completed  
      if (!fs.exists(successFile)) {
        LOG.info("Success File not found for segment:" + segmentId + ".Deleting partial outputs at:" + outputPath);
        // check to see if output folder exists... if so,delete it ... 
        if (fs.exists(outputPath)) { 
          fs.delete(outputPath, true);
        }
        segmentListOut.add(new Pair<Long, Collection<SplitInfo>>(segmentId,checkpointDetail.e1.values()));
      }
    }
    LOG.info("There are: " + segmentListOut.size() + " segments (post filtering)");
    return segmentListOut;
  }
  
  static final int MAX_SIMULTANEOUS_JOBS = 100;

  LinkedBlockingQueue<QueueItem> _queue = new LinkedBlockingQueue<QueueItem>();
  Semaphore jobThreadSemaphore = null;
  int maxSimultaneousJobs = MAX_SIMULTANEOUS_JOBS;

  /** 
   * helper class used to queue individual checkpoint segments for map-reduce processing 
   * @author rana
   *
   */
  static class QueueItem {
    QueueItem() { 
      
    }
    
    QueueItem(FileSystem fs,Configuration conf,long checkpointId,Pair<Long,Collection<SplitInfo>> segmentDetail) { 
      this.conf = conf;
      this.fs = fs;
      this.checkpointId = checkpointId;
      this.segmentDetail = segmentDetail;
    }
    
    public Configuration conf;
    public FileSystem fs;
    public long checkpointId;
    public Pair<Long,Collection<SplitInfo>> segmentDetail;
  }
  
  
  static void copySrcFileToDest(FileSystem fs,Path src,Path dest,Configuration conf)throws IOException { 
    FSDataInputStream inputStream = null;
    FSDataOutputStream outputStream = null;
    
    try { 
      inputStream = fs.open(src);
      outputStream = fs.create(dest);
      
      IOUtils.copyBytes(inputStream, outputStream, conf);
      
      outputStream.flush();
      
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      throw e;
    }
    finally {
      IOException exceptionOut = null;
      try { 
        if (inputStream != null)
          inputStream.close();
      }
      catch (IOException e) { 
        exceptionOut = e;
      }
      try { 
        if (outputStream != null)
          outputStream.close();
      }
      catch (IOException e) { 
        exceptionOut = e;
      }
      if (exceptionOut != null) 
        throw exceptionOut;
    }
  }
  
  /** 
   * Given a file system pointer (s3n) and a configuration, scan all previously processed segments that 
   * are NOT part of a previous checkpoint, and build a list of partially completed splits and fails splits.
   * Next, create a new checkpoint, and distribute the splits amongst a new set of segments (within the context of 
   * the checkpoint). Then queue up the segments for re-processing via map-reduce. Once all checkpoint segments have 
   * been processed, promote the 'staged' checkpoint to a real checkpoint.    
   * 
   * @param fs
   * @param conf
   * @throws IOException
   */
  public void doCheckpoint(final FileSystem fs,final Configuration conf)throws IOException { 
    LOG.info("Starting Checkpoint. Searching for existing or creating new staged checkpoint");
    final Pair<Long,Multimap<Long,SplitInfo>> checkpointInfo = findOrCreateCheckpoint(fs, conf);
    LOG.info("Checkpoint Id is:"+ checkpointInfo.e0 + " and it has:" + checkpointInfo.e1.keySet().size() + " segments."); 
    LOG.info("Filtering for completed segments");
    List<Pair<Long,Collection<SplitInfo>>> validSegments = filterCompletedSegments(fs, conf, checkpointInfo);
    LOG.info("Queueing Segments. There are:" + validSegments.size() + " segments out of a total of:" + checkpointInfo.e1.keySet().size());
    for (Pair<Long,Collection<SplitInfo>> segmentInfo : validSegments) { 
      try {
        _queue.put(new QueueItem(fs, conf, checkpointInfo.e0, segmentInfo));
      } catch (InterruptedException e) {
      }
    }

    // queue shutdown items 
    for (int i=0;i<maxSimultaneousJobs;++i) { 
      try {
        _queue.put(new QueueItem());
      } catch (InterruptedException e) {
      }
    }

    LOG.info("Starting Threads");
    // startup threads .. 
    for (int i=0;i<maxSimultaneousJobs;++i) { 
      Thread thread = new Thread(new QueueTask());
      thread.start();
    }
    
        
    // ok wait for them to die
    LOG.info("Waiting for Queue Threads to Die");
    jobThreadSemaphore.acquireUninterruptibly();
    
    // now promote the checkpoint
    // (1) walk completed segments ... 
    // 
    for (long segmentId : checkpointInfo.e1.keySet()) { 
      // establish paths ...
      Path segmentPath = new Path(S3N_BUCKET_PREFIX + CHECKPOINT_STAGING_PATH + checkpointInfo.e0 +"/" + segmentId+"/");
      Path segmentOutputPath = new Path(segmentPath,"output");
      
      // establish success file path 
      Path successFile = new Path(segmentPath,JOB_SUCCESS_FILE);
      // check to see if job already completed  
      if (fs.exists(successFile)) {
        LOG.info("Promoting Checkpoint Segment:" + segmentId);
        //   for each segment:

        Path validSegmentsBasePath = new Path(S3N_BUCKET_PREFIX + VALID_SEGMENTS_PATH+Long.toString(segmentId));

        if (fs.exists(segmentOutputPath)) { 
          //    (a) mkdir parse-output/valid_segments2/[segmentId]
          fs.mkdirs(validSegmentsBasePath);
  
          //    (b) copy failed_splits.txt,splits.txt,trailing_splits.txt to parse-output/valid_segments2/[segmentId]
          LOG.info("Writing manifests for Segment:" + segmentId + " to:" + validSegmentsBasePath);
          fs.rename(new Path(segmentOutputPath,TRAILING_SPLITS_MANIFEST_FILE),new Path(validSegmentsBasePath,TRAILING_SPLITS_MANIFEST_FILE));
          fs.rename(new Path(segmentOutputPath,FAILED_SPLITS_MANIFEST_FILE),new Path(validSegmentsBasePath,FAILED_SPLITS_MANIFEST_FILE));
          if (fs.exists(new Path(validSegmentsBasePath,SPLITS_MANIFEST_FILE))) { 
            copySrcFileToDest(fs,new Path(segmentPath,SPLITS_MANIFEST_FILE),new Path(validSegmentsBasePath,SPLITS_MANIFEST_FILE),conf);
          }
          
          // create final output path 
          Path finalOutputPath = new Path(S3N_BUCKET_PREFIX + SEGMENTS_PATH + segmentId);
  
          
          //   (c) rename output to final output path ...
          fs.rename(segmentOutputPath, finalOutputPath);
        }
        
        //   (d) write parse-output/valid_segments2/[segmentId]/manifest.txt
        Path segmentManifestPath = new Path(S3N_BUCKET_PREFIX + VALID_SEGMENTS_PATH + Long.toString(segmentId)+"/"+SEGMENT_MANIFEST_FILE);
        
        if (!fs.exists(segmentManifestPath)) { 
          Collection<Path> inputs = Collections2.transform(checkpointInfo.e1.get(segmentId), new Function<SplitInfo,Path>() {
  
            @Override
            @Nullable
            public Path apply(@Nullable SplitInfo arg0) {
              return new Path(arg0.sourceFilePath);
            }
          } );
          LOG.info("Writing split manifest file for Segment:" + segmentId + " to:" + validSegmentsBasePath);
          writeSegmentManifestFile(fs, segmentManifestPath, segmentId, inputs);
        }
        // (f) write is_checkpoint_segment marker
        LOG.info("Writing is_checkpoint_flag for Segment:" + segmentId + " to:" + validSegmentsBasePath);
        fs.createNewFile(new Path(validSegmentsBasePath,Constants.IS_CHECKPOINT_SEGMENT_FLAG));
      }
      else { 
        LOG.error("Found Invalid Checkpoint Segment at path:"+ segmentPath);
      }
    }
    // (2) mkdir parse-output/checkpoint/[staged_checkpoint_id] dir
    Path checkpointDir = new Path(S3N_BUCKET_PREFIX + CHECKPOINTS_PATH + checkpointInfo.e0);
    LOG.info("All checkpoint segments transferred. Generating checkpoint directory:" + checkpointDir);
    fs.mkdirs(checkpointDir);
    // (3) copy parse-output/checkpoint_staging/[staged_checkpoint_id]/splits.txt to parse-output/checkpoint/[staged_checkpoint_id]
    fs.rename(new Path(S3N_BUCKET_PREFIX + CHECKPOINT_STAGING_PATH_PROPERTY + checkpointInfo.e0,Constants.SPLITS_MANIFEST_FILE),
        new Path(S3N_BUCKET_PREFIX + CHECKPOINTS_PATH + checkpointInfo.e0,Constants.SPLITS_MANIFEST_FILE));
    // (4) rmr  parse-output/checkpoint_staging/[staged_checkpoint_id]
    // DONE. 
    LOG.info("Checkpoint:" + checkpointInfo.e0 + " Complete");
  }
    
    
  private static void writeSegmentManifestFile(FileSystem fs,Path manifestFilePath,long segmentTimestamp,Collection<Path> logsInSegment) throws IOException {
    LOG.info("Writing Segment Manifest for Segment: " + segmentTimestamp + " itemCount:" + logsInSegment.size());
    ImmutableList.Builder<String> listBuilder = new ImmutableList.Builder<String>();
    
    for (Path logPath : logsInSegment) { 
      listBuilder.add(logPath.toString().substring(S3N_BUCKET_PREFIX.length()));
    }
    listToTextFile(listBuilder.build(), fs, manifestFilePath);
  }
  
  
  /** 
   * Worker Thread that actually submits jobs to the TT
   * 
   * @author rana
   *
   */
  class QueueTask implements Runnable {


    @Override
    public void run() {
      while (true) {
        LOG.info("Queue Thread:" + Thread.currentThread().getId() + " Running");
        try {
          QueueItem item = _queue.take();
          
          
          if (item.segmentDetail != null) { 
            LOG.info("Queue Thread:" + Thread.currentThread().getId() + " got segment:" + item.segmentDetail.e0);
            LOG.info("Queue Thread:" + Thread.currentThread().getId() + " Starting Job");
            try {
              parse(item.fs,item.conf,item.checkpointId,item.segmentDetail);
            } catch (Exception e) {
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
  

  /** 
   * Custom InputFormat that returns a set of splits via the (previously generated) splits manifest file 
   * @author rana
   *
   * @param <Key>
   * @param <Value>
   */
  public static class CheckpointInputFormat<Key,Value> extends SequenceFileInputFormat<Key, Value> {
    
    static Pattern splitPattern = Pattern.compile("^([^:]*)://([^:]*):([^+]*)\\+(.*)$");
    
    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits)throws IOException {
      // get the checkpoint segment path ... 
      //String segmentPath = job.get(SEGMENT_PATH_PROPERTY);
      // get the splits file ... 
      Path segmentPath = FileInputFormat.getInputPaths(job)[0];
      Path splitsPath = new Path(segmentPath,SPLITS_MANIFEST_FILE);
      LOG.info("Splits Path is:" + splitsPath);
      // get fs 
      FileSystem fs = FileSystem.get(splitsPath.toUri(),job);
      // and read in splits ... 
      List<String> splits = textFileToList(fs, splitsPath);
      
      ArrayList<FileSplit> fileSplits = new ArrayList<FileSplit>(splits.size());
      
      // convert to FileSplits ... 
      for (String split : splits) {
        if (split.length() != 0 && !split.startsWith("#")) { 
          Matcher m = splitPattern.matcher(split);
          if (m.matches()) { 
            String sourceFilePath = m.group(1)+"://"+m.group(2);
            long offset = Long.parseLong(m.group(3));
            long length = Long.parseLong(m.group(4));
            
            fileSplits.add(new FileSplit(new Path(sourceFilePath),offset,length,(String[])null));
          }
          else { 
            throw new IOException("Failed to parse input split info:" + split);
          }
        }
      }
      return fileSplits.toArray(new FileSplit[0]);
    }
  }
  
  /** 
   * spawn a mapreduce job to parse a given checkpoint segment 
   * @param fs
   * @param conf
   * @param checkpointId
   * @param segmentDetail
   * @throws IOException
   */
  private static void parse(FileSystem fs,Configuration conf,long checkpointId,Pair<Long,Collection<SplitInfo>> segmentDetail)throws IOException { 
    
    // create segment path 
    Path fullyQualifiedSegmentPath = new Path(S3N_BUCKET_PREFIX + CHECKPOINT_STAGING_PATH +checkpointId+"/"+ segmentDetail.e0 +"/");
    
    // and derive output path 
    Path outputPath = new Path(fullyQualifiedSegmentPath,Constants.CHECKPOINT_JOB_OUTPUT_PATH);
    
    // delete the output if exists ... 
    fs.delete(outputPath, true);
    
    LOG.info("Starting Map-Reduce Job. SegmentId:" + segmentDetail.e0+ " OutputPath:" + outputPath);
    
    // run job...
    JobConf jobConf = new JobBuilder("parse job",conf)
      
      .input(fullyQualifiedSegmentPath) // TODO: HACK .. NOT NEEDED 
      .inputFormat(CheckpointInputFormat.class)
      .keyValue(Text.class, ParseOutput.class)
      .mapRunner(ParserMapRunner.class)
      .mapper(ParserMapper.class)
      // allow three attempts to process the split 
      .maxMapAttempts(3)
      .maxMapTaskFailures(1000)
      .speculativeExecution(true)
      .numReducers(0)
      .outputFormat(ParserOutputFormat.class)
      .output(outputPath)
      .reuseJVM(1000)
      .build();
    
    Path jobLogsPath = new Path(fullyQualifiedSegmentPath,Constants.CHECKPOINT_JOB_LOG_PATH);
    
    // delete if exists ... 
    fs.delete(jobLogsPath, true);
    
    jobConf.set("hadoop.job.history.user.location", jobLogsPath.toString());
    
    jobConf.set("fs.default.name", S3N_BUCKET_PREFIX);    
    jobConf.setLong("cc.segmet.id", segmentDetail.e0);
    // set task timeout to 120 minutes 
    jobConf.setInt("mapred.task.timeout", 20 * 60 * 1000);
    // set mapper runtime to max 2 hours .....  
    jobConf.setLong(ParserMapper.MAX_MAPPER_RUNTIME_PROPERTY, 120 * 60  * 1000);
    
    jobConf.setOutputCommitter(OutputCommitter.class);
    // allow lots of failures per tracker per job 
    jobConf.setMaxTaskFailuresPerTracker(Integer.MAX_VALUE);
    
    LOG.info("Initializing TDC for Thread:"+Thread.currentThread().getId() + " Segment:" + segmentDetail.e0);
    initializeTaskDataAwareJob(jobConf,segmentDetail.e0);

    LOG.info("Submmitting Hadoop Job for Thread:"+Thread.currentThread().getId() + " Segment:" + segmentDetail.e0);
    JobClient.runJob(jobConf);
    
    LOG.info("Finalizing Job for Thread:"+Thread.currentThread().getId() + " Segment:" + segmentDetail.e0);
    finalizeJob(fs,conf,jobConf,outputPath,segmentDetail.e0);

    
    // ok job execution was successful ... mark it so ... 
    Path successFile = new Path(fullyQualifiedSegmentPath,Constants.JOB_SUCCESS_FILE);
    
    fs.createNewFile(successFile);
    LOG.info("Map-Reduce Job for SegmentId:" + segmentDetail.e0+ " Completed Successfully");
  }
  
  /** 
   * Iterate previously parsed segments and collect partial and failed splits 
   * 
   * @param fs
   * @param lastCheckpointId
   * @throws IOException
   */
  
  static ArrayList<SegmentSplitDetail> iterateAvailableSegmentsCollectSplits(FileSystem fs,Configuration conf,long lastCheckpointId)throws IOException {
    ArrayList<SegmentSplitDetail> listOut = new ArrayList<SegmentSplitDetail>();
    
    for (long segmentId : buildValidSegmentListGivenCheckpointId(fs,conf, lastCheckpointId)) {
      LOG.info("Found Segment:" + segmentId);
      
      // get arc sizes by split upfront (because S3n wildcard operations are slow) 
      Multimap<Integer, Long> splitSizes= getArcFilesSizesSegment(fs,segmentId);
      
      LOG.info("Found ArcFiles for:" + splitSizes.keySet().size() + " Splits");
      
      // get failed and partial splits for segment 
      SortedSet<SegmentSplitDetail> allSplits = getAllSplits(fs, segmentId);
      SortedSet<SegmentSplitDetail> failedSplits = getFailedSplits(fs, segmentId);
      SortedSet<SegmentSplitDetail> partialSplits = getPartialSplits(fs, segmentId);
      
      LOG.info("Found:" + partialSplits.size() + " PartialSplits for Segment:" + segmentId);
      //LOG.info(partialSplits.toString());
      // ok add all partial splits to list up front ... 
      listOut.addAll(partialSplits);
      
      // now calculate a raw to arc split ratio ... 
      DescriptiveStatistics stats = calculateArcToRawRatio(allSplits,failedSplits,partialSplits,splitSizes);
      double arcToRawRatio = stats.getMean();
      // calculate std-dev
      double stdDev = stats.getStandardDeviation();
      
      LOG.info("ArcToRaw Ratio:" + arcToRawRatio + " StdDev:" + stdDev);
      LOG.info("There are " + partialSplits.size() + " Partial splits");
      // exclude partial from failed to see how many actually failed ... 
      Sets.SetView<SegmentSplitDetail> reallyFailedSet = Sets.difference(failedSplits,partialSplits);
      LOG.info("There are " + reallyFailedSet.size() + " Failed splits");
      
      LOG.info("Found: " + reallyFailedSet.size() + " really failed splits for Segment:" + segmentId);
      // walk each validating actual failure condidition
      for (SegmentSplitDetail split : reallyFailedSet) {
        // explicitly add failed list to list out ... 
        listOut.add(split);

        /**
        if (!splitSizes.containsKey(split.splitIndex)) {
          // add the failed split .. no questions asked ... 
          listOut.add(split);
        }
        else { 
          // ok otherwise ... get the arc sizes for the given split ... 
          Collection<Long> arcSizes = splitSizes.get(split.splitIndex);
          // iff 
          long totalArcSize = 0;
          for (long arcSize : splitSizes.get(split.splitIndex)) 
            totalArcSize += arcSize; 
          double itemRatio = (double) totalArcSize / (double) split.originalSplit.length;
          
          LOG.info("Failed Split: " 
          + split.splitIndex 
          + " has arc data:" 
          + splitSizes.get(split.splitIndex) 
          + " ItemRatio:"+ itemRatio + " Overall Ratio:" + arcToRawRatio);
        }
        **/
      }
      
      if (conf.getBoolean(CONF_PARAM_TEST_MODE, false)) {
        LOG.info("Breaking out early from segment iteration (test mode)");
        break;
      }
    }
    
    return listOut;
  }
  
  /** 
   * arc to raw ratio calc (unused for now)
   * 
   * @param allSplits
   * @param failedSplits
   * @param partialSplits
   * @param arcSizes
   * @return
   */
  private static DescriptiveStatistics calculateArcToRawRatio(
      SortedSet<SegmentSplitDetail> allSplits,
      SortedSet<SegmentSplitDetail> failedSplits,
      SortedSet<SegmentSplitDetail> partialSplits,
      Multimap<Integer, Long> arcSizes) {
    
    DescriptiveStatistics stats = new DescriptiveStatistics();

    for (SegmentSplitDetail split : allSplits) { 
      if (!failedSplits.contains(split)  && !partialSplits.contains(split)) { 
        long totalArcSize = 0;
        for (long arcSize : arcSizes.get(split.splitIndex)) 
          totalArcSize += arcSize;
        if (totalArcSize != 0)
          stats.addValue((double)totalArcSize / (double)split.originalSplit.length); 
      }
    }
    
    return stats;
  }

  /** 
   * scan valid segments and pick up any whose id exceeds given last 
   * checkpoint id 
   * @param fs
   * @param lastCheckpointId
   * @return
   * @throws IOException
   */
  static Set<Long> buildValidSegmentListGivenCheckpointId(FileSystem fs,Configuration conf,long lastCheckpointId)throws IOException { 
    return buildSegmentListGivenCheckpointId(fs, VALID_SEGMENTS_PATH, lastCheckpointId);
  }
  
  static Set<Long> buildSegmentListGivenCheckpointId(FileSystem fs,String validSegmentPath,long lastCheckpointId)throws IOException { 
    TreeSet<Long> validsegments = new TreeSet<Long>();
    for (FileStatus segmentStatus: fs.globStatus(new Path(validSegmentPath,"[0-9]*"))) {
      long segmentId = Long.parseLong(segmentStatus.getPath().getName());
      if (segmentId > lastCheckpointId) { 
        validsegments.add(segmentId);
      }
    }
    return validsegments;
  }

  

  static SortedSet<SegmentSplitDetail> getAllSplits(FileSystem fs,long segmentId)throws IOException { 
    return getSplitDetailsFromFile(fs,segmentId,new Path(S3N_BUCKET_PREFIX + VALID_SEGMENTS_PATH+Long.toString(segmentId)+"/"+SPLITS_MANIFEST_FILE),SPLITS_MANIFEST_FILE); 
  }

  static SortedSet<SegmentSplitDetail> getFailedSplits(FileSystem fs,long segmentId)throws IOException { 
    return getSplitDetailsFromFile(fs,segmentId,new Path(S3N_BUCKET_PREFIX + VALID_SEGMENTS_PATH+Long.toString(segmentId)+"/"+FAILED_SPLITS_MANIFEST_FILE),FAILED_SPLITS_MANIFEST_FILE); 
  }
  
  static SortedSet<SegmentSplitDetail> getPartialSplits(FileSystem fs,long segmentId)throws IOException { 
    return getSplitDetailsFromFile(fs,segmentId,new Path(S3N_BUCKET_PREFIX + VALID_SEGMENTS_PATH+Long.toString(segmentId)+"/"+TRAILING_SPLITS_MANIFEST_FILE),TRAILING_SPLITS_MANIFEST_FILE);
  }
  
  /** 
   * read split details given path and file type (partial, failed split etc.) 
   * 
   * @param fs
   * @param segmentId
   * @param path
   * @param splitType
   * @return
   * @throws IOException
   */
  static SortedSet<SegmentSplitDetail> getSplitDetailsFromFile(FileSystem fs,long segmentId,Path path,String splitType)throws IOException { 
        
    TreeSet<SegmentSplitDetail> splits = new TreeSet<EC2CheckpointTask.SegmentSplitDetail>();
    
    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path),Charset.forName("UTF-8")));
    try { 
      String line;
      int index=0;
      while ((line = reader.readLine()) != null) { 
        if (line.length() != 0 && !line.startsWith("#")) {  
          if (splitType == SPLITS_MANIFEST_FILE) { 
            SegmentSplitDetail splitDetail = new SegmentSplitDetail(segmentId);
            splitDetail.splitIndex = index++;
            splitDetail.originalSplit = new SplitInfo(line);
            splits.add(splitDetail);
          }
          else { 
            splits.add(splitDetailFromLogLine(segmentId,line, (splitType == TRAILING_SPLITS_MANIFEST_FILE)));
          }
        }
      }
    }
    finally { 
      reader.close();
    }
    return splits;
  }
  
  static Pattern partialSplitLogPattern = Pattern.compile("^([0-9]*),([^,]*),([^,]*)$");
  static Pattern failedSplitLogPattern = Pattern.compile("^([0-9]*),(.*)$");
  
  /** 
   * parse split detail given a split log line (based on type of split log)
   * @param segmentId
   * @param logLine
   * @param isPartialSplit
   * @return
   * @throws IOException
   */
  static SegmentSplitDetail splitDetailFromLogLine(long segmentId,String logLine,boolean isPartialSplit) throws IOException { 
    if (isPartialSplit) { 
      Matcher m = partialSplitLogPattern.matcher(logLine);
      if (m.matches() && m.groupCount() == 3) { 
        
        SegmentSplitDetail detail = new SegmentSplitDetail(segmentId);
        
        detail.splitIndex = Integer.parseInt(m.group(1));
        detail.partialSplit = new SplitInfo(m.group(2));
        detail.originalSplit = new SplitInfo(m.group(3));
        
        return detail;

      }
      else { 
        throw new IOException("Invalid Split Info:" + logLine);
      }
    }
    else { 
      Matcher m = failedSplitLogPattern.matcher(logLine);

      if (m.matches() && m.groupCount() == 2) { 
        
        SegmentSplitDetail detail = new SegmentSplitDetail(segmentId);
        
        detail.splitIndex = Integer.parseInt(m.group(1));
        detail.originalSplit = new SplitInfo(m.group(2));
        
        return detail;
      }
      else { 
        throw new IOException("Invalid Split Info:" + logLine);
      }
    }
  }
  
  
  /** 
   * Helper class than encapsulated a single file split's details
   * TODO: WHY ARE WE NOT EXTENDING FileSplit ???? 
   * @author rana
   *
   */
  static class SplitInfo implements Comparable<SplitInfo> { 
    
    String  sourceFilePath;
    long    offset;
    long    length;
    
    static Pattern pattern = Pattern.compile("^([^:]*)://([^:]*):([^+]*)\\+(.*)$");
    
    SplitInfo(String splitText)throws IOException { 
      Matcher m = pattern.matcher(splitText);
      if (m.matches() && m.groupCount() == 4) { 
        sourceFilePath = m.group(1)+"://"+m.group(2);
        offset = Long.parseLong(m.group(3));
        length = Long.parseLong(m.group(4));
      }
      else { 
        throw new IOException("Invalid Split:"+ splitText);
      }
    }

    @Override
    public int compareTo(SplitInfo other) {
      int result = sourceFilePath.compareTo(other.sourceFilePath);
      if (result == 0) { 
        result = (offset < other.offset) ? -1 : (offset > other.offset) ? 1: 0;
      }
      return result;
    }
    
    @Override
    public String toString() {
      return sourceFilePath+":"+offset+"+"+length;
    }
  }
  
  /** 
   * A class representing a partially processed or unprocessed split
   *  
   * @author rana
   *
   */
  static class SegmentSplitDetail implements Comparable<SegmentSplitDetail>{

    long    segmentId;
    int     splitIndex;
    SplitInfo originalSplit;
    SplitInfo partialSplit;    
    
    public SegmentSplitDetail(long segmentId) { 
      this.segmentId = segmentId;
    }
   
    
    @Override
    public int compareTo(SegmentSplitDetail o) {
      return (splitIndex < o.splitIndex) ? -1: (splitIndex > o.splitIndex) ? 1: 0;
    }
    
    public boolean isPartialSplit() { 
      return partialSplit != null;
    }
    
    @Override
    public String toString() {
      return Long.toString(segmentId)
          +","
          +splitIndex
          +","
          +((isPartialSplit()) ? "P":"F") 
          + "," 
          + (isPartialSplit() ? partialSplit.toString() : originalSplit.toString());
    }
  }
  
}
