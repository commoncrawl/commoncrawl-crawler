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
import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
public class EC2ParserTask {
  
  public static final Log LOG = LogFactory.getLog(EC2ParserTask.class);
  
  static final String S3N_BUCKET_PREFIX = "s3n://aws-publicdatasets";
  static final String CRAWL_LOG_INTERMEDIATE_PATH = "/common-crawl/crawl-intermediate/";
  static final String VALID_SEGMENTS_PATH = "/common-crawl/parse-output/valid_segments/";
  static final String SEGMENTS_PATH = "/common-crawl/parse-output/segment/";
  static final String SEGMENT_MANIFEST_FILE = "manfiest.txt";
  static final int    LOGS_PER_ITERATION = 1000;
  static final Pattern CRAWL_LOG_REG_EXP = Pattern.compile("CrawlLog_ccc[0-9]{2}-[0-9]{2}_([0-9]*)");
  static final int MAX_SIMULTANEOUS_JOBS = 100;
  
  
  LinkedBlockingQueue<QueueItem> _queue = new LinkedBlockingQueue<QueueItem>();
  Semaphore jobThreadSemaphore = new Semaphore(-(MAX_SIMULTANEOUS_JOBS-1));

  public EC2ParserTask(Configuration conf)throws Exception {
    FileSystem fs = FileSystem.get(new URI("s3n://aws-publicdatasets"),conf);
    LOG.info("FileSystem is:" + fs.getUri() +" Scanning for candidates at path:" + CRAWL_LOG_INTERMEDIATE_PATH);
    TreeSet<Path> candidateSet = buildCandidateList(fs, new Path(CRAWL_LOG_INTERMEDIATE_PATH));
    LOG.info("Scanning for completed segments"); 
    List<Path> processedLogs = scanForCompletedSegments(fs);
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

    //EC2ParserTask task = new EC2ParserTask(conf);
    //task.run();
    
    FileSystem fs = FileSystem.get(new URI("s3n://aws-publicdatasets"),conf);
    
    for (FileStatus fileStatus : fs.globStatus(new Path(VALID_SEGMENTS_PATH+"[0-9]*"))) {
    	
    	System.out.println("Building Splits for Segment:" + fileStatus.getPath().getName());
    	
    	// scan segment manifest file ... 
        List<Path> paths = scanSegmentManifestFile(fs,fileStatus.getPath());
        
        ArrayList<Path> absolutePaths = new ArrayList<Path>();
        for (Path relativePath : paths) { 
        	absolutePaths.add(new Path(new Path("s3n://aws-publicdatasets"),relativePath));
        }
        // create a Job Conf
        
        JobConf jobConf = new JobBuilder("parse job",conf)
        
        .inputs(absolutePaths)
        .inputFormat(SequenceFileInputFormat.class)
        .keyValue(Text.class, ParseOutput.class)
        .mapper(ParserMapper.class)
        .maxMapAttempts(3)
        .maxMapTaskFailures(100)
        .speculativeExecution(true)
        .numReducers(0)
        .outputFormat(ParserOutputFormat.class)
        .minSplitSize(134217728*4)
        .build();
      
      
        jobConf.set("fs.default.name", S3N_BUCKET_PREFIX);
        
        InputSplit[] splits = jobConf.getInputFormat().getSplits(jobConf, jobConf.getNumMapTasks());
        
        for (InputSplit split : splits) { 
        	System.out.println(split.toString());
        }
    }
  }
  
  private static void parse(FileSystem fs,Configuration conf,ImmutableList<Path> paths)throws IOException { 
    LOG.info("Need to Parse:" + paths.toString());
    // create output path 
    long segmentId = System.currentTimeMillis();
    Path outputPath = new Path(S3N_BUCKET_PREFIX + SEGMENTS_PATH+Long.toString(segmentId));
    LOG.info("Starting Map-Reduce Job. SegmentId:" + segmentId + " OutputPath:" + outputPath);
    // run job...
    JobConf jobConf = new JobBuilder("parse job",conf)
      
      .inputs(paths)
      .inputFormat(SequenceFileInputFormat.class)
      .keyValue(Text.class, ParseOutput.class)
      .mapper(ParserMapper.class)
      .maxMapAttempts(3)
      .maxMapTaskFailures(100)
      .speculativeExecution(true)
      .numReducers(0)
      .outputFormat(ParserOutputFormat.class)
      .output(outputPath)
      .minSplitSize(134217728*4)
      .build();
    
    
    jobConf.set("fs.default.name", S3N_BUCKET_PREFIX);
    jobConf.setInt("mapred.task.timeout", 20 * 60 * 1000);
    jobConf.setLong("cc.segmet.id", segmentId);
    jobConf.setOutputCommitter(OutputCommitter.class);
    
    JobClient.runJob(jobConf);
    
    LOG.info("Job Finished. Writing Segments Manifest File");
    writeSegmentManifestFile(fs,segmentId,paths);
  }
  
  private static List<Path> scanForCompletedSegments(FileSystem fs) throws IOException { 
    ImmutableList.Builder<Path> pathListBuilder = new ImmutableList.Builder<Path>();
    
    for (FileStatus fileStatus : fs.globStatus(new Path(VALID_SEGMENTS_PATH+"[0-9]*"))) { 
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
  
  private static void writeSegmentManifestFile(FileSystem fs,long segmentTimestamp,List<Path> logsInSegment) throws IOException {
    LOG.info("Writing Segment Manifest for Segment: " + segmentTimestamp + " itemCount:" + logsInSegment.size());
    ImmutableList.Builder<String> listBuilder = new ImmutableList.Builder<String>();
    for (Path logPath : logsInSegment) { 
      listBuilder.add(logPath.toString().substring(S3N_BUCKET_PREFIX.length()));
    }
    listToTextFile(listBuilder.build(), fs, new Path(VALID_SEGMENTS_PATH+Long.toString(segmentTimestamp)+"/"+SEGMENT_MANIFEST_FILE));
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
}
