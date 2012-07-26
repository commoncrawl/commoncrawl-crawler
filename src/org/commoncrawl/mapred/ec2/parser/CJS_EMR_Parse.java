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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.commoncrawl.protocol.ParseOutput;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.JobBuilder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

// -- CJS --
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * 
 */
public class CJS_EMR_Parse extends Configured implements Tool {
  
  public static final Log LOG = LogFactory.getLog(EC2ParserTask.class);
  
//static final String  S3N_BUCKET_PREFIX            = "s3n://aws-publicdatasets";
  static final String  S3N_BUCKET_PREFIX            = "s3n://commoncrawl-dev";
  static final String  CRAWL_LOG_INTERMEDIATE_PATH  = "/common-crawl/crawl-intermediate/";
  static final String  VALID_SEGMENTS_PATH          = "/common-crawl/parse-output/valid_segments/";
  static final String  SEGMENTS_PATH                = "/common-crawl/parse-output/segment/";
  static final String  SEGMENT_MANIFEST_FILE        = "manfiest.txt";
  static final int     LOGS_PER_ITERATION           = 1000;
  static final Pattern CRAWL_LOG_REG_EXP            = Pattern.compile("CrawlLog_ccc[0-9]{2}-[0-9]{2}_([0-9]*)");
  static final int     MAX_SIMULTANEOUS_JOBS        = 3;

  /**
   * 
   */
  public static void main(String[] args)
    throws Exception {

    Configuration conf = new Configuration();
  //conf.addResource(new Path("/home/hadoop/conf/core-site.xml"));
  //conf.addResource(new Path("/home/hadoop/conf/mapred-site.xml"));
    conf.addResource(new Path("/home/cstephens/commoncrawl-crawler/s3.xml"));

		FileSystem fs = FileSystem.get(new URI(S3N_BUCKET_PREFIX), conf);
		LOG.info("FileSystem is: " + fs.getUri());

		LOG.info("Scanning for candidates at path: " + CRAWL_LOG_INTERMEDIATE_PATH);
		TreeSet<Path> candidateSet = EMRParser.buildCandidateList(fs, new Path(CRAWL_LOG_INTERMEDIATE_PATH));

		LOG.info("Scanning for completed segments"); 
		List<Path> processedLogs = EMRParser.scanForCompletedSegments(fs);

		LOG.info("Found " + processedLogs.size() + " processed logs");

		// remove processed from candidate set ... 
		candidateSet.removeAll(processedLogs);

		// ok we are ready to go .. 
		LOG.info("There are " + candidateSet.size() + " logs in need of parsing");

		if (candidateSet.size() <= 0) {
		  LOG.info("No logs found to parse.  Exiting ...");
      System.exit(2);
    }

  //ArrayList<Path> paths = new ArrayList<Path>(candidateSet);
    ArrayList<Path> paths = new ArrayList<Path>();
    paths.add(candidateSet.first());
    
    int res = 0;

    // should be doing LOGS_PER_ITERATION
    for (Path path : paths) {
      
      conf = new Configuration();

      conf.set("commoncrawl.my.inputPath", path.toString());

		  LOG.info("Running ToolRunner ...");
      res = ToolRunner.run(conf, new EMRParser(), args);

      if (res > 0)
        System.exit(res);
    }

    System.exit(res);
	}
    
  /**
   *  
   */  
  @Override
  public int run(String[] args)
    throws Exception { 

    Configuration conf = this.getConf();

    long segmentId = System.currentTimeMillis();

    String S3N_BUCKET_OUTPUT = "s3n://commoncrawl-dev";

    Path inputPath  = new Path(conf.get("commoncrawl.my.inputPath"));
    Path outputPath = new Path(S3N_BUCKET_PREFIX + SEGMENTS_PATH + Long.toString(segmentId));

    LOG.info("Building MapReduce Job");
    LOG.info("> Segment ID:  " + segmentId);
    LOG.info("> Input Path:  " + inputPath.toString());
    LOG.info("> Output Path: " + outputPath.toString());

    // run job...
    JobConf jobConf = new JobBuilder("EMR Parser - CJS", conf)
      .input(inputPath)
      .inputFormat(SequenceFileInputFormat.class)
      .keyValue(Text.class, ParseOutput.class)
      .mapper(ParserMapper.class)
      .maxMapAttempts(3)
      .maxMapTaskFailures(100)
      .speculativeExecution(true)
      .numReducers(0)
      .outputFormat(ParserOutputFormat.class)
      .output(outputPath)
      .minSplitSize(134217728*2)
      .build();      

    jobConf.set("fs.default.name", S3N_BUCKET_PREFIX);

    jobConf.setOutputCommitter(OutputCommitter.class);
    
    LOG.info("Running MapReduce Job");
    RunningJob job = JobClient.runJob(jobConf);

    // !!! need to write out the manifest file !!!

    if (job.isSuccessful())
      return 0;
    else
      return 1;
  }
  
  private static List<Path> scanForCompletedSegments(FileSystem fs)
    throws IOException { 

    ImmutableList.Builder<Path> pathListBuilder = new ImmutableList.Builder<Path>();
    
    for (FileStatus fileStatus : fs.globStatus(new Path(VALID_SEGMENTS_PATH+"[0-9]*"))) { 
      pathListBuilder.addAll(scanSegmentManifestFile(fs,fileStatus.getPath()));
    }

    return pathListBuilder.build();
  }
  
  private static List<Path> scanSegmentManifestFile(FileSystem fs, Path segmentPath)
    throws IOException {

    LOG.info("Scanning Segment Manifest for segment at path: " + segmentPath);

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
  private static TreeSet<Path> buildCandidateList(FileSystem fs,Path logFilePath)
    throws IOException {
    
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
    
    LOG.info("Running glob for 'CrawlLog*' at: " + logFilePath);

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
