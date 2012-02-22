
/**
 * Copyright 2008 - CommonCrawl Foundation
 * 
 * CommonCrawl licenses this file to you under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.commoncrawl.crawl.database.crawlpipeline.merger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.util.NutchJob;
import org.commoncrawl.async.Timer;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.crawl.database.CrawlDBSegment;
import org.commoncrawl.crawl.database.CrawlDBServer;
import org.commoncrawl.crawl.database.crawlpipeline.CrawlDBUtils;
import org.commoncrawl.crawl.database.crawlpipeline.CrawlPipelineStep;
import org.commoncrawl.crawl.database.crawlpipeline.CrawlPipelineTask;
import org.commoncrawl.crawl.database.crawlpipeline.JobConfig;
import org.commoncrawl.crawl.database.crawlpipeline.merger.mappers.CrawlDatumTransformer;
import org.commoncrawl.crawl.database.crawlpipeline.merger.reducers.CrawlDatumCombiner;
import org.commoncrawl.db.RecordStore;
import org.commoncrawl.db.RecordStore.RecordStoreException;
import org.commoncrawl.protocol.CrawlDatumAndMetadata;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.internal.NodeAffinityMaskBuilder;
import org.commoncrawl.util.shared.CCStringUtils;

import com.google.common.collect.ImmutableList;

/** 
 * Generate Parse Segment Metadata 
 * 
 * @author rana
 *
 */
public class GenSegmentMetadataStep extends CrawlPipelineStep {

  public GenSegmentMetadataStep(CrawlPipelineTask task, String name) throws IOException {
    super(task, name);
  }


  static final Log LOG = LogFactory.getLog(GenSegmentMetadataStep.class);
  
  @Override
  public Log getLogger() {
    return LOG;
  }
  	
  ArrayList<Path> getParsedButNotMergedSegments()throws IOException { 
    FileSystem fs = FileSystem.get(CrawlEnvironment
        .getHadoopConfig());
    
    Path crawlDBSegmentBase = new Path(CrawlEnvironment.HDFS_ParseCandidateSegmentsDirectory);
    
    FileStatus candidates[] = fs.globStatus(new Path(crawlDBSegmentBase,"[0-9]*"));
    
    ArrayList<Path> paths = new ArrayList<Path>();
    for (FileStatus candidate : candidates) {
      int segmentId = Integer.parseInt(candidate.getPath().getName());
      //TODO:HACK
      if (segmentId >= 8944) { 
        if (CrawlDBUtils.isSegmentInState(fs, crawlDBSegmentBase, segmentId,CrawlDBSegment.Status.PARSED)) { 
          if (!CrawlDBUtils.isSegmentInState(fs, crawlDBSegmentBase,segmentId, CrawlDBSegment.Status.MERGED)) { 
            paths.add(candidate.getPath());
          }
        }
      }
    }
    return paths;
  }
  
  @Override
  public Path[] getDependencies() throws IOException {
    return getParsedButNotMergedSegments().toArray(new Path[0]);
  }
  
  public static final int SEGMENTS_PER_PASS = 75;
  public static final String SEGMENT_IDS_FILE = "Segments";
  
  void writeSegmentIdsToFile(ArrayList<Path> segments,Path segmentIdsFile)throws IOException {
    FSDataOutputStream outputFile = getFileSystem().create(segmentIdsFile);
    if (outputFile != null) { 
      try { 
        for (Path segmentId : segments) { 
          outputFile.write((segmentId.getName() +"\n").getBytes());
        }
        outputFile.flush();
        outputFile.close();
      }
      finally { 
        outputFile.close();
      }
    }
  }
  
  void generateSegmentMetadata(Path outputPath) throws IOException {
    
    // get list of parsed but not merged segments ... 
    ArrayList<Path> parsedButNotMergedSegments = getParsedButNotMergedSegments();

    // if something to do ... 
    if (parsedButNotMergedSegments.size() != 0) { 
      // write the list to intermediate dir ...
      writeSegmentIdsToFile(parsedButNotMergedSegments,new Path(outputPath,SEGMENT_IDS_FILE));
      
      // start iteration
      int iterationNumber = 0;
      
      FileSystem fs = getFileSystem();
      
      // make output dir ... 
      fs.mkdirs(outputPath);
      
  	  while (parsedButNotMergedSegments.size() != 0) {
  	    ArrayList<Path> segmentsThisPass = new ArrayList<Path>();
  	    
        while (parsedButNotMergedSegments.size() != 0
            && segmentsThisPass.size() < 75) {
          segmentsThisPass.add(parsedButNotMergedSegments.remove(0));
        }
  	      
        ImmutableList.Builder<Path> pathsListBuilder = new ImmutableList.Builder<Path>();
      
        for (Path segmentDir : segmentsThisPass) {
          LOG.info("Adding Segment:" + segmentDir);
          Path fetch = new Path(segmentDir, CrawlDatum.FETCH_DIR_NAME);
          Path parse = new Path(segmentDir, CrawlDatum.PARSE_DIR_NAME);
          pathsListBuilder.add(fetch);
          pathsListBuilder.add(parse);
        }
        
        // create output path for this iteration 
        Path iterationOutput = new Path(outputPath,Integer.toString(iterationNumber++));
        
        JobConf job 

          = new JobConfig(getDescription())
            
            .inputSeqFile()
            .inputs(pathsListBuilder.build())
            .mapper(CrawlDatumTransformer.class)
            .mapperKeyValue(URLFPV2.class,CrawlDatumAndMetadata.class)
            .reducer(CrawlDatumCombiner.class, true)
            .outputKeyValue(URLFPV2.class, CrawlDatumAndMetadata.class)
            .numReducers(CrawlEnvironment.NUM_DB_SHARDS)
            .outputSeqFile()
            .output(iterationOutput)
  
            .build();
  	      
        // set node affinity ...
        String affinityMask = NodeAffinityMaskBuilder.buildNodeAffinityMask(
            FileSystem.get(job), getTaskIdentityPath(), null);
        NodeAffinityMaskBuilder.setNodeAffinityMask(job, affinityMask);
        
        // run job
        JobClient.runJob(job);
  	  }
    }
  }


  @Override
  public String getPipelineStepName() {
    return CrawlDBMergerTask.SEGMENT_METADATA_STEP_NAME;
  }

  @Override
  public void run(Path outputPathLocation) throws IOException {
    generateSegmentMetadata(outputPathLocation);
  }	
}
