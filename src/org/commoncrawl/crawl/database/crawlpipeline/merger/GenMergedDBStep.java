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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.nutch.util.NutchJob;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.crawl.database.crawlpipeline.CrawlPipelineStep;
import org.commoncrawl.crawl.database.crawlpipeline.CrawlPipelineTask;
import org.commoncrawl.crawl.database.crawlpipeline.JobConfig;
import org.commoncrawl.crawl.database.crawlpipeline.merger.reducers.MergeReducer;
import org.commoncrawl.protocol.CrawlDatumAndMetadata;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.internal.NodeAffinityMaskBuilder;
import org.commoncrawl.util.internal.MultiFileMergeUtils.MultiFileMergeInputFormat;
import org.commoncrawl.util.internal.MultiFileMergeUtils.MultiFileMergePartitioner;

import com.google.common.collect.ImmutableList;

/** 
 * Generate a merged database 
 * 
 * @author rana
 *
 */
public class GenMergedDBStep extends CrawlPipelineStep {

	public GenMergedDBStep(CrawlPipelineTask task, String name) throws IOException {
    super(task, name);
  }

  static final Log LOG = LogFactory.getLog(GenMergedDBStep.class);

	@Override
	public Log getLogger() {
	  return LOG;
	}
	
	@Override
	public Path[] getDependencies() throws IOException {
	  ArrayList<Path> paths = new ArrayList<Path>();

	  // crawl db 
	  paths.add(getTaskIdentityPath());
	  
    paths.add(new Path(
        "crawl/merge/arcfileMetadata"));

    paths.add(new Path("crawl/linkdb/merged"
        + getTaskIdentityId() + "/linkMetadata"));
    
    paths.add(new Path("crawl/inverse_linkdb/merged"
        + getTaskIdentityId() + "/linkMetadata"));
    
    paths.add(new Path("crawl/pageRank/db/"
        + getTaskIdentityId() + "/fpToPR"));
	  
    Path parseSegmentInputBase = getBaseOutputDirForStep(CrawlDBMergerTask.SEGMENT_METADATA_STEP_NAME);
    
    // ok walk children 
    for (FileStatus parseSegmentInput : getFileSystem().globStatus(
          new Path(parseSegmentInputBase,"[0-9]*"))) { 
      paths.add(parseSegmentInput.getPath());
    }
    return paths.toArray(new Path[0]);
	}
	
	public void doMerge(Path outputPath) throws IOException {

		final FileSystem fs = FileSystem
				.get(CrawlEnvironment.getHadoopConfig());

		JobConf job = 
		    new JobConfig(getDescription())
		
		      .inputs(ImmutableList.of(getDependencies()))
		      .inputFormat(MultiFileMergeInputFormat.class)
		      .mapperKeyValue(IntWritable.class, Text.class)
		      .reducer(MergeReducer.class, false)
		      .numReducers(CrawlEnvironment.NUM_DB_SHARDS)
		      .outputKeyValue(URLFPV2.class,CrawlDatumAndMetadata.class)
		      .outputSeqFile()
		      .output(outputPath)
		      .partition(MultiFileMergePartitioner.class)
		      
		      .setAffinity(getTaskIdentityPath())

		      .build();
		
		JobClient.runJob(job);
	}

  @Override
  public String getPipelineStepName() {
    return CrawlDBMergerTask.GEN_MERGED_DB_STEP_NAME;
  }

  @Override
  public void run(Path outputPathLocation) throws IOException {
    
  }

}
