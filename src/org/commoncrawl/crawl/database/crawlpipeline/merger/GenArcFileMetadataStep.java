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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.nutch.util.NutchJob;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.crawl.database.crawlpipeline.CrawlPipelineStep;
import org.commoncrawl.crawl.database.crawlpipeline.CrawlPipelineTask;
import org.commoncrawl.crawl.database.crawlpipeline.JobConfig;
import org.commoncrawl.protocol.CrawlDatumAndMetadata;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.internal.NodeAffinityMaskBuilder;

/** 
 * Collect ARCFile Generator Metadata
 * 
 * @author rana
 *
 */
public class GenArcFileMetadataStep extends CrawlPipelineStep {

	public GenArcFileMetadataStep(CrawlPipelineTask task, String name) throws IOException {
    super(task, name);
  }

  static final Log LOG = LogFactory.getLog(GenArcFileMetadataStep.class);
	
	@Override
	public Log getLogger() {
	  return LOG;
	}
	
	@Override
	public Path[] getDependencies() throws IOException {
	  return new Path[] {
	      new Path("crawl/arc_generator/arcfiles/", Long.toString(getTaskIdentityId()))
	  };
	}
	public void reshardArcFileMetadata(Path outputPath)throws IOException {

		final Path arcFileMetadataInputPath = new Path(
				"crawl/arc_generator/arcfiles/" + getTaskIdentityId());
		
		JobConf job = 
		    
		    new JobConfig(getDescription()) 
		      
		      .input(arcFileMetadataInputPath)
		      .inputSeqFile()
		      .output(outputPath)
		      .outputSeqFile()
		      .numReducers(CrawlEnvironment.NUM_DB_SHARDS)
		      .setAffinity(getTaskIdentityPath())
		      
		      .build();
		
			
		JobClient.runJob(job);
	}


  @Override
  public String getPipelineStepName() {
    return CrawlDBMergerTask.ARC_METADATA_STEP_NAME;
  }


  @Override
  public void run(Path outputPathLocation) throws IOException {
    reshardArcFileMetadata(outputPathLocation);
  }
}
