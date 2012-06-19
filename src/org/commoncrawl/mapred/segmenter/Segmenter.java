/**
 * Copyright 2008 - CommonCrawl Foundation
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

package org.commoncrawl.mapred.segmenter;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.mapred.SegmentGeneratorBundleKey;
import org.commoncrawl.mapred.SegmentGeneratorItemBundle;
import org.commoncrawl.util.CCStringUtils;

public class Segmenter {
  static final Log LOG = LogFactory.getLog(Segmenter.class);
  
  public static final int NUM_BUCKETS_PER_CRAWLER = 8;
  public static final int      SEGMENT_SIZE_MIN = 10000;
  public static final int      SEGMENT_SIZE_MAX= 500000;
  public  static final int      SEGMENT_URLS_PER_HOST = 200;  
  
  public  static boolean generateCrawlSegments(long timestamp,String[] crawlerArray,Path bundleInputPath,Path finalOutputPath) { 
    try { 

      FileSystem fs = CrawlEnvironment.getDefaultFileSystem();
      Configuration conf = CrawlEnvironment.getHadoopConfig();

      final Path tempOutputDir 
        = new Path(CrawlEnvironment.getHadoopConfig().get("mapred.temp.dir", ".") +  System.currentTimeMillis());      

      JobConf job = new JobConf(conf);

      // compute crawlers string ... 
      String crawlers = new String();

      for (int i=0;i<crawlerArray.length;++i) { 
        if (i != 0)
          crawlers += ",";
        crawlers += crawlerArray[i];
      }

      LOG.info("Segment Generator:  crawlers:" + crawlers);

      job.set(CrawlEnvironment.PROPERTY_CRAWLERS, crawlers);
      LOG.info("Crawler Count:" + crawlerArray.length);
      job.setInt(CrawlEnvironment.PROPERTY_NUM_CRAWLERS, crawlerArray.length);
      LOG.info("Num Buckets Per Crawler:" + NUM_BUCKETS_PER_CRAWLER);
      job.setInt(CrawlEnvironment.PROPERTY_NUM_BUCKETS_PER_CRAWLER, NUM_BUCKETS_PER_CRAWLER);
      job.setJobName("Generate Segments");

      for (FileStatus candidate : fs.globStatus(new Path(bundleInputPath,"part-*"))) { 
        LOG.info("Adding File:" + candidate.getPath());
        FileInputFormat.addInputPath(job,candidate.getPath());
      }
      


      // multi file merger 
      job.setInputFormat(SequenceFileInputFormat.class);
      job.setMapOutputKeyClass(SegmentGeneratorBundleKey.class);
      job.setMapOutputValueClass(SegmentGeneratorItemBundle.class);
      job.setMapperClass(IdentityMapper.class);
      job.setReducerClass(SegmenterReducer.class);
      job.setPartitionerClass(BundleKeyPartitioner.class);
      job.setOutputKeyComparatorClass(BundleKeyComparator.class);
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(NullWritable.class);
      job.setOutputFormat(SequenceFileOutputFormat.class);
      FileOutputFormat.setOutputPath(job,tempOutputDir);
      job.setNumTasksToExecutePerJvm(1000);
      job.setNumReduceTasks(crawlerArray.length * NUM_BUCKETS_PER_CRAWLER);

      LOG.info("Running  Segmenter OutputDir:" + tempOutputDir);
      JobClient.runJob(job);
      LOG.info("Finished Running Segmenter OutputDir:" + tempOutputDir + " Final Output Dir:" + finalOutputPath);
      
      fs.rename(tempOutputDir, finalOutputPath);
      
      return true;
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      return false;
    }       
  }  
}
