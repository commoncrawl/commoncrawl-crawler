package org.commoncrawl.mapred.pipelineV3.crawllistgen;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.mapred.ec2.postprocess.crawldb.CrawlDBKey;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.mapred.pipelineV3.crawllistgen.NewPartitionUrlsStep.RankAndFilterMapper.Counters;
import org.commoncrawl.mapred.pipelineV3.domainmeta.crawlstats.CrawlStatsCommon;
import org.commoncrawl.mapred.pipelineV3.domainmeta.crawlstats.JoinDomainMetadataStep;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.JSONUtils;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLUtils;

import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class ShardSubDomainMetadataStep extends CrawlPipelineStep {

  public static final String OUTPUT_DIR_NAME = "subDomainMetadata";

  private static final Log LOG = LogFactory.getLog(ShardSubDomainMetadataStep.class);

  public ShardSubDomainMetadataStep(CrawlPipelineTask task) {
    super(task, "Shard Subdomain Metadata", OUTPUT_DIR_NAME);
  }

  @Override
  public Log getLogger() {
    return LOG;
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {
    
    Path joinDataPath = getRootTask().getOutputDirForStep(JoinDomainMetadataStep.class);
    
    ArrayList<Path> partFiles = Lists.newArrayList();
    
    FileSystem fs = FileSystem.get(joinDataPath.toUri(),new Configuration());
    
    FileStatus files[] = fs.globStatus(new Path(joinDataPath,"part-*"));
    
    for (FileStatus file:files) 
      partFiles.add(file.getPath());
    
    JobConf job = new JobBuilder("Shard Subdomain Metadata", new Configuration())
    
    
    .inputs(partFiles)
    .inputIsSeqFile()
    .mapper(SubDomainMetadataMapper.class)
    .keyValue(TextBytes.class, BooleanWritable.class)
    .numReducers(16)
    .output(outputPathLocation)
    .sort(CrawlDBKey.LinkKeyComparator.class)
    .outputIsSeqFile()
    .build();
    
    
    JobClient.runJob(job);
  }
  
  public static class SubDomainMetadataMapper implements Mapper<TextBytes,TextBytes,TextBytes,BooleanWritable> {

    @Override
    public void configure(JobConf job) {
      
    }

    @Override
    public void close() throws IOException {
      // TODO Auto-generated method stub
      
    }

    enum Counters { 
      BAD_HOSTNAME, USING_WWW_PREFIX, COULD_NOT_RESOLVE_WWW_PREFIX
    }
    
    
    JsonParser parser = new JsonParser();
    
    @Override
    public void map(TextBytes key, TextBytes value,OutputCollector<TextBytes, BooleanWritable> output, Reporter reporter)throws IOException {
      URLFPV2 fp = URLUtils.getURLFPV2FromHost(key.toString());
      if (fp == null) { 
        reporter.incrCounter(Counters.BAD_HOSTNAME, 1);
      }
      else { 
        fp.setUrlHash(Long.MIN_VALUE);
        
        TextBytes outputKey = CrawlDBKey.generateKey(fp, CrawlDBKey.Type.KEY_TYPE_SUBDOMAIN_METADATA_RECORD, 0L);
        JsonObject subDomainMetadata = parser.parse(value.toString()).getAsJsonObject();
        // json.addProperty("domain", key.toString());
        
        JsonObject crawlStats = subDomainMetadata.getAsJsonObject(CrawlStatsCommon.JOINEDMETDATA_PROPERTY_CRAWLSTATS);
        boolean prefixWithWWW = false;
        
        if (crawlStats != null) { 
          int urlCount = JSONUtils.safeGetInteger(crawlStats, CrawlStatsCommon.CRAWLSTATS_URL_COUNT,0);
          int WWWCount = JSONUtils.safeGetInteger(crawlStats, CrawlStatsCommon.CRAWLSTATS_NON_WWW_TO_WWW_REDIRECT,0);
          int nonWWWCount = JSONUtils.safeGetInteger(crawlStats, CrawlStatsCommon.CRAWLSTATS_WWW_TO_NON_WWW_REDIRECT,0);
          if (WWWCount > nonWWWCount) {
            if ((double)WWWCount / (double)(WWWCount + nonWWWCount) >= .30) {
              reporter.incrCounter(Counters.USING_WWW_PREFIX, 1);
              prefixWithWWW = true;
            }
            else { 
              reporter.incrCounter(Counters.COULD_NOT_RESOLVE_WWW_PREFIX,1);
              LOG.error("DName:" + key.toString()
                  + " WWWCount:"+ WWWCount + " NonWWWCount:" + nonWWWCount + " URLCount:"+ urlCount);
            }
          }
        }
        output.collect(outputKey,new BooleanWritable(prefixWithWWW));
      }
    }

    
  }
}
