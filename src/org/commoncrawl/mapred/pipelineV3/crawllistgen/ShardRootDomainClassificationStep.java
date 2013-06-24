package org.commoncrawl.mapred.pipelineV3.crawllistgen;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.mapred.ec2.postprocess.crawldb.CrawlDBKey;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.crawlstats.ClassifyDomains;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLUtils;

public class ShardRootDomainClassificationStep extends CrawlPipelineStep{

  public static final String OUTPUT_DIR_NAME = "rootClassification";

  private static final Log LOG = LogFactory.getLog(ShardRootDomainClassificationStep.class);

  public ShardRootDomainClassificationStep(CrawlPipelineTask task) {
    super(task, "Shard Root Classification", OUTPUT_DIR_NAME);
  }

  @Override
  public Log getLogger() {
    return LOG;
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {
    JobConf job = new JobBuilder("Shard Classification Data", new Configuration())
    
    .input(getRootTask().getOutputDirForStep(ClassifyDomains.class))
    .inputIsSeqFile()
    .mapper(KeyTransformer.class)
    .keyValue(TextBytes.class, TextBytes.class)
    .numReducers(8)
    .output(outputPathLocation)
    .sort(CrawlDBKey.LinkKeyComparator.class)
    .outputIsSeqFile()
    .build();
    
    JobClient.runJob(job);
  }
  
  public static class KeyTransformer implements Mapper<TextBytes,TextBytes,TextBytes,TextBytes> {

    @Override
    public void configure(JobConf job) {
      
    }

    @Override
    public void close() throws IOException {
      // TODO Auto-generated method stub
      
    }

    enum Counters { 
      BAD_HOSTNAME
    }
    
    
    @Override
    public void map(TextBytes key, TextBytes value,OutputCollector<TextBytes, TextBytes> output, Reporter reporter)throws IOException {
      URLFPV2 fp = URLUtils.getURLFPV2FromHost(key.toString());
      if (fp == null) { 
        reporter.incrCounter(Counters.BAD_HOSTNAME, 1);
      }
      else { 
        fp.setDomainHash(Long.MIN_VALUE);
        fp.setUrlHash(Long.MIN_VALUE);
      
        TextBytes outputKey = CrawlDBKey.generateKey(fp, CrawlDBKey.Type.KEY_TYPE_ROOTDOMAIN_METADATA_RECORD, 0L);
      
        output.collect(outputKey,value);
      }
    } 
    
  }

}
