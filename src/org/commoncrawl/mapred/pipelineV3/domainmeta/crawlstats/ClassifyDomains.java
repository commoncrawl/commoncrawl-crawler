package org.commoncrawl.mapred.pipelineV3.domainmeta.crawlstats;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.util.JSONUtils;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.TextBytes;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class ClassifyDomains extends CrawlPipelineStep {

  
  public static final String OUTPUT_DIR_NAME = "classifyDomains";

  private static final Log LOG = LogFactory.getLog(ClassifyDomains.class);

  public ClassifyDomains(CrawlPipelineTask task) {
    super(task, "Classify Root Domains", OUTPUT_DIR_NAME);
  }
  
  
  @Override
  public Log getLogger() {
    return LOG;
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {
    JobConf job = new JobBuilder("Classify Domains", new Configuration())
    
    .input(getOutputDirForStep(CollectSubDomainStatsStep.class))
    .inputIsSeqFile()
    .keyValue(TextBytes.class, TextBytes.class)
    .output(outputPathLocation)
    .outputIsSeqFile()
    .numReducers(8)
    .mapper(SimpleClassifier.class)
    .compressor(CompressionType.BLOCK, SnappyCodec.class)
    .build();
  
    JobClient.runJob(job);
  }
  
  public static class SimpleClassifier implements Mapper<TextBytes,TextBytes,TextBytes,TextBytes> {

    @Override
    public void configure(JobConf job) {
      
    }

    @Override
    public void close() throws IOException {
      
    }
    
    JsonParser parser = new JsonParser();
    
    @Override
    public void map(TextBytes key, TextBytes value,
        OutputCollector<TextBytes, TextBytes> output, Reporter reporter)
        throws IOException {
      
      JsonObject domainMetadata = parser.parse(value.toString()).getAsJsonObject();
      
      boolean isSuperDomain = JSONUtils.safeGetBoolean(domainMetadata,CrawlStatsCommon.ROOTDOMAIN_STATS_IS_SUPERDOMAIN);
      int totalURLS         = JSONUtils.safeGetInteger(domainMetadata,CrawlStatsCommon.ROOTDOMAIN_STATS_TOTAL_URLS);
      int subDomains      = JSONUtils.safeGetInteger(domainMetadata,CrawlStatsCommon.ROOTDOMAIN_STATS_SUBDOMAIN_COUNT);
      double blekkoURLS      = JSONUtils.safeGetDouble(domainMetadata,CrawlStatsCommon.ROOTDOMAIN_STATS_BLEKKO_TOTAL_URLS);
      double blekkoCrawledURLS      = JSONUtils.safeGetDouble(domainMetadata,CrawlStatsCommon.ROOTDOMAIN_STATS_BLEKKO_TOTAL_CRAWL_COUNT);
      double domainRankTotal = JSONUtils.safeGetDouble(domainMetadata, CrawlStatsCommon.ROOTDOMAIN_STATS_DOMAIN_RANK_TOTAL);
      double blekkoRankTotal = JSONUtils.safeGetDouble(domainMetadata, CrawlStatsCommon.ROOTDOMAIN_STATS_BLEKKO_RANK_TOTAL);
      double isSpam  = JSONUtils.safeGetDouble(domainMetadata, CrawlStatsCommon.ROOTDOMAIN_STATS_BLEKKO_IS_SPAM);
      double isPorn  = JSONUtils.safeGetDouble(domainMetadata, CrawlStatsCommon.ROOTDOMAIN_STATS_BLEKKO_IS_PORN);
      
      boolean limitedCrawl = false;
      boolean blackList = false;
      double  relativeDomainRank = 0.0;
      if (blekkoURLS != 0.0) { 
        relativeDomainRank = blekkoCrawledURLS / blekkoURLS;
      }
      if (isSuperDomain) { 
        double dRAvg = domainRankTotal / (double) subDomains;
        double bRAvg = blekkoRankTotal / (double)subDomains;
        if (dRAvg < .001 || bRAvg < .001) { 
          isSuperDomain = false;
        }
      }
      if (!isSuperDomain) { 
        if (isPorn > 0.0 || isSpam > 0.0) { 
          blackList = true;
        }
        else { 
          if (blekkoCrawledURLS == 0.0) { 
            blackList = true;
          }
          else if (blekkoCrawledURLS == 1.0) { 
            limitedCrawl = true;
          }
          else { 
            
          }
        }
      }
      JsonObject objectOut = new JsonObject();
      
      objectOut.addProperty(CrawlStatsCommon.ROOTDOMAIN_CLASSIFY_SUPERDOMAIN,isSuperDomain);
      objectOut.addProperty(CrawlStatsCommon.ROOTDOMAIN_CLASSIFY_BLACKLISTED,blackList);
      if (!blackList) { 
        objectOut.addProperty(CrawlStatsCommon.ROOTDOMAIN_CLASSIFY_LIMITED_CRAWL,limitedCrawl);
        objectOut.addProperty(CrawlStatsCommon.ROOTDOMAIN_CLASSIFY_RELATIVE_RANK,relativeDomainRank);
      }
      
      output.collect(key, new TextBytes(objectOut.toString()));
    } 
  }

}
