package org.commoncrawl.mapred.pipelineV3.domainmeta.crawlstats;

import static org.commoncrawl.mapred.pipelineV3.domainmeta.crawlstats.CrawlStatsCommon.ROOTDOMAIN_STATS_IS_SUPERDOMAIN;
import static org.commoncrawl.mapred.pipelineV3.domainmeta.crawlstats.CrawlStatsCommon.ROOTDOMAIN_STATS_SUBDOMAIN_COUNT;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.TextBytes;

import com.google.common.collect.Iterators;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import static org.commoncrawl.mapred.pipelineV3.domainmeta.crawlstats.CrawlStatsCommon.*;

public class WriteAggregatedDomainStatsFileStep extends CrawlPipelineStep {

  public static final String OUTPUT_DIR_NAME = "AggegateDomainStatsFile";

  private static final Log LOG = LogFactory.getLog(WriteAggregatedDomainStatsFileStep.class);

  public WriteAggregatedDomainStatsFileStep(CrawlPipelineTask task) {
    super(task, "Write Aggregated SubDomain Stats File", OUTPUT_DIR_NAME);
  }
  
  @Override
  public Log getLogger() {
    return LOG;
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {
    JobConf job = new JobBuilder("Write Stats File", new Configuration())
    
      .input(getOutputDirForStep(CollectSubDomainStatsStep.class))
      .inputIsSeqFile()
      .keyValue(TextBytes.class, TextBytes.class)
      .output(outputPathLocation)
      .outputFormat(TextOutputFormat.class)
      .numReducers(8)
      .reducer(JSONtoTabReducer.class, false)
      .compressType(CompressionType.NONE)
      .build();
    
    JobClient.runJob(job);
  }
  
  
  public static class JSONtoTabReducer implements Reducer<TextBytes,TextBytes,TextBytes,TextBytes> {

    @Override
    public void configure(JobConf job) {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void close() throws IOException {
      // TODO Auto-generated method stub
      
    }

    JsonParser parser = new JsonParser();
    
    @Override
    public void reduce(TextBytes key, Iterator<TextBytes> values,
        OutputCollector<TextBytes, TextBytes> output, Reporter reporter)
        throws IOException {
      JsonObject stats = parser.parse(Iterators.getNext(values, null).toString()).getAsJsonObject();
      
      StringBuffer sb = new StringBuffer();

      sb.append(stats.get(ROOTDOMAIN_STATS_IS_SUPERDOMAIN).getAsBoolean()+"\t"); // 2
      sb.append(stats.get(ROOTDOMAIN_STATS_TOTAL_URLS).getAsInt()+"\t"); // 3
      sb.append(stats.get(ROOTDOMAIN_STATS_SUBDOMAIN_COUNT).getAsInt()+"\t"); // 4

      sb.append(stats.get(ROOTDOMAIN_STATS_MEAN_URLS_PER_DOMAIN).getAsDouble()+"\t"); // 5
      sb.append(stats.get(ROOTDOMAIN_STATS_MAX_URLS_IN_A_DOMAIN).getAsDouble()+"\t"); // 6
      sb.append(stats.get(ROOTDOMAIN_STATS_URLS_PER_DOMAIN_STDDEV).getAsDouble()+"\t"); // 7
      sb.append(stats.get(ROOTDOMAIN_STATS_BLEKKO_TOTAL_URLS).getAsDouble()+"\t"); // 8
      sb.append(stats.get(ROOTDOMAIN_STATS_BLEKKO_MEAN_URLS_PER_DOMAIN).getAsDouble()+"\t"); // 9
      sb.append(stats.get(ROOTDOMAIN_STATS_BLEKKO_RANK_MEAN).getAsDouble()+"\t"); // 10
      sb.append(stats.get(ROOTDOMAIN_STATS_BLEKKO_RANK_MAX).getAsDouble()+"\t"); // 11
      sb.append(stats.get(ROOTDOMAIN_STATS_BLEKKO_RANK_TOTAL).getAsDouble()+"\t"); // 12
      sb.append(stats.get(ROOTDOMAIN_STATS_DOMAIN_RANK_MEAN).getAsDouble()+"\t"); // 13
      sb.append(stats.get(ROOTDOMAIN_STATS_DOMAIN_RANK_MAX).getAsDouble()+"\t");// 14
      sb.append(stats.get(ROOTDOMAIN_STATS_DOMAIN_RANK_TOTAL).getAsDouble()+"\t"); // 15
      sb.append(stats.get(ROOTDOMAIN_STATS_DOMAIN_RANK_STDDEV).getAsDouble()+"\t"); // 16
      sb.append(stats.get(ROOTDOMAIN_STATS_BLEKKO_IS_PORN).getAsDouble()+"\t");
      sb.append(stats.get(ROOTDOMAIN_STATS_BLEKKO_IS_SPAM).getAsDouble()+"\t");
      sb.append(stats.get(ROOTDOMAIN_STATS_BLEKKO_MEAN_CRAWL_COUNT).getAsDouble()+"\t");
      sb.append(stats.get(ROOTDOMAIN_STATS_BLEKKO_TOTAL_CRAWL_COUNT).getAsDouble()+"\t");//20
      sb.append(stats.get(ROOTDOMAIN_STATS_BLEKKO_MEAN_PR_COUNT).getAsDouble()+"\t");// 21
      sb.append(stats.get(ROOTDOMAIN_STATS_BLEKKO_MAX_PR_COUNT).getAsDouble()+"\t");//22
      sb.append(stats.get(ROOTDOMAIN_STATS_BLEKKO_TOTAL_PR_COUNT).getAsDouble()+"\t");//23
      sb.append(stats.get(ROOTDOMAIN_STATS_MEAN_EXT_LINKED_URLS).getAsDouble()+"\t");//24
      sb.append(stats.get(ROOTDOMAIN_STATS_MAX_EXT_LINKED_URLS).getAsDouble()+"\t");//25
      sb.append(stats.get(ROOTDOMAIN_STATS_TOTAL_EXT_LINKED_URLS).getAsDouble()+"\t");//26
      sb.append(stats.get(ROOTDOMAIN_STATS_EXT_LINKED_URLS_STDDEV).getAsDouble()+"\t");//27
      
      output.collect(key, new TextBytes(sb.toString()));
      
      
    } 
    
  }

}
