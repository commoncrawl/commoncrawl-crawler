package org.commoncrawl.mapred.pipelineV3.domainmeta.crawlstats;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.util.JSONUtils;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.TextBytes;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class MergeNewDomainStatsStep extends CrawlPipelineStep {

  private static final Log LOG = LogFactory.getLog(MergeNewDomainStatsStep.class);
  
  public static final String OUTPUT_DIR_NAME = "domainStatsMerged";
  
  public static final String SUPER_DOMAIN_FILE_PATH = "super-domain-list";


  public MergeNewDomainStatsStep(CrawlPipelineTask task) {
    super(task, "Merge Domain Stats", OUTPUT_DIR_NAME);
  }
  
  @Override
  public Log getLogger() {
    return LOG;
  }

  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }    

  @Override
  public void runStep(Path outputPathLocation) throws IOException {
    
    ImmutableList<Path> inputs 
    = new ImmutableList.Builder<Path>()
      .add(getOutputDirForStep(NewCrawlStatsCollectorStep.class))
      .build();

    JobConf job = new JobBuilder(getDescription(), getConf())

    .inputs(inputs).inputIsSeqFile()
    .keyValue(TextBytes.class, TextBytes.class)
    .reducer(MergingReducer.class,false)
    .numReducers(CrawlEnvironment.NUM_DB_SHARDS)
    .outputIsSeqFile()
    .output(outputPathLocation)
    .compressor(CompressionType.BLOCK, SnappyCodec.class)
    .maxMapTaskFailures(5)
    .build();

    JobClient.runJob(job);
    
  }
  
  public static class MergingReducer implements Reducer<TextBytes,TextBytes,TextBytes,TextBytes> {

    @Override
    public void configure(JobConf job) {
      
    }

    @Override
    public void close() throws IOException {
    }

    JsonParser parser = new JsonParser();
    HashSet<String> ips = new HashSet<String>();
    TextBytes valueText = new TextBytes();
    
    @Override
    public void reduce(TextBytes key, Iterator<TextBytes> values,OutputCollector<TextBytes, TextBytes> output, Reporter reporter)throws IOException {
      TextBytes firstValue = Iterators.getNext(values, null);
      JsonObject firstObject = parser.parse(firstValue.toString()).getAsJsonObject();
      ips.clear();
      JSONUtils.safeJsonArrayToStringCollection(firstObject, CrawlStatsCommon.CRAWLSTATS_IPS,ips);
      
      int mergedObjectCount = 0;
      
      while (values.hasNext()) { 
        JsonObject nextObject = parser.parse(values.next().toString()).getAsJsonObject();
        
//        public static final String CRAWLSTATS_IPS = "ips";
        
        JSONUtils.mergeCounters(firstObject,nextObject,CrawlStatsCommon.CRAWLSTATS_URL_COUNT);
        JSONUtils.mergeCounters(firstObject,nextObject,CrawlStatsCommon.CRAWLSTATS_ATTEMPTED_COUNT);
        JSONUtils.mergeCounters(firstObject,nextObject,CrawlStatsCommon.CRAWLSTATS_CRAWLED_COUNT);
        JSONUtils.mergeCounters(firstObject,nextObject,CrawlStatsCommon.CRAWLSTATS_REDIRECTED_COUNT);
        JSONUtils.mergeCounters(firstObject,nextObject,CrawlStatsCommon.CRAWLSTATS_REDIRECTED_OUT_COUNT);
        JSONUtils.mergeCounters(firstObject,nextObject,CrawlStatsCommon.CRAWLSTATS_WWW_TO_NON_WWW_REDIRECT);
        JSONUtils.mergeCounters(firstObject,nextObject,CrawlStatsCommon.CRAWLSTATS_NON_WWW_TO_WWW_REDIRECT);
        JSONUtils.mergeCounters(firstObject,nextObject,CrawlStatsCommon.CRAWLSTATS_EXTERNALLY_LINKED_URLS);
        JSONUtils.mergeCounters(firstObject,nextObject,CrawlStatsCommon.CRAWLSTATS_EXTERNALLY_LINKED_NOT_CRAWLED_URLS);
        JSONUtils.mergeCounters(firstObject,nextObject,CrawlStatsCommon.CRAWLSTATS_BLEKKO_URL);
        JSONUtils.mergeCounters(firstObject,nextObject,CrawlStatsCommon.CRAWLSTATS_BLEKKO_CRAWLED_COUNT);
        JSONUtils.mergeCounters(firstObject,nextObject,CrawlStatsCommon.CRAWLSTATS_BLEKKO_AND_CC_CRAWLED_COUNT);
        JSONUtils.mergeCounters(firstObject,nextObject,CrawlStatsCommon.CRAWLSTATS_BLEKKO_URL_NOT_IN_CC);
        JSONUtils.mergeCounters(firstObject,nextObject,CrawlStatsCommon.CRAWLSTATS_BLEKKO_URL_HAD_GT_1_RANK);
        
        JSONUtils.safeJsonArrayToStringCollection(nextObject, CrawlStatsCommon.CRAWLSTATS_IPS,ips);
        mergedObjectCount++;
      }
      
      if (mergedObjectCount != 0 && ips.size() != 0) { 
        JSONUtils.stringCollectionToJsonArray(firstObject,CrawlStatsCommon.CRAWLSTATS_IPS,ips);
      }
      
      if (mergedObjectCount != 0) {
        valueText.set(firstObject.toString());
        output.collect(key, valueText);
      }
      else { 
        output.collect(key, firstValue);
      }
    } 
    
  }

}
