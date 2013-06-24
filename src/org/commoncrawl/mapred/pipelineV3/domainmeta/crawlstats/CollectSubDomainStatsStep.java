package org.commoncrawl.mapred.pipelineV3.domainmeta.crawlstats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.tools.ant.SubBuildListener;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.mapred.ec2.postprocess.crawldb.CrawlDBCommon;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.rank.GenSuperDomainListStep;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.JSONUtils;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.SuperDomainList;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLUtils;


import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import static org.commoncrawl.mapred.pipelineV3.domainmeta.crawlstats.CrawlStatsCommon.*;

public class CollectSubDomainStatsStep extends CrawlPipelineStep {

  public static final String OUTPUT_DIR_NAME = "AggegateDomainStats";
  
  public static final String SUPER_DOMAIN_FILE_PATH = "superDomainFilePath";

  
  private static final Log LOG = LogFactory.getLog(JoinDomainMetadataStep.class);

  public CollectSubDomainStatsStep(CrawlPipelineTask task) {
    super(task, "Aggregate SubDomain Stats", OUTPUT_DIR_NAME);
  }

  
  @Override
  public Log getLogger() {
    return LOG;
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {
    Configuration conf = new Configuration();
    
    ArrayList<Path> paths = new ArrayList<Path>();
    FileSystem fs = FileSystem.get(outputPathLocation.toUri(),conf);
    FileStatus files[] = fs.globStatus(new Path(getOutputDirForStep(JoinDomainMetadataStep.class),"part-*"));
    for (FileStatus file: files) 
      paths.add(file.getPath());
    
    Path superDomainPath = new Path(getRootTask()
        .getOutputDirForStep(GenSuperDomainListStep.class) 
        , "part-00000");

    LOG.info("Super Domain File Path is:" + superDomainPath);
    
    
    JobConf job = new JobBuilder("Collect SubDomain Stats", new Configuration())
    
    .inputIsSeqFile()
    .inputs(paths)
    .keyValue(TextBytes.class, TextBytes.class)
    .mapper(SubDomainStatsMapper.class)
    .reducer(SubDomainStatsReducer.class, false)
    .outputIsSeqFile()
    .compressor(CompressionType.BLOCK, SnappyCodec.class)
    .numReducers(CrawlEnvironment.NUM_DB_SHARDS/2)
    .output(outputPathLocation)
    .set(SUPER_DOMAIN_FILE_PATH, superDomainPath.toString())
    .build();

    
    JobClient.runJob(job);
    
  }
  
  
  public static class SubDomainStatsMapper implements Mapper<TextBytes,TextBytes,TextBytes,TextBytes> {

    @Override
    public void configure(JobConf job) {      
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void map(TextBytes key, TextBytes value,
        OutputCollector<TextBytes, TextBytes> output, Reporter reporter)
        throws IOException {
      
      String domainName = key.toString();
      String rootDomain = URLUtils.extractRootDomainName(domainName);
      if (rootDomain != null) { 
        output.collect(new TextBytes(rootDomain), value);
      }
    } 
  }
  
  public static class SubDomainStatsReducer implements Reducer<TextBytes,TextBytes,TextBytes,TextBytes> {
    Set<Long> superDomainIdSet = null;
    
    @Override
    public void configure(JobConf job) {
      Path superDomainIdFile = new Path(job.get(SUPER_DOMAIN_FILE_PATH));

      try {
        superDomainIdSet = SuperDomainList.loadSuperDomainIdList(job, superDomainIdFile);
      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
        throw new RuntimeException(e);
      }
      
    }

    @Override
    public void close() throws IOException {
    }

    JsonParser parser = new JsonParser();
    
    @Override
    public void reduce(TextBytes key, Iterator<TextBytes> values,
        OutputCollector<TextBytes, TextBytes> output, Reporter reporter)
        throws IOException {
      
      // get root domain fp
      long rootDomainFP = SuperDomainList.domainFingerprintGivenName(key.toString());
      // is super domain
      boolean isSuperDomain = superDomainIdSet.contains(rootDomainFP);

      DescriptiveStatistics ccURLSPerDomain = new DescriptiveStatistics();
      DescriptiveStatistics blekkoURLSPerDomain = new DescriptiveStatistics();
      DescriptiveStatistics blekkoRank    = new DescriptiveStatistics();
      DescriptiveStatistics domainRank    = new DescriptiveStatistics();
      DescriptiveStatistics blekkoisPorn    = new DescriptiveStatistics();
      DescriptiveStatistics blekkoisSpam    = new DescriptiveStatistics();
      DescriptiveStatistics blekkoCrawlCount = new DescriptiveStatistics();
      DescriptiveStatistics blekkoPRCount = new DescriptiveStatistics();
      DescriptiveStatistics externallyLinkedCount = new DescriptiveStatistics();
      
      
      int subDomainCount = 0;
      int hadBelkkoMetadata = 0;
      int totalURLS = 0;
      
      while (values.hasNext()) { 
        JsonObject subDomainStats = parser.parse(values.next().toString()).getAsJsonObject();
        
        if (subDomainStats.has(CrawlStatsCommon.JOINEDMETDATA_PROPERTY_BELKKO)) { 
          hadBelkkoMetadata++;
          
          JsonObject blekkoMetadata = subDomainStats.getAsJsonObject(CrawlStatsCommon.JOINEDMETDATA_PROPERTY_BELKKO);
          
          double rank = JSONUtils.safeGetDouble(blekkoMetadata,CrawlDBCommon.BLEKKO_METADATA_RANK_10);
          int isPorn  = JSONUtils.safeGetInteger(blekkoMetadata, CrawlDBCommon.BLEKKO_METADATA_ISPORN);
          int isSpam  = JSONUtils.safeGetInteger(blekkoMetadata, CrawlDBCommon.BLEKKO_METADATA_ISSPAM);
          if (rank != Double.NaN) { 
            blekkoRank.addValue(rank);
          }
          if (isPorn == 1)
            blekkoisPorn.addValue(1.0);
          if (isSpam == 1)
            blekkoisSpam.addValue(1.0);
        }
        
        if (subDomainStats.has(CrawlStatsCommon.JOINEDMETDATA_PROPERTY_CRAWLSTATS)) {
          
          JsonObject crawlStatsMetadata = subDomainStats.getAsJsonObject(CrawlStatsCommon.JOINEDMETDATA_PROPERTY_CRAWLSTATS);
        
          int blekkoCrawlCountRaw = JSONUtils.safeGetInteger(crawlStatsMetadata, CrawlStatsCommon.CRAWLSTATS_BLEKKO_CRAWLED_COUNT);
          blekkoCrawlCount.addValue((blekkoCrawlCountRaw == -1)? 0.0 : (double)blekkoCrawlCountRaw);
          int blekkoPageRankCount = JSONUtils.safeGetInteger(crawlStatsMetadata, CrawlStatsCommon.CRAWLSTATS_BLEKKO_URL_HAD_GT_1_RANK);
          blekkoPRCount.addValue((blekkoPageRankCount == -1) ? 0.0 : (double) blekkoPageRankCount);
          int externallyLinkedURLS = JSONUtils.safeGetInteger(crawlStatsMetadata, CrawlStatsCommon.CRAWLSTATS_EXTERNALLY_LINKED_URLS);
          externallyLinkedCount.addValue((externallyLinkedURLS == -1) ? 0.0 : (double)externallyLinkedURLS);
          int urlCount = JSONUtils.safeGetInteger(crawlStatsMetadata, CrawlStatsCommon.CRAWLSTATS_URL_COUNT);
          totalURLS += Math.max(0, urlCount);
          if (urlCount != 0) { 
            ccURLSPerDomain.addValue((double)urlCount);
          }
          if (JSONUtils.safeGetInteger(crawlStatsMetadata, CrawlStatsCommon.CRAWLSTATS_BLEKKO_URL) != -1) { 
            blekkoURLSPerDomain.addValue((double)JSONUtils.safeGetInteger(crawlStatsMetadata, CrawlStatsCommon.CRAWLSTATS_BLEKKO_URL));
          }
        }
        if (subDomainStats.has(CrawlStatsCommon.JOINEDMETDATA_PROPERTY_RANK)) { 
          domainRank.addValue(subDomainStats.get(CrawlStatsCommon.JOINEDMETDATA_PROPERTY_RANK).getAsDouble());
        }
        subDomainCount++;
      }

      
      JsonObject summaryRecordOut = new JsonObject();
      
      summaryRecordOut.addProperty(ROOTDOMAIN_STATS_IS_SUPERDOMAIN, isSuperDomain);
      summaryRecordOut.addProperty(ROOTDOMAIN_STATS_TOTAL_URLS,totalURLS);
      summaryRecordOut.addProperty(ROOTDOMAIN_STATS_SUBDOMAIN_COUNT,subDomainCount);

      summaryRecordOut.addProperty(ROOTDOMAIN_STATS_MEAN_URLS_PER_DOMAIN,ccURLSPerDomain.getMean());
      summaryRecordOut.addProperty(ROOTDOMAIN_STATS_MAX_URLS_IN_A_DOMAIN,ccURLSPerDomain.getMax());
      //summaryRecordOut.addProperty(ROOTDOMAIN_STATS_URLS_PER_DOMAIN_90TH,ccURLSPerDomain.getPercentile(90));
      summaryRecordOut.addProperty(ROOTDOMAIN_STATS_URLS_PER_DOMAIN_STDDEV,ccURLSPerDomain.getStandardDeviation());
      summaryRecordOut.addProperty(ROOTDOMAIN_STATS_BLEKKO_TOTAL_URLS,blekkoURLSPerDomain.getSum());
      summaryRecordOut.addProperty(ROOTDOMAIN_STATS_BLEKKO_MEAN_URLS_PER_DOMAIN,blekkoURLSPerDomain.getMean());
      summaryRecordOut.addProperty(ROOTDOMAIN_STATS_BLEKKO_RANK_MEAN,blekkoRank.getMean());
      summaryRecordOut.addProperty(ROOTDOMAIN_STATS_BLEKKO_RANK_MAX,blekkoRank.getMax());
      summaryRecordOut.addProperty(ROOTDOMAIN_STATS_BLEKKO_RANK_TOTAL,blekkoRank.getSum());
      //summaryRecordOut.addProperty(ROOTDOMAIN_STATS_BLEKKO_RANK_90TH,blekkoRank.getPercentile(90));
      summaryRecordOut.addProperty(ROOTDOMAIN_STATS_DOMAIN_RANK_MEAN,domainRank.getMean());
      summaryRecordOut.addProperty(ROOTDOMAIN_STATS_DOMAIN_RANK_MAX,domainRank.getMax());
      summaryRecordOut.addProperty(ROOTDOMAIN_STATS_DOMAIN_RANK_TOTAL,domainRank.getSum());
      summaryRecordOut.addProperty(ROOTDOMAIN_STATS_DOMAIN_RANK_STDDEV,domainRank.getStandardDeviation());
      //summaryRecordOut.addProperty(ROOTDOMAIN_STATS_DOMAIN_RANK_90TH,domainRank.getPercentile(90));
      summaryRecordOut.addProperty(ROOTDOMAIN_STATS_BLEKKO_IS_PORN,blekkoisPorn.getN());
      summaryRecordOut.addProperty(ROOTDOMAIN_STATS_BLEKKO_IS_SPAM,blekkoisSpam.getN());
      summaryRecordOut.addProperty(ROOTDOMAIN_STATS_BLEKKO_MEAN_CRAWL_COUNT,blekkoCrawlCount.getMean());
      summaryRecordOut.addProperty(ROOTDOMAIN_STATS_BLEKKO_TOTAL_CRAWL_COUNT,blekkoCrawlCount.getSum());
      
      summaryRecordOut.addProperty(ROOTDOMAIN_STATS_BLEKKO_MEAN_PR_COUNT,blekkoPRCount.getMean());
      summaryRecordOut.addProperty(ROOTDOMAIN_STATS_BLEKKO_MAX_PR_COUNT,blekkoPRCount.getMax());
      summaryRecordOut.addProperty(ROOTDOMAIN_STATS_BLEKKO_TOTAL_PR_COUNT,blekkoPRCount.getSum());
      
      summaryRecordOut.addProperty(ROOTDOMAIN_STATS_MEAN_EXT_LINKED_URLS,externallyLinkedCount.getMean());
      summaryRecordOut.addProperty(ROOTDOMAIN_STATS_MAX_EXT_LINKED_URLS,externallyLinkedCount.getMax());
      summaryRecordOut.addProperty(ROOTDOMAIN_STATS_TOTAL_EXT_LINKED_URLS,externallyLinkedCount.getSum());
      //summaryRecordOut.addProperty(ROOTDOMAIN_STATS_EXT_LINKED_URLS_90TH,externallyLinkedCount.getPercentile(90));
      summaryRecordOut.addProperty(ROOTDOMAIN_STATS_EXT_LINKED_URLS_STDDEV,externallyLinkedCount.getStandardDeviation());
      
      output.collect(key, new TextBytes(summaryRecordOut.toString()));
      
    }
    
  }
}
