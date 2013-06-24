package org.commoncrawl.mapred.pipelineV3.domainmeta.crawlstats;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.commoncrawl.mapred.ec2.postprocess.crawldb.CrawlDBCommon;
import org.commoncrawl.mapred.ec2.postprocess.crawldb.CrawlDBKey;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.DomainMetadataTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.rank.GenSuperDomainListStep;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.GoogleURL;
import org.commoncrawl.util.JSONUtils;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader.KeyAndValueData;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader.RawRecordValue;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileMergeInputFormat;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileMergePartitioner;
import org.commoncrawl.util.SuperDomainList;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.Tuples.Pair;
import org.commoncrawl.util.URLUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class NewCrawlStatsCollectorStep extends CrawlPipelineStep{

  private static final Log LOG = LogFactory.getLog(NewCrawlStatsCollectorStep.class);

  public static final String OUTPUT_DIR_NAME = "crawlDBStatsV2";
  
  public static final String SUPER_DOMAIN_FILE_PATH = "super-domain-list";


  public NewCrawlStatsCollectorStep(CrawlPipelineTask task) {
    super(task, "Crawl DB Stats Collector", OUTPUT_DIR_NAME);
  }

  @Override
  public Log getLogger() {
    return LOG;
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {
    LOG.info("Task Identity Path is:" + getTaskIdentityPath());
    
    DomainMetadataTask rootTask = (DomainMetadataTask) getRootTask();
    
    ImmutableList<Path> paths = new ImmutableList.Builder<Path>().addAll(rootTask.getRestrictedMergeDBDataPaths()).build();

    Path superDomainListPath = new Path(getOutputDirForStep(GenSuperDomainListStep.class), "part-00000");

    JobConf jobConf = new JobBuilder("New Domain Stats Collector", getConf())

    .inputs(paths)
    .inputFormat(MultiFileMergeInputFormat.class)
    .mapperKeyValue(IntWritable.class, Text.class)
    .outputKeyValue(TextBytes.class, TextBytes.class)
    .outputFormat(SequenceFileOutputFormat.class)
    .reducer(CrawlDBStatsCollectingReducer.class, false)
    .partition(MultiFileMergePartitioner.class)
    .numReducers(1000)
    .maxReduceAttempts(4)
    .maxReduceTaskFailures(10)
    .speculativeExecution(true)
    .output(outputPathLocation)
    .compressMapOutput(false).compressor(CompressionType.BLOCK, SnappyCodec.class)
    .set(SUPER_DOMAIN_FILE_PATH, superDomainListPath.toString())

    .build();

    LOG.info("Starting JOB");
    JobClient.runJob(jobConf);
    LOG.info("Finsihed JOB");
    
  }
  
  public static class CrawlDBStatsCollectingReducer implements Reducer<IntWritable,Text,TextBytes,TextBytes> {

    Set<Long> superDomainIdSet;
    JobConf _jobConf;

    @Override
    public void configure(JobConf job) {
      
      _jobConf = job;
      
      Path superDomainIdFile = new Path(job.get(SUPER_DOMAIN_FILE_PATH));

      try {
        superDomainIdSet = SuperDomainList.loadSuperDomainIdList(job, superDomainIdFile);
      } catch (IOException e) {
        LOG.error(StringUtils.stringifyException(e));
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() throws IOException {
    }

    String      activeDomain = "";
    JsonObject  activeRecord = new JsonObject();
    OutputCollector<TextBytes, TextBytes> _collector;
    TextBytes   activeDomainKeyBytes = new TextBytes();
    TextBytes   activeDomainValueBytes = new TextBytes();
    HashSet<String> activeDomainIPs = new HashSet<String>();
    
    private void setActiveDomain(String hostName)throws IOException { 
      if (!activeDomain.equalsIgnoreCase(hostName)) { 
        if (activeDomain.length() != 0) {
          if (activeDomainIPs.size() != 0) { 
            JSONUtils.stringCollectionToJsonArray(activeRecord,CrawlStatsCommon.CRAWLSTATS_IPS,activeDomainIPs);
          }
          activeDomainKeyBytes.set(activeDomain);
          activeDomainValueBytes.set(activeRecord.toString());
          _collector.collect(activeDomainKeyBytes, activeDomainValueBytes);
        }
        activeDomain = hostName;
        activeRecord = new JsonObject();
        activeDomainIPs.clear();
      }
    }
    
    
    static final String stripWWW(String host) {
      if (host.startsWith("www.")) {
        return host.substring("www.".length());
      }
      return host;
    }
    
    enum Counters {
      HIT_EXCEPTION_PROCESSING_RECORD, NO_SOURCE_URL_PROPERTY, INVALID_URL_DETECTED, HIT_SUMMARY_RECORD, HAD_SUMMARY_DETAILS_ARRAY, HAD_ATTEMPT_COUNT, HAD_CRAWL_COUNT, HAD_REDIRECT_URL_PROPERTY, REDIRECT_WENT_OUT_OF_DOMAIN, WWW_TO_NON_WWW_DETECTED, NON_WWW_TO_WWW_DETECTED, GOT_LINK_STATUS_RECORD, GOT_EXTRADOMAIN_SOURCES_COUNT, GOT_BLEKKO_RECORD, GOT_BLEKKO_GT1_RECORD, GOT_MERGED_RECORD 
      
    }
    @Override
    public void reduce(IntWritable key, Iterator<Text> values,OutputCollector<TextBytes, TextBytes> output, Reporter reporter)throws IOException {
      // collect all incoming paths first
      Vector<Path> incomingPaths = new Vector<Path>();

      while (values.hasNext()) {
        String path = values.next().toString();
        LOG.info("Found Incoming Path:" + path);
        incomingPaths.add(new Path(path));
      }

      // set up merge attributes
      Configuration localMergeConfig = new Configuration(_jobConf);

      localMergeConfig.setClass(MultiFileInputReader.MULTIFILE_COMPARATOR_CLASS, CrawlDBKey.LinkKeyComparator.class,
          RawComparator.class);
      localMergeConfig.setClass(MultiFileInputReader.MULTIFILE_KEY_CLASS, TextBytes.class, WritableComparable.class);

      FileSystem fs = FileSystem.get(incomingPaths.get(0).toUri(),_jobConf);
      
      // ok now spawn merger
      MultiFileInputReader<TextBytes> multiFileInputReader 
        = new MultiFileInputReader<TextBytes>(fs, incomingPaths, localMergeConfig);

      try {
        
        Pair<KeyAndValueData<TextBytes>, Iterable<RawRecordValue>> nextItem = null;
    
        TextBytes valueText = new TextBytes();
        DataInputBuffer valueStream = new DataInputBuffer();
        JsonParser parser = new JsonParser();
        _collector = output;
        
        while ((nextItem = multiFileInputReader.getNextItemIterator()) != null) {
        
          long recordType = CrawlDBKey.getLongComponentFromKey(nextItem.e0._keyObject, CrawlDBKey.ComponentId.TYPE_COMPONENT_ID);
          if (recordType == CrawlDBKey.Type.KEY_TYPE_MERGED_RECORD.ordinal()) {
            reporter.incrCounter(Counters.GOT_MERGED_RECORD, 1);
            // walk records
            RawRecordValue rawValue = Iterators.getNext(nextItem.e1.iterator(),null);
            if (rawValue != null) {
              valueStream.reset(rawValue.data.getData(),0,rawValue.data.getLength());
              valueText.setFromRawTextBytes(valueStream);
              try {
                JsonObject mergeRecord = parser.parse(valueText.toString()).getAsJsonObject();
                
                if (mergeRecord.has(CrawlDBCommon.TOPLEVEL_SOURCE_URL_PROPRETY)) {
                  String sourceURL = mergeRecord.get(CrawlDBCommon.TOPLEVEL_SOURCE_URL_PROPRETY).getAsString();
                  GoogleURL urlObject = new GoogleURL(sourceURL);
                  if (urlObject.isValid()) { 
                    String hostName = stripWWW(urlObject.getHost());
                    setActiveDomain(hostName);
                    
                    JSONUtils.safeIncrementJSONCounter(activeRecord,CrawlStatsCommon.CRAWLSTATS_URL_COUNT);
                    
                    boolean crawled = false;
                    boolean inCrawlDB = false;
                    if (mergeRecord.has(CrawlDBCommon.TOPLEVEL_SUMMARYRECORD_PROPRETY)) {
                      reporter.incrCounter(Counters.HIT_SUMMARY_RECORD, 1);
                      
                      inCrawlDB = true;
                      JsonObject crawlStatus = mergeRecord.getAsJsonObject(CrawlDBCommon.TOPLEVEL_SUMMARYRECORD_PROPRETY);
                      
                      if (crawlStatus.has(CrawlDBCommon.SUMMARYRECORD_CRAWLDETAILS_ARRAY_PROPERTY)) {
                        reporter.incrCounter(Counters.HAD_SUMMARY_DETAILS_ARRAY, 1);
                        JsonArray crawlDetails = crawlStatus.getAsJsonArray(CrawlDBCommon.SUMMARYRECORD_CRAWLDETAILS_ARRAY_PROPERTY);
                        for (JsonElement arrayElement : crawlDetails) { 
                          JsonObject crawlDetail = arrayElement.getAsJsonObject();
                          if (crawlDetail.has(CrawlDBCommon.CRAWLDETAIL_SERVERIP_PROPERTY)) { 
                            activeDomainIPs.add(crawlDetail.get(CrawlDBCommon.CRAWLDETAIL_SERVERIP_PROPERTY).getAsString());
                          }
                        }
                      }
                      
                      if (crawlStatus.has(CrawlDBCommon.SUMMARYRECORD_ATTEMPT_COUNT_PROPERTY)) {
                        reporter.incrCounter(Counters.HAD_ATTEMPT_COUNT, 1);
                        JSONUtils.safeIncrementJSONCounter(activeRecord,CrawlStatsCommon.CRAWLSTATS_ATTEMPTED_COUNT);
                      }
                      if (crawlStatus.has(CrawlDBCommon.SUMMARYRECORD_CRAWLCOUNT_PROPERTY)) {
                        reporter.incrCounter(Counters.HAD_CRAWL_COUNT, 1);
                        if (crawlStatus.get(CrawlDBCommon.SUMMARYRECORD_CRAWLCOUNT_PROPERTY).getAsInt() > 0) { 
                          JSONUtils.safeIncrementJSONCounter(activeRecord,CrawlStatsCommon.CRAWLSTATS_CRAWLED_COUNT);
                          crawled = true;
                        }
                      }
                      if (crawlStatus.has(CrawlDBCommon.SUMMARYRECORD_REDIRECT_URL_PROPERTY)) {
                        reporter.incrCounter(Counters.HAD_REDIRECT_URL_PROPERTY, 1);
                        JSONUtils.safeIncrementJSONCounter(activeRecord,CrawlStatsCommon.CRAWLSTATS_REDIRECTED_COUNT);
                        String redirectURL = crawlStatus.get(CrawlDBCommon.SUMMARYRECORD_REDIRECT_URL_PROPERTY).getAsString();
                        GoogleURL redirectURLObj = new GoogleURL(redirectURL);
                        if (redirectURLObj.isValid()) { 
                          String originalRootHost = URLUtils.extractRootDomainName(urlObject.getHost());
                          String redirectRootHost = URLUtils.extractRootDomainName(redirectURLObj.getHost());
                          if (originalRootHost != null && redirectRootHost != null) { 
                            if (!originalRootHost.equalsIgnoreCase(redirectRootHost)){
                              reporter.incrCounter(Counters.REDIRECT_WENT_OUT_OF_DOMAIN, 1);
                              JSONUtils.safeIncrementJSONCounter(activeRecord,CrawlStatsCommon.CRAWLSTATS_REDIRECTED_OUT_COUNT);
                            }
                            else { 
                              if (!redirectURLObj.getHost().startsWith("www.") && urlObject.getHost().startsWith("www.")) {
                                reporter.incrCounter(Counters.WWW_TO_NON_WWW_DETECTED, 1);
                                JSONUtils.safeIncrementJSONCounter(activeRecord,CrawlStatsCommon.CRAWLSTATS_WWW_TO_NON_WWW_REDIRECT);
                              }
                              else if (redirectURLObj.getHost().startsWith("www.") && !urlObject.getHost().startsWith("www.")) {
                                reporter.incrCounter(Counters.NON_WWW_TO_WWW_DETECTED, 1);
                                JSONUtils.safeIncrementJSONCounter(activeRecord,CrawlStatsCommon.CRAWLSTATS_NON_WWW_TO_WWW_REDIRECT);
                              }
                            }
                          }
                        }
                      }
                    }
                    
                    if (mergeRecord.has(CrawlDBCommon.TOPLEVEL_LINKSTATUS_PROPERTY)) {
                      reporter.incrCounter(Counters.GOT_LINK_STATUS_RECORD, 1);
                      inCrawlDB = true;
                      JsonObject linkStatus = mergeRecord.getAsJsonObject(CrawlDBCommon.TOPLEVEL_LINKSTATUS_PROPERTY);
                      if (linkStatus.has(CrawlDBCommon.LINKSTATUS_EXTRADOMAIN_SOURCES_COUNT_PROPERTY)) {
                        reporter.incrCounter(Counters.GOT_EXTRADOMAIN_SOURCES_COUNT, 1);
                        JSONUtils.safeIncrementJSONCounter(activeRecord,CrawlStatsCommon.CRAWLSTATS_EXTERNALLY_LINKED_URLS);
                        if (!crawled) { 
                          JSONUtils.safeIncrementJSONCounter(activeRecord,CrawlStatsCommon.CRAWLSTATS_EXTERNALLY_LINKED_NOT_CRAWLED_URLS);
                        }
                      }
                    }
                    
                    if (mergeRecord.has(CrawlDBCommon.TOPLEVEL_BLEKKO_METADATA_PROPERTY)) {
                      reporter.incrCounter(Counters.GOT_BLEKKO_RECORD, 1);
                      JSONUtils.safeIncrementJSONCounter(activeRecord,CrawlStatsCommon.CRAWLSTATS_BLEKKO_URL);
                      if (!inCrawlDB) { 
                        JSONUtils.safeIncrementJSONCounter(activeRecord,CrawlStatsCommon.CRAWLSTATS_BLEKKO_URL_NOT_IN_CC);
                      }
                      
                      JsonObject blekkoStatus = mergeRecord.getAsJsonObject(CrawlDBCommon.TOPLEVEL_BLEKKO_METADATA_PROPERTY);
                      if (blekkoStatus.has(CrawlDBCommon.BLEKKO_METADATA_STATUS)) { 
                        String statusStr = blekkoStatus.get(CrawlDBCommon.BLEKKO_METADATA_STATUS).getAsString();
                        if (statusStr.equalsIgnoreCase("crawled")) { 
                          JSONUtils.safeIncrementJSONCounter(activeRecord,CrawlStatsCommon.CRAWLSTATS_BLEKKO_CRAWLED_COUNT);
                          if (crawled) { 
                            JSONUtils.safeIncrementJSONCounter(activeRecord,CrawlStatsCommon.CRAWLSTATS_BLEKKO_AND_CC_CRAWLED_COUNT);
                          }
                        }
                      }
                      if (blekkoStatus.has(CrawlDBCommon.BLEKKO_METADATA_RANK_10)) { 
                        if (blekkoStatus.get(CrawlDBCommon.BLEKKO_METADATA_RANK_10).getAsDouble() >= 1.0) {
                          reporter.incrCounter(Counters.GOT_BLEKKO_GT1_RECORD, 1);
                          JSONUtils.safeIncrementJSONCounter(activeRecord,CrawlStatsCommon.CRAWLSTATS_BLEKKO_URL_HAD_GT_1_RANK);
                        }
                      }
                    }
                    
                  }
                  else { 
                    reporter.incrCounter(Counters.INVALID_URL_DETECTED, 1);
                  }
                }
                else { 
                  reporter.incrCounter(Counters.NO_SOURCE_URL_PROPERTY, 1);
                }
              }
              catch (Exception e) {
                reporter.incrCounter(Counters.HIT_EXCEPTION_PROCESSING_RECORD, 1);
                LOG.error(CCStringUtils.stringifyException(e));
              }
            }
          }
        }
        setActiveDomain("");
      }
      finally { 
        multiFileInputReader.close();
      }      
    } 
    
  }

}
