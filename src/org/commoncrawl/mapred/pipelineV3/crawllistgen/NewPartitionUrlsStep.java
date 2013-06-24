package org.commoncrawl.mapred.pipelineV3.crawllistgen;

import java.io.IOException;
import java.net.MalformedURLException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.mapred.ec2.postprocess.crawldb.CrawlDBCommon;
import org.commoncrawl.mapred.ec2.postprocess.crawldb.CrawlDBKey;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.DomainMetadataTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.crawlstats.CrawlStatsCommon;
import org.commoncrawl.util.GoogleURL;
import org.commoncrawl.util.JSONUtils;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.S3NFileSystem;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader.KeyAndValueData;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader.RawRecordValue;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.Tuples.Pair;
import org.commoncrawl.util.URLUtils;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class NewPartitionUrlsStep extends CrawlPipelineStep {

  public NewPartitionUrlsStep(CrawlPipelineTask task) {
    super(task, "Partition URLS", OUTPUT_DIR_NAME);
  }

  public static final String ROOTDOMAIN_METADATA_PATH = "root.meta.path";
  public static final String SUBDOMAIN_METADATA_PATH = "subdomain.meta.path";
  
  public static final int NUM_PARTITIONS = 100;
  
  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }    

  private static final Log LOG = LogFactory.getLog(NewPartitionUrlsStep.class);

  public static final String OUTPUT_DIR_NAME = "paritionUrlsStep";
  
  @Override
  public Log getLogger() {
    return LOG;
  }

  static final String PARTITION_ID_START_PROPERTY = "listgen.partitionIdStart";
  static final String NUM_PARTITIONS_PROPERTY = "listgen.numPartitions";
  
  
  @Override
  public void runStep(Path outputPathLocation) throws IOException {
    
    
    // get entire set of input crawl db paths ... 
    DomainMetadataTask rootTask = (DomainMetadataTask)getRootTask();
    
    Configuration conf = new Configuration(rootTask.getConf()); 

    int partitionStartId = conf.getInt(PARTITION_ID_START_PROPERTY,0);
    int numPartitions = conf.getInt(NUM_PARTITIONS_PROPERTY,NUM_PARTITIONS);
    
    LOG.info("Partition Id Start:" + partitionStartId);
    LOG.info("Num Partitions:" + numPartitions);

    List<Path> crawlDBPaths = rootTask.getRestrictedMergeDBDataPaths();
    
    LOG.info("Input Paths are:" + crawlDBPaths);
    
    // partition ... 
    Iterable<List<Path>> partitions = Iterables.partition(crawlDBPaths, crawlDBPaths.size() / numPartitions);
    
    // get a file system object ... 
    FileSystem fs = FileSystem.get(outputPathLocation.toUri(),conf);
    // iterate partitions
    int partitionIndex = partitionStartId;
    for (List<Path> partitionPaths : partitions) {
      
      // construct output path ... 
      Path partitionOutputPath = new Path(outputPathLocation,NUMBER_FORMAT.format(partitionIndex));
      // if not present ... 
      if (!fs.exists(partitionOutputPath)) { 
        runStepForPartition(rootTask,partitionIndex,partitionPaths,partitionOutputPath);
      }
      ++partitionIndex;
    }
  }
  
  
  void runStepForPartition(DomainMetadataTask rootTask,int partitionIndex,List<Path> inputPaths,Path outputPath)throws IOException { 
    // build the basic job config ... 
    JobConf job = new JobBuilder("Parition URL List", new Configuration()) 
    
    .inputFormat(PartitionJoinInputFormat.class)
    .mapper(RankAndFilterMapper.class)
    .keyValue(CrawlListKey.class, TextBytes.class)
    .sort(CrawlListKey.CrawListKeyComparator.class)
    .partition(CrawlListKey.CrawlListKeyPartitioner.class)
    .numReducers(CrawlListGenCommon.NUM_LIST_PARTITIONS)
    .compressor(CompressionType.BLOCK, SnappyCodec.class)
    .output(outputPath)
    .outputIsSeqFile()
    .build();
    
    job.setInt("mapred.task.timeout",4*(60*(60*1000)));
    
    // write partition paths ... 
    PartitionJoinInputFormat.writeSinglePathPerPartition(inputPaths, job);
    // ok, figure out locations of dependent metadata ... 
    job.set(ROOTDOMAIN_METADATA_PATH, rootTask.getOutputDirForStep(ShardRootDomainClassificationStep.class).toString());
    job.set(SUBDOMAIN_METADATA_PATH, rootTask.getOutputDirForStep(ShardSubDomainMetadataStep.class).toString());
    
    // run it ... 
    JobClient.runJob(job);
  }

  
  @Override
  public boolean isComplete() throws IOException {
    
    /*
    // get entire set of input crawl db paths ... 
    DomainMetadataTask rootTask = (DomainMetadataTask)getRootTask();
    Configuration conf = new Configuration(rootTask.getConf());

    List<Path> crawlDBPaths = rootTask.getMergeDBDataPaths();
    
    int partitionStartId = conf.getInt(PARTITION_ID_START_PROPERTY,0);
    int numPartitions = conf.getInt(NUM_PARTITIONS_PROPERTY,NUM_PARTITIONS);

    Path outputPath = getOutputDir();
    
    // get a file system object ... 
    FileSystem fs = FileSystem.get(outputPath.toUri(),conf);
    // iterate partitions
    for (int partitionIndex=partitionStartId;partitionIndex<partitionStartId+numPartitions;++partitionIndex) { 
      // construct output path ... 
      Path partitionOutputPath = new Path(outputPath,NUMBER_FORMAT.format(partitionIndex));
      
      // if not present ... 
      if (!fs.exists(partitionOutputPath)) {
        LOG.info("Partition output path:" + partitionOutputPath + " not found!");
        return false;
      }
      else { 
        LOG.info("Found partition output path:" + partitionOutputPath);
      }
      ++partitionIndex;
    }
    */
    return true;
  }
  
  public static class RankAndFilterMapper implements  Mapper<IntWritable,Text,CrawlListKey,TextBytes> {

    Path rootDomainMetaPath;
    Path subDomainMetaPath;
    JobConf _conf;
    
    @Override
    public void configure(JobConf job) {
      rootDomainMetaPath = new Path(job.get(ROOTDOMAIN_METADATA_PATH));
      subDomainMetaPath  = new Path(job.get(SUBDOMAIN_METADATA_PATH));
      _conf = job;
    }

    @Override
    public void close() throws IOException {      
    }

    OutputCollector<CrawlListKey, TextBytes> _collector;

    enum Counters { 
      ROOT_DOMAIN_ID_MISMATCH, SKIPPING_BLACKLISTED_URL, SKIPPING_LIMITED_CRAWL_URL, SUBDOMAIN_METADATA_WITHOUT_MATCHING_ROOT_DOMAIN_METADATA, URL_METADATA_MISSING_URL, FOUND_LINK_RECORD, FILTERED_OUT_URL, SKIPPING_NON_RANKED_URL, BLEKKO_URL, CC_URL, EMITTED_URL_WITH_QUERY, COULD_NOT_RESOLVE_WWW_PREFIX, USING_WWW_PREFIX, SKIPPING_QUERY_URL, NO_SOURCE_URL
    }
    
    
    static void extractHTTPHeaderData(JsonObject metadataObject,JsonObject jsonOut) throws IOException { 
      if (metadataObject.has(CrawlDBCommon.TOPLEVEL_SUMMARYRECORD_PROPRETY)) { 
        JsonObject summaryRecord = metadataObject.getAsJsonObject(CrawlDBCommon.TOPLEVEL_SUMMARYRECORD_PROPRETY);
        if (summaryRecord.has(CrawlDBCommon.SUMMARYRECORD_CRAWLDETAILS_ARRAY_PROPERTY)) { 
          JsonArray crawlDetails = summaryRecord.getAsJsonArray(CrawlDBCommon.SUMMARYRECORD_CRAWLDETAILS_ARRAY_PROPERTY);
          
          long latestAttemptTime = -1;
          long lastModifiedTime = -1;
          String etag = null;
          
          for (JsonElement detailElement : crawlDetails) { 
            JsonObject detailRecord = detailElement.getAsJsonObject();
            int httpResult = JSONUtils.safeGetInteger(detailRecord, "http_result");
            if (httpResult >= 200 && httpResult < 300) {
              long attemptTime = JSONUtils.safeGetLong(detailRecord, "attempt_time");
              if (attemptTime != -1 && attemptTime > latestAttemptTime) { 
                if (detailRecord.has("last-modified")) { 
                  lastModifiedTime = detailRecord.get("last-modified").getAsLong();
                }
                else { 
                  lastModifiedTime = -1L;
                }
                if (detailRecord.has("etag")) { 
                  etag = detailRecord.get("etag").getAsString();
                }
                else { 
                  etag = null;
                }
              }
            }
          }
          
          if (lastModifiedTime != -1L) { 
            jsonOut.addProperty(CrawlListGenCommon.CRAWLLIST_METADATA_LAST_MODIFIED_TIME, lastModifiedTime);
          }
          if (etag != null) { 
            jsonOut.addProperty(CrawlListGenCommon.CRAWLLIST_METADATA_ETAG, etag);
          }
        }
      }
    }
    
    
    static final String stripWWW(String host) {
      if (host.startsWith("www.")) {
        return host.substring("www.".length());
      }
      return host;
    }
    
    
    public static String canonicalizeURL(GoogleURL urlObject,boolean prefixWithWWW) throws MalformedURLException {

      StringBuilder urlOut = new StringBuilder();

      urlOut.append(urlObject.getScheme());
      urlOut.append("://");

      if (urlObject.getUserName() != GoogleURL.emptyString) {
        urlOut.append(urlObject.getUserName());
        if (urlObject.getPassword() != GoogleURL.emptyString) {
          urlOut.append(":");
          urlOut.append(urlObject.getPassword());
        }
        urlOut.append("@");
      }

      String host = urlObject.getHost();
      if (host.endsWith(".")) {
        host = host.substring(0, host.length() - 1);
      }

      // and if we should prefix with www. add it back in ... 
      if (!host.startsWith("www.") && prefixWithWWW) {
        host = "www." + host;
      }
      urlOut.append(host);

      if (urlObject.getPort() != GoogleURL.emptyString
          && !urlObject.getPort().equals("80")) {
        urlOut.append(":");
        urlOut.append(urlObject.getPort());
      }
      if (urlObject.getPath() != GoogleURL.emptyString) {
        int indexOfSemiColon = urlObject.getPath().indexOf(';');
        if (indexOfSemiColon != -1) {
          urlOut.append(urlObject.getPath().substring(0, indexOfSemiColon));
        } else {
          urlOut.append(urlObject.getPath());
        }
      }
      if (urlObject.getQuery() != GoogleURL.emptyString) {
        urlOut.append("?");
        urlOut.append(urlObject.getQuery());
      }

      String canonicalizedURL = urlOut.toString();

      // phase 2 - remove common session id patterns
      canonicalizedURL = URLUtils.sessionIdNormalizer.normalize(canonicalizedURL, "");
      
      // phase 3 - stir back in ref if #!
      if (urlObject.getRef().length() != 0 && urlObject.getRef().charAt(0) == '!') { 
        canonicalizedURL += "#" + urlObject.getRef();
      }
      return canonicalizedURL;
    }
    
    static void addPartFileGivenPath(List<Path> paths,FileSystem fs,Path path) throws IOException { 
      FileStatus files[] = fs.globStatus(new Path(path,"part-*"));
      for (FileStatus file : files) { 
        paths.add(file.getPath());
      }
    }
    
    private static FileSystem getFileSystemForMergePath(Path path,Configuration conf)throws IOException { 
      // override S3N 
      if (path.toUri().getScheme().equalsIgnoreCase("s3n")) { 
        FileSystem fs = new S3NFileSystem();
        fs.initialize(path.toUri(), conf);
        return fs;
      }
      // conf.setClass("fs.s3n.impl", S3NFileSystem.class,FileSystem.class);
      return FileSystem.get(path.toUri(),conf);
    }
    
    
    @Override
    public void map(IntWritable key, Text value,OutputCollector<CrawlListKey, TextBytes> output, Reporter reporter) throws IOException {
      // set up merge attributes
      Configuration localMergeConfig = new Configuration(_conf);

      localMergeConfig.setClass(MultiFileInputReader.MULTIFILE_COMPARATOR_CLASS, CrawlDBKey.CrawlDBKeyComparator.class,
          Comparator.class);
      localMergeConfig.setClass(MultiFileInputReader.MULTIFILE_KEY_CLASS, CrawlDBKey.class, WritableComparable.class);
      
      // get the single input path... 
      Path inputPath = new Path(value.toString());
      
      // get fs based on path ... 
      FileSystem fs = FileSystem.get(inputPath.toUri(),_conf);
      
      ArrayList<Path> paths = Lists.newArrayList();
      // add join paths
      addPartFileGivenPath(paths, fs, rootDomainMetaPath);
      addPartFileGivenPath(paths, fs, subDomainMetaPath);
      paths.add(inputPath);
      
      LOG.info("Input Paths for Shard:" + key.get() + " Are:" + paths);
      
      
      // replace emr s3n for inputs ... 
      FileSystem mergefs = getFileSystemForMergePath(paths.get(0),localMergeConfig);
      
      // ok now spawn merger
      MultiFileInputReader<TextBytes> multiFileInputReader 
        = new MultiFileInputReader<TextBytes>(mergefs, paths, localMergeConfig);

      
      try { 
        Pair<KeyAndValueData<TextBytes>, Iterable<RawRecordValue>> nextItem = null;
        
        TextBytes valueText = new TextBytes();
        DataInputBuffer valueStream = new DataInputBuffer();
        JsonParser parser = new JsonParser();
        _collector = output;
        long       _rootDomainId = -1L;
        JsonObject _rootDomainMetadata = null;
        long       _subDomainId = -1L;
        boolean    _isSuperDomain = false;
        boolean    _isBlacklisted = false;
        boolean    _limitedCrawl = false;
        boolean    _prefixWithWWW = false;
        URLFilter filter = new URLFilter();
        CrawlListKey keyOut = new CrawlListKey();
        TextBytes valueOut = new TextBytes();
        
        while ((nextItem = multiFileInputReader.getNextItemIterator()) != null) {
        
          //LOG.info("Key:"+ nextItem.e0._keyObject.toString());
          
          long recordType = CrawlDBKey.getLongComponentFromKey(nextItem.e0._keyObject, CrawlDBKey.ComponentId.TYPE_COMPONENT_ID);
          
          if (recordType == CrawlDBKey.Type.KEY_TYPE_ROOTDOMAIN_METADATA_RECORD.ordinal()) {
            RawRecordValue rawValue = Iterables.getFirst(nextItem.e1,null);
  
            valueStream.reset(rawValue.data.getData(),0,rawValue.data.getLength());
            valueText.setFromRawTextBytes(valueStream);
  
            _rootDomainId = CrawlDBKey.getLongComponentFromKey(nextItem.e0._keyObject, CrawlDBKey.ComponentId.ROOT_DOMAIN_HASH_COMPONENT_ID);
            //LOG.info("Got Root Domain Record:"+  _rootDomainId);

            _rootDomainMetadata = parser.parse(valueText.toString()).getAsJsonObject();
            
            _isSuperDomain = JSONUtils.safeGetBoolean(_rootDomainMetadata,CrawlStatsCommon.ROOTDOMAIN_CLASSIFY_SUPERDOMAIN);
            _isBlacklisted = JSONUtils.safeGetBoolean(_rootDomainMetadata,CrawlStatsCommon.ROOTDOMAIN_CLASSIFY_BLACKLISTED);
            _limitedCrawl  = JSONUtils.safeGetBoolean(_rootDomainMetadata,CrawlStatsCommon.ROOTDOMAIN_CLASSIFY_LIMITED_CRAWL);
            _prefixWithWWW = false;
          }
          else if (recordType == CrawlDBKey.Type.KEY_TYPE_SUBDOMAIN_METADATA_RECORD.ordinal()) {

            _subDomainId = CrawlDBKey.getLongComponentFromKey(nextItem.e0._keyObject, CrawlDBKey.ComponentId.DOMAIN_HASH_COMPONENT_ID);
            
            RawRecordValue rawValue = Iterables.getFirst(nextItem.e1,null);
            valueStream.reset(rawValue.data.getData(),0,rawValue.data.getLength());
            
            long rootDomainId = CrawlDBKey.getLongComponentFromKey(nextItem.e0._keyObject, CrawlDBKey.ComponentId.ROOT_DOMAIN_HASH_COMPONENT_ID);
            if (rootDomainId != _rootDomainId) { 
              LOG.error("SubDomain:" + _subDomainId + " Root Id:" + rootDomainId 
                  + " did not match current root domain id:" + _rootDomainId);
              
              reporter.incrCounter(Counters.SUBDOMAIN_METADATA_WITHOUT_MATCHING_ROOT_DOMAIN_METADATA, 1);
              _isSuperDomain = false;
              _isBlacklisted = false;
              _limitedCrawl = false;
              _prefixWithWWW = false;
            }
            _prefixWithWWW = valueStream.readBoolean();
          }
          else if (recordType == CrawlDBKey.Type.KEY_TYPE_MERGED_RECORD.ordinal()) { 
            
            long currentRootDomainId = CrawlDBKey.getLongComponentFromKey(nextItem.e0._keyObject, CrawlDBKey.ComponentId.ROOT_DOMAIN_HASH_COMPONENT_ID);
            long currentDomainId = CrawlDBKey.getLongComponentFromKey(nextItem.e0._keyObject, CrawlDBKey.ComponentId.DOMAIN_HASH_COMPONENT_ID);
            if (currentRootDomainId == _rootDomainId && currentDomainId == _subDomainId) { 
              if (_isBlacklisted) { 
                reporter.incrCounter(Counters.SKIPPING_BLACKLISTED_URL, 1);
              }
              else if (_limitedCrawl) { 
                reporter.incrCounter(Counters.SKIPPING_LIMITED_CRAWL_URL, 1);
              }
              else {
                
                // get first record, which will be merge record ... 
                RawRecordValue firstRawValue = Iterables.getFirst(nextItem.e1, null);
  
                // convert to json object ... 
                valueStream.reset(firstRawValue.data.getData(),0,firstRawValue.data.getLength());
                valueText.setFromRawTextBytes(valueStream);
                              
                JsonObject mergedCrawlDBRecord = parser.parse(valueText.toString()).getAsJsonObject();
                
                int extRefCount = 0;
                int intRefCount = 0;
                // get external ref count 
                if (mergedCrawlDBRecord.has(CrawlDBCommon.TOPLEVEL_LINKSTATUS_PROPERTY)) { 
                  extRefCount = JSONUtils.safeGetInteger(mergedCrawlDBRecord.getAsJsonObject(CrawlDBCommon.TOPLEVEL_LINKSTATUS_PROPERTY),
                      CrawlDBCommon.LINKSTATUS_EXTRADOMAIN_SOURCES_COUNT_PROPERTY,0);
                  intRefCount = JSONUtils.safeGetInteger(mergedCrawlDBRecord.getAsJsonObject(CrawlDBCommon.TOPLEVEL_LINKSTATUS_PROPERTY),
                      CrawlDBCommon.LINKSTATUS_INTRADOMAIN_SOURCES_COUNT_PROPERTY,0);
                }
                
                
                String sourceURL = JSONUtils.safeGetStringFromElement(mergedCrawlDBRecord,CrawlDBCommon.TOPLEVEL_SOURCE_URL_PROPRETY);
                
                if (sourceURL == null || sourceURL.length() == 0) { 
                  reporter.incrCounter(Counters.URL_METADATA_MISSING_URL, 1);
                }
                else { 
                  GoogleURL urlObject = new GoogleURL(sourceURL);
                  
                  if (filter.isURLCrawlable(urlObject, mergedCrawlDBRecord)) {
                    double ccRank = calcualteScore(intRefCount, extRefCount);
                    double blekkoRank = 0.0;
                    boolean inBlekkoFrontier = false;
                    boolean crawledByBlekko = false;
                    if (mergedCrawlDBRecord.has(CrawlDBCommon.TOPLEVEL_BLEKKO_METADATA_PROPERTY)) {
                      inBlekkoFrontier = true;
                      JsonObject blekkoStatus = mergedCrawlDBRecord.getAsJsonObject(CrawlDBCommon.TOPLEVEL_BLEKKO_METADATA_PROPERTY);
                      if (blekkoStatus.has(CrawlDBCommon.BLEKKO_METADATA_STATUS)) { 
                        String statusStr = blekkoStatus.get(CrawlDBCommon.BLEKKO_METADATA_STATUS).getAsString();
                        if (statusStr.equalsIgnoreCase("crawled")) { 
                          crawledByBlekko = true;
                        }
                      }
                      if (blekkoStatus.has(CrawlDBCommon.BLEKKO_METADATA_RANK_10)) { 
                        blekkoRank = blekkoStatus.get(CrawlDBCommon.BLEKKO_METADATA_RANK_10).getAsDouble();
                      }                    
                    }
                    //if (inBlekkoFrontier || crawledByBlekko || blekkoRank != 0 || ((intRefCount >2  || extRefCount !=0))) 
                    {
                      boolean allowQueryURL = (inBlekkoFrontier && blekkoRank != 0 || crawledByBlekko);
                      if (urlObject.has_query() && !allowQueryURL ) { 
                        reporter.incrCounter(Counters.SKIPPING_QUERY_URL, 1);
                        /*
                        LOG.info("SKIPPED QueryURL Flags[" 
                            + " intRef:" + intRefCount
                            + " extRef:" + extRefCount
                            +"] URL:" 
                            + urlObject.getCanonicalURL());
                        */
                        
                      }
                      else { 
                        if (crawledByBlekko || blekkoRank != 0 || inBlekkoFrontier) { 
                          reporter.incrCounter(Counters.BLEKKO_URL, 1);
                        }
                        else { 
                          reporter.incrCounter(Counters.CC_URL, 1);
                        }
                        double cumilativeRank = 10000 * (crawledByBlekko ? 0 : 1);
                        cumilativeRank += 1000 * blekkoRank;
                        cumilativeRank += 10 * ccRank;
                        
                        // build output json 
                        JsonObject outputJSON = new JsonObject();
                                            
                        // extract http metadata ...
                        extractHTTPHeaderData(mergedCrawlDBRecord,outputJSON);
                        
                        if (urlObject.has_query()) { 
                          reporter.incrCounter(Counters.EMITTED_URL_WITH_QUERY, 1);
                          /*
                          LOG.info("Query URL Flags[" 
                              + " BlekkoF:" + inBlekkoFrontier
                              + " CByBlekko:" + crawledByBlekko
                              + " BR:" + blekkoRank
                              + " intRef:" + intRefCount
                              + " extRef:" + extRefCount
                              +"] URL:" 
                              + urlObject.getCanonicalURL());
                           */
                        }
                        
                        // canonicalize url ... 
                        String canonicalURL = canonicalizeURL(urlObject, _prefixWithWWW);
                        
                        // append it to output json  
                        outputJSON.addProperty(CrawlDBCommon.TOPLEVEL_SOURCE_URL_PROPRETY, canonicalURL);
    
                        // figure out partition domain ... 
                        // if not super domain, then partition on root domain id 
                        long partitionDomain = currentRootDomainId;
                        // if super domain then partition on root domain ... 
                        if (_isSuperDomain)
                          partitionDomain = currentDomainId;
                        // construct output key ... 
                        CrawlListKey.generateKey(keyOut, partitionDomain, currentDomainId, CrawlListKey.KEY_TYPE_URL, cumilativeRank, 0);
                        // set value text ... 
                        valueOut.set(outputJSON.toString());
                        // set 
                        output.collect(keyOut, valueOut);
                      }
                    }
                    /*
                    else { 
                      reporter.incrCounter(Counters.SKIPPING_NON_RANKED_URL, 1);
                      LOG.info("SKIPPED NonRankedURL Flags[" 
                          + " intRef:" + intRefCount
                          + " extRef:" + extRefCount
                          +"] URL:" 
                          + urlObject.getCanonicalURL());
                    }
                    */
                  }
                  else { 
                    reporter.incrCounter(Counters.FILTERED_OUT_URL, 1);
                  }
                }
              }
            }
            else { 
              reporter.incrCounter(Counters.ROOT_DOMAIN_ID_MISMATCH, 1);
              LOG.error("RootDomain Id Mismatch: Expected RH:" + _rootDomainId + " DH:" + _subDomainId 
                  + " Got:" + currentRootDomainId + ":" + currentDomainId);
            }
          }
          
          // keep pump primed... 
          reporter.progress();
        }
      }
      finally { 
        multiFileInputReader.close();
      }
    }
  }
  
  static double calcualteScore(int inlinksFromSameRoot, int inlinksFromDifferentRoot) {
    inlinksFromDifferentRoot = inlinksFromDifferentRoot+1;
    inlinksFromSameRoot = inlinksFromSameRoot+1;
    return (Math.min(Math.sqrt(Math.pow(Math.log(inlinksFromSameRoot) * .2, 2)
        + Math.pow(Math.log(inlinksFromDifferentRoot), 2)), 14) / 14.0) * 10.0;
  }
}
