package org.commoncrawl.mapred.pipelineV3.crawllistgen;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.commoncrawl.mapred.ec2.postprocess.crawldb.CrawlDBCommon;
import org.commoncrawl.mapred.ec2.postprocess.crawldb.CrawlDBKey;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.DomainMetadataTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.crawlstats.CrawlStatsCommon;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.GoogleURL;
import org.commoncrawl.util.JSONUtils;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.S3NFileSystem;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLFPBloomFilter;
import org.commoncrawl.util.URLUtils;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader.KeyAndValueData;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader.RawRecordValue;
import org.commoncrawl.util.Tuples.Pair;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class PartitionWikipediaUrlsStep extends CrawlPipelineStep {

  public static final String ROOTDOMAIN_METADATA_PATH = "root.meta.path";
  public static final String SUBDOMAIN_METADATA_PATH = "subdomain.meta.path";
  
  public static final String OUTPUT_DIR_NAME = "wikipedaURLS";
  
  public PartitionWikipediaUrlsStep(CrawlPipelineTask task) {
    super(task, "Partition Wikipedia", OUTPUT_DIR_NAME);
  }

  private static final Log LOG = LogFactory.getLog(PartitionWikipediaUrlsStep.class);
  
  @Override
  public Log getLogger() {
    return LOG;
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {
    
    // get entire set of input crawl db paths ... 
    DomainMetadataTask rootTask = (DomainMetadataTask)getRootTask();
    
    Configuration conf = new Configuration();
    
    List<Path> inputPaths = Lists.newArrayList();
    
    Path dbpediaDataPath = new Path("s3n://aws-publicdatasets/common-crawl/wikipedia/dbpedia/3.8"); 
    
    FileSystem fs = FileSystem.get(dbpediaDataPath.toUri(),conf);
    
    for (FileStatus file : fs.globStatus(new Path(dbpediaDataPath,"*.nt"))) { 
      inputPaths.add(file.getPath());
    }
        
    Path tempPath = new Path("s3n://aws-publicdatasets/common-crawl/wikipedia/dbpedia/3.8/partitioned");
    
    JobConf job = new JobBuilder("Pre-Partition Wikipedia URLS", new Configuration())
    
      .inputFormat(TextInputFormat.class)
      .inputs(inputPaths)
      .mapper(DBPediaEntryParser.class)
      .keyValue(TextBytes.class, TextBytes.class)
      .sort(CrawlDBKey.LinkKeyComparator.class)
      .numReducers(100)
      .compressor(CompressionType.BLOCK, SnappyCodec.class)
      .output(tempPath)
      .outputIsSeqFile()
      .build();
    
    //  
    JobClient.runJob(job);
    
    Path tempPath2 = new Path("s3n://aws-publicdatasets/common-crawl/wikipedia/dbpedia/3.8/joined");
    
    // join root domain metadata and wikipedia data
    job  = new JobBuilder("Join to Root Domain Metadata", new Configuration())
    
    .input(tempPath)
    .input(rootTask.getOutputDirForStep(ShardRootDomainClassificationStep.class))
    .inputIsSeqFile()
    .mapperKeyValue(TextBytes.class, TextBytes.class)
    .outputKeyValue(CrawlListKey.class, TextBytes.class)
    .sort(CrawlDBKey.CrawlDBKeyGroupByRootDomainComparator.class)
    .partition(CrawlDBKey.PartitionBySuperDomainPartitioner.class)
    .reducer(JoinRootDomainMetadataEmitLinkKeyReducer.class, false)
    .outputIsSeqFile()
    .output(tempPath2)
    .build();
    
    // JobClient.runJob(job);
    
    // partition and sort by list key 
    job  = new JobBuilder("Sort by ListKey", new Configuration())
    
    .input(tempPath2)
    .inputIsSeqFile()
    .keyValue(CrawlListKey.class, TextBytes.class)
    .sort(CrawlListKey.CrawListKeyComparator.class)
    .partition(CrawlListKey.CrawlListKeyPartitioner.class)
    .outputIsSeqFile()
    .compressor(CompressionType.BLOCK, SnappyCodec.class)
    .output(outputPathLocation)
    .jarByClass(PartitionWikipediaUrlsStep.class)
    .numReducers(CrawlListGenCommon.NUM_LIST_PARTITIONS)
    .build();
    
    // JobClient.runJob(job);
    
    /*    
    // collect input paths from first stage  
    List<Path> secondStageInputs = Lists.newArrayList();
    
    for (FileStatus file : fs.globStatus(new Path(tempPath,"part-*"))) { 
      secondStageInputs.add(file.getPath());
    }
    

    // build the basic job config ... 
    job = new JobBuilder("Parition Wikipedia URLS", new Configuration()) 
    
    .inputFormat(PartitionJoinInputFormat.class)
    .mapper(WikipediaURLPartitioner.class)
    .keyValue(CrawlListKey.class, TextBytes.class)
    .sort(CrawlListKey.CrawListKeyComparator.class)
    .partition(CrawlListKey.CrawlListKeyPartitioner.class)
    .numReducers(CrawlListGenCommon.NUM_LIST_PARTITIONS)
    .compressor(CompressionType.BLOCK, SnappyCodec.class)
    .output(outputPathLocation)
    .outputIsSeqFile()
    .build();
    
    
    
    
    job.setInt("mapred.task.timeout",4*(60*(60*1000)));
    
    // write partition paths ... 
    PartitionJoinInputFormat.writeSinglePathPerPartition(secondStageInputs, job);
    // ok, figure out locations of dependent metadata ... 
    job.set(ROOTDOMAIN_METADATA_PATH, rootTask.getOutputDirForStep(ShardRootDomainClassificationStep.class).toString());
    
    // run it ... 
    JobClient.runJob(job);
    */
    
    
  }
  
  static String parseDBPediaLine(String str) {
    int lastIndexOfGT = str.lastIndexOf('>');
    if (lastIndexOfGT >= 0) { 
      int lastIndexOfLT = str.lastIndexOf('<',lastIndexOfGT);
      if (lastIndexOfLT < lastIndexOfGT) { 
        return str.substring(lastIndexOfLT + 1,lastIndexOfGT);
      }
    }
    return null;
  }
  
  /** 
   * 
   * @author rana
   *
   */
  public static class DBPediaEntryParser implements Mapper<LongWritable,Text,TextBytes,TextBytes> {

    @Override
    public void configure(JobConf job) {
      
    }

    @Override
    public void close() throws IOException {
      
    }
    
    enum Counters { 
      FAILED_TO_PARSE_ENTRY
    , INVALID_URL, NULL_FP}

    @Override
    public void map(LongWritable key, Text value,OutputCollector<TextBytes, TextBytes> output, Reporter reporter)throws IOException {
      
      String url = parseDBPediaLine(value.toString());
      
      if (url == null) { 
        reporter.incrCounter(Counters.FAILED_TO_PARSE_ENTRY, 1);
      }
      else { 
        GoogleURL urlObject = new GoogleURL(url);
        
        if (!urlObject.isValid()) { 
          reporter.incrCounter(Counters.INVALID_URL, 1);
        }
        else { 
          // generate a fingerprint 
          URLFPV2 fp = URLUtils.getURLFPV2FromURLObject(urlObject);
          
          if (fp == null) { 
            reporter.incrCounter(Counters.NULL_FP, 1);
          }
          else { 
            JsonObject outputJSON = new JsonObject();
            // append it to output json  
            outputJSON.addProperty(CrawlDBCommon.TOPLEVEL_SOURCE_URL_PROPRETY, urlObject.getCanonicalURL());
            // emit a CrawlDBKey 
            TextBytes outputKey = CrawlDBKey.generateCrawlStatusKey(fp, 0);
            // write out
            output.collect(outputKey, new TextBytes(outputJSON.toString()));
          }
        }
      }
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

  static void addPartFileGivenPath(List<Path> paths,FileSystem fs,Path path) throws IOException { 
    FileStatus files[] = fs.globStatus(new Path(path,"part-*"));
    for (FileStatus file : files) { 
      paths.add(file.getPath());
    }
  }
  
  public static  class JoinRootDomainMetadataEmitLinkKeyReducer implements Reducer<TextBytes,TextBytes,CrawlListKey,TextBytes> {

    @Override
    public void configure(JobConf job) {
    }

    @Override
    public void close() throws IOException {      
    }

    JsonParser parser = new JsonParser();
    
    enum Counters { 
      FOUND_ROOT_DOMAIN_RECORD,
      DID_NOT_FIND_ROOT_DOMAIN_RECORD, BAD_URL, BAD_FP, EMITTED_HOMEPAGE_URL, JOINED_ROOT_DOMAIN_AND_WIKI_URL, BAD_JOIN_MORE_THAN_ONE_ROOT_DOMAIN, PARTITIONING_URL_WITH_SUBDOMAIN
    }
    
    static final int NUM_HASH_FUNCTIONS = 10;
    static final int NUM_BITS = 11;
    static final int NUM_ELEMENTS = 1 << 28;
    static final int FLUSH_THRESHOLD = 1 << 23;
    
    URLFPBloomFilter emittedTuplesFilter = new URLFPBloomFilter(NUM_ELEMENTS, NUM_HASH_FUNCTIONS, NUM_BITS);

    
    static String makeHomePageURLFromUrlObject(GoogleURL urlObject) {
      String urlOut = urlObject.getScheme();
      urlOut += (urlObject.getScheme());
      urlOut += ("://");

      if (urlObject.getUserName() != GoogleURL.emptyString) {
        urlOut += (urlObject.getUserName());
        if (urlObject.getPassword() != GoogleURL.emptyString) {
          urlOut += (":");
          urlOut += (urlObject.getPassword());
        }
        urlOut += ("@");
      }

      String host = urlObject.getHost();
      if (host.endsWith(".")) {
        host = host.substring(0, host.length() - 1);
      }
      urlOut += (host);
      urlOut += "/";
      
      return urlOut;
    }

    @Override
    public void reduce(TextBytes key, Iterator<TextBytes> values,OutputCollector<CrawlListKey, TextBytes> output, Reporter reporter)throws IOException {
      ArrayList<String> urls = new ArrayList<String>();
      boolean isSuperDomain = false;
      int     rootDomainRecordCount = 0;
      while (values.hasNext()) { 
        TextBytes nextValue = values.next();
        JsonObject object = parser.parse(nextValue.toString()).getAsJsonObject();
        if (object.has(CrawlDBCommon.TOPLEVEL_SOURCE_URL_PROPRETY)) { 
          urls.add(object.get(CrawlDBCommon.TOPLEVEL_SOURCE_URL_PROPRETY).getAsString());
        }
        else { 
          if (object.has(CrawlStatsCommon.ROOTDOMAIN_CLASSIFY_SUPERDOMAIN)) {
            rootDomainRecordCount++;
            reporter.incrCounter(Counters.FOUND_ROOT_DOMAIN_RECORD, 1);
            isSuperDomain = object.get(CrawlStatsCommon.ROOTDOMAIN_CLASSIFY_SUPERDOMAIN).getAsBoolean();
          }
        }
      }
      
      if (urls.size() != 0 && rootDomainRecordCount != 0) { 
        reporter.incrCounter(Counters.JOINED_ROOT_DOMAIN_AND_WIKI_URL, 1);
        if (rootDomainRecordCount > 1) { 
          reporter.incrCounter(Counters.BAD_JOIN_MORE_THAN_ONE_ROOT_DOMAIN,1);
        }
      }
      
      JsonObject objectOut = new JsonObject();
      CrawlListKey keyOut = new CrawlListKey();
      TextBytes valueOut = new TextBytes();
      URLFPV2 testKey = new URLFPV2();
      
      for (String url : urls) { 
        GoogleURL urlObject = new GoogleURL(url);
        if (!urlObject.isValid()) { 
          reporter.incrCounter(Counters.BAD_URL, 1);
        }
        else { 
          URLFPV2 fp = URLUtils.getURLFPV2FromURLObject(urlObject);
          if (fp == null) { 
            reporter.incrCounter(Counters.BAD_FP, 1);
          }
          else { 
            // if not super domain, then partition on root domain id 
            long partitionDomain = fp.getRootDomainHash();
            // if super domain then partition on root domain ... 
            if (isSuperDomain) { 
              partitionDomain = fp.getDomainHash();
              reporter.incrCounter(Counters.PARTITIONING_URL_WITH_SUBDOMAIN, 1);
            }

            // populate json ... 
            objectOut.addProperty(CrawlDBCommon.TOPLEVEL_SOURCE_URL_PROPRETY, url);
            
            // set into text output
            valueOut.set(objectOut.toString());
            
            // construct output key ... 
            CrawlListKey.generateKey(keyOut, partitionDomain, fp.getDomainHash(), CrawlListKey.KEY_TYPE_URL, 100000, 0);
            
            // output 
            output.collect(keyOut, valueOut);
            
            // ok check to see if we emitted this tuple ...
            testKey.setDomainHash(fp.getDomainHash());
            testKey.setUrlHash(fp.getDomainHash());

            
            if (!emittedTuplesFilter.isPresent(testKey)) {
              // add to bloom
              emittedTuplesFilter.add(testKey);
              // emit home page entry 
              String homePageURL = makeHomePageURLFromUrlObject(urlObject);
              
              // construct output key ... 
              CrawlListKey.generateKey(keyOut, partitionDomain, fp.getDomainHash(), CrawlListKey.KEY_TYPE_HOMEPAGE_URL, 1, 0);
              
              // populate json ... 
              objectOut.addProperty(CrawlDBCommon.TOPLEVEL_SOURCE_URL_PROPRETY, homePageURL);
              
              // set into text output
              valueOut.set(objectOut.toString());
                            
              output.collect(keyOut,valueOut);
              
              reporter.incrCounter(Counters.EMITTED_HOMEPAGE_URL, 1);
            }
          }
        }
      }
    } 
  }
  
  public static class WikipediaURLPartitioner implements Mapper<IntWritable,Text,CrawlListKey,TextBytes> {

    Path rootDomainMetaPath;
    JobConf _conf;
    OutputCollector<CrawlListKey, TextBytes> _collector;


    @Override
    public void configure(JobConf job) {
      rootDomainMetaPath = new Path(job.get(ROOTDOMAIN_METADATA_PATH));
      _conf = job;
    }

    @Override
    public void close() throws IOException {
      // TODO Auto-generated method stub

    }

    enum Counters {
      SUBDOMAIN_METADATA_WITHOUT_MATCHING_ROOT_DOMAIN_METADATA, NO_SOURCE_URL, FILTERED_OUT_URL, ROOT_DOMAIN_RECORD, CRAWL_STATUS_RECORD, INVALID_URL, EMITTED_HOMEPAGE_URL, SKIPPPED_ALREADY_EMITTED_HOMEPAGE_URL 

    }

    static final int NUM_HASH_FUNCTIONS = 10;
    static final int NUM_BITS = 11;
    static final int NUM_ELEMENTS = 1 << 28;
    static final int FLUSH_THRESHOLD = 1 << 23;
    
    URLFPBloomFilter emittedTuplesFilter = new URLFPBloomFilter(NUM_ELEMENTS, NUM_HASH_FUNCTIONS, NUM_BITS);
    
    static String makeHomePageURLFromUrlObject(GoogleURL urlObject) {
      String urlOut = urlObject.getScheme();
      urlOut += (urlObject.getScheme());
      urlOut += ("://");

      if (urlObject.getUserName() != GoogleURL.emptyString) {
        urlOut += (urlObject.getUserName());
        if (urlObject.getPassword() != GoogleURL.emptyString) {
          urlOut += (":");
          urlOut += (urlObject.getPassword());
        }
        urlOut += ("@");
      }

      String host = urlObject.getHost();
      if (host.endsWith(".")) {
        host = host.substring(0, host.length() - 1);
      }
      urlOut += (host);
      urlOut += "/";
      
      return urlOut;
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
        boolean    _isSuperDomain = false;
        CrawlListKey keyOut = new CrawlListKey();
        TextBytes valueOut = new TextBytes();
        URLFPV2 testKey = new URLFPV2();
        JsonObject jsonObjOut = new JsonObject(); 

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

            reporter.incrCounter(Counters.ROOT_DOMAIN_RECORD,1);
          }
          else if (recordType == CrawlDBKey.Type.KEY_TYPE_CRAWL_STATUS.ordinal()) { 

            reporter.incrCounter(Counters.CRAWL_STATUS_RECORD,1);

            long currentRootDomainId = CrawlDBKey.getLongComponentFromKey(nextItem.e0._keyObject, CrawlDBKey.ComponentId.ROOT_DOMAIN_HASH_COMPONENT_ID);
            long currentDomainId = CrawlDBKey.getLongComponentFromKey(nextItem.e0._keyObject, CrawlDBKey.ComponentId.DOMAIN_HASH_COMPONENT_ID);

            // get first record, which will be merge record ... 
            RawRecordValue firstRawValue = Iterables.getFirst(nextItem.e1, null);

            // convert to json object ... 
            valueStream.reset(firstRawValue.data.getData(),0,firstRawValue.data.getLength());
            valueText.setFromRawTextBytes(valueStream);

            JsonObject jsonObject = parser.parse(valueText.toString()).getAsJsonObject();

            // extract url ... 
            if (jsonObject.has(CrawlDBCommon.TOPLEVEL_SOURCE_URL_PROPRETY)) {
              String url = jsonObject.get(CrawlDBCommon.TOPLEVEL_SOURCE_URL_PROPRETY).getAsString();
              
              if (currentRootDomainId != _rootDomainId) {
                reporter.incrCounter(Counters.SUBDOMAIN_METADATA_WITHOUT_MATCHING_ROOT_DOMAIN_METADATA, 1);
                _isSuperDomain = false;
                _rootDomainId = currentRootDomainId;
                LOG.error("No Root Domain Info for URL:" + url);
              }

              // figure out partition domain ... 
              // if not super domain, then partition on root domain id 
              long partitionDomain = currentRootDomainId;
              // if super domain then partition on root domain ... 
              if (_isSuperDomain)
                partitionDomain = currentDomainId;
              // construct output key ... 
              CrawlListKey.generateKey(keyOut, partitionDomain, currentDomainId, CrawlListKey.KEY_TYPE_URL, 100000, 0);
              // set 
              output.collect(keyOut, valueText);
              
              // generate home page url 
              GoogleURL urlObject = new GoogleURL(url);
              
              if (urlObject.isValid()) { 
                // ok check to see if we emitted this tuple ...
                testKey.setDomainHash(currentDomainId);
                testKey.setUrlHash(currentDomainId);

                if (!emittedTuplesFilter.isPresent(testKey)) {
                  // add to bloom
                  emittedTuplesFilter.add(testKey);
                  // emit home page entry 
                  String homePageURL = makeHomePageURLFromUrlObject(urlObject);
                  
                  // construct output key ... 
                  CrawlListKey.generateKey(keyOut, partitionDomain, currentDomainId, CrawlListKey.KEY_TYPE_HOMEPAGE_URL, 1, 0);
                  
                  // and proper JSON 
                  jsonObjOut.addProperty(CrawlDBCommon.TOPLEVEL_SOURCE_URL_PROPRETY, homePageURL);
                  
                  valueOut.set(jsonObjOut.toString());
                  
                  output.collect(keyOut,valueOut);
                  
                  reporter.incrCounter(Counters.EMITTED_HOMEPAGE_URL, 1);
                }
                else { 
                  reporter.incrCounter(Counters.SKIPPPED_ALREADY_EMITTED_HOMEPAGE_URL, 1);
                }
              }
              else { 
                reporter.incrCounter(Counters.INVALID_URL, 1);
              }
            }
            else { 
              reporter.incrCounter(Counters.NO_SOURCE_URL, 1);
            }
          }
        }
      }
      finally { 
        multiFileInputReader.close();
      }
    }
  }
}
