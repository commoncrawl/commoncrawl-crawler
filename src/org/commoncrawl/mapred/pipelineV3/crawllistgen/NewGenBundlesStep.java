package org.commoncrawl.mapred.pipelineV3.crawllistgen;

import java.io.IOException;
import java.net.URI;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.mapred.SegmentGeneratorBundleKey;
import org.commoncrawl.mapred.SegmentGeneratorItem;
import org.commoncrawl.mapred.SegmentGeneratorItemBundle;
import org.commoncrawl.mapred.ec2.postprocess.crawldb.CrawlDBCommon;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.DomainMetadataTask;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.GoogleURL;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader.KeyAndValueData;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader.RawRecordValue;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileMergeInputFormat;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileMergePartitioner;
import org.commoncrawl.util.S3NFileSystem;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.Tuples.Pair;
import org.commoncrawl.util.URLFPBloomFilter;
import org.commoncrawl.util.URLUtils;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class NewGenBundlesStep extends CrawlPipelineStep {

  private static final Log LOG = LogFactory.getLog(NewGenBundlesStep.class);

  public static final String OUTPUT_DIR_NAME = "bundlesGenerator";

  static final int NUM_BITS = 11;

  static final int NUM_ELEMENTS = 1 << 28;

  static final int FLUSH_THRESHOLD = 1 << 23;

  public static final int SPILL_THRESHOLD = 250;
  
  
  enum Counters {
    SPILLED_1_MILLION_SKIPPED_REST, DOMAIN_WITH_GT_10MILLION_URLS, DOMAIN_WITH_GT_1MILLION_URLS, DOMAIN_WITH_GT_100K_URLS, DOMAIN_WITH_GT_50K_URLS, DOMAIN_WITH_GT_10K_URLS, DOMAIN_WITH_GT_1K_URLS, DOMAIN_WITH_GT_100_URLS, DOMAIN_WITH_GT_10_URLS, DOMAIN_WITH_LT_10_URLS, DOMAIN_WITH_1_URL, INVALID_SCHEME, INVALID_URL_OBJECT, SKIPPING_ALREADY_EMITTED_URL, NULL_FP_FOR_URL, NO_SOURCE_URL_IN_JSON, GENERATING_HOME_PAGE_URL, EMITTING_URL_OBJECT, GOT_RAW_RECORD_ITERATOR, GET_NEXT_RECORD_FROM_MERGER, GOT_RAW_RECORD_FROM_ITERATOR 
    
  }
  
  public NewGenBundlesStep(CrawlPipelineTask task) {
    super(task, "Generate Bundles", OUTPUT_DIR_NAME);
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

  
  static final String SINGLE_PARTITION_PROPERTY = "bundlegen.singlePartition";

  public void addCrawlListPaths(Configuration conf,int restrictedPartitionId,ArrayList<Path> pathsOut) throws IOException { 
    // get paritioned list path 
    Path partitionedListPath = getOutputDirForStep(NewPartitionUrlsStep.class);
    FileSystem fs = FileSystem.get(partitionedListPath.toUri(),conf);
    Path filterPath = new Path(partitionedListPath,"[0-9]*");
    for (FileStatus partitionPath : fs.globStatus(filterPath)) { 
      if (restrictedPartitionId != -1) { 
        pathsOut.add(new Path(partitionPath.getPath(),"part-" + NUMBER_FORMAT.format(restrictedPartitionId)));
      }
      else { 
        for (FileStatus part : fs.globStatus(new Path(partitionPath.getPath(),"part-*"))) { 
          pathsOut.add(part.getPath());
        }
      }
    }
  }
  
  public void addWikipediaPaths(Configuration conf,int restrictedPartitionId,ArrayList<Path> pathsOut) throws IOException { 
    // get partitioned list path 
    Path wikipediaURLSPath = getOutputDirForStep(PartitionWikipediaUrlsStep.class);
    FileSystem fs = FileSystem.get(wikipediaURLSPath.toUri(),conf);
    if (restrictedPartitionId != -1) { 
      pathsOut.add(new Path(wikipediaURLSPath,"part-" + NUMBER_FORMAT.format(restrictedPartitionId)));
    }
    else { 
      for (FileStatus part : fs.globStatus(new Path(wikipediaURLSPath,"part-*"))) {
        pathsOut.add(part.getPath());
      }
    }
  }
  
    
  @Override
  public void runStep(Path outputPathLocation) throws IOException {
    LOG.info("Task Identity Path is:" + getTaskIdentityPath());
    LOG.info("Temp Path is:" + outputPathLocation);
    
    DomainMetadataTask rootTask = (DomainMetadataTask) getRootTask();
    
    Configuration conf = new Configuration(rootTask.getConf());
    
    // check for restricted partition id ... 
    int restrictedPartitionId = rootTask.getConf().getInt(SINGLE_PARTITION_PROPERTY, -1);

    // collect paths ... 
    ArrayList<Path> paths = new ArrayList<Path>();
    
    addCrawlListPaths(conf,restrictedPartitionId,paths);
    addWikipediaPaths(conf,restrictedPartitionId,paths);
    
    JobConf jobConf = new JobBuilder("Generate Bundles", getConf())

    .inputs(paths)
    .inputFormat(MultiFileMergeInputFormat.class)
    .mapperKeyValue(IntWritable.class, Text.class)
    .outputKeyValue(SegmentGeneratorBundleKey.class, SegmentGeneratorItemBundle.class)
    .outputIsSeqFile()
    .reducer(BundleGenerator.class, false)
    .partition(MultiFileMergePartitioner.class)
    .speculativeExecution(false)
    .output(outputPathLocation)
    .compressMapOutput(false).compressor(CompressionType.BLOCK, SnappyCodec.class)
    .build();

    jobConf.setBoolean(MultiFileMergeInputFormat.PARTS_ARE_FILES_PROPERTY,true);
    
    if (restrictedPartitionId != -1) { 
      jobConf.setNumReduceTasks(1);
    }
    else { 
      jobConf.setNumReduceTasks(CrawlListGenCommon.NUM_LIST_PARTITIONS);
    }
    
    LOG.info("Starting JOB");
    JobClient.runJob(jobConf);
    LOG.info("Finsihed JOB");
    
  }

  public static class BundleGenerator implements Reducer<IntWritable,Text,SegmentGeneratorBundleKey,SegmentGeneratorItemBundle> {
    Configuration _conf;
    
    boolean _skipDomain = false;
    boolean _currentRootDomainIdValid = false;
    long _currentRootDomainId = -1;
    boolean _currentSubDomainIdValid = false;
    boolean _genHomePageURLForSubDomain = false;
    long _currentSubDomainId = -1;
    int _currentRootDomainURLCount = 0;
    int _currentRootDomainSpilledItemCount = 0;
    // spill state ...
    ArrayList<SegmentGeneratorItem> items = new ArrayList<SegmentGeneratorItem>();
    int currentDomainCrawlIdx = -1;
    SegmentGeneratorItemBundle currentBundle = null;
    double accumulatedRank = 0.0;
    int currentBundleId = 0;
    OutputCollector<SegmentGeneratorBundleKey, SegmentGeneratorItemBundle> _collector = null;
    int crawlerCount = CrawlEnvironment.NUM_CRAWLERS;
    
    static final int NUM_HASH_FUNCTIONS = 10;
    static final int NUM_BITS = 11;
    static final int NUM_ELEMENTS = 1 << 28;
    static final int FLUSH_THRESHOLD = 1 << 23;
    
    URLFPBloomFilter emittedTuplesFilter = new URLFPBloomFilter(NUM_ELEMENTS, NUM_HASH_FUNCTIONS, NUM_BITS);
    long urlsInFilter = 0;
    
    @Override
    public void configure(JobConf job) {
      _conf = job;
    }

    @Override
    public void close() throws IOException {
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

    @SuppressWarnings("resource")
    Pair<FileSystem,List<Path>> buildInputPathList(Configuration conf,Iterator<Text> values)throws IOException { 
      // collect all incoming paths first
      ArrayList<Path> incomingPaths = Lists.newArrayList();
      
      Set<String> fsType = new HashSet<String>();
      
      while(values.hasNext()){ 
        String path = values.next().toString();
        LOG.info("Found Incoming Path:" + path);
        incomingPaths.add(new Path(path));
        // convert to uri ... 
        URI uri = new Path(path).toUri();
        // get scheme if present ... 
        String scheme = uri.getScheme();
        if (scheme == null || scheme.length() == 0) { 
          fsType.add("default");
        }
        else { 
          fsType.add(scheme);
        }
      }
      
      if (fsType.size() != 1) { 
        throw new IOException("Only One Input Scheme at a time supported!");
      }
      
      // determine filesytem 
      FileSystem fs = null;
      if (fsType.contains("s3n")) { 
        fs = new S3NFileSystem();
        fs.initialize(incomingPaths.get(0).toUri(), conf);
      }
      else { 
        fs = FileSystem.get(incomingPaths.get(0).toUri(), conf);
      }
      return new Pair<FileSystem, List<Path>>(fs,incomingPaths);
    }
    
    static class RawValueIterator implements Iterator<TextBytes>  {

      CrawlListKey key = new CrawlListKey();
      TextBytes valueBytes = new TextBytes();
      DataInputBuffer keyInputBuffer = new DataInputBuffer();
      DataInputBuffer inputBuffer = new DataInputBuffer();
      Path currentSource = null;
      
      Iterator<RawRecordValue> rawIterator;
      void reset(Iterable<RawRecordValue> rawIterable) { 
        this.rawIterator = rawIterable.iterator();
      }
      
      @Override
      public boolean hasNext() {
        return rawIterator.hasNext();
      }
      
      CrawlListKey currentKey() { 
        return key;
      }
      
      Path currentSource() { 
        return currentSource;
      }

      @Override
      public TextBytes next(){
        try { 
          RawRecordValue nextRawValue = rawIterator.next();
          // read in text bytes key ... 
          keyInputBuffer.reset(nextRawValue.key.getData(),0,nextRawValue.key.getLength());
          inputBuffer.reset(nextRawValue.data.getData(),0,nextRawValue.data.getLength());
          int valueTextLen = WritableUtils.readVInt(inputBuffer);
          valueBytes.set(nextRawValue.data.getData(),inputBuffer.getPosition(),valueTextLen);
          key.readFields(keyInputBuffer);
          currentSource = nextRawValue.source;
          
          return valueBytes;
        }
        catch (IOException e) { 
          LOG.error(CCStringUtils.stringifyException(e));
          throw new RuntimeException(e);
        }
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("remove");
      } 
    }
    

    
    /** helper method **/
    private SegmentGeneratorItemBundle getBundleForDomain(long domainFP) throws IOException {

      currentBundle = new SegmentGeneratorItemBundle();
      currentBundle.setHostFP(domainFP);

      return currentBundle;
    }

    
    /** generate a bundle from the given list of items and simultaneously flush it **/
    private void generateABundle(long domainFP, List<SegmentGeneratorItem> items, Reporter reporter) throws IOException {

      SegmentGeneratorItemBundle bundle = getBundleForDomain(domainFP);

      // LOG.info("Generating Bundle:" + currentBundleId + " for DH:" + domainFP);
      float maxPageRank = 0.0f;
      for (SegmentGeneratorItem item : items) {
        // LOG.info("URL:" + item.getUrl() + " Status:" +
        // CrawlDatum.getStatusName(item.getStatus()) +" PR:" +
        // item.getMetadata().getPageRank());
        bundle.getUrls().add(item);
        _currentRootDomainURLCount++;
        maxPageRank = Math.max(maxPageRank, item.getPageRank());
      }
      // LOG.info("Done Generating Bunlde - PR is:" + maxPageRank);

      // set page rank for bundle
      bundle.setMaxPageRank(maxPageRank);

      flushCurrentBundle(reporter);
    }

    
    /** flush the currently active bundle **/
    private void flushCurrentBundle(Reporter reporter) throws IOException {
      if (currentBundle != null && currentBundle.getUrls().size() != 0) {
        int crawlerIndex = (((Long)currentBundle.getHostFP()).hashCode() & Integer.MAX_VALUE) % crawlerCount;
        // generate a bundle key
        SegmentGeneratorBundleKey bundleKey = new SegmentGeneratorBundleKey();

        bundleKey.setRecordType(0);
        bundleKey.setCrawlerId(crawlerIndex);
        bundleKey.setDomainFP(_currentRootDomainId);
        // and increment bundle id ...
        bundleKey.setBundleId(currentBundleId++);
        bundleKey.setAvgPageRank((float) accumulatedRank / (float)currentBundle.getUrls().size());

        if (reporter != null) {
          reporter.incrCounter("CRAWLER_", Long.toString(crawlerIndex) + "_BUNDLE_COUNT", 1);
        }

        // ok spill bundle ...
        _collector.collect(bundleKey, currentBundle);
      }
      // current bundle is now null
      currentBundle = null;
      accumulatedRank = 0.0;
    }


    /** spill cached items **/
    private void spillItems(Reporter reporter) throws IOException {
      // if item count exceeds spill threshold .. or we ran out of data ...
      if (items.size() != 0) {
        // LOG.info("Spilling Bundle:" + currentBundleId + " for DH:" +
        // currentDomain + " ItemCount:" + subList.size());
        // flush items
        generateABundle(_currentRootDomainId, items, reporter);
        if (reporter != null) {
          reporter.progress();
        }
        // ok, increment counts ...
        _currentRootDomainSpilledItemCount += items.size();

        //if (_currentRootDomainSpilledItemCount >= 1000000) {
          reporter.incrCounter(Counters.SPILLED_1_MILLION_SKIPPED_REST, 1);
          //_skipDomain = true;
        //}
      }
      // reset list ...
      items.clear();
    }

    
    private void flushRootDomain(Reporter reporter) throws IOException {
      if (items.size() != 0) {
        spillItems(reporter);
      }

      if (reporter != null) {
        if (_currentRootDomainSpilledItemCount >= 10000000) {
          reporter.incrCounter(Counters.DOMAIN_WITH_GT_10MILLION_URLS, 1);
        } else if (_currentRootDomainSpilledItemCount >= 1000000) {
          reporter.incrCounter(Counters.DOMAIN_WITH_GT_1MILLION_URLS, 1);
        } else if (_currentRootDomainSpilledItemCount >= 100000) {
          reporter.incrCounter(Counters.DOMAIN_WITH_GT_100K_URLS, 1);
        } else if (_currentRootDomainSpilledItemCount >= 50000) {
          reporter.incrCounter(Counters.DOMAIN_WITH_GT_50K_URLS, 1);
        } else if (_currentRootDomainSpilledItemCount >= 10000) {
          reporter.incrCounter(Counters.DOMAIN_WITH_GT_10K_URLS, 1);
        } else if (_currentRootDomainSpilledItemCount >= 1000) {
          reporter.incrCounter(Counters.DOMAIN_WITH_GT_1K_URLS, 1);
        } else if (_currentRootDomainSpilledItemCount >= 100) {
          reporter.incrCounter(Counters.DOMAIN_WITH_GT_100_URLS, 1);
        } else if (_currentRootDomainSpilledItemCount >= 10) {
          reporter.incrCounter(Counters.DOMAIN_WITH_GT_10_URLS, 1);
        } else if (_currentRootDomainSpilledItemCount > 1) {
          reporter.incrCounter(Counters.DOMAIN_WITH_LT_10_URLS, 1);
        } else if (_currentRootDomainSpilledItemCount == 1) {
          reporter.incrCounter(Counters.DOMAIN_WITH_1_URL, 1);
        }
      }

      int crawlerIndex = (((Long)_currentRootDomainId).hashCode() & Integer.MAX_VALUE) % crawlerCount;
      
      if (reporter != null) {
        reporter.incrCounter("CRAWLER_", Long.toString(crawlerIndex), 1);
      }

      
      _currentRootDomainIdValid = false;
      _currentRootDomainId = -1;
      _currentSubDomainIdValid = false;
      _genHomePageURLForSubDomain = true;
      _currentSubDomainId = -1;
      currentDomainCrawlIdx = -1;
      _currentRootDomainSpilledItemCount = 0;
      _currentRootDomainURLCount = 0;
    }
    
    /** potentially reset state based on domain id transition **/
    private void rootDomainTransition(long newDomainFP,Reporter reporter) throws IOException {
      if (_currentRootDomainIdValid) {
        flushRootDomain(reporter);
      }

      _skipDomain = false;

      // zero out item count ...
      items.clear();
      // reset domain id
      _currentRootDomainId = newDomainFP;
      _currentRootDomainIdValid = true;
      currentDomainCrawlIdx = (((int) _currentRootDomainId & Integer.MAX_VALUE) % crawlerCount);
      // reset current domain url count
      _currentRootDomainURLCount = 0;
      // and reset last bundle id
      currentBundleId = 0;
      // reset spill count for domain
      _currentRootDomainSpilledItemCount = 0;
    }
    
    Set<String> validSchemes = new ImmutableSet.Builder<String>()
        .add("http")
        .add("https")
        .build();
    
    //static Set<String> = ImmutableSet.Builder<String> 
    
    static String makeHomePageURLFromUrlObject(GoogleURL urlObject) {
      String urlOut = urlObject.getScheme();
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
    
    void emitURLObject(GoogleURL urlObject,JsonObject originalJSON,float rank,Reporter reporter)throws IOException { 
      URLFPV2 fp = URLUtils.getURLFPV2FromURLObject(urlObject);
      if (fp != null) {
        if (emittedTuplesFilter.isPresent(fp)) {
          reporter.incrCounter(Counters.SKIPPING_ALREADY_EMITTED_URL, 1);
        } else {
          
          reporter.incrCounter(Counters.EMITTING_URL_OBJECT, 1);
          
          emittedTuplesFilter.add(fp);
          urlsInFilter++;

          SegmentGeneratorItem itemValue = new SegmentGeneratorItem();

          itemValue.setDomainFP(fp.getDomainHash());
          itemValue.setRootDomainFP(fp.getRootDomainHash());
          itemValue.setUrlFP(fp.getUrlHash());
          itemValue.setUrl(urlObject.getCanonicalURL());
          itemValue.setPageRank(rank);
          itemValue.setModifiedStatus((byte) 0);
          
          if (originalJSON != null) { 
            if (originalJSON.has(CrawlListGenCommon.CRAWLLIST_METADATA_ETAG)) { 
              itemValue.setEtag(originalJSON.get(CrawlListGenCommon.CRAWLLIST_METADATA_ETAG).getAsString());
            }
            if (originalJSON.has(CrawlListGenCommon.CRAWLLIST_METADATA_LAST_MODIFIED_TIME)){
              itemValue.setLastModifiedTime(originalJSON.get(CrawlListGenCommon.CRAWLLIST_METADATA_LAST_MODIFIED_TIME).getAsLong());
            }
          }
          
          items.add(itemValue);

          if (items.size() >= SPILL_THRESHOLD)
            spillItems(reporter);

        }
      } else {
        reporter.incrCounter(Counters.NULL_FP_FOR_URL, 1);
      }
      
    }
    
    void emitURL(String url,float rank, JsonObject originalJSON, Reporter reporter)throws IOException { 
      GoogleURL urlObject = new GoogleURL(url);
      if (urlObject.isValid()) { 
        String scheme = urlObject.getScheme().toLowerCase();
        if (!validSchemes.contains(scheme)) { 
          reporter.incrCounter(Counters.INVALID_SCHEME, 1);
        }
        else { 
          if (_genHomePageURLForSubDomain) {
            reporter.incrCounter(Counters.GENERATING_HOME_PAGE_URL, 1);
            _genHomePageURLForSubDomain = false;
            // generate homepage url ... 
            String homePageURL = makeHomePageURLFromUrlObject(urlObject);
            if (homePageURL != null) { 
              GoogleURL homePageURLObj = new GoogleURL(homePageURL);
              if (homePageURLObj.isValid()) { 
                emitURLObject(homePageURLObj, null, 10000.00f, reporter);
              }
            }
          }
          
          emitURLObject(urlObject, originalJSON, rank, reporter);
        }
      }
      else { 
        reporter.incrCounter(Counters.INVALID_URL_OBJECT, 1);
      }
    }
    
    @Override
    public void reduce(IntWritable key,Iterator<Text> values,OutputCollector<SegmentGeneratorBundleKey, SegmentGeneratorItemBundle> output,Reporter reporter) throws IOException {
      
      // set up merge attributes
      Configuration localMergeConfig = new Configuration(_conf);

      localMergeConfig.setClass(MultiFileInputReader.MULTIFILE_COMPARATOR_CLASS, CrawlListKey.CrawListKeyComparator.class,
          RawComparator.class);
      localMergeConfig.setClass(MultiFileInputReader.MULTIFILE_KEY_CLASS, CrawlListKey.class, WritableComparable.class);
      
      // ingest input paths .  
      Pair<FileSystem,List<Path>> fileSystemPathTuple = buildInputPathList(localMergeConfig, values);
      
      RawValueIterator rawValueIterator = new RawValueIterator();

      JsonParser parser = new JsonParser();
      
      _collector = output;
      
      // startup merger ...
      LOG.info("FileSystem is:" + fileSystemPathTuple.e0);
      LOG.info("Merger Input Paths are:" + fileSystemPathTuple.e1);
      MultiFileInputReader<CrawlListKey> multiFileInputReader = new MultiFileInputReader<CrawlListKey>(fileSystemPathTuple.e0, fileSystemPathTuple.e1, localMergeConfig);

      try { 
        Pair<KeyAndValueData<CrawlListKey>,Iterable<RawRecordValue>> nextItem = null;

        // walk tuples and feed them to the actual reducer ...  
        while ((nextItem = multiFileInputReader.getNextItemIterator()) != null) {
          reporter.incrCounter(Counters.GET_NEXT_RECORD_FROM_MERGER, 1);
          // check the current domain id to see if need to do a domain transition  
          long newRootDomainId = nextItem.e0._keyObject.partitionDomainKey;
          if (!_currentRootDomainIdValid || newRootDomainId != _currentRootDomainId) { 
            // domain transition detected ... 
            rootDomainTransition(newRootDomainId, reporter);
          }
          long newSubDomainId = nextItem.e0._keyObject.comparisonDomainKey;
          // now check for subdomain transition ... 
          if (!_currentSubDomainIdValid || newSubDomainId != _currentSubDomainId) {
            _currentSubDomainId = newSubDomainId;
            _genHomePageURLForSubDomain = true;
          }
          
          // reset values iterator ...  
          rawValueIterator.reset(nextItem.e1);
          
          while (rawValueIterator.hasNext()) {
            reporter.incrCounter(Counters.GOT_RAW_RECORD_FROM_ITERATOR, 1);
            //LOG.info("Got Record From Source:" + rawValueIterator.currentSource);
            String json = rawValueIterator.next().toString();
            JsonObject jsonObj = parser.parse(json).getAsJsonObject();
            if (jsonObj.has(CrawlDBCommon.TOPLEVEL_SOURCE_URL_PROPRETY)) {
              emitURL(
                  jsonObj.get(CrawlDBCommon.TOPLEVEL_SOURCE_URL_PROPRETY).getAsString(),
                  (float)rawValueIterator.currentKey().rank0,
                  jsonObj,
                  reporter);
            }
            else { 
              reporter.incrCounter(Counters.NO_SOURCE_URL_IN_JSON, 1);
            }
          }
          reporter.progress();
        }
        // flush trailing domain
        rootDomainTransition(Long.MAX_VALUE, reporter);
      }
      finally { 
        multiFileInputReader.close();
      }
        
    } 
    
  }
}
