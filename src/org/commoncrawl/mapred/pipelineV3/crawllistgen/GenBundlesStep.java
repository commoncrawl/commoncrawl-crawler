/**
 * Copyright 2012 - CommonCrawl Foundation
 * 
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 **/
package org.commoncrawl.mapred.pipelineV3.crawllistgen;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.mapred.SegmentGeneratorBundleKey;
import org.commoncrawl.mapred.SegmentGeneratorItem;
import org.commoncrawl.mapred.SegmentGeneratorItemBundle;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.GoogleURL;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.SuperDomainList;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLFPBloomFilter;
import org.commoncrawl.util.URLUtils;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileMergeInputFormat;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileMergePartitioner;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader.KeyAndValueData;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader.RawRecordValue;
import org.commoncrawl.util.Tuples.Pair;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * 
 * @author rana
 *
 */
public class GenBundlesStep extends CrawlPipelineStep implements
    Reducer<IntWritable, Text, SegmentGeneratorBundleKey, SegmentGeneratorItemBundle> {

  enum Counters {
    GOT_RECORD, GOT_CRAWLSTATS, GOT_HOMEPAGE_DATA, GOT_BLOGPROBE_DATA, GOT_CRAWLURL_DATA, EMITTED_URL_RECORD,
    EMITTING_URL_RECORD_WITH_NULL_DOMAINSTATS, EMITTING_URL_RECORD_WITH_DOMINSTATS, EMITTED_RECORD_HAD_CRAWLSTATUS,
    EMITTED_RECORD_HAD_NULL_CRAWLSTATUS, GOT_FEEDURL_DATA, DOMAIN_WITH_GT_10MILLION_URLS, DOMAIN_WITH_GT_1MILLION_URLS,
    DOMAIN_WITH_GT_100K_URLS, DOMAIN_WITH_GT_10K_URLS, DOMAIN_WITH_GT_50K_URLS, DOMAIN_WITH_GT_1K_URLS,
    DOMAIN_WITH_GT_100_URLS, DOMAIN_WITH_GT_10_URLS, DOMAIN_WITH_LT_10_URLS, DOMAIN_WITH_1_URL, GOT_REDIRECT_DATA,
    SKIPPING_REDIRECTED_URL, SKIPPING_ALREADY_FETCHED, ALLOWING_HOMEPAGE_OR_FEEDURL, SKIPPING_BLOGPROBE_URL,
    RECRAWLING_BLOGPROBE_URL, SKIPPING_INVALID_URL, SKIPPING_BAD_DOMAIN_BASED_ON_CRAWL_HISTORY,
    SKIPPING_DOMAIN_EXCEEDED_URL_COUNT_AND_LOW_DR, SKIPPING_BAD_DOMAIN_URL, SKIPPING_EVERYTHING_BUT_HOMEPAGE_URL,
    SKIPPING_QUERY_URL, NULL_FP_FOR_URL, SKIPPING_ALREADY_EMITTED_URL, FLUSHED_BLOOMFILTER, SKIPPING_BLOCKED_DOMAIN,
    LET_THROUGH_QUERY_URL, HIT_QUERY_CHECK_CONDITION, SKIPPING_INVALID_LENGTH_URL, TRANSITIONING_DOMAIN,
    SPILLED_1_MILLION_SKIPPED_REST, SKIPPING_IP_ADDRESS

  }

  private static final Log LOG = LogFactory.getLog(GenBundlesStep.class);

  public static final String OUTPUT_DIR_NAME = "bundlesGenerator";

  static final int NUM_BITS = 11;

  static final int NUM_ELEMENTS = 1 << 28;

  static final int FLUSH_THRESHOLD = 1 << 23;

  public static final int SPILL_THRESHOLD = 250;

  private static void rawValueToTextBytes(DataOutputBuffer dataBuffer, DataInputBuffer inputBuffer, TextBytes textOut)
      throws IOException {
    inputBuffer.reset(dataBuffer.getData(), dataBuffer.getLength());
    int newLength = WritableUtils.readVInt(inputBuffer);
    textOut.set(inputBuffer.getData(), inputBuffer.getPosition(), newLength);
  }

  private static void rawValueToWritable(RawRecordValue rawValue, DataInputBuffer inputBuffer, Writable typeOut)
      throws IOException {
    inputBuffer.reset(rawValue.data.getData(), rawValue.data.getLength());
    typeOut.readFields(inputBuffer);
  }

  JsonParser parser = new JsonParser();

  public static final int HAS_HOMEPAGE_URLDATA = 2;
  public static final int HAS_BLOGPROBE_URLDATA = 4;
  public static final int HAS_FEED_URLDATA = 8;
  public static final int HAS_CRAWL_STATUS = 16;
  public static final int HAS_REDIRECT_DATA = 32;

  static final int NUM_HASH_FUNCTIONS = 10;
  int _flags = 0;
  boolean _skipDomain = false;
  boolean _skipEverythingButHomepage = false;

  TextBytes _newDomainBytes = new TextBytes();

  TextBytes _contextURLBytes = new TextBytes();

  TextBytes _newURLBytes = new TextBytes();
  BooleanWritable _blogURLSkipFlag = new BooleanWritable(true);
  TextBytes tempTextBuffer = new TextBytes();
  DataInputBuffer tempBuffer = new DataInputBuffer();
  JsonObject _domainStats = null;
  double _domainRank = 0.0;
  JsonObject _crawlStatus = null;
  URLFPBloomFilter _emittedURLSFilter;
  long _emittedURLSInFilter = 0;
  // spill state ...
  ArrayList<SegmentGeneratorItem> items = new ArrayList<SegmentGeneratorItem>();
  long _currentDomainId = -1;
  String currentDomainName = "";
  int currentDomainURLCount = 0;
  int currentDomainSpilledItemCount = 0;

  int currentDomainCrawlIdx = -1;
  SegmentGeneratorItemBundle currentBundle = null;
  int currentBundleId = 0;
  OutputCollector<SegmentGeneratorBundleKey, SegmentGeneratorItemBundle> _collector = null;
  int crawlerCount = 0;
  Pattern ipAddressRegExPattern = Pattern.compile("[0-9]*\\.[0-9]*\\.[0-9]*\\.[0-9]*");
  URLFPV2 fpTest = new URLFPV2();
  JobConf _jobConf = null;
  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }

  Writer urlDebugURLWriter;

  FSDataOutputStream debugURLStream;

  int partitionNumber;

  public GenBundlesStep() {
    super(null, null, null);
  }

  public GenBundlesStep(CrawlPipelineTask task) {
    super(task, "Generate Bundles", OUTPUT_DIR_NAME);
  }

  @Override
  public void close() throws IOException {

    flushDomain(null);

    urlDebugURLWriter.flush();
    debugURLStream.close();

  }

  public void configure(JobConf job) {

    _jobConf = job;

    crawlerCount = job.getInt(CrawlEnvironment.PROPERTY_NUM_CRAWLERS, CrawlEnvironment.CRAWLERS.length);

    partitionNumber = job.getInt("mapred.task.partition", -1);

    try {
      FileSystem fs = FileSystem.get(job);
      Path workPath = FileOutputFormat.getOutputPath(job);
      debugURLStream = fs.create(new Path(workPath, "debugURLS-" + NUMBER_FORMAT.format(partitionNumber)));
      urlDebugURLWriter = new OutputStreamWriter(debugURLStream, Charset.forName("UTF-8"));
      _emittedURLSFilter = new URLFPBloomFilter(NUM_ELEMENTS, NUM_HASH_FUNCTIONS, NUM_BITS);
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new RuntimeException(e);
    }

  }

  /** potentially reset state based on domain id transition **/
  private void domainTransition(long newDomainFP, String newDomainName, Reporter reporter) throws IOException {
    if (_currentDomainId != -1) {
      flushDomain(reporter);
    }

    _flags = 0;
    _domainStats = null;
    _domainRank = 0.0;
    _skipDomain = false;
    _skipEverythingButHomepage = false;

    // zero out item count ...
    items.clear();
    // reset domain id
    _currentDomainId = newDomainFP;
    currentDomainCrawlIdx = (((int) _currentDomainId & Integer.MAX_VALUE) % crawlerCount);
    // reset current domain url count
    currentDomainURLCount = 0;
    currentDomainName = newDomainName;
    // and reset last bundle id
    currentBundleId = 0;
    // reset spill count for domain
    currentDomainSpilledItemCount = 0;

    if (BlockedDomainList.blockedDomains.contains(newDomainFP)) {
      reporter.incrCounter(Counters.SKIPPING_BLOCKED_DOMAIN, 1);
      LOG.info("Skipping Blocked Domain:" + newDomainName);
      _skipDomain = true;
    }

    if (ipAddressRegExPattern.matcher(currentDomainName.trim()).matches()) {
      reporter.incrCounter(Counters.SKIPPING_IP_ADDRESS, 1);
      _skipDomain = true;
    }
  }

  void emitLastRecord(Reporter reporter) throws IOException {

    if (_flags != 0) {
      if (_domainStats == null) {
        reporter.incrCounter(Counters.EMITTING_URL_RECORD_WITH_NULL_DOMAINSTATS, 1);
      } else {
        reporter.incrCounter(Counters.EMITTING_URL_RECORD_WITH_DOMINSTATS, 1);
      }

      if (_crawlStatus != null) {
        reporter.incrCounter(Counters.EMITTED_RECORD_HAD_CRAWLSTATUS, 1);
      } else {
        reporter.incrCounter(Counters.EMITTED_RECORD_HAD_NULL_CRAWLSTATUS, 1);
      }
    }

    if (_contextURLBytes.getLength() >= 4097) {
      reporter.incrCounter(Counters.SKIPPING_INVALID_LENGTH_URL, 1);
    } else {
      GoogleURL urlObject = new GoogleURL(_contextURLBytes.toString());

      if (!skipRecord(urlObject, reporter)) {

        if (urlObject.has_query()) {
          reporter.incrCounter(Counters.LET_THROUGH_QUERY_URL, 1);
        }

        URLFPV2 fp = URLUtils.getURLFPV2FromURLObject(urlObject);
        if (fp != null) {
          if (_emittedURLSFilter.isPresent(fp)) {
            reporter.incrCounter(Counters.SKIPPING_ALREADY_EMITTED_URL, 1);
          } else {
            _emittedURLSFilter.add(fp);
            _emittedURLSInFilter++;

            SegmentGeneratorItem itemValue = new SegmentGeneratorItem();

            itemValue.setDomainFP(fp.getDomainHash());
            itemValue.setRootDomainFP(fp.getRootDomainHash());
            itemValue.setUrlFP(fp.getUrlHash());
            itemValue.setUrl(urlObject.getCanonicalURL());
            itemValue.setPageRank(0);
            itemValue.setModifiedStatus((byte) 0);

            items.add(itemValue);

            if (items.size() >= SPILL_THRESHOLD)
              spillItems(reporter);

          }
        } else {
          reporter.incrCounter(Counters.NULL_FP_FOR_URL, 1);
        }

      }
    }

    // reset stuff
    _flags = 0;
    _crawlStatus = null;
    _contextURLBytes.clear();
    _blogURLSkipFlag.set(true);
  }

  /** flush the currently active bundle **/
  void flushCurrentBundle(Reporter reporter) throws IOException {
    if (currentBundle != null && currentBundle.getUrls().size() != 0) {
      int crawlerIndex = ((int) currentBundle.getHostFP() & Integer.MAX_VALUE) % crawlerCount;
      // generate a bundle key
      SegmentGeneratorBundleKey bundleKey = new SegmentGeneratorBundleKey();

      bundleKey.setRecordType(0);
      bundleKey.setCrawlerId(crawlerIndex);
      bundleKey.setDomainFP(_currentDomainId);
      // and increment bundle id ...
      bundleKey.setBundleId(currentBundleId++);
      bundleKey.setAvgPageRank((float) _domainRank);

      if (reporter != null) {
        reporter.incrCounter("CRAWLER", Long.toString(crawlerIndex), 1);
      }

      // ok spill bundle ...
      _collector.collect(bundleKey, currentBundle);
    }
    // current bundle is now null
    currentBundle = null;
  }

  private void flushDomain(Reporter reporter) throws IOException {
    if (_currentDomainId != -1) {
      if (items.size() != 0) {
        spillItems(reporter);
      }

      if (reporter != null) {
        if (currentDomainSpilledItemCount >= 10000000) {
          reporter.incrCounter(Counters.DOMAIN_WITH_GT_10MILLION_URLS, 1);
        } else if (currentDomainSpilledItemCount >= 1000000) {
          reporter.incrCounter(Counters.DOMAIN_WITH_GT_1MILLION_URLS, 1);
        } else if (currentDomainSpilledItemCount >= 100000) {
          reporter.incrCounter(Counters.DOMAIN_WITH_GT_100K_URLS, 1);
        } else if (currentDomainSpilledItemCount >= 50000) {
          reporter.incrCounter(Counters.DOMAIN_WITH_GT_50K_URLS, 1);
        } else if (currentDomainSpilledItemCount >= 10000) {
          reporter.incrCounter(Counters.DOMAIN_WITH_GT_10K_URLS, 1);
        } else if (currentDomainSpilledItemCount >= 1000) {
          reporter.incrCounter(Counters.DOMAIN_WITH_GT_1K_URLS, 1);
        } else if (currentDomainSpilledItemCount >= 100) {
          reporter.incrCounter(Counters.DOMAIN_WITH_GT_100_URLS, 1);
        } else if (currentDomainSpilledItemCount >= 10) {
          reporter.incrCounter(Counters.DOMAIN_WITH_GT_10_URLS, 1);
        } else if (currentDomainSpilledItemCount > 1) {
          reporter.incrCounter(Counters.DOMAIN_WITH_LT_10_URLS, 1);
        } else if (currentDomainSpilledItemCount == 1) {
          reporter.incrCounter(Counters.DOMAIN_WITH_1_URL, 1);
        }
      }

      _currentDomainId = -1;
      currentDomainCrawlIdx = -1;
      currentDomainName = "";
      currentDomainSpilledItemCount = 0;
      currentDomainURLCount = 0;
    }
  }

  /** generate a bundle from the given list of items and simultaneously flush it **/
  void generateABundle(long domainFP, List<SegmentGeneratorItem> items, Reporter reporter) throws IOException {

    SegmentGeneratorItemBundle bundle = getBundleForDomain(domainFP);

    // LOG.info("Generating Bundle:" + currentBundleId + " for DH:" + domainFP);
    float maxPageRank = 0.0f;
    for (SegmentGeneratorItem item : items) {
      // LOG.info("URL:" + item.getUrl() + " Status:" +
      // CrawlDatum.getStatusName(item.getStatus()) +" PR:" +
      // item.getMetadata().getPageRank());
      bundle.getUrls().add(item);
      currentDomainURLCount++;
      maxPageRank = Math.max(maxPageRank, item.getPageRank());

      if (currentDomainURLCount <= 200) {
        urlDebugURLWriter.append(item.getUrl() + "\t" + item.getModifiedStatus() + "\t" + item.getPageRank() + "\n");
      }
    }
    // LOG.info("Done Generating Bunlde - PR is:" + maxPageRank);

    // set page rank for bundle
    bundle.setMaxPageRank(maxPageRank);

    flushCurrentBundle(reporter);
  }

  /** helper method **/
  private SegmentGeneratorItemBundle getBundleForDomain(long domainFP) throws IOException {

    currentBundle = new SegmentGeneratorItemBundle();
    currentBundle.setHostFP(domainFP);

    return currentBundle;
  }

  @Override
  public Log getLogger() {
    return LOG;
  }

  void iterateItems(MultiFileInputReader<TextBytes> multiFileInputReader, Reporter reporter) throws IOException {

    Pair<KeyAndValueData<TextBytes>, Iterable<RawRecordValue>> nextItem = null;

    int iterationCount = 0;

    while ((nextItem = multiFileInputReader.getNextItemIterator()) != null) {

      reporter.incrCounter(Counters.GOT_RECORD, 1);

      int type = PartitionUtils.getTypeGivenPartitionKey(nextItem.e0._keyObject);
      PartitionUtils.getDomainGivenPartitionKey(nextItem.e0._keyObject, _newDomainBytes);
      PartitionUtils.getURLGivenPartitionKey(nextItem.e0._keyObject, _newURLBytes);

      if (_newURLBytes.compareTo(_contextURLBytes) != 0) {
        emitLastRecord(reporter);
      }

      long newDomainFP = SuperDomainList.domainFingerprintGivenName(_newDomainBytes.toString());

      if (newDomainFP != _currentDomainId) {
        reporter.incrCounter(Counters.TRANSITIONING_DOMAIN, 1);
        domainTransition(newDomainFP, _newDomainBytes.toString(), reporter);
      }

      RawRecordValue valueRaw = Iterables.getFirst(nextItem.e1, null);

      switch (type) {

        case CrawlListGeneratorTask.KEY_TYPE_CRAWLSTATS: {
          reporter.incrCounter(Counters.GOT_CRAWLSTATS, 1);
          setDomainStats(rawValueToJsonObject(valueRaw.data, tempBuffer, tempTextBuffer), reporter);
        }
          break;

        case CrawlListGeneratorTask.KEY_TYPE_HOMEPAGE_URL: {
          reporter.incrCounter(Counters.GOT_HOMEPAGE_DATA, 1);
          rawValueToTextBytes(valueRaw.data, tempBuffer, _contextURLBytes);
          _flags |= HAS_HOMEPAGE_URLDATA;
        }
          break;

        case CrawlListGeneratorTask.KEY_TYPE_BLOGPROBE_URL: {
          reporter.incrCounter(Counters.GOT_BLOGPROBE_DATA, 1);
          rawValueToWritable(valueRaw, tempBuffer, _blogURLSkipFlag);
          _contextURLBytes.set(_newURLBytes);
          _flags |= HAS_BLOGPROBE_URLDATA;
        }
          break;

        case CrawlListGeneratorTask.KEY_TYPE_FEED_URL: {
          reporter.incrCounter(Counters.GOT_FEEDURL_DATA, 1);
          rawValueToTextBytes(valueRaw.data, tempBuffer, _contextURLBytes);
          _flags |= HAS_FEED_URLDATA;
        }
          break;

        case CrawlListGeneratorTask.KEY_TYPE_REDIRECT_RECORD: {
          reporter.incrCounter(Counters.GOT_REDIRECT_DATA, 1);
          _contextURLBytes.set(_newURLBytes);
          _flags |= HAS_REDIRECT_DATA;
        }
          break;

        case CrawlListGeneratorTask.KEY_TYPE_CRAWLDATA: {
          reporter.incrCounter(Counters.GOT_CRAWLURL_DATA, 1);
          _contextURLBytes.set(_newURLBytes);
          _crawlStatus = rawValueToJsonObject(valueRaw.data, tempBuffer, tempTextBuffer);
          _flags |= HAS_CRAWL_STATUS;
        }
          break;
      }
    }
    // flush trailing record ...
    emitLastRecord(reporter);
    flushDomain(reporter);
  }

  private JsonObject rawValueToJsonObject(DataOutputBuffer dataBuffer, DataInputBuffer stream, TextBytes tempTextBuffer)
      throws IOException {
    rawValueToTextBytes(dataBuffer, stream, tempTextBuffer);
    try {
      return parser.parse(tempTextBuffer.toString()).getAsJsonObject();
    } catch (Exception e) {
      throw new IOException("Exception Building Json from String:" + tempTextBuffer.toString());
    }
  }

  @Override
  public void reduce(IntWritable key, Iterator<Text> values,
      OutputCollector<SegmentGeneratorBundleKey, SegmentGeneratorItemBundle> output, Reporter reporter)
      throws IOException {
    // collect all incoming paths first
    Vector<Path> incomingPaths = new Vector<Path>();

    while (values.hasNext()) {
      String path = values.next().toString();
      LOG.info("Found Incoming Path:" + path);
      incomingPaths.add(new Path(path));
    }

    // set up merge attributes
    Configuration localMergeConfig = new Configuration(_jobConf);

    localMergeConfig.setClass(MultiFileInputReader.MULTIFILE_COMPARATOR_CLASS, TextBytes.Comparator.class,
        TextBytes.Comparator.class);
    localMergeConfig.setClass(MultiFileInputReader.MULTIFILE_KEY_CLASS, TextBytes.class, WritableComparable.class);

    // ok now spawn merger
    MultiFileInputReader<TextBytes> multiFileInputReader = new MultiFileInputReader<TextBytes>(
        FileSystem.get(_jobConf), incomingPaths, localMergeConfig);

    // save a reference to the collector
    _collector = output;

    iterateItems(multiFileInputReader, reporter);
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {

    ImmutableList<Path> pathList = ImmutableList.of(getOutputDirForStep(PartitionCrawlDBStep.class),
        getOutputDirForStep(GenFeedUrlsStep.class), getOutputDirForStep(GenHomepageUrlsStep.class),
        getOutputDirForStep(GenBlogPlatformUrlsStep.class), getOutputDirForStep(PartitionCrawlStatsStep.class),
        getOutputDirForStep(PartitionRedirectDataStep.class));

    JobConf jobConf = new JobBuilder("BundleWriter Step", getConf())

    .inputs(pathList).inputFormat(MultiFileMergeInputFormat.class).mapperKeyValue(IntWritable.class, Text.class)
        .outputKeyValue(SegmentGeneratorBundleKey.class, SegmentGeneratorItemBundle.class).outputFormat(
            SequenceFileOutputFormat.class).reducer(GenBundlesStep.class, false).partition(
            MultiFileMergePartitioner.class).numReducers(CrawlListGeneratorTask.NUM_SHARDS).speculativeExecution(false)
        .output(outputPathLocation).setAffinityNoBalancing(getOutputDirForStep(PartitionCrawlDBStep.class),
            ImmutableSet.of("ccd001.commoncrawl.org", "ccd006.commoncrawl.org")).compressMapOutput(false).compressor(
            CompressionType.BLOCK, SnappyCodec.class)

        .build();

    LOG.info("Starting JOB");
    JobClient.runJob(jobConf);
    LOG.info("Finsihed JOB");
  }

  void setDomainStats(JsonObject domainStats, Reporter reporter) throws IOException {

    _domainStats = domainStats;
    if (_domainStats.has("dR")) {
      _domainRank = _domainStats.get("dR").getAsDouble();
    } else {
      _domainRank = 0.0;
    }

    if (_domainStats.has("urls")) {
      int urlCount = _domainStats.get("urls").getAsInt();
      int crawledCount = _domainStats.get("crawled").getAsInt();
      int Http200Count = (_domainStats.has("200")) ? _domainStats.get("200").getAsInt() : 0;
      if (urlCount != 0 && crawledCount != 0 && Http200Count == 0) {
        reporter.incrCounter(Counters.SKIPPING_BAD_DOMAIN_BASED_ON_CRAWL_HISTORY, 1);
        LOG.info("Skipping Everything But Homepage for Domain:" + _newDomainBytes.toString() + " CrawledCount:"
            + crawledCount + " HTTP200Count:" + Http200Count + " URLCount:" + urlCount);
        _skipEverythingButHomepage = true;
      } else if (urlCount > 25000 && urlCount < 100000) {
        if (!_domainStats.has("dR") || _domainStats.get("dR").getAsDouble() < 3.0) {
          LOG.info("Skipping Domain:" + _newDomainBytes.toString());
          reporter.incrCounter(Counters.SKIPPING_DOMAIN_EXCEEDED_URL_COUNT_AND_LOW_DR, 1);
          _skipDomain = true;
        }
      } else if (urlCount > 250000 && urlCount < 1000000) {
        if (!_domainStats.has("dR") || _domainStats.get("dR").getAsDouble() < 4.0) {
          LOG.info("Skipping Domain:" + _newDomainBytes.toString());
          reporter.incrCounter(Counters.SKIPPING_DOMAIN_EXCEEDED_URL_COUNT_AND_LOW_DR, 1);
          _skipDomain = true;
        }
      } else if (urlCount > 1000000) {
        if (!_domainStats.has("dR") || _domainStats.get("dR").getAsDouble() < 5.0) {
          LOG.info("Skipping Domain:" + _newDomainBytes.toString());
          reporter.incrCounter(Counters.SKIPPING_DOMAIN_EXCEEDED_URL_COUNT_AND_LOW_DR, 1);
          _skipDomain = true;
        }
      }
    }
    if (_emittedURLSInFilter >= FLUSH_THRESHOLD) {
      _emittedURLSFilter.clear();
      _emittedURLSInFilter = 0;
      reporter.incrCounter(Counters.FLUSHED_BLOOMFILTER, 1);
    }
  }

  boolean skipRecord(GoogleURL urlObject, Reporter reporter) {

    if (_skipDomain) {
      reporter.incrCounter(Counters.SKIPPING_BAD_DOMAIN_URL, 1);
      return true;
    }

    if (!urlObject.isValid()) {
      reporter.incrCounter(Counters.SKIPPING_INVALID_URL, 1);
      return true;
    } else if (urlObject.has_query()) {
      reporter.incrCounter(Counters.HIT_QUERY_CHECK_CONDITION, 1);
      if ((_flags & (HAS_HOMEPAGE_URLDATA | HAS_FEED_URLDATA)) == 0) {
        reporter.incrCounter(Counters.SKIPPING_QUERY_URL, 1);
        return true;
      }
    } else {
      // if redirect ... skip
      if ((_flags & HAS_REDIRECT_DATA) != 0) {
        reporter.incrCounter(Counters.SKIPPING_REDIRECTED_URL, 1);
        return true;
      }

      if ((_flags & (HAS_HOMEPAGE_URLDATA | HAS_FEED_URLDATA)) != 0) {
        if (!_skipEverythingButHomepage || ((_flags & HAS_HOMEPAGE_URLDATA) != 0)) {
          reporter.incrCounter(Counters.ALLOWING_HOMEPAGE_OR_FEEDURL, 1);
          return false;
        }
      }

      if (_skipEverythingButHomepage) {
        reporter.incrCounter(Counters.SKIPPING_EVERYTHING_BUT_HOMEPAGE_URL, 1);
        return true;
      }

      if (_crawlStatus != null) {
        if (_crawlStatus.has("crawl_status")) {
          JsonObject realCrawlStatus = _crawlStatus.get("crawl_status").getAsJsonObject();
          if (realCrawlStatus.has("http_result")) {
            int httpResult = realCrawlStatus.get("http_result").getAsInt();
            if (httpResult == 200 || httpResult == 404) {
              if ((_flags & HAS_BLOGPROBE_URLDATA) != 0) {
                if (_blogURLSkipFlag.get()) {
                  reporter.incrCounter(Counters.SKIPPING_BLOGPROBE_URL, 1);
                  return true;
                } else {
                  reporter.incrCounter(Counters.RECRAWLING_BLOGPROBE_URL, 1);
                  return false;
                }
              } else {
                reporter.incrCounter(Counters.SKIPPING_ALREADY_FETCHED, 1);
                return true;
              }
            }
          }
        }
      }
    }
    return false;
  }

  /** spill cached items **/
  void spillItems(Reporter reporter) throws IOException {
    // if item count exceeds spill threshold .. or we ran out of data ...
    if (items.size() != 0) {
      // LOG.info("Spilling Bundle:" + currentBundleId + " for DH:" +
      // currentDomain + " ItemCount:" + subList.size());
      // flush items
      generateABundle(_currentDomainId, items, reporter);
      if (reporter != null) {
        reporter.progress();
      }
      // ok, increment counts ...
      currentDomainSpilledItemCount += items.size();

      if (currentDomainSpilledItemCount >= 1000000) {
        reporter.incrCounter(Counters.SPILLED_1_MILLION_SKIPPED_REST, 1);
        LOG.info("Skipping Remaining URLS for Domain:" + currentDomainName);
        _skipDomain = true;
      }
    }
    // reset list ...
    items.clear();
  }
}
