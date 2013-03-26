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
package org.commoncrawl.mapred.pipelineV3.domainmeta.rank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.mapred.ec2.postprocess.linkCollector.LinkKey;
import org.commoncrawl.mapred.ec2.postprocess.linkCollector.LinkKey.LinkKeyGroupingComparator;
import org.commoncrawl.mapred.ec2.postprocess.crawldb.CrawlDBKey;
import org.commoncrawl.mapred.ec2.postprocess.crawldb.CrawlDBKey.CrawlDBKeyGroupingComparator;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.FlexBuffer;
import org.commoncrawl.util.GoogleURL;
import org.commoncrawl.util.JobBuilder;
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
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * 
 * @author rana
 *
 */
public class LinkScannerStep extends CrawlPipelineStep implements Reducer<IntWritable, Text, TextBytes, TextBytes> {

  enum Counters {
    FAILED_TO_GET_LINKS_FROM_HTML, NO_HREF_FOR_HTML_LINK, EXCEPTION_IN_MAP, GOT_HTML_METADATA, GOT_FEED_METADATA,
    EMITTED_ATOM_LINK, EMITTED_HTML_LINK, EMITTED_RSS_LINK, GOT_PARSED_AS_ATTRIBUTE, GOT_LINK_OBJECT,
    NULL_CONTENT_OBJECT, NULL_LINKS_ARRAY, FP_NULL_IN_EMBEDDED_LINK, SKIPPED_ALREADY_EMITTED_LINK,
    FOUND_HTTP_DATE_HEADER, FOUND_HTTP_AGE_HEADER, FOUND_HTTP_LAST_MODIFIED_HEADER, FOUND_HTTP_EXPIRES_HEADER,
    FOUND_HTTP_CACHE_CONTROL_HEADER, FOUND_HTTP_PRAGMA_HEADER, REDUCER_GOT_LINK, REDUCER_GOT_STATUS,
    ONE_REDUNDANT_LINK_IN_REDUCER, TWO_REDUNDANT_LINKS_IN_REDUCER, THREE_REDUNDANT_LINKS_IN_REDUCER,
    GT_THREE_REDUNDANT_LINKS_IN_REDUCER, ONE_REDUNDANT_STATUS_IN_REDUCER, TWO_REDUNDANT_STATUS_IN_REDUCER,
    THREE_REDUNDANT_STATUS_IN_REDUCER, GT_THREE_REDUNDANT_STATUS_IN_REDUCER, GOT_RSS_FEED, GOT_ATOM_FEED,
    GOT_ALTERNATE_LINK_FOR_ATOM_ITEM, GOT_CONTENT_FOR_ATOM_ITEM, GOT_ITEM_LINK_FROM_RSS_ITEM,
    GOT_TOP_LEVEL_LINK_FROM_RSS_ITEM, GOT_TOP_LEVEL_LINK_FROM_ATOM_ITEM, EMITTED_REDIRECT_RECORD, DISCOVERED_NEW_LINK,
    GOT_LINK_FOR_ITEM_WITH_STATUS, FAILED_TO_GET_SOURCE_HREF, GOT_CRAWL_STATUS_NO_LINK, GOT_CRAWL_STATUS_WITH_LINK,
    GOT_EXTERNAL_DOMAIN_SOURCE, NO_SOURCE_URL_FOR_CRAWL_STATUS, OUTPUT_KEY_FROM_INTERNAL_LINK,
    OUTPUT_KEY_FROM_EXTERNAL_LINK, FOUND_HTML_LINKS, FOUND_FEED_LINKS, HAD_OUTLINK_DATA, HAD_NO_OUTLINK_DATA
  }

  public static final String OUTPUT_DIR_NAME = "linkScannerOutput";

  private static final Log LOG = LogFactory.getLog(RankTask.class);

  public static final int NUM_HASH_FUNCTIONS = 10;

  public static final int NUM_BITS = 11;

  public static final int NUM_ELEMENTS = 1 << 28;

  static final String stripWWW(String host) {
    if (host.startsWith("www.")) {
      return host.substring("www.".length());
    }
    return host;
  }

  private URLFPBloomFilter emittedTuplesFilter = new URLFPBloomFilter(NUM_ELEMENTS, NUM_HASH_FUNCTIONS, NUM_BITS);
  static final Path internalMergedSegmentPath = new Path("crawl/ec2Import/mergedSegment");

  static List<Path> filterMergeCandidtes(FileSystem fs, Configuration conf, long latestMergeDBTimestamp)
      throws IOException {
    ArrayList<Path> list = new ArrayList<Path>();
    FileStatus candidates[] = fs.globStatus(new Path(internalMergedSegmentPath, "[0-9]*"));

    for (FileStatus candidate : candidates) {
      long candidateTimestamp = Long.parseLong(candidate.getPath().getName());
      if (candidateTimestamp > latestMergeDBTimestamp) {
        list.add(candidate.getPath());
      }
    }
    return list;
  }

  JsonParser parser = new JsonParser();

  String outputKeyString = null;
  boolean outputKeyFromInternalLink = false;

  GoogleURL outputKeyURLObj = null;

  long latestLinkDataTime = -1L;
  TextBytes sourceDomain = new TextBytes();
  ArrayList<String> outlinks = new ArrayList<String>();
  TreeSet<Long> discoveredLinks = new TreeSet<Long>();
  JsonObject toJsonObject = new JsonObject();
  JsonObject fromJsonObject = new JsonObject();
  URLFPV2 bloomKey = new URLFPV2();
  JobConf _jobConf;

  public LinkScannerStep() {
    super(null, null, null);
  }

  public LinkScannerStep(CrawlPipelineTask task) {
    super(task, "Link Scanner Step", OUTPUT_DIR_NAME);
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void configure(JobConf job) {
    _jobConf = job;
  }

  @Override
  public Log getLogger() {
    return LOG;
  }

  @Override
  public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<TextBytes, TextBytes> output,
      Reporter reporter) throws IOException {
    // collect all incoming paths first
    Vector<Path> incomingPaths = new Vector<Path>();

    FlexBuffer scanArray[] = CrawlDBKey.allocateScanArray();

    while (values.hasNext()) {
      String path = values.next().toString();
      LOG.info("Found Incoming Path:" + path);
      incomingPaths.add(new Path(path));
    }

    // set up merge attributes
    Configuration localMergeConfig = new Configuration(_jobConf);

    localMergeConfig.setClass(MultiFileInputReader.MULTIFILE_COMPARATOR_CLASS, CrawlDBKeyGroupingComparator.class,
        RawComparator.class);
    localMergeConfig.setClass(MultiFileInputReader.MULTIFILE_KEY_CLASS, TextBytes.class, WritableComparable.class);

    // ok now spawn merger
    MultiFileInputReader<TextBytes> multiFileInputReader = new MultiFileInputReader<TextBytes>(
        FileSystem.get(_jobConf), incomingPaths, localMergeConfig);

    TextBytes keyBytes = new TextBytes();
    TextBytes valueBytes = new TextBytes();
    DataInputBuffer inputBuffer = new DataInputBuffer();
    TextBytes valueOut = new TextBytes();
    TextBytes keyOut = new TextBytes();

    Pair<KeyAndValueData<TextBytes>, Iterable<RawRecordValue>> nextItem = null;

    // pick up source fp from key ...
    URLFPV2 fpSource = new URLFPV2();

    while ((nextItem = multiFileInputReader.getNextItemIterator()) != null) {

      outputKeyString = null;
      outputKeyFromInternalLink = false;
      outputKeyURLObj = null;
      latestLinkDataTime = -1L;
      outlinks.clear();
      discoveredLinks.clear();

      // scan key components
      CrawlDBKey.scanForComponents(nextItem.e0._keyObject, ':', scanArray);

      // setup fingerprint ...
      fpSource.setRootDomainHash(CrawlDBKey.getLongComponentFromComponentArray(scanArray,
          CrawlDBKey.ComponentId.ROOT_DOMAIN_HASH_COMPONENT_ID));
      fpSource.setDomainHash(CrawlDBKey.getLongComponentFromComponentArray(scanArray,
          CrawlDBKey.ComponentId.DOMAIN_HASH_COMPONENT_ID));
      fpSource.setUrlHash(CrawlDBKey.getLongComponentFromComponentArray(scanArray,
          CrawlDBKey.ComponentId.URL_HASH_COMPONENT_ID));

      for (RawRecordValue rawValue : nextItem.e1) {

        inputBuffer.reset(rawValue.key.getData(), 0, rawValue.key.getLength());
        int length = WritableUtils.readVInt(inputBuffer);
        keyBytes.set(rawValue.key.getData(), inputBuffer.getPosition(), length);
        inputBuffer.reset(rawValue.data.getData(), 0, rawValue.data.getLength());
        length = WritableUtils.readVInt(inputBuffer);
        valueBytes.set(rawValue.data.getData(), inputBuffer.getPosition(), length);

        long linkType = CrawlDBKey.getLongComponentFromKey(keyBytes, CrawlDBKey.ComponentId.TYPE_COMPONENT_ID);

        if (linkType == CrawlDBKey.Type.KEY_TYPE_CRAWL_STATUS.ordinal()) {
          try {
            JsonObject object = parser.parse(valueBytes.toString()).getAsJsonObject();
            if (object != null) {
              updateCrawlStatsFromJSONObject(object, fpSource, reporter);
            }
          } catch (Exception e) {
            LOG.error("Error Parsing JSON:" + valueBytes.toString());
            throw new IOException(e);
          }
        }
        reporter.progress();
      }
      // ok now see if we have anything to emit ...
      if (discoveredLinks.size() != 0) {
        reporter.incrCounter(Counters.HAD_OUTLINK_DATA, 1);
        for (String outlink : outlinks) {
          // emit a to tuple
          toJsonObject.addProperty("to", outlink);
          valueBytes.set(toJsonObject.toString());
          output.collect(sourceDomain, valueBytes);
          // now emit a from tuple ...
          fromJsonObject.addProperty("from", sourceDomain.toString());
          keyBytes.set(outlink);
          valueBytes.set(fromJsonObject.toString());
          output.collect(keyBytes, valueBytes);
        }

        bloomKey.setDomainHash(fpSource.getDomainHash());

        for (long destDomainFP : discoveredLinks) {
          // set the bloom filter key ...
          bloomKey.setUrlHash(destDomainFP);
          // add it to the bloom filter
          emittedTuplesFilter.add(bloomKey);
        }
      } else {
        reporter.incrCounter(Counters.HAD_NO_OUTLINK_DATA, 1);
      }
    }
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {
    LOG.info("Task Identity Path is:" + getTaskIdentityPath());
    LOG.info("Temp Path is:" + outputPathLocation);

    ImmutableList<Path> paths = new ImmutableList.Builder<Path>().addAll(
        filterMergeCandidtes(getFileSystem(), getConf(), 0)).build();

    // establish an affinity path ...
    Path affinityPath = paths.get(0);

    JobConf jobConf = new JobBuilder("Link Scanner Job", getConf())

    .inputs(paths).inputFormat(MultiFileMergeInputFormat.class).mapperKeyValue(IntWritable.class, Text.class)
        .outputKeyValue(TextBytes.class, TextBytes.class).outputFormat(SequenceFileOutputFormat.class).reducer(
            LinkScannerStep.class, false).partition(MultiFileMergePartitioner.class).numReducers(
            CrawlEnvironment.NUM_DB_SHARDS).speculativeExecution(false).output(outputPathLocation)
        .setAffinityNoBalancing(affinityPath, ImmutableSet.of("ccd001.commoncrawl.org", "ccd006.commoncrawl.org"))
        .compressMapOutput(false).compressor(CompressionType.BLOCK, SnappyCodec.class)

        .build();

    LOG.info("Starting JOB");
    JobClient.runJob(jobConf);
    LOG.info("Finsihed JOB");
  }

  void updateCrawlStatsFromJSONObject(JsonObject jsonObject, URLFPV2 fpSource, Reporter reporter) throws IOException {

    JsonElement sourceHREFElement = jsonObject.get("source_url");

    if (sourceHREFElement != null) {
      if (outputKeyString == null || !outputKeyFromInternalLink) {
        outputKeyString = sourceHREFElement.getAsString();
        outputKeyURLObj = new GoogleURL(sourceHREFElement.getAsString());
      }
      String disposition = jsonObject.get("disposition").getAsString();
      long attemptTime = jsonObject.get("attempt_time").getAsLong();

      if (latestLinkDataTime == -1 || attemptTime > latestLinkDataTime) {

        if (disposition.equals("SUCCESS")) {

          int httpResult = jsonObject.get("http_result").getAsInt();

          if (httpResult == 200) {
            outputKeyFromInternalLink = true;
          }

          if (httpResult == 200) {

            JsonElement parsedAs = jsonObject.get("parsed_as");

            if (parsedAs != null) {

              String parsedAsString = parsedAs.getAsString();

              // if html ...
              if (parsedAsString.equals("html")) {
                JsonObject content = jsonObject.get("content").getAsJsonObject();
                if (content != null) {
                  JsonArray links = content.getAsJsonArray("links");
                  if (links != null) {
                    reporter.incrCounter(Counters.FOUND_HTML_LINKS, 1);
                    if (updateLinkStatsFromLinksArray(links, content, fpSource, reporter)) {
                      latestLinkDataTime = attemptTime;
                    }
                  }
                }
              } else if (parsedAsString.equals("feed")) {
                JsonObject content = jsonObject.get("content").getAsJsonObject();
                if (content != null) {
                  JsonArray items = content.getAsJsonArray("items");
                  if (items != null) {
                    for (JsonElement item : items) {
                      JsonObject linkContent = item.getAsJsonObject().getAsJsonObject("content");
                      if (linkContent != null) {
                        JsonArray links = linkContent.getAsJsonArray("links");
                        if (links != null) {
                          reporter.incrCounter(Counters.FOUND_FEED_LINKS, 1);
                          if (updateLinkStatsFromLinksArray(links, content, fpSource, reporter)) {
                            latestLinkDataTime = attemptTime;
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }

      }
    } else {
      reporter.incrCounter(Counters.NO_SOURCE_URL_FOR_CRAWL_STATUS, 1);
    }
  }

  boolean updateLinkStatsFromLinksArray(JsonArray links, JsonObject content, URLFPV2 fpSource, Reporter reporter) {

    if (links == null) {
      reporter.incrCounter(Counters.NULL_LINKS_ARRAY, 1);
    } else {
      // ok clear existing data ...
      discoveredLinks.clear();
      outlinks.clear();

      sourceDomain.set(stripWWW(outputKeyURLObj.getHost()));
      // walk links ...
      for (JsonElement link : links) {
        JsonObject linkObj = link.getAsJsonObject();
        if (linkObj != null && linkObj.has("href")) {
          JsonElement rel = linkObj.get("rel");
          if (rel == null || rel.getAsString().indexOf("nofollow") == -1) {
            String href = linkObj.get("href").getAsString();
            GoogleURL destURLObject = new GoogleURL(href);
            if (destURLObject.isValid()) {
              URLFPV2 linkFP = URLUtils.getURLFPV2FromURLObject(destURLObject);
              if (linkFP != null) {
                if (linkFP.getRootDomainHash() != fpSource.getRootDomainHash()
                    || linkFP.getDomainHash() != fpSource.getDomainHash()) {
                  // ok create our fake bloom key (hack) ...
                  // we set domain hash to be source domain's domain hash
                  bloomKey.setDomainHash(fpSource.getDomainHash());
                  // and url hash to dest domains domain hash
                  bloomKey.setUrlHash(linkFP.getDomainHash());
                  // the bloom filter is global to the reducer, so doing a check
                  // against it before emitting a tuple allows us to limit the
                  // number
                  // of redundant tuples produced by the reducer (a serious
                  // problem)
                  if (!emittedTuplesFilter.isPresent(bloomKey)) {
                    // one more level of check before we emit a tuple ...
                    if (!discoveredLinks.contains(linkFP.getDomainHash())) {
                      // add to discovered links ...
                      discoveredLinks.add(linkFP.getDomainHash());
                      // populate the tuple and add it
                      outlinks.add(stripWWW(destURLObject.getHost()));

                      // we don't add to the bloomfilter until we commit (write)
                      // this data ...
                    }
                  }
                }
              }
            }
          }
        }
      }
      return true;
    }
    return false;
  }

}
