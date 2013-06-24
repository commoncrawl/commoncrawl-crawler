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
import org.commoncrawl.mapred.ec2.postprocess.crawldb.CrawlDBCommon;
import org.commoncrawl.mapred.ec2.postprocess.crawldb.CrawlDBKey;
import org.commoncrawl.mapred.ec2.postprocess.crawldb.CrawlDBKey.CrawlDBKeyGroupByURLComparator;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.DomainMetadataTask;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.ByteArrayUtils;
import org.commoncrawl.util.CCStringUtils;
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
  , FOUND_LINK_RECORD_BUT_NO_MERGE_RECORD, EXCEPTION_IN_MERGE, BAD_SOURCE_URL, NO_SOURCE_URL_IN_MERGE_RECORD}

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

  JsonParser parser = new JsonParser();
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
  public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<TextBytes, TextBytes> output,Reporter reporter) throws IOException {
    // collect all incoming paths first
    Vector<Path> incomingPaths = new Vector<Path>();

    while (values.hasNext()) {
      String path = values.next().toString();
      LOG.info("Found Incoming Path:" + path);
      incomingPaths.add(new Path(path));
    }

    // set up merge attributes
    Configuration localMergeConfig = new Configuration(_jobConf);

    localMergeConfig.setClass(MultiFileInputReader.MULTIFILE_COMPARATOR_CLASS, CrawlDBKeyGroupByURLComparator.class,
        RawComparator.class);
    localMergeConfig.setClass(MultiFileInputReader.MULTIFILE_KEY_CLASS, TextBytes.class, WritableComparable.class);

    FileSystem fs = FileSystem.get(incomingPaths.get(0).toUri(),_jobConf);
    
    // ok now spawn merger
    MultiFileInputReader<TextBytes> multiFileInputReader 
      = new MultiFileInputReader<TextBytes>(fs, incomingPaths, localMergeConfig);

    try { 
      DataInputBuffer mergeDataBuffer = new DataInputBuffer();
      @SuppressWarnings("resource")
      DataInputBuffer linkDataBuffer = new DataInputBuffer();
      boolean foundMergeRecord = false;
      boolean foundLinksRecord = false;
      TextBytes mergeBytes = new TextBytes();
      TextBytes keyBytes = new TextBytes();
      TextBytes valueBytes = new TextBytes();
      DataInputBuffer keyInputBuffer = new DataInputBuffer();
      TextBytes outputValueBytes = new TextBytes();
  
      Pair<KeyAndValueData<TextBytes>, Iterable<RawRecordValue>> nextItem = null;
  
      while ((nextItem = multiFileInputReader.getNextItemIterator()) != null) {
  
        foundMergeRecord = false;
        foundLinksRecord = false;
  
        // walk records 
        for (RawRecordValue rawValue : nextItem.e1) {
          // init key buffer 
          keyInputBuffer.reset(rawValue.key.getData(),0,rawValue.key.getLength());
          keyBytes.setFromRawTextBytes(keyInputBuffer);
          // scan key components
          long recordType = CrawlDBKey.getLongComponentFromKey(keyBytes, CrawlDBKey.ComponentId.TYPE_COMPONENT_ID);
          
          if (recordType == CrawlDBKey.Type.KEY_TYPE_INCOMING_URLS_SAMPLE.ordinal()) { 
            foundLinksRecord = true;
            linkDataBuffer.reset(rawValue.data.getData(), rawValue.data.getLength());
          }
          else if (recordType == CrawlDBKey.Type.KEY_TYPE_MERGED_RECORD.ordinal()) { 
            foundMergeRecord = true;
            mergeDataBuffer.reset(rawValue.data.getData(),rawValue.data.getLength());
          }
          
          if (foundLinksRecord && foundMergeRecord) {
            mergeBytes.setFromRawTextBytes(mergeDataBuffer);
            try { 
              JsonObject mergeRecord = parser.parse(mergeBytes.toString()).getAsJsonObject();
              if (mergeRecord.has(CrawlDBCommon.TOPLEVEL_SOURCE_URL_PROPRETY)) { 
                String sourceURL = mergeRecord.get(CrawlDBCommon.TOPLEVEL_SOURCE_URL_PROPRETY).getAsString();
                GoogleURL sourceURLObj = new GoogleURL(sourceURL);
                
                if (sourceURLObj.isValid()) {
                  
                  // set key bytes ... 
                  keyBytes.set(stripWWW(sourceURLObj.getHost()));
                  
                  int curpos = 0;
                  int endpos = linkDataBuffer.getLength();
                  
                  byte lfPattern[] = { 0xA };
                  byte tabPattern[] = { 0x9 };
                  
                  while (curpos != endpos) { 
                    int tabIndex = ByteArrayUtils.indexOf(linkDataBuffer.getData(), curpos, endpos - curpos, tabPattern);
                    if (tabIndex == -1) { 
                      break;
                    }
                    else { 
                      int lfIndex = ByteArrayUtils.indexOf(linkDataBuffer.getData(), tabIndex + 1, endpos - (tabIndex + 1), lfPattern);
                      if (lfIndex == -1) { 
                        break;
                      }
                      else {
                        // skip the source domain hash 
                        //long sourceDomainHash = ByteArrayUtils.parseLong(inputData.getBytes(),curpos, tabIndex-curpos, 10);
                        
                        // get source url  
                        valueBytes.set(linkDataBuffer.getData(),tabIndex + 1,lfIndex - (tabIndex + 1));
                        // convert to url object 
                        GoogleURL urlObject = new GoogleURL(valueBytes.toString());
                        if (urlObject.isValid()) { 
                          String hostName = stripWWW(urlObject.getHost());
                          // now emit a from tuple ...
                          fromJsonObject.addProperty("from", hostName);
                          outputValueBytes.set(fromJsonObject.toString());
                          // emit tuple 
                          output.collect(keyBytes, outputValueBytes);
                        }
                        
                        curpos = lfIndex + 1;
                      }
                    }
                  }                
                }
                else { 
                  reporter.incrCounter(Counters.BAD_SOURCE_URL, 1);
                }
              }
              else { 
                reporter.incrCounter(Counters.NO_SOURCE_URL_IN_MERGE_RECORD, 1);
              }
            }
            catch (Exception e) { 
              reporter.incrCounter(Counters.EXCEPTION_IN_MERGE, 1);
              LOG.error(CCStringUtils.stringifyException(e));
            }
            
          }
          else if (foundLinksRecord && !foundMergeRecord) { 
            // record the anomaly ... 
            reporter.incrCounter(Counters.FOUND_LINK_RECORD_BUT_NO_MERGE_RECORD, 1);
          }
        }
      }
    }
    finally { 
      multiFileInputReader.close();
    }
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {
    LOG.info("Task Identity Path is:" + getTaskIdentityPath());
    LOG.info("Temp Path is:" + outputPathLocation);
    
    DomainMetadataTask rootTask = (DomainMetadataTask) getRootTask();
    
    ImmutableList<Path> paths = new ImmutableList.Builder<Path>().addAll(rootTask.getRestrictedMergeDBDataPaths()).build();

    JobConf jobConf = new JobBuilder("Link Scanner Job", getConf())

    .inputs(paths)
    .inputFormat(MultiFileMergeInputFormat.class)
    .mapperKeyValue(IntWritable.class, Text.class)
    .outputKeyValue(TextBytes.class, TextBytes.class)
    .outputFormat(SequenceFileOutputFormat.class)
    .reducer(LinkScannerStep.class, false)
    .partition(MultiFileMergePartitioner.class)
    .numReducers(CrawlEnvironment.NUM_DB_SHARDS)
    .speculativeExecution(false)
    .output(outputPathLocation)
    .compressMapOutput(false).compressor(CompressionType.BLOCK, SnappyCodec.class)
    .build();

    LOG.info("Starting JOB");
    JobClient.runJob(jobConf);
    LOG.info("Finsihed JOB");
  }
}
