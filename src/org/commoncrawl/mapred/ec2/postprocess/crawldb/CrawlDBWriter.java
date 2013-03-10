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

package org.commoncrawl.mapred.ec2.postprocess.crawldb;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.commoncrawl.mapred.ec2.postprocess.crawldb.CrawlDBKey.ComponentId;
import org.commoncrawl.mapred.ec2.postprocess.crawldb.CrawlDBKey.CrawlDBKeyGroupingComparator;
import org.commoncrawl.mapred.ec2.postprocess.crawldb.CrawlDBKey.CrawlDBKeyPartitioner;
import org.commoncrawl.mapred.ec2.postprocess.crawldb.CrawlDBKey.LinkKeyComparator;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.ByteArrayUtils;
import org.commoncrawl.util.GoogleURL;
import org.commoncrawl.util.HttpHeaderInfoExtractor;
import org.commoncrawl.util.JSONUtils;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLFPBloomFilter;
import org.commoncrawl.util.URLUtils;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * map reduce job that produces a crawldb given link graph/crawl status data emitted
 * from both the LinkGraphDataEmitter job and previous runs of the CrawlDBWriter itself.
 * 
 * @author rana
 *
 */
public class CrawlDBWriter extends JSONUtils implements Reducer<TextBytes, TextBytes ,TextBytes,TextBytes> {

  static final Log LOG = LogFactory.getLog(CrawlDBWriter.class);

  // The crawldb job emits data in the form a JSON data structure
  // The top level JSON object contains optionally, a link_status object, a summary object 
  // and a source_url string. 
  // The Summary object has the properties defined by the SUMMARYRECORD_ constant prefix. 
  // The LinkStatus object has properties defined by the LINKSTATUS_ prefix
  // The Summary object can contain zero to N CrawlDetail objects, one for each 
  // crawl attempt. The properties defined by CrawlDetail object are prefixed with 
  // the CRAWLDETAIL_ prefix.
  
  public static final String TOPLEVEL_LINKSTATUS_PROPERTY = "link_status";
  public static final String TOPLEVEL_SUMMARYRECORD_PROPRETY = "crawl_status";
  public static final String TOPLEVEL_SOURCE_URL_PROPRETY = "source_url";
  
  public static final String RSS_MIN_PUBDATE_PROPERTY = "minPubDate";
  public static final String RSS_MAX_PUBDATE_PROPERTY = "maxPubDate";
  public static final String RSS_ITEM_COUNT_PROPERTY = "itemCount";
  
  public static final String SUMMARYRECORD_ATTEMPT_COUNT_PROPERTY = "attempt_count";
  public static final String SUMMARYRECORD_LATEST_ATTEMPT_PROPERTY = "latest_attempt";
  public static final String SUMMARYRECORD_HTTP_RESULT_PROPERTY = "http_result";
  public static final String SUMMARYRECORD_LATEST_CRAWLTIME_PROPERTY = "latest_crawl";
  public static final String SUMMARYRECORD_CRAWLCOUNT_PROPERTY = "crawl_count";
  public static final String SUMMARYRECORD_PARSEDAS_PROPERTY = "parsed_as";
  public static final String SUMMARYRECORD_CRAWLDETAILS_ARRAY_PROPERTY = "crawl_stats";
  public static final String SUMMARYRECORD_REDIRECT_URL_PROPERTY = "redirect_url";
  public static final String SUMMARYRECORD_EXTERNALLY_REFERENCED_URLS = "ext_urls";
  public static final String SUMMARYRECORD_EXTERNALLY_REFERENCED_URLS_TRUNCATED = "ext_urls_truncated";

  
  public static final String CRAWLDETAIL_FAILURE = "fetch_failed";
  public static final String CRAWLDETAIL_FAILURE_REASON  = "failure_reason";
  public static final String CRAWLDETAIL_FAILURE_DETAIL  = "failure_detail";
  public static final String CRAWLDETAIL_SERVERIP_PROPERTY = "server_ip";
  public static final String CRAWLDETAIL_HTTPRESULT_PROPERTY = "http_result";
  public static final String CRAWLDETAIL_REDIRECT_URL      = "redirect_url";
  public static final String CRAWLDETAIL_CONTENTLEN_PROPERTY = "content_len";
  public static final String CRAWLDETAIL_MIMETYPE_PROPERTY = "mime_type";
  public static final String CRAWLDETAIL_MD5_PROPERTY = "md5";
  public static final String CRAWLDETAIL_TEXTSIMHASH_PROPERTY = "text_simhash";
  public static final String CRAWLDETAIL_PARSEDAS_PROPERTY = "parsed_as";
  public static final String CRAWLDETAIL_TITLE_PROPERTY = "title";
  public static final String CRAWLDETAIL_METATAGS_PROPERTY = "meta_tags";
  public static final String CRAWLDETAIL_UPDATED_PROPERTY = "updated";
  public static final String CRAWLDETAIL_ATTEMPT_TIME_PROPERTY  = "attempt_time";
  public static final String CRAWLDETAIL_INTRADOMAIN_LINKS = "intra_domain_links";
  public static final String CRAWLDETAIL_INTRAROOT_LINKS = "intra_root_links";
  public static final String CRAWLDETAIL_INTERDOMAIN_LINKS = "inter_domain_links";

  public static final String CRAWLDETAIL_HTTP_DATE_PROPERTY = "date";
  public static final String CRAWLDETAIL_HTTP_AGE_PROPERTY = "age";
  public static final String CRAWLDETAIL_HTTP_LAST_MODIFIED_PROPERTY = "last-modified";
  public static final String CRAWLDETAIL_HTTP_EXPIRES_PROPERTY = "expires";
  public static final String CRAWLDETAIL_HTTP_CACHE_CONTROL_PROPERTY = "cache-control";
  public static final String CRAWLDETAIL_HTTP_PRAGMA_PROPERTY = "pragma";
  public static final String CRAWLDETAIL_HTTP_ETAG_PROPERTY = "etag";

  
  public static final String LINKSTATUS_INTRADOMAIN_SOURCES_COUNT_PROPERTY = "int_src_count";
  public static final String LINKSTATUS_EXTRADOMAIN_SOURCES_COUNT_PROPERTY = "ext_src_count";
  public static final String LINKSTATUS_EARLIEST_DATE_PROPERTY = "earliest_date";
  public static final String LINKSTATUS_LATEST_DATE_PROPERTY = "latest_date"; 
  public static final String LINKSTATUS_TYPEANDRELS_PROPERTY = "typeAndRels";

  
  
  
  ///////////////////////////////////////////////////////////////////////////
  // EC2 PATHS 
  ///////////////////////////////////////////////////////////////////////////
  static final String S3N_BUCKET_PREFIX = "s3n://aws-publicdatasets";
  static final String MERGE_INTERMEDIATE_OUTPUT_PATH = "/common-crawl/crawl-db/intermediate/";
  static final String MERGE_DB_PATH = "/common-crawl/crawl-db/mergedDB/";

  static final int MAX_TYPE_SAMPLES = 5;
  
  static final int DEFAULT_OUTGOING_URLS_BUFFER_SIZE = 1 << 18; // 262K 
  static final int DEFAULT_OUTGOING_URLS_BUFFER_PAD_AMOUNT = 16384;
  static final int DEFAULT_EXT_SOURCE_SAMPLE_BUFFER_SIZE = 1 << 27; // 134 MB
  static final int DEFAULT_EXT_SOURCE_SAMPLE_BUFFER_PAD_AMOUNT = 16384;
  static final int MAX_EXTERNALLY_REFERENCED_URLS = 100;
  
  private int OUTGOING_URLS_BUFFER_SIZE = DEFAULT_OUTGOING_URLS_BUFFER_SIZE;
  private int OUTGOING_URLS_BUFFER_PAD_AMOUNT =DEFAULT_OUTGOING_URLS_BUFFER_PAD_AMOUNT;
  private int EXT_SOURCE_SAMPLE_BUFFER_SIZE = DEFAULT_EXT_SOURCE_SAMPLE_BUFFER_SIZE;
  private int EXT_SOURCE_SAMPLE_BUFFER_PAD_AMOUNT = DEFAULT_EXT_SOURCE_SAMPLE_BUFFER_PAD_AMOUNT;
  
  ///////////////////////////////////////////////////////////////////////////
  // Counters 
  ///////////////////////////////////////////////////////////////////////////
  
  enum Counters { 
    FAILED_TO_GET_LINKS_FROM_HTML,
    NO_HREF_FOR_HTML_LINK,
    EXCEPTION_IN_MAP,
    GOT_HTML_METADATA,
    GOT_FEED_METADATA,
    EMITTED_ATOM_LINK,
    EMITTED_HTML_LINK,
    EMITTED_RSS_LINK,
    GOT_PARSED_AS_ATTRIBUTE,
    GOT_LINK_OBJECT,
    NULL_CONTENT_OBJECT,
    NULL_LINKS_ARRAY,
    FP_NULL_IN_EMBEDDED_LINK,
    SKIPPED_ALREADY_EMITTED_LINK,
    FOUND_HTTP_DATE_HEADER,
    FOUND_HTTP_AGE_HEADER,
    FOUND_HTTP_LAST_MODIFIED_HEADER,
    FOUND_HTTP_EXPIRES_HEADER,
    FOUND_HTTP_CACHE_CONTROL_HEADER,
    FOUND_HTTP_PRAGMA_HEADER,
    REDUCER_GOT_LINK,
    REDUCER_GOT_STATUS,
    ONE_REDUNDANT_LINK_IN_REDUCER,
    TWO_REDUNDANT_LINKS_IN_REDUCER,
    THREE_REDUNDANT_LINKS_IN_REDUCER,
    GT_THREE_REDUNDANT_LINKS_IN_REDUCER,
    ONE_REDUNDANT_STATUS_IN_REDUCER,
    TWO_REDUNDANT_STATUS_IN_REDUCER,
    THREE_REDUNDANT_STATUS_IN_REDUCER,
    GT_THREE_REDUNDANT_STATUS_IN_REDUCER,
    GOT_RSS_FEED,
    GOT_ATOM_FEED,
    GOT_ALTERNATE_LINK_FOR_ATOM_ITEM,
    GOT_CONTENT_FOR_ATOM_ITEM,
    GOT_ITEM_LINK_FROM_RSS_ITEM,
    GOT_TOP_LEVEL_LINK_FROM_RSS_ITEM,
    GOT_TOP_LEVEL_LINK_FROM_ATOM_ITEM,
    EMITTED_REDIRECT_RECORD,
    DISCOVERED_NEW_LINK,
    GOT_LINK_FOR_ITEM_WITH_STATUS,
    FAILED_TO_GET_SOURCE_HREF,
    GOT_CRAWL_STATUS_RECORD,
    GOT_EXTERNAL_DOMAIN_SOURCE,
    NO_SOURCE_URL_FOR_CRAWL_STATUS,
    OUTPUT_KEY_FROM_INTERNAL_LINK,
    OUTPUT_KEY_FROM_EXTERNAL_LINK, GOT_HTTP_200_CRAWL_STATUS, GOT_REDIRECT_CRAWL_STATUS, BAD_REDIRECT_URL, GOT_MERGED_RECORD, MERGED_OBJECT_FIRST_OBJECT, ADOPTED_SOURCE_SUMMARY_RECORD, MERGED_SOURCE_SUMMARY_RECORD_INTO_DEST, ADOPTED_SOURCE_LINKSUMMARY_RECORD, MERGED_SOURCE_LINKSUMMARY_RECORD_INTO_DEST, ALLOCATED_TOP_LEVEL_OBJECT_IN_FLUSH, ENCOUNTERED_EXISTING_TOP_LEVEL_OBJECT_IN_FLUSH, ENCOUNTERED_SUMMARY_RECORD_IN_FLUSH, ENCOUNTERED_LINKSUMMARY_RECORD_IN_FLUSH, EMITTED_SOURCEINPUTS_RECORD
    
  }
  
  ///////////////////////////////////////////////////////////////////////////
  // Data Members 
  ///////////////////////////////////////////////////////////////////////////

  public static final int  NUM_HASH_FUNCTIONS = 10;
  public static final int  NUM_BITS = 11;
  public static final int  NUM_ELEMENTS = 1 << 26;
  public static final int  FLUSH_INTERVAL = 1 << 17;

  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }    

  // parser 
  JsonParser _parser = new JsonParser();
  // the top level object 
  JsonObject _topLevelJSONObject;
  // the current summary record ... 
  JsonObject _summaryRecord = null;
  // the current link summary record 
  JsonObject _linkSummaryRecord = null;
  // collection of types detected for current url 
  HashSet<String> _types = new HashSet<String>();
  // collection of external references urls in current document  
  HashSet<String> _extHrefs = new HashSet<String>();
  // the url string to use as the output key ... 
  String _outputKeyString = null;
  // freeze url key ...
  boolean _urlKeyForzen = false;
  // url object representing the current key 
  GoogleURL _outputKeyURLObj = null;
  // source inputs tracking bloomfilter 
  URLFPBloomFilter _sourceInputsTrackingFilter;
  // a count of the number of urls processed 
  long _urlsProcessed = 0;
  // key used to test bloomfilter 
  URLFPV2 _bloomFilterKey = new URLFPV2();
  // captured job conf
  JobConf _conf;
  // file system 
  FileSystem _fs;
  //SequenceFile.Writer _redirectWriter = null;
  // input buffer used to collect referencing urls  
  DataOutputBuffer _sourceInputsBuffer; 
  // count of referencing domains 
  int              _sourceSampleSize = 0;
  // current input key 
  URLFPV2 _currentKey = null;
  // temporary key used to transition input keys 
  URLFPV2 _tempKey = new URLFPV2();
  // cached collector pointer ... 
  OutputCollector<TextBytes, TextBytes> _outputCollector;
  Reporter _reporter;
  
  /** 
   * The CrawlDBWriter job can be spawned via the command line 
   * 
   * @param args
   * @throws Exception
   */
  public static void main(String[] args)throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(new URI(S3N_BUCKET_PREFIX),conf);
    
    // establish merge timestamp 
    long mergeTimesmap = System.currentTimeMillis();
    // write into final output path  
    Path finalOutputPath = new Path(S3N_BUCKET_PREFIX + MERGE_DB_PATH,Long.toString(mergeTimesmap));

    
    // find latest merge timestamp ... 
    long latestMergeDBTimestamp = findLatestMergeDBTimestamp(fs, conf);
    LOG.info("Latest MergeDB Timestmap is:" + latestMergeDBTimestamp);
    // find list of merge candidates ... 
    List<Path> candidateList = filterMergeCandidtes(fs, conf, latestMergeDBTimestamp);
    LOG.info("Merge Candidate List is:" + candidateList);
    if (candidateList.size() != 0) { 
      ArrayList<Path> inputPaths = new ArrayList<Path>();
      
      // add all input paths to list 
      inputPaths.addAll(candidateList);
      // add merge db path if it exists 
      if (latestMergeDBTimestamp != -1L) {
        inputPaths.add(new Path(S3N_BUCKET_PREFIX + MERGE_DB_PATH,Long.toString(latestMergeDBTimestamp)));
      }
            
      JobConf jobConf = new JobBuilder("Final Merge Job", conf)
      .inputs(inputPaths)
      .inputFormat(SequenceFileInputFormat.class)
      .mapperKeyValue(TextBytes.class, TextBytes.class)
      .outputKeyValue(TextBytes.class, TextBytes.class)
      .outputFormat(SequenceFileOutputFormat.class)
      .reducer(CrawlDBWriter.class,false)
      .partition(CrawlDBKeyPartitioner.class)
      .sort(LinkKeyComparator.class)
      .group(CrawlDBKeyGroupingComparator.class)
      .numReducers(10000)
      .speculativeExecution(true)
      .output(finalOutputPath)
      .compressMapOutput(true)
      .compressor(CompressionType.BLOCK, SnappyCodec.class)
      .build();
            
      LOG.info("Starting JOB");
      JobClient.runJob(jobConf);      
    }
  }
  
  @Override
  public void reduce(TextBytes keyBytes, Iterator<TextBytes> values,OutputCollector<TextBytes, TextBytes> output, Reporter reporter)throws IOException {
    
    if (_outputCollector == null) { 
      _outputCollector = output;
      _reporter = reporter;
    }
    
    // potentially transition to new url
    readFPCheckForTransition(keyBytes,output,reporter);
    
    // extract link type .. 
    long linkType = CrawlDBKey.getLongComponentFromKey(keyBytes,CrawlDBKey.ComponentId.TYPE_COMPONENT_ID);
    
    while (values.hasNext()) {  
      
      TextBytes valueBytes = values.next();
      
      //LOG.debug("ValueBytes:"+ valueBytes.toString());
      
      if (linkType == CrawlDBKey.Type.KEY_TYPE_MERGED_RECORD.ordinal()) {
        reporter.incrCounter(Counters.GOT_MERGED_RECORD, 1);
        JsonObject mergedObject = _parser.parse(valueBytes.toString()).getAsJsonObject();
        if (mergedObject != null) { 
          setSourceURLFromJSONObject(mergedObject,linkType);
          processMergedRecord(mergedObject,_currentKey,reporter);
        }
      }
      else if (linkType == CrawlDBKey.Type.KEY_TYPE_CRAWL_STATUS.ordinal()) {
        
          reporter.incrCounter(Counters.GOT_CRAWL_STATUS_RECORD,1);
          try { 
            JsonObject object = _parser.parse(valueBytes.toString()).getAsJsonObject();
            if (object != null) {
              // update url key if necessary ... 
              setSourceURLFromJSONObject(object,linkType);
              // emit a redirect record if necessary ... 
              JsonElement redirectObject = object.get("redirect_from");
              if (redirectObject != null) {
                emitRedirectRecord(object, redirectObject.getAsJsonObject(),output, reporter);
              }
              
              // get latest crawl time
              long latestCrawlTime = (_summaryRecord != null) ? safeGetLong(_summaryRecord,SUMMARYRECORD_LATEST_CRAWLTIME_PROPERTY) : -1;
              long   attemptTime = safeGetLong(object, "attempt_time");
              // if this is the latest crawl event, then we want to track the links associated with this crawl status ... 
              HashSet<String> extHrefs = (attemptTime > latestCrawlTime) ? _extHrefs : null;
              // create a crawl detail record from incoming JSON 
              JsonObject crawlDetail = crawlDetailRecordFromCrawlStatusRecord(object,_currentKey,extHrefs,reporter);
              // add to our list of crawl detail records ... 
              safeAddCrawlDetailToSummaryRecord(crawlDetail);
              // ok, now update summary stats based on incoming crawl detail record ... 
              updateSummaryRecordFromCrawlDetailRecord(crawlDetail,_currentKey,reporter);
            }
          }
          catch (Exception e) {
            LOG.error("Error Parsing JSON:" + valueBytes.toString());
            throw new IOException(e);
          }
          break;
        }
        else if (linkType >= CrawlDBKey.Type.KEY_TYPE_HTML_LINK.ordinal() && linkType <= CrawlDBKey.Type.KEY_TYPE_RSS_LINK.ordinal()) { 
          JsonObject object = _parser.parse(valueBytes.toString()).getAsJsonObject();
          if (object != null) {
            setSourceURLFromJSONObject(object,linkType);
            // LOG.debug("Got LinkData:" + JSONUtils.prettyPrintJSON(object));
            // ok this is a link ... 
            updateLinkStatsFromLinkJSONObject(object,_currentKey,reporter);
          }
        }
        else if (linkType == CrawlDBKey.Type.KEY_TYPE_INCOMING_URLS_SAMPLE.ordinal()) { 
          importLinkSourceData(_currentKey, valueBytes);
        }
        reporter.progress();
      }
  }

  @Override
  public void configure(JobConf job) {
    _sourceInputsBuffer = new DataOutputBuffer(EXT_SOURCE_SAMPLE_BUFFER_SIZE);
    _sourceInputsTrackingFilter = new URLFPBloomFilter(NUM_ELEMENTS, NUM_HASH_FUNCTIONS, NUM_BITS);
    _conf = job;
    try {
      _fs = FileSystem.get(_conf);
      int partitionId = _conf.getInt("mapred.task.partition", 0);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void close() throws IOException {
    flushCurrentRecord(_outputCollector,_reporter);
  }  

  /** 
   * internal helper - emit a redirect record give a source crawl status record  
   * 
   * @param jsonObject
   * @param redirectObj
   * @param output
   * @param reporter
   * @throws IOException
   */
  void emitRedirectRecord(JsonObject jsonObject,JsonObject redirectObj,OutputCollector<TextBytes, TextBytes> output,Reporter reporter)throws IOException { 

    // ok first things first, generate a fingerprint for redirect SOURCE
    URLFPV2 redirectFP = URLUtils.getURLFPV2FromURL(redirectObj.get("source_url").getAsString());
    if (redirectFP == null) { 
      reporter.incrCounter(Counters.BAD_REDIRECT_URL, 1);
    }
    else { 
      int httpResult = redirectObj.get("http_result").getAsInt();
      JsonObject redirectJSON = new JsonObject();
      
      redirectJSON.addProperty("disposition","SUCCESS");
      redirectJSON.addProperty("http_result",httpResult);
      redirectJSON.addProperty("server_ip",redirectObj.get("server_ip").getAsString());
      redirectJSON.addProperty("attempt_time",jsonObject.get("attempt_time").getAsLong());
      redirectJSON.addProperty("target_url",jsonObject.get("source_url").getAsString());
      redirectJSON.addProperty("source_url",redirectObj.get("source_url").getAsString());
  
      // ok emit the redirect record ... 
      TextBytes key = CrawlDBKey.generateKey(redirectFP,CrawlDBKey.Type.KEY_TYPE_CRAWL_STATUS,jsonObject.get("attempt_time").getAsLong());
      LOG.debug("!!!!!!Emitting Redirect Record:" + redirectJSON.toString());

      output.collect(key, new TextBytes(redirectJSON.toString()));

      reporter.incrCounter(Counters.EMITTED_REDIRECT_RECORD, 1);

      //_redirectWriter.append(new TextBytes(redirectObj.get("source_url").getAsString()), new TextBytes(redirectJSON.toString()));
    }
  }
    
  /** 
   * grab date headers and incorporate them into the crawl detail object 
   * 
   * @param jsonObject
   * @param crawlStatsJSON
   */
  static void populateDateHeadersFromJSONObject(JsonObject jsonObject,JsonObject crawlStatsJSON) { 
    JsonObject headers = jsonObject.getAsJsonObject("http_headers");
    if (headers != null) { 
      JsonElement httpDate = headers.get("date");
      JsonElement age             = headers.get("age");
      JsonElement lastModified    = headers.get("last-modified");
      JsonElement expires         = headers.get("expires");
      JsonElement cacheControl    = headers.get("cache-control");
      JsonElement pragma    = headers.get("pragma");
      JsonElement etag    = headers.get("etag");
      
      if (httpDate != null) { 
        crawlStatsJSON.addProperty(CRAWLDETAIL_HTTP_DATE_PROPERTY, HttpHeaderInfoExtractor.getTime(httpDate.getAsString()));
      }
      if (age != null) { 
        crawlStatsJSON.add(CRAWLDETAIL_HTTP_AGE_PROPERTY, age);
      }
      if (lastModified != null) { 
        crawlStatsJSON.addProperty(CRAWLDETAIL_HTTP_LAST_MODIFIED_PROPERTY, HttpHeaderInfoExtractor.getTime(lastModified.getAsString()));
      }
      if (expires != null) { 
        crawlStatsJSON.addProperty(CRAWLDETAIL_HTTP_EXPIRES_PROPERTY, HttpHeaderInfoExtractor.getTime(expires.getAsString()));
      }
      if (cacheControl != null) { 
        crawlStatsJSON.add(CRAWLDETAIL_HTTP_CACHE_CONTROL_PROPERTY, cacheControl);
      }
      if (pragma != null) { 
        crawlStatsJSON.add(CRAWLDETAIL_HTTP_PRAGMA_PROPERTY, pragma);
      }
      if (etag != null) { 
        crawlStatsJSON.add(CRAWLDETAIL_HTTP_ETAG_PROPERTY, etag);
      }
    }
  }
  

  /** 
   * 
   * @param contentObj
   * @param crawlStatsJSON
   */
  static void addMinMaxFeedItemTimes(JsonObject contentObj,JsonObject crawlStatsJSON) { 
    JsonArray items = contentObj.getAsJsonArray("items");
    
    if (items != null) {
      long minPubDate = -1L;
      long maxPubDate = -1L;
      int  itemCount = 0;
      
      for (JsonElement item : items) {
        long pubDateValue = -1;
        JsonElement pubDate = item.getAsJsonObject().get("published");
        
        if (pubDate != null) {
          pubDateValue = pubDate.getAsLong();
        }
        JsonElement updateDate = item.getAsJsonObject().get("updated");
        if (updateDate != null) { 
          if (updateDate.getAsLong() > pubDateValue) { 
            pubDateValue = updateDate.getAsLong();
          }
        }
        
        if (minPubDate == -1L || pubDateValue < minPubDate) { 
          minPubDate = pubDateValue;
        }
        if (maxPubDate == -1L || pubDateValue > maxPubDate) { 
          maxPubDate = pubDateValue;
        }
        itemCount++;
      }
      crawlStatsJSON.addProperty(RSS_MIN_PUBDATE_PROPERTY,minPubDate);
      crawlStatsJSON.addProperty(RSS_MAX_PUBDATE_PROPERTY,maxPubDate);
      crawlStatsJSON.addProperty(RSS_ITEM_COUNT_PROPERTY,itemCount);
    }
  }
  
  /** 
   * we need to extract source url from the JSON because it is not available via 
   * the key
   * 
   * @param jsonObject
   * @param keyType
   */
  void setSourceURLFromJSONObject(JsonObject jsonObject, long keyType) { 
    if (!_urlKeyForzen) { 
      JsonElement sourceElement = jsonObject.get("source_url");
      if (keyType == CrawlDBKey.Type.KEY_TYPE_CRAWL_STATUS.ordinal()) { 

        _outputKeyString = sourceElement.getAsString();
        _outputKeyURLObj = new GoogleURL(_outputKeyString);
        
        JsonElement httpResultElem = jsonObject.get("http_result");
        
        if (httpResultElem != null) {
          int httpResult = httpResultElem.getAsInt();
          if (httpResult >= 200 && httpResult <= 299) { 
            if (sourceElement != null && _outputKeyString == null) {
              _outputKeyString = sourceElement.getAsString();
              _outputKeyURLObj = new GoogleURL(_outputKeyString);
              if (_outputKeyURLObj.isValid())
                _urlKeyForzen = true;
            }
          }
        }
      }
      else if (keyType == CrawlDBKey.Type.KEY_TYPE_MERGED_RECORD.ordinal()) { 
        _outputKeyString = sourceElement.getAsString();
        _outputKeyURLObj = new GoogleURL(_outputKeyString);
        _urlKeyForzen = true;
      }
      else if (keyType >= CrawlDBKey.Type.KEY_TYPE_HTML_LINK.ordinal() && keyType <= CrawlDBKey.Type.KEY_TYPE_RSS_LINK.ordinal()) {
        if (_outputKeyString == null) { 
          JsonElement hrefElement = jsonObject.get("href");
          if (sourceElement != null && hrefElement != null) { 
            GoogleURL hrefSource = new GoogleURL(sourceElement.getAsString());
            if (hrefSource.isValid()) {
              _outputKeyString = hrefElement.getAsString();
              _outputKeyURLObj = new GoogleURL(_outputKeyString);
            }
          }
        }
      }
    }
  }
  
  void mergeLinkRecords(JsonObject sourceRecord,JsonObject topLevelJSONObject,Reporter reporter) {
    JsonElement destRecord = topLevelJSONObject.get(TOPLEVEL_LINKSTATUS_PROPERTY);
    if (destRecord == null) {
      if (sourceRecord != null) {
        reporter.incrCounter(Counters.ADOPTED_SOURCE_LINKSUMMARY_RECORD, 1);
        topLevelJSONObject.add(TOPLEVEL_LINKSTATUS_PROPERTY,sourceRecord);
        JsonArray typeAndRels = sourceRecord.getAsJsonArray(LINKSTATUS_TYPEANDRELS_PROPERTY);
        if (typeAndRels != null) { 
          for (JsonElement typeAndRel : typeAndRels) { 
            _types.add(typeAndRel.getAsString());
          }
        }
      }
    }
    else { 
      if (sourceRecord != null) { 
        reporter.incrCounter(Counters.MERGED_SOURCE_LINKSUMMARY_RECORD_INTO_DEST, 1);
        
        safeIncrementJSONCounter(destRecord.getAsJsonObject(),LINKSTATUS_INTRADOMAIN_SOURCES_COUNT_PROPERTY,sourceRecord.get(LINKSTATUS_INTRADOMAIN_SOURCES_COUNT_PROPERTY));
        safeIncrementJSONCounter(destRecord.getAsJsonObject(),LINKSTATUS_EXTRADOMAIN_SOURCES_COUNT_PROPERTY,sourceRecord.get(LINKSTATUS_EXTRADOMAIN_SOURCES_COUNT_PROPERTY));
        safeSetMinLongValue(destRecord.getAsJsonObject(),LINKSTATUS_EARLIEST_DATE_PROPERTY,sourceRecord.get(LINKSTATUS_EARLIEST_DATE_PROPERTY));
        safeSetMaxLongValue(destRecord.getAsJsonObject(),LINKSTATUS_LATEST_DATE_PROPERTY,sourceRecord.get(LINKSTATUS_LATEST_DATE_PROPERTY));
        
        JsonArray typeAndRels = sourceRecord.getAsJsonArray(LINKSTATUS_TYPEANDRELS_PROPERTY);
        if (typeAndRels != null) { 
          for (JsonElement typeAndRel : typeAndRels) { 
            _types.add(typeAndRel.getAsString());
          }
        }
      }
    }
  }
  
  /** 
   * merge two crawl summary records
   * @param incomingRecord
   * @param topLevelJSONObject
   * @param reporter
   * @throws IOException
   */
  void mergeSummaryRecords(JsonObject incomingRecord,JsonObject topLevelJSONObject,Reporter reporter)throws IOException { 
    JsonObject destinationSummaryRecord = topLevelJSONObject.getAsJsonObject(TOPLEVEL_SUMMARYRECORD_PROPRETY);

    if (destinationSummaryRecord == null) { 
      if (incomingRecord != null) { 
        reporter.incrCounter(Counters.ADOPTED_SOURCE_SUMMARY_RECORD, 1);
        // adopt source ... 
        topLevelJSONObject.add(TOPLEVEL_SUMMARYRECORD_PROPRETY,incomingRecord);
        _summaryRecord = incomingRecord;
      }
    }
    else { 
      if (incomingRecord != null) { 
        reporter.incrCounter(Counters.MERGED_SOURCE_SUMMARY_RECORD_INTO_DEST, 1);

        // walk crawl detail records in incoming record and merge them into destination record ...
        JsonElement crawlStatsArray = incomingRecord.get(SUMMARYRECORD_CRAWLDETAILS_ARRAY_PROPERTY);
        if (crawlStatsArray != null) { 
          for (JsonElement crawlDetail : crawlStatsArray.getAsJsonArray()) { 
            // add to our list of crawl detail records ... 
            safeAddCrawlDetailToSummaryRecord(crawlDetail.getAsJsonObject());
            // ok, now update summary stats based on incoming crawl detail record ... 
            updateSummaryRecordFromCrawlDetailRecord(crawlDetail.getAsJsonObject(),_currentKey,reporter);

          }
        }
      }
    }
  }
  
  /** 
   * for the current url, merge the currently accumulated information with a previously generated crawl summary record  
   * @param jsonObject
   * @param destFP
   * @param reporter
   * @throws IOException
   */
  void processMergedRecord(JsonObject jsonObject,URLFPV2 destFP,Reporter reporter)throws IOException { 
    if (_topLevelJSONObject == null) { 
      reporter.incrCounter(Counters.MERGED_OBJECT_FIRST_OBJECT, 1);
      _topLevelJSONObject = jsonObject;
      _summaryRecord = jsonObject.getAsJsonObject(TOPLEVEL_SUMMARYRECORD_PROPRETY);
      _linkSummaryRecord = jsonObject.getAsJsonObject(TOPLEVEL_LINKSTATUS_PROPERTY);
      if (_linkSummaryRecord != null) { 
        // read in type and rels collection ...
        safeJsonArrayToStringCollection(_linkSummaryRecord,LINKSTATUS_TYPEANDRELS_PROPERTY, _types);
      }
      
      // and ext hrefs ..
      if (_summaryRecord != null) { 
        safeJsonArrayToStringCollection(_summaryRecord, SUMMARYRECORD_EXTERNALLY_REFERENCED_URLS,_extHrefs);
      }
    }
    else { 
      mergeSummaryRecords(jsonObject.getAsJsonObject(TOPLEVEL_SUMMARYRECORD_PROPRETY),_topLevelJSONObject,reporter);
      mergeLinkRecords(jsonObject.getAsJsonObject(TOPLEVEL_LINKSTATUS_PROPERTY),_topLevelJSONObject,reporter);
    }
  }
  
  /** 
   * given a incoming link record, track the link source and also update stats and 
   * also capture document type information (if available via the href).
   * 
   * @param jsonObject
   * @param destFP
   * @param reporter
   * @throws IOException
   */
  void updateLinkStatsFromLinkJSONObject(JsonObject jsonObject,URLFPV2 destFP,Reporter reporter) throws IOException { 
    JsonElement sourceElement = jsonObject.get("source_url");
    JsonElement hrefElement = jsonObject.get("href");
    
    if (sourceElement != null && hrefElement != null) {
      //LOG.info("source:" + sourceElement.getAsString() + " href:" + hrefElement.getAsString());
      GoogleURL sourceURLObj = new GoogleURL(sourceElement.getAsString());
      
      if (sourceURLObj.isValid()) {
        if (_linkSummaryRecord == null) { 
          _linkSummaryRecord = new JsonObject();
        }
        
        // ok, first compare known host name with incoming link host name ... 
        // if not a match then ... 
        if (!_outputKeyURLObj.getHost().equals(sourceURLObj.getHost())) { 
          // ok now deeper check ...
          URLFPV2 sourceFP = URLUtils.getURLFPV2FromURLObject(sourceURLObj);
          if (sourceFP != null) { 
            reporter.incrCounter(Counters.GOT_EXTERNAL_DOMAIN_SOURCE, 1);
            // increment external source count
            safeIncrementJSONCounter(_linkSummaryRecord,LINKSTATUS_EXTRADOMAIN_SOURCES_COUNT_PROPERTY);
            
            //LOG.info("sourceFP:" + sourceFP.getKey() + " hrefFP:" + destFP.getKey());
            // ok track sources if from a different root domain (for now) 
            if (sourceFP.getRootDomainHash()  != destFP.getRootDomainHash()) {
              trackPotentialLinkSource(sourceFP,sourceElement.getAsString(),destFP);
            }
          }
        }
        // otherwise, count it as an internal link 
        else {
          // internal for sure ... 
          safeIncrementJSONCounter(_linkSummaryRecord,LINKSTATUS_INTRADOMAIN_SOURCES_COUNT_PROPERTY);
        }
                  
        JsonObject sourceHeaders = jsonObject.getAsJsonObject("source_headers");
        if (sourceHeaders != null) { 
          long httpDate = safeGetHttpDate(sourceHeaders, "date");
          long lastModified = safeGetHttpDate(sourceHeaders, "last-modified");
          if (lastModified != -1 && lastModified < httpDate)
            httpDate = lastModified;
          if (httpDate != -1L) { 
            safeSetMinLongValue(_linkSummaryRecord, LINKSTATUS_EARLIEST_DATE_PROPERTY, httpDate);
            safeSetMaxLongValue(_linkSummaryRecord, LINKSTATUS_LATEST_DATE_PROPERTY, httpDate);
          }
        }
        JsonElement typeElement = jsonObject.get("type");
        JsonElement relElement = jsonObject.get("rel");
        
        String sourceTypeAndRel = jsonObject.get("source_type").getAsString() + ":";
        
        if (typeElement != null) { 
          sourceTypeAndRel += typeElement.getAsString();
        }
        if (relElement != null) { 
          sourceTypeAndRel += ":" + relElement.getAsString();
        }
        
        if (_types.size() < MAX_TYPE_SAMPLES)
          _types.add(sourceTypeAndRel);
      }
    }
  }
  
  /** 
   * take linking href data and add it to our list of incoming hrefs
   * (used during the intermediate merge process)  
   * 
   * @param destFP
   * @param inputData
   * @throws IOException
   */
  void importLinkSourceData(URLFPV2 destFP,TextBytes inputData) throws IOException {
    

    TextBytes urlText = new TextBytes();
    
    int curpos =inputData.getOffset();
    int endpos = inputData.getOffset() + inputData.getLength();
    
    byte lfPattern[] = { 0xA };
    byte tabPattern[] = { 0x9 };
    
    while (curpos != endpos) { 
      int tabIndex = ByteArrayUtils.indexOf(inputData.getBytes(), curpos, endpos - curpos, tabPattern);
      if (tabIndex == -1) { 
        break;
      }
      else { 
        int lfIndex = ByteArrayUtils.indexOf(inputData.getBytes(), tabIndex + 1, endpos - (tabIndex + 1), lfPattern);
        if (lfIndex == -1) { 
          break;
        }
        else {
          long sourceDomainHash = ByteArrayUtils.parseLong(inputData.getBytes(),curpos, tabIndex-curpos, 10);
          urlText.set(inputData.getBytes(),tabIndex + 1,lfIndex - (tabIndex + 1));
          URLFPV2 bloomKey = sourceKeyFromSourceAndDest(sourceDomainHash,destFP.getUrlHash());
          if (!_sourceInputsTrackingFilter.isPresent(bloomKey)) {
            // if not, check to see that we are not about to overflow sample buffer ...  
            if (_sourceInputsBuffer.getLength() < EXT_SOURCE_SAMPLE_BUFFER_SIZE - EXT_SOURCE_SAMPLE_BUFFER_PAD_AMOUNT) {
              _sourceInputsBuffer.write(inputData.getBytes(),curpos,(lfIndex + 1) - curpos);
              _sourceSampleSize++;
            }
          }
          
          curpos = lfIndex + 1;
        }
      }
    }
  }
  
  /** 
   * given an incoming link for a given url, store it in a accumulation buffer IFF we have not 
   * seen a url from the given domain before 
   * 
   * @param sourceFP
   * @param sourceURL
   * @param destFP
   * @throws IOException
   */
  void trackPotentialLinkSource(URLFPV2 sourceFP,String sourceURL,URLFPV2 destFP) throws IOException { 
    URLFPV2 bloomKey = sourceKeyFromSourceAndDest(sourceFP.getDomainHash(),destFP.getUrlHash());
    // check to see if we have collected a sample for this source domain / destination url combo or not ... 
    if (!_sourceInputsTrackingFilter.isPresent(bloomKey)) {
      LOG.debug("sourceFP:" + sourceFP.getKey() + " passed BloomFilter Test");
      // if not, check to see that we are not about to overflow sample buffer ...  
      if (_sourceInputsBuffer.getLength() < EXT_SOURCE_SAMPLE_BUFFER_SIZE - EXT_SOURCE_SAMPLE_BUFFER_PAD_AMOUNT) {
        // ok store the external reference sample ... 
        // write source domain hash 
        _sourceInputsBuffer.write(Long.toString(sourceFP.getDomainHash()).getBytes());
        // delimiter 
        _sourceInputsBuffer.write(0x09);// TAB
        // and source url ... 
        _sourceInputsBuffer.write(sourceURL.getBytes(Charset.forName("UTF-8")));
        _sourceInputsBuffer.write(0x0A);// LF 
        _sourceSampleSize++;
        
        // add to bloom filter ... 
        _sourceInputsTrackingFilter.add(bloomKey);
      }
    }
    else { 
      LOG.debug("sourceFP:" + sourceFP.getKey() + " failed BloomFilter Test");
    }
  }

  /** 
   * construct a (hacked) fingerprint key consisting of the source domain and destination 
   * url fingerprint to be used for the purposes of setting bits in a bloomfilter
   * 
   * @param sourceDomain
   * @param destURLHash
   * @return
   */
  private URLFPV2 sourceKeyFromSourceAndDest(long sourceDomain,long destURLHash) { 
    _bloomFilterKey.setDomainHash(sourceDomain);
    _bloomFilterKey.setUrlHash(destURLHash);
    return _bloomFilterKey;
  }
  
  
  /** 
   * construct crawl detail record from incoming crawl status JSON 
   *  
   * @param jsonObject
   * @param fpSource
   * @param extHRefs
   * @param reporter
   * @return
   * @throws IOException
   */
  static JsonObject crawlDetailRecordFromCrawlStatusRecord(JsonObject jsonObject,URLFPV2 fpSource,HashSet<String> extHRefs,Reporter reporter)throws IOException { 
    
    String disposition = jsonObject.get("disposition").getAsString();
    long   attemptTime = jsonObject.get("attempt_time").getAsLong();

    // inject all the details into a JSONObject 
    JsonObject crawlStatsJSON = new JsonObject();

    crawlStatsJSON.addProperty(CRAWLDETAIL_ATTEMPT_TIME_PROPERTY, attemptTime);

    if (disposition.equals("SUCCESS")) {

      // basic stats ... starting with crawl time ...
      int httpResult = jsonObject.get("http_result").getAsInt();
      crawlStatsJSON.addProperty(CRAWLDETAIL_HTTPRESULT_PROPERTY,httpResult);
      crawlStatsJSON.addProperty(CRAWLDETAIL_SERVERIP_PROPERTY, jsonObject.get("server_ip").getAsString());

      //populate date headers ... 
      populateDateHeadersFromJSONObject(jsonObject,crawlStatsJSON);
      
      // if http 200 ... 
      if (httpResult >= 200 && httpResult <= 299) {

        reporter.incrCounter(Counters.GOT_HTTP_200_CRAWL_STATUS,1);
              
        crawlStatsJSON.addProperty(CRAWLDETAIL_CONTENTLEN_PROPERTY,jsonObject.get("content_len").getAsInt());
        if (jsonObject.get("mime_type") != null) { 
          crawlStatsJSON.addProperty(CRAWLDETAIL_MIMETYPE_PROPERTY,jsonObject.get("mime_type").getAsString());
        }
        if (jsonObject.get("md5") != null) {
          crawlStatsJSON.addProperty(CRAWLDETAIL_MD5_PROPERTY,jsonObject.get("md5").getAsString());
        }
        if (jsonObject.get("text_simhash") != null) {
          crawlStatsJSON.addProperty(CRAWLDETAIL_TEXTSIMHASH_PROPERTY,jsonObject.get("text_simhash").getAsLong());
        }
        
        JsonElement parsedAs = jsonObject.get("parsed_as");
        
        if (parsedAs != null) {
          // populate some info based on type ... 
          crawlStatsJSON.addProperty(CRAWLDETAIL_PARSEDAS_PROPERTY,parsedAs.getAsString());
          
          String parsedAsString = parsedAs.getAsString();
          
          // if html ... 
          if (parsedAsString.equals("html")) { 
            JsonObject content = jsonObject.get("content").getAsJsonObject();
            if (content != null) { 
              JsonElement titleElement = content.get("title");
              JsonElement metaElement = content.get("meta_tags");
              if (titleElement != null) { 
                crawlStatsJSON.add(CRAWLDETAIL_TITLE_PROPERTY, titleElement);
              }
              if (metaElement != null) { 
                crawlStatsJSON.add(CRAWLDETAIL_METATAGS_PROPERTY, metaElement);
              }
              // collect link stats for json ... 
              updateLinkStatsFromHTMLContent(crawlStatsJSON,jsonObject,extHRefs,fpSource,reporter);
            }
            
          }
          // if feed ... 
          else if (parsedAsString.equals("feed")) { 
            // get content ... 
            JsonObject content = jsonObject.get("content").getAsJsonObject();
            JsonElement titleElement = content.get("title");
            if (titleElement != null) { 
              crawlStatsJSON.add(CRAWLDETAIL_TITLE_PROPERTY, titleElement);
            }
            // set update time ... 
            long updateTime = safeGetLong(content, "updated");
            if (updateTime != -1) { 
              crawlStatsJSON.addProperty(CRAWLDETAIL_UPDATED_PROPERTY, updateTime);
            }
            
            addMinMaxFeedItemTimes(content,crawlStatsJSON);
          }
        }
      }
      // redirect ... 
      else if (httpResult >=300 && httpResult <= 399) {
        reporter.incrCounter(Counters.GOT_REDIRECT_CRAWL_STATUS,1);
        
        // get the target url ... 
        String targetURL = jsonObject.get("target_url").getAsString();
        if (targetURL != null) {
          // redirect details ...
          crawlStatsJSON.addProperty(CRAWLDETAIL_REDIRECT_URL, targetURL);
        }
      }
    }
    else { 
      // inject all the details into a JSONObject 
      
      // basic stats ... starting with crawl time ...
      crawlStatsJSON.addProperty(CRAWLDETAIL_FAILURE,true);
      crawlStatsJSON.addProperty(CRAWLDETAIL_FAILURE_REASON,jsonObject.get("failure_reason").getAsString());
      crawlStatsJSON.addProperty(CRAWLDETAIL_FAILURE_DETAIL,jsonObject.get("failure_detail").getAsString());
    }
    
    return crawlStatsJSON;
  
  }
  
  /** 
   * given a crawl detail json record, update summary record stats 
   * 
   * @param crawlDetailRecord
   * @param fpSource
   * @param reporter
   * @throws IOException
   */
  void updateSummaryRecordFromCrawlDetailRecord(JsonObject crawlDetailRecord,URLFPV2 fpSource,Reporter reporter) throws IOException { 
    
    if (_summaryRecord == null) { 
      _summaryRecord = new JsonObject();
    }
    
    boolean failure = safeGetBoolean(crawlDetailRecord,CRAWLDETAIL_FAILURE);
    long   attemptTime = crawlDetailRecord.get(CRAWLDETAIL_ATTEMPT_TIME_PROPERTY).getAsLong();
      
    // set latest attempt time ... 
    long latestAttemptTime = safeSetMaxLongValue(_summaryRecord,SUMMARYRECORD_LATEST_ATTEMPT_PROPERTY,attemptTime);
    // increment attempt count 
    safeIncrementJSONCounter(_summaryRecord,SUMMARYRECORD_ATTEMPT_COUNT_PROPERTY);
    
    // if this is the latest attempt ... 
    if (latestAttemptTime == attemptTime) {
      // add latest http result to summary 
      if (!failure && crawlDetailRecord.has(CRAWLDETAIL_HTTPRESULT_PROPERTY)) { 
        int httpResult = crawlDetailRecord.get(CRAWLDETAIL_HTTPRESULT_PROPERTY).getAsInt();
        // set last http result 
        _summaryRecord.addProperty(SUMMARYRECORD_HTTP_RESULT_PROPERTY,httpResult);
        if (httpResult >= 200 && httpResult <= 299) {
          // update the crawl timestamp 
          _summaryRecord.addProperty(SUMMARYRECORD_LATEST_CRAWLTIME_PROPERTY,attemptTime);
          // and the crawl count .... 
          safeIncrementJSONCounter(_summaryRecord,SUMMARYRECORD_CRAWLCOUNT_PROPERTY);

          // update parsed as 
          if (crawlDetailRecord.has(CRAWLDETAIL_PARSEDAS_PROPERTY)) { 
            _summaryRecord.addProperty(SUMMARYRECORD_PARSEDAS_PROPERTY, crawlDetailRecord.get(CRAWLDETAIL_PARSEDAS_PROPERTY).getAsString());
          }
        }
        else if (httpResult >=300 && httpResult <= 399) {
          if (crawlDetailRecord.has(CRAWLDETAIL_REDIRECT_URL)) { 
            _summaryRecord.addProperty(SUMMARYRECORD_REDIRECT_URL_PROPERTY, crawlDetailRecord.get(CRAWLDETAIL_REDIRECT_URL).getAsString());
          }
        }
      }
    }
  }
  
  /** 
   * given html content (json object), extract out of domain hrefs and cache them 
   * and ... update stats 
   * @param crawlStats
   * @param incomingJSONObject
   * @param extHRefs
   * @param fpSource
   * @param reporter
   */
  static void updateLinkStatsFromHTMLContent(JsonObject crawlStats,JsonObject incomingJSONObject,HashSet<String> extHRefs,URLFPV2 fpSource,Reporter reporter) { 
    JsonArray links = incomingJSONObject.getAsJsonArray("links");
    
    if (links == null) { 
      reporter.incrCounter(Counters.NULL_LINKS_ARRAY, 1);
    }
    else {
      
      // clear our snapshot of externally referenced urls 
      // we only want to capture this information from 
      // the links extracted via the latest content
      if (extHRefs != null) 
        extHRefs.clear();
      
      int intraDomainLinkCount = 0;
      int intraRootLinkCount = 0;
      int interDomainLinkCount = 0;
      
      for (JsonElement link : links) { 
        JsonObject linkObj = link.getAsJsonObject();
        if (linkObj != null && linkObj.has("href")) {
          String href = linkObj.get("href").getAsString();
          GoogleURL urlObject = new GoogleURL(href);
          if (urlObject.isValid()) { 
            URLFPV2 linkFP = URLUtils.getURLFPV2FromURLObject(urlObject);
            if (linkFP != null) { 
              if (linkFP.getRootDomainHash() == fpSource.getRootDomainHash()) {
                if (linkFP.getDomainHash() == fpSource.getDomainHash()) { 
                  intraDomainLinkCount ++;
                }
                else { 
                  intraRootLinkCount ++;
                }
              }
              else {
                interDomainLinkCount++;
                // track domains we link to
                if (extHRefs != null) {
                  extHRefs.add(urlObject.getCanonicalURL());
                }
              }
            }
          }
        }
      }
      // update counts in crawl stats data structure ... 
      crawlStats.addProperty(CRAWLDETAIL_INTRADOMAIN_LINKS, intraDomainLinkCount);
      crawlStats.addProperty(CRAWLDETAIL_INTRAROOT_LINKS, intraRootLinkCount);
      crawlStats.addProperty(CRAWLDETAIL_INTERDOMAIN_LINKS, interDomainLinkCount);
    }
  }
  
  /** 
   * flush currently accumulated JSON record
   * 
   * @param output
   * @param reporter
   * @throws IOException
   */
  private void flushCurrentRecord(OutputCollector<TextBytes, TextBytes> output, Reporter reporter)throws IOException {
    _urlsProcessed++;

    
    if (_outputKeyString == null || !_outputKeyURLObj.isValid()) { 
      reporter.incrCounter(Counters.FAILED_TO_GET_SOURCE_HREF, 1);
    }
    else { 
    
      if (_topLevelJSONObject != null || _summaryRecord != null || _linkSummaryRecord != null) { 
        
        if (_topLevelJSONObject == null) {
          reporter.incrCounter(Counters.ALLOCATED_TOP_LEVEL_OBJECT_IN_FLUSH, 1);
          _topLevelJSONObject = new JsonObject();
          _topLevelJSONObject.addProperty(TOPLEVEL_SOURCE_URL_PROPRETY,_outputKeyString);
        }
        else { 
          reporter.incrCounter(Counters.ENCOUNTERED_EXISTING_TOP_LEVEL_OBJECT_IN_FLUSH, 1);
        }
        
        if (_summaryRecord != null) {
          
          _summaryRecord.remove(SUMMARYRECORD_EXTERNALLY_REFERENCED_URLS);
          _summaryRecord.remove(SUMMARYRECORD_EXTERNALLY_REFERENCED_URLS_TRUNCATED);
          
          if (_extHrefs.size() != 0) { 
            // output links in the top level object ...
            stringCollectionToJsonArrayWithMax(_summaryRecord, SUMMARYRECORD_EXTERNALLY_REFERENCED_URLS, _extHrefs,MAX_EXTERNALLY_REFERENCED_URLS);
            if (_extHrefs.size() > MAX_EXTERNALLY_REFERENCED_URLS) { 
              _summaryRecord.addProperty(SUMMARYRECORD_EXTERNALLY_REFERENCED_URLS_TRUNCATED,true);
            }
          }
          
          reporter.incrCounter(Counters.ENCOUNTERED_SUMMARY_RECORD_IN_FLUSH, 1);
          _topLevelJSONObject.add(TOPLEVEL_SUMMARYRECORD_PROPRETY, _summaryRecord);
        }
        if (_linkSummaryRecord != null) {
          reporter.incrCounter(Counters.ENCOUNTERED_LINKSUMMARY_RECORD_IN_FLUSH, 1);

          if (_types != null && _types.size() != 0) { 
            stringCollectionToJsonArray(_linkSummaryRecord,LINKSTATUS_TYPEANDRELS_PROPERTY,_types);
          }
          _topLevelJSONObject.add(TOPLEVEL_LINKSTATUS_PROPERTY, _linkSummaryRecord);
        }        
        
        // output top level record ... 
        output.collect(CrawlDBKey.generateKey(_currentKey, CrawlDBKey.Type.KEY_TYPE_MERGED_RECORD, 0),new TextBytes(_topLevelJSONObject.toString()));
        // if there is link status available ...
        if (_sourceSampleSize != 0) {
          reporter.incrCounter(Counters.EMITTED_SOURCEINPUTS_RECORD, 1);
          TextBytes sourceInputsText= new TextBytes();
          sourceInputsText.set(_sourceInputsBuffer.getData(),0,_sourceInputsBuffer.getLength());
          output.collect(CrawlDBKey.generateKey(_currentKey, CrawlDBKey.Type.KEY_TYPE_INCOMING_URLS_SAMPLE, 0),sourceInputsText);
        }
      }
      
      if (_urlsProcessed % FLUSH_INTERVAL == 0) { 
        _sourceInputsTrackingFilter.clear();
      }
    }
    
    _sourceInputsBuffer.reset();
    _sourceSampleSize = 0;
    _topLevelJSONObject = null;
    _summaryRecord = null;
    _linkSummaryRecord = null;
    _types.clear();
    _extHrefs.clear();
    _outputKeyString = null;
    _urlKeyForzen = false;
    _outputKeyURLObj = null;        
  }
    
  
  /** 
   * Extract the fingerprint from the incoming key and potentially trigger a flush if it is indicative of a 
   * primary key transition 
   * @param key
   * @param output
   * @param reporter
   * @throws IOException
   */
  private void readFPCheckForTransition(TextBytes key,OutputCollector<TextBytes, TextBytes> output, Reporter reporter)throws IOException { 
    if (_tempKey == null) { 
      _tempKey = new URLFPV2();
    }
    
    _tempKey.setRootDomainHash(CrawlDBKey.getLongComponentFromKey(key, ComponentId.ROOT_DOMAIN_HASH_COMPONENT_ID));
    _tempKey.setDomainHash(CrawlDBKey.getLongComponentFromKey(key, ComponentId.DOMAIN_HASH_COMPONENT_ID));
    _tempKey.setUrlHash(CrawlDBKey.getLongComponentFromKey(key, ComponentId.URL_HASH_COMPONENT_ID));
    
    if (_currentKey == null) { 
      _currentKey = _tempKey;
      _tempKey = null;
    }
    else {
      // check for key transition ... 
      if (_currentKey.compareTo(_tempKey) != 0) { 
        // transition 
        flushCurrentRecord(output,reporter);
        
        // swap keys ... 
        URLFPV2 oldKey = _currentKey;
        _currentKey = _tempKey;
        _tempKey = oldKey;
      }
    }
  }

  /**
   * add crawl detail to summary record. construct a summary detail if none exists ... 
   * 
   * @param crawlStatsJSON
   */
  void safeAddCrawlDetailToSummaryRecord(JsonObject crawlStatsJSON) {
    if (_summaryRecord == null) { 
      _summaryRecord = new JsonObject();
    }
    // construct crawl stats array if necessary 
    JsonArray crawlStatsArray = _summaryRecord.getAsJsonArray(SUMMARYRECORD_CRAWLDETAILS_ARRAY_PROPERTY);
    if (crawlStatsArray == null) { 
      crawlStatsArray = new JsonArray();
      _summaryRecord.add(SUMMARYRECORD_CRAWLDETAILS_ARRAY_PROPERTY, crawlStatsArray);
    }
    // add crawl stats to it 
    crawlStatsArray.add(crawlStatsJSON);
  }

  
  /** 
   * scan the merge db path and find the latest crawl database timestamp 
   * 
   * @param fs
   * @param conf
   * @return
   * @throws IOException
   */
  static long findLatestMergeDBTimestamp(FileSystem fs,Configuration conf)throws IOException {
    long timestampOut = -1L;
    
    FileStatus files[] = fs.globStatus(new Path(S3N_BUCKET_PREFIX + MERGE_DB_PATH,"[0-9]*"));
    
    for (FileStatus candidate : files) { 
      Path successPath = new Path(candidate.getPath(),"_SUCCESS");
      if (fs.exists(successPath)) { 
        long timestamp = Long.parseLong(candidate.getPath().getName());
        timestampOut = Math.max(timestamp, timestampOut);
      }
    }
    return timestampOut;
  }
  
  /** 
   * iterate the intermediate link graph data and extract unmerged set ... 
   * 
   * @param fs
   * @param conf
   * @param latestMergeDBTimestamp
   * @return
   * @throws IOException
   */
  static List<Path> filterMergeCandidtes(FileSystem fs,Configuration conf, long latestMergeDBTimestamp )throws IOException { 
    ArrayList<Path> list = new ArrayList<Path>();
    FileStatus candidates[] = fs.globStatus(new Path(S3N_BUCKET_PREFIX + MERGE_INTERMEDIATE_OUTPUT_PATH,"[0-9]*"));
    
    for (FileStatus candidate : candidates) {
      LOG.info("Found Merge Candidate:" + candidate.getPath());
      long candidateTimestamp = Long.parseLong(candidate.getPath().getName());
      if (candidateTimestamp > latestMergeDBTimestamp) { 
        Path successPath = new Path(candidate.getPath(),"_SUCCESS");
        if (fs.exists(successPath)) { 
          list.add(candidate.getPath());
        }
        else { 
          LOG.info("Rejected Merge Candidate:" + candidate.getPath());
        }
      }
    }
    return list;
  }

  
  
  ///////////////////////////////////////////////////////////////////////////
  // TEST CODE 
  ///////////////////////////////////////////////////////////////////////////
  

  
  /* 
  // PARK THIS CODE FOR NOW SINCE WE ARE TRANSFERRING DATA PROCESSING TO EC2

    if (_skipPartition)
   return;
 // collect all incoming paths first
 Vector<Path> incomingPaths = new Vector<Path>();
 
 while(values.hasNext()){ 
   String path = values.next().toString();
   LOG.info("Found Incoming Path:" + path);
   incomingPaths.add(new Path(path));
 }
 
 FlexBuffer scanArray[] = LinkKey.allocateScanArray();


 // set up merge attributes
 Configuration localMergeConfig = new Configuration(_conf);
 
 localMergeConfig.setClass(
     MultiFileInputReader.MULTIFILE_COMPARATOR_CLASS,
     LinkKeyGroupingComparator.class, RawComparator.class);
 localMergeConfig.setClass(MultiFileInputReader.MULTIFILE_KEY_CLASS,
     TextBytes.class, WritableComparable.class);
 
 
 // ok now spawn merger
 MultiFileInputReader<TextBytes> multiFileInputReader = new MultiFileInputReader<TextBytes>(
     _fs, incomingPaths, localMergeConfig);

 TextBytes keyBytes = new TextBytes();
 TextBytes valueBytes = new TextBytes();
 DataInputBuffer inputBuffer = new DataInputBuffer();
 
 int processedKeysCount = 0;
 
 Pair<KeyAndValueData<TextBytes>,Iterable<RawRecordValue>> nextItem = null;
 while ((nextItem = multiFileInputReader.getNextItemIterator()) != null) {
   
   urlsProcessed++;
   _sourceInputsBuffer.reset();
   _sourceSampleSize = 0;
   summaryRecord = null;
   linkSummaryRecord = null;
   types.clear();
   outputKeyString = null;
   outputKeyFromInternalLink = false;
   outputKeyURLObj = null;
   extLinkedDomains.clear();
   
   int statusCount = 0;
   int linkCount = 0;
   
   // scan key components 
   LinkKey.scanForComponents(nextItem.e0._keyObject, ':',scanArray);
   
   // pick up source fp from key ... 
   URLFPV2 fpSource = new URLFPV2();
   
   fpSource.setRootDomainHash(LinkKey.getLongComponentFromComponentArray(scanArray,LinkKey.ComponentId.ROOT_DOMAIN_HASH_COMPONENT_ID));
   fpSource.setDomainHash(LinkKey.getLongComponentFromComponentArray(scanArray,LinkKey.ComponentId.DOMAIN_HASH_COMPONENT_ID));
   fpSource.setUrlHash(LinkKey.getLongComponentFromComponentArray(scanArray,LinkKey.ComponentId.URL_HASH_COMPONENT_ID));
   
   for (RawRecordValue rawValue: nextItem.e1) { 
     
     inputBuffer.reset(rawValue.key.getData(),0,rawValue.key.getLength());
     int length = WritableUtils.readVInt(inputBuffer);
     keyBytes.set(rawValue.key.getData(),inputBuffer.getPosition(),length);
     inputBuffer.reset(rawValue.data.getData(),0,rawValue.data.getLength());
     length = WritableUtils.readVInt(inputBuffer);
     valueBytes.set(rawValue.data.getData(),inputBuffer.getPosition(),length);
*/

}
