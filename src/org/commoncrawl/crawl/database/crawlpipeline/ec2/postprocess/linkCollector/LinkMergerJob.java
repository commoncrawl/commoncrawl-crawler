package org.commoncrawl.crawl.database.crawlpipeline.ec2.postprocess.linkCollector;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
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
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.crawl.database.crawlpipeline.JobConfig;
import org.commoncrawl.crawl.database.crawlpipeline.ec2.postprocess.linkCollector.LinkDataResharder.Counters;
import org.commoncrawl.crawl.database.crawlpipeline.ec2.postprocess.linkCollector.LinkKey.LinkKeyGroupingComparator;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.internal.GoogleURL;
import org.commoncrawl.util.internal.HttpHeaderInfoExtractor;
import org.commoncrawl.util.internal.URLUtils;
import org.commoncrawl.util.internal.MultiFileMergeUtils.MultiFileInputReader;
import org.commoncrawl.util.internal.MultiFileMergeUtils.MultiFileMergeInputFormat;
import org.commoncrawl.util.internal.MultiFileMergeUtils.MultiFileMergePartitioner;
import org.commoncrawl.util.internal.MultiFileMergeUtils.MultiFileInputReader.KeyAndValueData;
import org.commoncrawl.util.internal.MultiFileMergeUtils.MultiFileInputReader.RawRecordValue;
import org.commoncrawl.util.internal.Tuples.Pair;
import org.commoncrawl.util.shared.FlexBuffer;
import org.commoncrawl.util.shared.TextBytes;

import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

public class LinkMergerJob implements Reducer<IntWritable, Text,TextBytes,TextBytes>{

  static final Log LOG = LogFactory.getLog(LinkMergerJob.class);
  static final Path internalMergedSegmentPath = new Path("crawl/ec2Import/mergedSegment");
  static final Path internalMergedDBPath = new Path("crawl/ec2Import/mergedDB");

  static final int MAX_TYPE_SAMPLES = 5;
  static final int MAX_EXT_SOURCE_SAMPLES = 100;
  
  static long findLatestMergeDBTimestamp(FileSystem fs,Configuration conf)throws IOException {
    long timestampOut = -1L;
    
    FileStatus files[] = fs.globStatus(new Path(internalMergedDBPath,"[0-9]*"));
    
    for (FileStatus candidate : files) { 
      long timestamp = Long.parseLong(candidate.getPath().getName());
      timestampOut = Math.max(timestamp, timestampOut);
    }
    return timestampOut;
  }
  
  static List<Path> filterMergeCandidtes(FileSystem fs,Configuration conf, long latestMergeDBTimestamp )throws IOException { 
    ArrayList<Path> list = new ArrayList<Path>();
    FileStatus candidates[] = fs.globStatus(new Path(internalMergedSegmentPath,"[0-9]*"));
    
    for (FileStatus candidate : candidates) { 
      long candidateTimestamp = Long.parseLong(candidate.getPath().getName());
      if (candidateTimestamp > latestMergeDBTimestamp) { 
        list.add(candidate.getPath());
      }
    }
    return list;
  }
  
  public static void main(String[] args)throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    
    // establish merge timestamp 
    long mergeTimesmap = System.currentTimeMillis();
    // get a temp directory ... 
    Path outputPath = JobConfig.tempDir(conf, Long.toString(mergeTimesmap));

    
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
      // establish an affinity path ... 
      Path affinityPath = candidateList.get(0);
      // add merge db path if it exists 
      if (latestMergeDBTimestamp != -1L) {
        affinityPath = new Path(internalMergedDBPath,Long.toString(latestMergeDBTimestamp));
        inputPaths.add(affinityPath);
      }
      
      
      JobConf jobConf = new JobConfig("Final Merge Job", conf)
      .inputs(inputPaths)
      .inputFormat(MultiFileMergeInputFormat.class)
      .mapperKeyValue(IntWritable.class, Text.class)
      .outputKeyValue(TextBytes.class, TextBytes.class)
      .outputFormat(SequenceFileOutputFormat.class)
      .reducer(LinkMergerJob.class,false)
      .partition(MultiFileMergePartitioner.class)
      .numReducers(CrawlEnvironment.NUM_DB_SHARDS)
      .speculativeExecution(false)
      .output(outputPath)
      .setAffinityNoBalancing(affinityPath,ImmutableSet.of("ccd001.commoncrawl.org","ccd006.commoncrawl.org"))

      .compressMapOutput(false)
      .compressor(CompressionType.BLOCK, SnappyCodec.class)
      
      .build();
      
/*      JsonArray hack = new JsonArray();
      
      hack.add(new JsonPrimitive(5));
      hack.add(new JsonPrimitive(10));
      hack.add(new JsonPrimitive(28));
      hack.add(new JsonPrimitive(35));
      hack.add(new JsonPrimitive(61));
      hack.add(new JsonPrimitive(81));
      hack.add(new JsonPrimitive(81));

      jobConf.set("hack", hack.toString());*/
      
      LOG.info("Starting JOB");
      JobClient.runJob(jobConf);      
      
      
      Path finalOutputPath = new Path(internalMergedDBPath,Long.toString(mergeTimesmap));
      LOG.info("Renaming tempoutput:" + outputPath + " to:" + finalOutputPath);
      fs.rename(outputPath, finalOutputPath);       
    }
    
  }

  void emitRedirectRecord(JsonObject jsonObject,JsonObject redirectObj,Reporter reporter)throws IOException { 
    
    int httpResult = redirectObj.get("http_result").getAsInt();
    if (httpResult == 301) {
      JsonObject redirectJSON = new JsonObject();
      
      redirectJSON.addProperty("disposition","SUCCESS");
      redirectJSON.addProperty("http_result",301);
      redirectJSON.addProperty("attempt_time",jsonObject.get("attempt_time").getAsLong());
      redirectJSON.addProperty("target_url",jsonObject.get("source_url").getAsString());
      
      reporter.incrCounter(Counters.EMITTED_REDIRECT_RECORD, 1);
      _redirectWriter.append(new TextBytes(redirectObj.get("source_url").getAsString()), new TextBytes(redirectJSON.toString()));
    }
  }
    
  void populateDateHeadersFromJSONObject(JsonObject jsonObject,JsonObject crawlStatsJSON) { 
    JsonObject headers = jsonObject.getAsJsonObject("http_headers");
    JsonElement httpDate = headers.get("date");
    JsonElement age             = headers.get("age");
    JsonElement lastModified    = headers.get("last-modified");
    JsonElement expires         = headers.get("expires");
    JsonElement cacheControl    = headers.get("cache-control");
    JsonElement pragma    = headers.get("pragma");
    JsonElement etag    = headers.get("etag");
    
    if (httpDate != null) { 
      crawlStatsJSON.addProperty("date", HttpHeaderInfoExtractor.getTime(httpDate.getAsString()));
    }
    if (age != null) { 
      crawlStatsJSON.add("age", age);
    }
    if (lastModified != null) { 
      crawlStatsJSON.addProperty("last-modified", HttpHeaderInfoExtractor.getTime(lastModified.getAsString()));
    }
    if (expires != null) { 
      crawlStatsJSON.addProperty("expires", HttpHeaderInfoExtractor.getTime(expires.getAsString()));
    }
    if (cacheControl != null) { 
      crawlStatsJSON.add("cache-control", cacheControl);
    }
    if (pragma != null) { 
      crawlStatsJSON.add("pragma", pragma);
    }
    if (etag != null) { 
      crawlStatsJSON.add("etag", etag);
    }
  }
  
  static void safeIncrementJSONCounter(JsonObject jsonObj,String property) { 
    if (jsonObj.has(property)) { 
      jsonObj.addProperty(property, jsonObj.get(property).getAsInt() + 1);
    }
    else { 
      jsonObj.addProperty(property, 1);
    }
  }
  
  static long safeGetLong(JsonObject jsonObj,String property) { 
    JsonElement element = jsonObj.get(property);
    if (element != null) { 
      return element.getAsLong();
    }
    return -1;
  }

  static long safeGetHttpDate(JsonObject jsonObj,String property) { 
    JsonElement element = jsonObj.get(property);
    if (element != null) { 
      return HttpHeaderInfoExtractor.getTime(element.getAsString());
    }
    return -1;
  }

  static long safeSetMaxLongValue(JsonObject jsonObj,String property,long newValue) { 
    JsonElement element = jsonObj.get(property);
    if (element != null) {
      if (element.getAsLong() > newValue) {
        return element.getAsLong();
      }
    }
    jsonObj.addProperty(property, newValue);
    return newValue;
  }

  static long safeSetMinLongValue(JsonObject jsonObj,String property,long newValue) { 
    JsonElement element = jsonObj.get(property);
    if (element != null) {
      if (newValue > element.getAsLong()) {
        return element.getAsLong();
      }
    }
    jsonObj.addProperty(property, newValue);
    return newValue;
  }
  
  static void stringCollectionToJsonArray(JsonObject jsonObject,String propertyName,Collection<String> stringSet) { 
    JsonArray array = new JsonArray();
    for (String value : stringSet) { 
      array.add(new JsonPrimitive(value));
    }
    jsonObject.add(propertyName, array);
  }

  void addMinMaxFeedItemTimes(JsonObject contentObj,JsonObject crawlStatsJSON) { 
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
      crawlStatsJSON.addProperty("minPubDate",minPubDate);
      crawlStatsJSON.addProperty("maxPubDate",maxPubDate);
      crawlStatsJSON.addProperty("itemCount",itemCount);
    }
  }
  
  
  void updateLinkStatsFromLinkJSONObject(JsonObject jsonObject,URLFPV2 keyFP,Reporter reporter) { 
    JsonElement sourceElement = jsonObject.get("source_url");
    JsonElement hrefElement = jsonObject.get("href");
    
    if (sourceElement != null && hrefElement != null) { 
      incOutputKeyHitCount(hrefElement.getAsString());
      if (outputKeyURLObj == null) { 
        outputKeyURLObj = new GoogleURL(hrefElement.getAsString());
      }
      
      
      GoogleURL hrefSource = new GoogleURL(sourceElement.getAsString());
      
      if (hrefSource.isValid()) {

        if (linkSummaryRecord == null) { 
          linkSummaryRecord = new JsonObject();
        }
        
        // quick check first ... 
        if (!outputKeyURLObj.getHost().equals(hrefSource.getHost())) { 
          // ok now deeper check ...
          URLFPV2 sourceFP = URLUtils.getURLFPV2FromURLObject(hrefSource);
          if (sourceFP != null) { 
            if (sourceFP.getRootDomainHash() != keyFP.getRootDomainHash()) {
              reporter.incrCounter(Counters.GOT_EXTERNAL_DOMAIN_SOURCE, 1);
              safeIncrementJSONCounter(linkSummaryRecord,"ext_sources");
              if (linkSources == null || linkSources.size() < MAX_EXT_SOURCE_SAMPLES) {
                if (linkSources == null) 
                  linkSources = new HashMap<Long, String>();
                if (!linkSources.containsKey(sourceFP.getRootDomainHash())) { 
                  linkSources.put(sourceFP.getRootDomainHash(), sourceElement.getAsString());
                }
              }
            }
            else { 
              safeIncrementJSONCounter(linkSummaryRecord,"int_sources");
            }
          }
        }
        else {
          // internal for sure ... 
          safeIncrementJSONCounter(linkSummaryRecord,"int_sources");
        }
                  
        JsonObject sourceHeaders = jsonObject.getAsJsonObject("source_headers");
        if (sourceHeaders != null) { 
          long httpDate = safeGetHttpDate(sourceHeaders, "date");
          long lastModified = safeGetHttpDate(sourceHeaders, "last-modified");
          if (lastModified != -1 && lastModified < httpDate)
            httpDate = lastModified;
          if (httpDate != -1L) { 
            safeSetMinLongValue(linkSummaryRecord, "earlier_date", httpDate);
            safeSetMaxLongValue(linkSummaryRecord, "latest_date", httpDate);
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
        
        if (types.size() < MAX_TYPE_SAMPLES)
          types.add(sourceTypeAndRel);
      }
      
    }
  }
  
  void updateCrawlStatsFromJSONObject(JsonObject jsonObject,URLFPV2 fpSource,Reporter reporter) throws IOException { 
    
    JsonElement sourceHREFElement = jsonObject.get("source_url");

    if (sourceHREFElement != null) {
      incOutputKeyHitCount(sourceHREFElement.getAsString());
      if (outputKeyURLObj == null) { 
        outputKeyURLObj = new GoogleURL(sourceHREFElement.getAsString());
      }
    }
    
    if (summaryRecord == null) { 
      summaryRecord = new JsonObject();
    }
    String disposition = jsonObject.get("disposition").getAsString();
    long   attemptTime = jsonObject.get("attempt_time").getAsLong();
    
    safeIncrementJSONCounter(summaryRecord,"attempt_count");
    
    long latestAttemptTime = safeSetMaxLongValue(summaryRecord,"latest_attempt",attemptTime);
    
    JsonElement redirectObject = jsonObject.get("redirect_from");
    if (redirectObject != null) { 
      emitRedirectRecord(jsonObject, redirectObject.getAsJsonObject(), reporter);
    }

    if (latestAttemptTime == attemptTime) { 
      summaryRecord.addProperty("failed", (disposition.equals("SUCCESS")) ? false : true);
    }
    
    if (disposition.equals("SUCCESS")) {
    
      int httpResult = jsonObject.get("http_result").getAsInt();
      
      if (latestAttemptTime == attemptTime) { 
        summaryRecord.addProperty("http_result", httpResult);
      }
      
      if (httpResult == 200) {
        // inject all the details into a JSONObject 
        JsonObject crawlStatsJSON = new JsonObject();
        
        // basic stats ... starting with crawl time ...
        crawlStatsJSON.addProperty("server_ip", jsonObject.get("server_ip").getAsString());
        crawlStatsJSON.addProperty("attempt_time", attemptTime);
        crawlStatsJSON.addProperty("content_len",jsonObject.get("content_len").getAsInt());
        if (jsonObject.get("mime_type") != null) { 
          crawlStatsJSON.addProperty("mime_type",jsonObject.get("mime_type").getAsString());
        }
        if (jsonObject.get("md5") != null) {
          crawlStatsJSON.addProperty("md5",jsonObject.get("md5").getAsString());
        }
        if (jsonObject.get("text_simhash") != null) {
          crawlStatsJSON.addProperty("text_simhash",jsonObject.get("text_simhash").getAsLong());
        }
        //populate date headers ... 
        populateDateHeadersFromJSONObject(jsonObject,crawlStatsJSON);
        
        JsonElement parsedAs = jsonObject.get("parsed_as");
        
        if (parsedAs != null) {
          // populate some info based on type ... 
          crawlStatsJSON.addProperty("parsed_as",parsedAs.getAsString());
          
          String parsedAsString = parsedAs.getAsString();
          
          // if html ... 
          if (parsedAsString.equals("html")) { 
            JsonObject content = jsonObject.get("content").getAsJsonObject();
            if (content != null) { 
              JsonElement titleElement = content.get("title");
              JsonElement metaElement = content.get("meta_tags");
              if (titleElement != null) { 
                crawlStatsJSON.add("title", titleElement);
              }
              if (metaElement != null) { 
                crawlStatsJSON.add("meta_tags", metaElement);
              }
              // collect link stats for json ... 
              updateLinkStatsFromHTMLContent(crawlStatsJSON,content,fpSource,reporter);
            }
            
          }
          // if feed ... 
          else if (parsedAsString.equals("feed")) { 
            // get content ... 
            JsonObject content = jsonObject.get("content").getAsJsonObject();
            JsonElement titleElement = content.get("title");
            if (titleElement != null) { 
              crawlStatsJSON.add("title", titleElement);
            }
            // set update time ... 
            long updateTime = safeGetLong(content, "updated");
            if (updateTime != -1) { 
              crawlStatsJSON.addProperty("updated", updateTime);
            }
            
            addMinMaxFeedItemTimes(content,crawlStatsJSON);
          }
        }

        long latestCrawlTime = safeSetMaxLongValue(summaryRecord,"latest_crawl",attemptTime);
        
        if (latestCrawlTime == attemptTime) {
          // update latest http result 
          // update parsed as info 
          if (parsedAs != null) { 
            summaryRecord.addProperty("parsed_as", parsedAs.getAsString());
          }
          // update the timestamp 
          summaryRecord.addProperty("latest_crawl",attemptTime);
        }
        // always increment successfull crawl count .... 
        safeIncrementJSONCounter(summaryRecord,"crawl_count");

        // construct crawl stats array if necessary 
        JsonArray crawlStatsArray = summaryRecord.getAsJsonArray("crawl_stats");
        if (crawlStatsArray == null) { 
          crawlStatsArray = new JsonArray();
          summaryRecord.add("crawl_stats", crawlStatsArray);
        }
        // add crawl stats to it 
        crawlStatsArray.add(crawlStatsJSON);
      }
    }
  }
  
  
  void updateLinkStatsFromHTMLContent(JsonObject crawlStats,JsonObject content,URLFPV2 fpSource,Reporter reporter) { 
    JsonArray links = content.getAsJsonArray("links");
    
    if (links == null) { 
      reporter.incrCounter(Counters.NULL_LINKS_ARRAY, 1);
    }
    else { 
      int internalLinkCount = 0;
      int externalLinkCount = 0;
      
      for (JsonElement link : links) { 
        JsonObject linkObj = link.getAsJsonObject();
        if (linkObj != null && linkObj.has("href")) {
          String href = linkObj.get("href").getAsString();
          URLFPV2 linkFP = URLUtils.getURLFPV2FromURL(href);
          if (linkFP != null) { 
            if (linkFP.getRootDomainHash() == fpSource.getRootDomainHash()) { 
              internalLinkCount++;
            }
            else { 
              externalLinkCount++;
            }
          }
        }
      }
      crawlStats.addProperty("internal_links", internalLinkCount);
      crawlStats.addProperty("external_links", externalLinkCount);
    }
  }
  
  JsonParser _parser = new JsonParser();
  JsonObject summaryRecord = null;
  JsonObject linkSummaryRecord = null;
  HashSet<String> types = new HashSet<String>();
  HashMap<Long,String> linkSources = null;
  HashMap<String,Integer> outputKeyFreqCounter = new HashMap<String, Integer>();
  GoogleURL outputKeyURLObj = null;
  
  void incOutputKeyHitCount(String key) { 
    Integer existing = outputKeyFreqCounter.get(key);
    if (existing == null) { 
      outputKeyFreqCounter.put(key, 1);
    }
    else { 
      outputKeyFreqCounter.put(key, existing + 1);
    }
  }
  
  String getBestOutputKey() { 
    int highestFreq = 0;
    String bestTerm = null;
    for (Map.Entry<String,Integer> entry : outputKeyFreqCounter.entrySet()) { 
      if (entry.getValue() > highestFreq) {
        highestFreq = entry.getValue();
        bestTerm = entry.getKey();
      }
    }
    return bestTerm;
  }
  
  @Override
  public void reduce(IntWritable key, Iterator<Text> values,
      OutputCollector<TextBytes, TextBytes> output, Reporter reporter)
      throws IOException {

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
      
      summaryRecord = null;
      linkSummaryRecord = null;
      types.clear();
      linkSources = null;
      outputKeyFreqCounter.clear();
      outputKeyURLObj = null;
      
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
        
        long linkType = LinkKey.getLongComponentFromKey(keyBytes,LinkKey.ComponentId.TYPE_COMPONENT_ID);
        
        if (linkType == LinkKey.Type.KEY_TYPE_CRAWL_STATUS.ordinal()) {
          statusCount++;
          
          try { 
            JsonObject object = _parser.parse(valueBytes.toString()).getAsJsonObject();
            if (object != null) { 
              updateCrawlStatsFromJSONObject(object,fpSource,reporter);
            }
          }
          catch (Exception e) { 
            LOG.error("Error Parsing JSON:" + valueBytes.toString());
            throw new IOException(e);
          }
        }
        else {
          linkCount++;
          JsonObject object = _parser.parse(valueBytes.toString()).getAsJsonObject();
          // ok this is a link ... 
          updateLinkStatsFromLinkJSONObject(object,fpSource,reporter);
        }
        
        reporter.progress();
      }
      
      if (statusCount > 1) { 
        reporter.incrCounter(Counters.TWO_REDUNDANT_STATUS_IN_REDUCER,1);
      }
      
      if (statusCount == 0 && linkCount != 0) { 
        reporter.incrCounter(Counters.DISCOVERED_NEW_LINK, 1);
      }
      else { 
        if (statusCount >= 1 && linkCount >= 1) { 
          reporter.incrCounter(Counters.GOT_CRAWL_STATUS_WITH_LINK, 1);
        }
        else if (statusCount >= 1 && linkCount == 0) { 
          reporter.incrCounter(Counters.GOT_CRAWL_STATUS_NO_LINK, 1);
        }
      }
      
      if (summaryRecord != null || linkSummaryRecord != null) { 
        JsonObject compositeObject = new JsonObject();
        if (summaryRecord != null) { 
          compositeObject.add("crawl_status", summaryRecord);
        }
        if (linkSummaryRecord != null) { 
          if (types != null && types.size() != 0) { 
            stringCollectionToJsonArray(linkSummaryRecord,"typeAndRels",types);
            if (linkSources != null) { 
              stringCollectionToJsonArray(linkSummaryRecord,"sources",linkSources.values());
            }
          }
          compositeObject.add("link_status", linkSummaryRecord);
        }
        
        String bestKey = getBestOutputKey();
        if (bestKey != null && outputKeyURLObj != null && outputKeyURLObj.isValid()) { 
          output.collect(new TextBytes(bestKey),new TextBytes(compositeObject.toString()));
        }
        else { 
          reporter.incrCounter(Counters.FAILED_TO_GET_SOURCE_HREF, 1);
        }
      }
    }
  }

  
  
  JobConf _conf;
  FileSystem _fs;
  SequenceFile.Writer _redirectWriter = null;
  
  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }    

  boolean _skipPartition = false;
  
  @Override
  public void configure(JobConf job) {
  
    HashSet<Integer> onlyDoPartitions = null;
    String hack = job.get("hack");
    if (hack != null) {
      onlyDoPartitions = new HashSet<Integer>();
      JsonParser parser = new JsonParser();
      JsonArray hackArray = parser.parse(hack).getAsJsonArray();
      for (JsonElement element : hackArray) {   
        onlyDoPartitions.add(element.getAsInt());
      }
    }
    _conf = job;
    try {
      _fs = FileSystem.get(_conf);
      int partitionId = _conf.getInt("mapred.task.partition", 0);
      if (onlyDoPartitions == null || onlyDoPartitions.contains(partitionId)) { 
        Path redirectPath = new Path(FileOutputFormat.getWorkOutputPath(_conf),"redirect-" + NUMBER_FORMAT.format(partitionId));
        _redirectWriter = SequenceFile.createWriter(_fs, _conf, redirectPath, TextBytes.class, TextBytes.class,CompressionType.BLOCK);
      }
      else { 
        _skipPartition = true;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void close() throws IOException {
    if (_redirectWriter != null) { 
      _redirectWriter.close();
    }
    
  }
}
