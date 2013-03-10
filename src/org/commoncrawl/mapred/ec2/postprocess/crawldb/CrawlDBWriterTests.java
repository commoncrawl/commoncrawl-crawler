package org.commoncrawl.mapred.ec2.postprocess.crawldb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import junit.framework.Assert;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Counters.Counter;
import org.commoncrawl.mapred.ec2.postprocess.crawldb.CrawlDBKey.LinkKeyComparator;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.ByteArrayUtils;
import org.commoncrawl.util.JSONUtils;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLFPBloomFilter;
import org.commoncrawl.util.URLUtils;
import org.commoncrawl.util.Tuples.Pair;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ibm.icu.util.StringTokenizer;

public class CrawlDBWriterTests extends CrawlDBWriter {
  @Test
  public void testSourceInputOutputWriters()throws IOException { 
    _sourceInputsBuffer = new DataOutputBuffer(16348*4);
    _sourceInputsTrackingFilter = new URLFPBloomFilter(100000, NUM_HASH_FUNCTIONS, NUM_BITS);
    
    String sourceDomainURL = "http://sourcedomain.com/foo";
    URLFPV2 sourceFP = URLUtils.getURLFPV2FromCanonicalURL(sourceDomainURL);
    
    String urls[] = { 
        "http://somedomain.com/foo",
        "http://someother.com/bar"
    };
    
    for (String url : urls) { 
      URLFPV2 fp = URLUtils.getURLFPV2FromCanonicalURL(url);
      // double insert and validate actual single insertion 
      trackPotentialLinkSource(fp,url,sourceFP);
      trackPotentialLinkSource(fp,url,sourceFP);
    }
    
    //  validate data ... 
    TextBytes firstVersion = new TextBytes();
    firstVersion.set(_sourceInputsBuffer.getData(),0,_sourceInputsBuffer.getLength());
    
    StringTokenizer tokenizer = new StringTokenizer(firstVersion.toString(), "\n");
    int itemIndex = 0;
    while (tokenizer.hasMoreElements()) { 
      String nextLine = tokenizer.nextToken();
      String splits[] = nextLine.split("\t");
      // validate fp 
      URLFPV2 fp = URLUtils.getURLFPV2FromCanonicalURL(urls[itemIndex]);
      Assert.assertEquals(fp.getDomainHash(),Long.parseLong(splits[0]));
      // validate actual url ... 
      Assert.assertEquals(splits[1],urls[itemIndex]);
      itemIndex++;
    }
    
    // reset output buffer ... 
    _sourceInputsBuffer = new DataOutputBuffer(16348*4);
    // and source bloom filter ... 
    _sourceInputsTrackingFilter = new URLFPBloomFilter(10000000, NUM_HASH_FUNCTIONS, NUM_BITS);
    importLinkSourceData(sourceFP,firstVersion);
    // second text should match first .. 
    TextBytes secondVersion = new TextBytes();
    secondVersion.set(_sourceInputsBuffer.getData(),0,_sourceInputsBuffer.getLength());
    Assert.assertEquals(firstVersion,secondVersion);
  }
  
  /**
   * Mock Collector 
   */
  
  static class MockCollectorReporter implements OutputCollector<TextBytes,TextBytes>, Reporter {
    
    public ArrayList<Pair<TextBytes,TextBytes>> items = new ArrayList<Pair<TextBytes,TextBytes>>();
    
    @Override
    public void progress() {
    }

    @Override
    public void setStatus(String status) {
    }

    @Override
    public Counter getCounter(Enum<?> name) {
      return null;
    }

    @Override
    public Counter getCounter(String group, String name) {
      return null;
    }

    @Override
    public void incrCounter(Enum<?> key, long amount) {
    }

    @Override
    public void incrCounter(String group, String counter, long amount) {
    }

    @Override
    public InputSplit getInputSplit() throws UnsupportedOperationException {
      return null;
    }

    @Override
    public void collect(TextBytes key, TextBytes value) throws IOException {
      items.add(new Pair<TextBytes,TextBytes>(new TextBytes(key.toString()),new TextBytes(value.toString())));
    } 
  }
  
  private static final int CANNED_SIMHASH_VALUE = 1234;
  private static final String CANNED_MD5_VALUE = "5d41402abc4b2a76b9719d911017c592";
  private static final String CANNED_MIME_TYPE = "text/html";
  private static final int CANNED_CONTENT_LEN = 100;
  private static final String CANNED_IP = "1.1.1.1";
  private static final String CANNED_TITLE = "title";
  private static final String CANNED_META_PROPERTY_NAME =  "metaProperty";
  private static final String CANNED_META_PROPERTY_VALUE =  "metaValue";
  private static final String CANNED_LINKING_HOST_1 = "link.host.one.com";
  private static final String CANNED_LINKING_HOST_2 = "link.host.two.com";
  private static final String CANNED_LINKING_HOST_3 = "link.host.three.com";
  private static final String EXTRA_PROPERTY_REDIRECT_SOURCE = "redirectSource";
  private static final String EXTRA_PROPERTY_LINK_SOURCE = "linkSource";
  private static final String CANNED_CRAWL_URL_1 = "http://cannedone.com/";
  private static final String CANNED_URL_1_HOST = "cannedone.com";
  private static final String CANNED_CRAWL_URL_2 = "http://cannedtwo.com/";
  private static final String CANNED_URL_2_HOST = "cannedtwo.com";
  private static final String CANNED_CRAWL_URL_3 = "http://cannedthree.com/";
  private static final String CANNED_URL_3_HOST = "cannedthree.com";
  private static final String CANNED_CRAWL_URL_4 = "http://cannedfour.com/";
  private static final String CANNED_URL_4_HOST = "cannedfour.com";
  private static final String CANNED_CRAWL_URL_5 = "http://cannedfive.com/";
  private static final String CANNED_URL_5_HOST = "cannedfive.com";

  private static final long   CANNED_TIMESTAMP_0 = 123456700L;
  private static final long   CANNED_TIMESTAMP_1 = 123456789L;
  private static final long   CANNED_TIMESTAMP_2 = 123456799L;
  private static final long   CANNED_TIMESTAMP_3 = 123466799L;
  private static final String CANNED_FAILURE_REASON = "FailureReason";
  private static final String CANNED_FAILURE_DETAIL = "FailureDetail";
  
  static String sourceURLToLinkingHostURL(String sourceURL, String linkingHostName) {
    return "http://" + linkingHostName + "/" + MD5Hash.digest(sourceURL + Long.toString(System.nanoTime())).toString();
  }
  enum TestRecordType { 
    CRAWL_STATUS,
    CRAWL_STATUS_WITH_REDIRECT,
    INLINK
  };
  
  public static class TestModel implements Comparable<TestModel> { 
    
    TreeMap<URLFPV2,URLStateModel> fpToModelMap = new TreeMap<URLFPV2, URLStateModel>();
    
    /** 
     * update the model from the raw (generated tuples) 
     * @param tuple
     * @throws Exception
     */
    void updateModelFromInputTuple(Pair<TextBytes,TextBytes> tuple) throws Exception { 
      URLFPV2 fp = new URLFPV2();
      // get key ... 
      fp.setRootDomainHash(CrawlDBKey.getLongComponentFromKey(tuple.e0, CrawlDBKey.ComponentId.ROOT_DOMAIN_HASH_COMPONENT_ID));
      fp.setDomainHash(CrawlDBKey.getLongComponentFromKey(tuple.e0, CrawlDBKey.ComponentId.DOMAIN_HASH_COMPONENT_ID));
      fp.setUrlHash(CrawlDBKey.getLongComponentFromKey(tuple.e0, CrawlDBKey.ComponentId.URL_HASH_COMPONENT_ID));
      
      long recordType = CrawlDBKey.getLongComponentFromKey(tuple.e0,CrawlDBKey.ComponentId.TYPE_COMPONENT_ID);
      
      if (recordType == CrawlDBKey.Type.KEY_TYPE_CRAWL_STATUS.ordinal() || recordType == CrawlDBKey.Type.KEY_TYPE_HTML_LINK.ordinal()) {
        // update model given key ...
        URLStateModel urlModel = fpToModelMap.get(fp);
        if (urlModel == null) { 
          urlModel = new URLStateModel();
          urlModel.fp = fp;
          fpToModelMap.put(fp, urlModel);
        }
        
        if (recordType == CrawlDBKey.Type.KEY_TYPE_CRAWL_STATUS.ordinal()) { 
          JsonObject redirectJSON = urlModel.updateModelGivenCrawlStatus(tuple.e1);
          
          if (redirectJSON != null) { 
            URLFPV2 redirectFP = URLUtils.getURLFPV2FromURL(redirectJSON.get("source_url").getAsString());
            TextBytes key = CrawlDBKey.generateKey(redirectFP,CrawlDBKey.Type.KEY_TYPE_CRAWL_STATUS,redirectJSON.get("attempt_time").getAsLong());
            Pair<TextBytes,TextBytes> redirectTuple = new Pair<TextBytes, TextBytes>(key,new TextBytes(redirectJSON.toString()));
            updateModelFromInputTuple(redirectTuple);
          }
          
        }
        else if (recordType == CrawlDBKey.Type.KEY_TYPE_HTML_LINK.ordinal()) { 
          urlModel.updateModelGivenLinkRecord(tuple.e1);
        }
      }
    }
    
    /** 
     * build a model from a set of output tuples 
     * (captured by the mock collector)
     * @param tupleList
     */
    void buildModelFromOutputTuples(List<Pair<TextBytes,TextBytes>> tuples) throws IOException { 
      for (Pair<TextBytes,TextBytes> tuple : tuples) { 
        URLFPV2 fp = new URLFPV2();
        // get key ... 
        fp.setRootDomainHash(CrawlDBKey.getLongComponentFromKey(tuple.e0, CrawlDBKey.ComponentId.ROOT_DOMAIN_HASH_COMPONENT_ID));
        fp.setDomainHash(CrawlDBKey.getLongComponentFromKey(tuple.e0, CrawlDBKey.ComponentId.DOMAIN_HASH_COMPONENT_ID));
        fp.setUrlHash(CrawlDBKey.getLongComponentFromKey(tuple.e0, CrawlDBKey.ComponentId.URL_HASH_COMPONENT_ID));
        // get type ... 
        long recordType = CrawlDBKey.getLongComponentFromKey(tuple.e0,CrawlDBKey.ComponentId.TYPE_COMPONENT_ID);
        // validate that the record type is only of one of two varieties supported in final output ... 
        Assert.assertTrue(recordType == CrawlDBKey.Type.KEY_TYPE_MERGED_RECORD.ordinal() || recordType == CrawlDBKey.Type.KEY_TYPE_INCOMING_URLS_SAMPLE.ordinal());
        // create url model object if necessary ... 
        URLStateModel urlModel = fpToModelMap.get(fp);
        if (urlModel == null) { 
          urlModel = new URLStateModel();
          urlModel.fp = fp;
          fpToModelMap.put(fp, urlModel);
        }
        
        // ok do stuff based on record type ... 
        if (recordType == CrawlDBKey.Type.KEY_TYPE_MERGED_RECORD.ordinal()) { 
          urlModel.updateModelGivenMergedRecord(tuple.e1);
        }
        else { 
          urlModel.updateModelGivenUrlsSampleRecord(tuple.e1);
        }
      }
    }
    
    
    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      
      for (Map.Entry<URLFPV2,URLStateModel>  urlRecord : fpToModelMap.entrySet()) { 
        sb.append(urlRecord.getKey() + "\n" + urlRecord.toString());
      }
      return sb.toString();
    }

    @Override
    public int compareTo(TestModel other) {
      int result = 0;
      for (URLFPV2 fp : fpToModelMap.keySet()) { 
        if (!other.fpToModelMap.containsKey(fp)) { 
          System.out.println("FP:" + fp.getKey() + " not Found in Other Model");
          result = -1;
          break;
        }
        else { 
          URLStateModel leftSideModel = fpToModelMap.get(fp);
          URLStateModel rightSideModel = other.fpToModelMap.get(fp);
          
          result = leftSideModel.compareTo(rightSideModel);
          if (result != 0)  { 
            System.out.println("URLModels for FP:" + fp.getKey() + " did not match");
            break;
          }
        }
      }
      return result;
    }
    
  }
  
  public static class URLStateModel implements Comparable<URLStateModel> { 
    URLFPV2       fp;
    public String  source_url = null;
    public boolean has_crawl_status = false;
    public long   latest_attempt_time = -1;
    public long   latest_crawl_time = -1;
    public int    attempt_count = 0;
    public int    crawl_count = 0;
    public String parsed_as = null;
    public int    http_result = -1;
    public String redirect_url = null;
    public SortedSet<String> ext_urls = new TreeSet<String>();
    public SortedSet<JsonObject> details  = new TreeSet<JsonObject>(new Comparator<JsonObject>() {

      @Override
      public int compare(JsonObject o1, JsonObject o2) {
        String md51 = MD5Hash.digest(o1.toString()).toString();
        String md52 = MD5Hash.digest(o2.toString()).toString();
        return md51.compareTo(md52);
      }
    });
    
    public TreeMap<Long,String> incoming = new TreeMap<Long, String>();
    
    
    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("source:"+ source_url + "\n");
      sb.append("has_crawl_status:" +  has_crawl_status + "\n");
      sb.append("latest_crawl_time:" + latest_crawl_time + "\n");
      sb.append("attempt_count:" + attempt_count + "\n");
      sb.append("crawl_count:" + crawl_count + "\n");
      sb.append("parsed_as:" + parsed_as + "\n");
      sb.append("http_result:" + http_result + "\n");
      sb.append("redirect_url:" + redirect_url + "\n");
      for (String extURL : ext_urls) { 
        sb.append("extURL:" + extURL + "\n");
      }
      for (JsonObject jsonObj : details) { 
        sb.append("crawlStatus:" + jsonObj.toString() + "\n");
      }
      for (String value : incoming.values()) { 
        sb.append("incoming:" + value + "\n");
      }
      sb.append("\n");
      return sb.toString();
    }
    
    void updateModelGivenMergedRecord(TextBytes mergedJSON) throws IOException { 
      JsonObject mergeObject = new JsonParser().parse(mergedJSON.toString()).getAsJsonObject();
      
      source_url = mergeObject.get(TOPLEVEL_SOURCE_URL_PROPRETY).getAsString();
      has_crawl_status = mergeObject.has(TOPLEVEL_SUMMARYRECORD_PROPRETY);
      if (has_crawl_status) {
        JsonObject crawlStatusObj = mergeObject.getAsJsonObject(TOPLEVEL_SUMMARYRECORD_PROPRETY);
      
        latest_attempt_time =  crawlStatusObj.get(SUMMARYRECORD_LATEST_ATTEMPT_PROPERTY).getAsLong();
        latest_crawl_time   =  safeGetLong(crawlStatusObj, SUMMARYRECORD_LATEST_CRAWLTIME_PROPERTY);
        attempt_count = crawlStatusObj.get(SUMMARYRECORD_ATTEMPT_COUNT_PROPERTY).getAsInt();
        crawl_count = (crawlStatusObj.has(SUMMARYRECORD_CRAWLCOUNT_PROPERTY)) ? crawlStatusObj.get(SUMMARYRECORD_CRAWLCOUNT_PROPERTY).getAsInt() : 0;
        parsed_as = (crawlStatusObj.has(SUMMARYRECORD_PARSEDAS_PROPERTY)) ? crawlStatusObj.get(SUMMARYRECORD_PARSEDAS_PROPERTY).getAsString() : null;
        http_result = (crawlStatusObj.has(SUMMARYRECORD_HTTP_RESULT_PROPERTY)) ? crawlStatusObj.get(SUMMARYRECORD_HTTP_RESULT_PROPERTY).getAsInt() : -1;  
        redirect_url = (crawlStatusObj.has(SUMMARYRECORD_REDIRECT_URL_PROPERTY)) ? crawlStatusObj.get(SUMMARYRECORD_REDIRECT_URL_PROPERTY).getAsString() : null;
        safeJsonArrayToStringCollection(crawlStatusObj, SUMMARYRECORD_EXTERNALLY_REFERENCED_URLS, ext_urls);
        if (crawlStatusObj.has(SUMMARYRECORD_CRAWLDETAILS_ARRAY_PROPERTY)) { 
          for (JsonElement crawlDetail : crawlStatusObj.getAsJsonArray(SUMMARYRECORD_CRAWLDETAILS_ARRAY_PROPERTY)) { 
            details.add(crawlDetail.getAsJsonObject());
          }
        }
      }
    }
    
    void updateModelGivenUrlsSampleRecord(TextBytes inputData) { 
      int curpos =inputData.getOffset();
      int endpos = inputData.getOffset() + inputData.getLength();
      
      byte lfPattern[] = { 0xA };
      byte tabPattern[] = { 0x9 };
      
      TextBytes urlText = new TextBytes();
      
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
            incoming.put(sourceDomainHash, urlText.toString());
            curpos = lfIndex + 1;
          }
        }
      }
    }
    
    JsonObject updateModelGivenCrawlStatus(TextBytes statusJSON) throws IOException {
      has_crawl_status = true;
      JsonParser parser = new JsonParser();
      JsonObject jsonObj = parser.parse(statusJSON.toString()).getAsJsonObject();
      if (source_url == null) { 
        source_url = jsonObj.get("source_url").getAsString();
      }
      
      HashSet<String> extHrefs = new HashSet<String>();
      JsonObject crawlDetailRecord = crawlDetailRecordFromCrawlStatusRecord(jsonObj, fp, extHrefs, new MockCollectorReporter());
      long attemptTime = safeGetLong(crawlDetailRecord,CRAWLDETAIL_ATTEMPT_TIME_PROPERTY);
      latest_attempt_time = Math.max(attemptTime, latest_attempt_time);
      attempt_count++;
      int httpResult = safeGetInteger(crawlDetailRecord, CRAWLDETAIL_HTTPRESULT_PROPERTY);
      if (httpResult != -1) {
        if (latest_attempt_time == attemptTime) { 
          this.http_result = httpResult;
        }
        if (httpResult >= 200 && httpResult <= 299) { 
          latest_crawl_time = Math.max(attemptTime, latest_crawl_time);
          crawl_count++;
          if (latest_crawl_time == attemptTime) {
            this.parsed_as = crawlDetailRecord.get(CRAWLDETAIL_PARSEDAS_PROPERTY).getAsString();
            this.ext_urls.clear();
            this.ext_urls.addAll(extHrefs);
          }
        }
        else if (httpResult >= 300 && httpResult <= 399) { 
          this.redirect_url = (crawlDetailRecord.get(CRAWLDETAIL_REDIRECT_URL) != null) ? crawlDetailRecord.get(CRAWLDETAIL_REDIRECT_URL).getAsString() : null;
        }
      }
      this.details.add(crawlDetailRecord);
      
      if (jsonObj.has("redirect_from")) { 

        JsonObject redirectObject = jsonObj.get("redirect_from").getAsJsonObject();
        
        
        JsonObject redirectJSON = new JsonObject();
        
        int redirectHttpResult = redirectObject.get("http_result").getAsInt();
        
        redirectJSON.addProperty("disposition","SUCCESS");
        redirectJSON.addProperty("http_result",redirectHttpResult);
        redirectJSON.addProperty("server_ip",redirectObject.get("server_ip").getAsString());
        redirectJSON.addProperty("attempt_time",jsonObj.get("attempt_time").getAsLong());
        redirectJSON.addProperty("target_url",jsonObj.get("source_url").getAsString());
        redirectJSON.addProperty("source_url",redirectObject.get("source_url").getAsString());
        
        return redirectJSON;
      }
      return null;
    }
    
    public void updateModelGivenLinkRecord(TextBytes linkJSON) {

      JsonParser parser = new JsonParser();
      JsonObject jsonObj = parser.parse(linkJSON.toString()).getAsJsonObject();
      if (source_url == null) { 
        source_url = jsonObj.get("href").getAsString();
      }
      
      String sourceURL = jsonObj.get("source_url").getAsString();
      URLFPV2 urlfp = URLUtils.getURLFPV2FromURL(sourceURL);
      if (urlfp != null) {
        if (urlfp.getRootDomainHash() != fp.getRootDomainHash()) { 
          if (!incoming.containsKey(urlfp.getRootDomainHash())) {
            incoming.put(urlfp.getRootDomainHash(), sourceURL);
          }
        }
      }
    }

    static int compareStrings(String string1,String string2) {
      int result = 0;
      if (string1 == null && string2 != null || string1 != null && string2 == null) {
        result = -1;
      }
      if (result == 0 && string1 != null && string2 != null) { 
        result = string1.compareTo(string2);
      }
      return result;
    }
    
    void printMismatch(String mismatchDetail) { 
      System.out.println("FP:" + fp.getKey() + " SourceURL:" + source_url + " Detail:" + mismatchDetail);
    }
        
    @Override
    public int compareTo(URLStateModel o) {
      if (compareStrings(source_url,o.source_url) != 0) {
        printMismatch("source_url:"+source_url+ " other:" + o.source_url);
        return -1;
      }

      if (has_crawl_status != o.has_crawl_status) {
        printMismatch("has_crawl_status:"+has_crawl_status+ " other:" + o.has_crawl_status);
        return -1;
      }

      if (latest_attempt_time != o.latest_attempt_time) {
        printMismatch("latest_attempt_time:"+latest_attempt_time+ " other:" + o.latest_attempt_time);
        return -1;
      }

      if (latest_crawl_time != o.latest_crawl_time) {
        printMismatch("latest_crawl_time:"+latest_crawl_time+ " other:" + o.latest_crawl_time);
        return -1;
      }

      if (attempt_count != o.attempt_count) {
        printMismatch("attempt_count:"+attempt_count+ " other:" + o.attempt_count);
        return -1;
      }

      if (crawl_count != o.crawl_count) {
        printMismatch("crawl_count:"+crawl_count+ " other:" + o.crawl_count);
        return -1;
      }

      if (compareStrings(parsed_as,o.parsed_as) !=0) {
        printMismatch("parsed_as:"+parsed_as+ " other:" + o.parsed_as);
        return -1;
      }

      if (http_result != o.http_result) {
        printMismatch("http_result:"+http_result+ " other:" + o.http_result);
        return -1;
      }

      if (compareStrings(redirect_url,o.redirect_url) != 0) {
        printMismatch("redirect_url:"+redirect_url+ " other:" + o.redirect_url);
        return -1;
      }
      
      Set<String> urlDiff = Sets.difference(ext_urls, o.ext_urls);
      
      if (urlDiff.size() != 0) { 
        System.out.println("URLS:" + urlDiff + " missing from other Model");
        return -1;
      }
      
      Set<JsonObject> jsonDiff = Sets.difference(this.details, o.details);
      if (jsonDiff.size() != 0) {
        System.out.println("JSON Objects:" + jsonDiff + " missing from other Model");
        return -1;
      }
      return 0;
    }
  }
  
  static Pair<TextBytes,TextBytes> generateTestRecord(TestModel model,String url, long timestamp,boolean success,TestRecordType recordType,Map<String,String> extraProperties)throws Exception { 
    Pair<TextBytes,TextBytes> result = null;
    if (recordType == TestRecordType.CRAWL_STATUS || recordType == TestRecordType.CRAWL_STATUS_WITH_REDIRECT) {
      JsonObject topObject = new JsonObject();
    
      topObject.addProperty("source_url", url);
      topObject.addProperty("disposition", (success) ? "SUCCESS" : "FAILURE");
      topObject.addProperty("attempt_time", timestamp);
      if (success) { 
   
          topObject.addProperty("http_result",200);
          topObject.addProperty("server_ip",CANNED_IP);
          topObject.addProperty("content_len",CANNED_CONTENT_LEN);
          topObject.addProperty("mime_type",CANNED_MIME_TYPE);
          topObject.addProperty("md5",CANNED_MD5_VALUE);
          topObject.addProperty("text_simhash",CANNED_SIMHASH_VALUE);
          JsonObject headers = new JsonObject();
          headers.addProperty("date", timestamp);
          topObject.add("http_headers",headers);
          topObject.addProperty("parsed_as","html");
          JsonObject content = new JsonObject();
          content.addProperty("title", CANNED_TITLE);
          JsonArray metaTags = new JsonArray();
          JsonObject metaTag = new JsonObject();
          metaTag.addProperty(CANNED_META_PROPERTY_NAME, CANNED_META_PROPERTY_VALUE);
          metaTags.add(metaTag);
          content.add("meta_tags", metaTags);
          topObject.add("content", content);
          
          JsonArray links = new JsonArray();
          JsonObject link1 = new JsonObject();
          link1.addProperty("href", sourceURLToLinkingHostURL(url, CANNED_LINKING_HOST_1));
          JsonObject link2 = new JsonObject();
          link2.addProperty("href", sourceURLToLinkingHostURL(url, CANNED_LINKING_HOST_2));
          links.add(link1);
          links.add(link2);
          topObject.add("links", links);
          
          if (recordType == TestRecordType.CRAWL_STATUS_WITH_REDIRECT) { 
            JsonObject redirectObject = new JsonObject();
            redirectObject.addProperty("source_url", extraProperties.get(EXTRA_PROPERTY_REDIRECT_SOURCE));
            redirectObject.addProperty("http_result", 301);
            redirectObject.addProperty("server_ip",CANNED_IP);
            topObject.add("redirect_from", redirectObject);
          }
      }
      else { 
        topObject.addProperty("failure_reason",CANNED_FAILURE_REASON);
        topObject.addProperty("failure_detail",CANNED_FAILURE_DETAIL);
      }
      
      
      TextBytes keyOut = new TextBytes(CrawlDBKey.generateCrawlStatusKey(new Text(url), timestamp));
      TextBytes valueOut = new TextBytes(topObject.toString());
      
      result =  new Pair<TextBytes, TextBytes>(keyOut, valueOut);
    }
    else { 
      JsonObject linkData = new JsonObject();
      
      linkData.addProperty("href",url);
      linkData.addProperty("source_url", extraProperties.get(EXTRA_PROPERTY_LINK_SOURCE));
      linkData.addProperty("http_result", 200);
      linkData.addProperty("server_ip",CANNED_IP);
      linkData.addProperty("source_type","html");
      linkData.addProperty("type","a");
      linkData.addProperty("rel","text/html");

      TextBytes keyOut = new TextBytes(
          CrawlDBKey.generateLinkKey(
              new TextBytes(url),
              CrawlDBKey.Type.KEY_TYPE_HTML_LINK,
              MD5Hash.digest(extraProperties.get(EXTRA_PROPERTY_LINK_SOURCE)).toString()));
      
      TextBytes valueOut = new TextBytes(linkData.toString());
      
      result =  new Pair<TextBytes,TextBytes> (keyOut,valueOut);
    }
    
    model.updateModelFromInputTuple(result);
    return result;
  }

  static class TestDataComparator  implements java.util.Comparator<Pair<TextBytes,TextBytes>> { 
    CrawlDBKey.LinkKeyComparator comparator = new LinkKeyComparator();
    
    @Override
    public int compare(Pair<TextBytes,TextBytes> o1, Pair<TextBytes,TextBytes> o2) {
      return comparator.compare(o1.e0, o2.e0);
    } 
  }
  
  void reduceTestTuples(ArrayList<Pair<TextBytes,TextBytes>> tupleList,OutputCollector<TextBytes, TextBytes> collector, Reporter reporter)throws Exception { 
    Collections.sort(tupleList, new TestDataComparator());
    for (Pair<TextBytes,TextBytes> tuple : tupleList) {
      reduce(tuple.e0, Iterators.forArray(tuple.e1),collector,reporter);
    }
  }
  
  static void dumpTuples(List<Pair<TextBytes,TextBytes>> items) { 
    for (Pair<TextBytes,TextBytes> outputTuple : items) { 
      System.out.println("Key:" + outputTuple.e0);
      try { 
        JsonParser parser = new JsonParser();
        JsonElement e = parser.parse(outputTuple.e1.toString());
        JSONUtils.prettyPrintJSON(e);
      }
      catch (Exception e) { 
        System.out.println("Value:" + outputTuple.e1);
      }
    }
  }
  

  
  @Test
  public void testMerge()throws Exception  {
    _sourceInputsBuffer = new DataOutputBuffer(16348*4);
    _sourceInputsTrackingFilter = new URLFPBloomFilter(100000, NUM_HASH_FUNCTIONS, NUM_BITS);

    MockCollectorReporter collector = new MockCollectorReporter();
    
    TestModel inputModel = new TestModel();
    TestModel outputModel = new TestModel();
    
    ArrayList<Pair<TextBytes,TextBytes>> initialTupleList = new ArrayList<Pair<TextBytes,TextBytes>>();

    ///////////////////////////
    // STEP:1
    ///////////////////////////
    
    // populate an initial list of tuples ... 
    
    // crawl url 1 with http 200 crawl status  
    initialTupleList.add(generateTestRecord(inputModel,CANNED_CRAWL_URL_1,CANNED_TIMESTAMP_2,false,TestRecordType.CRAWL_STATUS,null));
    // crawl url 1 with failure 
    initialTupleList.add(generateTestRecord(inputModel,CANNED_CRAWL_URL_1,CANNED_TIMESTAMP_1,true,TestRecordType.CRAWL_STATUS,null));
    // inlinks into crawl url 1 ...  
    initialTupleList.add(generateTestRecord(inputModel,CANNED_CRAWL_URL_1,CANNED_TIMESTAMP_1,true,TestRecordType.INLINK,
        new ImmutableMap.Builder<String, String>()
          .put(EXTRA_PROPERTY_LINK_SOURCE, sourceURLToLinkingHostURL(CANNED_CRAWL_URL_1, CANNED_LINKING_HOST_1))
          .build()));
    initialTupleList.add(generateTestRecord(inputModel,CANNED_CRAWL_URL_1,CANNED_TIMESTAMP_1,true,TestRecordType.INLINK,
        new ImmutableMap.Builder<String, String>()
          .put(EXTRA_PROPERTY_LINK_SOURCE, sourceURLToLinkingHostURL(CANNED_CRAWL_URL_1, CANNED_LINKING_HOST_2))
          .build()));

    // crawl url 2 crawl status 
    initialTupleList.add(generateTestRecord(inputModel,CANNED_CRAWL_URL_2,CANNED_TIMESTAMP_1,true,TestRecordType.CRAWL_STATUS,null));

    // inlink into crawl url 3 with no crawl status 
    initialTupleList.add(generateTestRecord(inputModel,CANNED_CRAWL_URL_3,CANNED_TIMESTAMP_1,true,TestRecordType.INLINK,
        new ImmutableMap.Builder<String, String>()
          .put(EXTRA_PROPERTY_LINK_SOURCE, sourceURLToLinkingHostURL(CANNED_CRAWL_URL_3, CANNED_LINKING_HOST_1))
          .build()));
    initialTupleList.add(generateTestRecord(inputModel,CANNED_CRAWL_URL_3,CANNED_TIMESTAMP_1,true,TestRecordType.INLINK,
        new ImmutableMap.Builder<String, String>()
          .put(EXTRA_PROPERTY_LINK_SOURCE, sourceURLToLinkingHostURL(CANNED_CRAWL_URL_3, CANNED_LINKING_HOST_2))
          .build()));
    initialTupleList.add(generateTestRecord(inputModel,CANNED_CRAWL_URL_3,CANNED_TIMESTAMP_1,true,TestRecordType.INLINK,
        new ImmutableMap.Builder<String, String>()
          .put(EXTRA_PROPERTY_LINK_SOURCE, sourceURLToLinkingHostURL(CANNED_CRAWL_URL_3, CANNED_URL_3_HOST))
          .build()));
    
    // run reducer ... 
    reduceTestTuples(initialTupleList, collector, collector);
    close();
    System.out.println("STEP:1 DONE#########################################");
    
    ///////////////////////////
    // STEP:2
    ///////////////////////////

    // reset bloom filter ... 
    _sourceInputsTrackingFilter = new URLFPBloomFilter(100000, NUM_HASH_FUNCTIONS, NUM_BITS);
    // swap items ... 
    ArrayList<Pair<TextBytes,TextBytes>> tuples = collector.items;
    collector.items = new ArrayList<Pair<TextBytes,TextBytes>>();

    // tuples for step 2. 
    
    // another failure for crawl url 1
    tuples.add(generateTestRecord(inputModel,CANNED_CRAWL_URL_1,CANNED_TIMESTAMP_3,true,TestRecordType.CRAWL_STATUS,null));
    // inlink to crawl url 1 
    tuples.add(generateTestRecord(inputModel,CANNED_CRAWL_URL_1,CANNED_TIMESTAMP_3,true,TestRecordType.INLINK,
        new ImmutableMap.Builder<String, String>()
          .put(EXTRA_PROPERTY_LINK_SOURCE, sourceURLToLinkingHostURL(CANNED_CRAWL_URL_1, CANNED_LINKING_HOST_3))
          .build()));
    // crawl status from crawl url4 with a redirect from crawl url 5
    tuples.add(generateTestRecord(inputModel,CANNED_CRAWL_URL_4,CANNED_TIMESTAMP_1,true,TestRecordType.CRAWL_STATUS_WITH_REDIRECT,
        new ImmutableMap.Builder<String, String>()
        .put(EXTRA_PROPERTY_REDIRECT_SOURCE, CANNED_CRAWL_URL_5)
        .build()));

    // reduce
    reduceTestTuples(tuples, collector, collector);
    close();
    System.out.println("STEP:2 DONE#########################################");
    
    ///////////////////////////
    // STEP:3
    ///////////////////////////

    // reset bloom filter ... 
    _sourceInputsTrackingFilter = new URLFPBloomFilter(100000, NUM_HASH_FUNCTIONS, NUM_BITS);
    // swap items ... 
    tuples = collector.items;
    collector.items = new ArrayList<Pair<TextBytes,TextBytes>>();
    
    // run reducer again to pick up redirect record 
    reduceTestTuples(tuples, collector, collector);
    close();
    System.out.println("STEP:3 DONE#########################################");
    
    
    // dump tuples
    System.out.println("TUPLES DUMP STARTING#########################################");
    dumpTuples(collector.items);
    System.out.println("TUPLES DUMP COMPLETE#########################################");

    
    // feed final tuple set to output model ... 
    outputModel.buildModelFromOutputTuples(collector.items);
    
    // model comparison
    System.out.println("MODEL COMP STARTING#########################################");
    Assert.assertEquals(inputModel.compareTo(outputModel),0);
    System.out.println("MODEL COMP DONE    #########################################");
  }
}
