package org.commoncrawl.crawl.database.crawlpipeline.ec2.postprocess.deduper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.crawl.database.crawlpipeline.ec2.postprocess.deduper.DeduperUtils.DeduperValue;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.internal.GoogleURL;
import org.commoncrawl.util.internal.URLUtils;
import org.commoncrawl.util.shared.TextBytes;
import org.mortbay.log.Log;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

/** 
 * read the (new) merge db, read simhash values, and C(n,k) variations of the simhash 
 * based on n=20 and k=3
 * 
 * @author rana
 *
 */
public class Stage1Mapper implements Mapper<TextBytes,TextBytes,LongWritable, DeduperValue> {

  JsonParser parser = new JsonParser();
  
  enum Counters { 
    GOT_CRAWL_STATUS,
    GOT_HTTP_RESULT,
    RESULT_WAS_HTTP_200,
    GOT_CRAWL_STATS_ARRAY,
    GOT_CRAWL_STATS_OBJECT
  , GOT_CRAWL_STATUS_WITH_SIMHASH, GOT_CRAWL_STATUS_WITHOUT_SIMHASH, GOT_EXCEPTION_PARSING_OBJECT, GOT_NULL_FP}
  
  @Override
  public void map(TextBytes key, TextBytes value,OutputCollector<LongWritable, DeduperValue> output, Reporter reporter)throws IOException {
    try { 
      JsonObject containerObj = parser.parse(value.toString()).getAsJsonObject();
      GoogleURL urlObject = new GoogleURL(key.toString());
      if (urlObject.isValid()) {
        String sourceRootDomain = URLUtils.extractRootDomainName(urlObject.getHost());
        if (sourceRootDomain != null){
          JsonArray arrayOut= new JsonArray();

          JsonObject crawlStatus = containerObj.getAsJsonObject("crawl_status");
          if (crawlStatus != null) {
            reporter.incrCounter(Counters.GOT_CRAWL_STATUS, 1);
            arrayOut.add(new JsonPrimitive(true));
            if (crawlStatus.has("http_result")) { 
              int httpResult = crawlStatus.get("http_result").getAsInt();
              reporter.incrCounter(Counters.GOT_HTTP_RESULT, 1);
              arrayOut.add(new JsonPrimitive((httpResult == 200)));
              if (httpResult == 200) {
                reporter.incrCounter(Counters.RESULT_WAS_HTTP_200, 1);
                JsonArray crawlStatsArray = crawlStatus.getAsJsonArray("crawl_stats");
                if (crawlStatsArray != null && crawlStatsArray.size() != 0) {
                  reporter.incrCounter(Counters.GOT_CRAWL_STATS_ARRAY, 1);
                  
                  // ok walk the array looking for the best candidate 
                  JsonObject selectedCrawlStatus = null;
                  
                  for (JsonElement crawlStatusEl: crawlStatsArray) { 
                    JsonObject crawlStatusObj = crawlStatusEl.getAsJsonObject();
                    if (crawlStatusObj.has("text_simhash")) {
                      
                      reporter.incrCounter(Counters.GOT_CRAWL_STATUS_WITH_SIMHASH, 1);
                    
                      if (selectedCrawlStatus == null) { 
                        selectedCrawlStatus = crawlStatusObj;
                      }
                      else { 
                        // ok get attempt time ... 
                        long attemptTimeThis = crawlStatusObj.get("attempt_time").getAsLong();
                        long attemptTimeOther = selectedCrawlStatus.get("attempt_time").getAsLong();
                        if (attemptTimeThis > attemptTimeOther) { 
                          selectedCrawlStatus = crawlStatusObj;
                        }
                      }
                    }
                    else { 
                      reporter.incrCounter(Counters.GOT_CRAWL_STATUS_WITHOUT_SIMHASH, 1);
                    }
                  }
                  // ok ... if selected crawl status found ... 
                  if (selectedCrawlStatus != null) { 
                     
                    long simhashValue = selectedCrawlStatus.get("text_simhash").getAsLong();
                    // emit it
                    emitItem(key,simhashValue,reporter,output);
                  }
                }
              }
            }
          }
        }
      }
    }
    catch (Exception e) { 
      reporter.incrCounter(Counters.GOT_EXCEPTION_PARSING_OBJECT, 1);
    }
  }

  LongWritable writableKeyToEmit = new LongWritable();
  DeduperValue valueOut = new DeduperValue();
  
  void emitItem(TextBytes key,long simhashValue,Reporter reporter,OutputCollector<LongWritable, DeduperValue> collector)throws IOException {
    URLFPV2 fp = URLUtils.getURLFPV2FromURL(key.toString());
    
    if (fp == null) { 
      reporter.incrCounter(Counters.GOT_NULL_FP, 1);
    }
    else { 
      // emit a value for each possible key combination based on simhash value ...  
      for (int i=0;i<1;++i) { 
        // create a unique key based on pattern index  
        long keyBitsOut = DeduperUtils.buildKeyForPatternIndex(i,simhashValue);
        // ok setup the writable key 
        DeduperUtils.DeduperKey.setKey(writableKeyToEmit, i, keyBitsOut);
        // and the value ... 
        valueOut.setValue(simhashValue,fp.getRootDomainHash(), fp.getUrlHash(), key);
        // emit it ... 
        //Log.info("KeyBits:" + keyBitsOut + "Key:" + writableKeyToEmit.get());
        collector.collect(writableKeyToEmit, valueOut);
      }
    }
  }
  
  
  @Override
  public void configure(JobConf job) {
    
  }

  @Override
  public void close() throws IOException {    
  }

}
