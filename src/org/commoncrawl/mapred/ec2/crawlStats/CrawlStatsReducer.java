package org.commoncrawl.mapred.ec2.crawlStats;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class CrawlStatsReducer implements Reducer<Text,Text,Text,Text>{

	MultipleOutputs mos;
	JsonParser parser = new JsonParser();

	@Override
	public void configure(JobConf job) {
		mos = new MultipleOutputs(job);
	}

	@Override
	public void close() throws IOException {
		mos.close();
	}

	enum Counters { 
	  HAD_SPACE_IN_URL, URL_CORRUPT_IN_ARC_DATA, HAD_ARC_DATA
	}
	@Override
	public void reduce(Text hostKey, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

	  int crawledCount  =0;
	  int failedCount   =0;
	  int http200Count = 0;
	  int http403Count = 0;
	  int http404Count = 0;
	  int httpOtherCount = 0;
	  int redirectCount = 0;

	  
	  while (values.hasNext()) {
	    
	    String jsonText = values.next().toString();
	    
      JsonObject jsonObject = parser.parse(jsonText).getAsJsonObject();
      
      String canonicalURL = jsonObject.get("canonical_url").getAsString();
      String sourceURL    = jsonObject.get("source_url").getAsString();
      boolean hasARCData  = jsonObject.has("archiveInfo");

      JsonObject crawlRecord = new JsonObject();
      
      // build base crawl record ... 
      crawlRecord.addProperty("source_url", sourceURL);
      crawlRecord.addProperty("disposition",jsonObject.get("disposition").getAsString());
      crawlRecord.addProperty("attempt_time",jsonObject.get("attempt_time").getAsLong());
      
      if (hasARCData) { 
        reporter.incrCounter(Counters.HAD_ARC_DATA, 1);
      }
      if (sourceURL.indexOf(' ') != -1) { 
        reporter.incrCounter(Counters.HAD_SPACE_IN_URL, 1);
        if (hasARCData) { 
          reporter.incrCounter(Counters.URL_CORRUPT_IN_ARC_DATA, 1);
          
          JsonObject archiveInfo = jsonObject.get("archiveInfo").getAsJsonObject();
          
          
          String arcFileName 
          =  Long.toString(archiveInfo.get("arcSourceSegmentId").getAsLong()) + "_" +
             Long.toString(archiveInfo.get("arcFileDate").getAsLong()) +"_"+
             Integer.toString(archiveInfo.get("arcFileParition").getAsInt());
          
          JsonObject problemRecord = new JsonObject();
          
          problemRecord.addProperty("source_url", sourceURL);
          problemRecord.add("archiveInfo", archiveInfo);
          
          // emit trouble record ... 
          mos.getCollector(EC2StatsCollectorJob.ARC_TO_BAD_URL_NAMED_OUTPUT, reporter).collect(new Text(arcFileName), new Text(problemRecord.toString()));
        }
      }
      
      crawledCount++;
      
      if (jsonObject.get("disposition").getAsString().equals("SUCCESS")) {
        int httpResult = jsonObject.get("http_result").getAsInt();
        
        // add to crawl record 
        crawlRecord.addProperty("http_result", httpResult);
        crawlRecord.addProperty("content_len",jsonObject.get("content_len").getAsInt());
        
        if (httpResult == 200) 
          http200Count ++;
        else if (httpResult == 403)
          http403Count ++;
        else if (httpResult == 404) 
          http404Count ++;
        else
          httpOtherCount ++;
        
        if (jsonObject.has("redirect_from")) {
          redirectCount ++;
          crawlRecord.addProperty("via_redirect", true);
        }
      }
      else { 
        failedCount++;
      }
      
      mos.getCollector(EC2StatsCollectorJob.CRAWL_RECORD_NAMED_OUTPUT, reporter).collect(new Text(canonicalURL), new Text(crawlRecord.toString()));

	  }
	  
	  JsonObject summaryRecord = new JsonObject();
	  
	  summaryRecord.addProperty("crawledCount",crawledCount);
	  summaryRecord.addProperty("failedCount",failedCount);
	  summaryRecord.addProperty("http200Count",http200Count);
	  summaryRecord.addProperty("http403Count",http403Count);
	  summaryRecord.addProperty("http404Count",http404Count);
	  summaryRecord.addProperty("httpOtherCount",httpOtherCount);
	  summaryRecord.addProperty("redirectCount",redirectCount);
	  
	  output.collect(hostKey, new Text(summaryRecord.toString()));
	}

}
