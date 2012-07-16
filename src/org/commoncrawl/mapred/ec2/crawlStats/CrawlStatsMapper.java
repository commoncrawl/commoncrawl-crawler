package org.commoncrawl.mapred.ec2.crawlStats;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.util.GoogleURL;
import org.commoncrawl.util.URLUtils;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class CrawlStatsMapper implements Mapper<Text,Text,Text,Text> {

	JsonParser parser = new JsonParser();

	enum Counters { 
		BAD_URL,
		UNHANDLED_EXCEPTION, FAILED_TO_CANONICALIZE_URL
	}
	@Override
	public void configure(JobConf job) {
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public void map(Text key, Text jsonText,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		String strKey = key.toString();
		String strValue = jsonText.toString();
		
		GoogleURL urlObject = new GoogleURL(strKey);
		if (urlObject.isValid()) {
		  
		  String host = urlObject.getHost();
		  if (host.startsWith("www.")) { 
		    host = host.substring(4);
		  }
		  if (host.length() != 0) {
		    
		    String canoncialURL = URLUtils.canonicalizeURL(urlObject, true);
		    if (canoncialURL == null) { 
		      reporter.incrCounter(Counters.FAILED_TO_CANONICALIZE_URL, 1);
		    }
		    else { 
    	    try { 
    	    	JsonObject jsonObject = parser.parse(strValue).getAsJsonObject();
    	    	
    	    	jsonObject.addProperty("source_url", strKey);
    	    	jsonObject.addProperty("canonical_url", canoncialURL);
    	    	
    	    	output.collect(new Text(host),new Text(jsonObject.toString()));
    	    }
    	    catch (Exception e) {
    	    	reporter.incrCounter(Counters.UNHANDLED_EXCEPTION, 1);
    	    }
		    }
		  }
		}
		else { 
			reporter.incrCounter(Counters.BAD_URL, 1);
		}
	}
}
