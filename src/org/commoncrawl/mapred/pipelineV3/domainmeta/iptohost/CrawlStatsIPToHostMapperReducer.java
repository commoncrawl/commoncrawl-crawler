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
package org.commoncrawl.mapred.pipelineV3.domainmeta.iptohost;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.FPGenerator;
import org.commoncrawl.util.GoogleURL;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLFPBloomFilter;
import org.commoncrawl.util.URLUtils;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * 
 * @author rana
 *
 */
public class CrawlStatsIPToHostMapperReducer implements Mapper<TextBytes, TextBytes, TextBytes, TextBytes>,
    Reducer<TextBytes, TextBytes, TextBytes, TextBytes> {

  enum Counters {
    GOT_CRAWL_STATUS, GOT_HTTP_RESULT, RESULT_WAS_HTTP_200, GOT_CRAWL_STATS_ARRAY, GOT_CRAWL_STATS_OBJECT,
    SKIPPED_ALREADY_EMITTED_TUPLE, GOT_EXCEPTION_DURING_PARSE
  }

  JsonParser parser = new JsonParser();

  public static final int NUM_HASH_FUNCTIONS = 10;
  public static final int NUM_BITS = 11;
  public static final int NUM_ELEMENTS = 1 << 17;

  static URLFPBloomFilter bloomFilter = new URLFPBloomFilter(NUM_ELEMENTS, NUM_HASH_FUNCTIONS, NUM_BITS);
  URLFPV2 fp = new URLFPV2();

  @Override
  public void close() throws IOException {

  }

  @Override
  public void configure(JobConf job) {
  }

  @Override
  public void map(TextBytes key, TextBytes value, OutputCollector<TextBytes, TextBytes> output, Reporter reporter)
      throws IOException {
    try {
      JsonObject containerObj = parser.parse(value.toString()).getAsJsonObject();
      GoogleURL urlObject = new GoogleURL(key.toString());
      if (urlObject.isValid()) {
        String sourceRootDomain = URLUtils.extractRootDomainName(urlObject.getHost());
        if (sourceRootDomain != null) {

          JsonObject crawlStatus = containerObj.getAsJsonObject("crawl_status");
          if (crawlStatus != null) {
            reporter.incrCounter(Counters.GOT_CRAWL_STATUS, 1);
            if (crawlStatus.has("http_result")) {
              int httpResult = crawlStatus.get("http_result").getAsInt();
              reporter.incrCounter(Counters.GOT_HTTP_RESULT, 1);
              if (httpResult == 200) {
                reporter.incrCounter(Counters.RESULT_WAS_HTTP_200, 1);
                JsonArray crawlStatsArray = crawlStatus.getAsJsonArray("crawl_stats");
                if (crawlStatsArray != null && crawlStatsArray.size() != 0) {
                  reporter.incrCounter(Counters.GOT_CRAWL_STATS_ARRAY, 1);
                  JsonObject crawlStats = crawlStatsArray.get(0).getAsJsonObject();
                  if (crawlStats != null) {
                    reporter.incrCounter(Counters.GOT_CRAWL_STATS_OBJECT, 1);
                    String serverIP = crawlStats.get("server_ip").getAsString();
                    // create a unique string from root domain to ip mapping
                    String uniqueStr = serverIP + "->" + sourceRootDomain;
                    // hack a fingerprint of the string ...
                    fp.setDomainHash(FPGenerator.std64.fp(uniqueStr));
                    fp.setUrlHash(fp.getDomainHash());
                    // and use the bloom filter to skip if already emitted ...
                    if (!bloomFilter.isPresent(fp)) {
                      // output tuple
                      output.collect(new TextBytes(serverIP), new TextBytes(sourceRootDomain));
                      // add hacked fp into BF
                      bloomFilter.add(fp);
                    } else {
                      reporter.incrCounter(Counters.SKIPPED_ALREADY_EMITTED_TUPLE, 1);
                    }
                  }
                }
              }
            }
          }
        }
      }
    } catch (Exception e) {
      reporter.incrCounter(Counters.GOT_EXCEPTION_DURING_PARSE, 1);
    }
  }

  @Override
  public void reduce(TextBytes key, Iterator<TextBytes> values, OutputCollector<TextBytes, TextBytes> output,
      Reporter reporter) throws IOException {
    String serverIP = key.toString();

    while (values.hasNext()) {
      TextBytes value = values.next();
      String hostName = value.toString();
      // create a unique string from root domain to ip mapping
      String uniqueStr = serverIP + "->" + hostName;
      // hack a fingerprint of the string ...
      fp.setDomainHash(FPGenerator.std64.fp(uniqueStr));
      fp.setUrlHash(fp.getDomainHash());
      // and use the bloom filter to skip if already emitted ...
      if (!bloomFilter.isPresent(fp)) {
        // output tuple
        output.collect(key, value);
        // add hacked fp into BF
        bloomFilter.add(fp);
      }
    }
  }

}
