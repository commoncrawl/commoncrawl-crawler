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

package org.commoncrawl.mapred.pipelineV3.domainmeta.crawlstats;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLFPBloomFilter;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * 
 * @author rana
 *
 */
public class StatsAggregationReducer implements Reducer<TextBytes, TextBytes, TextBytes, TextBytes> {

  JsonParser parser = new JsonParser();

  static final int NUM_HASH_FUNCTIONS = 10;
  static final int NUM_BITS = 11;
  static final int NUM_ELEMENTS = 1 << 28;
  static final int FLUSH_THRESHOLD = 1 << 23;

  URLFPBloomFilter subDomainFilter = new URLFPBloomFilter(NUM_ELEMENTS, NUM_HASH_FUNCTIONS, NUM_BITS);
  long bloomFilterEntryCount = 0;
  URLFPV2 bloomKey = new URLFPV2();
  JsonObject objectOut = new JsonObject();

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void configure(JobConf job) {
    // TODO Auto-generated method stub

  }

  @Override
  public void reduce(TextBytes key, Iterator<TextBytes> values, OutputCollector<TextBytes, TextBytes> output,
      Reporter reporter) throws IOException {

    int urlCount = 0;
    int crawledCount = 0;
    int http200Count = 0;
    int http403Count = 0;
    int http404Count = 0;
    int subDomainCount = 0;
    int recentlyDiscoveredCount = 0;

    while (values.hasNext()) {
      // increment url count ...
      urlCount++;

      JsonObject jsonObject = parser.parse(values.next().toString()).getAsJsonObject();

      if (jsonObject.has("dh")) {

        long domainHash = jsonObject.get("dh").getAsLong();

        bloomKey.setDomainHash(domainHash);
        bloomKey.setUrlHash(domainHash);

        if (!subDomainFilter.isPresent(bloomKey)) {
          subDomainFilter.add(bloomKey);
          subDomainCount++;
        }
      }

      if (jsonObject.has("crawled")) {
        crawledCount++;
      }
      if (jsonObject.has("200"))
        http200Count++;
      else if (jsonObject.has("403"))
        http403Count++;
      else if (jsonObject.has("404"))
        http404Count++;

      if (jsonObject.has("recentlyDiscovered"))
        recentlyDiscoveredCount++;
    }

    objectOut.addProperty("urls", urlCount);
    objectOut.addProperty("crawled", crawledCount);
    objectOut.addProperty("200", http200Count);
    objectOut.addProperty("403", http403Count);
    objectOut.addProperty("404", http404Count);
    objectOut.addProperty("recent", recentlyDiscoveredCount);

    output.collect(key, new TextBytes(objectOut.toString()));

  }

}
