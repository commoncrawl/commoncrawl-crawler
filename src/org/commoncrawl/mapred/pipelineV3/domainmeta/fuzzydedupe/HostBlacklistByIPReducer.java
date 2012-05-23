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
package org.commoncrawl.mapred.pipelineV3.domainmeta.fuzzydedupe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.FPGenerator;
import org.commoncrawl.util.JoinByTextSortByTagMapper;
import org.commoncrawl.util.JoinValue;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLFPBloomFilter;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * 
 * @author rana
 *
 */
public class HostBlacklistByIPReducer implements Reducer<TextBytes, JoinValue, TextBytes, TextBytes> {

  enum Counters {
    HOST_WAS_FLAGGED_AS_QUANTCAST_HOST, HOST_WAS_FLAGGED_AS_BAD_IP_HOST, HOST_WAS_FLAGGED_AS_BLACKLISTED_AND_QUANTCAST,
    GOT_BLACKLIST_TAG, GOT_QUANTCAST_TAG, GOT_IP_TO_HOST_MAPPING, ENTIRE_HOST_FLAGGED_BAD, INDIVIDUAL_DOMAINS_BAD
  }

  private static final Log LOG = LogFactory.getLog(HostBlacklistByIPReducer.class);
  TextBytes realKey = new TextBytes();

  TextBytes tagKey = new TextBytes();
  String activeIPAddress = null;
  String blackListIPHost;
  String blackListReason;

  boolean flaggedAsQuantcastHost;
  int badHostCount = 0;
  int totalHostCount = 0;

  int dupeHitsCount = 0;

  // risk .. but expedient ...
  ArrayList<JsonObject> blackListedItems = new ArrayList<JsonObject>();

  public static final int NUM_HASH_FUNCTIONS = 10;
  public static final int NUM_BITS = 11;
  public static final int NUM_ELEMENTS = 1 << 25;
  private URLFPBloomFilter badHostFilter = new URLFPBloomFilter(NUM_ELEMENTS, NUM_HASH_FUNCTIONS, NUM_BITS);

  JsonParser parser = new JsonParser();
  URLFPV2 fp = new URLFPV2();

  double FAILURE_THRESHOLD = .75;

  long bloomKeyCount = 0;
  static final long BLOOM_FILTER_FLUSH_THRESHOLD = 1 << 21;

  @Override
  public void close() throws IOException {

  }

  @Override
  public void configure(JobConf job) {

  }

  @Override
  public void reduce(TextBytes key, Iterator<JoinValue> values, OutputCollector<TextBytes, TextBytes> output,
      Reporter reporter) throws IOException {

    JoinByTextSortByTagMapper.getKeyFromCompositeKey(key, realKey);
    JoinByTextSortByTagMapper.getTagFromCompositeKey(key, tagKey);

    // get ip address and tag ...
    String ipAddress = realKey.toString();
    String tag = tagKey.toString();
    // ok if ip-address transition ...
    LOG.info("Got Key:" + ipAddress.toString() + ":" + tag.toString());
    if (activeIPAddress == null || !activeIPAddress.equals(ipAddress)) {
      // LOG.info("Reset State");
      blackListedItems.clear();
      flaggedAsQuantcastHost = false;
      badHostCount = 0;
      totalHostCount = 0;
      // set active ip address ...
      activeIPAddress = ipAddress;
      dupeHitsCount = 0;
    }

    if (tag.equals(HostBlacklistByDupesStep.TAG_BAD_IP_MAPPING)) {
      dupeHitsCount++;
      String value = values.next().getTextValue().toString();
      // read the object
      JsonObject blackListInfo = parser.parse(value).getAsJsonObject();
      // set up a 'fake' fingerprint ...
      fp.setDomainHash(FPGenerator.std64.fp(blackListInfo.get("host").getAsString()));
      fp.setUrlHash(fp.getDomainHash());

      if (!badHostFilter.isPresent(fp)) {
        // set it in bad host bloom filter ...
        badHostFilter.add(fp);
        bloomKeyCount++;
        // queue up the object for later ...
        blackListedItems.add(blackListInfo);
        reporter.incrCounter(Counters.GOT_BLACKLIST_TAG, 1);
      }
    } else if (tag.equals(HostBlacklistByDupesStep.TAG_QUANTCAST_MAPPING)) {
      reporter.incrCounter(Counters.GOT_QUANTCAST_TAG, 1);
      flaggedAsQuantcastHost = true;
    } else {
      reporter.incrCounter(Counters.GOT_IP_TO_HOST_MAPPING, 1);

      if (dupeHitsCount != 0) {
        ArrayList<String> entireHostList = new ArrayList<String>();
        // ok walk hosts ...
        while (values.hasNext()) {

          String host = values.next().getTextValue().toString();
          // add host to list
          entireHostList.add(host);

          // set up a 'fake' fingerprint ...
          fp.setDomainHash(FPGenerator.std64.fp(host));
          fp.setUrlHash(fp.getDomainHash());

          // increment total host count ...
          totalHostCount++;
          if (badHostFilter.isPresent(fp)) {
            badHostCount++;
          }
        }
        double pctBad = (double) badHostCount / (double) totalHostCount;

        JsonObject objectOut = new JsonObject();

        if (pctBad >= FAILURE_THRESHOLD) {
          objectOut.addProperty("level", "ip");
        } else {
          objectOut.addProperty("level", "host");
        }
        JsonArray domains = new JsonArray();
        JsonArray sampleArray = null;

        objectOut.addProperty("level", "host");
        objectOut.addProperty("badHostCount", badHostCount);
        objectOut.addProperty("totalHostCount", totalHostCount);
        objectOut.add("domains", domains);

        for (JsonObject badHost : blackListedItems) {
          // add host to array
          domains.add(badHost.get("host"));
          if (sampleArray == null) {
            sampleArray = badHost.getAsJsonArray("samples");
            if (sampleArray != null && sampleArray.size() == 0) {
              sampleArray = null;
            }
          }
        }
        if (sampleArray != null) {
          objectOut.add("samples", sampleArray);
        }
        output.collect(new TextBytes(ipAddress), new TextBytes(objectOut.toString()));
      }
      if (bloomKeyCount >= BLOOM_FILTER_FLUSH_THRESHOLD) {
        badHostFilter.clear();
      }
    }
  }
}
