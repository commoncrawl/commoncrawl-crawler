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
import java.util.regex.Pattern;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.GoogleURL;
import org.commoncrawl.util.StringUtils;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLFPBloomFilter;
import org.commoncrawl.util.URLUtils;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

/**
 * 
 * @author rana
 *
 */
public class CrossDomainDupesReducer implements Reducer<TextBytes, TextBytes, TextBytes, TextBytes> {

  enum Counters {

  }

  JsonParser parser = new JsonParser();
  public static final int NUM_HASH_FUNCTIONS = 10;
  public static final int NUM_BITS = 11;
  public static final int NUM_ELEMENTS = 1 << 18;
  private URLFPBloomFilter filter = new URLFPBloomFilter(NUM_ELEMENTS, NUM_HASH_FUNCTIONS, NUM_BITS);

  String samples[] = new String[20];

  Pattern knownValidDupesPatterns = Pattern.compile("/(wp-includes|xmlrpc.php)");

  @Override
  public void close() throws IOException {

  }

  @Override
  public void configure(JobConf job) {

  }

  public double otherDomainToLocalDomainScore(double totalHits, double otherDomainHits) {
    return Math.log(otherDomainHits) / Math.log(totalHits);
  }

  @Override
  public void reduce(TextBytes key, Iterator<TextBytes> values, OutputCollector<TextBytes, TextBytes> output,
      Reporter reporter) throws IOException {

    filter.clear();
    double crossDomainDupesCount = 0;
    double totalHitsCount = 0;
    double uniqueRootDomainsCount = 0;
    double uniqueIPs = 0;
    double validDupePatternMatches = 0;

    URLFPV2 rootFP = URLUtils.getURLFPV2FromHost(key.toString());
    URLFPV2 fp = new URLFPV2();
    int sampleCount = 0;
    ArrayList<Integer> ipAddresses = new ArrayList<Integer>();
    JsonArray thisHostsDupes = new JsonArray();
    DescriptiveStatistics lengthStats = new DescriptiveStatistics();

    while (values.hasNext()) {
      JsonArray jsonArray = parser.parse(values.next().toString()).getAsJsonArray();
      for (JsonElement elem : jsonArray) {
        totalHitsCount++;
        fp.setRootDomainHash(elem.getAsJsonObject().get("dh").getAsLong());
        if (fp.getRootDomainHash() != rootFP.getRootDomainHash()) {
          crossDomainDupesCount++;
          fp.setDomainHash(fp.getRootDomainHash());
          fp.setUrlHash(fp.getRootDomainHash());
          // track length average ....
          lengthStats.addValue(elem.getAsJsonObject().get("length").getAsInt());

          if (!filter.isPresent(fp)) {
            uniqueRootDomainsCount++;
            filter.add(fp);
            if (sampleCount < samples.length) {
              String url = elem.getAsJsonObject().get("url").getAsString();
              GoogleURL urlObject = new GoogleURL(url);
              if (knownValidDupesPatterns.matcher(urlObject.getCanonicalURL()).find()) {
                validDupePatternMatches++;
              }
              samples[sampleCount++] = url;
            }
          }
        } else {
          thisHostsDupes.add(elem);
        }

        int ipAddress = elem.getAsJsonObject().get("ip").getAsInt();

        fp.setRootDomainHash(ipAddress);
        fp.setDomainHash(ipAddress);
        fp.setUrlHash(ipAddress);

        if (!filter.isPresent(fp)) {
          uniqueIPs++;
          filter.add(fp);
          ipAddresses.add(ipAddress);
        }
      }
    }

    if (totalHitsCount > 15 && crossDomainDupesCount >= 2) {

      double otherDomainToLocalScore = otherDomainToLocalDomainScore(totalHitsCount, crossDomainDupesCount);
      double spamIPScore = spamHostScore(totalHitsCount, crossDomainDupesCount, uniqueIPs);

      if (otherDomainToLocalScore >= .50 || spamIPScore > .50) {
        JsonObject objectOut = new JsonObject();

        objectOut.addProperty("ratio", (crossDomainDupesCount / totalHitsCount));
        objectOut.addProperty("totalHits", totalHitsCount);
        objectOut.addProperty("crossDomainDupes", crossDomainDupesCount);
        objectOut.addProperty("uniqueRootDomains", uniqueRootDomainsCount);
        objectOut.addProperty("otherDomainToLocalScore", otherDomainToLocalScore);
        objectOut.addProperty("spamIPScore", spamIPScore);
        objectOut.addProperty("validDupeMatches", validDupePatternMatches);
        objectOut.addProperty("content-len-mean", lengthStats.getMean());
        objectOut.addProperty("content-len-geo-mean", lengthStats.getGeometricMean());

        for (int i = 0; i < sampleCount; ++i) {
          objectOut.addProperty("sample-" + i, samples[i]);
        }
        // compute path edit distance ...
        if (sampleCount > 1) {
          int sampleEditDistanceSize = Math.min(sampleCount, 5);
          DescriptiveStatistics stats = new DescriptiveStatistics();
          for (int j = 0; j < sampleEditDistanceSize; ++j) {
            for (int k = 0; k < sampleEditDistanceSize; ++k) {
              if (k != j) {
                GoogleURL urlObjectA = new GoogleURL(samples[j]);
                GoogleURL urlObjectB = new GoogleURL(samples[k]);

                if (urlObjectA.getPath().length() < 100 && urlObjectB.getPath().length() < 100) {
                  stats.addValue(StringUtils.getLevenshteinDistance(urlObjectA.getPath(), urlObjectB.getPath()));
                }
              }
            }
          }
          if (stats.getMean() != 0.0) {
            objectOut.addProperty("lev-distance-mean", stats.getMean());
            objectOut.addProperty("lev-distance-geomean", stats.getGeometricMean());
          }
        }

        JsonArray ipAddressArray = new JsonArray();
        for (int j = 0; j < Math.min(1000, ipAddresses.size()); ++j) {
          ipAddressArray.add(new JsonPrimitive(ipAddresses.get(j)));
        }
        if (ipAddresses.size() != 0) {
          objectOut.add("ipList", ipAddressArray);
        }
        objectOut.add("thisHostDupes", thisHostsDupes);

        output.collect(key, new TextBytes(objectOut.toString()));
      }
    }

  }

  public double spamHostScore(double totalHits, double otherDomainHits, double uniqueIPAddresses) {
    double uniqueIPScore = 1 - (Math.log(uniqueIPAddresses) / Math.log(totalHits));
    return (Math.log(Math.max(otherDomainHits, 2)) / Math.log(Math.max(totalHits, 2))) * uniqueIPScore;
  }

}
