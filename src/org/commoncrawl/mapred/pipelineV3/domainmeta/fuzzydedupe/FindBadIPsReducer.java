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
import java.util.Iterator;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.util.IPAddressUtils;
import org.commoncrawl.util.TextBytes;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * 
 * @author rana
 *
 */
public class FindBadIPsReducer implements Reducer<TextBytes, TextBytes, TextBytes, TextBytes> {

  JsonParser parser = new JsonParser();

  public static double SPAM_IP_SCORE_CUTOFF = .40;

  @Override
  public void close() throws IOException {

  }

  @Override
  public void configure(JobConf job) {

  }

  @Override
  public void reduce(TextBytes key, Iterator<TextBytes> values, OutputCollector<TextBytes, TextBytes> output,
      Reporter reporter) throws IOException {

    while (values.hasNext()) {
      TextBytes value = values.next();
      JsonObject dupeInfo = parser.parse(value.toString()).getAsJsonObject();

      if (dupeInfo != null) {
        if (dupeInfo.get("spamIPScore").getAsDouble() >= SPAM_IP_SCORE_CUTOFF) {

          if (dupeInfo.get("content-len-geo-mean").getAsDouble() <= 500.0) {

            if (dupeInfo.has("lev-distance-mean") && dupeInfo.get("lev-distance-mean").getAsDouble() >= 15.0) {
              if (dupeInfo.get("uniqueRootDomains").getAsDouble() >= 75.0) {
                if (dupeInfo.has("thisHostDupes")) {
                  // get this host's dupes ...
                  JsonArray thisHostsDupes = dupeInfo.getAsJsonArray("thisHostDupes");

                  // create the samples array
                  JsonArray samples = new JsonArray();
                  for (int i = 0; i < 10; ++i) {
                    if (dupeInfo.has("sample-" + i)) {
                      samples.add(dupeInfo.get("sample-" + i));
                    }
                  }
                  String host = key.toString();
                  // ok walk this host's dupes ...
                  for (JsonElement hostDupe : thisHostsDupes) {
                    JsonObject hostDupeObj = hostDupe.getAsJsonObject();
                    // add samples back in ...
                    hostDupeObj.add("samples", samples);
                    // and host id so we can pass it on later ...
                    hostDupeObj.addProperty("host", host);
                    // ok now output dupe info to this host's ip address...
                    output.collect(new TextBytes(IPAddressUtils.IntegerToIPAddressString(hostDupeObj.get("ip")
                        .getAsInt())), new TextBytes(hostDupeObj.toString()));
                  }
                }
              }
            }
          }
        }
      }
    }
  }

}
