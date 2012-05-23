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
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.mapred.pipelineV3.domainmeta.quantcast.ImportQuantcastStep;
import org.commoncrawl.util.JoinValue;
import org.commoncrawl.util.TextBytes;

import com.google.gson.JsonObject;

/**
 * 
 * @author rana
 *
 */
public class QuantcastWhitelistByIPReducer implements Reducer<TextBytes, JoinValue, TextBytes, TextBytes> {

  Set<String> ipAddresses = new TreeSet<String>();

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void configure(JobConf job) {
    // TODO Auto-generated method stub

  }

  @Override
  public void reduce(TextBytes key, Iterator<JoinValue> values, OutputCollector<TextBytes, TextBytes> output,
      Reporter reporter) throws IOException {
    JsonObject jsonObject = new JsonObject();

    int quantcastRank = -1;
    ipAddresses.clear();
    while (values.hasNext()) {
      JoinValue value = values.next();
      if (value.getTag().toString().equals(ImportQuantcastStep.OUTPUT_DIR_NAME)) {
        quantcastRank = (int) value.getLongValue();
      } else {
        ipAddresses.add(value.getTextValue().toString());
      }
    }

    // ok if ip address list is not empty ...
    if (ipAddresses.size() != 0 && quantcastRank != -1) {
      jsonObject.addProperty("name", key.toString());
      jsonObject.addProperty("rank", quantcastRank);

      TextBytes jsonObjectText = new TextBytes(jsonObject.toString());

      for (String ipAddress : ipAddresses) {
        output.collect(new TextBytes(ipAddress), jsonObjectText);
      }
    }
  }

}
