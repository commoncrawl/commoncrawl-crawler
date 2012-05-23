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
package org.commoncrawl.mapred.pipelineV3.domainmeta.linkstats;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.util.TextBytes;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

/**
 * 
 * @author rana
 *
 */
public class UniqueIncomingRootDomainCounter implements Reducer<TextBytes, TextBytes, TextBytes, TextBytes> {

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

    HashSet<String> rootDomainList = new HashSet<String>();

    Iterators.addAll(rootDomainList, Iterators.transform(values, new Function<TextBytes, String>() {

      @Override
      public String apply(TextBytes arg0) {
        return arg0.toString();
      }

    }));

    JsonObject jsonObject = new JsonObject();
    JsonArray jsonArray = new JsonArray();

    jsonObject.addProperty("domain-count", rootDomainList.size());

    for (String domain : rootDomainList) {
      jsonArray.add(new JsonPrimitive(domain));
    }
    jsonObject.add("domains", jsonArray);

    if (rootDomainList.size() != 0) {
      output.collect(key, new TextBytes(jsonObject.toString()));
    }
  }

}
