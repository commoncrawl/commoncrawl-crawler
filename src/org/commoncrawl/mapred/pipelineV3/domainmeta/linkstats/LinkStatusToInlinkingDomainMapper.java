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

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.util.GoogleURL;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLUtils;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * 
 * @author rana
 *
 */
public class LinkStatusToInlinkingDomainMapper implements Mapper<TextBytes, TextBytes, TextBytes, TextBytes> {

  JsonParser parser = new JsonParser();

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
      JsonObject o = parser.parse(value.toString()).getAsJsonObject();
      JsonObject linkStatus = o.getAsJsonObject("link_status");
      if (linkStatus != null) {
        GoogleURL urlObject = new GoogleURL(key.toString());
        if (urlObject.isValid()) {
          String sourceRootDomain = URLUtils.extractRootDomainName(urlObject.getHost());
          if (sourceRootDomain != null) {
            JsonArray sources = linkStatus.getAsJsonArray("sources");
            for (JsonElement source : sources) {
              urlObject = new GoogleURL(source.getAsString());
              if (urlObject.isValid()) {
                String rootDomain = URLUtils.extractRootDomainName(urlObject.getHost());
                if (rootDomain != null) {
                  output.collect(new TextBytes(sourceRootDomain), new TextBytes(rootDomain));
                }
              }
            }
          }
        }
      }
    } catch (Exception e) {

    }

  }

}
