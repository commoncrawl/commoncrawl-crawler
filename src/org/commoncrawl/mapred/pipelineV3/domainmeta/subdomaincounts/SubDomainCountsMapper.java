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
package org.commoncrawl.mapred.pipelineV3.domainmeta.subdomaincounts;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.util.GoogleURL;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLUtils;

/**
 * 
 * @author rana
 *
 */
public class SubDomainCountsMapper implements Mapper<TextBytes, TextBytes, TextBytes, TextBytes> {

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void configure(JobConf job) {
    // TODO Auto-generated method stub

  }

  @Override
  public void map(TextBytes key, TextBytes value, OutputCollector<TextBytes, TextBytes> output, Reporter reporter)
      throws IOException {

    GoogleURL urlObject = new GoogleURL(key.toString());
    String rootDomain = URLUtils.extractRootDomainName(urlObject.getHost());
    String host = urlObject.getHost();
    if (rootDomain != null) {
      String subDomain = rootDomain;
      if (!host.equals(rootDomain)) {
        subDomain = host.substring(0, host.length() - rootDomain.length());
      }
      output.collect(new TextBytes(rootDomain), new TextBytes(subDomain));
    }
  }

}
