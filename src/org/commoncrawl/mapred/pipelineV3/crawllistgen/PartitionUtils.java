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

package org.commoncrawl.mapred.pipelineV3.crawllistgen;

import java.util.Set;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.commoncrawl.util.ByteArrayUtils;
import org.commoncrawl.util.FlexBuffer;
import org.commoncrawl.util.GoogleURL;
import org.commoncrawl.util.SuperDomainList;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLUtils;

import com.google.common.collect.Sets;

/**
 * 
 * @author rana
 *
 */
public class PartitionUtils {

  public static class PartitionKeyPartitioner implements Partitioner<TextBytes, Writable> {

    FlexBuffer scratchBuffer = new FlexBuffer();

    @Override
    public void configure(JobConf job) {

    }

    @Override
    public int getPartition(TextBytes key, Writable value, int numPartitions) {
      return getPartitionGivenPartitionKey(key, scratchBuffer, numPartitions);
    }

  }

  static byte pattern[] = { ':' };

  public static void generatePartitionKeyGivenDomain(Set<Long> superDomainIdList, String rootDomain, int type,
      TextBytes partitionKeyOut) {
    partitionKeyOut.set(rootDomain + ":" + Integer.toString(type) + ":");
  }

  public static boolean generatePartitionKeyGivenURL(Set<Long> superDomainIdList, GoogleURL urlObject, int type,
      TextBytes partitionKeyOut) {
    String domain = urlObject.getHost();
    String rootDomain = URLUtils.extractRootDomainName(domain);
    if (rootDomain != null) {
      long domainFP = SuperDomainList.domainFingerprintGivenName(rootDomain);
      if (!superDomainIdList.contains(domainFP)) {
        domain = rootDomain;
      }
      partitionKeyOut.set(domain + ":" + Integer.toString(type) + ":" + urlObject.getCanonicalURL());
      return true;
    }
    return false;
  }

  public static boolean generatePartitionKeyGivenURL(Set<Long> superDomainIdList, TextBytes urlKey, int type,
      TextBytes partitionKeyOut) {
    GoogleURL urlObject = new GoogleURL(urlKey.toString());
    if (urlObject.isValid()) {
      return generatePartitionKeyGivenURL(superDomainIdList, urlObject, type, partitionKeyOut);
    }
    return false;
  }

  public static TextBytes getDomainGivenPartitionKey(TextBytes partitionKey, TextBytes domainOut) {
    int index = ByteArrayUtils.indexOf(partitionKey.getBytes(), partitionKey.getOffset(), partitionKey.getLength(),
        pattern);
    domainOut.set(partitionKey.getBytes(), partitionKey.getOffset(), index - partitionKey.getOffset());
    return domainOut;
  }

  public static int getPartitionGivenPartitionKey(TextBytes partitionKey, FlexBuffer scratchBuffer, int numParitions) {
    int index = ByteArrayUtils.indexOf(partitionKey.getBytes(), partitionKey.getOffset(), partitionKey.getLength(),
        pattern);
    scratchBuffer.set(partitionKey.getBytes(), partitionKey.getOffset(), index - partitionKey.getOffset());
    return (scratchBuffer.hashCode() & Integer.MAX_VALUE) % numParitions;
  }

  public static int getTypeGivenPartitionKey(TextBytes partitionKey) {
    byte bytes[] = partitionKey.getBytes();
    int offset = partitionKey.getOffset();
    int index = ByteArrayUtils.indexOf(partitionKey.getBytes(), partitionKey.getOffset(), partitionKey.getLength(),
        pattern);
    int startIndex = ++index;
    while (bytes[index + offset] != ':')
      ++index;
    return (int) ByteArrayUtils.parseLong(bytes, offset + startIndex, index - startIndex, 10);
  }

  public static void getURLGivenPartitionKey(TextBytes partitionKey, TextBytes urlOut) {
    byte bytes[] = partitionKey.getBytes();
    int offset = partitionKey.getOffset();
    int index = ByteArrayUtils.indexOf(partitionKey.getBytes(), partitionKey.getOffset(), partitionKey.getLength(),
        pattern);
    ++index;
    while (bytes[index + offset] != ':')
      ++index;

    if (index + 1 < partitionKey.getLength()) {
      urlOut.set(partitionKey.getBytes(), partitionKey.getOffset() + index + 1, partitionKey.getLength() - (index + 1));
    } else {
      urlOut.clear();
    }
  }

  public static void main(String[] args) {

    TextBytes partitionKeyOut = new TextBytes();
    Set<Long> emptySet = Sets.newHashSet();
    FlexBuffer scratchBuffer = new FlexBuffer();
    TextBytes urlOut = new TextBytes();
    TextBytes domainBytes = new TextBytes();

    generatePartitionKeyGivenURL(emptySet, new TextBytes("http://www.google.com/someurl"), 0, partitionKeyOut);
    System.out.println("ParitiionKey:" + partitionKeyOut.toString());
    System.out.println("Parition:" + getPartitionGivenPartitionKey(partitionKeyOut, scratchBuffer, 10));
    System.out.println("Domain:" + getDomainGivenPartitionKey(partitionKeyOut, domainBytes));
    System.out.println("Type:" + getTypeGivenPartitionKey(partitionKeyOut));
    getURLGivenPartitionKey(partitionKeyOut, urlOut);
    System.out.println("URL:" + urlOut.toString());

    generatePartitionKeyGivenDomain(emptySet, "google.com", 0, partitionKeyOut);
    System.out.println("ParitiionKey:" + partitionKeyOut.toString());
    System.out.println("Parition:" + getPartitionGivenPartitionKey(partitionKeyOut, scratchBuffer, 10));
    System.out.println("Domain:" + getDomainGivenPartitionKey(partitionKeyOut, domainBytes));
    System.out.println("Type:" + getTypeGivenPartitionKey(partitionKeyOut));
    getURLGivenPartitionKey(partitionKeyOut, urlOut);
    System.out.println("URL:" + urlOut.toString());

  }
}
