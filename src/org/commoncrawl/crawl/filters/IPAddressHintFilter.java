/**
 * Copyright 2008 - CommonCrawl Foundation
 * 
 * CommonCrawl licenses this file to you under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.commoncrawl.crawl.filters;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.crawl.filter.FilterResults;
import org.commoncrawl.io.internal.NIODNSCache;
import org.commoncrawl.protocol.CrawlURLMetadata;
import org.commoncrawl.util.internal.URLUtils;
import org.commoncrawl.util.shared.CCStringUtils;
import org.commoncrawl.util.shared.IPAddressUtils;
import org.junit.Test;

/** 
 * A filter that stirs in an IP Address hint for an unresolved domain name
 * @author rana
 *
 */
public class IPAddressHintFilter extends Filter {

  private static final Log LOG = LogFactory.getLog(IPAddressHintFilter.class);
  private NIODNSCache cache = new NIODNSCache();

  public IPAddressHintFilter() { 
    
  }
  
  public IPAddressHintFilter(String filterPath,boolean hasMasterFile) { 
    super(filterPath,hasMasterFile);
  }
  
  @Override
  public FilterResult filterItem(String rootDomainName,String fullyQualifiedDomainName, String urlPath,CrawlURLMetadata metadata, FilterResults results) {
    String normalizedName = URLUtils.normalizeHostName(fullyQualifiedDomainName,true);
    
    if (normalizedName != null) { 
	    NIODNSCache.Node node = cache.findNode(normalizedName);
	    
	    // if we found a node then this domain, then truncate to the specified super domain name ...
	    if (node != null && node.isSuperNode()) { 
	      results.setIpAddressHint(node.getIPAddress());
	      return FilterResult.Filter_Modified;
	    }
    }
    return FilterResult.Filter_NoAction;    
  }
  
  @Override
  public void loadFilterItem(String filterItemLine) throws IOException {
    if (filterItemLine.length() == 0) { 
      LOG.error("filterItemLine is zero length");
    }
    else { 
      String items[] = filterItemLine.split("\t");
      if (items.length == 2) {
        
        String domainName = items[0];
        
        if (domainName.charAt(0) == '.')
          domainName = domainName.substring(1);
    
        domainName = URLUtils.normalizeHostName(domainName,true);
        
        try { 
          int ipAddress = IPAddressUtils.IPV4AddressToInteger(InetAddress.getByName(items[1]).getAddress());
          cache.cacheIPAddressForHost(filterItemLine,ipAddress,Long.MAX_VALUE,null).markAsSuperNode();
        }
        catch (Exception e) { 
          LOG.error(CCStringUtils.stringifyException(e));
        }
      }
    }
  }
  
  @Override
  public void clear() {
    cache = new NIODNSCache();
  };
  
  @Test
  public void testname() throws Exception {
    cache.cacheIPAddressForHost("cnn.com", 1, Long.MAX_VALUE,null).markAsSuperNode();
    cache.cacheIPAddressForHost("netscape.cnn.com",2, Long.MAX_VALUE,null).markAsSuperNode();
    
    NIODNSCache.Node node1 = cache.findNode("netscape.cnn.com");
    NIODNSCache.Node node2 = cache.findNode("www.cnn.com");
    NIODNSCache.Node node3 = cache.findNode("cnn.com");
    NIODNSCache.Node node4 = cache.findNode("ccnn.com");
    
    assertTrue(node1 != null);
    assertTrue(node2 != null);
    assertTrue(node3 != null);
    assertTrue(node4 == null);
    assertTrue(node1.isSuperNode());
    assertTrue(node2.isSuperNode());
    assertTrue(node3.isSuperNode());
    assertTrue(node1.getIPAddress() == 2);
    assertTrue(node2.getIPAddress() == 1);
    assertTrue(node3.getIPAddress() == 1);
  }
}
