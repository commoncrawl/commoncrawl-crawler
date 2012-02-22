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

import java.io.IOException;

import org.commoncrawl.crawl.filter.FilterResults;
import org.commoncrawl.io.internal.NIODNSCache;
import org.commoncrawl.protocol.CrawlURLMetadata;
import org.commoncrawl.util.internal.URLUtils;

/**
 * An IP based filter
 * @author rana
 *
 */
public class BigDomainListFilter extends Filter {

	NIODNSCache _cache = new NIODNSCache();
	
	
	public BigDomainListFilter(String filterFile,boolean hasMasterFile) { 
		super(filterFile,hasMasterFile);
	}
	
	@Override
  public void clear() {
		_cache = new NIODNSCache();	  
  }

	@Override
	public void loadFilterItem(String filterItemLine) throws IOException {
		if (URLUtils.isValidDomainName(filterItemLine)) { 
      _cache.cacheIPAddressForHost(filterItemLine,1,Long.MAX_VALUE,null).markAsSuperNode();
		}
	}
	
	
	@Override
  public FilterResult filterItem(String rootDomain, String domainName,String urlPath, CrawlURLMetadata metadata, FilterResults results) {
    NIODNSCache.Node node = _cache.findNode(rootDomain);
    // if we found a node then this domain, then truncate to the specified super domain name ...
    if (node != null && node.isSuperNode()) { 
      return FilterResult.Filter_Accept;
    }
    return FilterResult.Filter_NoAction;
  }

}
