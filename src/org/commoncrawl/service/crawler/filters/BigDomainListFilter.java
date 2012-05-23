package org.commoncrawl.service.crawler.filters;

import java.io.IOException;

import org.commoncrawl.io.NIODNSCache;
import org.commoncrawl.protocol.CrawlURLMetadata;
import org.commoncrawl.service.crawler.filter.FilterResults;
import org.commoncrawl.util.URLUtils;

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
