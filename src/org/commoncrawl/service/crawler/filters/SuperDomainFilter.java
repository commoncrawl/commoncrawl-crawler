package org.commoncrawl.service.crawler.filters;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.net.FingerClient;
import org.commoncrawl.protocol.CrawlURLMetadata;
import org.commoncrawl.service.crawler.filter.FilterResults;
import org.commoncrawl.util.FPGenerator;
import org.commoncrawl.util.URLFingerprint;
import org.commoncrawl.util.URLUtils;

public class SuperDomainFilter extends Filter{
	
	
	Set<Integer> 	_validFingerprints = new TreeSet<Integer>();
	Set<String> 	_validNames = new TreeSet<String>();
	Set<Long>			_validV2Fingerprints = new TreeSet<Long>();
	
	
	/**
	 * default constructor where filter is loaded directly from a file
	 */
	public SuperDomainFilter() { 
		
	}
	
	public SuperDomainFilter(String filterFile) { 
		super(filterFile,false);
	}
	
	@Override
  public void clear() {
		_validFingerprints.clear();
		_validV2Fingerprints.clear();
  }

	
	@Override
	public void loadFilterItem(String filterItemLine) throws IOException {
		String domainName = URLUtils.normalizeHostName(filterItemLine, false);
		if (domainName != null) { 
			_validNames.add(domainName);
			_validFingerprints.add(URLFingerprint.generate32BitHostFP(domainName));
			_validV2Fingerprints.add(FPGenerator.std64.fp(domainName));
		}
	}
	
	public FilterResult filterItemByHashId(int hashId) { 
		return _validFingerprints.contains(hashId) ? FilterResult.Filter_Accept : FilterResult.Filter_NoAction;
	}
	public FilterResult filterItemByHashIdV2(long hashId) { 
		return _validV2Fingerprints.contains(hashId) ? FilterResult.Filter_Accept : FilterResult.Filter_NoAction;
	}
	
	@Override
  public FilterResult filterItem(String rootDomain, String domainName,String urlPath, CrawlURLMetadata metadata, FilterResults results) {
		if (_validNames.contains(rootDomain)) { 
			return FilterResult.Filter_Accept;
		}
		return FilterResult.Filter_NoAction;
			
	}

}
