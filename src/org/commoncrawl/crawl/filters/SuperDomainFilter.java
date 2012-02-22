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
import java.util.Set;
import java.util.TreeSet;

import org.commoncrawl.crawl.filter.FilterResults;
import org.commoncrawl.protocol.CrawlURLMetadata;
import org.commoncrawl.util.internal.URLFingerprint;
import org.commoncrawl.util.internal.URLUtils;
import org.commoncrawl.util.shared.FPGenerator;

/** 
 * A filter that identifies super-domains given a host name 
 * 
 * @author rana
 *
 */
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
