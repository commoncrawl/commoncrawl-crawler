/**
 * Copyright 2008 - CommonCrawl Foundation
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

package org.commoncrawl.service.dns;

import java.io.IOException;
import java.util.regex.PatternSyntaxException;

import org.commoncrawl.protocol.CrawlURLMetadata;
import org.commoncrawl.service.crawler.filters.FilterResults;
import org.commoncrawl.service.crawler.filters.URLPatternBlockFilter;
import org.commoncrawl.util.CCStringUtils;

/**
 * 
 * @author rana
 *
 */
public class DNSNoCacheFilter extends URLPatternBlockFilter  {
  
	public DNSNoCacheFilter() { 
		
	}
	
	public DNSNoCacheFilter(String filterPath) { 
		super(filterPath,false);
	}
	
  @Override
  public void loadFilterItem(String filterItemLine) throws java.io.IOException {
  	String items[] = filterItemLine.split(",");
  	if (items.length == 2) { 
	    try { 
	      super.loadFilterItem(filterItemLine + ",.*");
	    }
	    catch (PatternSyntaxException e) { 
	      throw new IOException("Pattern syntax exception parsing line:" + filterItemLine + "\nException:" + CCStringUtils.stringifyException(e));
	    }
  	}
  }

  @Override
  public FilterResult filterItem(String rootDomainName,String fullyQualifiedDomainName, String urlPath,CrawlURLMetadata metadata, FilterResults results) {
  	return super.filterItem(rootDomainName, fullyQualifiedDomainName, "*", metadata, results); 
  }
}
