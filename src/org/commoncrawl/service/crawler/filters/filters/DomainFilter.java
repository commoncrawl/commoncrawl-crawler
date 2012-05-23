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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.crawl.filter.DomainFilterData;
import org.commoncrawl.crawl.filter.FilterResults;
import org.commoncrawl.protocol.CrawlURLMetadata;

/**
 * A filter that operates on domain names 
 * 
 * @author rana
 *
 */
public class DomainFilter extends URLPatternBlockFilter {
  
  
  private static final Log LOG = LogFactory.getLog(DomainFilter.class);

  
  private DomainFilterData filterDataObject = new DomainFilterData();
  
  public DomainFilter(int filterType) { 
    filterDataObject.setFilterType((byte)filterType); 
  }
  
  
  public DomainFilter(int filterType,String filterPath,boolean hasMasterFile) { 
    super(filterPath,hasMasterFile);
    filterDataObject.setFilterType((byte)filterType);
  }
  
 
  @Override
  public FilterResult filterItem(String rootDomain,String fullyQualifiedDomain,String urlText, CrawlURLMetadata metadata,FilterResults results) {

  	if (super.filterItem(rootDomain, fullyQualifiedDomain, "*", metadata, results) == FilterResult.Filter_Reject) { 
      if (filterDataObject.getFilterType() == DomainFilterData.Type.Type_ExlusionFilter)
        return FilterResult.Filter_Reject;
      else 
        return FilterResult.Filter_Accept;
  	}
    return FilterResult.Filter_NoAction;
  }

  @Override
  public void loadFilterItem(String filterItemLine) throws IOException {
    // LOG.info("Processing Filter Line:" + filterItemLine);
    int indexOfFirstComma = filterItemLine.indexOf(',');
    if (indexOfFirstComma != -1) {
    	int indexOfNextComma = filterItemLine.indexOf(',',indexOfFirstComma + 1);
    	if (indexOfNextComma != -1) { 
    	
    		String rootDomain = filterItemLine.substring(0,indexOfFirstComma);
    		String subDomainRegExp = "";
    		if (indexOfNextComma - indexOfFirstComma > 1) { 
    			subDomainRegExp = filterItemLine.substring(indexOfFirstComma + 1,indexOfNextComma);
    		}
    		super.loadFilterItem(rootDomain + "," + subDomainRegExp + ",.*");
    	}
    }
  }

}
