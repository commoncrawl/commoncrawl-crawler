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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Pattern;

import org.commoncrawl.crawl.filter.FilterResults;
import org.commoncrawl.protocol.CrawlURLMetadata;
import org.junit.Test;

import com.google.common.collect.TreeMultimap;

/** 
 * A filter that can block based on a url pattern
 * 
 * @author rana
 *
 */
public class URLPatternBlockFilter extends Filter {

  public URLPatternBlockFilter() { 
    
  }
  
  public URLPatternBlockFilter(String filterPath,boolean hasMasterFile) { 
    super(filterPath,hasMasterFile);
  }
  
  static class DomainURLPatternItem implements Comparable<DomainURLPatternItem>{
    
    public DomainURLPatternItem(String domainRegEx,String pathRegEx) { 
      this.domainRegEx = domainRegEx;
      if (domainRegEx.length() != 0) { 
      	this.domainPatternObj   = Pattern.compile(domainRegEx);
      }

    	this.pathRegEx = pathRegEx;
      this.pathPatternObj   = Pattern.compile(pathRegEx);
    }
    
    String  domainRegEx;
    Pattern domainPatternObj = null;
    String  pathRegEx;
    Pattern pathPatternObj;
		
    @Override
    public int compareTo(DomainURLPatternItem o) {
	    int result = domainRegEx.compareTo(o.domainRegEx);
	    if (result == 0) { 
	    	result = pathRegEx.compareTo(o.pathRegEx);
	    }
	    return result;
    }

  }
  
  private Vector<DomainURLPatternItem> globalPatternList = new Vector<DomainURLPatternItem>();
  private TreeMultimap<String,DomainURLPatternItem> domainToPatternList = TreeMultimap.create();
  

  @Override
  public void loadFilterItem(String filterItemLine) throws IOException {
    int indexOfFirstComma = filterItemLine.indexOf(',');
    if (indexOfFirstComma != -1) {
    	int indexOfNextComma = filterItemLine.indexOf(',',indexOfFirstComma + 1);
    	if (indexOfNextComma != -1) { 
    	
    		String rootDomain = filterItemLine.substring(0,indexOfFirstComma);
    		String subDomainRegExp = "";
    		if (indexOfNextComma - indexOfFirstComma > 1) { 
    			subDomainRegExp = filterItemLine.substring(indexOfFirstComma + 1,indexOfNextComma);
    		}
    		String urlPattern = filterItemLine.substring(indexOfNextComma + 1);
      
	      if (!rootDomain.equals("*") & !rootDomain.equals(".*")) { 
	        domainToPatternList.put(rootDomain,new DomainURLPatternItem(subDomainRegExp,urlPattern));
	      }
	      else {
	        globalPatternList.add(new DomainURLPatternItem(subDomainRegExp,urlPattern));
	      }
    	}
    }
    else { 
      throw new IOException("Invalid Boost Fileter Line:" + filterItemLine);
    }
  }
  
  
  @Test
  public void testFilter() throws Exception {
    loadFilterItem("google.com,photos.google.com,.*");
    loadFilterItem("biblio.com,,/review.php.*");
    loadFilterItem("*,,.*\\.gif");
    CrawlURLMetadata metadata = new CrawlURLMetadata();
    FilterResults resultsOut = new FilterResults();
    assertTrue(filterItem("biblio.com","","/review.php",metadata,resultsOut) == FilterResult.Filter_Reject);
    assertTrue(filterItem("google.com","photos.google.com","/foobar/zzzz",metadata,resultsOut) == FilterResult.Filter_Reject);
    assertFalse(filterItem("google.com","","/foobar/zzzz",metadata,resultsOut) == FilterResult.Filter_Reject);
    assertFalse(filterItem("google.com","feeds.google.com","/foobar/zzzz",metadata,resultsOut) == FilterResult.Filter_Reject);
    assertTrue(filterItem("twitter.com","","/foobar.gif",metadata,resultsOut)  == FilterResult.Filter_Reject);
  }

  @Override
  public FilterResult filterItem(String rootDomainName,String fullyQualifiedDomainName, String urlPath,CrawlURLMetadata metadataIn,FilterResults resultsOut) {    

    for (DomainURLPatternItem globalBoostItem : globalPatternList) {
    	if (globalBoostItem.domainPatternObj == null || globalBoostItem.domainPatternObj.matcher(fullyQualifiedDomainName).matches()) { 
	      if (globalBoostItem.pathPatternObj.matcher(urlPath).matches()) { 
	        return FilterResult.Filter_Reject;
	      }
    	}
    }
    
    Set<DomainURLPatternItem> boostItemSet = domainToPatternList.get(rootDomainName);
    
    for (DomainURLPatternItem boostItem : boostItemSet) {
    	
    	if (boostItem.domainPatternObj == null || boostItem.domainPatternObj.matcher(fullyQualifiedDomainName).matches()) { 
    		if (boostItem.pathPatternObj.matcher(urlPath).matches()) { 
    			return FilterResult.Filter_Reject;
    		}
    	}
    }
    return FilterResult.Filter_NoAction;
  }


  @Override
  public void clear() {
    globalPatternList.clear();
    domainToPatternList.clear();
  }
  
  public static void main(String[] args) {
  	URLPatternBlockFilter filter = new URLPatternBlockFilter();
  	try {
	    filter.testFilter();
    } catch (Exception e) {
	    e.printStackTrace();
    }
  }
  
  
}
