package org.commoncrawl.service.crawler.filters;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Pattern;

import junit.framework.TestFailure;

import org.apache.hadoop.mapred.JobConf;
import org.commoncrawl.io.NIODNSCache;
import org.commoncrawl.protocol.CrawlURLMetadata;
import org.commoncrawl.rpc.base.shared.RPCStruct;
import org.commoncrawl.service.crawler.filter.DomainFilterData;
import org.commoncrawl.service.crawler.filter.FilterResults;
import org.commoncrawl.util.URLUtils;
import org.junit.Test;

import com.google.common.collect.TreeMultimap;

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
