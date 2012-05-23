package org.commoncrawl.service.crawler.filters;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.commoncrawl.io.NIODNSCache;
import org.commoncrawl.protocol.CrawlURLMetadata;
import org.commoncrawl.rpc.base.shared.RPCStruct;
import org.commoncrawl.service.crawler.filter.DomainFilterData;
import org.commoncrawl.service.crawler.filter.FilterResults;
import org.commoncrawl.util.URLUtils;
import org.junit.Test;

import com.google.common.collect.TreeMultimap;

public class PageRankBoostFilter extends Filter {

  private static final Log LOG = LogFactory.getLog(PageRankBoostFilter.class);
  
  public PageRankBoostFilter(String filterPath,boolean hasMasterFile) { 
    super(filterPath,hasMasterFile);
  }
  
  static class PageRankBoostItem implements Comparable<PageRankBoostItem>{
    
    public PageRankBoostItem(String domainRegEx,String pathRegEx,float urlBoostValue) { 
      this.domainRegEx = domainRegEx;
    	this.pathRegEx = pathRegEx;
    	if (domainRegEx.length() != 0) { 
    		domainPatternObj = Pattern.compile(domainRegEx);
    	}
      this.pathPatternObj   = Pattern.compile(pathRegEx);
      this.urlBoostValue = urlBoostValue;
    }
    
    String  domainRegEx;
    String  pathRegEx;
    Pattern domainPatternObj = null;
    Pattern pathPatternObj;
    float   urlBoostValue;
    
		@Override
    public int compareTo(PageRankBoostItem o) {
	    int result = domainRegEx.compareTo(o.domainRegEx);
	    if (result == 0) { 
	    	result = pathRegEx.compareTo(o.pathRegEx);
	    }
	    return result;
    }
  };
  
  
  private Vector<PageRankBoostItem> globalBoostItems = new Vector<PageRankBoostItem>();
  private TreeMultimap<String,PageRankBoostItem> domainToBoostMap = TreeMultimap.create();
  

  @Override
  public void loadFilterItem(String filterItemLine) throws IOException {
    String items[] = filterItemLine.split(",");
    if (items.length == 4) {
      String rootDomain 				  = items[0];
      String fullyQualifiedDomain = items[1];
      String urlPattern 					= items[2];
      float  boostValue = Float.parseFloat(items[3]);
      
      Map<String,PageRankBoostItem> boostItemMap = null;
      
      if (!rootDomain.equals("*") && !rootDomain.equals(".*")) { 
        domainToBoostMap.put(rootDomain,new PageRankBoostItem(fullyQualifiedDomain, urlPattern, boostValue));
      }
      else { 
        PageRankBoostItem boostItem = new PageRankBoostItem(fullyQualifiedDomain,urlPattern,boostValue);
        globalBoostItems.add(boostItem);
      }
    }
    else { 
      LOG.error("Invalid Boost Fileter Line:" + filterItemLine);
    }
  }
  
  
  @Test
  public void testFilter() throws Exception {
    loadFilterItem("*,,.*,2.00");
    loadFilterItem("google.com,,/foobar/.*,1.00");
    loadFilterItem("twitter.com,,/[^/]*,1.00");
    CrawlURLMetadata metadata = new CrawlURLMetadata();
    FilterResults resultsOut = new FilterResults();
    filterItem("google.com","","/foobar/zzzz",metadata,resultsOut);
    filterItem("twitter.com","","/foobar",metadata,resultsOut);
  }

  @Override
  public FilterResult filterItem(String rootDomainName,String fullyQualifiedDomainName,String urlPath,CrawlURLMetadata metadataIn,FilterResults resultsOut) {    

    for (PageRankBoostItem globalBoostItem : globalBoostItems) { 
      if (globalBoostItem.pathPatternObj.matcher(urlPath).matches()) { 
        resultsOut.setPageRankBoostValue(resultsOut.getPageRankBoostValue() + globalBoostItem.urlBoostValue);
      }
    }
    
    Set<PageRankBoostItem> boostItemSet = domainToBoostMap.get(rootDomainName);
    
    for (PageRankBoostItem boostItem : boostItemSet) { 
      if (boostItem.domainPatternObj == null || boostItem.domainPatternObj.matcher(fullyQualifiedDomainName).matches()) { 
      	if (boostItem.pathPatternObj.matcher(urlPath).matches()) { 
      		resultsOut.setPageRankBoostValue(resultsOut.getPageRankBoostValue() + boostItem.urlBoostValue);
      	}
      }
    }
    return (resultsOut.isFieldDirty(FilterResults.Field_PAGERANKBOOSTVALUE)) ? FilterResult.Filter_Modified : FilterResult.Filter_NoAction;
  }


  @Override
  public void clear() {
    globalBoostItems.clear();
    domainToBoostMap.clear();
  }
  
  
  
}
