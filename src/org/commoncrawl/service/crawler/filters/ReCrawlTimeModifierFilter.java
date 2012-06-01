package org.commoncrawl.service.crawler.filters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Pattern;

import org.apache.hadoop.mapred.JobConf;
import org.commoncrawl.io.NIODNSCache;
import org.commoncrawl.protocol.CrawlURLMetadata;
import org.commoncrawl.rpc.base.shared.RPCStruct;
import org.commoncrawl.service.crawler.filters.DomainFilterData;
import org.commoncrawl.service.crawler.filters.FilterResults;
import org.commoncrawl.util.URLUtils;
import org.junit.Test;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

public class ReCrawlTimeModifierFilter extends Filter {

  public ReCrawlTimeModifierFilter(String itemPath,boolean hasMasterFile) { 
    super(itemPath,hasMasterFile);
  }
  
  static class RecrawlTimeModifierItem implements Comparable<RecrawlTimeModifierItem> {
    
    public RecrawlTimeModifierItem(String domainRegEx,String pathRegEx,long recrawlTimeValue) { 
    	domainPatterStr = domainRegEx;
    	pathPatternStr = pathRegEx;
    	
    	if (domainRegEx.length() != 0) { 
      	this.domainPattern = Pattern.compile(domainRegEx);
      }
    	this.pathPattern   = Pattern.compile(pathRegEx);
      this.recrawlTimeValue = recrawlTimeValue;
    }
    
    String domainPatterStr;
    String pathPatternStr;
    
    Pattern domainPattern = null;
    Pattern pathPattern = null;
    long    recrawlTimeValue;
		@Override
    public int compareTo(RecrawlTimeModifierItem o) {
	    int result = domainPatterStr.compareTo(o.domainPatterStr);
	    if (result == 0) { 
	    	result = pathPatternStr.compareTo(o.pathPatternStr);
	    }
	    return result;
    }
  };
  
  
  private Vector<RecrawlTimeModifierItem> globalBoostItems = new Vector<RecrawlTimeModifierItem>();
  private TreeMultimap<String,RecrawlTimeModifierItem> domainToModifierListMap = TreeMultimap.create();
  
  
  @Override
  public void loadFilterItem(String filterItemLine) throws IOException {
    String items[] = filterItemLine.split(",");
    if (items.length == 4) {
      String rootDomain = items[0];
      String domainPattern = items[1];
      String urlPattern = items[2];
      long   modifiedTimeValue = Long.parseLong(items[3]);
      
      Map<String,RecrawlTimeModifierItem> modifierItemMap = null;
      
      if (!rootDomain.equals("*") && !rootDomain.equals(".*")) {
      	RecrawlTimeModifierItem newItem = new RecrawlTimeModifierItem(domainPattern,urlPattern,modifiedTimeValue);
      	domainToModifierListMap.put(rootDomain, newItem);
      }
      else { 
        RecrawlTimeModifierItem boostItem = new RecrawlTimeModifierItem(domainPattern,urlPattern,modifiedTimeValue);
        globalBoostItems.add(boostItem);
      }
    }
    else { 
      throw new IOException("Invalid Boost Fileter Line:" + filterItemLine);
    }
  }
  
  
  @Test
  public void testFilter() throws Exception {
    loadFilterItem("*,(^/$|(^/index\\.[^/]*$)),0");
    loadFilterItem("twitter.com,/[^/]*,0");
    CrawlURLMetadata metadata = new CrawlURLMetadata();
    FilterResults resultsOut = new FilterResults();
    filterItem("google.com","","/",metadata,resultsOut);
    filterItem("twitter.com","","/foobar",metadata,resultsOut);
    filterItem("kotay.com","","/index.html",metadata,resultsOut);
    filterItem("kotay.com","","/index.php",metadata,resultsOut);
  }

  @Override
  public FilterResult filterItem(String rootDomainName,String fullyQualifiedDomainName, String urlPath,CrawlURLMetadata metadataIn,FilterResults resultsOut) {    

  	
    for (RecrawlTimeModifierItem globalBoostItem : globalBoostItems) { 
      if (globalBoostItem.pathPattern.matcher(urlPath).matches()) {
        if (resultsOut.isFieldDirty(FilterResults.Field_MODIFIEDRECRAWLTIME))
          resultsOut.setModifiedRecrawlTime(Math.min(resultsOut.getModifiedRecrawlTime(),globalBoostItem.recrawlTimeValue));
        else 
          resultsOut.setModifiedRecrawlTime(globalBoostItem.recrawlTimeValue);
      }
    }
    
    Set<RecrawlTimeModifierItem> items = domainToModifierListMap.get(rootDomainName);
    
    for (RecrawlTimeModifierItem boostItem : items) {
    	if (boostItem.domainPattern == null || boostItem.domainPattern.matcher(fullyQualifiedDomainName).matches()) { 
    		if (boostItem.pathPattern.matcher(urlPath).matches()) { 
	        if (resultsOut.isFieldDirty(FilterResults.Field_MODIFIEDRECRAWLTIME))
	          resultsOut.setModifiedRecrawlTime(Math.min(resultsOut.getModifiedRecrawlTime(),boostItem.recrawlTimeValue));
	        else 
	          resultsOut.setModifiedRecrawlTime(boostItem.recrawlTimeValue);
    		}
    	}
    }
    return (resultsOut.isFieldDirty(FilterResults.Field_MODIFIEDRECRAWLTIME)) ? FilterResult.Filter_Modified : FilterResult.Filter_NoAction;
  }


  @Override
  public void clear() {
    globalBoostItems.clear();
    domainToModifierListMap.clear();    
  }
}
