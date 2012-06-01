package org.commoncrawl.service.crawler.filters;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.Vector;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.commoncrawl.io.NIODNSCache;
import org.commoncrawl.protocol.CrawlURLMetadata;
import org.commoncrawl.rpc.base.shared.RPCStruct;
import org.commoncrawl.service.crawler.filters.DomainFilterData;
import org.commoncrawl.service.crawler.filters.FilterResults;
import org.commoncrawl.service.crawler.filters.Filter.FilterResult;
import org.commoncrawl.util.URLUtils;
import org.junit.Test;

import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;


public class CrawlRateOverrideFilter extends Filter {

  private static final Log LOG = LogFactory.getLog(CrawlRateOverrideFilter.class);

  public CrawlRateOverrideFilter() { 
    
  }
  
  public CrawlRateOverrideFilter(String filterPath,boolean hasMasterFile) { 
    super(filterPath,hasMasterFile);
  }
  
  static class CrawlRateBoostItem implements Comparable<CrawlRateBoostItem> {
    
    public CrawlRateBoostItem(String subDomainRegEx,String pathRegEx,int  crawlRateValue) { 
      
      if (subDomainRegEx.length() != 0 && !subDomainRegEx.equals("*")) { 
        this.subDomainRegEx = Pattern.compile(subDomainRegEx);
      }
      if (!pathRegEx.equals("*")) { 
        this.pathRegEx = Pattern.compile(pathRegEx);
      }
      this.crawlRateValue = crawlRateValue;
    }
    
    Pattern subDomainRegEx = null;
    Pattern pathRegEx = null;
    int     crawlRateValue;
    
    @Override
    public int compareTo(CrawlRateBoostItem o) {
      if (crawlRateValue < o.crawlRateValue)
        return -1;
      else if (crawlRateValue > o.crawlRateValue) 
        return 1;
      return 0;
    }
  };
  
  
  private SortedSetMultimap<String,CrawlRateBoostItem> rootDomainToBoostMap = TreeMultimap.create(); 
  
  // private Map<String,Map<String,CrawlRateBoostItem> > rootDomainToBoostMap = new HashMap<String,Map<String,CrawlRateBoostItem>>();
  

  @Override
  public void loadFilterItem(String filterItemLine) throws IOException {
    String items[] = filterItemLine.split(",");
    if (items.length == 4) {
      String rootDomain = items[0];
      String subDomainRegExp = items[1];
      String pathRegExp = items[2];
      int  desiredCrawlRate = Integer.parseInt(items[3]);

      LOG.info("Processing Valid Line. " 
          + "RootDomain:" + rootDomain 
          + " SubDomain:" + subDomainRegExp 
          + " Path:" + pathRegExp 
          + " CrawRate:" + desiredCrawlRate);
      
      CrawlRateBoostItem boostItem = new CrawlRateBoostItem(subDomainRegExp,pathRegExp, desiredCrawlRate);
      
      rootDomainToBoostMap.put(rootDomain, boostItem);
    }
    else { 
      LOG.error("Skipping invalid line:" + filterItemLine);
    }
  }
  
  
  private static String[] testInputs = new String[] { 
    "amazon.de,*,*,50",
    "amazon.com,*,*,50",
    "amazon.ca,*,*,50",
    "amazon.fr,*,*,50",
    "amazon.co.jp,*,*,50",
    "amazon.co.uk,*,*,50",
    "barnesandnoble.com,*,*,1000",
    "borders.com,*,*,1000",
    "allbookstores.com,*,*,15000",
    "booksamillion.com,*,*,5000",
    "ebay.com,*,*,50",
    "yelp.com,*,*,2500",
    "tripadvisor.com,*,*,2500",
    "yahoo.com,shopping.yahoo.com,*,50",
    "books-by-isbn.com,*,*,1500",
    "blogspot.com,*,*,10",
    "wordpress.com,*,*,50"
  };
  

  @Override
  public FilterResult filterItem(String rootDomain,String domainName, String urlPath,CrawlURLMetadata metadataIn,FilterResults resultsOut) {    

    SortedSet<CrawlRateBoostItem> items = rootDomainToBoostMap.get(rootDomain);
    
    for (CrawlRateBoostItem item : items) {
      if (item.subDomainRegEx == null || item.subDomainRegEx.matcher(domainName).matches()) { 
        if (item.pathRegEx == null || item.pathRegEx.matcher(urlPath).matches()) {  
          resultsOut.setCrawlRateOverride(item.crawlRateValue);
          return FilterResult.Filter_Modified;
        }
      }
    }
    return FilterResult.Filter_NoAction;
  }


  @Override
  public void clear() {
    rootDomainToBoostMap.clear();
  }
  
  public static int checkForCrawlRateOverride(CrawlRateOverrideFilter filter,URL url) { 
    FilterResults resultsOut = new FilterResults();
    String rootDomain = URLUtils.extractRootDomainName(url.getHost());
    if (rootDomain != null) { 
	    if (filter.filterItem(rootDomain,url.getHost(), url.getPath(), null, resultsOut) == FilterResult.Filter_Modified) { 
	      return resultsOut.getCrawlRateOverride();
	    }
    }
    return -1;
  }

  public static void main(String[] args) {
    CrawlRateOverrideFilter filter = new CrawlRateOverrideFilter();
    for (String inputLine : testInputs) { 
      try {
        filter.loadFilterItem(inputLine);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    try {
      URL targetURL = new URL("http://chicagoconnie.blogspot.com/");
      
      LOG.info("CrawlRate:" + checkForCrawlRateOverride(filter,targetURL));
      
    } catch (MalformedURLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    
    for (String inputLine : testInputs) { 
      String parts[] = inputLine.split(",");
      String rootDomain = parts[0];
      String subDomain  = parts[1];
      String path = parts[2];
      
      FilterResults filterResults = new FilterResults();
      
      
      
      if (subDomain.equals("*")) { 
        //assertTrue(filter.filterItem(rootDomain,"www." + rootDomain, "/foobar", null, filterResults) == FilterResult.Filter_Modified);
        //assertTrue(filter.filterItem(rootDomain+"Other","www." + rootDomain+"Other", "/foobar", null, filterResults) == FilterResult.Filter_NoAction);
      }
      else { 
        //assertTrue(filter.filterItem(rootDomain,subDomain, "/foobar", null, filterResults) == FilterResult.Filter_Modified);
        //assertTrue(filter.filterItem(rootDomain,"prefix" + subDomain, "/foobar", null, filterResults) == FilterResult.Filter_NoAction);
      }
      
      
    }
  }
  
}
