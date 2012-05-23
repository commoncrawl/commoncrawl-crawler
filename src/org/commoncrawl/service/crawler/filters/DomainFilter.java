package org.commoncrawl.service.crawler.filters;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.io.NIODNSCache;
import org.commoncrawl.protocol.CrawlURLMetadata;
import org.commoncrawl.rpc.base.shared.RPCStruct;
import org.commoncrawl.service.crawler.filter.DomainFilterData;
import org.commoncrawl.service.crawler.filter.FilterResults;
import org.commoncrawl.service.directory.BlockingClient;
import org.commoncrawl.util.URLFingerprint;
import org.commoncrawl.util.URLUtils;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

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
