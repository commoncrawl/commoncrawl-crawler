package org.commoncrawl.crawl.filters;

import java.io.IOException;
import java.util.TreeMap;

import org.commoncrawl.crawl.filter.FilterResults;
import org.commoncrawl.protocol.CrawlURLMetadata;
import org.commoncrawl.util.internal.URLFingerprint;

/**
 * A Filter that operates on URL Fingerprints
 * 
 * @author rana
 *
 */
public class DomainHashFilter extends Filter  {
  
  private TreeMap<Integer,Integer> _mapDomainHashToPos = new TreeMap<Integer,Integer>();
  private int lastPosition = 0;
  
  public DomainHashFilter() {
    super();
  }
  
  public DomainHashFilter(String filterPath,boolean hasMasterFile) {
    super(filterPath, hasMasterFile);
  }

  @Override
  public void loadFilterItem(String filterItemLine) throws IOException {
    if (filterItemLine.length() == 0) { 
      // LOG.error("filterItemLine is zero length");
    }
    else { 
      // LOG.info("Processing Filter Line:" + filterItemLine);
      if (filterItemLine.charAt(0) == '.')
        filterItemLine = filterItemLine.substring(1);
      String items[] = filterItemLine.split(",");
      if (items.length >= 1) { 
        _mapDomainHashToPos.put(URLFingerprint.generate32BitHostFP(items[0].toLowerCase()),++lastPosition);
      }
    }    
  }
  
  public int getHashValueCount() { 
    return _mapDomainHashToPos.size();
  }

  @Override
  public void clear() {
    _mapDomainHashToPos.clear();    
  }

  @Override
  public FilterResult filterItem(String rootDomainName,String fullyQualifiedDomainName, String urlPath,CrawlURLMetadata metadata, FilterResults results) {
    int fingerprint = URLFingerprint.generate32BitHostFP(rootDomainName.toLowerCase());
    Integer position = _mapDomainHashToPos.get(fingerprint);
    if (position != null) { 
      results.setPosition(position);
      return FilterResult.Filter_Accept;
    }
    return FilterResult.Filter_Reject;
  }
}
