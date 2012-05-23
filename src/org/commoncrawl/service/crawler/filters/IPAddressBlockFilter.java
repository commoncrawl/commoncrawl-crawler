package org.commoncrawl.service.crawler.filters;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;

import org.commoncrawl.protocol.CrawlURLMetadata;
import org.commoncrawl.service.crawler.filter.FilterResults;
import org.commoncrawl.util.IPAddressUtils;
import org.commoncrawl.util.IntrusiveList;
import org.commoncrawl.util.IntrusiveList.IntrusiveListElement;
import org.junit.Test;

public class IPAddressBlockFilter extends Filter {

  public IPAddressBlockFilter() { 
    
  }
  
  public IPAddressBlockFilter(String filterPath,boolean hasMasterFile) { 
    super(filterPath,hasMasterFile);
  }  
  
  private static final int CLASS_B_MASK = 0xFFFF0000;
  
  private static class IPAddressRange extends IntrusiveListElement<IPAddressRange> {
    
    public IPAddressRange(int ipAddressSubnet,int ipAddressMask) { 
      this.ipAddressSubnet = ipAddressSubnet;
      this.ipAddressMask   =  ipAddressMask;
    }
    public int ipAddressSubnet;
    public int ipAddressMask;
  }
  
  private Map<Integer,IntrusiveList<IPAddressRange>> _rangeMap = new TreeMap<Integer,IntrusiveList<IPAddressRange>>();
   
  
  @Override
  public void clear() {
    _rangeMap.clear();
  }
  
  @Override
  public void loadFilterItem(String filterItemLine) throws IOException {
    String tokens[] = filterItemLine.split(",");
    if (tokens.length >= 2) { 
      int ipAddress     = IPAddressUtils.IPV4AddressToInteger(InetAddress.getByName(tokens[0]).getAddress());
      int ipAddressMask = IPAddressUtils.IPV4AddressToInteger(InetAddress.getByName(tokens[1]).getAddress());
      
      IntrusiveList<IPAddressRange> rangeList = rangeListForIPAddress(ipAddress,true);
      
      rangeList.addTail(new IPAddressRange(ipAddress,ipAddressMask));
    }
  }


  @Override
  public FilterResult filterItem(String rootDomainName,String fullyQualifiedDomainName, String urlPath,CrawlURLMetadata metadata, FilterResults results) {
    
    IntrusiveList<IPAddressRange> rangeList = rangeListForIPAddress(metadata.getServerIP(),false);
    
    if (rangeList != null) { 
      for (IPAddressRange rangeItem : rangeList) { 
        if ((metadata.getServerIP() & rangeItem.ipAddressMask) == rangeItem.ipAddressSubnet) { 
          return FilterResult.Filter_Reject;
        }
      }
    }
    return FilterResult.Filter_NoAction;
  }

  private IntrusiveList<IPAddressRange> rangeListForIPAddress(int ipAddress,boolean addIfMissing) { 
    IntrusiveList<IPAddressRange> rangeList = _rangeMap.get((ipAddress & CLASS_B_MASK));
    if (rangeList == null && addIfMissing) { 
      rangeList = new IntrusiveList<IPAddressRange>();
      _rangeMap.put((ipAddress & CLASS_B_MASK),rangeList);
    }
    return rangeList;
  }
  
  
  
  @Test
  public void validateFilter() throws Exception {
    loadFilterItem("69.64.144.0,255.255.240.0,FOO COMMENT");
    loadFilterItem("83.138.128.248,255.255.255.255,BAR COMMENT");
    
    CrawlURLMetadata metadata = new CrawlURLMetadata();
    
    
    metadata.setServerIP(IPAddressUtils.IPV4AddressToInteger(InetAddress.getByName("69.64.144.0").getAddress()));
    assertTrue(filterItem(null,null, null, metadata, null) == FilterResult.Filter_Reject);
    metadata.setServerIP(IPAddressUtils.IPV4AddressToInteger(InetAddress.getByName("69.64.159.255").getAddress()));
    assertTrue(filterItem(null,null, null, metadata, null) == FilterResult.Filter_Reject);
    metadata.setServerIP(IPAddressUtils.IPV4AddressToInteger(InetAddress.getByName("69.64.160.0").getAddress()));
    assertTrue(filterItem(null,null, null, metadata, null) == FilterResult.Filter_NoAction);
    metadata.setServerIP(IPAddressUtils.IPV4AddressToInteger(InetAddress.getByName("69.64.143.0").getAddress()));
    assertTrue(filterItem(null,null, null, metadata, null) == FilterResult.Filter_NoAction);

    
    metadata.setServerIP(IPAddressUtils.IPV4AddressToInteger(InetAddress.getByName("83.138.128.248").getAddress()));
    assertTrue(filterItem(null,null, null, metadata, null) == FilterResult.Filter_Reject);
    metadata.setServerIP(IPAddressUtils.IPV4AddressToInteger(InetAddress.getByName("83.138.128.247").getAddress()));
    assertTrue(filterItem(null,null, null, metadata, null) == FilterResult.Filter_NoAction);
    metadata.setServerIP(IPAddressUtils.IPV4AddressToInteger(InetAddress.getByName("83.138.128.249").getAddress()));
    assertTrue(filterItem(null,null, null, metadata, null) == FilterResult.Filter_NoAction);
    
  }
}
