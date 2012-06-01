package org.commoncrawl.service.crawler.filters;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.hadoop.mapred.JobConf;
import org.commoncrawl.protocol.CrawlURLMetadata;
import org.commoncrawl.rpc.base.shared.RPCStruct;
import org.commoncrawl.service.crawler.filters.FilterResults;

public abstract class Filter {

  public enum FilterResult { 
    Filter_NoAction,
    Filter_Reject,
    Filter_Accept,
    Filter_Modified
  }  
  
  protected String  _filterPath = null;
  protected boolean _hasMasterFile = false;
  
  public Filter() { 
    
  }
  
  public Filter(String filterPath,boolean hasMasterFile) { 
    _filterPath = filterPath;
    _hasMasterFile = hasMasterFile;
  }
  
  public void publishFilter(JobConf job)throws IOException {
    if (_filterPath != null) { 
      Utils.publishFilterToCache(job, this, _filterPath, _hasMasterFile);
    }
  }
  
  
  public void load(InetAddress directorServerAddress) throws IOException { 
    Utils.loadFilterFromPath(directorServerAddress, this, _filterPath, _hasMasterFile);
  }
  
  public void loadFromPath(InetAddress directoryServerAddress,String filterPath,boolean hasMasterFile) throws IOException {
    _filterPath = filterPath;
    _hasMasterFile = hasMasterFile;
    Utils.loadFilterFromPath(directoryServerAddress, this, filterPath, hasMasterFile);
  }
  
  public void loadFromCache(JobConf job)throws IOException {
    if (_filterPath != null) { 
      Utils.loadFilterFromCache(job, _filterPath, this);
    }
  }
  
  public void loadFilterItem(String filterItemLine) throws IOException {}

  public abstract FilterResult filterItem(String rootDomain,String domainName,String urlPath, CrawlURLMetadata metadata,FilterResults results);
  
  public abstract void clear();
    
}
