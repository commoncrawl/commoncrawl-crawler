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
import java.net.InetAddress;

import org.apache.hadoop.mapred.JobConf;
import org.commoncrawl.crawl.filter.FilterResults;
import org.commoncrawl.protocol.CrawlURLMetadata;

/**
 * Filter base class 
 * 
 * @author rana
 *
 */
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
