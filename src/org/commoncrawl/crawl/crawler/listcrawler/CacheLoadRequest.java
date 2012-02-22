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

package org.commoncrawl.crawl.crawler.listcrawler;


import org.commoncrawl.crawl.crawler.listcrawler.CacheManager.CacheItemCheckCallback;

/**
 * A cache load request 
 * 
 * @author rana
 *
 */
class CacheLoadRequest { 
  
  public enum RequestType { 
    LocalCacheLoadRequest,
    HDFSCacheLoadRequest
  }
  
  public CacheLoadRequest(String url,long fingerprint,CacheItemCheckCallback callback) { 
    _fingerprint = fingerprint;
    _targetURL = url;
    _callback = callback;
    _requestType = RequestType.HDFSCacheLoadRequest;
  }
  
  public CacheLoadRequest(String url,Long[] locationHints,CacheItemCheckCallback callback) { 
    _targetURL = url;
    _loacations = locationHints;
    _callback = callback;
    _pendingItemCount = locationHints.length;
    _requestType = RequestType.LocalCacheLoadRequest;
  }
  
  
  public RequestType _requestType;
  public String _targetURL;
  public Long[] _loacations;
  public CacheItemCheckCallback _callback;
  public int _pendingItemCount;
  public long _fingerprint;
}