/**
 * Copyright 2008 - CommonCrawl Foundation
 * 
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 **/

package org.commoncrawl.service.listcrawler;


import org.commoncrawl.service.listcrawler.CacheManager.CacheItemCheckCallback;

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
    _locations = locationHints;
    _callback = callback;
    _pendingItemCount = locationHints.length;
    _requestType = RequestType.LocalCacheLoadRequest;
  }
  
  
  public RequestType _requestType;
  public String _targetURL;
  public Long[] _locations;
  public CacheItemCheckCallback _callback;
  public int _pendingItemCount;
  public long _fingerprint;
}