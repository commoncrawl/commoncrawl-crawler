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


import java.util.concurrent.Semaphore;

import org.commoncrawl.protocol.CacheItem;


/** 
 * a cache write request
 * 
 * @author rana
 *
 */
class CacheWriteRequest { 
  public enum RequestType { 
    WriteRequest,
    ExitThreadRequest
  }
  
  public CacheWriteRequest(long itemFingerprint,CacheItem itemToWrite,Semaphore optionalSemaphore) { 
    _item = itemToWrite;
    _itemFingerprint = itemFingerprint;
    _requestType = RequestType.WriteRequest;
    _optionalSemaphore = optionalSemaphore;
  }
  
  public CacheWriteRequest() { 
    _requestType = RequestType.ExitThreadRequest;
  }

  public CacheItem      _item = null;
  public long           _itemFingerprint;
  public long           _recordOffset = -1;
  public CacheWriteRequest.RequestType    _requestType;
  public Semaphore 			_optionalSemaphore;
}