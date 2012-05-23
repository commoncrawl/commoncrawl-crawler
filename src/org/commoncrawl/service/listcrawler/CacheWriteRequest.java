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