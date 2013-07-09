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
package org.commoncrawl.io;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.commoncrawl.io.NIODNSCache.Node;
import org.commoncrawl.io.NIODNSQueryClient.Status;
import org.commoncrawl.util.IPAddressUtils;
import org.commoncrawl.util.IntrusiveList.IntrusiveListElement;

/**
 * 
 * @author rana
 *
 */
public abstract class NIODNSResolver extends IntrusiveListElement<NIODNSResolver> implements Comparable<NIODNSResolver> {

  /** cache **/
  public static NIODNSCache _dnsCache                       = new NIODNSCache();
  /** inverse cache **/
  public static NIODNSCache _badHostCache                   = new NIODNSCache();
  /** ttl override **/
  public static final int MIN_TTL_VALUE = 60 * 20 * 1000;
  private static long TTL_DELTA_MIN = 1000 * 60 * 10; // min ttl lifetime is 10 minutes for us

  public static final int    SERVER_FAIL_BAD_HOST_LIFETIME   = 3600 * 1000;                         // ONE HOUR BY DEFAULT
  public static final int    NXDOMAIN_FAIL_BAD_HOST_LIFETIME = 5 * 60 * 1000;                       // 5 minutes
  
  private int _queuedCount;
  
  static NIODNSQueryLogger      _logger                         = null;

  AtomicLong _cacheHits = new AtomicLong();
  
  /**
   * queue a DNS resolution request
   * 
   * @param client
   *          - the callback interface
   * @param theHost
   *          - the host name to query for
   * @return
   * @throws IOException
   */
  public abstract Future<NIODNSQueryResult> resolve(NIODNSQueryClient client, String theHost, boolean noCache,boolean highPriorityRequest, int timeoutValue) throws IOException;

  
  /** 
   * stats 
   * @return queued item count - if this is a queueing resolver 
   */
  public int getQueuedItemCount() { return _queuedCount; }
  
  /** 
   * increment queued count 
   */
  
  public void incQueuedCount() { _queuedCount++; } 
  
  /** 
   * stats
   * @return cache hits if this resolver makes use of caching ... 
   */
  public long getCacheHitCount() { return 0; } 
  
  public static NIODNSQueryResult checkCache(NIODNSQueryClient client,
      String hostName) throws UnknownHostException {

    NIODNSCache.DNSResult result = _dnsCache.getIPAddressForHost(hostName);

    if (result != null) {

      // get delta between time to live and now ...
      long ttlDelta = result.getTTL() - System.currentTimeMillis();

      // if theoretically this ip address has expired
      if (ttlDelta < 0) {
        if (Math.abs(ttlDelta) > TTL_DELTA_MIN) {
          return null;
        }
      }

      /*
       * if (Environment.detailLogEnabled()) {
       * LOG.info("Cache HIT for host:"+hostName
       * +" ip:"+IPAddressUtils.IntegerToIPAddressString
       * (result.getIPAddress())); }
       */

      NIODNSQueryResult queryResult = new NIODNSQueryResult(null, client, hostName);

      queryResult.setAddress(IPAddressUtils.IntegerToInetAddress(result
          .getIPAddress()));
      queryResult.setCName(result.getCannonicalName());
      queryResult.setTTL(result.getTTL());
      queryResult.setStatus(Status.SUCCESS);

      return queryResult;
    }

    // check bad host cache ...
    Node resolvedNode = _badHostCache.findNode(hostName);

    if (resolvedNode != null) {
      // LOG.info("Found in Bad Host Cache:" + hostName + " ttl:" + new
      // Date(resolvedNode.getTimeToLive()));
    }
    // IFF found and the bad node has not expired ...
    if (resolvedNode != null
        && resolvedNode.getTimeToLive() >= System.currentTimeMillis()) {

      // LOG.info("Host:" + hostName + " Identified as Bad Host via Cache");

      NIODNSQueryResult queryResult = new NIODNSQueryResult(null, client, hostName);

      queryResult.setStatus(Status.SERVER_FAILURE);
      queryResult.setErrorDesc("Failed via Bad Host Cache");

      return queryResult;
    }

    return null;
  }
  
  public int compareTo(NIODNSResolver o) {
    return (_queuedCount < o._queuedCount) ? -1 : (_queuedCount > o._queuedCount) ? 1 : 0;
  }

  
  public static NIODNSCache getDNSCache() {
    return _dnsCache;
  }

  public static NIODNSCache getBadHostCache() {
    return _badHostCache;
  }

  public static void setLogger(NIODNSQueryLogger logger) {
    _logger = logger;
  }

  public static NIODNSQueryLogger getLogger() {
    return _logger;
  }


}
