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

package org.commoncrawl.crawl.crawler;

import org.commoncrawl.util.internal.RuntimeStatsCollector;


/**
 * Stats namespace for the crawler
 * 
 * @author rana
 *
 */
public final class CrawlerEngineStats extends RuntimeStatsCollector.Namespace { 
  
 
  public enum  Name { 
    
    // Engine Stats
    CrawlerEngine_TotalProcessedURLCount,
    CrawlerEngine_FetchFailedCount,
    CrawlerEngine_FetchSucceededCount,
    CrawlerEngine_DNSAddToCacheTime,
    CrawlerEngine_DNSLookupFromCacheTime,
    CrawlerEngine_DNSCacheNodeCount,
    CrawlerEngine_DNSProcessResultTime,
    CrawlerEngine_ActiveLoadCount,
    CrawlerEngine_DeferredLoadCount,
    CrawlerEngine_PendingCount,
    CrawlerEngine_QueuedCount,
    CrawlerEngine_UpTime,
    CrawlerEngine_BuildTime,
    
    
    
    // Crawl Queue Stats
    CrawlQueue_ActiveHostsCount,
    CrawlQueue_ScheduledHostsCount,
    CrawlQueue_IdledHostsCount,
    // CrawlQueue_ZombieList,
    CrawlQueue_CNameToFPCacheSize,
    CrawlQueue_PurgedHostsCount,
    
    
    // HTTP Fetcher Stats
    HTTPFetcher_ActiveConnections,
    HTTPFetcher_FetcherQueueSize,
    HTTPFetcher_TotalSuccessfulConnects,
    HTTPFetcher_TotalFailedConnects,
    HTTPFetcher_ConnectionsInResolvingState,
    HTTPFetcher_ConnectionsInConnectingState,
    HTTPFetcher_ConnectionsInSendingState,
    HTTPFetcher_ConnectionsInRecevingState,
    HTTPFetcher_TimeDeltaBetweenSnapshots,
    HTTPFetcher_SnapshotURLSPerSecond,
    HTTPFetcher_MovingAverageURLSPerSecond,
    HTTPFetcher_SmoothedURLSPerSecond,
    HTTPFetcher_SnapshotKBPerSec,
    HTTPFetcher_MovingAverageKBPerSec,
    HTTPFetcher_SmoothedKBPerSec,
    HTTPFetcher_ConnectionMap,
    HTTPFetcher_LaggingConnectionDetailArray,
    HTTPFetcher_CumilativeKBytesIN,
    HTTPFetcher_CumilativeKBytesOUT,
    
    //DNS 
    DNS_TotalDNSQueries,
    DNS_TotalCacheHits,
    DNS_TotalCacheMisses,
    
    // Flusher Stats ... 
    CrawlLog_FlushTimeAVG,
    CrawlLog_FlushTimeSmoothed,
    CrawlLog_FlushTimeLast
    
  }
  public static CrawlerEngineStats ID = new CrawlerEngineStats();
  
  private CrawlerEngineStats() {
    RuntimeStatsCollector.registerNames(this, Name.values());
  }  
   
}