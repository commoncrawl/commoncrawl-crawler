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