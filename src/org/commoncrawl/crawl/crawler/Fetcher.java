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

import java.util.LinkedList;

import org.commoncrawl.crawl.statscollector.CrawlerStats;
import org.commoncrawl.util.internal.RuntimeStatsCollector;

/**
 * Abstraction of a service that knows how to fetch CrawlTargets
 * 
 * @author rana
 *
 */
public interface Fetcher {

	public static interface Callback { 
	
		public void fetchComplete(CrawlTarget url);

	}
		
	/** start fetching urls **/
	public void start();
	
	/** stop fetching **/
	public void stop();
	
	/** pause **/
	public void pause();
	
	/** resume **/
	public void resume();
	
	/** clear queues **/
	public void clearQueues();
	
	/** shutdown **/
	public void shutdown();
	
	/** add urls to the queue **/
	public void queueURLs(LinkedList<CrawlTarget> url);
	
	/** add a single url to the queue **/
	public void queueURL(CrawlTarget target);
	
	/** dump the fetcher's state **/
	public void collectStats(CrawlerStats crawlerStats,RuntimeStatsCollector stats);
}
