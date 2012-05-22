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
