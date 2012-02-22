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

import org.commoncrawl.crawl.crawler.RobotRulesParser.RobotRuleSet;
import org.commoncrawl.util.internal.HttpCookieUtils.CookieStore;

/**
 * @author rana
 * 
 * Further specialization of the CrawlHost class
 *  
 */
public interface CrawlListHost extends CrawlHost {

	enum CounterId {
		RobotsExcludedCount, SuccessfullGetCount, Http200Count, Http403Count, FailedDomainCount, FailedGetCount, ConsecutiveIOErrorCount
	}

	/** associated the given content crc with the givn robotruleset object**/
	public void cacheRobotsFile(RobotRuleSet ruleSet, long robotsCRC);

	/** get cached robots entry given crc */
	public RobotRuleSet getCachedRobotsEntry(long crcValue);

	/** get the cookie store associated with the host **/
	public CookieStore getCookieStore();

	/** get the crawl delay to use for this host **/
	public int getCrawlDelay();

	/** get the scheme (http/https) used to retrieve urls for this host **/
	public String getScheme();

	/** increment stats for a specific count **/
	public void incrementCounter(CrawlListHost.CounterId counter, int amount);

	/** is this a blacklisted host **/
	public boolean isBlackListedHost();

	/** is this a failed host */
	public boolean isFailedServer();

	/** called whenever the Active CrawlList's diposition (state) changes */
	public void listDispositionChanged(CrawlList list,CrawlList.Disposition oldDisposition, CrawlList.Disposition newDisposition);

	/** reset given counter variable **/
	public void resetCounter(CrawlListHost.CounterId counter);

	/** update fetch start time for this host **/
	public void updateLastFetchStartTime(long time);
	
	
	/** is host in a paused state **/
	public boolean isPaused();
	
	
	
}