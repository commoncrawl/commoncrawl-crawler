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

package org.commoncrawl.service.crawler;

import org.commoncrawl.service.crawler.RobotRulesParser.RobotRuleSet;
import org.commoncrawl.util.HttpCookieUtils.CookieStore;

/**
 * @author rana
 * 
 * Further specialization of the CrawlHost class 
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