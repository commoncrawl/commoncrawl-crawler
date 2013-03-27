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

import java.io.IOException;
import java.io.Writer;

/**
 * 
 * @author rana
 *
 * interface defining a CrawlList container as seen by the CrawlQueue
 */
public interface CrawlQueueHost extends CrawlHost {

	/** clear wait state on this host **/
	public void clearWaitState();

	/** feed items to the queue **/
	public void feedQueue();

	/** get or create a new CrawlList given list id **/
	public CrawlList getCrawlList(int listId);

	/** get queued url count for this host **/
	public int getQueuedURLCount();

	/** get wait time for this host, if in a wait state **/
	public long getWaitTime();

	/** purge all object references to help garbage collection process */
	public void purgeReferences();
	
	/** returns true if no more active lists are present **/
	public boolean noActiveLists();
	
	
	/** dump debug information **/
	public void dumpDetails(Writer writer) throws IOException;

	
}
