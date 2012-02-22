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

	/** purge all object refrences to help garbage collection process */
	public void purgeReferences();
	
	/** returns true if no more active lists are present **/
	public boolean noActiveLists();
	
	
	/** dump debug information **/
	public void dumpDetails(Writer writer) throws IOException;

	
}
