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

package org.commoncrawl.crawl.crawler.listcrawler;


import java.io.File;
import java.io.IOException;
import java.util.TreeSet;

import org.commoncrawl.crawl.crawler.listcrawler.CrawlHistoryManager.ItemUpdater;
import org.commoncrawl.protocol.URLFP;

/**
 * abstraction for the crawl history store
 * 
 * @author rana
 *
 */
public interface CrawlHistoryStorage { 
	
	public void syncList(long listId,TreeSet<URLFP> matchCriteria,ItemUpdater targetList)throws IOException;
	
	public File getLocalDataDir();
	
}