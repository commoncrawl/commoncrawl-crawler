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
package org.commoncrawl.service.listcrawler;


import java.io.File;
import java.io.IOException;
import java.util.TreeSet;

import org.commoncrawl.protocol.URLFP;
import org.commoncrawl.service.listcrawler.CrawlHistoryManager.ItemUpdater;

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