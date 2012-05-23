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

package org.commoncrawl.service.crawler;

import org.commoncrawl.io.NIOHttpConnection;
import org.commoncrawl.protocol.CrawlURL;

/**
 * A callback interface used to notify different layers of the crawl status of a CrawlTarget
 * 
 * @author rana
 *
 */
public interface CrawlItemStatusCallback {
  public void crawlStarting(CrawlTarget target);
  public void crawlComplete(NIOHttpConnection connection,CrawlURL url,CrawlTarget optTargetObj,boolean successOrFailure);
}
