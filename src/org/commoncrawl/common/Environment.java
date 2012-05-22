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

package org.commoncrawl.common;

/**
 * 
 * @author rana
 *
 */
public class Environment {
  
  public static final int CommonServerStats_NAMESPACE_ID = 1;
  public static final int CrawlerStats_NAMESPACE_ID = 2;

  public static boolean detailLoggingEnabled = false;
  
  public static final boolean detailLogEnabled() { 
    return detailLoggingEnabled;
  }
  
  public static final void enableDetailLog(boolean enableDisable) { 
    detailLoggingEnabled = enableDisable;
  }
  
  public static final String HDFS_LOGCOLLECTOR_BASEDIR = "crawl/logs"; 
}
