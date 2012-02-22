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

package org.commoncrawl.common;

/**
 * Some global constants
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
