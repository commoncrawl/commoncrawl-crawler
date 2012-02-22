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

package org.commoncrawl.crawl.common.shared;

/**
 * ARC File related headers
 * 
 * @author rana
 *
 */
public interface Constants {

  /** arc file header **/
  public static final String ARCFileHeader_ParseSegmentId   = "x_commoncrawl_ParseSegmentId";
  public static final String ARCFileHeader_OriginalURL      = "x_commoncrawl_OriginalURL";
  public static final String ARCFileHeader_URLFP            = "x_commoncrawl_URLFP";
  public static final String ARCFileHeader_HostFP           = "x_commoncrawl_HostFP";
  public static final String ARCFileHeader_Signature        = "x_commoncrawl_Signature";
  public static final String ARCFileHeader_CrawlNumber      = "x_commoncrawl_CrawlNo";
  public static final String ARCFileHeader_CrawlerId        = "x_commoncrawl_CrawlerId";
  public static final String ARCFileHeader_FetchTimeStamp   = "x_commoncrawl_FetchTimestamp";
  public static final String ARCFileHeader_ContentTruncated = "x-commoncrawl-ContentTruncated";
  public static final String ARCFileHeader_SOURCE_IS_GZIPED = "x_commoncrawl_SourceIsGZIP";

}
