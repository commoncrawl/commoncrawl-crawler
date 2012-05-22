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

package org.commoncrawl.crawl.common.shared;

/**
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
