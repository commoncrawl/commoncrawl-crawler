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

package org.commoncrawl.service.parser.ec2;

public interface Constants {
  public static final String Text_SUFFIX = "_Text";
  public static final String Links_SUFFIX = "_Links";
  public static final String Hashes_SUFFIX = "_Hashes";
  public static final String Feed_SUFFIX = "_Feeds";
  public static final String CrawlStatus_SUFFIX = "_CrawlStatus";
  public static final String DONE_SUFFIX = ".DONE";

  public static final String CC_BUCKET_ROOT = "common-crawl";
  public static final String CC_CRAWLLOG_SOURCE = "/crawl-intermediate/";
  public static final String CC_PARSER_INTERMEDIATE = "/parse-intermediate/";
  public static final String CC_PARSER_OUTPUT = "/parse-output/";
}
