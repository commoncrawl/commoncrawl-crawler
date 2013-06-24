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

package org.commoncrawl.crawl.common.internal;

import java.io.IOException;
import java.net.URI;
import java.text.NumberFormat;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.commoncrawl.common.Environment;
import org.mortbay.log.Log;

import com.google.common.collect.Lists;

public final class CrawlEnvironment extends Environment {

  
  public static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }    
  
  public static final int      MAX_URL_LENGTH_ALLOWED                  = 2048;

  /** ccbot user agent string **/
  public static final String   CCBOT_UA                                = "ccbot";

  public static final int      MIN_DNS_CACHE_TIME                      = 60 * 60 * 1000;

  public static final int NUM_CRAWLERS = 32;

  
  /** OLD crawler LIST  **/
  
  public static final String[] CRAWLERS                                = {
      "ccc01-01", "ccc01-02", "ccc02-01", "ccc02-02", "ccc03-01", "ccc03-02",
      "ccc04-01", "ccc04-02",                                         };
  

  /** crawl datum metadata **/
  public static final String   MetaData_FailureReason                  = "_cc_failCode_";
  public static final String   MetaData_DatumSource                    = "_cc_dsrc_";
  public static final String   MetaData_CrawlURLMetadata               = "_cc_md_";
  public static final Text     CrawlURLMetadataKey                     = new Text(
                                                                           MetaData_CrawlURLMetadata);

  /** map reduce property names **/
  public static final String   PROPERTY_CRAWL_NUMBER                   = "crawldbserver.crawl.number";
  public static final String   PROPERTY_CRAWERLDB_SEGMENT_PATH         = "crawldbserver.segment.path";
  public static final String   PROPERTY_CRAWERLDB_SEGMENT_ID           = "crawldbserver.segment.id";
  public static final String   PROPERTY_CRAWLERS                       = "crawldbserver.crawlers";
  public static final String   PROPERTY_NUM_CRAWLERS                   = "crawldbserver.number.of.crawlers";
  public static final String   PROPERTY_NUM_BUCKETS_PER_CRAWLER        = "crawldbserver.number.of.buckets.per.crawlers";
  public static final String   PROPERTY_START_SEGMENT_ID               = "crawldbserver.start.segment.id";
  public static final String   PROPERTY_MAX_URLS_PER_SEGMENT           = "crawldbserver.max.urls.per.segment";
  public static final String   PROPERTY_SEGMENT_OUTPUT_TEMP_DIR        = "crawldbserver.segment.output.temp.dir";
  public static final String   PROPERTY_SEGMENT_LIST_ID                = "crawldbserver.segment.list.id";

  /** hdfs paths **/
  public static       String   CC_ROOT_DIR                             = "/crawl";

  public static final String   HDFS_LinkDBDir                          = "linkdb";
  public static final String   HDFS_MergedLinkDB                       = "current";

  public static final String   HDFS_OldLinkDBDir                       = "old_linkDB";
  public static final String   HDFS_InverseLinkDBDir                   = "inverse_linkdb";
  public static final String   HDFS_InverseLinkDBCurrent               = "current";
  public static final String   HDFS_PageRankDBDir                      = "pageRank";

  public static final String   HDFS_PageRankDir                        = "pagerank";
  public static final String   HDFS_PageRankDB                         = "pagerank_db";
  public static final String   HDFS_PRSeedValues                       = "values";
  public static final String   HDFS_InternalRankPRSeedValues           = "InternalRankValues";
  public static final String   HDFS_PRDistributionEdges                = "edges";
  public static final String   HDFS_PRDistributionInternalEdges        = "internal-edges";

  public static final int      PR_NUMSLAVES                            = 46;

  // number of shards to use when creating various system databases...
  public static final int      NUM_DB_SHARDS                           = 96;

  public static final String   HDFS_HeaderDB                           = "header_db";
  public static final String   HDFS_UploadCandidateDB                  = "uploadCandidates_db";
  public static final String   HDFS_ArcFileHistoryDB                   = "arcFileHistory_db";
  public static final String   HDFS_PurgeCandidateDB                   = "purge_db";
  public static final String   HDFS_DomainDB                           = "domain_db";
  public static final String   HDFS_SuperDomainDB                      = "super_domain_db";
  public static final String   HDFS_SpamListsDB                        = "spam_lists";
  public static final String   HDFS_SuperDomainTLDList                 = "top_level_domain_db";

  public static final String   HDFS_HistoryServerBase                  = "/crawl/history";
  public static final String   HDFS_HistoryServerCheckpointMutex       = "checkpointMutex";

  public static final String   ActiveCrawlLog                          = "ActiveCrawlLog";
  public static final String   CheckpointCrawlLog                      = "CheckpointCrawlLog";

  public static final String   ActiveSegmentLog                        = "ActiveSegmentLog";
  public static final String   CheckpointSegmentLog                    = "SegmentLog";
  public static final String   SegmentCompletionLog                    = "CompletionLog";

  private static final String   HDFS_CrawlDBDirectory                   = "crawldb";
  
  private static final String   HDFS_CrawlSegmentsDataDirectory         = "crawl_segments";
  
  private static final String   HDFS_CrawlSegmentLogsDirectory          = "crawl_segment_logs";
  
  private static final String   HDFS_ParseCandidateSegmentsDirectory    = "parse_segments";
  
  private static final String   HDFS_CheckpointDataDirectory            = "checkpoint_data";
  
  private static final String   HDFS_CheckpointStagingDirectory         = "checkpoint_staging";
  
  private static final String   HDFS_RobotsDBDirectory                  =  "robotsDB";

  private static final String   HDFS_StatsDirectory                     =  "stats";
  
  public static final String   HDFS_CrawlSegmentsFileName               = "crawlSegmentStats";

  // lists
  
  public static final String   ROOT_SUPER_DOMAIN_PATH                  = "/lists/super_domain_list"; // (UNUSED IN PROD)
  public static final String   BLOCKED_DOMAIN_LIST                     = "/lists/blocked_doman_list"; // (CRAWLER)
  public static final String   TEMPORARILY_BLOCKED_DOMAIN_LIST         = "/lists/temporary_blocked_doman_list"; // (CRAWLER)
  public static final String   IP_BLOCK_LIST                           = "/lists/ip_block_list"; // (CRAWLER)

  public static final String   CRAWL_RATE_MOD_FILTER_PATH              = "/lists/crawl_rate_override"; // (CRAWLER)
  public static final String   PROXY_CRAWL_RATE_MOD_FILTER_PATH        = "/lists/proxy/crawl_rate_override"; // CCPROXY
  public static final String   PROXY_URL_BLOCK_LIST_FILTER_PATH        = "/lists/proxy/url_block_list"; // CCPROXY

  public static final String   DNS_REWRITE_RULES                       = "/lists/dns_rewrite_rules"; // (CRAWLER,DNSSERVICE)
  public static final String   DNS_NOCACHE_RULES                       = "/lists/dns_nocache_rules"; // (DNSSERVICE)


  /** local paths **/
  public static final String   SegmentLocalDirectory                   = "segments";

  /** crawler paths **/
  public static final String   CrawlerResultPath                       = "crawl";

  // defaults shared between the servers ...
  public static final String   DEFAULT_DATA_DIR                        = "./data";
  public static final String   DEFAULT_RPC_INTERFACE                   = "localhost";
  public static final String   DEFAULT_HTTP_INTERFACE                  = "localhost";

  // master specific defaults ...
  public static final String   MASTER_DB                               = "master_state.db";
  public static final int      DEFAULT_MASTER_RPC_PORT                 = 8020;
  public static final int      DEFAULT_MASTER_HTTP_PORT                = 8021;
  public static final String   MASTER_WEBAPP_NAME                      = "master";
  // crawler history server defaults ...
  public static final String   CRAWLER_HISTORY_DB                      = "crawler_history_state.db";
  public static final int      DEFAULT_CRAWLER_HISTORY_RPC_PORT        = 8032;
  public static final int      DEFAULT_CRAWLER_HISTORY_HTTP_PORT       = 8033;
  public static final String   CRAWLER_HISTORY_WEBAPP_NAME             = "crawler_history";

  // crawler specific defaults ...
  public static final String   CRAWLER_DB                              = "crawler_state.db";
  public static final int      DEFAULT_CRAWLER_RPC_PORT                = 8010;
  public static final int      DEFAULT_CRAWLER_HTTP_PORT               = 8011;
  public static final String   CRAWLER_WEBAPP_NAME                     = "crawler";
  // database specific defaults ...
  public static final String   CRAWLDB_DB                              = "crawldb_state.db";
  public static final int      DEFAULT_DATABASE_RPC_PORT               = 8030;
  public static final int      DEFAULT_DATABASE_HTTP_PORT              = 8031;
  public static final String   CRAWLMASTER_WEBAPP_NAME                 = "crawlmaster";
  // query master specific defaults ...
  public static final String   QMASTER_DB                              = "qmaster_state.db";
  public static final int      DEFAULT_QUERY_MASTER_RPC_PORT           = 8040;
  public static final int      DEFAULT_QUERY_MASTER_HTTP_PORT          = 8041;
  public static final String   QUERY_MASTER_WEBAPP_NAME                = "qmaster";
  // query slave specific defaults ...
  public static final int      DEFAULT_QUERY_SLAVE_RPC_PORT            = 8070;
  public static final int      DEFAULT_QUERY_SLAVE_HTTP_PORT           = 8071;
  public static final String   QUERY_SLAVE_WEBAPP_NAME                 = "qslave";
  // pagerank master specific defaults ...
  public static final String   PRMASTER_DB                             = "prmaster_state.db";
  public static final int      DEFAULT_PAGERANK_MASTER_RPC_PORT        = 8050;
  public static final int      DEFAULT_PAGERANK_MASTER_HTTP_PORT       = 8051;
  public static final String   PAGERANK_MASTER_WEBAPP_NAME             = "prmaster";
  // pagerank slave specific defaults ...
  public static final int      DEFAULT_PAGERANK_SLAVE_RPC_PORT         = 8060;
  public static final int      DEFAULT_PAGERANK_SLAVE_HTTP_PORT        = 8061;
  public static final String   PAGERANK_SLAVE_WEBAPP_NAME              = "prslave";
  // directory service specific defaults
  public static final String   DIRECTORY_SERVICE_DB                    = "directory_service_state.db";
  public static final int      DIRECTORY_SERVICE_RPC_PORT              = 8052;
  public static final int      DIRECTORY_SERVICE_HTTP_PORT             = 8053;
  public static final String   DIRECTORY_SERVICE_WEBAPP_NAME           = "dservice";
  public static final String   DIRECTORY_SERVICE_HDFS_ROOT             = "dservice_root";
  public static final String   DIRECTORY_SERVICE_ADDRESS_PROPERTY      = "directory.service.address";

  public static final String   DNS_SERVICE_DB                          = "dnsservice.db";
  public static final int      DNS_SERVICE_RPC_PORT                    = 8054;
  public static final int      DNS_SERVICE_HTTP_PORT                   = 8055;
  public static final String   DNS_SERVICE_WEBAPP_NAME                 = "dnsservice";

  public static final String   CRAWLER_TEST_SERVICE_DB                 = "crawlerTestProxy.db";
  public static final int      CRAWLER_TEST_SERVICE_RPC_PORT           = 8056;
  public static final int      CRAWLER_TEST_SERVICE_HTTP_PORT          = 8057;

  // directory service specific defaults
  public static final String   STATS_SERVICE_DB                        = "stats_service_state.db";
  public static final int      STATS_SERVICE_RPC_PORT                  = 8058;
  public static final int      STATS_SERVICE_HTTP_PORT                 = 8059;
  public static final String   STATS_SERVICE_WEBAPP_NAME               = "statsservice";
  public static final String   STATS_SERVICE_HDFS_ROOT                 = "stats_service";

  // directory service specific defaults
  public static final String   CRAWLSTATSCOLLECTOR_SERVICE_DB          = "crawlstats_service_state.db";
  public static final int      CRAWLSTATSCOLLECTOR_SERVICE_RPC_PORT    = 8042;
  public static final int      CRAWLSTATSCOLLECTOR_SERVICE_HTTP_PORT   = 8043;
  public static final String   CRAWLSTATSCOLLECTOR_SERVICE_WEBAPP_NAME = "crawlstats";

  public static final String   PROXY_SERVICE_DB                        = "proxyServer.db";
  public static final int      PROXY_SERVICE_RPC_PORT                  = 8022;
  public static final int      PROXY_SERVICE_HTTP_PORT                 = 8023;

  // parser slave specific defaults ...
  public static final int      DEFAULT_PARSER_SLAVE_RPC_PORT           = 8072;
  public static final int      DEFAULT_PARSER_SLAVE_HTTP_PORT          = 8073;
  public static final String   DEFAULT_PARSER_SLAVE_WEBAPP_NAME        = "pslave";

  // ec2 master specific defaults ...
  public static final int      DEFAULT_EC2MASTER_RPC_PORT              = 8074;
  public static final int      DEFAULT_EC2MASTER_HTTP_PORT             = 8075;
  public static final String   DEFAULT_EC2MASTER_WEBAPP_NAME           = "pslave";

  private static boolean       _unitTestMode                           = false;
  private static String        _defaultHadoopFS                        = null;
  private static Configuration _hadoopConfig                           = null;
  private static String        _crawlSegmentDataDirectory              = null;
  private static String        _crawlSegmentLogsDirectory              = null;
  private static String        _parseCandidateSegmentDataDirectory     = null;
  private static String        _checkpointDataDirectory                = null;
  private static String        _checkpointStagingDirectory             = null;

  private static String        CRAWL_LOG_CHECKPOINT_PREFIX             = "CrawlLog_";

  /** limits **/
  public static final int      ORIGINAL_CONTENT_SIZE_LIMIT             = 2 << 16;                                                                    // 131072
  public static final int      CONTENT_SIZE_LIMIT                      = 2 << 20;                                                                    // 2097152
  public static final int      GUNZIP_SIZE_LIMIT                       = CONTENT_SIZE_LIMIT * 3;

  public static void setUnitTestMode(boolean unitTestMode) {
    _unitTestMode = unitTestMode;
  }

  public static boolean inUnitTestMode() {
    return _unitTestMode;
  }

  public static void setHadoopConfig(Configuration config) {
    _hadoopConfig = config;
  }

  public static Configuration getHadoopConfig() {
    return _hadoopConfig;
  }

  public static FileSystem getDefaultFileSystem() throws IOException {
    // if an override was not specified... get the default file system via
    // hadoop-site.xml
    if (getDefaultHadoopFSURI() == null) {
      return FileSystem.get(getHadoopConfig());
    }
    // otherwise ...
    else {
      return FileSystem.get(URI.create(getDefaultHadoopFSURI()),
          getHadoopConfig());
    }
  }

  public static void setCCRootDir(String directory) { 
    CC_ROOT_DIR = directory;
  }
  
  public static String getCrawlDBDirectory() {
    return CC_ROOT_DIR + "/" + HDFS_CrawlDBDirectory;
  }

  public static String getCrawlSegmentDataDirectory() {
    if (_crawlSegmentDataDirectory == null) { 
      return CC_ROOT_DIR + "/" + HDFS_CrawlSegmentsDataDirectory;
    }
    return _crawlSegmentDataDirectory;
  }

  public static void setCrawlSegmentDataDirectory(String directory) {
    _crawlSegmentDataDirectory = directory;
  }

  public static String getCrawlSegmentLogsDirectory() {
    if (_crawlSegmentLogsDirectory == null) { 
      return CC_ROOT_DIR + "/" + HDFS_CrawlSegmentLogsDirectory;
    }
    return _crawlSegmentLogsDirectory;
  }
  
  public static void setCrawlSegmentLogsDirectory(String directory) { 
    _crawlSegmentLogsDirectory = directory;
  }
  
  public static String getParseCandidateSegmentDataDirectory() {
    if (_parseCandidateSegmentDataDirectory == null) { 
      return CC_ROOT_DIR + "/" + HDFS_ParseCandidateSegmentsDirectory;
    }
    return _parseCandidateSegmentDataDirectory;
  }

  public static String buildCrawlLogCheckpointName(String nodeName,
      long checkpointId) {
    return CRAWL_LOG_CHECKPOINT_PREFIX + nodeName + "_" + checkpointId;
  }

  public static String buildCrawlLogCheckpointWildcardString() {
    return "*";
  }

  public static String buildCrawlSegmentLogCheckpointFileName(long checkpointId) {
    return CheckpointSegmentLog + "_" + checkpointId;
  }

  public static String buildCrawlSegmentCompletionLogFileName(String nodeName) {
    return SegmentCompletionLog + "_" + nodeName;
  }

  public static Path getRemoteCrawlSegmentLogWildcardPath(Path rootPath, String hostId) { 
    Path relativePath 
    = new Path(
        hostId + "/" 
            + "*" + "/" 
            + "*" + "/" 
            + "SegmentLog_*");
    
    return new Path(rootPath,relativePath);
  }
  
  public static Path getRemoteCrawlSegmentLogCheckpointPath(Path rootPath, String hostId,long checkpointId, int listId, int segmentId) throws IOException {
    Path relativePath 
      = new Path(
          hostId + "/" 
              + CrawlEnvironment.formatListId(listId) + "/" 
              + segmentId + "/" 
              + CrawlEnvironment.buildCrawlSegmentLogCheckpointFileName(checkpointId));
    
    return new Path(rootPath,relativePath); 
  }
  

  public static void setParseSegmentDataDirectory(String directory) {
    _parseCandidateSegmentDataDirectory = directory;
  }

  public static String getCheckpointDataDirectory() {
    if (_checkpointDataDirectory == null) { 
      return CC_ROOT_DIR + "/" + HDFS_CheckpointDataDirectory;
    }
    return _checkpointDataDirectory;
  }

  public void setCheckpointDataDirectory(String checkpointDataDirectory) {
    _checkpointDataDirectory = checkpointDataDirectory;
  }

  public static String getCheckpointStagingDirectory() {
    if (_checkpointStagingDirectory == null) { 
      return CC_ROOT_DIR + "/" + HDFS_CheckpointStagingDirectory;
    }
    return _checkpointStagingDirectory;
  }

  public void setCheckpointStagingDirectory(String checkpointStagingDirectory) {
    _checkpointStagingDirectory = checkpointStagingDirectory;
  }

  public static void setDefaultHadoopFSURI(String hadoopFSURI) {
    _defaultHadoopFS = hadoopFSURI;
  }

  public static String getDefaultHadoopFSURI() {
    return _defaultHadoopFS;
  }

  public static String getCrawlerLocalOutputPath() {
    if (CrawlEnvironment.inUnitTestMode()) {
      return "unitTest_" + CrawlEnvironment.CrawlerResultPath;
    } else {
      return CrawlEnvironment.CrawlerResultPath;
    }
  }

  public static int getCurrentCrawlNumber() {
    return 2;
  }
  
  public static String getCrawlerNameGivenId(int hostId) { 
    return NUMBER_FORMAT.format(hostId);
  }
  
  public static String formatListId(int listId) { 
    return NUMBER_FORMAT.format(listId);
  }
  
  public static ArrayList<String> getCrawlerNames() { 
    ArrayList<String> crawlers = Lists.newArrayList();
    for (int i=0;i<NUM_CRAWLERS;++i) { 
      crawlers.add(NUMBER_FORMAT.format(i));
    }
    return crawlers;
  }

}
