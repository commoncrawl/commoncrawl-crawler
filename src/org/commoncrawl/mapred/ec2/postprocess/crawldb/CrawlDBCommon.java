package org.commoncrawl.mapred.ec2.postprocess.crawldb;

public interface CrawlDBCommon {
  
  public static final int NUM_SHARDS = 10000;
  
  public static final String BLEKKO_DOMAIN_METADATA_PATH = "s3n://aws-publicdatasets/common-crawl/blekko/blekko-domain-meta";
  
  public static final String TOPLEVEL_LINKSTATUS_PROPERTY = "link_status";
  public static final String TOPLEVEL_SUMMARYRECORD_PROPRETY = "crawl_status";
  public static final String TOPLEVEL_SOURCE_URL_PROPRETY = "source_url";
  public static final String TOPLEVEL_BLEKKO_METADATA_PROPERTY = "blekko";
  public static final String TOPLEVEL_WIKIPEDIA_METADATA_PROPERTY = "wikipedia";
  
  public static final String RSS_MIN_PUBDATE_PROPERTY = "minPubDate";
  public static final String RSS_MAX_PUBDATE_PROPERTY = "maxPubDate";
  public static final String RSS_ITEM_COUNT_PROPERTY = "itemCount";
  
  public static final String SUMMARYRECORD_ATTEMPT_COUNT_PROPERTY = "attempt_count";
  public static final String SUMMARYRECORD_LATEST_ATTEMPT_PROPERTY = "latest_attempt";
  public static final String SUMMARYRECORD_HTTP_RESULT_PROPERTY = "http_result";
  public static final String SUMMARYRECORD_LATEST_CRAWLTIME_PROPERTY = "latest_crawl";
  public static final String SUMMARYRECORD_CRAWLCOUNT_PROPERTY = "crawl_count";
  public static final String SUMMARYRECORD_PARSEDAS_PROPERTY = "parsed_as";
  public static final String SUMMARYRECORD_CRAWLDETAILS_ARRAY_PROPERTY = "crawl_stats";
  public static final String SUMMARYRECORD_REDIRECT_URL_PROPERTY = "redirect_url";
  public static final String SUMMARYRECORD_EXTERNALLY_REFERENCED_URLS = "ext_urls";
  public static final String SUMMARYRECORD_EXTERNALLY_REFERENCED_URLS_TRUNCATED = "ext_urls_truncated";

  
  public static final String CRAWLDETAIL_FAILURE = "fetch_failed";
  public static final String CRAWLDETAIL_FAILURE_REASON  = "failure_reason";
  public static final String CRAWLDETAIL_FAILURE_DETAIL  = "failure_detail";
  public static final String CRAWLDETAIL_SERVERIP_PROPERTY = "server_ip";
  public static final String CRAWLDETAIL_HTTPRESULT_PROPERTY = "http_result";
  public static final String CRAWLDETAIL_REDIRECT_URL      = "redirect_url";
  public static final String CRAWLDETAIL_CONTENTLEN_PROPERTY = "content_len";
  public static final String CRAWLDETAIL_MIMETYPE_PROPERTY = "mime_type";
  public static final String CRAWLDETAIL_MD5_PROPERTY = "md5";
  public static final String CRAWLDETAIL_TEXTSIMHASH_PROPERTY = "text_simhash";
  public static final String CRAWLDETAIL_PARSEDAS_PROPERTY = "parsed_as";
  public static final String CRAWLDETAIL_TITLE_PROPERTY = "title";
  public static final String CRAWLDETAIL_METATAGS_PROPERTY = "meta_tags";
  public static final String CRAWLDETAIL_UPDATED_PROPERTY = "updated";
  public static final String CRAWLDETAIL_ATTEMPT_TIME_PROPERTY  = "attempt_time";
  public static final String CRAWLDETAIL_INTRADOMAIN_LINKS = "intra_domain_links";
  public static final String CRAWLDETAIL_INTRAROOT_LINKS = "intra_root_links";
  public static final String CRAWLDETAIL_INTERDOMAIN_LINKS = "inter_domain_links";

  public static final String CRAWLDETAIL_HTTP_DATE_PROPERTY = "date";
  public static final String CRAWLDETAIL_HTTP_AGE_PROPERTY = "age";
  public static final String CRAWLDETAIL_HTTP_LAST_MODIFIED_PROPERTY = "last-modified";
  public static final String CRAWLDETAIL_HTTP_EXPIRES_PROPERTY = "expires";
  public static final String CRAWLDETAIL_HTTP_CACHE_CONTROL_PROPERTY = "cache-control";
  public static final String CRAWLDETAIL_HTTP_PRAGMA_PROPERTY = "pragma";
  public static final String CRAWLDETAIL_HTTP_ETAG_PROPERTY = "etag";

  
  public static final String LINKSTATUS_INTRADOMAIN_SOURCES_COUNT_PROPERTY = "int_src_count";
  public static final String LINKSTATUS_EXTRADOMAIN_SOURCES_COUNT_PROPERTY = "ext_src_count";
  public static final String LINKSTATUS_EARLIEST_DATE_PROPERTY = "earliest_date";
  public static final String LINKSTATUS_LATEST_DATE_PROPERTY = "latest_date"; 
  public static final String LINKSTATUS_TYPEANDRELS_PROPERTY = "typeAndRels";

  
  public static final String BLEKKO_METADATA_TIMESTAMP_PROPERTY = "timestmap";
  public static final String BLEKKO_METADATA_RANK = "rank";
  public static final String BLEKKO_METADATA_RANK_10 = "rank10";
  public static final String BLEKKO_METADATA_STATUS = "status";
  public static final String BLEKKO_METADATA_ISPORN = "blekko_is_porn";
  public static final String BLEKKO_METADATA_ISSPAM = "blekko_is_spam";
  public static final String BLEKKO_METADATA_WWW_PREFIX = "blekko_had_www";
  public static final String BLEKKO_METADATA_IP = "blekko_ip";
  
  public static final String WIKIPEDIA_REF_COUNT = "wikipedia_refs";
  
}
