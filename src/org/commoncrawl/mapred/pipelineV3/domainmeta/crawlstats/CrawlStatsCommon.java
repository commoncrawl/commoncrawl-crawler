package org.commoncrawl.mapred.pipelineV3.domainmeta.crawlstats;

public interface CrawlStatsCommon {
  
  public static final String JOINEDMETDATA_PROPERTY_BELKKO = "j_belkko";
  public static final String JOINEDMETDATA_PROPERTY_RANK  = "j_rank";
  public static final String JOINEDMETDATA_PROPERTY_CRAWLSTATS = "j_stats";
  
  public static final String CRAWLSTATS_URL_COUNT = "urls";
  public static final String CRAWLSTATS_ATTEMPTED_COUNT = "attempted";
  public static final String CRAWLSTATS_CRAWLED_COUNT = "crawled";
  public static final String CRAWLSTATS_REDIRECTED_COUNT = "redirected";
  public static final String CRAWLSTATS_REDIRECTED_OUT_COUNT = "redirected_out";
  public static final String CRAWLSTATS_WWW_TO_NON_WWW_REDIRECT = "www_to_non";
  public static final String CRAWLSTATS_NON_WWW_TO_WWW_REDIRECT = "non_to_www";
  public static final String CRAWLSTATS_EXTERNALLY_LINKED_URLS = "ext_linked";
  public static final String CRAWLSTATS_EXTERNALLY_LINKED_NOT_CRAWLED_URLS = "ext_not_crld";
  public static final String CRAWLSTATS_BLEKKO_URL = "blekko_urls";
  public static final String CRAWLSTATS_BLEKKO_CRAWLED_COUNT = "blekko_crld";
  public static final String CRAWLSTATS_BLEKKO_AND_CC_CRAWLED_COUNT = "blekko_cc_crld";
  public static final String CRAWLSTATS_BLEKKO_URL_NOT_IN_CC= "blekko_not_cc";
  public static final String CRAWLSTATS_BLEKKO_URL_HAD_GT_1_RANK = "blekko_gt1_rank";
  public static final String CRAWLSTATS_IPS = "ips";
  
  public static final String ROOTDOMAIN_STATS_IS_SUPERDOMAIN = "is_super_domain";
  public static final String ROOTDOMAIN_STATS_SUBDOMAIN_COUNT = "subdomain_count";
  public static final String ROOTDOMAIN_STATS_TOTAL_URLS = "total_urls";
  public static final String ROOTDOMAIN_STATS_MEAN_URLS_PER_DOMAIN = "urls_per_domain";
  public static final String ROOTDOMAIN_STATS_MAX_URLS_IN_A_DOMAIN = "max_urls_in_a_domain";
  //public static final String ROOTDOMAIN_STATS_URLS_PER_DOMAIN_90TH = "urls_per_domain_90th";
  public static final String ROOTDOMAIN_STATS_URLS_PER_DOMAIN_STDDEV = "urls_per_domain-stddev";
  public static final String ROOTDOMAIN_STATS_BLEKKO_TOTAL_URLS = "blekko_total_urls";
  public static final String ROOTDOMAIN_STATS_BLEKKO_MEAN_URLS_PER_DOMAIN = "blekko_urls_per_domain";
  public static final String ROOTDOMAIN_STATS_BLEKKO_RANK_MEAN = "blekko_rank";
  public static final String ROOTDOMAIN_STATS_BLEKKO_RANK_MAX = "blekko_rank_max";
  public static final String ROOTDOMAIN_STATS_BLEKKO_RANK_TOTAL = "blekko_rank_total";
  //public static final String ROOTDOMAIN_STATS_BLEKKO_RANK_90TH = "blekko_rank_90th";
  public static final String ROOTDOMAIN_STATS_DOMAIN_RANK_MEAN = "dR";
  public static final String ROOTDOMAIN_STATS_DOMAIN_RANK_MAX = "dR_max";
  public static final String ROOTDOMAIN_STATS_DOMAIN_RANK_TOTAL = "dR_total";
  //public static final String ROOTDOMAIN_STATS_DOMAIN_RANK_90TH = "dR_90th";
  public static final String ROOTDOMAIN_STATS_DOMAIN_RANK_STDDEV = "dR-stddev";
  public static final String ROOTDOMAIN_STATS_BLEKKO_IS_PORN = "is_porn";
  public static final String ROOTDOMAIN_STATS_BLEKKO_IS_SPAM = "is_spam";
  public static final String ROOTDOMAIN_STATS_BLEKKO_MEAN_CRAWL_COUNT = "blekko_crawl_count_per_domain";
  public static final String ROOTDOMAIN_STATS_BLEKKO_TOTAL_CRAWL_COUNT = "blekko_total_crawl_count";
  public static final String ROOTDOMAIN_STATS_BLEKKO_MEAN_PR_COUNT = "blekko_pr_count_per_domain";
  public static final String ROOTDOMAIN_STATS_BLEKKO_MAX_PR_COUNT = "blekko_pr_count_per_domain_max";
  public static final String ROOTDOMAIN_STATS_BLEKKO_TOTAL_PR_COUNT = "blekko_pr_count_per_domain_total";
  public static final String ROOTDOMAIN_STATS_MEAN_EXT_LINKED_URLS = "ext_linked_urls_per_domain";
  public static final String ROOTDOMAIN_STATS_MAX_EXT_LINKED_URLS = "ext_linked_urls_per_domain_max";
  public static final String ROOTDOMAIN_STATS_TOTAL_EXT_LINKED_URLS = "ext_linked_urls_per_domain_total";
  //public static final String ROOTDOMAIN_STATS_EXT_LINKED_URLS_90TH = "ext_linked_urls_per_domain_90th";
  public static final String ROOTDOMAIN_STATS_EXT_LINKED_URLS_STDDEV = "ext_linked_urls_per_domain-stddev";
  
  public static final String ROOTDOMAIN_CLASSIFY_SUPERDOMAIN = "c_super";
  public static final String ROOTDOMAIN_CLASSIFY_LIMITED_CRAWL = "c_limited";
  public static final String ROOTDOMAIN_CLASSIFY_BLACKLISTED = "c_blisted";
  public static final String ROOTDOMAIN_CLASSIFY_RELATIVE_RANK = "c_rr";
  
}
