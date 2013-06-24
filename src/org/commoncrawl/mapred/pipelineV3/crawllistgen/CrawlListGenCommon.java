package org.commoncrawl.mapred.pipelineV3.crawllistgen;

public interface CrawlListGenCommon {
  public static final int NUM_LIST_PARTITIONS = 1000;
  
  public static final String CRAWLLIST_METADATA_LAST_MODIFIED_TIME = "last_mod";
  public static final String CRAWLLIST_METADATA_ETAG = "etag";
  
}
