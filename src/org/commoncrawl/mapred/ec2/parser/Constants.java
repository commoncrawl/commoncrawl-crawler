package org.commoncrawl.mapred.ec2.parser;

public interface Constants {

  static final String S3N_BUCKET_PREFIX = "s3n://aws-publicdatasets";
  static final String CRAWL_LOG_INTERMEDIATE_PATH = "/common-crawl/crawl-intermediate/";
  
  static final String VALID_SEGMENTS_PATH = "/common-crawl/parse-output/valid_segments2/";
  static final String TEST_VALID_SEGMENTS_PATH = "/common-crawl/parse-output-test/valid_segments/";
  static final String VALID_SEGMENTS_PATH_PROPERTY = "cc.valid.segments.path";
  
  static final String SEGMENTS_PATH = "/common-crawl/parse-output/segment/";
  static final String TEST_SEGMENTS_PATH = "/common-crawl/parse-output-test/segment/";
  static final String SEGMENT_PATH_PROPERTY = "cc.segment.path";

  static final String CHECKPOINT_STAGING_PATH = "/common-crawl/parse-output/checkpoint_staging/";
  static final String TEST_CHECKPOINT_SEGMENTS_PATH = "/common-crawl/parse-output-test/checkpoint_staging/";
  static final String CHECKPOINT_STAGING_PATH_PROPERTY = "cc.checkpoint.staging.path";

  
  static final String CHECKPOINTS_PATH = "/common-crawl/parse-output/checkpoints/";
  static final String TEST_CHECKPOINTS_PATH = "/common-crawl/parse-output-test/checkpoints/";
  static final String CHECKPOIINTS_PATH_PROPERTY = "cc.checkpoint.path";
  
  
  static final String JOB_LOGS_PATH = "/common-crawl/job-logs/";
  static final String TEST_JOB_LOGS_PATH = "/common-crawl/test-job-logs/";
  static final String JOB_LOGS_PATH_PROPERTY = "cc.job.log.path";

  
  static final String SEGMENT_MANIFEST_FILE = "manfiest.txt";
  static final String SPLITS_MANIFEST_FILE = "splits.txt";
  static final String TRAILING_SPLITS_MANIFEST_FILE = "trailing_splits.txt";
  static final String FAILED_SPLITS_MANIFEST_FILE = "failed_splits.txt";
  static final String IS_CHECKPOINT_SEGMENT_FLAG = "is_checkpoint.txt";
  
  
  static final int DEFAULT_PARSER_JOB_SPLIT_SIZE = 134217728 * 4;
  static final int DEFAULT_PARSER_CHECKPOINT_JOB_SPLIT_SIZE = 134217728 / 2;
  static final int DEFAULT_PARSER_CHECKPOINT_SPLITS_PER_JOB = 1000;
  
  static final String CHECKPOINT_JOB_OUTPUT_PATH = "output";
  static final String CHECKPOINT_JOB_LOG_PATH = "logs";
  static final String JOB_SUCCESS_FILE = "_SUCCESS";
  
  public static final String CONF_PARAM_TEST_MODE = "TestMode";
}
