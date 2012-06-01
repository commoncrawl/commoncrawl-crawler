package org.commoncrawl.mapred.pipelineV1;

import java.io.IOException;

public abstract class CrawlDBCustomJob {

  abstract public String getJobDescription();
  abstract public void runJob() throws IOException;

}
