/**
 * Copyright 2012 - CommonCrawl Foundation
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

package org.commoncrawl.mapred.pipelineV3.domainmeta.crawlstats;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTaskAsStep;

/**
 * 
 * @author rana
 *
 */
public class CrawlStatsCollectorTask extends CrawlPipelineTaskAsStep {

  public static final String OUTPUT_DIR_NAME = "crawlStats";

  public static final String SUPER_DOMAIN_FILE_PATH = "super-domain-list";

  private static final Log LOG = LogFactory.getLog(CrawlStatsCollectorTask.class);

  public CrawlStatsCollectorTask(CrawlPipelineTask task) throws IOException {
    super(task, "Crawl Stats Collector", OUTPUT_DIR_NAME);

    addStep(new CrawlDBStatsCollectorStep(this));
    addStep(new CrawlDBRedirectStatsCollectorStep(this));
    addStep(new WWWPrefixStatsCollectorStep(this));
    addStep(new WWWPrefixStatsWriterStep(this));
    addStep(new DNSFailuresCollectorStep(this));
    addStep(new DNSAndCrawlStatsJoinStep(this));
    addStep(new RankAndCrawlStatsJoinStep(this));
    addStep(new NonSuperSubdomainCollectorStep(this));
    addStep(new JoinSubDomainsAndCrawlStatsStep(this));
    addStep(new JoinIPAddressAndCrawlStatsStep(this));
  }

  @Override
  public Log getLogger() {
    return LOG;
  }

  @Override
  protected boolean promoteFinalStepOutput() {
    return true;
  }

}
