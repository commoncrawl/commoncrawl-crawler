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

package org.commoncrawl.mapred.pipelineV3.domainmeta.rank;

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
public class RankTask extends CrawlPipelineTaskAsStep {

  public static final String OUTPUT_DIR_NAME = "domainGraph";

  private static final Log LOG = LogFactory.getLog(RankTask.class);

  public RankTask(CrawlPipelineTask parentTask) throws IOException {
    super(parentTask, "Domain Graph Builder", OUTPUT_DIR_NAME);

    addStep(new LinkScannerStep(this));
    addStep(new DedupedDomainLinksStep(this));
    addStep(new IdSuperDomainsStep(this));
    addStep(new GenSuperDomainListStep(this));
    addStep(new GenDomainRankStep(this));
    addStep(new JoinQuantcastAndDomainRankStep(this));

  }

  @Override
  public Log getLogger() {
    return LOG;
  }

  @Override
  protected boolean promoteFinalStepOutput() {
    return false;
  }

}
