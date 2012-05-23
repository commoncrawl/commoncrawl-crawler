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

package org.commoncrawl.mapred.pipelineV3.domainmeta.blogs.postfrequency;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTaskAsStep;

/**
 * 
 * 
 * @author rana
 * 
 */
public class GenPostFrequencyStep extends CrawlPipelineTaskAsStep {

  public static final String OUTPUT_DIR_NAME = "postFreqCalcultor";

  private static final Log LOG = LogFactory.getLog(GenPostFrequencyStep.class);

  public GenPostFrequencyStep(CrawlPipelineTask parentTask) throws IOException {

    super(parentTask, "Blog Post Frequencey Calculator", OUTPUT_DIR_NAME);

    addStep(new ScanDatabaseStep(this));
    addStep(new AggregateStatsByMonth(this));
    addStep(new GroupByDomainStep(this));
    addStep(new JoinWithGraphDataStep(this));
  }

  @Override
  public Log getLogger() {
    return LOG;
  }

}
