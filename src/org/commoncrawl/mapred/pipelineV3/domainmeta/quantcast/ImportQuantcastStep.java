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
package org.commoncrawl.mapred.pipelineV3.domainmeta.quantcast;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.TextBytes;

/**
 * 
 * @author rana
 *
 */
public class ImportQuantcastStep extends CrawlPipelineStep {

  private static final Log LOG = LogFactory.getLog(ImportQuantcastStep.class);

  public static final String OUTPUT_DIR_NAME = "quantcastData";

  public ImportQuantcastStep(CrawlPipelineTask task) throws IOException {
    super(task, "ImportQuantcast Data", OUTPUT_DIR_NAME);
  }

  @Override
  public Log getLogger() {
    return LOG;
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {
    JobConf jobConf = new JobBuilder("Import Quantcast Data", getConf()).input(
        new Path("crawl/static_data/quantcast_top_1M.txt")).inputFormat(TextInputFormat.class).mapper(
        QuntcastDataMapper.class).keyValue(TextBytes.class, IntWritable.class).output(outputPathLocation)
        .outputIsSeqFile().numReducers(10).build();

    LOG.info("Running Step");
    JobClient.runJob(jobConf);
    LOG.info("Finished Running Step");
  }

}
