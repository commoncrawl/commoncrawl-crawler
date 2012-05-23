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
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.mapred.database.HitsByMonth;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.TextBytes;

import com.google.common.collect.ImmutableList;

/**
 * Phase 2 Aggregator
 * 
 * @author rana
 * 
 */
public class AggregateStatsByMonth extends CrawlPipelineStep implements
    Reducer<TextBytes, HitsByMonth, TextBytes, HitsByMonth> {

  public static final String OUTPUT_DIR_NAME = "phase-2";

  private static final Log LOG = LogFactory.getLog(AggregateStatsByMonth.class);

  /** default constructor (for mapper) **/
  public AggregateStatsByMonth() {
    super(null, null, null);
  }

  /** step constructor **/
  public AggregateStatsByMonth(CrawlPipelineTask task) throws IOException {
    super(task, task.getDescription() + " - Phase 2", OUTPUT_DIR_NAME);
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public void configure(JobConf job) {

  }

  @Override
  public Log getLogger() {
    return LOG;
  }

  @Override
  public void reduce(TextBytes key, Iterator<HitsByMonth> values, OutputCollector<TextBytes, HitsByMonth> output,
      Reporter reporter) throws IOException {

    Map<HitsByMonth, HitsByMonth> hitCounts = new TreeMap<HitsByMonth, HitsByMonth>();

    while (values.hasNext()) {
      HitsByMonth nextHit = values.next();

      HitsByMonth existingHit = hitCounts.get(nextHit);
      if (existingHit == null) {
        HitsByMonth hit = null;
        try {
          hit = (HitsByMonth) nextHit.clone();
        } catch (CloneNotSupportedException e) {
        }
        hitCounts.put(hit, hit);
      } else {
        existingHit.setHitCount(existingHit.getHitCount() + 1);
        existingHit.setFlags(existingHit.getFlags() | nextHit.getFlags());
        hitCounts.put(existingHit, existingHit);
      }
    }

    for (HitsByMonth hit : hitCounts.values()) {
      output.collect(key, hit);
    }
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {
    LOG.info("Task Identity Path is:" + getTaskIdentityPath());
    LOG.info("Temp Path is:" + outputPathLocation);

    ImmutableList<Path> paths = new ImmutableList.Builder<Path>().add(
        makeUniqueOutputDirPath(_task.getOutputDirForStep(ScanDatabaseStep.OUTPUT_DIR_NAME), getTaskIdentityId()))
        .build();

    JobConf job = new JobBuilder(getDescription() + " - Phase 2", getConf()).inputIsSeqFile().inputs(paths).reducer(
        AggregateStatsByMonth.class, false).keyValue(TextBytes.class, HitsByMonth.class).numReducers(
        CrawlEnvironment.NUM_DB_SHARDS / 2).output(outputPathLocation).outputIsSeqFile().build();

    JobClient.runJob(job);
  }

}