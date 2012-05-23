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
import org.commoncrawl.mapred.database.PostFrequencyInfo;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.util.GoogleURL;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.TextBytes;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonObject;

/**
 * 
 * @author rana
 * 
 */
public class GroupByDomainStep extends CrawlPipelineStep implements
    Reducer<TextBytes, HitsByMonth, TextBytes, TextBytes> {

  public static final String OUTPUT_DIR_NAME = "groupedByDomain";

  private static final Log LOG = LogFactory.getLog(GroupByDomainStep.class);

  /** default constructor (for mapper) **/
  public GroupByDomainStep() {
    super(null, null, null);
  }

  /** step constructor **/
  public GroupByDomainStep(CrawlPipelineTask task) throws IOException {
    super(task, task.getDescription() + " - Phase 3", OUTPUT_DIR_NAME);
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void configure(JobConf job) {
    // TODO Auto-generated method stub

  }

  @Override
  public Log getLogger() {
    return LOG;
  }

  @Override
  public void reduce(TextBytes key, Iterator<HitsByMonth> values, OutputCollector<TextBytes, TextBytes> output,
      Reporter reporter) throws IOException {
    int min = 0;
    int max = 0;
    int total = 0;
    int count = 0;
    int flags = 0;
    int lastYearWithPost = 0;

    while (values.hasNext()) {
      HitsByMonth hit = values.next();
      flags |= hit.getFlags();
      if (hit.getHitCount() != 0) {
        total += hit.getHitCount();
        lastYearWithPost = Math.max(lastYearWithPost, hit.getYear());
        count++;
        if (min == 0 || min > hit.getHitCount()) {
          min = hit.getHitCount();
        }
        if (max == 0 || hit.getHitCount() > max) {
          max = hit.getHitCount();
        }
      }
    }
    // int average = (int)((float)total / (float)count);

    GoogleURL urlObject = new GoogleURL(key.toString());

    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty("url", key.toString());
    jsonObject.addProperty("avg", ((double) total / (double) count));
    jsonObject.addProperty("min", min);
    jsonObject.addProperty("max", max);
    jsonObject.addProperty("count", count);
    if ((flags & PostFrequencyInfo.Flags.HAS_INDEX_HTML_AFTER_DATE) != 0) {
      jsonObject.addProperty("hasIndexHTML", true);
    }
    if ((flags & PostFrequencyInfo.Flags.HAS_YEAR_MONTH_SLASH_INDEX) != 0) {
      jsonObject.addProperty("hasYearMonthIndex", true);
    }
    if ((flags & PostFrequencyInfo.Flags.FLAG_GENERATOR_IS_BLOGGER) != 0) {
      jsonObject.addProperty("blogger", true);
    }
    if ((flags & PostFrequencyInfo.Flags.FLAG_GENERATOR_IS_WORDPRESS) != 0) {
      jsonObject.addProperty("wordpress", true);
    }
    if ((flags & PostFrequencyInfo.Flags.FLAG_GENERATOR_IS_TYPEPAD) != 0) {
      jsonObject.addProperty("typepad", true);
    }
    jsonObject.addProperty("lastYearWithPost", lastYearWithPost);

    output.collect(new TextBytes(urlObject.getHost()), new TextBytes(jsonObject.toString()));
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {
    LOG.info("Task Identity Path is:" + getTaskIdentityPath());
    LOG.info("Temp Path is:" + outputPathLocation);

    ImmutableList<Path> paths = new ImmutableList.Builder<Path>().add(
        makeUniqueOutputDirPath(_task.getOutputDirForStep(AggregateStatsByMonth.OUTPUT_DIR_NAME), getTaskIdentityId()))
        .build();

    JobConf job = new JobBuilder(getDescription() + " - Phase 3", getConf()).inputIsSeqFile().inputs(paths).reducer(
        GroupByDomainStep.class, false).mapperKeyValue(TextBytes.class, HitsByMonth.class).outputKeyValue(
        TextBytes.class, TextBytes.class).numReducers(CrawlEnvironment.NUM_DB_SHARDS / 2).output(outputPathLocation)
        .outputIsSeqFile().build();

    JobClient.runJob(job);
  }
}