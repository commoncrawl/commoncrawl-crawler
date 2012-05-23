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
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.DomainMetadataTask;
import org.commoncrawl.util.GoogleURL;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLUtils;

import com.google.gson.JsonObject;

/**
 * 
 * @author rana
 *
 */
public class WWWPrefixStatsCollectorStep extends CrawlPipelineStep implements
    Mapper<TextBytes, TextBytes, TextBytes, IntWritable>, Reducer<TextBytes, IntWritable, TextBytes, TextBytes> {

  public static final String OUTPUT_DIR_NAME = "wwwPrefixStats";

  private static final Log LOG = LogFactory.getLog(WWWPrefixStatsCollectorStep.class);

  IntWritable negOne = new IntWritable(-1);

  IntWritable plusOne = new IntWritable(1);

  TextBytes keyOut = new TextBytes();

  Pattern wwwMatchPattern = Pattern.compile("www[\\-0-9]*\\.");

  JsonObject hitJSON = new JsonObject();
  TextBytes valueJSON = new TextBytes();

  public WWWPrefixStatsCollectorStep() {
    super(null, null, null);
  }

  public WWWPrefixStatsCollectorStep(CrawlPipelineTask task) {
    super(task, "WWW Prefix Id Step", OUTPUT_DIR_NAME);
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
  public void map(TextBytes key, TextBytes value, OutputCollector<TextBytes, IntWritable> output, Reporter reporter)
      throws IOException {
    String url = key.toString();

    GoogleURL urlObject = new GoogleURL(url);

    if (urlObject.isValid()) {
      String host = urlObject.getHost();

      // extract root domain
      String rootDomain = URLUtils.extractRootDomainName(host);
      if (rootDomain != null) {
        // straight match ??
        if (rootDomain.equals(host)) {
          keyOut.set(rootDomain);
          output.collect(keyOut, negOne);
        } else {
          // extract prefix ...
          String prefix = host.substring(0, host.length() - rootDomain.length());
          // straight match ...
          if (prefix.equals("www.")) {
            keyOut.set(rootDomain);
            output.collect(keyOut, plusOne);
          } else if (prefix.startsWith("www") && wwwMatchPattern.matcher(prefix).matches()) {
            output.collect(keyOut, plusOne);
          }
        }
      }
    }
  }

  @Override
  public void reduce(TextBytes key, Iterator<IntWritable> values, OutputCollector<TextBytes, TextBytes> output,
      Reporter reporter) throws IOException {
    int wwwHits = 0;
    int nonWWWHits = 0;

    while (values.hasNext()) {
      int value = values.next().get();
      if (value < 0)
        nonWWWHits++;
      else
        wwwHits++;
    }

    hitJSON.addProperty("www", wwwHits);
    hitJSON.addProperty("nonWWW", nonWWWHits);

    valueJSON.set(hitJSON.toString());

    output.collect(key, valueJSON);
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {

    DomainMetadataTask rootTask = findTaskOfType(DomainMetadataTask.class);

    JobConf job = new JobBuilder("WWW Prefix Stats Collector", getConf())

    .inputs(rootTask.getMergeDBDataPaths()).inputIsSeqFile().mapper(WWWPrefixStatsCollectorStep.class).mapperKeyValue(
        TextBytes.class, IntWritable.class).reducer(WWWPrefixStatsCollectorStep.class, false).outputKeyValue(
        TextBytes.class, TextBytes.class).output(outputPathLocation).outputIsSeqFile().numReducers(
        CrawlEnvironment.NUM_DB_SHARDS / 2).build();

    JobClient.runJob(job);
  }

}
