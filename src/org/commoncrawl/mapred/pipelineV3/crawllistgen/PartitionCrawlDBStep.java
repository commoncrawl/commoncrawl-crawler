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
package org.commoncrawl.mapred.pipelineV3.crawllistgen;

import java.io.IOException;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.DomainMetadataTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.rank.GenSuperDomainListStep;
import org.commoncrawl.util.GoogleURL;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.MimeTypeFilter;
import org.commoncrawl.util.SuperDomainList;
import org.commoncrawl.util.TextBytes;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * 
 * @author rana
 *
 */
public class PartitionCrawlDBStep extends CrawlPipelineStep implements
    Mapper<TextBytes, TextBytes, TextBytes, TextBytes> {

  enum Counters {
    REJECTED_RSS_PATTERN, REJECTED_ROBOTS_TXT, REJECTED_INVALID_EXTENSION, REJECTED_INVALID_TYPEANDREL
  }

  private static final Log LOG = LogFactory.getLog(PartitionCrawlDBStep.class);

  public static final String OUTPUT_DIR_NAME = "crawlDBParitioned";

  public static final String SUPER_DOMAIN_FILE_PATH = "super-domain-list";

  TextBytes outputKey = new TextBytes();

  Pattern atomRSSPattern = Pattern.compile(".*(application/atom.xml|application/rss.xml).*");

  JsonParser parser = new JsonParser();

  Set<Long> superDomainIdSet;

  public PartitionCrawlDBStep() {
    super(null, null, null);
  }

  public PartitionCrawlDBStep(CrawlPipelineTask task) {
    super(task, "Crawl DB Paritioner", OUTPUT_DIR_NAME);
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public void configure(JobConf job) {
    Path superDomainIdFile = new Path(job.get(SUPER_DOMAIN_FILE_PATH));

    try {
      superDomainIdSet = SuperDomainList.loadSuperDomainIdList(job, superDomainIdFile);
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new RuntimeException(e);
    }
  }

  @Override
  public Log getLogger() {
    return LOG;
  }

  @Override
  public void map(TextBytes key, TextBytes value, OutputCollector<TextBytes, TextBytes> output, Reporter reporter)
      throws IOException {

    GoogleURL urlObject = new GoogleURL(key.toString());

    if (urlObject.isValid()) {
      if (PartitionUtils.generatePartitionKeyGivenURL(superDomainIdSet, urlObject,
          CrawlListGeneratorTask.KEY_TYPE_CRAWLDATA, outputKey)) {
        if (atomRSSPattern.matcher(value.toString()).matches()) {
          reporter.incrCounter(Counters.REJECTED_RSS_PATTERN, 1);
          return;
        } else if (key.toString().endsWith("robots.txt")) {
          reporter.incrCounter(Counters.REJECTED_ROBOTS_TXT, 1);
          return;
        } else {

          int indexOfDot = key.toString().lastIndexOf('.');

          if (indexOfDot != -1 && indexOfDot + 1 != key.toString().length()) {
            String extension = key.toString().substring(indexOfDot + 1);
            if (MimeTypeFilter.invalidExtensionMatcher.exactMatch(extension)) {
              reporter.incrCounter(Counters.REJECTED_INVALID_EXTENSION, 1);
              return;
            }
          }

          JsonObject containerObj = parser.parse(value.toString()).getAsJsonObject();
          JsonObject linkStatus = containerObj.getAsJsonObject("link_status");

          if (linkStatus != null) {
            JsonArray typeAndRels = linkStatus.getAsJsonArray("typeAndRels");
            if (typeAndRels != null) {
              for (JsonElement e : typeAndRels) {
                String parts[] = e.getAsString().split(":");
                if (parts.length != 2 || (parts.length == 2 && !parts[1].equalsIgnoreCase("a"))) {
                  reporter.incrCounter(Counters.REJECTED_INVALID_TYPEANDREL, 1);
                  return;
                }
              }
            }
          }
          output.collect(outputKey, value);
        }
      }
    }
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {

    Path superDomainListPath = new Path(getOutputDirForStep(GenSuperDomainListStep.class), "part-00000");

    // Path superDomainListPath = new
    // Path("crawl/ec2Import/domainMetadata/domainGraph/superDomainList/1334363621365","part-00000");

    DomainMetadataTask metadataTask = findTaskOfType(DomainMetadataTask.class);

    JobConf job = new JobBuilder(getDescription(), getConf())

    .inputs(metadataTask.getMergeDBDataPaths()).inputIsSeqFile().mapper(PartitionCrawlDBStep.class).partition(
        PartitionUtils.PartitionKeyPartitioner.class).keyValue(TextBytes.class, TextBytes.class).numReducers(
        CrawlListGeneratorTask.NUM_SHARDS).output(outputPathLocation).outputIsSeqFile().set(SUPER_DOMAIN_FILE_PATH,
        superDomainListPath.toString())

    .build();

    JobClient.runJob(job);
  }

}
