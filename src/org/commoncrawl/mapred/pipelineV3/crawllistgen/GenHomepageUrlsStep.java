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
import org.commoncrawl.mapred.pipelineV3.domainmeta.crawlstats.CrawlStatsCollectorTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.rank.GenSuperDomainListStep;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.SuperDomainList;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLUtils;

import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * 
 * @author rana
 *
 */
public class GenHomepageUrlsStep extends CrawlPipelineStep implements
    Mapper<TextBytes, TextBytes, TextBytes, TextBytes> {

  enum Counters {
    SKIPPED_BAD_DNSRESULT
  }

  public static String OUTPUT_DIR_NAME = "homePageUrls";

  private static final Log LOG = LogFactory.getLog(GenHomepageUrlsStep.class);

  JsonParser parser = new JsonParser();

  TextBytes keyOut = new TextBytes();

  TextBytes urlOut = new TextBytes();

  Set<Long> superDomainIdSet;

  public GenHomepageUrlsStep() {
    super(null, null, null);
  }

  public GenHomepageUrlsStep(CrawlPipelineTask task) {
    super(task, "Generate Home Page URLS", OUTPUT_DIR_NAME);
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void configure(JobConf job) {
    Path superDomainIdFile = new Path(job.get(CrawlStatsCollectorTask.SUPER_DOMAIN_FILE_PATH));

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
    String domain = key.toString();
    String rootDomain = URLUtils.extractRootDomainName(domain);
    if (rootDomain != null) {
      JsonObject crawlStats = parser.parse(value.toString()).getAsJsonObject();

      boolean skip = false;
      if (!crawlStats.has("crawled") || crawlStats.get("crawled").getAsInt() == 0) {
        if (crawlStats.has("dnsStats")) {
          JsonObject dnsStats = crawlStats.get("dnsStats").getAsJsonObject();
          if (dnsStats.has(domain)) {
            int totalErrors = dnsStats.get(domain).getAsJsonObject().get("totalErrors").getAsInt();
            if (totalErrors != 0) {
              reporter.incrCounter(Counters.SKIPPED_BAD_DNSRESULT, 1);
              skip = true;
            }
          }
        }
      }

      if (!skip) {
        boolean emitWWW = false;
        if (domain.equals(rootDomain)) {
          emitWWW = true;
          if (crawlStats.has("www")) {
            double wwwCount = crawlStats.get("www").getAsDouble();
            double nonWWWCount = crawlStats.get("nonWWW").getAsDouble();
            if (wwwCount + nonWWWCount != 0) {
              if (nonWWWCount > wwwCount) {
                emitWWW = false;
                double pctWWW = wwwCount / (nonWWWCount + wwwCount);
                if (nonWWWCount + wwwCount > 100) {
                  if (pctWWW >= .40f)
                    emitWWW = true;
                }
              }
            }
          }
        }
        urlOut.set("http://" + ((emitWWW) ? "www." : "") + domain + "/");
        if (PartitionUtils.generatePartitionKeyGivenURL(superDomainIdSet, urlOut,
            CrawlListGeneratorTask.KEY_TYPE_HOMEPAGE_URL, keyOut)) {
          output.collect(keyOut, urlOut);
        }
      }
      if (crawlStats.has("subdomains")) {
        JsonArray subDomains = crawlStats.get("subdomains").getAsJsonArray();
        for (JsonElement element : subDomains) {
          String subDomain = element.getAsString();
          urlOut.set("http://" + subDomain + "/");
          if (PartitionUtils.generatePartitionKeyGivenURL(superDomainIdSet, urlOut,
              CrawlListGeneratorTask.KEY_TYPE_HOMEPAGE_URL, keyOut)) {
            output.collect(keyOut, urlOut);
          }
        }
      }
    }
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {

    Path superDomainListPath = new Path(getOutputDirForStep(GenSuperDomainListStep.class), "part-00000");

    JobConf job = new JobBuilder(getDescription(), getConf())

    .input(getOutputDirForStep(CrawlStatsCollectorTask.class)).inputIsSeqFile().keyValue(TextBytes.class,
        TextBytes.class).mapper(GenHomepageUrlsStep.class).partition(PartitionUtils.PartitionKeyPartitioner.class)
        .numReducers(CrawlListGeneratorTask.NUM_SHARDS).output(outputPathLocation).outputIsSeqFile()
        .setAffinityNoBalancing(getOutputDirForStep(PartitionCrawlDBStep.class),
            ImmutableSet.of("ccd001.commoncrawl.org", "ccd006.commoncrawl.org")).set(
            CrawlStatsCollectorTask.SUPER_DOMAIN_FILE_PATH, superDomainListPath.toString())

        .build();

    JobClient.runJob(job);
  }

}
