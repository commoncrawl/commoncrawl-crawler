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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.rank.GenSuperDomainListStep;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.SuperDomainList;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLUtils;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * 
 * @author rana
 *
 */
public class DNSFailuresCollectorStep extends CrawlPipelineStep implements
    Mapper<LongWritable, Text, TextBytes, TextBytes>, Reducer<TextBytes, TextBytes, TextBytes, TextBytes> {

  private static final Log LOG = LogFactory.getLog(DNSFailuresCollectorStep.class);

  public static final String OUTPUT_DIR_NAME = "dnsFailures";

  Pattern pattern = Pattern.compile("^([^,]*),(.*)$");

  Set<Long> superDomainIdSet;

  JsonParser parser = new JsonParser();

  public DNSFailuresCollectorStep() {
    super(null, null, null);
  }

  public DNSFailuresCollectorStep(CrawlPipelineTask task) {
    super(task, "DNS Failures Collector", OUTPUT_DIR_NAME);
  }

  @Override
  public void close() throws IOException {

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
  public void map(LongWritable key, Text value, OutputCollector<TextBytes, TextBytes> output, Reporter reporter)
      throws IOException {
    Matcher m = pattern.matcher(value.toString());
    if (m.matches() && m.groupCount() >= 2) {
      String domain = m.group(1);
      String rootDomain = URLUtils.extractRootDomainName(domain);

      if (rootDomain != null) {
        long domainFP = SuperDomainList.domainFingerprintGivenName(rootDomain);
        if (superDomainIdSet.contains(domainFP)) {
          domain = rootDomain;
        }

        JsonObject objectOut = new JsonObject();

        String failureReason = m.group(2);
        if (failureReason.equalsIgnoreCase("NXDOMAIN")) {
          objectOut.addProperty("nxdomain", 1);
        } else if (failureReason.equalsIgnoreCase("SERVFAIL")) {
          objectOut.addProperty("servfail", 1);
        } else if (failureReason.equalsIgnoreCase("NOERROR")) {
          objectOut.addProperty("noerror", 1);
        } else {
          objectOut.addProperty("other", 1);
        }
        output.collect(new TextBytes(domain), new TextBytes(objectOut.toString()));
      }
    }
  }

  @Override
  public void reduce(TextBytes key, Iterator<TextBytes> values, OutputCollector<TextBytes, TextBytes> output,
      Reporter reporter) throws IOException {

    int nxdomain = 0;
    int servfail = 0;
    int noerror = 0;
    int other = 0;

    while (values.hasNext()) {
      JsonObject obj = parser.parse(values.next().toString()).getAsJsonObject();
      if (obj.has("nxdomain"))
        nxdomain++;
      else if (obj.has("servfail"))
        servfail++;
      else if (obj.has("noerror"))
        noerror++;
      else
        other++;
    }

    int totalErrors = nxdomain + servfail + noerror + other;

    JsonObject result = new JsonObject();

    result.addProperty("domain", key.toString());
    result.addProperty("totalErrors", totalErrors);
    result.addProperty("nxdomain", nxdomain);
    result.addProperty("servfail", servfail);
    result.addProperty("noerror", noerror);
    result.addProperty("other", other);

    String rootDomain = URLUtils.extractRootDomainName(key.toString());

    if (rootDomain != null) {
      output.collect(new TextBytes(rootDomain), new TextBytes(result.toString()));
    }
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {

    ArrayList<Path> paths = new ArrayList<Path>();
    FileStatus candidates[] = getFileSystem().globStatus(new Path("crawl/logs/dns/*/"));
    for (FileStatus candidate : candidates)
      paths.add(candidate.getPath());

    Path superDomainListPath = new Path(getOutputDirForStep(GenSuperDomainListStep.class), "part-00000");

    JobConf job = new JobBuilder(getDescription(), getConf())

    .inputs(paths).inputFormat(TextInputFormat.class).mapper(DNSFailuresCollectorStep.class).reducer(
        DNSFailuresCollectorStep.class, false).keyValue(TextBytes.class, TextBytes.class).output(outputPathLocation)
        .outputIsSeqFile().numReducers(CrawlEnvironment.NUM_DB_SHARDS / 2).set(
            CrawlStatsCollectorTask.SUPER_DOMAIN_FILE_PATH, superDomainListPath.toString())

        .build();

    JobClient.runJob(job);
  }

}
