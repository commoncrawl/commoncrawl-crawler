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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.DomainMetadataTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.rank.GenSuperDomainListStep;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.GoogleURL;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.SuperDomainList;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLFPBloomFilter;
import org.commoncrawl.util.URLUtils;

import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;

/**
 * 
 * @author rana
 *
 */
public class NonSuperSubdomainCollectorStep extends CrawlPipelineStep implements
    Mapper<TextBytes, TextBytes, TextBytes, TextBytes>, Reducer<TextBytes, TextBytes, TextBytes, TextBytes> {

  enum Counters {
    HIT_MAXSUBDOMAIN_LIMIT, SKIPPED_SUBDOMAIN_SAME_AS_ROOT_VIA_ID, SKIPEED_SUBDOMAIN_SAME_AS_ROOT_BUT_WWW_PREFIX,
    SKIPEED_SUBDOMAIN_SAME_AS_ROOT_BUT_WWW_PATTERN_MATCH
  }

  private static final Log LOG = LogFactory.getLog(NonSuperSubdomainCollectorStep.class);

  static final int NUM_HASH_FUNCTIONS = 10;

  static final int NUM_BITS = 11;
  static final int NUM_ELEMENTS = 1 << 29;
  static final int FLUSH_THRESHOLD = 1 << 23;
  public static final String SUPER_DOMAIN_FILE_PATH = "super-domain-list";

  URLFPBloomFilter subDomainFilter;

  public static final String OUTPUT_DIR_NAME = "nonsuper-subdomains";

  URLFPV2 bloomKey = new URLFPV2();

  TextBytes emptyTextBytes = new TextBytes();

  Pattern wwwMatchPattern = Pattern.compile("www[\\-0-9]*\\.");

  Set<Long> superDomainIdSet;
  HashSet<String> domains = new HashSet<String>();

  static final int MAX_SUBDOMAINS_ALLOWED = 100;

  public NonSuperSubdomainCollectorStep() {
    super(null, null, null);
  }

  public NonSuperSubdomainCollectorStep(CrawlPipelineTask task) {
    super(task, "SubDomain Collector", OUTPUT_DIR_NAME);
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public void configure(JobConf job) {

    if (job.getBoolean("mapred.task.is.map", false)) {
      Path superDomainIdFile = new Path(job.get(SUPER_DOMAIN_FILE_PATH));

      try {
        superDomainIdSet = SuperDomainList.loadSuperDomainIdList(job, superDomainIdFile);
      } catch (IOException e) {
        LOG.error(StringUtils.stringifyException(e));
        throw new RuntimeException(e);
      }

      subDomainFilter = new URLFPBloomFilter(NUM_ELEMENTS, NUM_HASH_FUNCTIONS, NUM_BITS);
    }
  }

  @Override
  public Log getLogger() {
    return LOG;
  }

  @Override
  public void map(TextBytes key, TextBytes value, OutputCollector<TextBytes, TextBytes> output, Reporter reporter)
      throws IOException {
    String url = key.toString();
    GoogleURL urlObject = new GoogleURL(url);

    if (urlObject.isValid()) {

      String rootDomain = URLUtils.extractRootDomainName(urlObject.getHost());

      if (rootDomain != null) {
        long rootDomainId = SuperDomainList.domainFingerprintGivenName(rootDomain);

        if (!superDomainIdSet.contains(rootDomainId)) {

          long subDomainId = SuperDomainList.domainFingerprintGivenName(urlObject.getHost());

          if (subDomainId == rootDomainId) {
            reporter.incrCounter(Counters.SKIPPED_SUBDOMAIN_SAME_AS_ROOT_VIA_ID, 1);
            return;
          }

          // extract prefix ...
          String prefix = urlObject.getHost().substring(0, urlObject.getHost().length() - rootDomain.length());

          // straight match ...
          if (prefix.equals("www.")) {
            reporter.incrCounter(Counters.SKIPEED_SUBDOMAIN_SAME_AS_ROOT_BUT_WWW_PREFIX, 1);
            return; // skip
          } else if (prefix.startsWith("www") && wwwMatchPattern.matcher(prefix).matches()) {
            reporter.incrCounter(Counters.SKIPEED_SUBDOMAIN_SAME_AS_ROOT_BUT_WWW_PATTERN_MATCH, 1);
            return;
          }

          bloomKey.setDomainHash(subDomainId);
          bloomKey.setUrlHash(subDomainId);

          if (subDomainFilter.isPresent(bloomKey)) {
            // hacky but ,oh well, pressed for time
            return;
          }

          // add it to the BF NOW
          subDomainFilter.add(bloomKey);

          // emit as root domain , sub domain
          output.collect(new TextBytes(rootDomain), new TextBytes(urlObject.getHost()));
        }
      }
    }
  }

  @Override
  public void reduce(TextBytes key, Iterator<TextBytes> values, OutputCollector<TextBytes, TextBytes> output,
      Reporter reporter) throws IOException {
    while (values.hasNext()) {
      domains.add(values.next().toString());
      if (domains.size() >= MAX_SUBDOMAINS_ALLOWED) {
        reporter.incrCounter(Counters.HIT_MAXSUBDOMAIN_LIMIT, 1);
        break;
      }
    }

    if (domains.size() != 0 && domains.size() < MAX_SUBDOMAINS_ALLOWED) {
      JsonArray array = new JsonArray();
      for (String domain : domains) {
        array.add(new JsonPrimitive(domain));
      }
      output.collect(key, new TextBytes(array.toString()));
    }
    domains.clear();
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {

    DomainMetadataTask rootTask = findTaskOfType(DomainMetadataTask.class);
    Path superDomainListPath = new Path(getOutputDirForStep(GenSuperDomainListStep.class), "part-00000");

    JobConf job = new JobBuilder(getDescription(), getConf()).inputs(rootTask.getRestrictedMergeDBDataPaths())
        .inputIsSeqFile().mapper(NonSuperSubdomainCollectorStep.class).reducer(NonSuperSubdomainCollectorStep.class,
            false).numReducers(CrawlEnvironment.NUM_DB_SHARDS / 2).keyValue(TextBytes.class, TextBytes.class).output(
            outputPathLocation).outputIsSeqFile().set(SUPER_DOMAIN_FILE_PATH, superDomainListPath.toString()).reuseJVM(
            1000).build();

    JobClient.runJob(job);
  }

}
