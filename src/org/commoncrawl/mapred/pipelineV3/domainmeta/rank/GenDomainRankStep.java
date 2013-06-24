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
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.SuperDomainList;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLFPBloomFilter;
import org.commoncrawl.util.URLUtils;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Hopelessly simplistic hack to get a domain ranking to minimally help
 * prioritize the crawl
 * 
 * @author rana
 * 
 */
public class GenDomainRankStep extends CrawlPipelineStep {

  public static class GenerateRankReducer implements Reducer<TextBytes, TextBytes, TextBytes, DoubleWritable> {

    JsonParser parser = new JsonParser();

    static final int NUM_HASH_FUNCTIONS = 10;
    static final int NUM_BITS = 11;
    static final int NUM_ELEMENTS = 1 << 28;
    static final int FLUSH_THRESHOLD = 1 << 23;
    Set<Long> superDomainIdSet = null;
    URLFPBloomFilter emittedTuplesFilter = new URLFPBloomFilter(NUM_ELEMENTS, NUM_HASH_FUNCTIONS, NUM_BITS);
    int bloomEntriesCount = 0;
    URLFPV2 testKey = new URLFPV2();

    static final double SAME_ROOT_WEIGHT = .20;

    static double calcualteScore(int inlinksFromSameRoot, int inlinksFromDifferentRoot) {
      inlinksFromDifferentRoot = Math.max(1, inlinksFromDifferentRoot);
      inlinksFromSameRoot = Math.max(1, inlinksFromSameRoot);
      return (Math.min(Math.sqrt(Math.pow(Math.log(inlinksFromSameRoot) * SAME_ROOT_WEIGHT, 2)
          + Math.pow(Math.log(inlinksFromDifferentRoot), 2)), 14) / 14.0) * 10.0;
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
    public void reduce(TextBytes key, Iterator<TextBytes> values, OutputCollector<TextBytes, DoubleWritable> output,
        Reporter reporter) throws IOException {

      int inlinksFromDifferentRoot = 0;
      int inlinksFromSameRoot = 0;

      String currentDomain = key.toString();
      String rootDomain = URLUtils.extractRootDomainName(currentDomain);

      if (rootDomain != null) {
        // get root domain fp
        long rootDomainFP = SuperDomainList.domainFingerprintGivenName(rootDomain);
        // is super domain
        boolean isSuperDomain = superDomainIdSet.contains(rootDomainFP);

        while (values.hasNext()) {
          JsonObject linkObject = parser.parse(values.next().toString()).getAsJsonObject();
          if (linkObject.has("from")) {
            String fromHost = linkObject.get("from").getAsString();
            String fromRoot = URLUtils.extractRootDomainName(fromHost);
            if (fromRoot != null) {
              if (!fromRoot.equals(rootDomain)) {
                // get the domain's fp
                long rootFP = SuperDomainList.domainFingerprintGivenName(fromRoot);
                // is it a super domain ... ?
                if (superDomainIdSet.contains(rootFP)) {
                  // ok every entry counts
                  inlinksFromDifferentRoot++;
                } else {
                  // ok check to see if we emitted this tuple ...
                  testKey.setDomainHash(SuperDomainList.domainFingerprintGivenName(fromRoot + "->" + rootDomain));
                  testKey.setUrlHash(testKey.getDomainHash());

                  if (!emittedTuplesFilter.isPresent(testKey)) {
                    inlinksFromDifferentRoot++;
                    // add to bloom
                    emittedTuplesFilter.add(testKey);
                    // increment count ...
                    bloomEntriesCount++;
                  }
                }
              } else {
                if (isSuperDomain) {
                  inlinksFromSameRoot++;
                }
              }
            }
          }
        }
        output.collect(key, new DoubleWritable(calcualteScore(inlinksFromSameRoot, inlinksFromDifferentRoot)));
      }

      if (bloomEntriesCount >= FLUSH_THRESHOLD) {
        emittedTuplesFilter.clear();
        bloomEntriesCount = 0;
      }
    }
  }

  public static final String OUTPUT_DIR_NAME = "domainRank";

  public static final String SUPER_DOMAIN_FILE_PATH = "superDomainFilePath";

  private static final Log LOG = LogFactory.getLog(GenDomainRankStep.class);

  public GenDomainRankStep(CrawlPipelineTask task) {
    super(task, "Domain Rank Generator", OUTPUT_DIR_NAME);
  }

  @Override
  public Log getLogger() {
    return LOG;
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {

    Path superDomainPath = new Path(makeUniqueOutputDirPath(_task
        .getOutputDirForStep(GenSuperDomainListStep.OUTPUT_DIR_NAME), getTaskIdentityId()), "part-00000");

    LOG.info("Super Domain File Path is:" + superDomainPath);

    JobConf job = new JobBuilder(getDescription(), getConf())

        .input(
            makeUniqueOutputDirPath(_task.getOutputDirForStep(DedupedDomainLinksStep.OUTPUT_DIR_NAME),
                getTaskIdentityId()))
        .inputIsSeqFile()
        .mapperKeyValue(TextBytes.class, TextBytes.class)
        .reducer(GenerateRankReducer.class, false)
        .numReducers(CrawlEnvironment.NUM_DB_SHARDS)
        .outputKeyValue(TextBytes.class, DoubleWritable.class)
        .output(outputPathLocation).outputIsSeqFile()
        .set(SUPER_DOMAIN_FILE_PATH, superDomainPath.toString())

        .build();

    JobClient.runJob(job);
  }

}
