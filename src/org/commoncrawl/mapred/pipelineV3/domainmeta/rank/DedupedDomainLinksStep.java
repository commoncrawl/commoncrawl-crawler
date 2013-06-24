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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.FPGenerator;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLFPBloomFilter;

/**
 * 
 * @author rana
 *
 */
public class DedupedDomainLinksStep extends CrawlPipelineStep implements
    Reducer<TextBytes, TextBytes, TextBytes, TextBytes> {

  enum Counters {
    GOT_BLANK_TO_TUPLE, SKIPPED_DUPLICATE_TUPLE, CLEARED_BLOOM_FILTER

  }

  public static final int NUM_HASH_FUNCTIONS = 10;
  public static final int NUM_BITS = 11;

  public static final int NUM_ELEMENTS = 1 << 27;

  private URLFPBloomFilter emittedTuplesFilter = new URLFPBloomFilter(NUM_ELEMENTS, NUM_HASH_FUNCTIONS, NUM_BITS);

  public static final String OUTPUT_DIR_NAME = "linksCleaned";

  private static final Log LOG = LogFactory.getLog(DedupedDomainLinksStep.class);

  URLFPV2 bloomKey = new URLFPV2();

  private static final int FILTER_CLEAR_THRESHOLD = 1 << 24;

  long totalBloomEntries = 0;

  public DedupedDomainLinksStep() {
    super(null, null, null);
  }

  public DedupedDomainLinksStep(CrawlPipelineTask task) {
    super(task, "Remove Dupes", OUTPUT_DIR_NAME);
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
  public void reduce(TextBytes key, Iterator<TextBytes> values, OutputCollector<TextBytes, TextBytes> output,
      Reporter reporter) throws IOException {

    if (key.toString().length() != 0) {
      while (values.hasNext()) {
        TextBytes value = values.next();
        if (value.toString().equals("{\"to\":\"\"}")) {
          reporter.incrCounter(Counters.GOT_BLANK_TO_TUPLE, 1);
        } else {
          bloomKey.setDomainHash(FPGenerator.std64.fp(key.toString() + "-" + value.toString()));
          bloomKey.setUrlHash(bloomKey.getDomainHash());
          if (!emittedTuplesFilter.isPresent(bloomKey)) {
            totalBloomEntries += 1;
            emittedTuplesFilter.add(bloomKey);
            output.collect(key, value);
          } else {
            reporter.incrCounter(Counters.SKIPPED_DUPLICATE_TUPLE, 1);
          }
        }
      }
    }

    if (totalBloomEntries >= FILTER_CLEAR_THRESHOLD) {
      emittedTuplesFilter.clear();
      totalBloomEntries = 0;
      reporter.incrCounter(Counters.CLEARED_BLOOM_FILTER, 1);
    }
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {
    JobConf job = new JobBuilder(getDescription(), getConf())

    .input(makeUniqueOutputDirPath(_task.getOutputDirForStep(LinkScannerStep.OUTPUT_DIR_NAME), getTaskIdentityId()))
    .inputIsSeqFile()
    .keyValue(TextBytes.class, TextBytes.class)
    .reducer(DedupedDomainLinksStep.class, false)
    .numReducers(CrawlEnvironment.NUM_DB_SHARDS)
    .output(outputPathLocation).outputIsSeqFile()
    .build();

    JobClient.runJob(job);
  }

}
