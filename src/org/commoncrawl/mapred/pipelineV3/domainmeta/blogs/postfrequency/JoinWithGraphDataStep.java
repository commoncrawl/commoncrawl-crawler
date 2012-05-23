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
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.rank.DedupedDomainLinksStep;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.JoinMapper;
import org.commoncrawl.util.JoinValue;
import org.commoncrawl.util.TextBytes;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * 
 * @author rana
 *
 */
public class JoinWithGraphDataStep extends CrawlPipelineStep implements
    Reducer<TextBytes, JoinValue, TextBytes, TextBytes> {

  static class Candidate {

    String type;

    int hits;

    Candidate(String type) {
      this.type = type;
    }
  }

  enum Counters {
    HAD_TUMBLR_REFERENCE, HAD_BLOGGER_REFERENCE, HAD_WORDPRESS_REFERENCE, HAD_TYPEPAD_REFERENCE,
    SET_GENERATOR_VIA_OUTLINKS, GOT_POST_FREQ_RECORD, FOUND_CANIDATE, POST_FREQ_CANDIDATE_MISMATCH
  }

  public static final String OUTPUT_DIR = "graphJoinResult";

  private static final Log LOG = LogFactory.getLog(JoinWithGraphDataStep.class);

  JsonParser parser = new JsonParser();

  static Candidate candidates[] = { new Candidate("blogger"), new Candidate("wordpress"), new Candidate("typepad"),
      new Candidate("tumblr") };

  static final int BLOGGER_OFFSET = 0;

  static final int WORDPRESS_OFFSET = 1;

  static final int TYPEPAD_OFFSET = 2;

  static final int TUMBLR_OFFSET = 3;

  static final Candidate findBestCandidate(Reporter reporter) {
    Candidate candidateOut = null;
    int maxHits = 0;
    for (Candidate candidate : candidates) {
      if (candidate.hits > maxHits) {
        candidateOut = candidate;
        maxHits = candidate.hits;
      }
    }
    if (candidateOut != null) {
      reporter.incrCounter(Counters.FOUND_CANIDATE, 1);
    }
    return candidateOut;
  }

  public JoinWithGraphDataStep() {
    super(null, null, null);
  }

  public JoinWithGraphDataStep(CrawlPipelineTask task) {
    super(task, "Join Post Info with Graph", OUTPUT_DIR);
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
  public void reduce(TextBytes key, Iterator<JoinValue> values, OutputCollector<TextBytes, TextBytes> output,
      Reporter reporter) throws IOException {
    JsonObject postFreqInfo = null;

    for (Candidate candidate : candidates)
      candidate.hits = 0;

    while (values.hasNext()) {
      JoinValue value = values.next();
      if (value.getTag().toString().equals(DedupedDomainLinksStep.OUTPUT_DIR_NAME)) {
        JsonObject graphJSON = parser.parse(value.getTextValue().toString()).getAsJsonObject();
        if (graphJSON.has("to")) {
          String toDomain = graphJSON.get("to").getAsString();
          if (toDomain.equals("www.blogger.com")) {
            candidates[BLOGGER_OFFSET].hits++;
            reporter.incrCounter(Counters.HAD_BLOGGER_REFERENCE, 1);
          } else if (toDomain.equals("static.tumblr.com")) {
            candidates[TUMBLR_OFFSET].hits++;
            reporter.incrCounter(Counters.HAD_TUMBLR_REFERENCE, 1);
          } else if (toDomain.endsWith(".wp.com")) {
            candidates[WORDPRESS_OFFSET].hits++;
            reporter.incrCounter(Counters.HAD_WORDPRESS_REFERENCE, 1);
          } else if (toDomain.equals("static.typepad.com")) {
            candidates[TYPEPAD_OFFSET].hits++;
            reporter.incrCounter(Counters.HAD_TYPEPAD_REFERENCE, 1);
          }
        }
      } else if (value.getTag().toString().equals(GroupByDomainStep.OUTPUT_DIR_NAME)) {
        reporter.incrCounter(Counters.GOT_POST_FREQ_RECORD, 1);
        postFreqInfo = parser.parse(value.getTextValue().toString()).getAsJsonObject();
        Candidate bestCandidate = findBestCandidate(reporter);
        if (bestCandidate != null) {
          if (!postFreqInfo.has("blogger") || !postFreqInfo.has("typepad") || !postFreqInfo.has("wordpress")) {
            reporter.incrCounter(Counters.SET_GENERATOR_VIA_OUTLINKS, 1);
            postFreqInfo.addProperty(bestCandidate.type, true);
            postFreqInfo.addProperty(bestCandidate.type + "_hits", bestCandidate.hits);
          } else {

            if (postFreqInfo.has("blogger") && !bestCandidate.type.equals("blogger")) {
              reporter.incrCounter(Counters.POST_FREQ_CANDIDATE_MISMATCH, 1);
              LOG.info("Post Freq Said Blogger, Best Candidate Was:" + bestCandidate.type);
            } else if (postFreqInfo.has("tumblr") && !bestCandidate.type.equals("tumblr")) {
              reporter.incrCounter(Counters.POST_FREQ_CANDIDATE_MISMATCH, 1);
              LOG.info("Post Freq Said tumblr, Best Candidate Was:" + bestCandidate.type);
            } else if (postFreqInfo.has("wordpress") && !bestCandidate.type.equals("wordpress")) {
              reporter.incrCounter(Counters.POST_FREQ_CANDIDATE_MISMATCH, 1);
              LOG.info("Post Freq Said wordpress, Best Candidate Was:" + bestCandidate.type);
            } else if (postFreqInfo.has("typepad") && !bestCandidate.type.equals("typepad")) {
              reporter.incrCounter(Counters.POST_FREQ_CANDIDATE_MISMATCH, 1);
              LOG.info("Post Freq Said typepad, Best Candidate Was:" + bestCandidate.type);
            }
          }
        }
        output.collect(key, new TextBytes(postFreqInfo.toString()));
      }
    }
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {
    LOG.info(getOutputDirForStep(DedupedDomainLinksStep.class));

    ImmutableList<Path> inputs = new ImmutableList.Builder<Path>().add(
        getOutputDirForStep(DedupedDomainLinksStep.class)).add(getOutputDirForStep(GroupByDomainStep.class)).build();

    JobConf jobConf = new JobBuilder(getPipelineStepName(), getConf())

    .inputs(inputs).inputIsSeqFile().mapper(JoinMapper.class).mapperKeyValue(TextBytes.class, JoinValue.class).reducer(
        JoinWithGraphDataStep.class, false).numReducers(CrawlEnvironment.NUM_DB_SHARDS / 2).outputKeyValue(
        TextBytes.class, TextBytes.class).output(outputPathLocation).outputIsSeqFile().build();

    JobClient.runJob(jobConf);
  }

}
