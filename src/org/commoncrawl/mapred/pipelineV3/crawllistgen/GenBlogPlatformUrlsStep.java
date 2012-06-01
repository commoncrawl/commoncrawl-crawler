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
import java.text.NumberFormat;
import java.util.Date;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;
import org.commoncrawl.mapred.PostFrequencyInfo;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.blogs.postfrequency.GenPostFrequencyStep;
import org.commoncrawl.mapred.pipelineV3.domainmeta.rank.GenSuperDomainListStep;
import org.commoncrawl.util.FPGenerator;
import org.commoncrawl.util.GoogleURL;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.SuperDomainList;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLUtils;
import org.commoncrawl.util.Tuples.Pair;
import org.commoncrawl.util.time.SerialDate;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * 
 * @author rana
 *
 */
public class GenBlogPlatformUrlsStep extends CrawlPipelineStep implements
    Mapper<TextBytes, TextBytes, TextBytes, BooleanWritable> {

  enum Counters {
    ROOT_DOMAIN_WAS_NULL, ROOT_WAS_WORDPRESS, ROOT_WAS_TUMBLR, ROOT_WAS_BLOGGER, ROOT_WAS_TYPEPAD, EMITTING_WORDPRESS,
    EMITTING_BLOGGER, EMITTING_TYPEPAD, EMITTING_TUMBLR, EMITTING_OTHER
  }

  private static final Log LOG = LogFactory.getLog(GenBlogPlatformUrlsStep.class);

  public static final String OUTPUT_DIR_NAME = "blogUrls";

  public static final String SUPER_DOMAIN_FILE_PATH = "super-domain-list";

  Set<Long> superDomainIdSet;

  JsonParser parser = new JsonParser();
  PostFrequencyInfo freqInfo = new PostFrequencyInfo();

  static long wikipediaRootHash = FPGenerator.std64.fp("wikipedia.org");

  static long flickrRootHash = FPGenerator.std64.fp("flickr.com");

  static long stumbleUponRootHash = FPGenerator.std64.fp("stumbleupon.com");
  static long wordpressRootHash = FPGenerator.std64.fp("wordpress.com");

  static long ebayRootHash = FPGenerator.std64.fp("ebay.com");

  static long technoratiRootHash = FPGenerator.std64.fp("technorati.com");
  static long imdbRootHash = FPGenerator.std64.fp("imdb.com");
  static long quoraRootHash = FPGenerator.std64.fp("quora.com");
  static long stackOverflowRootHash = FPGenerator.std64.fp("stackoverflow.com");
  static long slideShareRootHash = FPGenerator.std64.fp("slideshare.net");
  static long youtubeRootHash = FPGenerator.std64.fp("youtube.com");
  static long amazonRootHash = FPGenerator.std64.fp("amazon.com");
  static long tumblrRootHash = FPGenerator.std64.fp("tumblr.com");
  static long typepadRootHash = FPGenerator.std64.fp("typepad.com");
  static long blogspotRootHash = FPGenerator.std64.fp("blogspot.com");
  static long facebookRootHash = FPGenerator.std64.fp("facebook.com");
  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }
  private static final NumberFormat SHORT_MONTH_FORMAT = NumberFormat.getInstance();
  static {
    SHORT_MONTH_FORMAT.setMinimumIntegerDigits(2);
    SHORT_MONTH_FORMAT.setGroupingUsed(false);
  }
  private static final NumberFormat YEAR_FORMAT = NumberFormat.getInstance();

  static {
    YEAR_FORMAT.setMinimumIntegerDigits(4);
    YEAR_FORMAT.setGroupingUsed(false);
  }

  static ImmutableList<Pair<Integer, Integer>> getProbeDates(int maxMonthsToProbe) {

    Pair<Integer, Integer> startYearMonth = getStartYearMonth();

    ImmutableList.Builder<Pair<Integer, Integer>> builder = new ImmutableList.Builder<Pair<Integer, Integer>>();

    int months = 0;
    outer: for (int year = startYearMonth.e0; year >= 2000; --year) {
      int maxMonth = (year == startYearMonth.e0) ? startYearMonth.e1 : 12;
      for (int month = maxMonth; month >= 1; --month) {
        builder.add(new Pair<Integer, Integer>(year, month));
        if (++months == maxMonthsToProbe)
          break outer;
      }
    }
    return builder.build();
  }

  static Pair<Integer, Integer> getStartYearMonth() {
    // more detailed fetch ...
    SerialDate today = SerialDate.createInstance(new Date(System.currentTimeMillis()));
    return new Pair<Integer, Integer>(today.getYYYY(), today.getMonth());
  }

  TextBytes keyBuffer = new TextBytes();

  TextBytes urlBuffer = new TextBytes();
  BooleanWritable skipData = new BooleanWritable();

  public GenBlogPlatformUrlsStep() {
    super(null, null, null);
  }

  public GenBlogPlatformUrlsStep(CrawlPipelineTask parentTask) throws IOException {
    super(parentTask, "Blog URL Injector", OUTPUT_DIR_NAME);
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub

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

  private void emitBlogspotDomain(String blogspotURL, PostFrequencyInfo postFrequencyInfo,
      OutputCollector<TextBytes, BooleanWritable> output, Reporter reporter) throws IOException {
    // emit home page
    emitItem(blogspotURL, output, reporter, false);

    if (postFrequencyInfo != null && postFrequencyInfo.getLastYearWithPosts() >= 2011) {
      // more detailed fetch ...

      ImmutableList<Pair<Integer, Integer>> probeDates = getProbeDates(24);

      int probeItemIndex = 0;

      for (Pair<Integer, Integer> probeDate : probeDates) {
        emitItem(blogspotURL + YEAR_FORMAT.format(probeDate.e0) + "_" + SHORT_MONTH_FORMAT.format(probeDate.e1)
            + "_01_archive.html", output, reporter, (probeItemIndex != 0));
        ++probeItemIndex;
      }
    }
  }

  private void emitItem(String url, OutputCollector<TextBytes, BooleanWritable> output, Reporter reporter,
      boolean skipIfDupe) throws IOException {
    skipData.set(skipIfDupe);
    urlBuffer.set(url);
    if (PartitionUtils.generatePartitionKeyGivenURL(superDomainIdSet, urlBuffer,
        CrawlListGeneratorTask.KEY_TYPE_BLOGPROBE_URL, keyBuffer)) {
      output.collect(keyBuffer, skipData);
    }
  }

  private void emitOtherBlogPlatformDomain(String siteURL, PostFrequencyInfo postFrequencyInfo,
      OutputCollector<TextBytes, BooleanWritable> output, Reporter reporter) throws IOException {
    emitItem(siteURL, output, reporter, false);
  }

  private void emitTumblrDomain(String siteURL, PostFrequencyInfo postFrequencyInfo,
      OutputCollector<TextBytes, BooleanWritable> output, Reporter reporter) throws IOException {
    GoogleURL urlObject = new GoogleURL(siteURL);

    if (urlObject.isValid()) {
      // emit home page
      emitItem(siteURL, output, reporter, false);
      // emit archive url
      emitItem("http://" + urlObject.getHost() + "/archive", output, reporter, false);
    }
  }

  private void emitTypepadDomain(String siteURL, PostFrequencyInfo postFrequencyInfo,
      OutputCollector<TextBytes, BooleanWritable> output, Reporter reporter) throws IOException {
    siteURL = postFrequencyInfo.getBlogPath();
    if (!siteURL.endsWith("/")) {
      siteURL += "/";
    }

    // emit home page
    emitItem(siteURL, output, reporter, false);

    if (postFrequencyInfo != null && postFrequencyInfo.getLastYearWithPosts() >= 2011) {
      ImmutableList<Pair<Integer, Integer>> probeDates = getProbeDates(24);

      int probeItemIndex = 0;

      for (Pair<Integer, Integer> probeDate : probeDates) {
        emitItem(siteURL + YEAR_FORMAT.format(probeDate.e0) + "/" + SHORT_MONTH_FORMAT.format(probeDate.e1)
            + "/index.html", output, reporter, (probeItemIndex != 0));

        probeItemIndex++;
      }
    }
  }

  private void emitWordPressDomain(String wordpressURL, PostFrequencyInfo postFrequencyInfo,
      OutputCollector<TextBytes, BooleanWritable> output, Reporter reporter) throws IOException {

    emitItem(wordpressURL, output, reporter, false);

    if (postFrequencyInfo != null && postFrequencyInfo.getLastYearWithPosts() >= 2011) {

      ImmutableList<Pair<Integer, Integer>> probeDates = getProbeDates(24);

      // number of paginations is based on avg posts per month
      int paginations = (int) Math.ceil((float) postFrequencyInfo.getAvgPostsPerMonth() / (float) 10);

      int probeItemIndex = 0;

      for (Pair<Integer, Integer> probeDate : probeDates) {
        for (int page = 1; page < paginations; ++page) {
          if (page == 1) {
            emitItem(wordpressURL + YEAR_FORMAT.format(probeDate.e0) + "/" + SHORT_MONTH_FORMAT.format(probeDate.e1)
                + "/", output, reporter, (probeItemIndex != 0));
          } else {
            emitItem(wordpressURL + YEAR_FORMAT.format(probeDate.e0) + "/" + SHORT_MONTH_FORMAT.format(probeDate.e1)
                + "/page/" + (page + 1) + "/", output, reporter, (probeItemIndex != 0));
          }
        }
        ++probeItemIndex;
      }
    }
  }

  @Override
  public Log getLogger() {
    return LOG;
  }

  @Override
  public void map(TextBytes key, TextBytes value, OutputCollector<TextBytes, BooleanWritable> output, Reporter reporter)
      throws IOException {
    JsonObject postFreqJSON = parser.parse(value.toString()).getAsJsonObject();

    freqInfo.setBlogPath(postFreqJSON.get("url").getAsString());
    freqInfo.setLastYearWithPosts(postFreqJSON.get("lastYearWithPost").getAsInt());
    freqInfo.setAvgPostsPerMonth((int) Math.ceil(postFreqJSON.get("avg").getAsDouble()));

    boolean isBlogger = postFreqJSON.has("blogger");
    boolean isWordpress = postFreqJSON.has("wordpress");
    boolean isTypepad = postFreqJSON.has("typepad");
    boolean isTumblr = postFreqJSON.has("tumblr");

    if (!isBlogger && !isWordpress && !isTypepad && !isTumblr) {
      String rootDomain = URLUtils.extractRootDomainName(key.toString());
      if (rootDomain == null) {
        reporter.incrCounter(Counters.ROOT_DOMAIN_WAS_NULL, 1);
        return;
      } else {
        long rootFP = SuperDomainList.domainFingerprintGivenName(rootDomain);
        if (rootFP == wordpressRootHash) {
          reporter.incrCounter(Counters.ROOT_WAS_WORDPRESS, 1);
          isWordpress = true;
        } else if (rootFP == tumblrRootHash) {
          reporter.incrCounter(Counters.ROOT_WAS_TUMBLR, 1);
          isTumblr = true;
        } else if (rootFP == blogspotRootHash) {
          reporter.incrCounter(Counters.ROOT_WAS_BLOGGER, 1);
          isBlogger = true;
        } else if (rootFP == typepadRootHash) {
          reporter.incrCounter(Counters.ROOT_WAS_TYPEPAD, 1);
          isTypepad = true;
        }
      }
    }

    if (isWordpress) {
      reporter.incrCounter(Counters.EMITTING_WORDPRESS, 1);
      emitWordPressDomain(freqInfo.getBlogPath(), freqInfo, output, reporter);
    } else if (isBlogger) {
      reporter.incrCounter(Counters.EMITTING_BLOGGER, 1);
      emitBlogspotDomain(freqInfo.getBlogPath(), freqInfo, output, reporter);
    } else if (isTypepad) {
      reporter.incrCounter(Counters.EMITTING_TYPEPAD, 1);
      emitTypepadDomain(freqInfo.getBlogPath(), freqInfo, output, reporter);
    } else if (isTumblr) {
      reporter.incrCounter(Counters.EMITTING_TUMBLR, 1);
      emitTumblrDomain(freqInfo.getBlogPath(), freqInfo, output, reporter);
    } else {
      reporter.incrCounter(Counters.EMITTING_OTHER, 1);
      emitOtherBlogPlatformDomain(freqInfo.getBlogPath(), freqInfo, output, reporter);
    }
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {

    Path superDomainListPath = new Path(getOutputDirForStep(GenSuperDomainListStep.class), "part-00000");

    JobConf job = new JobBuilder(getDescription(), getConf())

    .input(getOutputDirForStep(GenPostFrequencyStep.class)).inputIsSeqFile().keyValue(TextBytes.class,
        BooleanWritable.class).mapper(GenBlogPlatformUrlsStep.class).partition(
        PartitionUtils.PartitionKeyPartitioner.class).numReducers(CrawlListGeneratorTask.NUM_SHARDS).output(
        outputPathLocation).outputIsSeqFile().setAffinityNoBalancing(getOutputDirForStep(PartitionCrawlDBStep.class),
        ImmutableSet.of("ccd001.commoncrawl.org", "ccd006.commoncrawl.org")).set(SUPER_DOMAIN_FILE_PATH,
        superDomainListPath.toString())

    .build();

    JobClient.runJob(job);

  }
}
