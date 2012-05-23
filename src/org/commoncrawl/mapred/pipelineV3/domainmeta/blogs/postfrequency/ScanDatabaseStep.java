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
import java.util.Date;
import java.util.regex.Matcher;
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
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.mapred.database.HitsByMonth;
import org.commoncrawl.mapred.database.PostFrequencyInfo;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.DomainMetadataTask;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.GoogleURL;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLUtils;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * 
 * @author rana
 *
 */
public class ScanDatabaseStep extends CrawlPipelineStep implements Mapper<TextBytes, TextBytes, TextBytes, HitsByMonth> {

  enum Counters {
    MATCHED_NESTED_INDEX_HTML_PATTERN, MATCHED_NESTED_INDEX_PATTERN, MATCHED_INDEX_PATTERN, MATCHED_INDEX_HTML_PATTERN,
    MATCHED_TOP_LEVEL_POST_PATTERN, MATCHED_TOP_PATTERN, MATCHED_NESTED_POST_PATTERN, MATCHED_TUMBLR_BLOG_POST_PATTERN,
    CAUGHT_EXCEPTION_DURING_METADATA_PARSE, DETECTED_WORDPRESS_DURING_METADATA_PARSE,
    DETECTED_BLOGGER_DURING_METADATA_PARSE, DETECTED_TYPEPAD_DURING_METADATA_PARSE,
    CAUGHT_EXCEPTION_DURING_TUMBLR_POST_PARSE
  }

  public static final String OUTPUT_DIR_NAME = "phase-1";

  private static final Log LOG = LogFactory.getLog(ScanDatabaseStep.class);

  static Pattern topLevelBlogPattern = Pattern.compile("http://([^/?]*)/([0-9]{4})/([0-9]{2})/.*$");

  static Pattern nestedBlogPattern = Pattern.compile("http://([^/?]*)/([^/?]*)/([0-9]{4})/([0-9]{2})/.*$");
  static Pattern indexHTMLBlogPattern = Pattern.compile("http://([^/?]*)/([0-9]{4})/([0-9]{2})/index.html$");
  static Pattern indexBlogPattern = Pattern.compile("http://([^/?]*)/([0-9]{4})/([0-9]{2})/$");
  static Pattern nestedIndexHTMLBlogPattern = Pattern
      .compile("http://([^/?]*)/([^/?]*)/([0-9]{4})/([0-9]{2})/index.html$");
  static Pattern nestedIndexBlogPattern = Pattern.compile("http://([^/?]*)/([^/?]*)/([0-9]{4})/([0-9]{2})/$");
  static Pattern tumblrStyleBlogPattern = Pattern.compile("http://([^/?]*)/post/([0-9]{5,})/[^/]*[/]*$");
  JsonParser parser = new JsonParser();

  /** default constructor (for mapper) **/
  public ScanDatabaseStep() {
    super(null, null, null);
  }

  /** step constructor **/
  public ScanDatabaseStep(CrawlPipelineTask task) throws IOException {
    super(task, task.getDescription() + " - Phase 1", OUTPUT_DIR_NAME);
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
  public void map(TextBytes key, TextBytes jsonMetadata, OutputCollector<TextBytes, HitsByMonth> collector,
      Reporter reporter) throws IOException {

    String url = key.toString();

    Matcher topLevelMatcher = topLevelBlogPattern.matcher(url);
    Matcher nestedBlogMatcher = nestedBlogPattern.matcher(url);
    Matcher indexHTMLBlogMatcher = indexHTMLBlogPattern.matcher(url);
    Matcher indexBlogMatcher = indexBlogPattern.matcher(url);
    Matcher nestedIndexHTMLBlogMatcher = nestedIndexHTMLBlogPattern.matcher(url);
    Matcher nestedIndexBlogMatcher = nestedIndexBlogPattern.matcher(url);
    Matcher tumblrPostMatcher = tumblrStyleBlogPattern.matcher(url);

    if (indexHTMLBlogMatcher.matches() && indexHTMLBlogMatcher.groupCount() >= 1) {
      reporter.incrCounter(Counters.MATCHED_INDEX_HTML_PATTERN, 1);
      HitsByMonth hits = new HitsByMonth();
      hits.setFlags(PostFrequencyInfo.Flags.HAS_INDEX_HTML_AFTER_DATE);
      collector.collect(new TextBytes("http://" + indexHTMLBlogMatcher.group(1) + "/"), hits);
    } else if (indexBlogMatcher.matches() && indexBlogMatcher.groupCount() >= 1) {
      reporter.incrCounter(Counters.MATCHED_INDEX_PATTERN, 1);
      HitsByMonth hits = new HitsByMonth();
      hits.setFlags(PostFrequencyInfo.Flags.HAS_YEAR_MONTH_SLASH_INDEX);
      collector.collect(new TextBytes("http://" + indexBlogMatcher.group(1) + "/"), hits);
    } else if (nestedIndexHTMLBlogMatcher.matches() && nestedIndexHTMLBlogMatcher.groupCount() >= 2) {
      reporter.incrCounter(Counters.MATCHED_NESTED_INDEX_HTML_PATTERN, 1);
      HitsByMonth hits = new HitsByMonth();
      hits.setFlags(PostFrequencyInfo.Flags.HAS_INDEX_HTML_AFTER_DATE);
      collector.collect(new TextBytes("http://" + nestedIndexHTMLBlogMatcher.group(1) + "/"
          + nestedIndexHTMLBlogMatcher.group(2) + "/"), hits);
    } else if (nestedIndexBlogMatcher.matches() && nestedIndexBlogMatcher.groupCount() >= 2) {
      reporter.incrCounter(Counters.MATCHED_NESTED_INDEX_PATTERN, 1);
      HitsByMonth hits = new HitsByMonth();
      hits.setFlags(PostFrequencyInfo.Flags.HAS_YEAR_MONTH_SLASH_INDEX);
      collector.collect(new TextBytes("http://" + nestedIndexBlogMatcher.group(1) + "/"
          + nestedIndexBlogMatcher.group(2) + "/"), hits);
    } else if (tumblrPostMatcher.matches() && tumblrPostMatcher.groupCount() >= 2) {
      reporter.incrCounter(Counters.MATCHED_TUMBLR_BLOG_POST_PATTERN, 1);

      String uniqueURL = new String("http://" + tumblrPostMatcher.group(1) + "/");

      try {
        // HACK
        long postId = Long.parseLong(tumblrPostMatcher.group(2));
        long relativeMonth = postId / 1000000000L;
        Date dateStart = new Date(110, 6, 1);
        Date dateOfPost = new Date(dateStart.getTime() + (relativeMonth * 30 * 24 * 60 * 60 * 1000));

        HitsByMonth hits = new HitsByMonth();
        hits.setHitCount(1);
        hits.setYear(dateOfPost.getYear() + 1900);
        hits.setMonth(dateOfPost.getMonth() + 1);

        collector.collect(new TextBytes(uniqueURL), hits);
      } catch (Exception e) {
        reporter.incrCounter(Counters.CAUGHT_EXCEPTION_DURING_TUMBLR_POST_PARSE, 1);
        LOG.error("Exception parsing url:" + url + " Exception:" + StringUtils.stringifyException(e));
      }

    } else if (topLevelMatcher.matches() && topLevelMatcher.groupCount() >= 3) {

      reporter.incrCounter(Counters.MATCHED_TOP_LEVEL_POST_PATTERN, 1);

      String uniqueURL = new String("http://" + topLevelMatcher.group(1) + "/");
      int year = Integer.parseInt(topLevelMatcher.group(2));
      int month = Integer.parseInt(topLevelMatcher.group(3));

      HitsByMonth hits = new HitsByMonth();
      hits.setHitCount(1);
      hits.setYear(year);
      hits.setMonth(month);

      hits.setFlags(scanForGenerator(key, jsonMetadata, reporter));

      collector.collect(new TextBytes(uniqueURL), hits);
    } else if (nestedBlogMatcher.matches() && nestedBlogMatcher.groupCount() >= 4) {

      reporter.incrCounter(Counters.MATCHED_NESTED_POST_PATTERN, 1);

      if (!nestedBlogMatcher.group(1).endsWith("tumblr.com")) {
        String uniqueURL = new String("http://" + nestedBlogMatcher.group(1) + "/" + nestedBlogMatcher.group(2) + "/");

        int year = Integer.parseInt(nestedBlogMatcher.group(3));
        int month = Integer.parseInt(nestedBlogMatcher.group(4));

        HitsByMonth hits = new HitsByMonth();
        hits.setHitCount(1);
        hits.setYear(year);
        hits.setMonth(month);

        hits.setFlags(scanForGenerator(key, jsonMetadata, reporter));

        collector.collect(new TextBytes(uniqueURL), hits);
      }
    }

  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {
    LOG.info("Task Identity Path is:" + getTaskIdentityPath());
    LOG.info("Temp Path is:" + outputPathLocation);

    ImmutableList<Path> paths = new ImmutableList.Builder<Path>().addAll(
        ((DomainMetadataTask) getTask().getTask()).getMergeDBDataPaths()).build();

    JobConf job = new JobBuilder(getDescription() + " - Phase 1", getConf()).inputIsSeqFile().inputs(paths).mapper(
        ScanDatabaseStep.class).keyValue(TextBytes.class, HitsByMonth.class).numReducers(
        CrawlEnvironment.NUM_DB_SHARDS / 2).output(outputPathLocation).outputIsSeqFile().build();

    JobClient.runJob(job);

  }

  int scanForGenerator(TextBytes key, TextBytes value, Reporter reporter) {
    try {
      JsonObject containerObj = parser.parse(value.toString()).getAsJsonObject();
      GoogleURL urlObject = new GoogleURL(key.toString());
      if (urlObject.isValid()) {
        String sourceRootDomain = URLUtils.extractRootDomainName(urlObject.getHost());
        if (sourceRootDomain != null) {
          URLFPV2 fp = URLUtils.getURLFPV2FromURLObject(urlObject);

          JsonObject objectOut = new JsonObject();
          if (fp != null) {
            objectOut.addProperty("dh", fp.getDomainHash());
          }

          JsonObject crawlStatus = containerObj.getAsJsonObject("crawl_status");

          if (crawlStatus != null) {
            if (crawlStatus.has("http_result")) {
              int httpResult = crawlStatus.get("http_result").getAsInt();
              if (httpResult == 200) {
                JsonArray crawlStatsArray = crawlStatus.getAsJsonArray("crawl_stats");
                if (crawlStatsArray != null && crawlStatsArray.size() != 0) {
                  JsonObject crawlStats = crawlStatsArray.get(0).getAsJsonObject();
                  if (crawlStats != null) {
                    JsonArray metaTags = crawlStats.getAsJsonArray("meta_tags");
                    if (metaTags != null) {
                      for (JsonElement metaObject : metaTags) {
                        String metaValue = metaObject.getAsJsonObject().get("value").getAsString();
                        if (metaValue.contains("Wordpress")) {
                          reporter.incrCounter(Counters.DETECTED_WORDPRESS_DURING_METADATA_PARSE, 1);
                          return PostFrequencyInfo.Flags.FLAG_GENERATOR_IS_WORDPRESS;
                        } else if (metaValue.contains("blogger")) {
                          reporter.incrCounter(Counters.DETECTED_BLOGGER_DURING_METADATA_PARSE, 1);
                          return PostFrequencyInfo.Flags.FLAG_GENERATOR_IS_BLOGGER;
                        } else if (metaValue.contains("http://www.typepad.com/")) {
                          reporter.incrCounter(Counters.DETECTED_TYPEPAD_DURING_METADATA_PARSE, 1);
                          return PostFrequencyInfo.Flags.FLAG_GENERATOR_IS_TYPEPAD;
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    } catch (Exception e) {
      reporter.incrCounter(Counters.CAUGHT_EXCEPTION_DURING_METADATA_PARSE, 1);
      LOG.error("Key:" + key.toString() + " Value:" + value.toString() + "\n" + StringUtils.stringifyException(e));
    }
    return 0;
  }

}