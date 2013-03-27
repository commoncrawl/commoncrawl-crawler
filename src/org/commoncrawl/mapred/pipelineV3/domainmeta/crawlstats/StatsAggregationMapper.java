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
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.GoogleURL;
import org.commoncrawl.util.SuperDomainList;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLUtils;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * 
 * @author rana
 *
 */
public class StatsAggregationMapper implements Mapper<TextBytes, TextBytes, TextBytes, TextBytes> {

  enum Counters {
    GOT_CRAWL_STATUS, GOT_HTTP_RESULT, RESULT_WAS_HTTP_200, GOT_CRAWL_STATS_ARRAY, GOT_CRAWL_STATS_OBJECT,
    GOT_EXCEPTION_IN_MAPPER, HIT_TUMBLR_SUB_DOMAIN, HIT_TUMBLR_ROOT_DOMAIN, HIT_TUMBLR_DOMAIN,
    TUMBLR_DOMAIN_DID_NOT_PASS_SUPERDOMAIN_TEST, BAD_ROOT_DOMAIN,

  }

  JsonParser parser = new JsonParser();

  private static final Log LOG = LogFactory.getLog(StatsAggregationMapper.class);

  static final long DURATION_MS_ONE_MONTH = (long)1000 * 60 * 60 * 24 * 30;

  Set<Long> superDomainIdSet;

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
  public void map(TextBytes key, TextBytes value, OutputCollector<TextBytes, TextBytes> output, Reporter reporter)
      throws IOException {

    try {
      JsonObject objectOut = new JsonObject();

      JsonObject containerObj = parser.parse(value.toString()).getAsJsonObject();

      GoogleURL urlObject = new GoogleURL(key.toString());

      if (urlObject.isValid()) {

        URLFPV2 fp = URLUtils.getURLFPV2FromURLObject(urlObject);

        if (fp != null) {

          objectOut.addProperty("dh", fp.getDomainHash());

          String rootDomain = URLUtils.extractRootDomainName(urlObject.getHost());
          if (rootDomain == null) {
            reporter.incrCounter(Counters.BAD_ROOT_DOMAIN, 1);
            return;
          }

          if (rootDomain != null) {
            long rootDomainFP = SuperDomainList.domainFingerprintGivenName(rootDomain);
            if (superDomainIdSet.contains(rootDomainFP)) {
              rootDomain = urlObject.getHost();
            }

            JsonObject crawlStatus = containerObj.getAsJsonObject("crawl_status");

            if (crawlStatus != null) {
              reporter.incrCounter(Counters.GOT_CRAWL_STATUS, 1);
              boolean crawled = crawlStatus.has("http_result");

              objectOut.addProperty("crawled", crawled);
              if (crawled) {
                int httpResult = crawlStatus.get("http_result").getAsInt();
                reporter.incrCounter(Counters.GOT_HTTP_RESULT, 1);
                if (httpResult == 200) {
                  objectOut.addProperty("200", 1);
                  reporter.incrCounter(Counters.RESULT_WAS_HTTP_200, 1);
                  JsonArray crawlStatsArray = crawlStatus.getAsJsonArray("crawl_stats");
                  if (crawlStatsArray != null && crawlStatsArray.size() != 0) {
                    reporter.incrCounter(Counters.GOT_CRAWL_STATS_ARRAY, 1);
                    JsonObject crawlStats = crawlStatsArray.get(0).getAsJsonObject();

                    if (crawlStats != null) {
                      reporter.incrCounter(Counters.GOT_CRAWL_STATS_OBJECT, 1);
                      objectOut.addProperty("server_ip", crawlStats.get("server_ip").getAsString());
                    }
                  }
                } else if (httpResult == 403) {
                  objectOut.addProperty("403", 1);
                } else if (httpResult == 404) {
                  objectOut.addProperty("404", 1);
                }
              }
            }

            JsonObject linkStatus = containerObj.getAsJsonObject("link_status");
            if (linkStatus != null && linkStatus.has("latest_date")) {
              long delta = System.currentTimeMillis() - linkStatus.get("latest_date").getAsLong();
              if (delta < (DURATION_MS_ONE_MONTH * 3)) {
                objectOut.addProperty("recentlyDiscovered", 1);
              }
            }
          }
          output.collect(new TextBytes(rootDomain), new TextBytes(objectOut.toString()));
        }
      }
    } catch (Exception e) {
      reporter.incrCounter(Counters.GOT_EXCEPTION_IN_MAPPER, 1);
      LOG.error(StringUtils.stringifyException(e));
    }
  }

}
