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

package org.commoncrawl.mapred.pipelineV3.domainmeta.blogs.feedurlid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeMap;
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
import org.commoncrawl.util.GoogleURL;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.TextBytes;

import com.google.common.collect.MinMaxPriorityQueue;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 *  @author rana
 */
public class FeedUrlIdStep extends CrawlPipelineStep implements Mapper<TextBytes, TextBytes, TextBytes, TextBytes>,
    Reducer<TextBytes, TextBytes, TextBytes, TextBytes> {

  enum Counters {
    GOT_EXCEPTION_PROCESSING_MAP, GOT_EXCEPTION_PROCESSING_REDUCE, REJECTED_HOST_TOO_MANY_KEYS,
    REJECTED_NESTED_BLOG_POST_FEED_ITEM, DETECTED_TOO_MANY_HOST_KEYS, REJECTED_EXCEEDED_MAX_COLLAPSED_CANDIDATES,
    REJECTED_TAG_FEED_ITEM, SKIPPING_BLOG_PLATFORM_URL

  }

  static class URLCandidate implements Comparable<URLCandidate> {

    String[] parts;

    GoogleURL urlObject;

    URLCandidate(GoogleURL urlObject) {
      this.urlObject = urlObject;
      String path = urlObject.getPathAndQuery();
      if (path.charAt(0) == '/') {
        this.parts = path.substring(1).split("/");
      } else {
        this.parts = path.split("/");
      }
    }

    @Override
    public int compareTo(URLCandidate o) {
      int result = parts.length < o.parts.length ? -1 : (parts.length > o.parts.length) ? 1 : 0;
      if (result == 0) {
        for (int i = 0; i < parts.length; ++i) {
          result = parts[i].compareTo(o.parts[i]);
          if (result != 0)
            break;
        }
      }
      return result;
    }

    @Override
    public String toString() {
      return urlObject.getCanonicalURL();
    }
  }

  public static final String OUTPUT_DIR_NAME = "feedURLIdentifier";

  private static final Log LOG = LogFactory.getLog(FeedUrlIdStep.class);

  JsonParser parser = new JsonParser();

  static final int HAS_ATOM_XML = 1 << 0;

  static final int HAS_FEED_XML = 1 << 1;

  JsonObject objectOut = new JsonObject();

  Pattern atomRSSPattern = Pattern.compile(".*(application/atom.xml|application/rss.xml).*");
  Pattern blogPostFeedPattern = Pattern.compile(".*/[0-9]{4}/[0-9]{2}/.*");

  Pattern tagPattern = Pattern.compile(".*/tag/.*");

  Pattern blogPlatformURLPattern = Pattern.compile("http://[^/]*.(blogspot|wordpress|tumblr|typepad).com/.*");
  TreeMap<String, JsonObject> treeMap = new TreeMap<String, JsonObject>();
  static final int MAX_SAMPLES = 100;
  static final int MAX_SAME_LEVEL_SAMPLES = 4;

  private static final int SIMILARITY_THRESHOLD = 7;

  static ArrayList<URLCandidate> collapseCandidates(ArrayList<URLCandidate> candidateList, int similarityThreshold) {

    for (int i = 0; i < candidateList.size(); ++i) {
      URLCandidate head = candidateList.get(i);
      if (head.parts.length >= 2 && head.parts[head.parts.length - 1].equals("feed")) {
        boolean endsWithSlash = head.urlObject.getCanonicalURL().endsWith("/");
        int trailingItemIndex = head.parts.length - 1;
        int similar = 0;
        for (int j = i + 1; j < candidateList.size(); ++j) {
          URLCandidate other = candidateList.get(j);
          if (other.parts.length == head.parts.length) {
            if (other.parts[trailingItemIndex].equals("feed")) {
              if (head.parts.length == 2
                  || head.parts[trailingItemIndex - 2].equals(other.parts[trailingItemIndex - 2])) {
                similar++;
              }
            }
          }
        }
        if (similar >= similarityThreshold) {
          for (int k = 0; k < similar; ++k) {
            candidateList.remove(i + 1);
          }
          candidateList.remove(i);
          StringBuffer finalString = new StringBuffer("/");
          for (int k = 0; k <= trailingItemIndex - 2; ++k) {
            finalString.append(head.parts[k]);
            finalString.append("/");
          }
          finalString.append("feed");
          if (endsWithSlash)
            finalString.append("/");

          candidateList.add(new URLCandidate(new GoogleURL(head.urlObject.getScheme() + "://"
              + head.urlObject.getHost() + finalString.toString())));
        }
      }
    }

    for (int i = 0; i < candidateList.size(); ++i) {
      URLCandidate head = candidateList.get(i);
      String prefix = getPrefixGivenPath(head.urlObject.getPathAndQuery());
      if (prefix.length() != 0) {
        for (int j = i + 1; j < candidateList.size();) {
          if (candidateList.get(j).urlObject.getPathAndQuery().startsWith(prefix)) {
            candidateList.remove(j);
          } else {
            ++j;
          }
        }
      }
    }
    return candidateList;
  }

  public static ArrayList<URLCandidate> drainToArrayList(MinMaxPriorityQueue<URLCandidate> queue) {
    int queueSize = queue.size();
    ArrayList<URLCandidate> list = new ArrayList<URLCandidate>(queueSize);
    for (int i = 0; i < queueSize; ++i) {
      list.add(queue.removeFirst());
    }
    return list;
  }

  static String getPrefixGivenPath(String path) {
    int lastIndexOfSlash;
    if (path.lastIndexOf('/') == path.length() - 1) {
      lastIndexOfSlash = path.lastIndexOf('/', path.length() - 2);
    } else {
      lastIndexOfSlash = path.lastIndexOf('/');
    }

    if (lastIndexOfSlash != -1) {
      return path.substring(0, lastIndexOfSlash);
    }
    return path;
  }

  public static void main(String[] args) {
    ArrayList<URLCandidate> candidates = new ArrayList<URLCandidate>();
    candidates.add(new URLCandidate(new GoogleURL("http://2010.goldenplains.com.au/info/feed/")));
    candidates.add(new URLCandidate(new GoogleURL("http://2010.goldenplains.com.au/supernatural-amphitheatre/feed/")));
    candidates.add(new URLCandidate(new GoogleURL("http://2010.goldenplains.com.au/tickets-pre-ballot/feed/")));

    collapseCandidates(candidates, 2);

    System.out.println(candidates.toString());

    MinMaxPriorityQueue<URLCandidate> deque2 = MinMaxPriorityQueue.create();
    deque2.add(new URLCandidate(new GoogleURL("http://2010.goldenplains.com.au/blog/feed/")));
    deque2.add(new URLCandidate(new GoogleURL("http://2010.goldenplains.com.au/blog/supernatural-amphitheatre/feed/")));
    deque2.add(new URLCandidate(new GoogleURL("http://2010.goldenplains.com.au/vlog/tickets-pre-ballot/feed/")));
    deque2.add(new URLCandidate(new GoogleURL("http://2010.goldenplains.com.au/blog/tickets-pre-ballot/feed/")));
    deque2.add(new URLCandidate(new GoogleURL("http://2010.goldenplains.com.au/vlog/tickets-pre-ballot-2/feed/")));
    deque2.add(new URLCandidate(new GoogleURL("http://2010.goldenplains.com.au/vlog/tickets-pre-ballot-3/feed/")));
    ArrayList<URLCandidate> test = drainToArrayList(deque2);
    System.out.println(test);

    collapseCandidates(test, 2);

    System.out.println(test.toString());
  }

  MinMaxPriorityQueue<URLCandidate> candidateList = MinMaxPriorityQueue.maximumSize(MAX_SAMPLES).create();
  ArrayList<URLCandidate> lookAhead = new ArrayList<URLCandidate>();

  public FeedUrlIdStep() {
    super(null, null, null);
  }

  public FeedUrlIdStep(CrawlPipelineTask task) {
    super(task, "Feed URL Identifier", OUTPUT_DIR_NAME);
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
  public void map(TextBytes key, TextBytes value, OutputCollector<TextBytes, TextBytes> output, Reporter reporter)
      throws IOException {
    try {
      if (blogPlatformURLPattern.matcher(key.toString()).matches()) {
        reporter.incrCounter(Counters.SKIPPING_BLOG_PLATFORM_URL, 1);
      } else {
        if (atomRSSPattern.matcher(value.toString()).matches()) {
          JsonObject containerObj = parser.parse(value.toString()).getAsJsonObject();
          JsonObject linkStatus = containerObj.getAsJsonObject("link_status");

          if (linkStatus != null) {
            JsonArray typeAndRels = linkStatus.getAsJsonArray("typeAndRels");
            if (typeAndRels != null) {
              for (JsonElement e : typeAndRels) {
                String parts[] = e.getAsString().split(":");
                if (parts.length == 3
                    && (parts[1].equalsIgnoreCase("application/atom+xml") || parts[1]
                        .equalsIgnoreCase("application/rss+xml"))) {

                  if (blogPostFeedPattern.matcher(key.toString()).matches()) {
                    reporter.incrCounter(Counters.REJECTED_NESTED_BLOG_POST_FEED_ITEM, 1);
                  } else if (tagPattern.matcher(key.toString()).matches()) {
                    reporter.incrCounter(Counters.REJECTED_TAG_FEED_ITEM, 1);
                  } else {
                    GoogleURL urlObject = new GoogleURL(key.toString());
                    if (urlObject.isValid()) {

                      objectOut.addProperty("url", urlObject.getCanonicalURL());
                      objectOut.addProperty("type", parts[1]);
                      output.collect(new TextBytes(urlObject.getHost()), new TextBytes(objectOut.toString()));

                      break;
                    }
                  }
                }
              }
            }
          }
        }
      }
    } catch (Exception e) {
      reporter.incrCounter(Counters.GOT_EXCEPTION_PROCESSING_MAP, 1);
    }
  }

  @Override
  public void reduce(TextBytes key, Iterator<TextBytes> values, OutputCollector<TextBytes, TextBytes> output,
      Reporter reporter) throws IOException {

    candidateList.clear();
    lookAhead.clear();

    try {
      while (values.hasNext()) {
        JsonObject object = parser.parse(values.next().toString()).getAsJsonObject();
        String url = object.get("url").getAsString();
        candidateList.add(new URLCandidate(new GoogleURL(url)));
      }

      if (candidateList.size() != 0) {
        if (candidateList.size() == 1) {
          output.collect(key, new TextBytes(candidateList.peek().urlObject.getCanonicalURL()));
        } else {
          // pop first candidate ...
          URLCandidate firstCandidate = candidateList.removeFirst();

          while (candidateList.peek() != null && candidateList.peek().parts.length == firstCandidate.parts.length) {
            // ok // reject outright if too many candidates of this length ...
            lookAhead.add(candidateList.removeFirst());
          }

          if (lookAhead.size() > MAX_SAME_LEVEL_SAMPLES) {
            reporter.incrCounter(Counters.DETECTED_TOO_MANY_HOST_KEYS, 1);
            return;
          } else {
            output.collect(key, new TextBytes(firstCandidate.urlObject.getCanonicalURL()));
            for (URLCandidate nextCandidate : lookAhead) {
              output.collect(key, new TextBytes(nextCandidate.urlObject.getCanonicalURL()));
            }
          }
        }
      }
    } catch (Exception e) {
      reporter.incrCounter(Counters.GOT_EXCEPTION_PROCESSING_REDUCE, 1);
      LOG.error("Key:" + key + "\n" + StringUtils.stringifyException(e));
    }
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {

    JobConf job = new JobBuilder(getDescription(), getConf())

    .inputs(((DomainMetadataTask) getTask()).getMergeDBDataPaths()).inputIsSeqFile().mapper(FeedUrlIdStep.class)
        .reducer(FeedUrlIdStep.class, false).outputIsSeqFile().output(outputPathLocation).outputKeyValue(
            TextBytes.class, TextBytes.class).numReducers(CrawlEnvironment.NUM_DB_SHARDS / 2).build();

    JobClient.runJob(job);
  }

}
