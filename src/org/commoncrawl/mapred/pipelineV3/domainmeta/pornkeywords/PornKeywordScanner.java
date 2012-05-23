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

package org.commoncrawl.mapred.pipelineV3.domainmeta.pornkeywords;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;
import org.commoncrawl.util.GoogleURL;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLUtils;
import org.commoncrawl.util.Tuples.Pair;
import org.commoncrawl.util.Tuples.Triple;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

/**
 * 
 * @author rana
 *
 */
public class PornKeywordScanner implements Mapper<TextBytes, TextBytes, TextBytes, TextBytes> {

  enum Counters {
    GOT_CRAWL_STATUS, GOT_HTTP_RESULT, RESULT_WAS_HTTP_200, GOT_CRAWL_STATS_ARRAY, GOT_CRAWL_STATS_OBJECT,
    GOT_PORN_HIT_IN_TITLE, GOT_PORN_HIT_IN_META, GOT_EXCEPTION_DURING_DETECTION, GOT_TITLE_IN_JSON_OBJ,
    GOT_META_TAGS_IN_JSON_OBJ, GOT_KEYWORD_HITS_IN_ROOT_DOMAIN, GOT_KEYWORD_HITS_IN_SUB_DOMAIN,
    GOT_KEYWORD_HITS_IN_PATH
  }

  private static final Log LOG = LogFactory.getLog(PornKeywordScanner.class);

  ImmutableList<String> regExList = new ImmutableList.Builder<String>()

      .add(
          "\\b(flexible girl|flexible girls|girls lingerie|girls high heels|little girls nude|cyber girls|sexy hot girls|sex girl|naked college girls|naked little girls|hot girl|hot nude girls|girls lingerie|beautiful girls|girls high heels|girls bikini|girls on webcams|girls webcams|live girls|girls webcam)\\b")
      .add(
          "\\b(sexy videos|photo sexy|webcam live|video playmate|play boy|jurgita valts|playmates videos|karen mcdougal|playboytv|web cam live|sexy photos|playboyplus|nude women|playboytv com|sexy naked women|hugh hefner|playmate|cumshot|shemale|footjob|housewife galleries|schoolgirls pictures|allwam|xnxx galleries|stockings|retro porn|twistys|bukkake|twistys com|high heels pictures|bikini thumbs|upskirt|spanking galleries|titjob|handjob pictures|shaved pussy|pissing|spanking gallery|3d porn|xxnx com|anal|xnxx|adult 89 com|hairy movie|mature cunt|thai xxx|pussy org|juicy gals|89 com xxx|comics xxx|bigcock|comic xxx|small tits|89 com movies|lingerie mania|asian lesbians|voyeur russian|granny hairy|http www 89 com|asian teen|89 com video|fat fucking|lesbian fuck|upskirt|asian facials|bigtit|teens porno|handjob|black butt|little teen|anal hardcore|sex sites|porn 89|big cocks|ass hunter|big cock|www pichunter|xxx petite|pichunter|ass gallery|hunter sex|xxx pictures|tiny tits|picture hunter|pussy hunter|cock pictures|live webcams|porn world|sex teen movies|biggest tits in the world|worldsex|sexy pics|teen movies|freehardcore|hardcoresex|pornstars|hard cock)\\b")
      .add(
          "\\b(suck cock|tits sucking|suck dick|sucking dick|suck tits|divas hot|hot shoes|hot bottom|hot shorts|hot bra|hot nylons|hot swimsuit|hot bikini|hot gals|hot dress|hot bodies|hot single girl|hot thongs|hot rods|hot girl|hot females|hot bottoms|hot super models|hot bras|hot bikinis|hot outfit|hot pants|hot latinas|hot garters|hot swimsuits|hot underwear|adult thumbs|galleries adult|fuck adult|adult pic|adult pics|adult pussy|89 adult|asian teens|teen suck|teen porno|sexy teens|teens xxx|porno teens|xxx teen|teen hunter|big tits asian|big tits|big cocks|big cunt|teen hardcore sex|hardcore sex|hardcore sex vids|anal hardcore sex|lesbian gallery|xnxx galleries|mature movie galleries|galleries pussy|teen photos|teen com|teen sexy|teen forum|teen pussy|teen videos|sexy teens|teen video|hot teen clothes|teen new|teen www|fuck sexy|slut fuck|sucking and fucking|teens fuck|lesbian fuck|fucking teens|fucking machines|fucking hard|fucking grannies|teen fucking|fuck hardcore|fucking lesbians|fuck hard)\\b")
      .add(
          "\\b(anime sex movies|black sex movies|adult sex movies|asian sex movies|sex movies|hardcore sex movies|world sex movies|sex movies|sex movies hot|mature sex movies|sex moviehot movies|nude movies|porn movies|movies sexycock sucking movies|porno movies|movies galleries|movies sexy|movies xxxsex teen movies|x rated)\\b")
      .add(
          "\\b(sexy nude pics|naked pics|naked pictures|nude photos|amateur pics|nude scenes|pic sex|hard pics|fuck pic|lesbian pic|pic hunter|pussy pic|teen pic|pic sexy|blowjob pics|hunter pics|hot pic|girl sexy pic|pic xxx|ass pic|pic hunter com|sex pics anal|foot pics|feet pics|high heels pics|latex pics|toe pics|gothic pics|teen photos|teen com|teen sexy|teen forum|porn pictures|teen pussy|pictures xxx|teen videos|sexy teens|teen video|hot teen clothes|teen new|xxx picture|sexy pictures|teen www)\\b")
      .add(
          "\\b(free porn|porn movies|adult porn movies|pictures of porn|pics porn|porn naughty|adult porn|chinese porn|hungarian porn|porn granny|69 porn|porn 89|thai porn|bride porn|89 movies porn|dutch porn|brazilian porn|porn secretary|russian porn|porn directories|ass porn|porn stockings|porn teens|sexy porn|porn girl|movies porn|young girls porn|porn fucking|housewife porn|young porn|girl on girl porn|blond porn|porn hunter|blonde porn|sexy hot porn|porn babe|pic hunter porn|nice porn|fuck porn|hot girl porn|porn gallery|ebony porno|porno lesbians|mature porno|porno hard|pictures porno|picture porno|porno babes|black porno|porno girl|porno gallery)\\b")
      .add(
          "\\b(sexy women|sexy nude women|sexy girls|hottest girls|gorgeous girls|busty babes|hot models|models sex|revista sex|hot sex|sex boy|cam sex|play boy sex|online sex|pictures of sex|anime sex|fuck sex|threesome sex|xxx sex|hardcore lesbian sex|sex hardcore pictures|www sex|pic sex|sex group|blonde sex|anal sex|sex teen|sex anal movies|www world sex|outdoor sex|hot pictures sex|mature sex|fucking sex|pics sex|sex hardcore pics|sex babes|videos sex|sex vids|hot sex|world of sex|sex world|movies of sex|89 movies sex|sex picture|sex pictures|russian sex|swedish sex|brazilian sex|hairy sex|pissing sex|sex secretary|sex cunt|nurse sex|galleries sex|wet sex|sex bikini|89 sex com|group sex pictures|group sex|sex granny|89 video sex|sex 89|young sex|sex movies|sex pictures|sex teens|sex film|asian sex|sex vid?o|sex naughty|asian sex clips|sex photos|video black sex|nurse sex|sex pics|girls sex|video de sex|sex links|sex search|videos de sex|xxx sex|sex pictures|adult sex pictures|adult sex|adult sex sites|photos of sex|videos of sex|pictures of sex|sex com|world sex com|video sex)\\b")
      .add(
          "\\b(porn video|porn video black|asian porn videos|milf video|adult video search|nude video|black videos porn|video naughty|videos porn|video xxx|naughty videos|naughty sex videos|pregnant sex videos|adult sex videos|asian sex videos|sex videos anime|amateur sex videos|hard sex video|asian sex video|anime sex video|amateur sex video|sex videos|black sex videos|adult sex video|amateur sex videos|anime sex videos|hot sex videos|sex news)\\b")
      .add("\\b(porn reviews|porn site reviews|adult reviews|pornsites)\\b")

      .build();

  static final Pattern strongKeywords = Pattern
      .compile("(anal|ass|blowjob|bondage|bukkake|busty|butt|cock|cum|cunt|dick|erotic|fuck|handjob|hardcore|milf|naked|nipple|nude|pissing|porn|pussy|sex|shemale|slut|spanking|teen|threesome|tit|upskirt|xxx)");
  static final Pattern supportingKeywords = Pattern
      .compile("(3d|69|89|adult|amateur|anime|asian|babe|beautiful|big|bikini|black|blond|bodies|bottom|boy|bra|brazilian|bride|cam|celebrities|centerfolds|chinese|clips|clothes|college|comic|core|cyber|directories|divas|dress|dutch|ebony|facial|fat|feet|female|film|flexible|foot|footjob|forum|free|galleries|gallery|gals|garters|gay|girl|gorgeous|gothic|grannies|granny|group|hairy|hard|heels|high|hot|housewife|hungarian|hunter|juicy|jurgita|latex|latina|lesbian|lingerie|little|live|mania|masturbate|mature|moan|models|movie|naughty|nipples|nurse|nylons|outdoor|outfit|orgasm|panties|panty|passionate|penetration|petite|photo|phone|pic|play|pregnant|rated|retro|reviews|rods|russian|scenes|schoolgirls|secretary|shaved|shoes|shot|shorts|single|small|stockings|suck|super|swedish|swimsuit|thai|thongs|thumbs|tiny|toe|toy|twistys|underwear|video|vids|voyeur|webcam|wet|women|world|young)");
  static final Pattern strongKeywordsWithBreaks = Pattern
      .compile("\\b(anal|ass|blowjob|bondage|bukkake|busty|butt|cock|cum|cunt|dick|erotic|fuck|handjob|hardcore|milf|naked|nipple|nude|pissing|porn|pussy|sex|shemale|slut|spanking|teen|threesome|tit|upskirt|xxx)\\b");
  static final Pattern supportingKeywordsWithBreaks = Pattern
      .compile("\\b(3d|69|89|adult|amateur|anime|asian|babe|beautiful|big|bikini|black|blond|bodies|bottom|boy|bra|brazilian|bride|cam|celebrities|centerfolds|chinese|clips|clothes|college|comic|core|cyber|directories|divas|dress|dutch|ebony|facial|fat|feet|female|film|flexible|foot|footjob|forum|free|galleries|gallery|gals|garters|gay|girl|gorgeous|gothic|grannies|granny|group|hairy|hard|heels|high|hot|housewife|hungarian|hunter|juicy|jurgita|latex|latina|lesbian|lingerie|little|live|mania|masturbate|mature|moan|models|movie|naughty|nipples|nurse|nylons|outdoor|outfit|orgasm|panties|panty|passionate|penetration|petite|photo|phone|pic|play|pregnant|rated|retro|reviews|rods|russian|scenes|schoolgirls|secretary|shaved|shoes|shot|shorts|single|small|stockings|suck|super|swedish|swimsuit|thai|thongs|thumbs|tiny|toe|toy|twistys|underwear|video|vids|voyeur|webcam|wet|women|world|young)\\b");

  JsonParser parser = new JsonParser();

  static final double PORN_KEYWORD_IN_TITLE_OR_META_SCORE = .40;
  static final double PORN_SUPPORTING_KEYWORD_IN_TITLE_OR_META_SCORE = .15;
  static final double PORN_FRAGMENT_IN_TITLE_OR_META_SCORE = .50;

  private static double STRONG_KEYWORD_SCORE_VALUE = .15;

  private static double SUBDOMAIN_WEAK_KEYWORD_SCORE_VALUE = .05;

  private static final double MAX_SCORE_VALUE = 1.0;

  public static void main(String[] args) {
    PornKeywordScanner scanner = new PornKeywordScanner();
    scanner.configure(null);
    JsonObject testObj = new JsonObject();
    testObj.addProperty("title", "porn stuff");
    // System.out.println(scanner.titleMatchCount(testObj, null));
    System.out.println(PornKeywordScanner.pornKeywordScoreForURLComponent("adultlivesexchat.com", 0, null).e1);
    System.out.println(PornKeywordScanner.pornKeywordScoreForURLComponent("www3sex.com", 0, null).e1);
    System.out.println(PornKeywordScanner.pornKeywordScoreForURLComponent("www.northessex.com", 0, null).e1);
    System.out.println(PornKeywordScanner.pornKeywordScoreForURLComponent("1-sexy-charme.com", 0, null).e1);
    System.out.println(PornKeywordScanner.pornKeywordScoreForURLComponent("3dcartoonsex.org", 0, null).e1);
    System.out.println(PornKeywordScanner.pornKeywordScoreForURLComponent("assemblyservicesexpress.com", 0, null).e1);
    System.out.println(PornKeywordScanner.pornKeywordScoreForURLComponent("666sex.biz", 0, null).e1);

    String foo = "RESIDENT-evil-hentai.-Bondage-comics-sample-xxxmovie-mpegs--01-01.html";
    String foobar = "hot-chick-with-petite-body-was-fucked-in-these-teen-sex-videos";

    System.out.println(score(foo.toLowerCase(), PORN_KEYWORD_IN_TITLE_OR_META_SCORE,
        PORN_SUPPORTING_KEYWORD_IN_TITLE_OR_META_SCORE, 0.0, MAX_SCORE_VALUE, true, null).e1);

    String test = "Naughty girls alone at home with webcams. One of the biggest sites on the net. More than 1500 girls ready to masturbate with you."
        + "Sexiest webcam babes getting naked and spreading wide for love and tender penetrations is gathered here."
        + "You`ll be impressed with their uncensored action they make to share hot feelings and long lasting orgasms!";
    System.out.println(score(test.toLowerCase(), PORN_KEYWORD_IN_TITLE_OR_META_SCORE,
        PORN_SUPPORTING_KEYWORD_IN_TITLE_OR_META_SCORE, 0.0, MAX_SCORE_VALUE, true, null).e1);
  }

  static Pair<Set<String>, Double> pornKeywordScoreForURLComponent(String component, double weakToStrongKeywordBoost,
      Set<String> optionalIgnoreKeywordSet) {
    return score(component, STRONG_KEYWORD_SCORE_VALUE, SUBDOMAIN_WEAK_KEYWORD_SCORE_VALUE, weakToStrongKeywordBoost,
        MAX_SCORE_VALUE, false, optionalIgnoreKeywordSet);
  }

  static Pair<Set<String>, Double> score(String component, double keywordScore, double supportingKeywordScore,
      double weakToStrongBoostFactor, double maxPossibleScore, boolean useLineBreaks, Set<String> ignoreKeywords) {

    Pair<Set<String>, Double> resultOut = new Pair<Set<String>, Double>(ignoreKeywords, 0.0);

    Pattern keywordPattern = (useLineBreaks) ? strongKeywordsWithBreaks : strongKeywords;
    Pattern supportingKeywordPattern = (useLineBreaks) ? supportingKeywordsWithBreaks : supportingKeywords;

    Matcher m = keywordPattern.matcher(component);
    // int lastMatchEnd = -1;
    ArrayList<Triple<Integer, Integer, Double>> matches = null;

    while (m.find()) {

      String keyword = component.substring(m.start(), m.end() + 1);

      if (ignoreKeywords == null || !ignoreKeywords.contains(keyword)) {
        if (matches == null) {
          matches = new ArrayList<Triple<Integer, Integer, Double>>();
        }
        matches.add(new Triple<Integer, Integer, Double>(m.start(), m.end(), keywordScore));

        if (resultOut.e0 == null) {
          resultOut.e0 = new HashSet<String>();
        }
        resultOut.e0.add(keyword);
      }
    }

    m = supportingKeywordPattern.matcher(component);
    while (m.find()) {
      if (matches == null) {
        matches = new ArrayList<Triple<Integer, Integer, Double>>();
      }
      matches.add(new Triple<Integer, Integer, Double>(m.start(), m.end(), supportingKeywordScore));
    }

    if (matches != null) {
      int matchIndex = 0;
      if (!useLineBreaks) {

        Collections.sort(matches, new Comparator<Triple<Integer, Integer, Double>>() {

          @Override
          public int compare(Triple<Integer, Integer, Double> o1, Triple<Integer, Integer, Double> o2) {
            return o1.e0 < o2.e0 ? -1 : o1.e0 > o2.e0 ? 1 : 0;
          }
        });

        // not line break mode ... sniff for token terminations and boost
        // accordingly ...
        for (Triple<Integer, Integer, Double> match : matches) {
          double boost = 1.0;
          // if match is at start of string, or if match is at next char pos
          // after last match, or if the last char at one pos prior to start was
          // not a letter ...
          if (match.e0 == 0 || (matchIndex != 0 && matches.get(matchIndex - 1).e1 == (match.e0))
              || !Character.isLetter(component.charAt(match.e0 - 1))) {
            // boost 50%
            boost = 1.5;
          }
          // if match terminates string, or if match terminates on next match,
          // or if the last char at one pos after end is not a letter ...

          if (match.e1 == component.length()
              || (matchIndex + 1 != matches.size() && matches.get(matchIndex + 1).e0 == match.e1)
              || !Character.isLetter(component.charAt(match.e1))) {
            boost *= 3.0; // +300%
          }
          match.e2 *= boost;
          matchIndex++;
        }
      }

      double score = 0.0;

      for (Triple<Integer, Integer, Double> match : matches) {
        score += match.e2;
      }
      resultOut.e1 = Math.min(score, maxPossibleScore) / maxPossibleScore;
    }

    return resultOut;
  }

  ImmutableList<Pattern> _patterns = null;

  @Override
  public void close() throws IOException {

  }

  @Override
  public void configure(JobConf job) {
    ImmutableList.Builder<Pattern> builder = new ImmutableList.Builder<Pattern>();

    for (String regEx : regExList) {
      builder.add(Pattern.compile(regEx));
    }
    _patterns = builder.build();
  }

  @Override
  public void map(TextBytes key, TextBytes value, OutputCollector<TextBytes, TextBytes> output, Reporter reporter)
      throws IOException {
    try {
      JsonObject containerObj = parser.parse(value.toString()).getAsJsonObject();
      GoogleURL urlObject = new GoogleURL(key.toString());
      if (urlObject.isValid()) {
        String sourceRootDomain = URLUtils.extractRootDomainName(urlObject.getHost());
        if (sourceRootDomain != null) {

          double maxPossibleHostScore = 1.0;
          double hostComponentScore = 0;
          Pair<Set<String>, Double> hostScoreResult = pornKeywordScoreForURLComponent(sourceRootDomain, 0, null);

          hostComponentScore = hostScoreResult.e1;

          if (sourceRootDomain.compareTo(urlObject.getHost()) != 0) {

            hostScoreResult = pornKeywordScoreForURLComponent(urlObject.getHost().substring(0,
                urlObject.getHost().length() - sourceRootDomain.length()), 0, hostScoreResult.e0);

            hostComponentScore += hostScoreResult.e1;

            maxPossibleHostScore += 1.0;
          }
          Pair<Set<String>, Double> pathScoreResult = pornKeywordScoreForURLComponent(
              urlObject.getPath().toLowerCase(), 0.0, hostScoreResult.e0);
          double pathComponentScore = pathScoreResult.e1;

          hostComponentScore = hostComponentScore / maxPossibleHostScore;

          // ok check to see if it starts with a number ...
          if (Character.isDigit(sourceRootDomain.charAt(0))) {
            // boost score ..
            hostComponentScore = Math.min(hostComponentScore * 1.3, 1.0);
          }

          double titleMetaScore = 0.0;
          double scoredItems = 0;
          JsonArray arrayOut = new JsonArray();

          JsonObject crawlStatus = containerObj.getAsJsonObject("crawl_status");
          if (crawlStatus != null) {
            reporter.incrCounter(Counters.GOT_CRAWL_STATUS, 1);
            arrayOut.add(new JsonPrimitive(true));
            if (crawlStatus.has("http_result")) {
              int httpResult = crawlStatus.get("http_result").getAsInt();
              reporter.incrCounter(Counters.GOT_HTTP_RESULT, 1);
              arrayOut.add(new JsonPrimitive((httpResult == 200)));
              if (httpResult == 200) {
                reporter.incrCounter(Counters.RESULT_WAS_HTTP_200, 1);
                JsonArray crawlStatsArray = crawlStatus.getAsJsonArray("crawl_stats");
                if (crawlStatsArray != null && crawlStatsArray.size() != 0) {
                  reporter.incrCounter(Counters.GOT_CRAWL_STATS_ARRAY, 1);
                  for (JsonElement element : crawlStatsArray) {
                    reporter.incrCounter(Counters.GOT_CRAWL_STATS_OBJECT, 1);
                    JsonObject crawlStat = element.getAsJsonObject();
                    Pair<Set<String>, Double> titleScoreResult = titleScore(crawlStat, reporter, null);
                    double titleScore = titleScoreResult.e1;
                    if (titleScore != 0) {
                      titleMetaScore += Math.pow(titleScore, 2);
                      scoredItems++;
                    }

                    Pair<Set<String>, Double> metaScoreResult = metaScore(crawlStat, reporter, null);
                    double metaScore = metaScoreResult.e1;
                    if (metaScore != 0) {
                      titleMetaScore += Math.pow(metaScore, 2);
                      scoredItems++;
                    }
                  }
                }
              }
            }
          }

          if (titleMetaScore != 0) {
            titleMetaScore = Math.sqrt(titleMetaScore) / Math.sqrt(scoredItems);
          }

          if (hostComponentScore != 0.0 || titleMetaScore != 0.0 || pathComponentScore != 0.0) {

            JsonObject pornHitObj = new JsonObject();

            pornHitObj.addProperty("source", key.toString());
            pornHitObj.addProperty("host-score", hostComponentScore);
            pornHitObj.addProperty("path-score", pathComponentScore);
            pornHitObj.addProperty("titlemeta-score", titleMetaScore);

            output.collect(new TextBytes(sourceRootDomain), new TextBytes(pornHitObj.toString()));
          } else {
            JsonObject pornHitObj = new JsonObject();
            pornHitObj.addProperty("no-score", 0);
            output.collect(new TextBytes(sourceRootDomain), new TextBytes(pornHitObj.toString()));
          }
        }
      }
    } catch (Exception e) {
      reporter.incrCounter(Counters.GOT_EXCEPTION_DURING_DETECTION, 1);
      LOG.error(StringUtils.stringifyException(e));
    }
  }

  double matchFragments(String text, double perMatchedFragmentScore) {
    double scoreOut = 0;
    for (Pattern p : _patterns) {
      Matcher m = p.matcher(text);
      if (m.find()) {
        scoreOut += perMatchedFragmentScore;
      }
    }
    return scoreOut;
  }

  Pair<Set<String>, Double> metaScore(JsonObject object, Reporter reporter, Set<String> ignoreKeywords) {
    double scoreOut = 0.0;
    Pair<Set<String>, Double> scoreResult = new Pair<Set<String>, Double>(ignoreKeywords, 0.0);

    JsonArray metaTags = object.getAsJsonArray("meta_tags");
    if (metaTags != null) {
      if (reporter != null)
        reporter.incrCounter(Counters.GOT_META_TAGS_IN_JSON_OBJ, 1);

      for (JsonElement meta : metaTags) {
        JsonObject metaObject = meta.getAsJsonObject();
        JsonElement value = metaObject.get("value");
        if (value != null) {
          String valueStr = value.getAsString().toLowerCase();
          scoreResult = score(valueStr, PORN_KEYWORD_IN_TITLE_OR_META_SCORE,
              PORN_SUPPORTING_KEYWORD_IN_TITLE_OR_META_SCORE, 0.0, MAX_SCORE_VALUE, true, scoreResult.e0);
          scoreOut = Math.max(scoreOut, scoreResult.e1);
        }
      }
    }
    scoreResult.e1 = scoreOut;
    return scoreResult;
  }

  Pair<Set<String>, Double> titleScore(JsonObject object, Reporter reporter, Set<String> ignoreKeywords) {

    Pair<Set<String>, Double> scoreOut = new Pair<Set<String>, Double>(ignoreKeywords, 0.0);

    JsonElement title = object.get("title");
    if (title != null) {
      if (reporter != null)
        reporter.incrCounter(Counters.GOT_TITLE_IN_JSON_OBJ, 1);
      String titleStr = title.getAsString().toLowerCase();
      scoreOut = score(titleStr, PORN_KEYWORD_IN_TITLE_OR_META_SCORE, PORN_SUPPORTING_KEYWORD_IN_TITLE_OR_META_SCORE,
          0.0, MAX_SCORE_VALUE, true, scoreOut.e0);
    }
    return scoreOut;
  }
}
