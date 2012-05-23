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

package org.commoncrawl.mapred.ec2.postprocess.linkCollector;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.FPGenerator;
import org.commoncrawl.util.GoogleURL;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLFPBloomFilter;
import org.commoncrawl.util.URLUtils;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * 
 * @author rana
 *
 */
public class LinkGraphDataEmitter implements Mapper<TextBytes,TextBytes,TextBytes,TextBytes> , Reducer<TextBytes,TextBytes,TextBytes,TextBytes> {

  static final Log LOG = LogFactory.getLog(LinkGraphDataEmitter.class);
  
  enum Counters { 
    FAILED_TO_GET_LINKS_FROM_HTML, NO_HREF_FOR_HTML_LINK, EXCEPTION_IN_MAP, GOT_HTML_METADATA, GOT_FEED_METADATA, EMITTED_ATOM_LINK, EMITTED_HTML_LINK, EMITTED_RSS_LINK, GOT_PARSED_AS_ATTRIBUTE, GOT_LINK_OBJECT, NULL_CONTENT_OBJECT, NULL_LINKS_ARRAY, FP_NULL_IN_EMBEDDED_LINK, SKIPPED_ALREADY_EMITTED_LINK, FOUND_HTTP_DATE_HEADER, FOUND_HTTP_AGE_HEADER, FOUND_HTTP_LAST_MODIFIED_HEADER, FOUND_HTTP_EXPIRES_HEADER, FOUND_HTTP_CACHE_CONTROL_HEADER, FOUND_HTTP_PRAGMA_HEADER, REDUCER_GOT_LINK, REDUCER_GOT_STATUS, ONE_REDUNDANT_LINK_IN_REDUCER, TWO_REDUNDANT_LINKS_IN_REDUCER, THREE_REDUNDANT_LINKS_IN_REDUCER, GT_THREE_REDUNDANT_LINKS_IN_REDUCER, ONE_REDUNDANT_STATUS_IN_REDUCER, TWO_REDUNDANT_STATUS_IN_REDUCER, THREE_REDUNDANT_STATUS_IN_REDUCER, GT_THREE_REDUNDANT_STATUS_IN_REDUCER, GOT_RSS_FEED, GOT_ATOM_FEED, GOT_ALTERNATE_LINK_FOR_ATOM_ITEM, GOT_CONTENT_FOR_ATOM_ITEM, GOT_ITEM_LINK_FROM_RSS_ITEM, GOT_TOP_LEVEL_LINK_FROM_RSS_ITEM, GOT_TOP_LEVEL_LINK_FROM_ATOM_ITEM, EMITTED_REDIRECT_RECORD, DISCOVERED_NEW_LINK, GOT_LINK_FOR_ITEM_WITH_STATUS, FAILED_TO_GET_SOURCE_HREF, GOT_CRAWL_STATUS_NO_LINK, GOT_CRAWL_STATUS_WITH_LINK, GOT_EXTERNAL_DOMAIN_SOURCE, NO_SOURCE_URL_FOR_CRAWL_STATUS, OUTPUT_KEY_FROM_INTERNAL_LINK, OUTPUT_KEY_FROM_EXTERNAL_LINK
  }
  
  JsonParser parser = new JsonParser();

  static JsonObject dateHeadersFromJsonObject(JsonObject jsonObj,Reporter reporter) {
    JsonObject outputHeaders = new JsonObject();
    outputHeaders.addProperty("x-cc-attempt-time",jsonObj.get("attempt_time").getAsLong()); 
    JsonObject headers = jsonObj.getAsJsonObject("http_headers");
    JsonElement httpDate = headers.get("date");
    JsonElement age             = headers.get("age");
    JsonElement lastModified    = headers.get("last-modified");
    JsonElement expires         = headers.get("expires");
    JsonElement cacheControl    = headers.get("cache-control");
    JsonElement pragma    = headers.get("pragma");
    
    if (httpDate != null) { 
      outputHeaders.add("date", httpDate);
      reporter.incrCounter(Counters.FOUND_HTTP_DATE_HEADER, 1);
    }
    if (age != null) { 
      outputHeaders.add("age", age);
      reporter.incrCounter(Counters.FOUND_HTTP_AGE_HEADER, 1);
    }
    if (lastModified != null) { 
      outputHeaders.add("last-modified", lastModified);
      reporter.incrCounter(Counters.FOUND_HTTP_LAST_MODIFIED_HEADER, 1);
    }
    if (expires != null) { 
      outputHeaders.add("expires", expires);
      reporter.incrCounter(Counters.FOUND_HTTP_EXPIRES_HEADER, 1);
    }
    if (cacheControl != null) { 
      outputHeaders.add("cache-control", cacheControl);
      reporter.incrCounter(Counters.FOUND_HTTP_CACHE_CONTROL_HEADER, 1);
    }
    if (pragma != null) { 
      outputHeaders.add("pragma", pragma);
      reporter.incrCounter(Counters.FOUND_HTTP_PRAGMA_HEADER, 1);
    }
    
    return headers;
  }
  
  public static final int  NUM_HASH_FUNCTIONS = 10;
  public static final int  NUM_BITS = 11;
  public static final int  NUM_ELEMENTS = 1 << 28;  
  private URLFPBloomFilter emittedTuplesFilter = new URLFPBloomFilter(NUM_ELEMENTS, NUM_HASH_FUNCTIONS, NUM_BITS);  
  
  
  @Override
  public void map(TextBytes key, TextBytes value,OutputCollector<TextBytes, TextBytes> output, Reporter reporter)throws IOException {
    try { 
      JsonObject o = parser.parse(value.toString()).getAsJsonObject();
      if (o.get("disposition").getAsString().equals("SUCCESS")) {
        if (o.get("parsed_as") != null) {
          JsonObject dateHeaders = dateHeadersFromJsonObject(o,reporter);
          reporter.incrCounter(Counters.GOT_PARSED_AS_ATTRIBUTE, 1);
          if (o.get("parsed_as").getAsString().equals("html")) {
            reporter.incrCounter(Counters.GOT_HTML_METADATA, 1);
            emitLinksFromHTMLContent(key,"html",dateHeaders,o,output,reporter);
          }
          else if (o.get("parsed_as").getAsString().equals("feed")) {
            reporter.incrCounter(Counters.GOT_FEED_METADATA, 1);
            emitLinksFromFeedContent(key,"feed",dateHeaders,o,output,reporter);
          }
        }
      }
    }
    catch (Exception e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      reporter.incrCounter(Counters.EXCEPTION_IN_MAP, 1);
    }
  }
  
  void emitLinksFromFeedContent(TextBytes sourceURL,String sourceType,JsonObject dateHeaders,JsonObject object,OutputCollector<TextBytes, TextBytes> output, Reporter reporter) throws IOException {
    JsonObject contentObject = object.get("content").getAsJsonObject();
    if (contentObject != null) { 
      String type = contentObject.get("type").getAsString();
      if (type.equals("rss-feed")) {
        reporter.incrCounter(Counters.GOT_RSS_FEED, 1);
        emitLinksFromRSSContent(sourceURL, sourceType, dateHeaders, object, contentObject, output, reporter);
      }
      else if (type.equals("atom-feed")) { 
        reporter.incrCounter(Counters.GOT_ATOM_FEED, 1);
        emitLinksFromAtomContent(sourceURL, sourceType, dateHeaders, object, contentObject, output, reporter);
      }
    }
  }
  
  static String safeGetLinkTypeInLink(TextBytes sourceURL,String sourceType,JsonObject dateHeaders,JsonElement linkProperty,String linkType,OutputCollector<TextBytes, TextBytes> output,Reporter reporter) throws IOException { 
    if (linkProperty != null) {
      JsonArray array = null;
      if (linkProperty.isJsonArray()) {
        array = linkProperty.getAsJsonArray();
      }
      else { 
        array = new JsonArray();
        array.add(linkProperty);
      }
      for (int i=0;i<array.size();++i) { 
        JsonObject embeddedLinkObject = array.get(i).getAsJsonObject();
        if (embeddedLinkObject != null && embeddedLinkObject.has("rel") && embeddedLinkObject.has("href")) { 
          JsonElement rel = embeddedLinkObject.get("rel");
          if (rel.getAsString().equals(linkType)) { 
            if (embeddedLinkObject.get("href") != null) {
              if (URLUtils.getURLFPV2FromURL(embeddedLinkObject.get("href").getAsString()) != null) { 
                return embeddedLinkObject.get("href").getAsString();
              }
            }
          }
        }
      }
    }
    return null;
  }
  
  
  void emitLinksFromAtomContent(TextBytes sourceURL,String sourceType,JsonObject dateHeaders,JsonObject object,JsonObject contentObject,OutputCollector<TextBytes, TextBytes> output, Reporter reporter) throws IOException {
    JsonElement items = contentObject.get("items");
    if (items != null) { 
      JsonArray itemsArray = items.getAsJsonArray();
      for (int i=0;i<itemsArray.size();++i) { 
        JsonObject item = itemsArray.get(i).getAsJsonObject();
        String itemURL = safeGetLinkTypeInLink(sourceURL,sourceType,dateHeaders,item.get("link"),"alternate",output,reporter);
        if (itemURL != null) { 
          reporter.incrCounter(Counters.GOT_ALTERNATE_LINK_FOR_ATOM_ITEM, 1);
        }
        JsonObject itemContent = item.getAsJsonObject("content");
        if (itemContent != null) {
          reporter.incrCounter(Counters.GOT_CONTENT_FOR_ATOM_ITEM, 1);

          emitLinksFromHTMLContent(
              (itemURL != null)? new TextBytes(itemURL) : sourceURL,
              "html",dateHeaders,
              item,
              output,reporter);
        }
      }
    }
  }

  static String safeGetRSSLinkTypeInLink(TextBytes sourceURL,String sourceType,JsonObject dateHeaders,JsonElement linkProperty,OutputCollector<TextBytes, TextBytes> output,Reporter reporter) throws IOException { 
    if (linkProperty != null) {
      JsonArray array = null;
      if (linkProperty.isJsonArray()) {
        array = linkProperty.getAsJsonArray();
      }
      else { 
        array = new JsonArray();
        array.add(linkProperty);
      }
      for (int i=0;i<array.size();++i) { 

        JsonObject embeddedLinkObject = array.get(i).getAsJsonObject();
        
        if (embeddedLinkObject.has("href") && URLUtils.getURLFPV2FromURL(embeddedLinkObject.get("href").getAsString()) != null) { 
          return embeddedLinkObject.get("href").getAsString();
        }
      }
    }
    return null;
  }
  
  void emitLinksFromRSSContent(TextBytes sourceURL,String sourceType,JsonObject dateHeaders,JsonObject object,JsonObject contentObject,OutputCollector<TextBytes, TextBytes> output, Reporter reporter) throws IOException {
    JsonElement items = contentObject.get("items");
    if (items != null) { 
      JsonArray itemsArray = items.getAsJsonArray();
      for (int i=0;i<itemsArray.size();++i) { 
        JsonObject item = itemsArray.get(i).getAsJsonObject();
        String itemURL = safeGetRSSLinkTypeInLink(sourceURL,sourceType,dateHeaders,item.get("link"),output,reporter);
        if (itemURL != null) { 
          reporter.incrCounter(Counters.GOT_ITEM_LINK_FROM_RSS_ITEM, 1);
        }
        JsonObject itemContent = item.getAsJsonObject("content");
        if (itemContent != null) { 
          emitLinksFromHTMLContent(
              (itemURL != null)? new TextBytes(itemURL) : sourceURL,
              "html",
              dateHeaders,
              item,
              output,reporter);
        }
      }
    }
  }

  JsonObject toJsonObject = new JsonObject();
  JsonObject fromJsonObject = new JsonObject();
  URLFPV2 bloomKey = new URLFPV2();
  TextBytes sourceDomain = new TextBytes();
  TextBytes valueOut = new TextBytes();
  TextBytes keyOut = new TextBytes();
  
  static final String stripWWW(String host) { 
    if (host.startsWith("www.")) { 
      return host.substring("www.".length());
    }
    return host;
  }

  
  void emitLinksFromHTMLContent(TextBytes sourceURL,String sourceType,JsonObject dateHeaders,JsonObject object,OutputCollector<TextBytes, TextBytes> output, Reporter reporter) throws IOException {
    GoogleURL srcURLObject = new GoogleURL(sourceURL.toString());
   
    if (srcURLObject.isValid()) {
      
      URLFPV2 fpSource = URLUtils.getURLFPV2FromURLObject(srcURLObject);
      
      sourceDomain.set(stripWWW(srcURLObject.getHost()));
      
      try {
        JsonObject contentObject = object.getAsJsonObject("content");
        if (contentObject == null) { 
          reporter.incrCounter(Counters.NULL_CONTENT_OBJECT, 1);
          return;
        }
        else { 
          JsonArray linksArray = contentObject.getAsJsonArray("links");
          if (linksArray == null) {
            reporter.incrCounter(Counters.NULL_LINKS_ARRAY, 1);
          }
          else { 
            for (int i=0;i<linksArray.size();++i) {
              reporter.incrCounter(Counters.GOT_LINK_OBJECT, 1);
              JsonObject linkObject = linksArray.get(i).getAsJsonObject();
              JsonElement href = linkObject.get("href");
              if (href != null) { 
                GoogleURL destURLObject = new GoogleURL(href.getAsString());
                URLFPV2 linkFP = URLUtils.getURLFPV2FromURLObject(destURLObject);
                
                if (linkFP == null) { 
                  reporter.incrCounter(Counters.FP_NULL_IN_EMBEDDED_LINK, 1);
                }
                else {
                  if (linkFP.getRootDomainHash() != fpSource.getRootDomainHash() || linkFP.getDomainHash() != fpSource.getDomainHash()) {
                    // ok create our fake bloom key (hack) ... 
                    // we set domain hash to be source domain's domain hash 
                    bloomKey.setDomainHash(fpSource.getDomainHash());
                    // and url hash to dest domains domain hash
                    bloomKey.setUrlHash(linkFP.getDomainHash());
                    // the bloom filter is global to the reducer, so doing a check
                    // against it before emitting a tuple allows us to limit the number
                    // of redundant tuples produced by the reducer (a serious problem) 
                    if (!emittedTuplesFilter.isPresent(bloomKey)) { 

                      // ok add it to the BF 
                      emittedTuplesFilter.add(bloomKey);
                      
                      // emit a to tuple 
                      toJsonObject.addProperty("t", stripWWW(destURLObject.getHost()));
                      fromJsonObject.addProperty("dh", linkFP.getDomainHash());
                      
                      valueOut.set(toJsonObject.toString());
                      output.collect(sourceDomain, valueOut);
                      // emit a from tuple ... 
                      fromJsonObject.addProperty("f", sourceDomain.toString());
                      fromJsonObject.addProperty("dh", fpSource.getDomainHash());
                      
                      keyOut.set(stripWWW(destURLObject.getHost()));
                      valueOut.set(fromJsonObject.toString());
                      output.collect(keyOut, valueOut);
                    }
                  }
                }
              }
              else { 
                reporter.incrCounter(Counters.NO_HREF_FOR_HTML_LINK, 1);
              }
            }
          }
        }
      }
      catch (Exception e) {
        LOG.error(CCStringUtils.stringifyException(e));
        reporter.incrCounter(Counters.FAILED_TO_GET_LINKS_FROM_HTML, 1);
      }
    }
  }

  @Override
  public void configure(JobConf job) {
    
  }

  @Override
  public void close() throws IOException {
    
  }

  URLFPV2 emittedFPTest = new URLFPV2();
  
  @Override
  public void reduce(TextBytes key, Iterator<TextBytes> values,OutputCollector<TextBytes, TextBytes> output, Reporter reporter)throws IOException {
    
    
    while (values.hasNext()) { 
      TextBytes valueOut = values.next();
      String cumilativeKey = key.toString() + "-" + valueOut.toString();
      long fp = FPGenerator.std64.fp8(cumilativeKey);
      emittedFPTest.setDomainHash(fp);
      emittedFPTest.setUrlHash(fp);
      if (!emittedTuplesFilter.isPresent(emittedFPTest)) {
        emittedTuplesFilter.add(emittedFPTest);
        output.collect(key, valueOut);
      }
    }
  }

}
