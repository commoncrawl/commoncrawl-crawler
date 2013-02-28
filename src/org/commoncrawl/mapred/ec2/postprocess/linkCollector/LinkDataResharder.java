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
import java.util.HashSet;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.TextBytes;
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
public class LinkDataResharder implements Mapper<Text,Text,TextBytes,TextBytes> , Reducer<TextBytes,TextBytes,TextBytes,TextBytes> {

  static final Log LOG = LogFactory.getLog(LinkDataResharder.class);
  
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
  
  @Override
  public void map(Text key, Text value,OutputCollector<TextBytes, TextBytes> output, Reporter reporter)throws IOException {
    try { 
      JsonObject o = parser.parse(value.toString()).getAsJsonObject();
      long attemptTime = o.get("attempt_time").getAsLong();
      TextBytes crawlStatusKey = LinkKey.generateCrawlStatusKey(key, attemptTime);
      
      if (crawlStatusKey != null) {
        o.addProperty("source_url", key.toString());
        output.collect(crawlStatusKey,new TextBytes(o.toString()));
      }
      HashSet<Long> linkSet = new HashSet<Long>();
      
      if (o.get("disposition").getAsString().equals("SUCCESS")) {
        if (o.get("parsed_as") != null) {
          JsonObject dateHeaders = dateHeadersFromJsonObject(o,reporter);
          reporter.incrCounter(Counters.GOT_PARSED_AS_ATTRIBUTE, 1);
          if (o.get("parsed_as").getAsString().equals("html")) {
            reporter.incrCounter(Counters.GOT_HTML_METADATA, 1);
            emitLinksFromHTMLContent(key,"html",dateHeaders,o,linkSet,output,reporter);
          }
          else if (o.get("parsed_as").getAsString().equals("feed")) {
            reporter.incrCounter(Counters.GOT_FEED_METADATA, 1);
            emitLinksFromFeedContent(key,"feed",dateHeaders,o,linkSet,output,reporter);
          }
        }
      }
    }
    catch (Exception e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      reporter.incrCounter(Counters.EXCEPTION_IN_MAP, 1);
    }
  }
  
  void emitLinksFromFeedContent(Text sourceURL,String sourceType,JsonObject dateHeaders,JsonObject object,HashSet<Long> hashSet,OutputCollector<TextBytes, TextBytes> output, Reporter reporter) throws IOException {
    JsonObject contentObject = object.get("content").getAsJsonObject();
    if (contentObject != null) { 
      String type = contentObject.get("type").getAsString();
      if (type.equals("rss-feed")) {
        reporter.incrCounter(Counters.GOT_RSS_FEED, 1);
        emitLinksFromRSSContent(sourceURL, sourceType, dateHeaders, object, contentObject, hashSet, output, reporter);
      }
      else if (type.equals("atom-feed")) { 
        reporter.incrCounter(Counters.GOT_ATOM_FEED, 1);
        emitLinksFromAtomContent(sourceURL, sourceType, dateHeaders, object, contentObject, hashSet, output, reporter);
      }
    }
  }
  
  static String safeEmitLinkTypeInLink(Text sourceURL,String sourceType,JsonObject dateHeaders,JsonElement linkProperty,String linkType,HashSet<Long> hashSet,OutputCollector<TextBytes, TextBytes> output,Reporter reporter) throws IOException { 
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
        if (embeddedLinkObject != null) { 
          JsonElement rel = embeddedLinkObject.get("rel");
          if (rel.getAsString().equals(linkType)) { 
            if (embeddedLinkObject.get("href") != null) {

              URLFPV2 fp = URLUtils.getURLFPV2FromURL(embeddedLinkObject.get("href").getAsString());
              
              if (fp == null) { 
                reporter.incrCounter(Counters.FP_NULL_IN_EMBEDDED_LINK, 1);
              }
              else {
                
                if (hashSet.contains(fp.getUrlHash())) { 
                  reporter.incrCounter(Counters.SKIPPED_ALREADY_EMITTED_LINK, 1);
                }
                else { 
                  reporter.incrCounter(Counters.EMITTED_ATOM_LINK, 1);
                  
                  String md5Hash = MD5Hash.digest(embeddedLinkObject.toString()).toString();
    
                  embeddedLinkObject.addProperty("source_url",sourceURL.toString());
                  embeddedLinkObject.addProperty("source_type",sourceType);
                  embeddedLinkObject.add("source_headers",dateHeaders);
    
                  TextBytes key = LinkKey.generateLinkKey(fp,LinkKey.Type.KEY_TYPE_HTML_LINK,md5Hash);
                  output.collect(key,new TextBytes(embeddedLinkObject.toString()));
                  
                  hashSet.add(fp.getUrlHash());
                  
                  return embeddedLinkObject.get("href").getAsString();
                }
              }
            }
          }
        }
      }
    }
    return null;
  }
  
  
  void emitLinksFromAtomContent(Text sourceURL,String sourceType,JsonObject dateHeaders,JsonObject object,JsonObject contentObject,HashSet<Long> hashSet,OutputCollector<TextBytes, TextBytes> output, Reporter reporter) throws IOException {
    String topLevelLink = safeEmitLinkTypeInLink(sourceURL,sourceType,dateHeaders,contentObject.get("link"),"alternate",hashSet,output,reporter);
    if (topLevelLink != null) { 
      reporter.incrCounter(Counters.GOT_TOP_LEVEL_LINK_FROM_ATOM_ITEM, 1);
    }
    JsonElement items = contentObject.get("items");
    if (items != null) { 
      JsonArray itemsArray = items.getAsJsonArray();
      for (int i=0;i<itemsArray.size();++i) { 
        JsonObject item = itemsArray.get(i).getAsJsonObject();
        String itemURL = safeEmitLinkTypeInLink(sourceURL,sourceType,dateHeaders,item.get("link"),"alternate",hashSet,output,reporter);
        if (itemURL != null) { 
          reporter.incrCounter(Counters.GOT_ALTERNATE_LINK_FOR_ATOM_ITEM, 1);
        }
        JsonObject itemContent = item.getAsJsonObject("content");
        if (itemContent != null) {
          reporter.incrCounter(Counters.GOT_CONTENT_FOR_ATOM_ITEM, 1);

          emitLinksFromHTMLContent(
              (itemURL != null)? new Text(itemURL) : sourceURL,
              "html",dateHeaders,
              item,
              hashSet,
              output,reporter);
        }
      }
    }
  }

  static String safeEmitRSSLinkTypeInLink(Text sourceURL,String sourceType,JsonObject dateHeaders,JsonElement linkProperty,HashSet<Long> hashSet,OutputCollector<TextBytes, TextBytes> output,Reporter reporter) throws IOException { 
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

        URLFPV2 fp = URLUtils.getURLFPV2FromURL(embeddedLinkObject.get("href").getAsString());
        
        if (fp == null) { 
          reporter.incrCounter(Counters.FP_NULL_IN_EMBEDDED_LINK, 1);
        }
        else {
          
          if (hashSet.contains(fp.getUrlHash())) { 
            reporter.incrCounter(Counters.SKIPPED_ALREADY_EMITTED_LINK, 1);
          }        
          else { 
            String md5Hash = MD5Hash.digest(embeddedLinkObject.toString()).toString();
  
            embeddedLinkObject.addProperty("source_url",sourceURL.toString());
            embeddedLinkObject.addProperty("source_type",sourceType);
            embeddedLinkObject.add("source_headers",dateHeaders);
  
            TextBytes key = LinkKey.generateLinkKey(fp,LinkKey.Type.KEY_TYPE_HTML_LINK,md5Hash);
            
            reporter.incrCounter(Counters.EMITTED_RSS_LINK, 1);
            output.collect(key,new TextBytes(embeddedLinkObject.toString()));
            hashSet.add(fp.getUrlHash());
            
            return embeddedLinkObject.get("href").getAsString();
          }
        }
      }
    }
    return null;
  }  
  void emitLinksFromRSSContent(Text sourceURL,String sourceType,JsonObject dateHeaders,JsonObject object,JsonObject contentObject,HashSet<Long> hashSet,OutputCollector<TextBytes, TextBytes> output, Reporter reporter) throws IOException {
    String topLevelLink = safeEmitRSSLinkTypeInLink(sourceURL,sourceType,dateHeaders,contentObject.get("link"),hashSet,output,reporter);
    if (topLevelLink != null) { 
      reporter.incrCounter(Counters.GOT_TOP_LEVEL_LINK_FROM_RSS_ITEM, 1);
    }
    JsonElement items = contentObject.get("items");
    if (items != null) { 
      JsonArray itemsArray = items.getAsJsonArray();
      for (int i=0;i<itemsArray.size();++i) { 
        JsonObject item = itemsArray.get(i).getAsJsonObject();
        String itemURL = safeEmitRSSLinkTypeInLink(sourceURL,sourceType,dateHeaders,item.get("link"),hashSet,output,reporter);
        if (itemURL != null) { 
          reporter.incrCounter(Counters.GOT_ITEM_LINK_FROM_RSS_ITEM, 1);
        }
        JsonObject itemContent = item.getAsJsonObject("content");
        if (itemContent != null) { 
          emitLinksFromHTMLContent(
              (itemURL != null)? new Text(itemURL) : sourceURL,
              "html",
              dateHeaders,
              item,
              hashSet,
              output,reporter);
        }
      }
    }
  }

  
  void emitLinksFromHTMLContent(Text sourceURL,String sourceType,JsonObject dateHeaders,JsonObject object,HashSet<Long> hashSet,OutputCollector<TextBytes, TextBytes> output, Reporter reporter) throws IOException { 
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
              URLFPV2 fp = URLUtils.getURLFPV2FromURL(href.getAsString());
              if (fp == null) { 
                reporter.incrCounter(Counters.FP_NULL_IN_EMBEDDED_LINK, 1);
              }
              else {
                if (!hashSet.contains(fp.getUrlHash())) { 
                  // calculate md5 hash of link ... 
                  String md5Hash = MD5Hash.digest(linkObject.toString()).toString();
                  // write src into json object 
                  linkObject.addProperty("source_url",sourceURL.toString());
                  linkObject.addProperty("source_type",sourceType);
                  linkObject.add("source_headers",dateHeaders);
                  
                  reporter.incrCounter(Counters.EMITTED_HTML_LINK, 1);
                  // generate new key ...
                  TextBytes key = LinkKey.generateLinkKey(fp,LinkKey.Type.KEY_TYPE_HTML_LINK,md5Hash);
                  output.collect(key,new TextBytes(linkObject.toString()));
                  
                  hashSet.add(fp.getUrlHash());
                }
                else {
                  reporter.incrCounter(Counters.SKIPPED_ALREADY_EMITTED_LINK, 1);
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

  @Override
  public void configure(JobConf job) {
    
  }

  @Override
  public void close() throws IOException {
    
  }

  @Override
  public void reduce(TextBytes key, Iterator<TextBytes> values,OutputCollector<TextBytes, TextBytes> output, Reporter reporter)throws IOException {
    
    long linkType = LinkKey.getLongComponentFromKey(key,LinkKey.ComponentId.TYPE_COMPONENT_ID);
    
    if (linkType != LinkKey.Type.KEY_TYPE_CRAWL_STATUS.ordinal()) {
      reporter.incrCounter(Counters.REDUCER_GOT_LINK, 1);
      output.collect(key, values.next());
      int redundantLinkCount = 0;
      while (values.hasNext()) {
        values.next();
        redundantLinkCount++;
      }
      if (redundantLinkCount == 1) { 
        reporter.incrCounter(Counters.ONE_REDUNDANT_LINK_IN_REDUCER, 1);
      }
      else if (redundantLinkCount == 2) { 
        reporter.incrCounter(Counters.TWO_REDUNDANT_LINKS_IN_REDUCER, 1);
      }
      else if (redundantLinkCount == 3) { 
        reporter.incrCounter(Counters.THREE_REDUNDANT_LINKS_IN_REDUCER, 1);
      }
      else if (redundantLinkCount > 3) { 
        reporter.incrCounter(Counters.GT_THREE_REDUNDANT_LINKS_IN_REDUCER, 1);
      }
      
    }
    else { 
      reporter.incrCounter(Counters.REDUCER_GOT_STATUS, 1);
      
      int redundantStatusCount = -1;
      
      while (values.hasNext()) {  
        output.collect(key, values.next());
        redundantStatusCount++;
      }
      
      if (redundantStatusCount == 1) { 
        reporter.incrCounter(Counters.ONE_REDUNDANT_STATUS_IN_REDUCER, 1);
      }
      else if (redundantStatusCount == 2) { 
        reporter.incrCounter(Counters.TWO_REDUNDANT_STATUS_IN_REDUCER, 1);
      }
      else if (redundantStatusCount == 3) { 
        reporter.incrCounter(Counters.THREE_REDUNDANT_STATUS_IN_REDUCER, 1);
      }
      else if (redundantStatusCount > 3) { 
        reporter.incrCounter(Counters.GT_THREE_REDUNDANT_STATUS_IN_REDUCER, 1);
      }
    }
  }

}
