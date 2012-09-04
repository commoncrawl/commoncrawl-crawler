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

package org.commoncrawl.mapred.ec2.parser;

import java.io.IOException;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.List;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Counters.Counter;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.crawl.common.shared.Constants;
import org.commoncrawl.service.parser.ParseResult;
import org.commoncrawl.io.NIOHttpHeaders;
import org.commoncrawl.protocol.CrawlURL;
import org.commoncrawl.protocol.CrawlURLMetadata;
import org.commoncrawl.protocol.ParseOutput;
import org.commoncrawl.protocol.shared.CrawlMetadata;
import org.commoncrawl.protocol.shared.FeedAuthor;
import org.commoncrawl.protocol.shared.FeedContent;
import org.commoncrawl.protocol.shared.FeedItem;
import org.commoncrawl.protocol.shared.FeedLink;
import org.commoncrawl.protocol.shared.HTMLContent;
import org.commoncrawl.protocol.shared.HTMLLink;
import org.commoncrawl.protocol.shared.HTMLMeta;
import org.commoncrawl.protocol.shared.HTMLMetaAttribute;
import org.commoncrawl.service.parser.server.ParseWorker;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.CharsetUtils;
import org.commoncrawl.util.FlexBuffer;
import org.commoncrawl.util.GZIPUtils;
import org.commoncrawl.util.GoogleURL;
import org.commoncrawl.util.HttpHeaderInfoExtractor;
import org.commoncrawl.util.IPAddressUtils;
import org.commoncrawl.util.JSONUtils;
import org.commoncrawl.util.MimeTypeFilter;
import org.commoncrawl.util.SimHash;
import org.commoncrawl.util.TaskDataUtils;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLUtils;
import org.commoncrawl.util.GZIPUtils.UnzipResult;
import org.commoncrawl.util.MimeTypeFilter.MimeTypeDisposition;
import org.commoncrawl.util.TaskDataUtils.TaskDataClient;
import org.commoncrawl.util.Tuples.Pair;
import org.xml.sax.InputSource;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.stream.JsonReader;
import com.sun.syndication.feed.WireFeed;
import com.sun.syndication.feed.atom.Content;
import com.sun.syndication.feed.atom.Entry;
import com.sun.syndication.feed.atom.Feed;
import com.sun.syndication.feed.rss.Category;
import com.sun.syndication.feed.rss.Channel;
import com.sun.syndication.feed.rss.Description;
import com.sun.syndication.feed.rss.Item;
import com.sun.syndication.io.WireFeedInput;

/**
 * Initial version of a Mapper that takes a URL and CrawlURL 
 * (data structure produced by crawlers) and emits metadata and raw content 
 * via a custom OutputFormat to S3.
 * 
 * This version only handles HTML,RSS,and ATOM content mainly because we were 
 * rushed for time to get this job running on EC2 and also because of the 
 * desire to have a very resilient, tightly controlled codebase to ensure smooth
 * and reliable EC2 performance. Needs to be refactored at some point. 
 * 
 * 
 * 
 * @author rana
 *
 */
public class ParserMapper implements Mapper<Text,CrawlURL,Text,ParseOutput> {

  public static final Log LOG = LogFactory.getLog(ParserMapper.class);
  
  public static final String JSON_DISPOSITION_PROPERTY = "disposition";
  public static final String ORIGINAL_RESPONSE_CODE_HTTP_HEADER = "response";

  enum Counters {
    BAD_REDIRECT_URL, FAILED_TO_PARSE_HTML, PARSED_HTML_DOC,
    FAILED_TO_PARSE_FEED_URL, PARSED_FEED_URL, GUNZIP_FAILED,
    GUNZIP_DATA_TRUNCATED, WROTE_METADATA_RECORD, WROTE_TEXT_CONTENT,
    WROTE_RAW_CONTENT, GOT_UNHANDLED_IO_EXCEPTION,
    GOT_UNHANDLED_RUNTIME_EXCEPTION, MALFORMED_FINAL_URL, GOT_RSS_FEED,
    GOT_ATOM_FEED, TRYING_RSS_FEED_PARSER, EXCEPTION_DURING_FEED_PARSE,
    FAILED_TO_ID_FEED, FAILED_TO_PARSE_XML_AS_FEED, EXCEPTION_PARSING_LINK_JSON, SKIPPING_ROBOTS_TXT, ERROR_CANONICALIZING_LINK_URL
  }
  
  public static final String MAX_MAPPER_RUNTIME_PROPERTY = "cc.max.mapper.runtime";
  // 50 minutes per mapper MAX
  public static final long   DEFAULT_MAX_MAPPER_RUNTIME = 50  * 60 * 1000; 
  
  
  public static final String BAD_TASK_TASKDATA_KEY = "bad";
  public static final String GOOD_TASK_TASKDATA_KEY = "good";
  
  private static ImmutableSet<String> dontKeepHeaders = ImmutableSet.of(
      "proxy-connection",
      "connection",
      "keep-alive",
      "transfer-encoding",
      "te",
      "trailer",
      "proxy-authorization",
      "proxy-authenticate",
      "upgrade",
      "set-cookie",
      "content-encoding"
      );

  
  public static JsonObject httpHeadersToJsonObject(NIOHttpHeaders headers)throws IOException { 
    JsonObject httpHeaderObject = new JsonObject();
    
    // iterate entires in header object
    for (int i=0;i<headers.getKeyCount();++i) {
      String key = headers.getKey(i);
      String value = headers.getValue(i);
      
      if (key == null && i==0) { 
        httpHeaderObject.addProperty(ORIGINAL_RESPONSE_CODE_HTTP_HEADER, value);
      }
      else if (key != null && value != null) { 
        if (!dontKeepHeaders.contains(key.toLowerCase())) {
          // and send other ones through 
          httpHeaderObject.addProperty(key.toLowerCase(),value);
        }
      }
    }
    return httpHeaderObject;
  }  
  
  private Pair<URL,JsonObject> buildRedirectObject(URL originalURL,CrawlURL value,CrawlMetadata metadata,Reporter reporter)throws IOException { 
    
    JsonObject redirectObject = new JsonObject();
    
    redirectObject.addProperty("source_url",originalURL.toString());
    metadata.getRedirectData().setSourceURL(originalURL.toString());
    
    String canonicalRedirectURL = canonicalizeURL(value.getRedirectURL());
    if (canonicalRedirectURL == null) { 
      reporter.incrCounter(Counters.BAD_REDIRECT_URL, 1);
      return null;
    }

    URL finalURLObj = null;

    try { 
      finalURLObj = new URL(canonicalRedirectURL);
    }
    catch (MalformedURLException e) { 
      LOG.error("Malformed URL:" + CCStringUtils.stringifyException(e));
      reporter.incrCounter(Counters.BAD_REDIRECT_URL, 1);
      return null;
    }        
    
    redirectObject.addProperty("http_result",(int)value.getOriginalResultCode());
    metadata.getRedirectData().setHttpResult(value.getOriginalResultCode());
    redirectObject.addProperty("server_ip",IPAddressUtils.IntegerToIPAddressString(value.getOriginalServerIP()));
    metadata.getRedirectData().setServerIP(value.getOriginalServerIP());
    redirectObject.add("http_headers",httpHeadersToJsonObject(NIOHttpHeaders.parseHttpHeaders(value.getOriginalHeaders())));
    metadata.getRedirectData().setHttpHeaders(value.getOriginalHeaders());
    
    return new Pair<URL,JsonObject>(finalURLObj,redirectObject);
  }

  private JsonObject parseResultToJsonObject(URL baseURL,ParseResult result,HTMLContent htmlMeta,Reporter reporter)throws IOException {
    
    JsonParser parser = new JsonParser();
    
    JsonObject objectOut = new JsonObject();
    
    objectOut.addProperty("type","html-doc");
    
    safeSetString(objectOut,"title",result.getTitle());
    if (result.isFieldDirty(ParseResult.Field_TITLE))
      htmlMeta.setTitle(result.getTitle());
    
    if (result.getMetaTags().size() != 0) { 
      JsonArray metaArray = new JsonArray();
      for (HTMLMeta htmlMetaObject : result.getMetaTags()) { 
        JsonObject jsonMetaObject = new JsonObject();
        // populate meta tag based on attributes 
        for (HTMLMetaAttribute attribute : htmlMetaObject.getAttributes()) { 
          jsonMetaObject.addProperty(attribute.getName(),attribute.getValue());
        }
        metaArray.add(jsonMetaObject);
        htmlMeta.getMetaTags().add(htmlMetaObject);
      }
      objectOut.add("meta_tags", metaArray);
    }
    if (result.getExtractedLinks().size() != 0) { 
      JsonArray linkArray = new JsonArray();
      for (org.commoncrawl.service.parser.Link link : result.getExtractedLinks()) {
        try {
          String canonicalLinkURL = canonicalizeURL(link.getUrl());
          if (canonicalLinkURL == null) { 
            reporter.incrCounter(Counters.ERROR_CANONICALIZING_LINK_URL, 1);
          }
          else { 
            JsonObject linkObj = parser.parse(new JsonReader(new StringReader(link.getAttributes()))).getAsJsonObject();
            linkObj.addProperty("href", canonicalLinkURL);
            linkArray.add(linkObj);
  
            HTMLLink linkMeta = new HTMLLink();
            linkMeta.setAttributes(link.getAttributes());
            linkMeta.setHref(canonicalLinkURL);
            
            htmlMeta.getLinks().add(linkMeta);
          }
        }
        catch (Exception e) { 
          LOG.error("Error Parsing JSON Link Attributes for Link: " + link.getUrl() + " in Doc:" + baseURL + " Exception:\n" + CCStringUtils.stringifyException(e));
          reporter.incrCounter(Counters.EXCEPTION_PARSING_LINK_JSON, 1);
        }
      }
      objectOut.add("links", linkArray);
    }
    return objectOut; 
  }
  
  private static String cleanupDescription(Object d) {
    
    String value = null;
    
    if (d instanceof Description)
      value = (d != null) ? ((Description)d).getValue() : null;
    else if (d instanceof String)
      value = (String)d;
    else if (d instanceof Content) 
      value = (d != null) ? ((Content)d).getValue() : null;

    if (value == null) 
      return "";
    String[] parts = value.split("<[^>]*>");
    StringBuffer buf = new StringBuffer();

    for (String part : parts)
      buf.append(part);

    return buf.toString().trim();
  }
  
  private static void safeSetDate(JsonObject jsonObj,String propertyName,Date date) { 
    if (date != null) { 
      jsonObj.addProperty(propertyName,date.getTime());
    }
  }
  
  private static void setRSSCategories(JsonObject jsonObj,List<TextBytes> metaCategories,StringBuffer contentOut,List categories) { 
    if (categories.size() != 0) {
      JsonArray jsonArray = new JsonArray();
      for (Object category : categories) {
        if (((Category)category).getValue() != null && ((Category)category).getValue().length() != 0) { 
          safeAppendContentFromString(contentOut,((Category)category).getValue());
          jsonArray.add(new JsonPrimitive(((Category)category).getValue()));
          if (((Category)category).getValue() != null) { 
            metaCategories.add(new TextBytes(((Category)category).getValue()));
          }
        }
      }
      jsonObj.add("categories", jsonArray);
    }
  }
  
  private static void setAtomCategories(JsonObject jsonObj,List<TextBytes> metaCategoryList,StringBuffer contentOut,List categories) { 
    if (categories.size() != 0) {
      JsonArray jsonArray = new JsonArray();
      for (Object category : categories) {
        com.sun.syndication.feed.atom.Category categoryObj = (com.sun.syndication.feed.atom.Category) category;
        
        if (categoryObj.getLabel() != null && categoryObj.getLabel().length() != 0) { 
          safeAppendContentFromString(contentOut,categoryObj.getLabel());
          jsonArray.add(new JsonPrimitive(categoryObj.getLabel()));
          if (categoryObj.getLabel() != null) 
            metaCategoryList.add(new TextBytes(categoryObj.getLabel()));
        }
      }
      jsonObj.add("categories", jsonArray);
    }
  }
  
  private static void safeSetString(JsonObject jsonObj,String propertyName,String propertyValue) { 
    if (propertyValue != null && propertyValue.length() != 0) { 
      jsonObj.addProperty(propertyName,propertyValue);
    }
  }
  
  private static void safeSetInteger(JsonObject jsonObj,String propertyName,int propertyValue) { 
    if (propertyValue != -1) { 
      jsonObj.addProperty(propertyName,propertyValue);
    }
  }
  
  private Pair<JsonObject,String> parseHTMLDocument(URL baseURL,String rawHeaders, FlexBuffer data,HTMLContent contentMetaOut,Reporter reporter) throws IOException { 
    ParseResult resultOut = new ParseResult();
    ParseWorker parseWorker = new ParseWorker();
    parseWorker.parseDocument(resultOut, 0, 0, baseURL, rawHeaders, data);
    if (resultOut.getParseSuccessful()) { 
      return new Pair<JsonObject,String>(parseResultToJsonObject(baseURL, resultOut,contentMetaOut,reporter),resultOut.getText());
    }
    return null;
  }
  
  private Pair<JsonObject,String> parseHTMLSnippet(URL baseURL,String htmlSnippet,HTMLContent contentMetaOut, Reporter reporter) throws IOException { 
    ParseResult resultOut = new ParseResult();
    ParseWorker parseWorker = new ParseWorker();
    parseWorker.parsePartialHTMLDocument(resultOut, baseURL, htmlSnippet);
    if (resultOut.getParseSuccessful()) { 
      return new Pair<JsonObject,String>(parseResultToJsonObject(baseURL, resultOut,contentMetaOut,reporter),resultOut.getText());
    }
    return null;
  }

  private static String safeAppendContentFromString(StringBuffer buffer,String content) {
    if (content != null) { 
      String contentTrimmed = content.trim();
    
      if (contentTrimmed.length() != 0) { 
        if (buffer.length() != 0) 
          buffer.append(" ");
        buffer.append(contentTrimmed);
      }
      return contentTrimmed;
    }
    return null;
  }

  private static String safeAppendContentFromContentObj(StringBuffer buffer,Content content) {
    if (content != null && content.getValue() != null) { 
      String contentTrimmed = content.getValue().trim();
    
      if (contentTrimmed.length() != 0) { 
        if (buffer.length() != 0) 
          buffer.append(" ");
        buffer.append(contentTrimmed);
      }
      return contentTrimmed;
    }
    return null;
  }

  
  private static void safeAppendLinksFromFeed(JsonObject feedOrItemObj,ImmutableMap<String,String> validLinkTypes,List<FeedLink> feedMetaLinks,List links)throws IOException { 
    for (Object link : links) { 
      com.sun.syndication.feed.atom.Link linkObj = (com.sun.syndication.feed.atom.Link) link;
      if (linkObj.getHref() != null && linkObj.getRel() != null) { 
        
        String canonicalHref = canonicalizeURL(linkObj.getHref());
        
        if (canonicalHref == null) { 
          LOG.error("Failed to Canoniclize Link URL:" + linkObj.getHref());
        }
        else { 
          if (validLinkTypes.keySet().contains(linkObj.getRel())) {
            JsonObject jsonLink = new JsonObject();
            FeedLink metaLink = new FeedLink();
            
            safeSetString(jsonLink, "type", linkObj.getType());
            if (linkObj.getType() != null) metaLink.setType(linkObj.getType());
            safeSetString(jsonLink, "href",canonicalHref);
            if (linkObj.getHref() != null) metaLink.setHref(canonicalHref);
            safeSetString(jsonLink, "rel",linkObj.getRel());
            if (linkObj.getRel() != null) metaLink.setRel(linkObj.getRel());
            
            safeSetString(jsonLink, "title", linkObj.getTitle());
            if (linkObj.getTitle() != null) metaLink.setTitle(linkObj.getTitle());
            
            feedMetaLinks.add(metaLink);
            
            String linkName = validLinkTypes.get(linkObj.getRel());
                      
            JsonElement existing = feedOrItemObj.get(linkName);
            if (existing != null) { 
              JsonArray array = null;
              if (!existing.isJsonArray()) { 
                array = new JsonArray();
                array.add(existing);
                feedOrItemObj.remove(linkName);
                feedOrItemObj.add(linkName, array);
              }
              else { 
                array = existing.getAsJsonArray();
              }
              array.add(jsonLink);
            }
            else { 
              feedOrItemObj.add(linkName,jsonLink);
            }
          }
        }
      }
    }
  }

  private static void safeAppendAuthorsFromFeed(JsonObject feedOrItemObj,List<FeedAuthor> metaAuthorList,List authors)throws IOException { 
    if (authors.size() != 0) {
      JsonArray authorArray = new JsonArray();
      for (Object author : authors) { 
        com.sun.syndication.feed.atom.Person authorObj = (com.sun.syndication.feed.atom.Person) author;
        if (authorObj.getName() != null) { 
            
          JsonObject jsonAuthor = new JsonObject();
          FeedAuthor metaAuthor = new FeedAuthor();

          String canonicalURL = canonicalizeURL(authorObj.getUrl());
          safeSetString(jsonAuthor, "name", authorObj.getName());
          if (canonicalURL != null) {
            safeSetString(jsonAuthor, "url", canonicalURL);
          }
          
          if (authorObj.getName() != null) metaAuthor.setName(authorObj.getName());
          if (canonicalURL != null) metaAuthor.setUrl(canonicalURL);
          
          authorArray.add(jsonAuthor);
          metaAuthorList.add(metaAuthor);
        }
      }
      feedOrItemObj.add("authors", authorArray);
    }
  }
  
  private static void safeAppendLinkFromString(JsonObject jsonObj,List<FeedLink> metaLinks,String propertyName,String linkValue)throws IOException { 
    if (linkValue != null && linkValue.length() != 0) { 
      
      String canonicalURL = canonicalizeURL(linkValue);
      
      if (canonicalURL != null) { 
      
        JsonObject jsonLink = new JsonObject();
        FeedLink metaLink = new FeedLink();
        
        jsonLink.addProperty("href",canonicalURL);
        metaLink.setHref(canonicalURL);
        
  
        jsonObj.add(propertyName, jsonLink);
        metaLinks.add(metaLink);
      }
      //TODO REPORT FAILURE
    }
  }
  
  private Pair<JsonObject,String> rssFeedToJson(URL url,Channel channelObject,FeedContent feedMeta, Reporter reporter )throws IOException { 
    
    JsonObject rssObject = new JsonObject();
    
    StringBuffer contentOut = new StringBuffer();
    
    rssObject.addProperty("type","rss-feed");
    feedMeta.setType(FeedContent.Type.RSS);
    
    String feedTitle = cleanupDescription(channelObject.getTitle());
    rssObject.addProperty("title", safeAppendContentFromString(contentOut,feedTitle));
    if (feedTitle != null) feedMeta.setTitle(feedTitle);
    
    safeAppendLinkFromString(rssObject,feedMeta.getLinks(),"link", channelObject.getLink());
    
    String feedDesc = cleanupDescription(channelObject.getDescription());
    rssObject.addProperty("description", safeAppendContentFromString(contentOut,feedDesc));
    if (feedDesc != null) feedMeta.setDescription(feedDesc);
    
    if (channelObject.getLastBuildDate() != null)  {
      safeSetDate(rssObject,"updated",channelObject.getLastBuildDate());
      feedMeta.setUpdated(channelObject.getLastBuildDate().getTime());
    }
    else if (channelObject.getPubDate() != null ) {  
      safeSetDate(rssObject,"updated",channelObject.getPubDate());
      feedMeta.setUpdated(channelObject.getPubDate().getTime());
    }
    
    setRSSCategories(rssObject,feedMeta.getCategories(),contentOut, channelObject.getCategories());
    
    safeSetString(rssObject, "generator", channelObject.getGenerator());
    if (channelObject.getGenerator() != null) feedMeta.setGenerator(channelObject.getGenerator());
    
    safeSetInteger(rssObject, "ttl", channelObject.getTtl());
    if (channelObject.getTtl() != -1) feedMeta.setTtl(channelObject.getTtl());

    JsonArray itemArray = new JsonArray();
    for (Object itemObj : channelObject.getItems()) { 
      Item item = (Item)itemObj;
      JsonObject itemObject = new JsonObject();
      FeedItem metaItem = new FeedItem();
      
      String itemTitle =cleanupDescription(item.getTitle());
      itemObject.addProperty("title", safeAppendContentFromString(contentOut,itemTitle));
      if (itemTitle != null) metaItem.setTitle(itemTitle);
      
      String itemDesc = cleanupDescription(item.getDescription());
      itemObject.addProperty("description",safeAppendContentFromString(contentOut,itemDesc));
      if (itemDesc != null) metaItem.setDescription(itemDesc);
      
      safeAppendLinkFromString(itemObject,metaItem.getLinks(), "link", item.getLink()); 
      
      safeSetString(itemObject, "author", item.getAuthor());
      if (item.getAuthor() != null) { 
        FeedAuthor metaAuthor = new FeedAuthor();
        metaAuthor.setName(item.getAuthor());
        metaItem.getAuthors().add(metaAuthor);
      }
      
      setRSSCategories(itemObject,metaItem.getCategories(),contentOut,item.getCategories());
      
      safeSetString(itemObject, "comments", item.getComments());
      
      safeSetDate(itemObject,"published",item.getPubDate());
      if (item.getPubDate() != null) metaItem.setPublished(item.getPubDate().getTime());
      
      if (item.getGuid() != null) { 
        safeSetString(itemObject,"guid",item.getGuid().getValue());
        if (item.getGuid().getValue() != null) metaItem.setGuid(item.getGuid().getValue()); 
      }
      if (item.getContent() != null && item.getContent().getValue() != null) {
        if (item.getContent().getType() == null || item.getContent().getType().contains("html")) {
          HTMLContent metaContent = new HTMLContent();
          Pair<JsonObject,String> contentTuple = parseHTMLSnippet(url, item.getContent().getValue(),metaContent,reporter);
          metaItem.getEmbeddedLinks().addAll(metaContent.getLinks());
          if (contentTuple.e0 != null) { 
            itemObject.add("content", contentTuple.e0);
          }
          if (contentTuple.e1 != null && contentTuple.e1.length() != 0) { 
            safeAppendContentFromString(contentOut, contentTuple.e1);
          }
        }
      }
      itemArray.add(itemObject);
    }
    rssObject.add("items",itemArray);
    
    return new Pair<JsonObject,String> (rssObject,contentOut.toString());
  }
  
  
  
  static ImmutableMap<String, String> validFeedLinks
    = new ImmutableMap.Builder<String,String>()
      .put("alternate", "link")
      .build();
  
  static ImmutableMap<String, String> feedEntryLinks
  = new ImmutableMap.Builder<String,String>()
    .put("alternate", "link")
    .put("self", "self")
    .put("replies", "replies")
    .build();
  
  private Pair<JsonObject,String> atomFeedToJson(URL url,Feed feedObject,FeedContent feedMeta,Reporter reporter)throws IOException { 
    JsonObject jsonFeed= new JsonObject();
    StringBuffer contentOut = new StringBuffer();
    
    jsonFeed.addProperty("type","atom-feed");
    feedMeta.setType(FeedContent.Type.ATOM);
    String title = cleanupDescription(feedObject.getTitle());
    jsonFeed.addProperty("title", safeAppendContentFromString(contentOut,title));
    if (title != null) feedMeta.setTitle(title);
    
    safeAppendLinksFromFeed(jsonFeed, validFeedLinks, feedMeta.getLinks(),feedObject.getAlternateLinks());
    safeAppendAuthorsFromFeed(jsonFeed,feedMeta.getAuthors(),feedObject.getAuthors());
    if (feedObject.getGenerator() != null) { 
      safeSetString(jsonFeed, "generator", feedObject.getGenerator().getValue());
      if (feedObject.getGenerator().getValue() != null) { 
        feedMeta.setGenerator(feedObject.getGenerator().getValue());
      }
    }
    
    safeSetDate(jsonFeed, "updated", feedObject.getUpdated());
    if (feedObject.getUpdated() != null) { 
      feedMeta.setUpdated(feedObject.getUpdated().getTime());
    }
    
    setAtomCategories(jsonFeed,feedMeta.getCategories(),contentOut, feedObject.getCategories());
    JsonArray itemArray = new JsonArray();
    for (Object entry : feedObject.getEntries()) {
      Entry entryObj = (Entry)entry;
      JsonObject jsonEntry = new JsonObject();
      FeedItem metaItem = new FeedItem();

      String itemTitle = cleanupDescription(entryObj.getTitle());
      jsonEntry.addProperty("title", safeAppendContentFromString(contentOut,itemTitle));
      if (itemTitle != null) metaItem.setTitle(itemTitle);
      
      String itemDesc = cleanupDescription(entryObj.getSummary());
      jsonEntry.addProperty("description",safeAppendContentFromString(contentOut,itemDesc));
      if (itemDesc != null) metaItem.setDescription(itemDesc);
      
      safeSetDate(jsonFeed, "published", entryObj.getPublished());
      if (entryObj.getPublished() != null) metaItem.setPublished(entryObj.getPublished().getTime());
      
      safeSetDate(jsonFeed, "updated", entryObj.getUpdated());
      if (entryObj.getUpdated() != null) metaItem.setUpdated(entryObj.getUpdated().getTime());
      
      safeAppendLinksFromFeed(jsonEntry, feedEntryLinks,metaItem.getLinks(), entryObj.getAlternateLinks());
      safeAppendLinksFromFeed(jsonEntry, feedEntryLinks,metaItem.getLinks(), entryObj.getOtherLinks());
      safeAppendAuthorsFromFeed(jsonEntry,metaItem.getAuthors(),entryObj.getAuthors());
      setAtomCategories(jsonEntry,metaItem.getCategories(),contentOut,entryObj.getCategories());
      
      for (Object content : entryObj.getContents()) { 
        com.sun.syndication.feed.atom.Content contentObj = (com.sun.syndication.feed.atom.Content) content;
        if (contentObj.getValue() != null && contentObj.getValue().length() != 0) { 
          if (contentObj.getType() == null || contentObj.getType().contains("html")) {
            HTMLContent metaContent = new HTMLContent();
            Pair<JsonObject,String> contentTuple = parseHTMLSnippet(url, contentObj.getValue(),metaContent,reporter);
            metaItem.getEmbeddedLinks().addAll(metaContent.getLinks());
            if (contentTuple.e0 != null) {
              if (jsonEntry.has("content")) { 
                JsonArray array = null;
                JsonElement existing = jsonEntry.get("content");
                if (!existing.isJsonArray()) { 
                  array = new JsonArray();
                  array.add(existing);
                  jsonEntry.remove("content");
                  jsonEntry.add("content", array);
                }
                else { 
                  array = existing.getAsJsonArray();
                }
                array.add(contentTuple.e0);
              }
              else { 
                jsonEntry.add("content", contentTuple.e0);
              }
              if (contentTuple.e1 != null && contentTuple.e1.length() != 0) { 
                safeAppendContentFromString(contentOut, contentTuple.e1);
              }
            }
          }
        }
      }
      itemArray.add(jsonEntry);
    }
    
    jsonFeed.add("items",itemArray);
    
    return new Pair<JsonObject,String>(jsonFeed,contentOut.toString()); 
  }
  
  private static final String feedEntryEnd = "</entry>";
  private static final String feedItemEnd = "</item>";
  private Pair<JsonObject,String> parseFeedDocument(URL baseURL,String rawHeaders, String feedContent,FeedContent feedMeta,boolean truncatedDocument,Reporter reporter) throws IOException {
    
    if (truncatedDocument) { 
      LOG.warn("Fixing Up Trancated Doc:" + baseURL);
      int indexOfEntryEnd = feedContent.lastIndexOf(feedEntryEnd);
      if (indexOfEntryEnd != -1) { 
        feedContent = feedContent.substring(0,indexOfEntryEnd + feedEntryEnd.length());
        feedContent += "</feed>";
      }
      else { 
        int indexOfItemEnd = feedContent.lastIndexOf(feedItemEnd);
        if (indexOfItemEnd != -1) { 
          feedContent = feedContent.substring(0,indexOfEntryEnd + feedItemEnd.length());
          feedContent += "</channel></rss>";
          
        }
      }
    }
    
    InputSource source = new InputSource(new StringReader(feedContent));
    WireFeedInput input = new WireFeedInput();
    
    Pair<JsonObject,String> resultTuple = null;
    
    try { 
      WireFeed feed = input.build(source);
      if (feed != null) {
        
        if (feed instanceof Channel) {
          reporter.incrCounter(Counters.TRYING_RSS_FEED_PARSER, 1);
          resultTuple = rssFeedToJson(baseURL,(Channel)feed,feedMeta,reporter);
          reporter.incrCounter(Counters.GOT_RSS_FEED, 1);
        }
        else if (feed instanceof Feed) { 
          resultTuple = atomFeedToJson(baseURL,(Feed)feed,feedMeta,reporter);
          reporter.incrCounter(Counters.GOT_ATOM_FEED, 1);
        }
        else { 
          reporter.incrCounter(Counters.FAILED_TO_ID_FEED, 1);
          LOG.error("Failed to ID Feed:" + baseURL);
        }
        
      }
    }
    catch (Exception e) {
      reporter.incrCounter(Counters.EXCEPTION_DURING_FEED_PARSE, 1);
      LOG.error("Failed to parse Feed:" + baseURL + "\n ContentLen:" + feedContent.length() + "\n with Exception:" + CCStringUtils.stringifyException(e));
    }
    return resultTuple;
  }
  
  
  private Pair<String,Pair<TextBytes,FlexBuffer>> populateContentMetadata(URL finalURL,CrawlURL value,Reporter reporter,JsonObject metadata,CrawlMetadata crawlMeta)throws IOException { 
    
    FlexBuffer contentOut = null;
    String textOut = null;
    
    NIOHttpHeaders finalHeaders = NIOHttpHeaders
      .parseHttpHeaders(value.getHeaders());
    
    CrawlURLMetadata urlMetadata = new CrawlURLMetadata();
    
    // extract information from http headers ... 
    HttpHeaderInfoExtractor.parseHeaders(finalHeaders, urlMetadata);    
    // get the mime type ... 
    String normalizedMimeType = urlMetadata.isFieldDirty(CrawlURLMetadata.Field_CONTENTTYPE) ? urlMetadata.getContentType() : "text/html";
    
    metadata.addProperty("mime_type", normalizedMimeType);
    crawlMeta.setMimeType(normalizedMimeType);
    
    // get download size ... 
    int downloadSize = value.getContentRaw().getCount();
    
    // set original content len ... 
    metadata.addProperty("download_size", downloadSize);
    crawlMeta.setDownloadSize(downloadSize);
    
    // set truncation flag 
    if ((value.getFlags() & CrawlURL.Flags.TruncatedDuringDownload) != 0) { 
      metadata.addProperty("download_truncated", true);
      crawlMeta.setFlags(crawlMeta.getFlags() | CrawlMetadata.Flags.Download_Truncated);
    }
    
    if (downloadSize > 0) { 
      // get content type, charset and encoding 
      String encoding    = finalHeaders.findValue("Content-Encoding");
      boolean isGZIP     = false;
      if (encoding != null && encoding.equalsIgnoreCase("gzip")) {
        isGZIP = true;
      }

      byte[] contentBytes = value.getContentRaw().getReadOnlyBytes();
      int    contentLen   = value.getContentRaw().getCount();

      // assume we are going to output original data ... 
      contentOut = new FlexBuffer(contentBytes,0,contentLen);
      
      if (isGZIP) {
        metadata.addProperty("content_is_gzip", isGZIP);
        crawlMeta.setFlags(crawlMeta.getFlags() | CrawlMetadata.Flags.ContentWas_GZIP);
      
        UnzipResult unzipResult = null;
        try { 
          // LOG.info("BEFORE GUNZIP");
          unzipResult = GZIPUtils.unzipBestEffort(contentBytes,0,contentLen,CrawlEnvironment.GUNZIP_SIZE_LIMIT);
        }
        catch (Exception e) { 
          LOG.error(CCStringUtils.stringifyException(e));
        }
        
        if (unzipResult != null && unzipResult.data != null) { 
          
          if (unzipResult.wasTruncated) {
            LOG.warn("Truncated Document During GZIP:" + finalURL);
            reporter.incrCounter(Counters.GUNZIP_DATA_TRUNCATED, 1);
          }
          
          contentBytes = unzipResult.data.get();
          contentLen   = unzipResult.data.getCount();
          
          metadata.addProperty("gunzip_content_len",unzipResult.data.getCount());
          crawlMeta.setGunzipSize(unzipResult.data.getCount());
          
          // update content out ... 
          contentOut = new FlexBuffer(contentBytes,0,contentLen);
        }
        else {
          
          metadata.addProperty("gunzip_failed",true);
          crawlMeta.setFlags(crawlMeta.getFlags() | CrawlMetadata.Flags.GUNZIP_Failed);
          
          reporter.incrCounter(Counters.GUNZIP_FAILED, 1);
          
          contentBytes = null;
          contentLen = 0;
          
          contentOut = null;
        }
          // LOG.info("AFTER GUNZIP");
      }
      
      if (contentBytes != null) {
        
        // ok compute an md5 hash 
        MD5Hash md5Hash = MD5Hash.digest(contentBytes,0,contentLen);
        
        metadata.addProperty("md5", md5Hash.toString());
        crawlMeta.setMd5(new FlexBuffer(md5Hash.getDigest(),0,md5Hash.getDigest().length));
        // get normalized mime type 
        if (MimeTypeFilter.isTextType(normalizedMimeType)) { 
          // ok time to decode the data into ucs2 ... 
          Pair<Pair<Integer,Charset>,String> decodeResult=  CharsetUtils.bestEffortDecodeBytes(value.getHeaders(), contentBytes, 0, contentLen);
          // ok write out decode metadata 
          metadata.addProperty("charset_detected", decodeResult.e0.e1.toString());
          crawlMeta.setCharsetDetected(decodeResult.e0.e1.toString());
          metadata.addProperty("charset_detector", decodeResult.e0.e0);
          crawlMeta.setCharsetDetector(decodeResult.e0.e0);
          // add appropriate http header (for detected charset)
          finalHeaders.add(Constants.ARCFileHeader_DetectedCharset, decodeResult.e0.e1.toString());
          
          // get the content 
          String textContent = decodeResult.e1;
          // compute simhash 
          long simhash = SimHash.computeOptimizedSimHashForString(textContent);
          metadata.addProperty("text_simhash", simhash);
          crawlMeta.setTextSimHash(simhash);
          
          // figure out simplified mime type ... 
          MimeTypeDisposition mimeTypeDisposition = MimeTypeFilter.checkMimeTypeDisposition(normalizedMimeType);
          
          boolean parseComplete = false;
          
          Pair<JsonObject,String> tupleOut = null;
          
          // write it out 
          if (mimeTypeDisposition == MimeTypeDisposition.ACCEPT_HTML) {
            //LOG.info("Parsing:" + finalURL.toString() + " Headers:" + value.getHeaders() + " ContentLen:" + contentLen);
             // ok parse as html 
            tupleOut = parseHTMLDocument(finalURL,value.getHeaders(),new FlexBuffer(contentBytes,0,contentLen),crawlMeta.getHtmlContent(),reporter);
             
             if (tupleOut == null) {
               reporter.incrCounter(Counters.FAILED_TO_PARSE_HTML, 1);
               LOG.error("Unable to Parse as HTML:" + finalURL.toString());
               mimeTypeDisposition = MimeTypeDisposition.ACCEPT_TEXT;
             }
             else { 
               reporter.incrCounter(Counters.PARSED_HTML_DOC, 1);
               metadata.addProperty("parsed_as", "html");
               crawlMeta.setParsedAs(CrawlMetadata.ParsedAs.HTML);
               parseComplete = true;
             }
          }
          
          if (!parseComplete && (mimeTypeDisposition == MimeTypeDisposition.ACCEPT_FEED || mimeTypeDisposition == MimeTypeDisposition.ACCEPT_XML)) {
            
            // ok try parse this document as a feed ...
            tupleOut = parseFeedDocument(finalURL, value.getHeaders(), textContent,crawlMeta.getFeedContent(),((value.getFlags() & CrawlURL.Flags.TruncatedDuringDownload) != 0),reporter);
            
            if (tupleOut == null) {
              if (mimeTypeDisposition == MimeTypeDisposition.ACCEPT_FEED) { 
                reporter.incrCounter(Counters.FAILED_TO_PARSE_FEED_URL, 1);
                //TODO:HACK 
                //LOG.info("Failed to Parse:" + finalURL + " RawContentLen:" + value.getContentRaw().getCount() + " ContentLen:" + contentLen + " Metadata:" + metadata.toString());
                mimeTypeDisposition = MimeTypeDisposition.ACCEPT_TEXT;
              }
            }
            else { 
              reporter.incrCounter(Counters.PARSED_FEED_URL, 1);
              metadata.addProperty("parsed_as", "feed");
              crawlMeta.setParsedAs(CrawlMetadata.ParsedAs.HTML);
              parseComplete = true;
            }
          }
          
          if (!parseComplete && mimeTypeDisposition == MimeTypeDisposition.ACCEPT_XML) { 
            reporter.incrCounter(Counters.FAILED_TO_PARSE_XML_AS_FEED, 1); 
            mimeTypeDisposition = MimeTypeDisposition.ACCEPT_TEXT;
          }
          if (!parseComplete && mimeTypeDisposition == MimeTypeDisposition.ACCEPT_TEXT) {
            // LOG.info("Identified URL" + finalURL + " w/ mimetype:" + normalizedMimeType + " as text");
            // TODO: FIX THIS BUT PUNT FOR NOW :-(
            //tupleOut = new Pair<JsonObject,String>(null,textContent);
          }
          
          if (tupleOut != null) {
            if (tupleOut.e0 != null) { 
              metadata.add("content", tupleOut.e0);
            }
            textOut = tupleOut.e1;
          }
        }
      }
    }
    return new Pair<String,Pair<TextBytes,FlexBuffer>>(textOut,new Pair<TextBytes,FlexBuffer>(new TextBytes(finalHeaders.toString()),contentOut));
  }
  
  static void safeSetJsonPropertyFromJsonProperty(JsonObject destinationObj,String destinationProperty,JsonElement sourceObj,String sourceProperty)throws IOException {
    if (sourceObj != null && sourceObj.isJsonObject()) { 
      JsonElement sourceElement = sourceObj.getAsJsonObject().get(sourceProperty);
      if (sourceElement != null) { 
        destinationObj.add(destinationProperty, sourceElement);
      }
    }
  }
  
  private static String canonicalizeURL(String sourceURL) throws IOException {
    if (sourceURL != null) { 
      GoogleURL urlObject = new GoogleURL(sourceURL);
      return URLUtils.canonicalizeURL(urlObject, false);
    }
    return null;
  }
  
  int mapCalls = 0;
  
  @Override
  public void map(Text sourceURL, CrawlURL value, OutputCollector<Text, ParseOutput> output,Reporter reporter) throws IOException {
    
    if (System.currentTimeMillis() > _killTime) {
      
      if (_runtimeAlreadyExtended || _lastProgressValue < .70) { 
        LOG.error("Expended Max Allowed Time for Mapper! Progress was at:" + _lastProgressValue);
        // send message to the task data master ... 
        _taskDataClient.updateTaskData(BAD_TASK_TASKDATA_KEY, _splitInfo);
        // and bail from the task ... 
        throw new IOException("Max Map Task Runtime Exceeded! Progress was at:" + _lastProgressValue);
      }
      else { 
        // extend runtime ... 
        LOG.info("Extending runtime by 1/2 of original max time ... ");
        _killTime = System.currentTimeMillis() + (_maxRunTime / 2);
        _runtimeAlreadyExtended = true;
      }
    }
    else { 
      // every 10 map calls ... check with tdc to see if we should fast fail this mapper ... 
      if (++mapCalls % 10 == 0) { 
        String badTaskDataValue = _taskDataClient.queryTaskData(BAD_TASK_TASKDATA_KEY);
        if (badTaskDataValue != null && badTaskDataValue.length() != 0) { 
          throw new IOException("Fast Failing Blacklisted (by TDC) Mapper");
        }
      }
    }
    
    if (sourceURL.getLength() == 0) { 
      LOG.error("Hit NULL URL. Original URL:" + value.getRedirectURL());
      return;
    }
        
    try { 
      // allocate parse output 
      ParseOutput parseOutput = new ParseOutput();
      // initialize segment id in output upfront ... 
      parseOutput.setDestSegmentId(_segmentId);
      // json object out ... 
      JsonObject jsonObj = new JsonObject();
      // and create a crawl metadata 
      CrawlMetadata metadata = parseOutput.getCrawlMetadata();
      
      // and content (if available) ... 
      Pair<String,Pair<TextBytes,FlexBuffer>> contentOut = null;

      // canonicalize the url (minimally) 
      String canonicalURL = canonicalizeURL(sourceURL.toString());
      
      // if canonicalization failed... bail early
      if (canonicalURL == null) { 
        reporter.incrCounter(Counters.MALFORMED_FINAL_URL, 1);
        return;
      }
      
      URL originalURL = null;
      
      try  { 
        originalURL = new URL(canonicalURL);
      }
      catch (MalformedURLException e) { 
        LOG.error("Malformed URL:" + CCStringUtils.stringifyException(e));
        reporter.incrCounter(Counters.MALFORMED_FINAL_URL, 1);
        return;
      }
      
      if (originalURL.getPath().endsWith("/robots.txt")) { 
        reporter.incrCounter(Counters.SKIPPING_ROBOTS_TXT, 1);
        return;
      }
      
      URL finalURL = originalURL;
      
      jsonObj.addProperty("attempt_time",value.getLastAttemptTime());
      metadata.setAttemptTime(value.getLastAttemptTime());
  
      // first step write status 
      jsonObj.addProperty("disposition",
          (value.getLastAttemptResult() == CrawlURL.CrawlResult.SUCCESS) 
            ? "SUCCESS" : "FAILURE");
      metadata.setCrawlDisposition((byte)
          ((value.getLastAttemptResult() == CrawlURL.CrawlResult.SUCCESS) ? 0 : 1));
      
      // deal with redirects ... 
      if ((value.getFlags() & CrawlURL.Flags.IsRedirected) != 0) {
        Pair<URL,JsonObject> redirect = buildRedirectObject(originalURL,value,metadata,reporter);
        if (redirect == null) { 
          return;
        }
         
        jsonObj.add("redirect_from",redirect.e1);
        finalURL = redirect.e0;
      }

      if (value.getLastAttemptResult() == CrawlURL.CrawlResult.FAILURE) {
        jsonObj.addProperty("failure_reason",CrawlURL.FailureReason.toString(value.getLastAttemptFailureReason()));
        metadata.setFailureReason(value.getLastAttemptFailureReason());
        jsonObj.addProperty("failure_detail",value.getLastAttemptFailureDetail());
        metadata.setFailureDetail(value.getLastAttemptFailureDetail());
      }
      else { 
        jsonObj.addProperty("server_ip",IPAddressUtils.IntegerToIPAddressString(value.getServerIP()));
        metadata.setServerIP(value.getServerIP());
        jsonObj.addProperty("http_result",value.getResultCode());
        metadata.setHttpResult(value.getResultCode());
        jsonObj.add("http_headers",httpHeadersToJsonObject(NIOHttpHeaders.parseHttpHeaders(value.getHeaders())));
        metadata.setHttpHeaders(value.getHeaders());
        jsonObj.addProperty("content_len",value.getContentRaw().getCount());
        metadata.setContentLength(value.getContentRaw().getCount());
        if (value.getResultCode() >= 200 && value.getResultCode() <= 299 && value.getContentRaw().getCount() > 0) { 
          contentOut = populateContentMetadata(finalURL,value,reporter,jsonObj,metadata);
          if (metadata.isFieldDirty(CrawlMetadata.Field_CHARSETDETECTED)) { 
            parseOutput.setDetectedCharset(metadata.getCharsetDetected());
          }
        }
      }
      
      // ok ... write stuff out ...
      reporter.incrCounter(Counters.WROTE_METADATA_RECORD, 1);
      //////////////////////////////////////////////////////////////
      // echo some stuff to parseOutput ... 
      parseOutput.setMetadata(jsonObj.toString());
      JsonElement mimeType = jsonObj.get("mime_type");
      if (mimeType != null) { 
        parseOutput.setNormalizedMimeType(mimeType.getAsString());
      }
      JsonElement md5 = jsonObj.get("md5");
      if (md5 != null) { 
        MD5Hash hash = new MD5Hash(md5.getAsString());
        byte[] bytes = hash.getDigest();
        parseOutput.setMd5Hash(new FlexBuffer(bytes,0,bytes.length));
      }
      JsonElement simHash = jsonObj.get("text_simhash");
      if (simHash != null) { 
        parseOutput.setSimHash(simHash.getAsLong());
      }
      parseOutput.setHostIPAddress(IPAddressUtils.IntegerToIPAddressString(value.getServerIP()));
      parseOutput.setFetchTime(value.getLastAttemptTime());
      ////////////////////////////////////////////////////////////
      
      if (contentOut != null) { 
        if (contentOut.e0 != null) {  
          parseOutput.setTextContent(contentOut.e0);
          reporter.incrCounter(Counters.WROTE_TEXT_CONTENT, 1);
        }
        if (contentOut.e1 != null) {

          // directly set the text bytes ... 
          parseOutput.getHeadersAsTextBytes().set(contentOut.e1.e0);
          // mark it dirty !!!
          parseOutput.setFieldDirty(ParseOutput.Field_HEADERS);
          // if content available ... 
          if (contentOut.e1.e1 != null) { 
            parseOutput.setRawContent(contentOut.e1.e1);
          }
          reporter.incrCounter(Counters.WROTE_RAW_CONTENT, 1);
        }
      }
      
      //buildCompactMetadata(parseOutput,jsonObj,urlMap);
      
      output.collect(new Text(finalURL.toString()), parseOutput);
    }
    catch (IOException e) { 
      LOG.error("Exception Processing URL:" + sourceURL.toString() + "\n" + CCStringUtils.stringifyException(e));
      reporter.incrCounter(Counters.GOT_UNHANDLED_IO_EXCEPTION, 1);
      //TODO:HACK
      //throw e;
    }
    catch (Exception e) {
      LOG.error("Exception Processing URL:" + sourceURL.toString() + "\n" + CCStringUtils.stringifyException(e));
      reporter.incrCounter(Counters.GOT_UNHANDLED_RUNTIME_EXCEPTION, 1);
      //TODO: HACK 
      //throw new IOException(e);
    }
  }

  /** 
   * inform tdc of successful task completion ... 
   * @throws IOException
   */
  public void commitTask() throws IOException { 
    _taskDataClient.updateTaskData(GOOD_TASK_TASKDATA_KEY, _splitInfo);
  }
  
  
  public void updateProgress(double progress) { 
    _lastProgressValue = progress;
  }
  
  double _lastProgressValue;
  long _segmentId;
  long _startTime;
  long _killTime;
  long _maxRunTime;
  boolean  _runtimeAlreadyExtended = false;
  
  TaskDataClient _taskDataClient;
  String         _splitInfo;
  @Override
  public void configure(JobConf job) {
    LOG.info("LIBRARY PATH:" + System.getenv().get("LD_LIBRARY_PATH"));
    _segmentId = job.getLong("cc.segmet.id", -1L);
    LOG.info("Job Conf says Segment Id is:" + _segmentId);
    _startTime = System.currentTimeMillis();
    _maxRunTime = job.getLong(MAX_MAPPER_RUNTIME_PROPERTY, DEFAULT_MAX_MAPPER_RUNTIME);
    LOG.info("Job Max Runtime (per config) is:" + _maxRunTime);
    _killTime = _startTime + _maxRunTime;
    // initialize the Task Data Client ... 
    try {
      _taskDataClient = TaskDataUtils.getTaskDataClientForTask(job);
    } catch (IOException e) {
      LOG.fatal("Unable to Initialize Task Data Client with Error:" + CCStringUtils.stringifyException(e));
      // hard fail
      throw new RuntimeException("Unable to Initialize Task Data Client with Error:" + CCStringUtils.stringifyException(e));
    }
    
    _splitInfo = job.get("map.input.file");
    _splitInfo += ":" +job.getLong("map.input.start",-1);
    _splitInfo += ":" +job.getLong("map.input.length",-1);
  }

  @Override
  public void close() throws IOException {
    _taskDataClient.shutdown();
  }
  
  
  private static class MockReporter implements Reporter {

    @Override
    public Counter getCounter(Enum<?> name) {
      return null;
    }
    @Override
    public Counter getCounter(String group, String name) {
      return null;
    }
    @Override
    public InputSplit getInputSplit() throws UnsupportedOperationException {
      return null;
    }
    @Override
    public void incrCounter(Enum<?> key, long amount) {}
    @Override
    public void incrCounter(String group, String counter, long amount) {}
    @Override
    public void setStatus(String status) {}
    @Override
    public void progress() {} 
  }
  
  /** 
   * some test code ... 
   * 
   * @param args
   * @throws IOException
   */
  public static void main(String[] args)throws IOException {
    Configuration conf = new Configuration();
    Path pathToCrawlLog = new Path(args[0]);
    FileSystem fs = FileSystem.get(pathToCrawlLog.toUri(),conf);
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, pathToCrawlLog, conf);
    
    Text url = new Text();
    CrawlURL urlData = new CrawlURL();
    
    ParserMapper mapper = new ParserMapper();
    MockReporter reporter = new MockReporter();
    final JsonParser parser = new JsonParser();
    
    while (reader.next(url, urlData)) { 
      mapper.map(url, urlData, 
          new OutputCollector<Text, ParseOutput>() {

            @Override
            public void collect(Text key, ParseOutput value) throws IOException {
              
              long timeStart = System.currentTimeMillis();
              JsonObject metadata = parser.parse(new JsonReader(new StringReader(value.getMetadata()))).getAsJsonObject();
              long timeEnd   = System.currentTimeMillis();
              
              System.out.println("Key:" + key.toString() + " Parse Took:" + (timeEnd-timeStart));
              System.out.println("Key:" + key.toString() + " Metadata Size:" + value.getMetadataAsTextBytes().getLength());
              System.out.println("Key:" + key.toString() + " Text-Size" + value.getTextContentAsTextBytes().getLength());
              System.out.println("Key:" + key.toString() + " RAW-Size" + value.getRawContent().getCount());
              System.out.println("Key:" + key.toString() + " Metadata:");
              JSONUtils.prettyPrintJSON(metadata);
              System.out.println("Key:" + key.toString() + " Text:");
              System.out.println(value.getTextContent());
            }
          }, reporter);
    }
    
    reader.close();
  }
}
