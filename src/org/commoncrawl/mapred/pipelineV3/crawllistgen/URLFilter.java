package org.commoncrawl.mapred.pipelineV3.crawllistgen;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.commoncrawl.mapred.ec2.postprocess.crawldb.CrawlDBCommon;
import org.commoncrawl.util.GoogleURL;
import org.commoncrawl.util.MimeTypeFilter;
import org.commoncrawl.util.URLUtils;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class URLFilter {
  
  static Pattern isAnchor = Pattern.compile("^html:a[ \\t\\n\\x0B\\f\\r:]*(.*)$");
  static Pattern hasNoFollow = Pattern.compile("[ \\t\\n\\x0B\\f\\r:]*(nofollow|no-follow)[ \\t\\n\\x0B\\f\\r:]*");
  static Pattern isImage= Pattern.compile("^html:img[ \\t\\n\\x0B\\f\\r:]*(.*)$");
  static Pattern hasTrackback = Pattern.compile("[ \\t\\n\\x0B\\f\\r:]*(trackback|pingback)[ \\t\\n\\x0B\\f\\r:]*");
  static Pattern hasTag = Pattern.compile("[ \\t\\n\\x0B\\f\\r:]*(tag)[ \\t\\n\\x0B\\f\\r:]*");
  static Pattern isMultimedia = Pattern.compile("[ \\t\\n\\x0B\\f\\r:]*(image|video|audio|img)[ \\t\\n\\x0B\\f\\r:]*");
  static Pattern urlWithExtension = Pattern.compile(".*[/]*[^.]+[.]([^.]+)$");
  static Pattern mimeTypeExtractor = Pattern.compile("[ \\t\\n\\x0B\\f\\r:]*([^/]*)/([^/]*)[ \\t\\n\\x0B\\f\\r:]*");
  public URLFilter() { 
    
  }
  
  public boolean isURLCrawlable(GoogleURL urlObject,JsonObject mergeRecord) {
    
    if (mergeRecord.has(CrawlDBCommon.TOPLEVEL_SUMMARYRECORD_PROPRETY)) {
      JsonObject summaryRecord = mergeRecord.getAsJsonObject(CrawlDBCommon.TOPLEVEL_SUMMARYRECORD_PROPRETY);
      if (summaryRecord.has(CrawlDBCommon.SUMMARYRECORD_CRAWLDETAILS_ARRAY_PROPERTY)) { 
        JsonArray crawlStatusArray = summaryRecord.getAsJsonArray(CrawlDBCommon.SUMMARYRECORD_CRAWLDETAILS_ARRAY_PROPERTY);
        for (JsonElement crawlStatus : crawlStatusArray) { 
          JsonObject crawlStatusObj = crawlStatus.getAsJsonObject();
          if (crawlStatusObj.has(CrawlDBCommon.CRAWLDETAIL_HTTPRESULT_PROPERTY)) {
            int httpResult = crawlStatusObj.get(CrawlDBCommon.CRAWLDETAIL_HTTPRESULT_PROPERTY).getAsInt(); 
            if (httpResult == 404 || httpResult == 301) { 
              // reject 404s or permanent 301s 
              return false;
            }
          }
          
          if (crawlStatusObj.has(CrawlDBCommon.CRAWLDETAIL_MIMETYPE_PROPERTY)) { 
            if (!MimeTypeFilter.isTextType(crawlStatusObj.get(CrawlDBCommon.CRAWLDETAIL_MIMETYPE_PROPERTY).getAsString())) { 
              // reject if returned mime type was not text from previous crawl...
              return false;
            }
          }
        }
      }
    }
    
    String path = urlObject.getPath();
    // match invalid extensions ... 
    int lastIndexOfDot = path.lastIndexOf('.');
    if (lastIndexOfDot != -1 && lastIndexOfDot + 1 != path.length()) { 
      String extension = path.substring(lastIndexOfDot + 1);
      if (MimeTypeFilter.invalidExtensionMatcher.matches(extension)) { 
        return false;
      }
      else if (MimeTypeFilter.isTextType(extension)){ 
        return true;
      }
    }
    /*
    Matcher extensionMatcher = urlWithExtension.matcher(path);
    if (extensionMatcher.find()) { 
      String extension = extensionMatcher.group(1);
      if (MimeTypeFilter.invalidExtensionMatcher.matches(extension)) { 
        return false;
      }
      else if (MimeTypeFilter.isTextType(extension)){ 
        return true;
      }
    }
    */
    
    if (mergeRecord.has(CrawlDBCommon.TOPLEVEL_LINKSTATUS_PROPERTY)) { 
      JsonObject linkRecord = mergeRecord.getAsJsonObject(CrawlDBCommon.TOPLEVEL_LINKSTATUS_PROPERTY);
      // if we had anchor information ...  
      if (linkRecord.has(CrawlDBCommon.LINKSTATUS_TYPEANDRELS_PROPERTY)) { 
        JsonArray typeAndRels = linkRecord.getAsJsonArray(CrawlDBCommon.LINKSTATUS_TYPEANDRELS_PROPERTY);
        for (JsonElement typeAndRel : typeAndRels) { 
          String typeAndRelText = typeAndRel.getAsString();
          // only accept html anchor tags ... 
          //TODO: OTHER TYPES ??? 
          Matcher anchorMatcher = isAnchor.matcher(typeAndRelText);
          if (anchorMatcher.matches()) { 
            if (anchorMatcher.groupCount() == 1) { 
              String anchorRelText = anchorMatcher.group(1);
              // reject rels of nofollow, tag, trackback, and multimedia etc. 
              if (hasNoFollow.matcher(anchorRelText).find()
                  || hasTrackback.matcher(anchorRelText).find()
                    || hasTag.matcher(anchorRelText).find()
                     || isMultimedia.matcher(anchorRelText).find()) { 
                
                return false;
              }
              return true;
            }
          }
          else {
            // if not anchor tag, see mime type is available in rel context... 
            // let text related mime types through ... 
            Matcher mimeTypeMatcher = mimeTypeExtractor.matcher(typeAndRelText);
            if (mimeTypeMatcher.find() && mimeTypeMatcher.groupCount() >= 1) { 
              if (MimeTypeFilter.isTextType(mimeTypeMatcher.group(1)+"/"+mimeTypeMatcher.group(2))) { 
                return true;
              }
            }
          }
        }
        // if we had tag type and rel info and we did not pass this url, reject it here ...
        if (typeAndRels.size() != 0) { 
          return false;
        }
      }
    }
    return true;
  }
  
  /** 
   * test code 
   * 
   * @param typeAndRels
   * @return
   */
  static JsonObject buildTestLinkAndRel(Integer optionalHttpResult,String optionalMimeType,ImmutableList<String> typeAndRels) { 
    JsonObject objectOut = new JsonObject();
    
    JsonObject linkStatusObj = new JsonObject();
    objectOut.add(CrawlDBCommon.TOPLEVEL_LINKSTATUS_PROPERTY, linkStatusObj);
    if (typeAndRels != null && typeAndRels.size() != 0) { 
      JsonArray array = new JsonArray();
      for (String typeAndRel : typeAndRels) { 
        array.add(new JsonPrimitive(typeAndRel));
      }
      linkStatusObj.add(CrawlDBCommon.LINKSTATUS_TYPEANDRELS_PROPERTY, array);
    }
    
    if (optionalHttpResult != null || optionalMimeType != null) { 
      JsonObject crawlSummary = new JsonObject();
      JsonObject crawlDetail = new JsonObject();
      if (optionalHttpResult != null) { 
        crawlDetail.add(CrawlDBCommon.CRAWLDETAIL_HTTPRESULT_PROPERTY, new JsonPrimitive(optionalHttpResult));
      }
      if (optionalMimeType != null) { 
        crawlDetail.add(CrawlDBCommon.CRAWLDETAIL_MIMETYPE_PROPERTY, new JsonPrimitive(optionalMimeType));
      }
      crawlSummary.add(CrawlDBCommon.SUMMARYRECORD_CRAWLDETAILS_ARRAY_PROPERTY, new JsonArray());
      crawlSummary.getAsJsonArray(CrawlDBCommon.SUMMARYRECORD_CRAWLDETAILS_ARRAY_PROPERTY).add(crawlDetail);
      objectOut.add(CrawlDBCommon.TOPLEVEL_SUMMARYRECORD_PROPRETY, crawlSummary);
    }
    return objectOut;
  }
  
  void validateURLAndMetadata(String url,Integer optionalHttpResult,String optionalMimeType,ImmutableList<String> typeAndRels,boolean expectedResult ) { 
    GoogleURL urlObject = new GoogleURL(url);
    Assert.assertTrue(urlObject.isValid());
    Assert.assertEquals(isURLCrawlable(urlObject,buildTestLinkAndRel(optionalHttpResult,optionalMimeType,typeAndRels)),expectedResult);
  }
  
  @Test
  public void validateFilter() throws Exception { 
      validateURLAndMetadata("http://test.com/test.jpg",null,null, null, false);
      validateURLAndMetadata("http://test.com/test.png",null,null, null, false);
      validateURLAndMetadata("http://test.com/test.pdf",null,null, null, true);
      validateURLAndMetadata("http://test.com/test.ext",null,null, ImmutableList.of("html:application/pdf"), true);
      
      validateURLAndMetadata("http://test.com/test.txt",null,null, null, true);
      validateURLAndMetadata("http://test.com/test.html",null,null, null, true);
      validateURLAndMetadata("http://test.com/test.pdf",null,null, null, true);
      validateURLAndMetadata("http://test.com/test.pdf",null,null, ImmutableList.of("html:img"), true);
      validateURLAndMetadata("http://www.buffalo.edu/postcards/index.html?.CGI%3A%3AObjects.p.image.ab3e0c4078fb824e9f43cd07dd44834c\u003dnorth_campus_p.jpg",
          null,null,null,false);
      
      validateURLAndMetadata("http://test.com/test.ext", null,null,ImmutableList.of("html:img"), false);
      validateURLAndMetadata("http://test.com/test.ext", null,null,ImmutableList.of("html:img:a"), false);
      validateURLAndMetadata("http://test.com/test.ext", null,null,ImmutableList.of("html:img:"), false);
      
      validateURLAndMetadata("http://test.com/test.ext", null,null,ImmutableList.of("html:a"), true);
      validateURLAndMetadata("http://test.com/test.ext", null,null,ImmutableList.of("html:a:"), true);
      validateURLAndMetadata("http://test.com/test.ext", null,null,ImmutableList.of("html:a:bar foo"), true);
      
      validateURLAndMetadata("http://test.com/test.ext", null,null,ImmutableList.of("html:a:nofollow"), false);
      validateURLAndMetadata("http://test.com/test.ext", null,null,ImmutableList.of("html:a:me nofollow"), false);
      
      validateURLAndMetadata("http://test.com/test.ext", null,null,ImmutableList.of("html:a:tag"), false);
      validateURLAndMetadata("http://test.com/test.ext", null,null,ImmutableList.of("html:a:tag foo"), false);
      validateURLAndMetadata("http://test.com/test.ext", null,null,ImmutableList.of("html:a:foo tag"), false);

      validateURLAndMetadata("http://test.com/test.ext", null,null,ImmutableList.of("html:a:image"), false);
      validateURLAndMetadata("http://test.com/test.ext", null,null,ImmutableList.of("html:a:audio"), false);
      validateURLAndMetadata("http://test.com/test.ext", null,null,ImmutableList.of("html:a:video"), false);


      validateURLAndMetadata("http://test.com/test.ext", null,null,ImmutableList.of("html:a:audio"), false);
      validateURLAndMetadata("http://test.com/test.ext", null,null,ImmutableList.of("html:a:video"), false);

      validateURLAndMetadata("http://test.com/test.ext", 301,null,null, false);
      validateURLAndMetadata("http://test.com/test.ext", 404,null,null, false);
      
      validateURLAndMetadata("http://test.com/test.ext", 200,"image/png",null, false);
      validateURLAndMetadata("http://test.com/test.ext", 200,"image/jpeg",null, false);
      validateURLAndMetadata("http://test.com/test.ext", 200,"text/pdf",null, true);
      validateURLAndMetadata("http://test.com/test.ext", 200,"text/plain",null, true);
      
      System.out.println(URLUtils.getURLFPV2FromHost("mayerm%c3%bcllerschulze.de"));
      System.out.println(URLUtils.canonicalizeURL("http://" + "mayerm%c3%bcllerschulze.de", false));
      
  }
}
