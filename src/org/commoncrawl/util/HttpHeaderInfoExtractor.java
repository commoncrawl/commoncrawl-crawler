/**
 * Copyright 2008 - CommonCrawl Foundation
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

package org.commoncrawl.util;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.text.SimpleDateFormat;

import junit.framework.Assert;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.io.NIOHttpHeaders;
import org.commoncrawl.protocol.CrawlURLMetadata;
import org.commoncrawl.util.DateUtils.DateParser;
import org.commoncrawl.util.Tuples.Pair;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;



/** 
 * uses http header information to populate http cache related information into the CrawlURLMetadata data structure 
 * 
 * @author rana
 */

public class HttpHeaderInfoExtractor {

  private static final Log LOG = LogFactory.getLog(HttpHeaderInfoExtractor.class);

  public static void parseHeaders(NIOHttpHeaders headers, CrawlURLMetadata metadataInOut)throws IOException {
    parseStatusLine(headers,metadataInOut);
    parseContentType(headers, metadataInOut);
    parseContentLength(headers,metadataInOut);
    populateETag(headers, metadataInOut);
    populateAgeValue(headers, metadataInOut);
    populateDateValue(headers, metadataInOut);
    populateLastModifiedValue(headers,metadataInOut);
    populateExpiresValue(headers,metadataInOut);
    populateCacheControlFlags(headers,metadataInOut);
  }
  

  public static void parseStatusLine(NIOHttpHeaders headers,CrawlURLMetadata metadata) {
    
    String responseLine = headers.getValue(0);
    
    parseStatusLine(responseLine,metadata);
    
  }
  
  public static void parseStatusLine(String responseLine,CrawlURLMetadata metadata) { 
    Pair<Integer,Integer> result = parseStatusLine(responseLine);
    if (result.e1 != 0) 
      metadata.setHttpResponseFlags((byte)result.e1.byteValue());
    metadata.setHttpResultCode(result.e0);
  }
  
  public static Pair<Integer,Integer> parseStatusLine(String responseLine) {
    
    Pair<Integer,Integer> resultOut = new Pair<Integer, Integer>(200,0);
    if (responseLine == null || responseLine.length() < 4) { 
      resultOut.e1 = CrawlURLMetadata.HTTPResponseFlags.HEADER_MISSING;
    }
    else{ 
      responseLine = responseLine.toLowerCase();
      if (!responseLine.startsWith("http")) { 
        resultOut.e1 = CrawlURLMetadata.HTTPResponseFlags.HEADER_MISSING;
      }
      else{
        boolean versionValid = false;
        
        if (responseLine.length() > 4 || responseLine.charAt(4) == '/') { 
          int indexOfDot = responseLine.indexOf(".",5);
          if (indexOfDot != -1 && indexOfDot != 5 || indexOfDot + 1 < responseLine.length()) { 
            char majorVersionChar = responseLine.charAt(5);
            char minorVersionChar = responseLine.charAt(indexOfDot + 1);
            if (majorVersionChar >= '0' && majorVersionChar <= '9' && minorVersionChar >= '0' && minorVersionChar <= '9') {
              int majorVersion = majorVersionChar - '0';
              int minorVersion = minorVersionChar - '0';
              if (majorVersion == 1 && minorVersion == 0) { 
                resultOut.e1 = CrawlURLMetadata.HTTPResponseFlags.VERSION_1_0;
              }
              else if (majorVersion == 1 && minorVersion == 1){ 
                resultOut.e1 = CrawlURLMetadata.HTTPResponseFlags.VERSION_1_1;
              }
              else {  
                resultOut.e1 = CrawlURLMetadata.HTTPResponseFlags.VERSION_0_9;
              }
              versionValid = true;
              
              // now skip past
              int spaceIndex = responseLine.indexOf(' ',indexOfDot + 1);
              if (spaceIndex + 1 < responseLine.length()) { 
                int digitStart = spaceIndex + 1;
                int digitEnd   = digitStart;
                while (digitEnd < responseLine.length()) {
                  char c = responseLine.charAt(digitEnd);
                  if (c >= '0' && c <= '9')
                    ++digitEnd;
                  else 
                    break;
                }
                if (digitEnd - digitStart != 0) { 
                  try { 
                    resultOut.e0 = Integer.parseInt(responseLine.substring(digitStart,digitEnd));
                  }
                  catch (NumberFormatException e) { 
                    
                  }
                }
              }
            }
          }
        }
        if (!versionValid) { 
          resultOut.e1 = CrawlURLMetadata.HTTPResponseFlags.VERSION_MISSING;
        }
      }
    }
    return resultOut;
  }
  
  static void populateETag(NIOHttpHeaders headers,CrawlURLMetadata metadata) { 
    String etagValue = headers.findValue("Etag");
    if (etagValue != null) { 
      metadata.setETag(etagValue);
    }
  }

  static void populateAgeValue(NIOHttpHeaders headers,CrawlURLMetadata metadata) { 
    String ageValue = headers.findValue("Age");
    if (ageValue != null) { 
      try { 
        long ageInSeconds= Long.parseLong(ageValue);
        metadata.setAge(ageInSeconds);
      }
      catch (NumberFormatException e) { 
        
      }
    }
  }
  
  static void populateDateValue(NIOHttpHeaders headers,CrawlURLMetadata metadata) { 
    long timeValue = getTimeHeaderValue("Date", headers);
    if (timeValue != -1) { 
      metadata.setHttpDate(timeValue);
    }
  }

  static void populateLastModifiedValue(NIOHttpHeaders headers,CrawlURLMetadata metadata) { 
    long timeValue = getTimeHeaderValue("Last-Modified", headers);
    if (timeValue != -1) { 
      metadata.setLastModifiedTime(timeValue);
    }
  }

  static void populateExpiresValue(NIOHttpHeaders headers,CrawlURLMetadata metadata) { 
    long timeValue = getTimeHeaderValue("Expires", headers);
    if (timeValue != -1) { 
      metadata.setExpires(timeValue);
    }
  }

  static final String kMaxAgePrefix = "max-age=";
  static void populateCacheControlFlags(NIOHttpHeaders headers,CrawlURLMetadata metadataInOut)throws IOException { 
    Iterator<String> i = headers.multiValueIterator("cache-control");
    while (i.hasNext()) { 
      String ccValue = i.next();
      
      StringTokenizer tokenizer = new StringTokenizer(ccValue,",");
      
      while (tokenizer.hasMoreElements()) { 
        
        String value = tokenizer.nextToken();
        
        if (value.equals("no-cache")) { 
          metadataInOut.setCacheControlFlags((byte)(
              metadataInOut.getCacheControlFlags() | CrawlURLMetadata.CacheControlFlags.NO_CACHE));
        }
        else if (value.equals("no-store")) { 
          metadataInOut.setCacheControlFlags((byte)(
              metadataInOut.getCacheControlFlags() | CrawlURLMetadata.CacheControlFlags.NO_STORE));
        }
        else if (value.equals("must-revalidate")) { 
          metadataInOut.setCacheControlFlags((byte)(
              metadataInOut.getCacheControlFlags() | CrawlURLMetadata.CacheControlFlags.NO_STORE));
        }
        else if (value.equals("private")) { 
          metadataInOut.setCacheControlFlags((byte)(
              metadataInOut.getCacheControlFlags() | CrawlURLMetadata.CacheControlFlags.PRIVATE));
        }
  
        else {
          if (value.length() > kMaxAgePrefix.length()) { 
            String valueLowerCase = value.toLowerCase();
            if (valueLowerCase.startsWith(kMaxAgePrefix)) {
              try { 
                long maxAgeInSeconds = Long.parseLong(value.substring(kMaxAgePrefix.length()));
                metadataInOut.setMaxAge(maxAgeInSeconds);
              }
              catch (NumberFormatException e) { 
                
              }
            }
          }
        }
      }
    }
    Iterator<String> j = headers.multiValueIterator("pragma");
    while (j.hasNext()) {
      String value = j.next();
      if (value.equals("no-cache")) { 
        metadataInOut.setCacheControlFlags((byte)(
            metadataInOut.getCacheControlFlags() | CrawlURLMetadata.CacheControlFlags.NO_CACHE));
      }
    }
    
    String varyValue = headers.findValue("vary");
    if (varyValue != null && varyValue.equals("*")) { 
      metadataInOut.setCacheControlFlags((byte)(
          metadataInOut.getCacheControlFlags() | CrawlURLMetadata.CacheControlFlags.VARY));
    }
  }

  
  static void parseContentLength(NIOHttpHeaders headers,CrawlURLMetadata metadata) { 
    String contentLenValue = headers.findValue("Content-Length");
    if (contentLenValue != null) { 
      try { 
        metadata.setHttpContentLength(Integer.parseInt(contentLenValue));
      }
      catch (Exception e) { 
      }
    }
  }
  

  
  static void parseContentType(NIOHttpHeaders headers,CrawlURLMetadata metadata) {
    
    Iterator<String> j = headers.multiValueIterator("content-type");

    while (j.hasNext()) { 
      
      String contentType = j.next();
      
      if (contentType != null) { 
      	parseContentType(metadata,contentType);
      }
    }
  }  

  public static final void  parseContentType(CrawlURLMetadata metadataOut,String contentType) { 
    //  Trim leading and trailing whitespace from type.  We include '(' in
    //  the trailing trim set to catch media-type comments, which are not at all
    //  standard, but may occur in rare cases.
	  int type_val = HttpHeaderUtils.skipPastLWS(contentType,0);
	  type_val = Math.min(type_val,contentType.length());
	  int type_end = HttpHeaderUtils.skipToLWSAndExtra(contentType, type_val);
	  if (type_end == -1)
	    type_end = contentType.length();
	    
	  int charset_val = 0;
	  int charset_end = 0;
	
	  //  Iterate over parameters
	  boolean type_has_charset = false;
	  int param_start = contentType.indexOf(';', type_end);
	  if (param_start != -1) {
	    //    We have parameters.  Iterate over them.
	    int cur_param_start = param_start + 1;
	    do {
	      int cur_param_end = contentType.indexOf(';',cur_param_start);
	      if (cur_param_end == -1) 
	        cur_param_end = contentType.length();
	      int param_name_start = HttpHeaderUtils.skipPastLWS(contentType,cur_param_start);
	      param_name_start = Math.min(param_name_start, cur_param_end);
	      int charset_end_offset = Math.min(param_name_start + HttpHeaderUtils.kCharset.length(), cur_param_end);
	      if (contentType.substring(param_name_start,charset_end_offset).equalsIgnoreCase(HttpHeaderUtils.kCharset)) {
	        charset_val = param_name_start + HttpHeaderUtils.kCharset.length();
	        charset_end = cur_param_end;
	        type_has_charset = true;
	      }
	      cur_param_start = cur_param_end + 1;
	    } while (cur_param_start < contentType.length());
	  }
	
	  if (type_has_charset) {
	    try { 
	//    Trim leading and trailing whitespace from charset_val.  We include
	//    '(' in the trailing trim set to catch media-type comments, which are
	//    not at all standard, but may occur in rare cases.
	      charset_val = HttpHeaderUtils.skipPastLWS(contentType,charset_val);
	      charset_val = Math.min(charset_val, charset_end);
	      if (charset_val == contentType.length()) { 
	        type_has_charset = false;
	      }
	      else { 
	        char first_char = contentType.charAt(charset_val);
	        if (first_char == '"' || first_char == '\'') {
	          ++charset_val;
	          charset_end = contentType.indexOf(first_char,charset_val);
	          if (charset_end == -1) 
	            charset_end = HttpHeaderUtils.skipToLWSAndExtra(contentType,charset_val);
	        } else {
	          charset_end = Math.min(HttpHeaderUtils.skipToLWSAndExtra(contentType,charset_val),charset_end);
	        }
	      }
	    }
	    catch (IndexOutOfBoundsException e) { 
	      type_has_charset = false;
	    }
	  }
	
	  //  if the server sent "*/*", it is meaningless, so do not store it.
	  //  also, if type_val is the same as mime_type, then just update the
	  //  charset.  however, if charset is empty and mime_type hasn't
	  //  changed, then don't wipe-out an existing charset.  We
	  //  also want to reject a mime-type if it does not include a slash.
	  //  some servers give junk after the charset parameter, which may
	  //  include a comma, so this check makes us a bit more tolerant.
	  if (contentType.length() != 0 && !contentType.equals("*/*") && contentType.indexOf('/') != -1) {
	    String originalContentType = metadataOut.getContentType();
	    metadataOut.setContentType(contentType.substring(type_val,type_end).toLowerCase());
	    
	    if (type_has_charset) {
	    	metadataOut.setCharset(contentType.substring(charset_val,charset_end).toLowerCase());
	    }
	    else { 
	      if (metadataOut.getCharset().length() != 0 && !originalContentType.equals(metadataOut.getContentType())) { 
	      	metadataOut.setCharset("");
	      	metadataOut.setFieldClean(CrawlURLMetadata.Field_CHARSET);
	      }
	    }
	  }
  	
  }
  
  private static long getTimeHeaderValue(String keyName,NIOHttpHeaders headers) { 
    String value = headers.findValue(keyName);
    if (value != null) {
      return getTime(value);
    }
    return -1;
  }
  
  static String _datePatterns[] =  
    new String [] {
        "EEE, dd-MMM-yyyy HH:mm:ss zzz",
        "EEE MMM dd HH:mm:ss yyyy",
        "EEE MMM dd HH:mm:ss yyyy zzz",
        "EEE, MMM dd HH:mm:ss yyyy zzz",
        "EEE, dd MMM yyyy HH:mm:ss zzz",
        "EEE,dd MMM yyyy HH:mm:ss zzz",
        "EEE, dd MMM yyyy HH:mm:sszzz",
        "EEE, dd MMM yyyy HH:mm:ss",
        "EEE, dd-MMM-yy HH:mm:ss zzz",
        "EEE, dd-MMM-yy zzz",
        "EEE, dd MMM yyyy zzz",
        "EEE MMM dd yyyy zzz",
        "EEE, dd MMM yyyy HH:mm zzz",
        "yyyy/MM/dd HH:mm:ss.SSS zzz",
        "yyyy/MM/dd HH:mm:ss.SSS",
        "yyyy/MM/dd HH:mm:ss zzz",
        "yyyy/MM/dd",
        "yyyy.MM.dd HH:mm:ss",
        "yyyy.MM.dd",
        "yyyy-MM-dd HH:mm",
        "yyyy-MM-dd HH:mm:ss",
        "MMM dd yyyy HH:mm:ss. zzz",
        "MMM dd yyyy HH:mm:ss zzz",
        "dd.MM.yyyy HH:mm:ss zzz",
        "dd MM yyyy HH:mm:ss zzz",
        "dd.MM.yyyy; HH:mm:ss",
        "dd.MM.yyyy HH:mm:ss",
        "dd.MM.yyyy zzz",
        "dd.MM.yyyy",
        "dd/MM/yyyy hh:mm:ss aa zzz",
        "dd/MM/yyyy hh:mm:ss aa",
        "dd/MM/yyyy HH:mm:ss zzz",
        "dd/MM/yyyy HH:mm:ss",
        "dd.MM.yyyy zzz"
  };
  
  static ThreadLocal<DateParser> _dateParser = new ThreadLocal<DateParser>() { 
    protected DateParser initialValue() {
      return new DateParser(_datePatterns);
    }
  };
  
  static ThreadLocal<SimpleDateFormat> _httpDateParser = new ThreadLocal<SimpleDateFormat>() { 
    
    @Override
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US); 
    }
  };
  
  
  static ImmutableSet<String> badDatePatterns = 
    new ImmutableSet.Builder<String>()
      
    .add("-1")
    .add("0")
    .add("GMT")
    .add("now")
    .add("Now()")
      .build();
  
  static Pattern onlyDigits = Pattern.compile("[0-9]*");
  
  static Pattern specialTSMatcher = Pattern.compile("\\{\\s*ts\\s*'([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2}):([0-9]{2}):([0-9]{2})'\\s*\\}");
  
  @SuppressWarnings("deprecation")
  public static long getTime(String date) {
    long time = -1;
    
    if (date != null) {
      date = date.trim();
      if (date.length() != 0 && !badDatePatterns.contains(date)) {
        
        try {
          
          try { 
            if (onlyDigits.matcher(date).matches()) { 
              time = Long.parseLong(date);
            }
            Matcher specialTS = specialTSMatcher.matcher(date);
            if (specialTS.matches()) { 
              time = new Date(
                  Integer.parseInt(specialTS.group(1)), // year
                  Integer.parseInt(specialTS.group(2)), // month
                  Integer.parseInt(specialTS.group(3)), // day    
                  Integer.parseInt(specialTS.group(4)), // hr
                  Integer.parseInt(specialTS.group(5)), // min
                  Integer.parseInt(specialTS.group(6))).getTime(); // ss
            }
          }
          catch (Exception e) { 
            
          }
          if (time == -1 ) { 
            long timeStartForSimpleDateFormatParse = System.currentTimeMillis();
            time = _httpDateParser.get().parse(date).getTime();
            long timeEndForSimpleDateFormatParse = System.currentTimeMillis();
            // LOG.info("#### Date Parse (MostCommon) Took:" + (timeEndForSimpleDateFormatParse - timeStartForSimpleDateFormatParse));
          }
        } catch (Exception e) {
          
            time = DateUtils.parseHttpDate(date);
            if (time == -1) { 
              // try to parse it as date in alternative format
              try {
                  
                  long timeStartForNewParser = System.currentTimeMillis();
                  Date parsedDate = _dateParser.get().parseDate(date);
                  long timeEndForNewPaser = System.currentTimeMillis();
                  // LOG.info("#### Date Parse (New) Took:" + (timeEndForNewPaser - timeStartForNewParser));
                  
                  time = parsedDate.getTime();
                  // if (LOG.isWarnEnabled()) {
                  //   LOG.warn(url + ": parsed date: " + date +" to:"+time);
                  // }
              } catch (Exception e2) {
                LOG.error("can't parse erroneous date: " + date);
              }
            }
        }
      }
    }
    return time;
  }
  
  @Test
  public void validateParser() throws Exception {
    validateCacheControlParser();
    validateContentTypeParser();
  }
  
  private void validateContentTypeParser() throws Exception { 
    String sampleHeaders[] = { 
        "HTTP/1.1 200 OK\n"
        +        "Content-type: text/html\n",
              "text/html", 
              "", 
            // Multiple content-type headers should give us the last one.
         "HTTP/1.1 200 OK\n"
        +        "Content-type: text/html\n"
        +        "Content-type: text/html\n",
              "text/html", 
              "", 
         "HTTP/1.1 200 OK\n"
        +        "Content-type: text/plain\n"
        +        "Content-type: text/html\n"
        +        "Content-type: text/plain\n"
        +        "Content-type: text/html\n",
              "text/html", 
              "", 
            // Test charset parsing.
         "HTTP/1.1 200 OK\n"
        +        "Content-type: text/html\n"
        +        "Content-type: text/html; charset=ISO-8859-1\n",
              "text/html", 
              "iso-8859-1", 
            // Test charset in double quotes.
         "HTTP/1.1 200 OK\n"
        +        "Content-type: text/html\n"
        +        "Content-type: text/html; charset=\"ISO-8859-1\"\n",
              "text/html", 
              "iso-8859-1", 
            // If there are multiple matching content-type headers, we carry
            // over the charset value.
         "HTTP/1.1 200 OK\n"
        +        "Content-type: text/html;charset=utf-8\n"
        +        "Content-type: text/html\n",
              "text/html", 
              "utf-8", 
            // Test single quotes.
         "HTTP/1.1 200 OK\n"
        +        "Content-type: text/html;charset='utf-8'\n"
        +        "Content-type: text/html\n",
              "text/html", 
              "utf-8", 
            // Last charset wins if matching content-type.
         "HTTP/1.1 200 OK\n"
        +        "Content-type: text/html;charset=utf-8\n"
        +        "Content-type: text/html;charset=iso-8859-1\n",
              "text/html", 
              "iso-8859-1", 
            // Charset is ignored if the content types change.
         "HTTP/1.1 200 OK\n"
        +        "Content-type: text/plain;charset=utf-8\n"
        +        "Content-type: text/html\n",
              "text/html", 
              "", 
            // Empty content-type
         "HTTP/1.1 200 OK\n"
        +        "Content-type: \n",
              "", 
              "", 
            // Emtpy charset
         "HTTP/1.1 200 OK\n"
        +        "Content-type: text/html;charset=\n",
              "text/html", 
              "", 
            // Multiple charsets, last one wins.
         "HTTP/1.1 200 OK\n"
        +        "Content-type: text/html;charset=utf-8; charset=iso-8859-1\n",
              "text/html", 
              "iso-8859-1", 
            // Multiple params.
         "HTTP/1.1 200 OK\n"
        +        "Content-type: text/html; foo=utf-8; charset=iso-8859-1\n",
              "text/html", 
              "iso-8859-1", 
         "HTTP/1.1 200 OK\n"
        +        "Content-type: text/html ; charset=utf-8 ; bar=iso-8859-1\n",
              "text/html", 
              "utf-8", 
            // Comma embeded in quotes.
         "HTTP/1.1 200 OK\n"
        +        "Content-type: text/html ; charset='utf-8,text/plain' ;\n",
              "text/html", 
              "utf-8,text/plain", 
            // Charset with leading spaces.
         "HTTP/1.1 200 OK\n"
        +        "Content-type: text/html ; charset= 'utf-8' ;\n",
              "text/html", 
              "utf-8", 
            // Media type comments in mime-type.
         "HTTP/1.1 200 OK\n"
        +        "Content-type: text/html (html)\n",
              "text/html", 
              "", 
            // Incomplete charset= param
         "HTTP/1.1 200 OK\n"
        +        "Content-type: text/html; char=\n",
              "text/html", 
              "", 
            // Invalid media type: no slash
         "HTTP/1.1 200 OK\n"
        +        "Content-type: texthtml\n",
              "", 
              "", 
            // Invalid media type: */*
         "HTTP/1.1 200 OK\n"
        +        "Content-type: */*\n",
              "", 
              ""
        
    };
    
    int testCount = sampleHeaders.length / 3;
    
    for (int i=0;i<testCount;++i) { 
      String header = sampleHeaders[i*3];
      String expectedContentType = sampleHeaders[(i*3) + 1];
      String expectedCharsetType = sampleHeaders[(i*3) + 2];
      
      NIOHttpHeaders headers = NIOHttpHeaders.parseHttpHeaders(header);
      CrawlURLMetadata metadata = new CrawlURLMetadata();
      System.out.println("****Original Header:" + header);
      System.out.println("Exepcted ContentType:" + expectedContentType);
      System.out.println("Exepcted Charset:"     + expectedCharsetType);
      System.out.println("****Parsed Results:");
      parseContentType(headers,metadata);
      if (metadata.isFieldDirty(CrawlURLMetadata.Field_CONTENTTYPE)) { 
        System.out.println("ContentType:" + metadata.getContentType());
        Assert.assertTrue(expectedContentType.length() == metadata.getContentType().length());
        if (expectedContentType.length() != 0) { 
          Assert.assertTrue(expectedContentType.equals(metadata.getContentType()));
        }
        
      }
      else { 
        Assert.assertTrue(expectedContentType.length() == 0);
      }
      
      if (metadata.isFieldDirty(CrawlURLMetadata.Field_CHARSET)) { 
        System.out.println("Charset:" + metadata.getCharset());
        Assert.assertTrue(expectedCharsetType.length() == metadata.getCharset().length());
        if (expectedCharsetType.length() != 0) { 
          Assert.assertTrue(expectedCharsetType.equals(metadata.getCharset()));
        }
      }
      else { 
        Assert.assertTrue(expectedCharsetType.length() == 0);
      }
    }
  }
  
  
  private void validateCacheControlParser() throws Exception { 
    String sampleHeaders[] = { 
        
        "HTTP/1.1 200 OK\n"
        +      "Etag: \"34534-d3 134q\"\n"
        +      "\n",
            // valid for a little while
            "HTTP/1.1 200 OK\n"
        +      "cache-control: max-age=10000\n"
        +      "\n",
            // expires in the future
            "HTTP/1.1 200 OK\n"
        +      "date: Wed, 28 Nov 2007 00:40:11 GMT\n"
        +      "expires: Wed, 28 Nov 2007 01:00:00 GMT\n"
        +      "\n",
            // expired already
            "HTTP/1.1 200 OK\n"
        +      "date: Wed, 28 Nov 2007 00:40:11 GMT\n"
        +      "expires: Wed, 28 Nov 2007 00:00:00 GMT\n"
        +      "\n",
            // max-age trumps expires
            "HTTP/1.1 200 OK\n"
        +    "HTTP/1.1 200 OK\n"
        +      "\n",
            // valid for a little while
            "HTTP/1.1 200 OK\n"
        +      "cache-control: max-age=10000\n"
        +      "\n",
            // expires in the future
            "HTTP/1.1 200 OK\n"
        +      "date: Wed, 28 Nov 2007 00:40:11 GMT\n"
        +      "expires: Wed, 28 Nov 2007 01:00:00 GMT\n"
        +      "\n",
            // expired already
            "HTTP/1.1 200 OK\n"
        +      "date: Wed, 28 Nov 2007 00:40:11 GMT\n"
        +      "expires: Wed, 28 Nov 2007 00:00:00 GMT\n"
        +      "\n",
            // max-age trumps expires
            "HTTP/1.1 200 OK\n"
        +      "date: Wed, 28 Nov 2007 00:40:11 GMT\n"
        +      "expires: Wed, 28 Nov 2007 00:00:00 GMT\n"
        +      "cache-control: max-age=10000\n"
        +      "\n",
            // last-modified heuristic: modified a while ago
            "HTTP/1.1 200 OK\n"
        +      "date: Wed, 28 Nov 2007 00:40:11 GMT\n"
        +      "last-modified: Wed, 27 Nov 2007 08:00:00 GMT\n"
        +      "\n",
            // last-modified heuristic: modified recently
            "HTTP/1.1 200 OK\n"
        +      "date: Wed, 28 Nov 2007 00:40:11 GMT\n"
        +      "last-modified: Wed, 28 Nov 2007 00:40:10 GMT\n"
        +      "\n",
            // cached permanent redirect
            "HTTP/1.1 301 Moved Permanently\n"
        +      "\n",
            // cached redirect: not reusable even though by default it would be
            "HTTP/1.1 300 Multiple Choices\n"
        +      "Cache-Control: no-cache\n"
        +      "\n",
            // cached forever by default
            "HTTP/1.1 410 Gone\n"
        +      "\n",
            // cached temporary redirect: not reusable
            "HTTP/1.1 302 Found\n"
        +      "\n",
            // cached temporary redirect: reusable
            "HTTP/1.1 302 Found\n"
        +      "cache-control: max-age=10000\n"
        +      "\n",
            // cache-control: max-age=N overrides expires: date in the past
            "HTTP/1.1 200 OK\n"
        +      "date: Wed, 28 Nov 2007 00:40:11 GMT\n"
        +      "expires: Wed, 28 Nov 2007 00:20:11 GMT\n"
        +      "cache-control: max-age=10000\n"
        +      "\n",
            // cache-control: no-store overrides expires: in the future
            "HTTP/1.1 200 OK\n"
        +      "date: Wed, 28 Nov 2007 00:40:11 GMT\n"
        +      "expires: Wed, 29 Nov 2007 00:40:11 GMT\n"
        +      "cache-control: no-store,private,no-cache=\"foo\"\n"
        +      "\n",
            // pragma: no-cache overrides last-modified heuristic
            "HTTP/1.1 200 OK\n"
        +      "date: Wed, 28 Nov 2007 00:40:11 GMT\n"
        +      "last-modified: Wed, 27 Nov 2007 08:00:00 GMT\n"
        +      "pragma: no-cache\n"
        +      "\n",      "date: Wed, 28 Nov 2007 00:40:11 GMT\n"
        +      "expires: Wed, 28 Nov 2007 00:00:00 GMT\n"
        +      "cache-control: max-age=10000\n"
        +      "\n",
            // last-modified heuristic: modified a while ago
            "HTTP/1.1 200 OK\n"
        +      "date: Wed, 28 Nov 2007 00:40:11 GMT\n"
        +      "last-modified: Wed, 27 Nov 2007 08:00:00 GMT\n"
        +      "\n",
            // last-modified heuristic: modified recently
            "HTTP/1.1 200 OK\n"
        +      "date: Wed, 28 Nov 2007 00:40:11 GMT\n"
        +      "last-modified: Wed, 28 Nov 2007 00:40:10 GMT\n"
        +      "\n",
            // cached permanent redirect
            "HTTP/1.1 301 Moved Permanently\n"
        +      "\n",
            // cached redirect: not reusable even though by default it would be
            "HTTP/1.1 300 Multiple Choices\n"
        +      "Cache-Control: no-cache\n"
        +      "\n",
            // cached forever by default
            "HTTP/1.1 410 Gone\n"
        +      "\n",
            // cached temporary redirect: not reusable
            "HTTP/1.1 302 Found\n"
        +      "\n",
            // cached temporary redirect: reusable
            "HTTP/1.1 302 Found\n"
        +      "cache-control: max-age=10000\n"
        +      "\n",
            // cache-control: max-age=N overrides expires: date in the past
            "HTTP/1.1 200 OK\n"
        +      "date: Wed, 28 Nov 2007 00:40:11 GMT\n"
        +      "expires: Wed, 28 Nov 2007 00:20:11 GMT\n"
        +      "cache-control: max-age=10000\n"
        +      "\n",
            // cache-control: no-store overrides expires: in the future
            "HTTP/1.1 200 OK\n"
        +      "date: Wed, 28 Nov 2007 00:40:11 GMT\n"
        +      "expires: Wed, 29 Nov 2007 00:40:11 GMT\n"
        +      "cache-control: no-store,private,no-cache=\"foo\"\n"
        +      "\n",
            // pragma: no-cache overrides last-modified heuristic
            "HTTP/1.1 200 OK\n"
        +      "date: Wed, 28 Nov 2007 00:40:11 GMT\n"
        +      "last-modified: Wed, 27 Nov 2007 08:00:00 GMT\n"
        +      "pragma: no-cache\n"
        +      "\n"        
    };
    
    for (String header : sampleHeaders) { 
      NIOHttpHeaders headers = NIOHttpHeaders.parseHttpHeaders(header);
      CrawlURLMetadata metadata = new CrawlURLMetadata();
      System.out.println("****Original Header:" + header);
      System.out.println("****Parsed Results:");
      try { 
        parseHeaders(headers,metadata);
        if (metadata.isFieldDirty(CrawlURLMetadata.Field_HTTPRESPONSEFLAGS)) {
          StringBuffer buffer = new StringBuffer();
          buffer.append("ResponseFlags:");
          if ((metadata.getHttpResponseFlags() & CrawlURLMetadata.HTTPResponseFlags.HEADER_MISSING) != 0) { 
            buffer.append(",HeaderMissing");
          }
          if ((metadata.getHttpResponseFlags() & CrawlURLMetadata.HTTPResponseFlags.VERSION_MISSING) != 0) { 
            buffer.append(",VersionMissing");
          }
          if ((metadata.getHttpResponseFlags() & CrawlURLMetadata.HTTPResponseFlags.VERSION_0_9) != 0) { 
            buffer.append(",Version0.9");
          }
          if ((metadata.getHttpResponseFlags() & CrawlURLMetadata.HTTPResponseFlags.VERSION_1_0) != 0) { 
            buffer.append(",Version1.0");
          }
          if ((metadata.getHttpResponseFlags() & CrawlURLMetadata.HTTPResponseFlags.VERSION_1_1) != 0) { 
            buffer.append(",Version1.1");
          }
          System.out.println(buffer.toString());
        }
        if (metadata.isFieldDirty(CrawlURLMetadata.Field_HTTPRESULTCODE)) { 
          System.out.println("HttpResultCode:" + metadata.getHttpResultCode());
        }
        if (metadata.isFieldDirty(CrawlURLMetadata.Field_ETAG)) { 
          System.out.println("ETag:" + metadata.getETag());
        }
        if (metadata.isFieldDirty(CrawlURLMetadata.Field_AGE)) { 
          System.out.println("Age:" + metadata.getAge());
        }
        if (metadata.isFieldDirty(CrawlURLMetadata.Field_HTTPDATE)) { 
          System.out.println("Date:" + metadata.getHttpDate());
        }
        if (metadata.isFieldDirty(CrawlURLMetadata.Field_LASTMODIFIEDTIME)) { 
          System.out.println("Last-Modified:" + metadata.getLastModifiedTime());
        }
        if (metadata.isFieldDirty(CrawlURLMetadata.Field_EXPIRES)) { 
          System.out.println("Expires:" + metadata.getExpires());
        }
        if (metadata.isFieldDirty(CrawlURLMetadata.Field_MAXAGE)) { 
          System.out.println("MaxAge:" + metadata.getMaxAge());
        }
        if (metadata.isFieldDirty(CrawlURLMetadata.Field_CACHECONTROLFLAGS)) { 
          StringBuffer buffer = new StringBuffer();
          buffer.append("CacheControl:");
          if ((metadata.getCacheControlFlags() & CrawlURLMetadata.CacheControlFlags.NO_CACHE) != 0)
            buffer.append("no-cache,");
          if ((metadata.getCacheControlFlags() & CrawlURLMetadata.CacheControlFlags.NO_STORE) != 0)
            buffer.append("no-store,");
          if ((metadata.getCacheControlFlags() & CrawlURLMetadata.CacheControlFlags.MUST_REVALIDATE) != 0)
            buffer.append("must-revalidate,");
          if ((metadata.getCacheControlFlags() & CrawlURLMetadata.CacheControlFlags.VARY) != 0)
            buffer.append("vary,");
          if ((metadata.getCacheControlFlags() & CrawlURLMetadata.CacheControlFlags.PRIVATE) != 0)
            buffer.append("private,");
          
          System.out.println(buffer.toString());
        }
      }
      catch (IOException e) { 
        System.out.println(CCStringUtils.stringifyException(e));
      }
    }
  }
  
  
  
  
}
