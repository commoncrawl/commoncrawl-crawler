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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.io.shared.NIOHttpHeaders;
import org.commoncrawl.protocol.CrawlURLMetadata;
import org.junit.Test;

/**
 * 
 * @author rana
 *
 */
public class HttpCacheUtils {
  
  static final long kDefaultExpireTime = 86400000L * 7;
  
  private static final Log LOG = LogFactory.getLog(HttpCacheUtils.class);  

  
  public static boolean isCacheable(CrawlURLMetadata metadata) { 
    if ((((int)metadata.getCacheControlFlags()) & 
        (CrawlURLMetadata.CacheControlFlags.NO_CACHE | CrawlURLMetadata.CacheControlFlags.NO_STORE | CrawlURLMetadata.CacheControlFlags.VARY)) != 0) {
      return false;
    }
    return true;
  }
  
  public static class LifeTimeInfo { 
    
    enum Source { 
      CacheControl,
      MaxAge,
      Expires,
      LastModifiedTime,
      CurrentTime,
      PermanentRedirect,
      NoCacheMetadata
    }
    public long _lifetime = 0;
    public Source _source;
  }
  
  public static LifeTimeInfo getFreshnessLifetimeInMilliseconds(CrawlURLMetadata metadata) {
    
    LifeTimeInfo infoOut = new LifeTimeInfo();
    
    if ((((int)metadata.getCacheControlFlags()) & 
          (CrawlURLMetadata.CacheControlFlags.NO_CACHE | CrawlURLMetadata.CacheControlFlags.NO_STORE | CrawlURLMetadata.CacheControlFlags.VARY)) != 0) {
      //LOG.info("#### CACHE GetFreshness - Found no-cache or no-store or vary. Freshness Lifetime = 0");
      infoOut._source = LifeTimeInfo.Source.CacheControl;
      return infoOut;
    }
    
    if (metadata.isFieldDirty(CrawlURLMetadata.Field_MAXAGE)) {
      //LOG.info("#### CACHE GetFreshness - Found max-age of:" + metadata.getMaxAge());
      // return max-age in milliseconds 
      infoOut._lifetime = (metadata.getMaxAge() * 1000);
      infoOut._source = LifeTimeInfo.Source.MaxAge;
      return infoOut;
    }    
    // figure out fetch time to use ... either date or actual fetch time 
    long fetchTime = 0;
    if (metadata.isFieldDirty(CrawlURLMetadata.Field_HTTPDATE)) { 
      fetchTime = metadata.getHttpDate();
    }
    else if (metadata.isFieldDirty(CrawlURLMetadata.Field_LASTFETCHTIMESTAMP)) {
      fetchTime = metadata.getLastFetchTimestamp();
    }


    
    // otherwise , if no max-age ... see if expires is present 
    if (metadata.isFieldDirty(CrawlURLMetadata.Field_EXPIRES)) {
      infoOut._lifetime = Math.max(0,metadata.getExpires() - fetchTime);
      infoOut._source = LifeTimeInfo.Source.Expires;
      return infoOut;
    }

    // otherwise ... if http 200 ... 
    if (metadata.getHttpResultCode() == 200 || metadata.getHttpResultCode() == 203) {
      // and cache control does not specify must-revalidate ... 
      if ((((int)metadata.getCacheControlFlags()) & CrawlURLMetadata.CacheControlFlags.MUST_REVALIDATE) == 0) {
        long timeRemainingBeforeValidate = 0;
        // if last_modified is present ... 
        if (metadata.isFieldDirty(CrawlURLMetadata.Field_LASTMODIFIEDTIME) && metadata.getLastModifiedTime() <= fetchTime) {
          //LOG.info("#### CACHE GetFreshness - returning expireTime  - (fetchTime - lastModifiedTime):" + Math.max(0,kDefaultExpireTime - (fetchTime - metadata.getLastModifiedTime())));
          
          infoOut._lifetime = Math.max(0,kDefaultExpireTime - (fetchTime - metadata.getLastModifiedTime()));
          infoOut._source = LifeTimeInfo.Source.LastModifiedTime;
        }
        
        if (infoOut._lifetime == 0) { 
          // use fetch time as last modified time ... 
          //LOG.info("#### CACHE GetFreshness - returning expireTime  - (currentTime - lastModifiedTime):" + Math.max(0,kDefaultExpireTime - (System.currentTimeMillis() - fetchTime)));
          infoOut._lifetime = Math.max(0,kDefaultExpireTime - (System.currentTimeMillis() - fetchTime));
          infoOut._source = LifeTimeInfo.Source.CurrentTime;
        }
        return infoOut;
      }
    }
    if (metadata.getHttpResultCode() == 300 || metadata.getHttpResultCode() == 301 || metadata.getHttpResultCode() == 410) { 
      infoOut._lifetime = Long.MAX_VALUE;
      infoOut._source = LifeTimeInfo.Source.PermanentRedirect;

      return infoOut;
    }
    infoOut._source = LifeTimeInfo.Source.NoCacheMetadata;
    
    return infoOut;
  }
  
  public static long getCurrentAgeInMilliseconds(CrawlURLMetadata metadata) { 
    // If there is no Date header, then assume that the server response was
    // generated at the time when we received the response.
    long date_value = metadata.getLastFetchTimestamp();
    if (metadata.isFieldDirty(CrawlURLMetadata.Field_HTTPDATE)) { 
      date_value = metadata.getHttpDate();
    }
    // If there is no Age header, then assume age is zero.  GetAgeValue does not
    // modify its out param if the value does not exist.
    long age_value = metadata.getAge() * 1000;
        
    long apparent_age = Math.max(0, metadata.getLastFetchTimestamp() - date_value);
    long corrected_received_age = Math.max(apparent_age, age_value);
    long resident_time          = System.currentTimeMillis() - metadata.getLastFetchTimestamp();
    long current_age            = corrected_received_age + resident_time;
    
    return current_age;
  }
  
  public static boolean requiresValidation(CrawlURLMetadata metadata) { 
    LifeTimeInfo lifetimeInfo = getFreshnessLifetimeInMilliseconds(metadata);
    if (lifetimeInfo._lifetime == 0){ 
      //LOG.info("#### CACHE requiresValidation - (getFreshnessLifetime returned zero) - YES");
      return true;
    }
    else {
      //LOG.info("#### CACHE requiresValidation - (lifetime <= getCurrentAgeInMilliseconds(metadata)) -" +(lifetime <= getCurrentAgeInMilliseconds(metadata))); 
      return (lifetimeInfo._lifetime <= getCurrentAgeInMilliseconds(metadata));
    }
  }
  
  @Test
  public void TestCacheUtils() throws Exception {
    validateCacheControlParser();
  }
  
  
  private void validateCacheControlParser() throws Exception { 
    String sampleHeaders[] = { 
        // last-modified heuristic: modified a while ago
        "HTTP/1.1 200 OK\n"
        +      "date: Wed, 28 Nov 2007 00:40:11 GMT\n"
        +      "last-modified: Wed, 27 Nov 2007 08:00:00 GMT\n"
        +      "\n",
        
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
        HttpHeaderInfoExtractor.parseHeaders(headers,metadata);
        // set fetch time 
        metadata.setLastFetchTimestamp(System.currentTimeMillis());
        
        LifeTimeInfo freshnessLifetime = getFreshnessLifetimeInMilliseconds(metadata);
        long currentAge        = getCurrentAgeInMilliseconds(metadata);
        boolean requiresValidation = requiresValidation(metadata);
        
        System.out.println("Freshness:" + freshnessLifetime._lifetime + " CurrentAge:" + currentAge + " RequiresRevalidation:" + requiresValidation);
        
      }
      catch (IOException e) { 
        
      }
    }
  }

}
