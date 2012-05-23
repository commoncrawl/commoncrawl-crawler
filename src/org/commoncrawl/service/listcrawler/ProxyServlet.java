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

package org.commoncrawl.service.listcrawler;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.record.Buffer;
import org.apache.nutch.util.GZIPUtils;
import org.apache.tools.ant.filters.StringInputStream;
import org.commoncrawl.async.Timer;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.io.NIOHttpConnection;
import org.commoncrawl.io.NIOHttpHeaders;
import org.commoncrawl.protocol.CacheItem;
import org.commoncrawl.protocol.CrawlURL;
import org.commoncrawl.protocol.CrawlURLMetadata;
import org.commoncrawl.protocol.shared.ArcFileHeaderItem;
import org.commoncrawl.protocol.shared.ArcFileItem;
import org.commoncrawl.rpc.base.internal.AsyncRequest;
import org.commoncrawl.rpc.base.shared.RPCException;
import org.commoncrawl.service.crawler.CrawlItemStatusCallback;
import org.commoncrawl.service.crawler.CrawlTarget;
import org.commoncrawl.service.listcrawler.CacheManager.CacheItemCheckCallback;
import org.commoncrawl.service.queryserver.ContentQueryRPCInfo;
import org.commoncrawl.service.queryserver.ContentQueryRPCResult;
import org.commoncrawl.util.GoogleURL;
import org.commoncrawl.util.HttpHeaderInfoExtractor;
import org.commoncrawl.util.URLFingerprint;
import org.commoncrawl.util.URLUtils;
import org.commoncrawl.util.ArcFileItemUtils;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.CharsetUtils;
import org.commoncrawl.util.FlexBuffer;

import com.google.common.collect.ImmutableSet;

/** 
 * Servlet that serves cached content via the crawler's cache 
 * 
 * @author rana
 *
 */
public class ProxyServlet extends HttpServlet { 
  
  private static final Log LOG = LogFactory.getLog(ProxyServlet.class);
  
  private static final String PROXY_HEADER_SOURCE="x-ccproxy-source";
  private static final String PROXY_HEADER_ORIG_STATUS="x-ccproxy-original-status";
  private static final String PROXY_HEADER_TIMER="x-ccproxy-timer";
  private static final String PROXY_HEADER_FINALURL="x-ccproxy-final-url";
  private static final String PROXY_HEADER_TRUNCATION="x-ccproxy-truncated";
  private static final String PROXY_HEADER_ORIGINAL_CONTENT_LEN="x-ccproxy-orig-content-len";
  
  private static final String PROXY_RENDER_TYPE_TEXT = "text";
  private static final String PROXY_RENDER_TYPE_NONE = "none";

  
  private static class AsyncResponse {
    public enum ResponseType { 
      HTTPErrorResponse,
      CacheItemResponse,
      CrawlURLResponse,
      S3Response
    }
    
    private long         _startTime = System.currentTimeMillis();
    private int          _httpErrorCode = 400;
    private String       _httpErrorCodeDesc = "";
    private ResponseType _responseType = ResponseType.HTTPErrorResponse;
    private CacheItem    _cacheItem = null;
    private CrawlURL     _urlItem = null;
    private ArcFileItem  _arcFileItem = null;
    private boolean      _isCrawlComplete = false;
    
    public ResponseType getResponseType() { return _responseType; }
    public CacheItem getCacheItem() { return _cacheItem; }
    public ArcFileItem getArcFileItem() { return _arcFileItem; }
    
    public CrawlURL getCrawlURL() { return _urlItem; }
    public int      getHttpErrorCode() { return _httpErrorCode; }
    public String   getHttpErrorDesc() { return _httpErrorCodeDesc; }
    
    public synchronized boolean isCrawlComplete() { return _isCrawlComplete; }
    public synchronized void setCrawlComplete(boolean isComplete) { _isCrawlComplete = isComplete; }
      
    
    public void setStartTime(long startTime) { 
      _startTime = startTime;
    }
    
    public long getStartTime() { return _startTime; }
    
    public void setCacheItemRespone(CacheItem item) { 
      _responseType = ResponseType.CacheItemResponse;
      _cacheItem    = item;
    }
    
    public void setS3ItemResponse(ArcFileItem item){
      _responseType = ResponseType.S3Response;
      _arcFileItem = item;
    }
    
    public void setURLItemRespone(CrawlURL item) { 
      _responseType = ResponseType.CrawlURLResponse;
      _urlItem    = item;
    }
    
    public void setHttpErrorResponse(int httpErrorCode,String httpErrorResponse) { 
      _responseType = ResponseType.HTTPErrorResponse;
      _httpErrorCode = httpErrorCode;
      _httpErrorCodeDesc = httpErrorResponse;
    }
  };
  
  
  public ProxyServlet() { 
    
  }
  
  
  private static ArrayList<ArcFileHeaderItem> populateHeaders(String headerData){ 

    ArrayList<ArcFileHeaderItem> headerItems = new ArrayList<ArcFileHeaderItem>();
    
    BufferedReader reader = new BufferedReader(new InputStreamReader(new StringInputStream(headerData)));      
    
    String line = null;
    
    try { 
      while ((line = reader.readLine()) != null) { 
        if (line.length() != 0) { 
          int colonPos = line.indexOf(':');

          ArcFileHeaderItem item = new ArcFileHeaderItem();

          if (colonPos != -1 && colonPos != line.length() - 1) { 

            item.setItemKey(line.substring(0,colonPos));
            item.setItemValue(line.substring(colonPos + 1));
          }
          else {
            item.setItemValue(line);
           }
          headerItems.add(item);
        }
      }
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
    }
    return headerItems;
  }
  
  private static void cacheS3ItemResult(ArcFileItem itemResult,String targetURL,long fingerpint) { 
    CacheItem cacheItem = new CacheItem();
    
    cacheItem.setUrlFingerprint(fingerpint);
    cacheItem.setUrl(targetURL);
    cacheItem.setSource((byte)CacheItem.Source.S3Cache);
    cacheItem.setHeaderItems(itemResult.getHeaderItems());
    cacheItem.setFieldDirty(CacheItem.Field_HEADERITEMS);
    cacheItem.setContent(new Buffer(itemResult.getContent().getReadOnlyBytes(),0,itemResult.getContent().getCount()));
    if ((itemResult.getFlags() & ArcFileItem.Flags.TruncatedInDownload) != 0) { 
      cacheItem.setFlags(cacheItem.getFlags() | CacheItem.Flags.Flag_WasTruncatedDuringDownload);
    }
    if ((itemResult.getFlags() & ArcFileItem.Flags.TruncatedInInflate) != 0) { 
      cacheItem.setFlags(cacheItem.getFlags() | CacheItem.Flags.Flag_WasTruncatedDuringInflate);
    }
    

    ProxyServer.getSingleton().getCache().cacheItem(cacheItem,null);
  }
  
  /**
   * Calculate the number of IO operations requires to cache a given CrawlURL 
   */
  public static int calculateCachedItemCountGivenCrawlURL(CrawlURL urlObject) { 
  	int cachedItemCount = 0;
  	try { 
	  	if ((urlObject.getFlags() & CrawlURL.Flags.IsRedirected) != 0) {
	      String originalCanonicalURL = URLUtils.canonicalizeURL(urlObject.getUrl(),true);
	      String redirectCanonicalURL = URLUtils.canonicalizeURL(urlObject.getRedirectURL(),true);
	      
	      if (!originalCanonicalURL.equals(redirectCanonicalURL)) { 
	      	cachedItemCount++;
	      }
	  	}
	  	
	    if (urlObject.getLastAttemptResult() == CrawlURL.CrawlResult.SUCCESS) { 
	    	cachedItemCount++;
	    }
  	}
  	catch (IOException e) { 
  		LOG.error("Encountered Exception while calculating cachedItemCount:" + CCStringUtils.stringifyException(e));
  	}
  	return cachedItemCount;
  }
  
  /**
   * Process a CrawlURL object, and inject any valid contents into the cache
   * @param urlResult  - the CrawlURL object containing crawl result
   * @param completionSempahore - a completion semaphore that will be released an appropriate number of times 
   * when IO operations complete - SEE calculateCachedItemCountGivenCrawlURL
   */
  public static void cacheCrawlURLResult(CrawlURL urlResult,Semaphore optionalCompletionSempahore) { 
    try { 
      // first check to see this was a redirect ...  
      if ((urlResult.getFlags() & CrawlURL.Flags.IsRedirected) != 0) {
        
        // check to see if canonical urls are the same 
        String originalCanonicalURL = URLUtils.canonicalizeURL(urlResult.getUrl(),true);
        String redirectCanonicalURL = URLUtils.canonicalizeURL(urlResult.getRedirectURL(),true);
        
        if (!originalCanonicalURL.equals(redirectCanonicalURL)) { 
          // try to cache the redirect ... 
          CacheItem cacheItem = new CacheItem();
          
          cacheItem.setUrlFingerprint(urlResult.getFingerprint());
          cacheItem.setUrl(URLUtils.canonicalizeURL(urlResult.getUrl(),true));
          cacheItem.setFinalURL(urlResult.getRedirectURL());
          cacheItem.setSource((byte)CacheItem.Source.WebRequest);
          cacheItem.setHeaderItems(populateHeaders(urlResult.getOriginalHeaders()));
          cacheItem.setFieldDirty(CacheItem.Field_HEADERITEMS);
  
          switch (urlResult.getOriginalResultCode()) { 
            case 301: cacheItem.setFlags((byte)CacheItem.Flags.Flag_IsPermanentRedirect);break;
            default:  cacheItem.setFlags((byte)CacheItem.Flags.Flag_IsTemporaryRedirect);break;
          }
          
          if ((urlResult.getFlags() & CrawlURL.Flags.TruncatedDuringDownload) !=0) { 
            cacheItem.setFlags(cacheItem.getFlags() | CacheItem.Flags.Flag_WasTruncatedDuringDownload);
          }
          
          //LOG.info("### CACHING Item:" + cacheItem.getUrl());
          ProxyServer.getSingleton().getCache().cacheItem(cacheItem,optionalCompletionSempahore);
        }
      }
      
      if (urlResult.getLastAttemptResult() == CrawlURL.CrawlResult.SUCCESS) { 
        
        CacheItem cacheItem = new CacheItem();
        
        boolean isRedirect = (urlResult.getFlags() & CrawlURL.Flags.IsRedirected) != 0;
        
        String cannonicalURL = URLUtils.canonicalizeURL((isRedirect) ? urlResult.getRedirectURL() : urlResult.getUrl(),true);
        
        cacheItem.setUrl(cannonicalURL);
        cacheItem.setUrlFingerprint(URLFingerprint.generate64BitURLFPrint(cannonicalURL));
        cacheItem.setSource((byte)CacheItem.Source.WebRequest);
        cacheItem.setFieldDirty(CacheItem.Field_HEADERITEMS);
        cacheItem.setHeaderItems(populateHeaders(urlResult.getHeaders()));
        
        // detect content encoding 
        for (ArcFileHeaderItem headerItem : cacheItem.getHeaderItems()) { 
          if (headerItem.getItemKey().equalsIgnoreCase("content-encoding")) { 
            if (headerItem.getItemValue().equalsIgnoreCase("gzip") || headerItem.getItemValue().equalsIgnoreCase("deflate")) { 
              // set compressed flag
              cacheItem.setFlags((byte)(cacheItem.getFlags() | CacheItem.Flags.Flag_IsCompressed));
            }
            break;
          }
        }
        cacheItem.setContent(new FlexBuffer(urlResult.getContentRaw().getReadOnlyBytes(),0,urlResult.getContentRaw().getCount()));
        
        //LOG.info("### CACHING Item:" + cacheItem.getUrl());
        ProxyServer.getSingleton().getCache().cacheItem(cacheItem,optionalCompletionSempahore);
      }
    }
    catch (MalformedURLException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
    }
  }
  
  private static void addHeaderItem(ArrayList<ArcFileHeaderItem> items,String name,String value) { 
    ArcFileHeaderItem item = new ArcFileHeaderItem();
    item.setItemKey(name);
    item.setItemValue(value);
    items.add(1,item);
  }
  private static void removeHeaderItem(ArrayList<ArcFileHeaderItem> items,String name) { 
    for (int i=0;i<items.size();++i) { 
      if (items.get(i).getItemKey().equalsIgnoreCase(name)) { 
        items.remove(i);
        break;
      }
    }
  }
  
  private static ImmutableSet<String> dontProxyHeaders = ImmutableSet.of(
      "proxy-connection",
      "connection",
      "keep-alive",
      "transfer-encoding",
      "te",
      "trailer",
      "proxy-authorization",
      "proxy-authenticate",
      "upgrade",
      "content-length",
      "content-encoding"
      );
  
  private static BufferedReader readerForCharset(NIOHttpHeaders headers,byte[] content,int contentLength,PrintWriter debugWriter)throws IOException { 
    
    CrawlURLMetadata metadata = new CrawlURLMetadata();
    HttpHeaderInfoExtractor.parseHeaders(headers, metadata);
    
    String charset = metadata.getCharset();
    
    if (charset.length() !=0 ) { 
      debugWriter.println("***** Charset(via HttpHeaders):" + charset);
    }
    else { 
      charset = CharsetUtils.sniffCharacterEncoding(content,0,contentLength);
      if (charset != null) { 
        debugWriter.println("***** Charset(via HTML MetaTag):" + charset);
      }
    }
    if (charset == null || charset.length() == 0) { 
      charset = "ASCII";
      debugWriter.println("***** Charset(NotFount-UsingDefault):ASCII");                
    }
    

    Charset charsetObj = Charset.forName(charset);
    
    if (charsetObj == null) { 
      debugWriter.println("***** Could Not Create CharsetDecoder for charset:" + charset);
      LOG.info("Unable to create Charsetcharset. Using ASCII");
      charsetObj = Charset.forName("ASCII");
    }
    
    debugWriter.println("***** Content:");
    
    return new BufferedReader(new InputStreamReader(new ByteArrayInputStream(content,0,contentLength)));      
  }
  
  private static void sendS3ItemResponse(final HttpServletRequest req, final HttpServletResponse response,ArcFileItem responseItem,String renderAs,AsyncResponse responseObject,long requestStartTime)throws IOException {
    
    CacheItem cacheItem = new CacheItem();
    
    // populate a cache item object ... 
    cacheItem.setHeaderItems(responseItem.getHeaderItems());
    cacheItem.setFieldDirty(CacheItem.Field_HEADERITEMS);
    cacheItem.setUrl(responseItem.getUri());
    cacheItem.setUrlFingerprint(URLUtils.getCanonicalURLFingerprint(responseItem.getUri(),true));
    cacheItem.setSource((byte)CacheItem.Source.S3Cache);
    cacheItem.setContent(new Buffer(responseItem.getContent().getReadOnlyBytes(),0,responseItem.getContent().getCount()));
    
    sendCacheItemResponse(req,response,cacheItem,true,renderAs,responseObject,requestStartTime);
    
  }
  
  private static void sendCacheItemResponse(final HttpServletRequest req, final HttpServletResponse response,CacheItem responseItem,boolean isS3Response,String renderAs,AsyncResponse responseObject,long requestStartTime)throws IOException { 
    
    // remove default headers ... 
    response.setHeader("Date",null);
    response.setHeader("Server",null);
    
    // parse response code in headers ... 
    CrawlURLMetadata metadata = new CrawlURLMetadata();
    
    HttpHeaderInfoExtractor.parseStatusLine(responseItem.getHeaderItems().get(0).getItemValue(), metadata);
    
    if (!metadata.isFieldDirty(CrawlURLMetadata.Field_HTTPRESULTCODE)) {
      metadata.setHttpResultCode(200);
    }
    // set the result code ... 
    response.setStatus(metadata.getHttpResultCode());

    if (renderAs.equals(PROXY_RENDER_TYPE_TEXT)) {
      
      response.setHeader("content-type", "text/plain");
      
      PrintWriter writer = response.getWriter();

      writer.write(responseItem.getHeaderItems().get(0).getItemValue() + "\n");
      
      if (isS3Response)
        writer.write(PROXY_HEADER_SOURCE+":s3\n");
      else 
        writer.write(PROXY_HEADER_SOURCE+":cache\n");
      writer.write(PROXY_HEADER_TIMER+":" +  (System.currentTimeMillis() - requestStartTime) +"MS\n");
      writer.write(PROXY_HEADER_FINALURL+":" +  responseItem.getFinalURL() + "\n");

      
      writer.write("content-length:" + Integer.toString(responseItem.getContent().getCount()) + "\n");
      if ((responseItem.getFlags() & CacheItem.Flags.Flag_IsCompressed) != 0) {
        writer.write("content-encoding:gzip\n");
      }
      
      String truncationFlags = "";
      if ((responseItem.getFlags() & CacheItem.Flags.Flag_WasTruncatedDuringDownload) != 0) { 
        truncationFlags += ArcFileItem.Flags.toString(ArcFileItem.Flags.TruncatedInDownload);
      }
      if ((responseItem.getFlags() & CacheItem.Flags.Flag_WasTruncatedDuringInflate) != 0) {
        if (truncationFlags.length() !=0)
          truncationFlags +=",";
        truncationFlags += ArcFileItem.Flags.toString(ArcFileItem.Flags.TruncatedInInflate);
      }

      if (truncationFlags.length() !=0) { 
        writer.write(PROXY_HEADER_TRUNCATION + ":" + truncationFlags + "\n");
      }
            // iterate items 
      for (ArcFileHeaderItem headerItem : responseItem.getHeaderItems()) {
        // ignore unwanted items
        if (headerItem.getItemKey().length() != 0) { 
          if (headerItem.getItemValue().length() !=0) { 
            if (!dontProxyHeaders.contains(headerItem.getItemKey().toLowerCase())) {
              // and send other ones through 
              writer.write(headerItem.getItemKey()+ ":"+headerItem.getItemValue()+"\n");
            }
            else { 
              if (headerItem.getItemKey().equalsIgnoreCase("content-length")) { 
                writer.write(PROXY_HEADER_ORIGINAL_CONTENT_LEN+":"+headerItem.getItemValue()+"\n");
              }              
            }
          }
        }
      }
      writer.write("\n");
      
      int contentLength = responseItem.getContent().getCount();
      byte contentData[] = responseItem.getContent().getReadOnlyBytes();
      
      if ((responseItem.getFlags() & CacheItem.Flags.Flag_IsCompressed) != 0) {
        contentData = GZIPUtils.unzipBestEffort(contentData,CrawlEnvironment.CONTENT_SIZE_LIMIT);
        contentLength = contentData.length;
      }
      
      NIOHttpHeaders headers = ArcFileItemUtils.buildHeaderFromArcFileItemHeaders(responseItem.getHeaderItems());
      
      BufferedReader bufferedReader = readerForCharset(headers, contentData, contentLength, writer);
      try { 
        String line = null;
        while ((line = bufferedReader.readLine()) != null) { 
          writer.println(line);
        }
      }
      finally { 
        bufferedReader.close();
      }
      writer.flush();
    }
    else { 
      
      // set the content length ... 
      response.setHeader("content-length", Integer.toString(responseItem.getContent().getCount()));
      if ((responseItem.getFlags() & CacheItem.Flags.Flag_IsCompressed) != 0) {
        response.setHeader("content-encoding","gzip");
      }
      if (isS3Response)
        response.setHeader(PROXY_HEADER_SOURCE,"s3");
      else 
        response.setHeader(PROXY_HEADER_SOURCE,"cache");
      
      response.setHeader(PROXY_HEADER_TIMER,(System.currentTimeMillis() - requestStartTime) +"MS");
      response.setHeader(PROXY_HEADER_FINALURL,responseItem.getFinalURL());

      String truncationFlags = "";
      if ((responseItem.getFlags() & CacheItem.Flags.Flag_WasTruncatedDuringDownload) != 0) { 
        truncationFlags += ArcFileItem.Flags.toString(ArcFileItem.Flags.TruncatedInDownload);
      }
      if ((responseItem.getFlags() & CacheItem.Flags.Flag_WasTruncatedDuringInflate) != 0) {
        if (truncationFlags.length() !=0)
          truncationFlags +=",";
        truncationFlags += ArcFileItem.Flags.toString(ArcFileItem.Flags.TruncatedInInflate);
      }
      if (truncationFlags.length() !=0) { 
        response.setHeader(PROXY_HEADER_TRUNCATION,truncationFlags);
      }      
      
      // iterate items 
      for (ArcFileHeaderItem headerItem : responseItem.getHeaderItems()) {
        // ignore unwanted items
        if (headerItem.getItemKey().length() != 0) { 
          if (headerItem.getItemValue().length() !=0) { 
            if (!dontProxyHeaders.contains(headerItem.getItemKey().toLowerCase())) {
              // and send other ones through 
              response.setHeader(headerItem.getItemKey(), headerItem.getItemValue());
            }
            else { 
              if (headerItem.getItemKey().equalsIgnoreCase("content-length")) { 
                response.setHeader(PROXY_HEADER_ORIGINAL_CONTENT_LEN,headerItem.getItemValue());
              }                
            }
          }
        }
      }

      ServletOutputStream responseOutputStream = response.getOutputStream();
      BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(responseOutputStream,Charset.forName("ASCII")));
      
      // write out content bytes 
      responseOutputStream.write(responseItem.getContent().getReadOnlyBytes(), 0, responseItem.getContent().getCount());
      
    }
    ProxyServer.getSingleton().logProxySuccess(metadata.getHttpResultCode(), (isS3Response) ? "s3" : "cache", responseItem.getUrl(), responseItem.getFinalURL(), responseObject.getStartTime());
  }
  
  private static void sendCrawlURLResponse(final HttpServletRequest req, final HttpServletResponse response,CrawlURL url,String renderAs,AsyncResponse responseObject,long requestStartTime)throws IOException { 
    
    if (url.getLastAttemptResult() == CrawlURL.CrawlResult.SUCCESS) { 
            
      // remove default headers ... 
      response.setHeader("Date",null);
      response.setHeader("Server",null);
      // set the result code ... 
      response.setStatus(200);
      
      if (renderAs.equals(PROXY_RENDER_TYPE_TEXT)) {
       
        response.setHeader("content-type", "text/plain");

        // parse headers ... 
        NIOHttpHeaders headers = NIOHttpHeaders.parseHttpHeaders(url.getHeaders());
        
        
        PrintWriter writer = response.getWriter();
       
        writer.write(PROXY_HEADER_SOURCE+":origin\n");
        writer.write(PROXY_HEADER_ORIG_STATUS+":" + headers.getValue(0) + "\n");
        writer.write(PROXY_HEADER_TIMER+":" +  (System.currentTimeMillis() - requestStartTime) +"MS\n");
        writer.write(PROXY_HEADER_FINALURL+":" +  (((url.getFlags() & CrawlURL.Flags.IsRedirected) != 0) ? url.getRedirectURL() : url.getUrl()) + "\n");

        
        // and put then in a map ... 
        Map<String,List<String> > headerItems = NIOHttpHeaders.parseHttpHeaders(url.getHeaders()).getHeaders();
        
        
        writer.write("content-length:" + Integer.toString(url.getContentRaw().getCount()) + "\n");
        
        // pull out content encoding if it is set ...
        String contentEncoding = headers.findValue("content-encoding");
        
        if (contentEncoding != null) { 
          writer.write("content-encoding:" + contentEncoding + "\n");
        }
        
        String truncationFlags = "";
        if ((url.getFlags() & CrawlURL.Flags.TruncatedDuringDownload) != 0) { 
          truncationFlags += ArcFileItem.Flags.toString(ArcFileItem.Flags.TruncatedInDownload);
        }
        if (truncationFlags.length() !=0) { 
          writer.write(PROXY_HEADER_TRUNCATION + ":" + truncationFlags + "\n");
        }
        
        // now walk remaining headers ... 
        for (Map.Entry<String, List<String>> entry : headerItems.entrySet()) { 
          // if not in exclusion list ... 
          if (entry.getKey() != null && entry.getKey().length() != 0) { 
            if (!dontProxyHeaders.contains(entry.getKey().toLowerCase())) { 
              // and it has values ... 
              if (entry.getValue() != null) { 
                for (String value : entry.getValue()) { 
                  writer.write(entry.getKey() + ":" + value + "\n");
                }
              }
            }
            else { 
              if (entry.getKey().equalsIgnoreCase("content-length") && entry.getValue() != null) { 
                writer.write(PROXY_HEADER_ORIGINAL_CONTENT_LEN+":"+entry.getValue().get(0)+"\n");
              }
            }
          }
        }
        writer.write("\n");
        
        int contentLength = url.getContentRaw().getCount();
        byte contentData[] = url.getContentRaw().getReadOnlyBytes();
        
        if (contentEncoding != null && contentEncoding.equalsIgnoreCase("gzip")) {
          contentData = GZIPUtils.unzipBestEffort(contentData,CrawlEnvironment.CONTENT_SIZE_LIMIT);
          contentLength = contentData.length;
        }
        
        BufferedReader bufferedReader = readerForCharset(headers, contentData, contentLength, writer);
        
        try { 
          String line = null;
          while ((line = bufferedReader.readLine()) != null) { 
            writer.println(line);
          }
        }
        finally { 
          bufferedReader.close();
        }
        writer.flush();   
      }
      else { 
        
        response.setHeader(PROXY_HEADER_SOURCE,"origin");
        response.setHeader(PROXY_HEADER_TIMER,(System.currentTimeMillis() - requestStartTime) +"MS");
        response.setHeader(PROXY_HEADER_FINALURL,(((url.getFlags() & CrawlURL.Flags.IsRedirected) != 0) ? url.getRedirectURL() : url.getUrl()));

        // parse headers ... 
        NIOHttpHeaders headers = NIOHttpHeaders.parseHttpHeaders(url.getHeaders());
        // and put then in a map ... 
        Map<String,List<String> > headerItems = NIOHttpHeaders.parseHttpHeaders(url.getHeaders()).getHeaders();
        
        // set the content length ... 
        response.setHeader("content-length", Integer.toString(url.getContentRaw().getCount()));
        
        String truncationFlags = "";
        if ((url.getFlags() & CrawlURL.Flags.TruncatedDuringDownload) != 0) { 
          truncationFlags += ArcFileItem.Flags.toString(ArcFileItem.Flags.TruncatedInDownload);
        }
        if (truncationFlags.length() !=0) { 
          response.setHeader(PROXY_HEADER_TRUNCATION,truncationFlags);
        }
        
        // pull out content encoding if it is set ...
        String contentEncoding = headers.findValue("content-encoding");
        
        if (contentEncoding != null) { 
          response.setHeader("content-encoding", contentEncoding);
        }
        
        // now walk remaining headers ... 
        for (Map.Entry<String, List<String>> entry : headerItems.entrySet()) { 
          // if not in exclusion list ... 
          if (entry.getKey() != null && entry.getKey().length() != 0) { 
            if (!dontProxyHeaders.contains(entry.getKey().toLowerCase())) { 
              // and it has values ... 
              if (entry.getValue() != null) { 
                for (String value : entry.getValue()) { 
                  response.setHeader(entry.getKey(),value);
                }
              }
            }
            else { 
              if (entry.getKey().equalsIgnoreCase("content-length") && entry.getValue() != null) { 
                response.setHeader(PROXY_HEADER_ORIGINAL_CONTENT_LEN,entry.getValue().get(0));
              }
            }
            
          }
        }
        
        ServletOutputStream responseOutputStream = response.getOutputStream();
        // write out content bytes 
        responseOutputStream.write(url.getContentRaw().getReadOnlyBytes(), 0, url.getContentRaw().getCount());
      }
    }
    // otherwise failed for some other reason ... 
    else {
      /*
      ProxyServer.getSingleton().logProxyFailure(500, CrawlURL.FailureReason.toString(url.getLastAttemptFailureReason()) + " - " + url.getLastAttemptFailureDetail(),
          url.getUrl(),
          url.getRedirectURL(),
          requestStartTime);
      */
      // report the reason ... 
      response.sendError(500, CrawlURL.FailureReason.toString(url.getLastAttemptFailureReason()) + " - " + url.getLastAttemptFailureDetail());
    }
  }
  
  private static void queueQueryMasterURLRequest(final String targetURL,final long urlFingerprint,final AsyncResponse responseData,final Semaphore completionSemaphore,final long timeoutInMS,final boolean skipHTTPFetch) {
    ContentQueryRPCInfo rpcQueryInfo = new ContentQueryRPCInfo();
    //TODO:UNFORTUNATE HACK 
    GoogleURL canonicalURL = new GoogleURL(targetURL);
    rpcQueryInfo.setUrl(canonicalURL.getCanonicalURL());
    
    try {
      ProxyServer.getSingleton().getQueryMasterStub().doContentQuery(rpcQueryInfo,new AsyncRequest.Callback<ContentQueryRPCInfo,ContentQueryRPCResult> () {

        @Override
        public void requestComplete(AsyncRequest<ContentQueryRPCInfo, ContentQueryRPCResult> request) {
          if (request.getStatus() == AsyncRequest.Status.Success && request.getOutput().getSuccess()) { 
            if (request.getOutput().getArcFileResult().getContent().getCount() == (CrawlEnvironment.ORIGINAL_CONTENT_SIZE_LIMIT + 1)) {
              LOG.error("RPC to QueryMaster Successfull BUT content size is 131072. Suspecting truncation. REJECTING S3 Data for targetURL:" + targetURL);
              queueHighPriorityURLRequest(targetURL,urlFingerprint,responseData,completionSemaphore,timeoutInMS,skipHTTPFetch);
            }
            else { 
              LOG.info("RPC to QueryMaster Successfull. Servicing request for targetURL:" + targetURL + " via s3 cache");
              // cache the http result 
              cacheS3ItemResult(request.getOutput().getArcFileResult(),targetURL,urlFingerprint);
              // set the result data .. 
              responseData.setS3ItemResponse(request.getOutput().getArcFileResult());
              // and set the completion semaphore ... 
              completionSemaphore.release();
            }
          }
          else { 
            LOG.info("RPC to QueryMaster Failed. Servicing request for targetURL:" + targetURL + " via crawler");
            queueHighPriorityURLRequest(targetURL,urlFingerprint,responseData,completionSemaphore,timeoutInMS,skipHTTPFetch);
          }
        } 
      });
    } catch (RPCException e) {
      LOG.error("RPC to Query Master for targetURL:" + targetURL + " Failed with Exception:" + CCStringUtils.stringifyException(e));
      // queue it up for direct service via crawler ... 
      queueHighPriorityURLRequest(targetURL,urlFingerprint,responseData,completionSemaphore,timeoutInMS,skipHTTPFetch);
    }
  }
  private static void queueHighPriorityURLRequest(final String targetURL,final long urlFingerprint,final AsyncResponse responseData,final Semaphore completionSemaphore,final long timeoutInMS,final boolean skipHTTPFetch) { 
    
    // first check skip fetch flag ... 
    if (skipHTTPFetch) { 
      // setup an async callback ... 
      ProxyServer.getSingleton().getEventLoop().setTimer(new Timer(0,false, new Timer.Callback() {

        @Override
        public void timerFired(Timer timer) {
          responseData.setHttpErrorResponse(403, "Request Not Found In Cache");
          responseData.setCrawlComplete(true);
          // and set the completion semaphore ... 
          completionSemaphore.release();
        }
      }));
      
      return;
    }
    
    // 3. ok time to dispatch this request via the crawler ... 
    ProxyServer.getSingleton().queueHighPriorityURL(targetURL, urlFingerprint, new CrawlItemStatusCallback() {

      @Override
      public void crawlComplete(NIOHttpConnection connection,CrawlURL urlObject, CrawlTarget optTargetObj,boolean success) {
        if (!success) { 
          // set failure code on url .. 
          urlObject.setLastAttemptResult((byte)CrawlURL.CrawlResult.FAILURE);
        }
        // cache the http result 
        cacheCrawlURLResult(urlObject,null);
        
        // if item was not timed out ... 
        if (!responseData.isCrawlComplete()) { 
          // set the result data .. 
          responseData.setURLItemRespone(urlObject);
          // and set the completion semaphore ... 
          completionSemaphore.release();
        }
      }

      @Override
      public void crawlStarting(CrawlTarget target) {
        // reset start time to http request start time ...
        responseData.setStartTime(System.currentTimeMillis());
      } 
      
    });
    
    // and setup a timeout timer ... 
    ProxyServer.getSingleton().getEventLoop().setTimer(new Timer(timeoutInMS,false, new Timer.Callback() {

      @Override
      public void timerFired(Timer timer) {
        // check to see if request is already complete or not 
        if (!responseData.isCrawlComplete()) { 
          responseData.setHttpErrorResponse(500, "Request Timed Out");
          responseData.setCrawlComplete(true);
          // and set the completion semaphore ... 
          completionSemaphore.release();
        }
      }
    }));
  }
  
  private static boolean checkCacheForURL(final String targetURL,final AsyncResponse responseData,final Semaphore completionSemaphore,final long timeoutInMS,final boolean skipHTTPFetch) {
    
    // normalize the url ... 
    
    try {
      final String normalizedURL = URLUtils.canonicalizeURL(targetURL,true);
      final long  urlFingerprint = URLFingerprint.generate64BitURLFPrint(normalizedURL);
    
      //1.  check cache for data 
      ProxyServer.getSingleton().getCache().checkCacheForItem(normalizedURL,urlFingerprint, new CacheItemCheckCallback() {

        @Override
        public void cacheItemAvailable(String url, CacheItem item) {
          
          // if redirected ... get redirected url ... 
          if ((item.getFlags() & (CacheItem.Flags.Flag_IsPermanentRedirect|CacheItem.Flags.Flag_IsTemporaryRedirect)) != 0) {
            LOG.info("Redirect Detected for TargetURL:" + targetURL + " Checking Cache for Final URL:" + item.getFinalURL());
            // resubmit the request to the cache 
            if (!checkCacheForURL(item.getFinalURL(), responseData, completionSemaphore,timeoutInMS,skipHTTPFetch)) { 
              // immediate failure detected ...
              responseData.setHttpErrorResponse(400,"Malformed Exception parsing Redirect URL:" + item.getFinalURL());
              // release completion semaphore 
              completionSemaphore.release();                
            }
          }
          // otherwise no redirects detected .. 
          else { 
            LOG.info("Servicing Response for URL:" + url + " via cache. Item Content Size is:" + item.getContent().getCount());
            // if cached data is available ... 
            // set the appropriate data member in the response object ... 
            // and return to the calling thread (so that it can do the blocking io to service the request)
            responseData.setCacheItemRespone(item);
            // release completion semaphore 
            completionSemaphore.release();
          }
        }

        @Override
        public void cacheItemNotFound(String url) {
          
          // 2. time to hit the query master server (if available)
          if (false /*ProxyServer.getSingleton().isConnectedToQueryMaster()*/) { 
            LOG.info("Query Master Online. Sending Request:" + targetURL + " to queryMaster");
            queueQueryMasterURLRequest(targetURL, urlFingerprint, responseData, completionSemaphore,timeoutInMS,skipHTTPFetch);
          }
          else {
            LOG.info("Query Master Offline. Sending Request:" + targetURL + " directly to crawler");
            // otherwise skip and go direct to crawler queue ... 
            queueHighPriorityURLRequest(targetURL, urlFingerprint, responseData, completionSemaphore,timeoutInMS,skipHTTPFetch);
          }
          // 2. ok hit the query master if available 
          // 
            
        } 
      });
      // response will complete asynchronously ... 
      return true;
    }
    catch(MalformedURLException e){ 
      responseData.setHttpErrorResponse(400,"Malformed Exception parsing URL:" + targetURL);
      // immdediate response 
      return false;
    }
  }

  private static boolean checkCacheForURLV2(final String targetURL,final AsyncResponse responseData,final Semaphore completionSemaphore,final long timeoutInMS,final boolean skipHTTPFetch) {
    
    // normalize the url ... 
    
    try {
      final String normalizedURL = URLUtils.canonicalizeURL(targetURL,true);
      final long   urlFingerprint = URLFingerprint.generate64BitURLFPrint(normalizedURL);
    
      //1.  check cache for data 
      CacheItem item = ProxyServer.getSingleton().getCache().checkCacheForItemInWorkerThread(normalizedURL,urlFingerprint);
      
      if (item != null) {
          // if redirected ... get redirected url ... 
          if ((item.getFlags() & (CacheItem.Flags.Flag_IsPermanentRedirect|CacheItem.Flags.Flag_IsTemporaryRedirect)) != 0) {
            LOG.info("Redirect Detected for TargetURL:" + targetURL + " Checking Cache for Final URL:" + item.getFinalURL());
            // resubmit the request to the cache 
            return checkCacheForURLV2(item.getFinalURL(), responseData, completionSemaphore,timeoutInMS,skipHTTPFetch);
          }
          // otherwise no redirects detected .. 
          else { 
            LOG.info("Servicing Response for URL:" + targetURL + " via cache. Item Content Size is:" + item.getContent().getCount());
            // if cached data is available ... 
            // set the appropriate data member in the response object ... 
            // and return to the calling thread (so that it can do the blocking io to service the request)
            responseData.setCacheItemRespone(item);
            return false;
          }
      }
      else {
          ProxyServer.getSingleton().getEventLoop().setTimer(new Timer(0,false,new Timer.Callback() {

            @Override
            public void timerFired(Timer timer) {
              LOG.info("Query Master Offline. Sending Request:" + targetURL + " directly to crawler");
              // otherwise skip and go direct to crawler queue ... 
              queueHighPriorityURLRequest(targetURL, urlFingerprint, responseData, completionSemaphore,timeoutInMS,skipHTTPFetch);
            } 
          }));
          
          // response will complete asynchronously ... 
          return true;
      }
    }
    catch(MalformedURLException e){ 
      responseData.setHttpErrorResponse(400,"Malformed Exception parsing URL:" + targetURL);
    }
    // immdediate response 
    return false;
    
  }

  /*
  @Override
  public void doGet(final HttpServletRequest req, final HttpServletResponse response)throws ServletException, IOException {
    
    // allocate a response data object ... which will be used by async thread to pass data to calling thread...
    final AsyncResponse responseData = new AsyncResponse();

    final String path   = req.getParameter("url");
    final String format = (req.getParameter("renderAs") != null) ? req.getParameter("renderAs") : PROXY_RENDER_TYPE_NONE;
    final String timeoutStr = req.getParameter("timeout");
    final String skipHTTPGET = req.getParameter("nocachenodice");
    
    final long   desiredTimeOutInMS = (timeoutStr != null) ? Long.parseLong(timeoutStr) : 30000;
    final boolean skipHTTPGet = (skipHTTPGET != null && skipHTTPGET.equals("1"));
    
    LOG.info("Got Request:" + path);
    
    final long requestStartTime = System.currentTimeMillis();
    
    AsyncWebServerRequest request = new AsyncWebServerRequest("proxyRequest") {

      @Override
      public boolean handleRequest(final Semaphore completionSemaphore)throws IOException {
        
        // called within async event thread context ...
        // so, we have to be careful NOT to do any cpu intensive / blocking operations here !!!
        
        LOG.info("Processing Request:" + path);
        
        String hostName = (path != null) ? URLUtils.fastGetHostStringFromURL(path): "";
        if (path == null || !path.startsWith("http:") || hostName.length() == 0) {
          LOG.info("URL From Proxy Request:" + path + " is Invalid. Sending 400 Result Code");
          responseData.setHttpErrorResponse(400,"URL From Proxy Request:" + path + " is Invalid");
          return false;
        }
        else { 

          LOG.info("Scheduling Cache Lookup for URL:" + path);
          checkCacheForURL(path,responseData,completionSemaphore,desiredTimeOutInMS,skipHTTPGet);
          return true;
        }
      } 
      
    };
   
    // ok this call will block ... 
    request.dispatch(ProxyServer.getSingleton().getEventLoop());

    // upon return we need to check the response object ... 
    if (responseData.getResponseType() == AsyncResponse.ResponseType.CacheItemResponse) { 
      // send cache item response ... 
      sendCacheItemResponse(req,response,responseData.getCacheItem(),false,format,responseData,requestStartTime);
    }
    else if (responseData.getResponseType() == AsyncResponse.ResponseType.CrawlURLResponse) { 
      sendCrawlURLResponse(req,response,responseData.getCrawlURL(),format,responseData,requestStartTime);
    }
    else if (responseData.getResponseType() == AsyncResponse.ResponseType.S3Response) { 
      sendS3ItemResponse(req,response,responseData.getArcFileItem(),format,responseData,requestStartTime);
    }
    else { 
      response.sendError(responseData.getHttpErrorCode(),responseData.getHttpErrorDesc());
      ProxyServer.getSingleton().logProxyFailure(responseData.getHttpErrorCode(), responseData.getHttpErrorDesc(),path,"",responseData.getStartTime());
    }
    
    request = null;
  }
  */

  @Override
  public void doGet(final HttpServletRequest req, final HttpServletResponse response)throws ServletException, IOException {
    
    // allocate a response data object ... which will be used by async thread to pass data to calling thread...
    final AsyncResponse responseData = new AsyncResponse();

    String queryString = req.getQueryString();
    final String originalPath   = req.getParameter("url");
    final String format = (req.getParameter("renderAs") != null) ? req.getParameter("renderAs") : PROXY_RENDER_TYPE_NONE;
    final String timeoutStr = req.getParameter("timeout");
    final String skipHTTPGET = req.getParameter("nocachenodice");
    
    final long   desiredTimeOutInMS = (timeoutStr != null) ? Long.parseLong(timeoutStr) : 30000;
    final boolean skipHTTPGet = (skipHTTPGET != null && skipHTTPGET.equals("1"));
    final Semaphore semaphore = new Semaphore(0);
    
    //LOG.info("Got Request:" + originalPath);
    
    final long requestStartTime = System.currentTimeMillis();
    
    //LOG.info("Processing Request:" + originalPath);
    
    String hostName = (originalPath != null) ? URLUtils.fastGetHostFromURL(originalPath): "";
    String fullPath = null;
    if (originalPath == null || !originalPath.startsWith("http:") || hostName.length() == 0 || queryString == null) {
      LOG.info("URL From Proxy Request:" + originalPath + " is Invalid. Sending 400 Result Code");
      responseData.setHttpErrorResponse(400,"URL From Proxy Request:" + originalPath + " is Invalid");
    }
    else { 
      
      // build url path from query string 
      int pathIndex = queryString.indexOf("url=");
      // grab the whole path ... 
      fullPath = queryString.substring(pathIndex + "url=".length());
      // unescape it 
      fullPath = URLDecoder.decode(fullPath,"UTF-8");
      
      //LOG.info("Doing Cache Lookup for URL:" + fullPath);
      boolean isAsyncOperation = checkCacheForURLV2(fullPath,responseData,semaphore,desiredTimeOutInMS,skipHTTPGet);
      if (isAsyncOperation) { 
        //LOG.info("Waiting on Async Completion for URL:" + fullPath);
        semaphore.acquireUninterruptibly();
        //LOG.info("Done Waiting for Async Completion for URL:" + fullPath);
      }
    }

    // upon return we need to check the response object ... 
    if (responseData.getResponseType() == AsyncResponse.ResponseType.CacheItemResponse) { 
      // send cache item response ... 
      sendCacheItemResponse(req,response,responseData.getCacheItem(),false,format,responseData,requestStartTime);
    }
    else if (responseData.getResponseType() == AsyncResponse.ResponseType.CrawlURLResponse) { 
      sendCrawlURLResponse(req,response,responseData.getCrawlURL(),format,responseData,requestStartTime);
    }
    else if (responseData.getResponseType() == AsyncResponse.ResponseType.S3Response) { 
      sendS3ItemResponse(req,response,responseData.getArcFileItem(),format,responseData,requestStartTime);
    }
    else { 
      response.sendError(responseData.getHttpErrorCode(),responseData.getHttpErrorDesc());
      ProxyServer.getSingleton().logProxyFailure(responseData.getHttpErrorCode(), responseData.getHttpErrorDesc(),fullPath,"",responseData.getStartTime());
    }
  }

};