/**
 * Copyright 2008 - CommonCrawl Foundation
 * 
 * CommonCrawl licenses this file to you under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.commoncrawl.crawl.crawler.listcrawler;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.record.Buffer;
import org.commoncrawl.crawl.proxy.DiskCacheItem;
import org.commoncrawl.io.internal.NIOBufferList;
import org.commoncrawl.io.internal.NIOBufferListInputStream;
import org.commoncrawl.io.internal.NIOHttpConnection;
import org.commoncrawl.io.internal.NIOHttpConnection.DataSource;
import org.commoncrawl.io.internal.NIOHttpConnection.State;
import org.commoncrawl.io.shared.NIOHttpHeaders;
import org.commoncrawl.protocol.CrawlURLMetadata;
import org.commoncrawl.protocol.shared.ArcFileHeaderItem;
import org.commoncrawl.rpc.base.shared.BinaryProtocol;
import org.commoncrawl.util.internal.HttpCacheUtils;
import org.commoncrawl.util.internal.HttpCookieUtils;
import org.commoncrawl.util.internal.HttpHeaderInfoExtractor;
import org.commoncrawl.util.internal.URLFingerprint;
import org.commoncrawl.util.internal.URLUtils;
import org.commoncrawl.util.internal.HttpCacheUtils.LifeTimeInfo;
import org.commoncrawl.util.internal.HttpCookieUtils.CanonicalCookie;
import org.commoncrawl.util.shared.CCStringUtils;
import org.mortbay.util.IO;


/** 
 * An experimental new version of the crawler cache serving servlet 
 * @author rana
 *
 */
public class ProxyServlet2 extends HttpServlet
{
  
  private static final Log LOG = LogFactory.getLog(ProxyServlet.class);  
  private int _tunnelTimeoutMs=3000;
  private ExecutorService _threadPool = Executors.newFixedThreadPool(100);
  private HttpCookieUtils.CookieStore _cookieStore = new HttpCookieUtils.CookieStore();
  
 
 protected HashSet _DontProxyHeaders = new HashSet();
 {
     _DontProxyHeaders.add("proxy-connection");
     _DontProxyHeaders.add("connection");
     _DontProxyHeaders.add("keep-alive");
     _DontProxyHeaders.add("transfer-encoding");
     _DontProxyHeaders.add("te");
     _DontProxyHeaders.add("trailer");
     _DontProxyHeaders.add("proxy-authorization");
     _DontProxyHeaders.add("proxy-authenticate");
     _DontProxyHeaders.add("upgrade");
     
     _DontProxyHeaders.add("cache-control");
     _DontProxyHeaders.add("pragma");
     _DontProxyHeaders.add("last-modified");
     _DontProxyHeaders.add("date");
     _DontProxyHeaders.add("age");
     _DontProxyHeaders.add("etag");
     _DontProxyHeaders.add("expires");
     _DontProxyHeaders.add("user-agent");
     
 }
 
 private ServletConfig config;
 private ServletContext context;
 
 /* (non-Javadoc)
  * @see javax.servlet.Servlet#init(javax.servlet.ServletConfig)
  */
 public void init(ServletConfig config) throws ServletException
 {
     this.config=config;
     this.context=config.getServletContext();
 }

 /* (non-Javadoc)
  * @see javax.servlet.Servlet#getServletConfig()
  */
 public ServletConfig getServletConfig()
 {
     return config;
 }


 
 private static File cachePathFromURL(URL theURL) throws MalformedURLException { 
   String canonicalURL = URLUtils.canonicalizeURL(theURL.toString(),true);
   long   fingerprint = URLFingerprint.generate64BitURLFPrint(canonicalURL);
   File cachePath = new File(ProxyServer.getSingleton().getDataDirectory(),"diskCache");
   cachePath.mkdir();
   File filePath  = new File(cachePath,Long.toString(fingerprint));
   return filePath;
 }
 
 public static class CacheLoadRequest { 
   URL _theURL;
   
   public CacheLoadRequest(URL theURL) { 
     _theURL = theURL;
   }
   
   public DiskCacheItem executeRequest() { 
      
     try { 
       // ok ... first construct file path to url ...
       File cacheFilePath = cachePathFromURL(_theURL);
       // now check to see if file exists ... 
       if (cacheFilePath.exists() && cacheFilePath.isFile()) { 
         // ok, we are running in the servlet thread context here ... so 
         // it is ok to block on io requests directly ... 
         FileInputStream inputStream = new FileInputStream(cacheFilePath);
         
         try { 
           // load cache item from stream ... 
           DataInputStream dataInput = new DataInputStream(inputStream);
           // load it 
           DiskCacheItem item = new DiskCacheItem();
           
           item.deserialize(dataInput,new BinaryProtocol());
           
           return item;
         }
         finally { 
           if (inputStream != null) { 
             inputStream.close();
           }
         }
       }
     }
     catch (IOException e) { 
       LOG.error(CCStringUtils.stringifyException(e));
     }
     return null;
   }
   
   
 }
 public static class NIOConnectionWrapper implements NIOHttpConnection.Listener, DataSource {

   private Semaphore _blockingSemaphore = new Semaphore(0);

   NIOHttpConnection _connection;
   byte[]            _uploadBuffer;
   boolean           _connectionFailed = false;
   
   public NIOConnectionWrapper(NIOHttpConnection connection) { 
     _connection = connection;
     _connection.setListener(this);
   }
   
   public void setUploadBuffer(byte[] buffer) { 
     _uploadBuffer = buffer;
   }
   
  @Override
  public void HttpConnectionStateChanged(NIOHttpConnection theConnection,State oldState, State state) {
    if (state == State.DONE || state == State.ERROR) {
      
      _connectionFailed = (state == State.ERROR);
      _blockingSemaphore.release();
      _connection.setListener(null);
    }
  }

  @Override
  public void HttpContentAvailable(NIOHttpConnection theConnection,NIOBufferList contentBuffer) {
    // NOOP
  }

  @Override
  public boolean read(NIOBufferList dataBuffer) throws IOException {
    if (_uploadBuffer != null) { 
      dataBuffer.write(_uploadBuffer, 0, _uploadBuffer.length);
      _uploadBuffer = null;
    }
    return true;
  } 
   
  public boolean waitForCompletion() { 
    _blockingSemaphore.acquireUninterruptibly();
    return !_connectionFailed;
  }
 }

 /** build a NIOHttpHeader object from the cahce file header item array 
  * 
  */
 private static NIOHttpHeaders buildHeaderFromHeaderItems(ArrayList<ArcFileHeaderItem> items) { 
   
   NIOHttpHeaders headers = new NIOHttpHeaders();
   
   for (ArcFileHeaderItem item : items){ 
     headers.add(item.getItemKey(), item.getItemValue());
   }
   
   return headers;
 }
 
 private static class RequestDetails { 
   
   public URL url;
   ArrayList<String> log = new ArrayList<String>();
   
   @Override
  public String toString() {
    StringBuffer outputBuffer = new StringBuffer();
    
    outputBuffer.append("URL:" + url.toString() + "\n");
    for (String logline : log) { 
      outputBuffer.append("--" + logline + "\n");
    }
    return outputBuffer.toString();
  }
 }
 
 public void serviceProxyInternalRequest(ServletRequest request,ServletResponse response) throws IOException { 
   String uri=((HttpServletRequest)request).getRequestURI();
   
   if (uri.equalsIgnoreCase("/dumpCookies")) { 
     Vector<CanonicalCookie> cookies = new Vector<CanonicalCookie>();
     
     // get a copy of all the cookie objects ... 
     _cookieStore.GetAllCookies(cookies);
     
     PrintWriter writer = response.getWriter();
     
     HttpServletResponse resp = (HttpServletResponse)response;
     
     resp.setStatus(200);
     resp.setContentType("text/html");
     
     writer.println("<pre>");
     for (CanonicalCookie cookie : cookies) { 
       writer.println(cookie.toString());
     }
     writer.println("</pre>");
   }
 }
 
 /* (non-Javadoc)
  * @see javax.servlet.Servlet#service(javax.servlet.ServletRequest, javax.servlet.ServletResponse)
  */
 public void service(ServletRequest req, ServletResponse res) throws ServletException,
         IOException
 {
     
     HttpServletRequest request = (HttpServletRequest)req;
     HttpServletResponse response = (HttpServletResponse)res;
     if ("CONNECT".equalsIgnoreCase(request.getMethod()))
     {
         handleConnect(request,response);
     }
     else
     {
         final RequestDetails details = new RequestDetails();
         
         String uri=request.getRequestURI();
         
         
         if (request.getQueryString()!=null)
             uri+="?"+request.getQueryString();
         final URL url = new URL(request.getScheme(),
                       request.getServerName(),
                       request.getServerPort(),
                       uri);

         
         if (request.getServerName().equals("proxy")) { 
           serviceProxyInternalRequest(req,res);
           return;
         }
         
         
         
         // context.log("URL="+url);
         details.url = url;
         
         // attempt cache load first ... 
         CacheLoadRequest cacheLoad = new CacheLoadRequest(url);
         details.log.add("Executing Disk Load Request");
         DiskCacheItem cacheItem = cacheLoad.executeRequest();
         details.log.add("Disk Load Request Returned:" + cacheItem);

         // create metadata placeholder
         CrawlURLMetadata metadata = new CrawlURLMetadata();
         NIOHttpHeaders headers    = null;
         
         boolean revalidate = false;
         boolean cacheItemValid = true;

         if (cacheItem != null) { 
           // get headers 
           headers = buildHeaderFromHeaderItems(cacheItem.getHeaderItems());
           // set last fetch time in metadata 
           metadata.setLastFetchTimestamp(cacheItem.getFetchTime());
           // parse headers 
           HttpHeaderInfoExtractor.parseHeaders(headers, metadata);
           // ok now validate cache 
           if (HttpCacheUtils.requiresValidation(metadata)) { 
             details.log.add("CACHE Item Present But Needs Revalidation");
             revalidate = true;
           }
         }

         // if no cache item or we to revalidate cache item .. 
         if (cacheItem == null || revalidate) { 
           
           NIOHttpConnection connection = new NIOHttpConnection(
               url,
               ProxyServer.getSingleton().getEventLoop().getSelector(),
               ProxyServer.getSingleton().getEventLoop().getResolver(),_cookieStore);
         
           NIOConnectionWrapper wrapper = new NIOConnectionWrapper(connection);
         
         

           // URLConnection connection = url.openConnection();
           // connection.setAllowUserInteraction(false);
           
           // Set method
           /*
           HttpURLConnection http = null;
           if (connection instanceof HttpURLConnection)
           {
               http = (HttpURLConnection)connection;
               http.setRequestMethod(request.getMethod());
               http.setInstanceFollowRedirects(false);
           }
            */
           connection.setMethod(request.getMethod());

           // check connection header
           String connectionHdr = request.getHeader("Connection");
           if (connectionHdr!=null)
           {
               connectionHdr=connectionHdr.toLowerCase();
               if (connectionHdr.equals("keep-alive")||
                   connectionHdr.equals("close"))
                   connectionHdr=null;
           }
           
           // copy headers
           boolean xForwardedFor=false;
           boolean hasContent=false;
           Enumeration enm = request.getHeaderNames();
           while (enm.hasMoreElements())
           {
               // TODO could be better than this!
               String hdr=(String)enm.nextElement();
               String lhdr=hdr.toLowerCase();
  
               if (_DontProxyHeaders.contains(lhdr) || lhdr.equals("cookie"))
                   continue;
               if (connectionHdr!=null && connectionHdr.indexOf(lhdr)>=0)
                   continue;
  
               if ("content-type".equals(lhdr))
                   hasContent=true;
  
               Enumeration vals = request.getHeaders(hdr);
               while (vals.hasMoreElements())
               {
                   String val = (String)vals.nextElement();
                   if (val!=null)
                   {
                       connection.getRequestHeaders().set(hdr, val);
                       // connection.addRequestProperty(hdr,val);
                       details.log.add("req header: "+hdr+": "+val);
                       xForwardedFor|="X-Forwarded-For".equalsIgnoreCase(hdr);
                   }
               }
           }
  
           String cookies = _cookieStore.GetCookies(url);
           if (cookies.length() != 0) { 
             details.log.add("req injected-header: Cookie:" +cookies);
             connection.getRequestHeaders().set("Cookie", cookies);
           }
           
           // Proxy headers
           connection.getRequestHeaders().set("Via", "1.1 (jetty)");
           // cache headers (if required) 
           if (metadata.isFieldDirty(CrawlURLMetadata.Field_LASTMODIFIEDTIME)) {
             details.log.add("Sending If-Modified-Since");
             connection.getRequestHeaders().set("If-Modified-Since",headers.findValue("Last-Modified"));
           }
           if (metadata.isFieldDirty(CrawlURLMetadata.Field_ETAG)) {
             details.log.add("Sending If-None-Match");
             connection.getRequestHeaders().set("If-None-Match", metadata.getETag());
           }
           if (!xForwardedFor)
             connection.getRequestHeaders().set("X-Forwarded-For",request.getRemoteAddr());
               //connection.addRequestProperty("X-Forwarded-For",request.getRemoteAddr());
  
           // a little bit of cache control
           String cache_control = request.getHeader("Cache-Control");
           /*
           if (cache_control!=null &&
               (cache_control.indexOf("no-cache")>=0 ||
                cache_control.indexOf("no-store")>=0))
               connection.setUseCaches(false);
           */
  
           // customize Connection
           
           try
           {    
               // connection.setDoInput(true);
               
               // do input thang!
               InputStream in=request.getInputStream();
               if (hasContent)
               {
                   //connection.setDoOutput(true);
                   ByteArrayOutputStream stream = new ByteArrayOutputStream();
                   IO.copy(in,stream);
                   wrapper.setUploadBuffer(stream.toByteArray());
               }
               
               // Connect
               connection.open();    
           }
           catch (Exception e)
           {
             details.log.add(CCStringUtils.stringifyException(e));
           }
           
           boolean connectionSucceeded = wrapper.waitForCompletion();
           
           InputStream proxy_in = null;
  
           // handler status codes etc.
           int code=500;
           
           if (connectionSucceeded) {
             
               // set last fetch time in metadata 
               metadata.setLastFetchTimestamp(System.currentTimeMillis());

               code=connection.getResponseHeaders().getHttpResponseCode();
               
               if (revalidate && code != 304) {
                 details.log.add("Item ReValidate FAILED");
                 cacheItemValid = false;
               }
               
               if (code != 304) {
                 
                 HttpHeaderInfoExtractor.parseHeaders(connection.getResponseHeaders(), metadata);
                 
                 response.setStatus(code,"");
                 details.log.add("response code:"+code);
                 
                 // clear response defaults.
                 response.setHeader("Date",null);
                 response.setHeader("Server",null);
                 
                 // set response headers
                 int h=0;
                 String hdr=connection.getResponseHeaders().getKey(h);
                 String val=connection.getResponseHeaders().getValue(h);
                 while(hdr!=null || val!=null)
                 {
                     String lhdr = hdr!=null?hdr.toLowerCase():null;
                     if (hdr!=null && val!=null && !_DontProxyHeaders.contains(lhdr))
                         response.addHeader(hdr,val);
    
                     details.log.add("response header:" +hdr+": "+val);
                     
                     h++;
                     hdr=connection.getResponseHeaders().getKey(h);
                     val=connection.getResponseHeaders().getValue(h);
                 }
                 response.addHeader("Via","1.1 (jetty)");
                 response.addHeader("cache-control","no-cache,no-store");
                 response.addHeader("Connection","close");

                 // IF RESULT IS CACHEABLE ...
                 LifeTimeInfo lifeTimeInfo = HttpCacheUtils.getFreshnessLifetimeInMilliseconds(metadata);
                 details.log.add("getFreshnessLifetime returned:" + lifeTimeInfo._lifetime);
                 details.log.add("getFreshnessLifetime source:" + lifeTimeInfo._source);
                 
                 if (lifeTimeInfo._lifetime != 0) {
                   
                   details.log.add("item is cachable - issuing cache request");
                   // construct a disk cache item ... 
                   final DiskCacheItem cacheItemForWrite = new DiskCacheItem();
                   // populate 
                   cacheItemForWrite.setFetchTime(System.currentTimeMillis());
                   cacheItemForWrite.setResponseCode(code);
                   // headers .. 
                   h=0;
                   hdr=connection.getResponseHeaders().getKey(h);
                   val=connection.getResponseHeaders().getValue(h);
                   while(hdr!=null || val!=null)
                   {
                       String lhdr = hdr!=null?hdr.toLowerCase():null;
                       if (hdr!=null && val!=null) {
                         if (!hdr.toLowerCase().equals("set-cookie")) { 
                           ArcFileHeaderItem item = new ArcFileHeaderItem();
                           item.setItemKey(hdr);
                           item.setItemValue(val);
                           cacheItemForWrite.getHeaderItems().add(item);
                         }
                       }
                       h++;
                       hdr=connection.getResponseHeaders().getKey(h);
                       val=connection.getResponseHeaders().getValue(h);
                   }
                   
                   if (connection.getContentBuffer().available() != 0) { 
                     // copy result to byte array
                     //VERY INEFFICIENT ... BUT ONLY FOR TESTING ... 
                     ByteArrayOutputStream tempStream = new ByteArrayOutputStream();
                     IO.copy(new NIOBufferListInputStream(connection.getContentBuffer()),tempStream);
                     // get the underlying buffer 
                     byte[] responseBuffer = tempStream.toByteArray();
                     // set it into the cache item ... 
                     cacheItemForWrite.setContent(new Buffer(responseBuffer));
                     // and now write out buffer 
                     IO.copy(new ByteArrayInputStream(responseBuffer),response.getOutputStream());
                   }
                   
                   // ok schedule a disk cache write ... 
                   _threadPool.execute(new Runnable() {

                    @Override
                    public void run() {
                      LOG.info("Writing Cache Item for URL:" + url);
                      File cacheFileName;
                      try {
                        cacheFileName = cachePathFromURL(url);
                        
                        try {
                          FileOutputStream fileStream = new FileOutputStream(cacheFileName);
                          try { 
                            DataOutputStream dataOutputStream = new DataOutputStream(fileStream);
                            cacheItemForWrite.serialize(dataOutputStream,new BinaryProtocol());
                          }
                          finally { 
                            fileStream.close();
                          }
                        } catch (IOException e) {
                          LOG.error(CCStringUtils.stringifyException(e));
                        }
                        
                      } catch (MalformedURLException e) {
                        LOG.error(CCStringUtils.stringifyException(e));
                      }
                      
                    } 
                     
                   });
                 }
                 else { 
                   details.log.add("FRESHNESS LIFETIME == 0 - SKIPPING CACHE!");
                   // no cache direct copy case 
                   if (connection.getContentBuffer().available() != 0) { 
                       IO.copy(new NIOBufferListInputStream(connection.getContentBuffer()),response.getOutputStream());
                   }
                 }
               }
           }
           else { 
             response.setStatus(500,"Proxy Request Failed");
             details.log.add("Proxy Request Failed");
           }
         }
         // ok now, if cache item != null and cache-item is still valid 
         if (cacheItem != null && cacheItemValid) { 
           // service request from cache
           details.log.add("Servicing Request From Disk Cache");
           
           // clear response defaults.
           response.setHeader("Date",null);
           response.setHeader("Server",null);

           // set response code 
           response.setStatus(cacheItem.getResponseCode());
           
           // set response headers
           for (ArcFileHeaderItem headerItem : cacheItem.getHeaderItems()) { 
             String key = headerItem.getItemKey().toLowerCase();
             // if not in don't proxy headers ... 
             if (key.length() != 0) {
               if (!_DontProxyHeaders.contains(key) && !key.equals("set-cookie")) { 
                 response.addHeader(headerItem.getItemKey(),headerItem.getItemValue());
                 details.log.add("cache response: "+headerItem.getItemKey()+": "+headerItem.getItemValue());
               }
               else { 
                 details.log.add("cache hidden-hdr: "+headerItem.getItemKey()+": "+headerItem.getItemValue());
               }
             }
           }
           
           response.addHeader("Via","1.1 (jetty)");
           response.addHeader("cache-control","no-cache,no-store");
           response.addHeader("Connection","close");
           
           if (cacheItem.getContent().getCount() != 0) { 
             response.setHeader("Content-Length",null);
             response.addHeader("Content-Length",Integer.toString(cacheItem.getContent().getCount()));
             IO.copy(new ByteArrayInputStream(cacheItem.getContent().getReadOnlyBytes()),response.getOutputStream());
           }
         }
         
         LOG.info(details.toString());
     }
 }


 /* ------------------------------------------------------------ */
 public void handleConnect(HttpServletRequest request,
                           HttpServletResponse response)
     throws IOException
 {
     String uri = request.getRequestURI();
     
     context.log("CONNECT: "+uri);
     
     // InetAddrPort addrPort=new InetAddrPort(uri);
     URL url = new URL(uri);
     
     InetAddress address = InetAddress.getByName(url.getHost());
     int port = (url.getPort() != -1) ? url.getPort() : 80;
     
     
     //if (isForbidden(HttpMessage.__SSL_SCHEME,addrPort.getHost(),addrPort.getPort(),false))
     //{
     //    sendForbid(request,response,uri);
     //}
     //else
     {
         InputStream in=request.getInputStream();
         OutputStream out=response.getOutputStream();
         
         Socket socket = new Socket(address,port);
         context.log("Socket: "+socket);
         
         response.setStatus(200);
         response.setHeader("Connection","close");
         response.flushBuffer();
         
         System.err.println(response);

         context.log("out<-in");
         IO.copyThread(socket.getInputStream(),out);
         context.log("in->out");
         IO.copy(in,socket.getOutputStream());
     }
 }
 
 
 
 
 /* (non-Javadoc)
  * @see javax.servlet.Servlet#getServletInfo()
  */
 public String getServletInfo()
 {
     return "Proxy Servlet";
 }

 /* (non-Javadoc)
  * @see javax.servlet.Servlet#destroy()
  */
 public void destroy()
 {

 }
}
