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


import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Date;
import java.util.concurrent.Semaphore;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DataOutputBuffer;
import org.commoncrawl.crawl.crawler.listcrawler.DocUploadMultiPartFilter.UploadData;
import org.commoncrawl.io.shared.NIOHttpHeaders;
import org.commoncrawl.protocol.CrawlURL;
import org.commoncrawl.protocol.CrawlURLMetadata;
import org.commoncrawl.util.internal.HttpHeaderInfoExtractor;
import org.commoncrawl.util.shared.CCStringUtils;
import org.commoncrawl.util.shared.FlexBuffer;
import org.json.JSONException;
import org.json.JSONWriter;

@SuppressWarnings("serial")
/**
 * servlet used to upload documents into the crawler's cache 
 * @author rana
 *
 */
public class DocUploadServlet extends HttpServlet {
	
	private static final Log LOG = LogFactory.getLog(DocUploadServlet.class);
	
	private final static String FILES ="org.mortbay.servlet.MultiPartFilter.files";
	private static final int MAX_QUEUED_IO_REQUESTS = 20;
	static Semaphore ioRequestSemaphore = new Semaphore(MAX_QUEUED_IO_REQUESTS);
	
	
	public static class DocInCacheCheck extends HttpServlet { 
		@Override
		protected void doGet(HttpServletRequest req, HttpServletResponse resp)throws ServletException, IOException {
			String url = req.getParameter("url");
			String timestamp = req.getParameter("timestamp");
			if (url == null) { 
				resp.sendError(500,"No URL Specified");
			}
			else { 
			    boolean getTimestamp = (timestamp != null && timestamp.equals("1"));
				long timestampOut = ProxyServer.getSingleton().getCache().checkCacheForFingerprint(CacheManager.getFingerprintGivenURL(url),getTimestamp);
				// ok
				try { 
					resp.setContentType("application/json");
					
					PrintWriter writer = resp.getWriter();
					JSONWriter jsonWriter = new JSONWriter(writer);
					
	        jsonWriter.object();
	        jsonWriter.key("inCache");
	        jsonWriter.value(timestampOut != 0 ? true:false);
	        if (getTimestamp) { 
	          jsonWriter.key("latestTimestamp");
	          jsonWriter.value(timestampOut);
              jsonWriter.key("latestTimestampGMT");
              jsonWriter.value(new Date(timestampOut).toGMTString());
	        }
	          
	        jsonWriter.endObject();
	        
	        writer.flush();
				}
				catch (JSONException e) { 
					LOG.error(CCStringUtils.stringifyException(e));
					resp.sendError(500, "Internal Error");
				}
			}

		}
	}
	
	public static class DocUploadForm extends HttpServlet { 
		
		
		
		@Override
		protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
			
			resp.setContentType("text/html");
		  PrintWriter writer = resp.getWriter();
		  
		  writer.println("<HTML>");
		  
		  writer.println("<form method='post' action='/DocUploader' enctype='multipart/form-data'>");
		  writer.println("<table border=0>");
		  writer.println("<tr><td>url:<td><input name='url' type='text' width=20 /></tr>");
		  writer.println("<tr><td>Header Data:<td><input name='headers' type='file' /></tr>");
		  writer.println("<tr><td>Content:<td><input name='body' type='file' /></tr>");
		  writer.println("<tr><td colspan=2>&nbsp</tr>");
		  writer.println("<tr><td colspan=2><input type='submit' /></tr>");
		  writer.println("</table>");

		  writer.println("</HTML>");
		  writer.flush();
		}
	}

	@Override
	protected void doPut(HttpServletRequest req, HttpServletResponse resp)
	  throws ServletException, IOException {
	  String url = null;
	  try { 
  	  // ok pick out the relevant fields ... 
  	  url =req.getParameter("url");
  	  // and get header size param 
  	  String headerSizeParam = req.getParameter("headerSize");
  	  
  	  LOG.info("###DocUpload: Got Put. URL:" + url + " HeaderSize:" + headerSizeParam);
  	  
  	  // ok if valid 
  	  if (url == null || url.length() == 0 || headerSizeParam == null || headerSizeParam.length() == 0) {
  	    LOG.error("###DocUpload: Invalid URL or HeaderSize");
  	  }
  	  int headerSize = Integer.parseInt(headerSizeParam);
  	  // ok read up to header bytes data 
  	  byte headerBytes[] = new byte[headerSize];
  	  int headerRead = 0;
  	  int lastRead = -1;
  	  // get an input stream
  	  InputStream inputStream = req.getInputStream();
  	  while ((lastRead = inputStream.read(headerBytes, headerRead,headerSize - headerRead)) != -1) { 
  	    headerRead += lastRead;
  	    if (headerRead == headerSize)
  	      break;
  	  }
  	  if (lastRead == -1) { 
  	    LOG.error("###DocUpload: URL:" + url +" Waited For:" + headerSize 
  	        + " header bytes received:" + headerRead);
  	    resp.sendError(500,"Invalid Payload. Not enough Header Bytes!");
  	    return;
  	  }
  	  // ok decode header bytes 
  	  String headerString = new String(headerBytes,"UTF-8");
  	  // parse 
  	  NIOHttpHeaders headers = NIOHttpHeaders.parseHttpHeaders(headerString);
  	  
  	  // parse response code in headers ... 
        CrawlURLMetadata metadata = new CrawlURLMetadata();
        HttpHeaderInfoExtractor.parseStatusLine(headers.getValue(0), metadata);
  
        if (metadata.getHttpResultCode() != 200) { 
          LOG.error("###DocUpload: URL:" + url +" Header contains invalid " +
          		"HTTP Response Line:" + headers.getValue(0));        
            resp.sendError(500,"Invalid HTTP Result Code in Headers");
            return;
        }
        
        // ok go ahead and collect data 
        DataOutputBuffer contentBuffer = new DataOutputBuffer();
        
        // allocate a buffer ... 
        byte incomingBuffer[] = new byte[ 1 << 16 ];
        int bytesRead = -1;
        int totalBytesRead = 0;
        // get input stream 
        InputStream input = req.getInputStream();
        while ((bytesRead = input.read(incomingBuffer)) != -1) { 
          LOG.info("###DocUpload: Read:" + bytesRead + " bytes from:" + url);
          contentBuffer.write(incomingBuffer,0,bytesRead);
          totalBytesRead += bytesRead;
        }
        // ok now see if gzip is specified ... 
        String contentEncoding = headers.findValue("content-encoding");
        if (contentEncoding != null && contentEncoding.equalsIgnoreCase("gzip")) {
          // ok yes... sniff bytes in content to ensure gzip stream ... 
          // sniff stream to make sure gzip encoding is intact 
          if (contentBuffer.getLength() < 4) { 
              resp.sendError(500, "Invalid or Corrupt Body");
              return;
          }
      
          int head = ((int)contentBuffer.getData()[0] & 0xff) | ((contentBuffer.getData()[1] << 8) & 0xff00);       
      
          if (head != java.util.zip.GZIPInputStream.GZIP_MAGIC) {
            // ok , stream is not gzip encoded ... remove content-encoding 
            headers.remove("content-encoding");
          }
        }
        // ok now remove content length field ..
        headers.remove("content-length");
        // and add it back based on PUT value 
        headers.add("Content-Length", Integer.toString(contentBuffer.getLength()));
        
        // add cc headers 
        headers.add("x-ccproxy-import-source", "proxy");
        headers.add("x-ccproxy-import-source-ip", ((HttpServletRequest)req).getRemoteAddr());
        headers.add("x-ccproxy-import-source-timestamp",Long.toString(System.currentTimeMillis()));
        // ok construct a crawl url object 
        CrawlURL crawlURL = new CrawlURL();
        
        crawlURL.setUrl(url);
        crawlURL.setLastAttemptResult((byte) CrawlURL.CrawlResult.SUCCESS);
        crawlURL.setResultCode(200);
        
        // set modified headers 
        crawlURL.setHeaders(headers.toString());
        // set content ... 
        crawlURL.setContentRaw(new FlexBuffer(contentBuffer.getData(),0,contentBuffer.getLength()));        
        
        // ok now we need to calculate number of io operations required to complete this request ... 
        int numberOfIOOperationsRequiredForCompletion = ProxyServlet.calculateCachedItemCountGivenCrawlURL(crawlURL);
        // ok acquire the appropriate number of semaphore
        LOG.info("###DocUpload: *** Acqurining " + numberOfIOOperationsRequiredForCompletion
                + " permits for url:" + url);
        ioRequestSemaphore.acquireUninterruptibly(numberOfIOOperationsRequiredForCompletion);
        LOG.info("###DocUpload: *** Acquired " + numberOfIOOperationsRequiredForCompletion  
                + " permits for url:" + url);
        LOG.info("###DocUpload: Injecting URL:" + url +" into ProxyServer:" + crawlURL.getUrl());
        ProxyServer.getSingleton().injectCrawlURL(crawlURL,ioRequestSemaphore);
        
        resp.getWriter().println("###DocUpload: for URL:" + url + " DONE!");        
      }
	  catch (IOException e) { 
        LOG.info("###DocUpload: Upload of URL:" + url + " threw Exception:" + 
            CCStringUtils.stringifyException(e));
        resp.sendError(500,"Upload Threw Exception:" + CCStringUtils.stringifyException(e));
        return;
	  }
	  
	}
	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

		LOG.info("Received Doc Upload Request");
		
		UploadData uploadData = (UploadData)req.getAttribute(DocUploadMultiPartFilter.UPLOAD_DATA);

		if (uploadData == null ) { 
			resp.sendError(500,"No UploadData. Invalid Request ");
		}
		else { 
			
			
			if (uploadData.url == null || uploadData.url.length() == 0) { 
				resp.sendError(500,"No url found");
			}

			if (uploadData.headers == null || uploadData.headers.getLength() == 0) { 
				resp.sendError(500,"No header data found");
			}
			if (uploadData.body == null || uploadData.body.getLength() == 0) { 
				resp.sendError(500,"No body data found");
			}
			
			LOG.info("Doc Upload Request URL:" + uploadData.url + " HeaderLength:" + uploadData.headers.getLength() + " Body Length:" + uploadData.body.getLength());
			
			// ok construct a crawl url object 
			CrawlURL crawlURL = new CrawlURL();
			
			crawlURL.setUrl(uploadData.url);
			crawlURL.setLastAttemptResult((byte) CrawlURL.CrawlResult.SUCCESS);
			crawlURL.setResultCode(200);
			
			// parse headers ... 
			NIOHttpHeaders headers = NIOHttpHeaders.parseHttpHeaders(new String(uploadData.headers.getData(),0,uploadData.headers.getLength(),"UTF-8"));
			// ok now, check to see if a status line is present
			if (headers.getKey(0) != null) {
				headers.prepend(null, "HTTP/1.0 200 OK");
			}
	    // parse response code in headers ... 
	    CrawlURLMetadata metadata = new CrawlURLMetadata();
	    HttpHeaderInfoExtractor.parseStatusLine(headers.getValue(0), metadata);

	    if (metadata.getHttpResultCode() != 200) { 
	    	resp.sendError(500,"Invalid HTTP Result Code in Headers");
	    	return;
	    }
	    
	    String contentEncoding = headers.findValue("content-encoding");
	    if (contentEncoding != null && contentEncoding.equalsIgnoreCase("gzip")) { 
	    	// sniff stream to make sure gzip encoding is intact 
	    	if (uploadData.body.getLength() < 4) { 
	    		resp.sendError(500, "Invalid or Corrupt Body");
	    		return;
	    	}
        
	    	int head = ((int)uploadData.body.getData()[0] & 0xff) | ((uploadData.body.getData()[1] << 8) & 0xff00);       
        
        if (head != java.util.zip.GZIPInputStream.GZIP_MAGIC) { 
        	// ok remove content-encoding 
        	headers.remove("content-encoding");
        }
	    }
	    // add cc headers 
	    headers.add("x-ccproxy-import-source", "proxy");
	    headers.add("x-ccproxy-import-source-ip", ((HttpServletRequest)req).getRemoteAddr());
	    headers.add("x-ccproxy-import-source-timestamp",Long.toString(System.currentTimeMillis()));
	    headers.remove("x-ccproxy-source");
	    headers.remove("x-ccproxy-timer");
	    headers.remove("x-ccproxy-final-url");
	    headers.remove("x-ccproxy-orig-content-len");
	    // set modified headers 
			crawlURL.setHeaders(headers.toString());			
			crawlURL.setContentRaw(new FlexBuffer(uploadData.body.getData(),0,uploadData.body.getLength()));
			
			// ok now we need to calculate number of io operations required to complete this request ... 
			int numberOfIOOperationsRequiredForCompletion = ProxyServlet.calculateCachedItemCountGivenCrawlURL(crawlURL);
			// ok acquire the appropriate number of semaphore
			LOG.info("*** Acqurining " + numberOfIOOperationsRequiredForCompletion
					+ " permits for url:" + uploadData.url);
			ioRequestSemaphore.acquireUninterruptibly(numberOfIOOperationsRequiredForCompletion);
			LOG.info("*** Acquired " + numberOfIOOperationsRequiredForCompletion  
					+ " permits for url:" + uploadData.url);
			LOG.info("Injecting URL into ProxyServer:" + crawlURL.getUrl());
			ProxyServer.getSingleton().injectCrawlURL(crawlURL,ioRequestSemaphore);
			
			resp.getWriter().println("DONE!");
		}
	}
	
}

