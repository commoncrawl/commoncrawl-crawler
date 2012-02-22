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


//========================================================================
//Copyright 1996-2005 Mort Bay Consulting Pty. Ltd.
//------------------------------------------------------------------------
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at 
//http://www.apache.org/licenses/LICENSE-2.0
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.
//========================================================================


import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DataOutputBuffer;
import org.commoncrawl.util.shared.CCStringUtils;
import org.mortbay.util.MultiMap;
import org.mortbay.util.StringUtil;
import org.mortbay.util.TypeUtil;

import com.oreilly.servlet.multipart.FilePart;
import com.oreilly.servlet.multipart.MultipartParser;
import com.oreilly.servlet.multipart.ParamPart;
import com.oreilly.servlet.multipart.Part;

/* ------------------------------------------------------------ */
/**
* Multipart Form Data Filter.
* <p>
* This class decodes the multipart/form-data stream sent by a HTML form that uses a file input
* item.  Any files sent are stored to a tempary file and a File object added to the request 
* as an attribute.  All other values are made available via the normal getParameter API and
* the setCharacterEncoding mechanism is respected when converting bytes to Strings.
* 
* If the init paramter "delete" is set to "true", any files created will be deleted when the
* current request returns.
* 
* @author Greg Wilkins
* @author Jim Crossley
*/
public class DocUploadMultiPartFilter implements Filter
{
 public final static String UPLOAD_DATA="upload_data";
 private ServletContext _context;

 public static final Log LOG = LogFactory.getLog(DocUploadMultiPartFilter.class);
 public static final int MAX_UPLOAD_SIZE = 1 * 1024 * 1024; // 1 MByte
 
 public static class UploadData {
	 public String								url = new String();
	 public DataOutputBuffer			headers = new DataOutputBuffer();
	 public DataOutputBuffer			body = new DataOutputBuffer();
 }

 /* ------------------------------------------------------------------------------- */
 /**
  * @see javax.servlet.Filter#init(javax.servlet.FilterConfig)
  */
 public void init(FilterConfig filterConfig) throws ServletException
 {
     _context=filterConfig.getServletContext();
 }

 /* ------------------------------------------------------------------------------- */
 /**
  * @see javax.servlet.Filter#doFilter(javax.servlet.ServletRequest,
  *      javax.servlet.ServletResponse, javax.servlet.FilterChain)
  */
 @Override
 public void doFilter(ServletRequest request,ServletResponse response,FilterChain chain) 
     throws IOException, ServletException
 {
	 LOG.info("Processing Request");
	 // construct upload data datastructure
	 UploadData uploadData = new UploadData();
	 // set it in request 
	 request.setAttribute(UPLOAD_DATA, uploadData);
	 // construct multipart parser 
	 MultipartParser parser = new MultipartParser((HttpServletRequest)request, MAX_UPLOAD_SIZE);
	 
	 // iterate parts 
	 Part currentPart = null;
	 while ((currentPart = parser.readNextPart()) != null) { 
		 if (currentPart instanceof FilePart) { 
			 // if file part
			 if (currentPart.getName().equals("headers")) {
				 LOG.info("Got Headers");
				 // write to header buffer  
				 uploadData.headers.reset();
				 ((FilePart)currentPart).writeTo(uploadData.headers);
			 }
			 else if (currentPart.getName().equals("body")) {
				 LOG.info("Got Body");
				 // write to header buffer  
				 uploadData.body.reset();
				 ((FilePart)currentPart).writeTo(uploadData.body);
			 }
		 }
		 else { 
			 if (currentPart.getName().equals("url")) {
				 uploadData.url = ((ParamPart)currentPart).getStringValue("UTF-8");
				 LOG.info("Got URL:" + uploadData.url);
			 }
		 }
	 }
	 
	 LOG.info("Calling super do Filter");
   // handle request
   chain.doFilter(request,response);
 }

	@Override
	public void destroy() {
		// TODO Auto-generated method stub
		
	}
}
