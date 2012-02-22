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
import org.commoncrawl.util.shared.CCStringUtils;
import org.mortbay.util.MultiMap;
import org.mortbay.util.StringUtil;
import org.mortbay.util.TypeUtil;

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
public class MultiPartFilter implements Filter
{
 private final static String FILES ="org.mortbay.servlet.MultiPartFilter.files";
 private File tempdir;
 private boolean _deleteFiles;
 private ServletContext _context;

 public static final Log LOG = LogFactory.getLog(MultiPartFilter.class);
 
 public static class UploadFileData {
	 public String 		fieldName;
	 public File 			incomingFile;
	 public String 		incomingFilename;
	 public String 		incomingContentType;
 }

 /* ------------------------------------------------------------------------------- */
 /**
  * @see javax.servlet.Filter#init(javax.servlet.FilterConfig)
  */
 public void init(FilterConfig filterConfig) throws ServletException
 {
	 	
     tempdir=(File)filterConfig.getServletContext().getAttribute("javax.servlet.context.tempdir");
     LOG.info("tempdir:" + tempdir);
     _deleteFiles=true;
     LOG.info("deleteFiles:" + _deleteFiles);
     _context=filterConfig.getServletContext();
 }

 /* ------------------------------------------------------------------------------- */
 /**
  * @see javax.servlet.Filter#doFilter(javax.servlet.ServletRequest,
  *      javax.servlet.ServletResponse, javax.servlet.FilterChain)
  */
 public void doFilter(ServletRequest request,ServletResponse response,FilterChain chain) 
     throws IOException, ServletException
 {
	 	LOG.info("Hit doFilter");
     HttpServletRequest srequest=(HttpServletRequest)request;
     if(srequest.getContentType()==null||!srequest.getContentType().startsWith("multipart/form-data"))
     {	
    	 LOG.info(srequest.toString());
    	 LOG.info("Rejecting. Invalid mime type:" + srequest.getContentType());
         chain.doFilter(request,response);
         return;
     }
     
     LOG.info("OK Looks Good So Far");
     
     BufferedInputStream in = new BufferedInputStream(request.getInputStream());
     String content_type=srequest.getContentType();
     
     // TODO - handle encodings
     
     String boundary="--"+value(content_type.substring(content_type.indexOf("boundary=")));
     byte[] byteBoundary=(boundary+"--").getBytes(StringUtil.__ISO_8859_1);
     MultiMap params = new MultiMap();
     
     try
     {
         // Get first boundary
         byte[] bytes=TypeUtil.readLine(in);
         String line=bytes==null?null:new String(bytes,"UTF-8");
         if(line==null || !line.equals(boundary))
         {
             throw new IOException("Missing initial multi part boundary");
         }
         
         // Read each part
         boolean lastPart=false;
         String content_disposition=null;
         String file_content_type = null;
         while(!lastPart)
         {
             while(true)
             {
                 bytes=TypeUtil.readLine(in);
                 // If blank line, end of part headers
                 if(bytes==null || bytes.length==0)
                     break;
                 line=new String(bytes,"UTF-8");
                 
                 // place part header key and value in map
                 int c=line.indexOf(':',0);
                 if(c>0)
                 {
                     String key=line.substring(0,c).trim().toLowerCase();
                     String value=line.substring(c+1,line.length()).trim();
                     if(key.equals("content-disposition"))
                         content_disposition=value;
                     else if (key.equals("content-type"))
                    	 file_content_type=value;
                 }
             }
             // Extract content-disposition
             boolean form_data=false;
             if(content_disposition==null)
             {
                 throw new IOException("Missing content-disposition");
             }
             
             StringTokenizer tok=new StringTokenizer(content_disposition,";");
             String name=null;
             String filename=null;
             while(tok.hasMoreTokens())
             {
                 String t=tok.nextToken().trim();
                 String tl=t.toLowerCase();
                 if(t.startsWith("form-data"))
                     form_data=true;
                 else if(tl.startsWith("name="))
                     name=value(t);
                 else if(tl.startsWith("filename="))
                     filename=value(t);
             }
             
             // Check disposition
             if(!form_data)
             {
                 continue;
             }
             if(name==null||name.length()==0)
             {
                 continue;
             }
             
             OutputStream out=null;
             File file=null;
             try
             {
                 if (filename!=null && filename.length()>0)
                 {
                     file = File.createTempFile("MultiPart", "", tempdir);
                     LOG.info("Got FileName:" + filename + " Creating Temp File:" + file);
                     out = new FileOutputStream(file);
                     
                     UploadFileData uploadData = new UploadFileData();
                     
                     uploadData.fieldName = name;
                     uploadData.incomingFile = file;
                     uploadData.incomingFilename = filename;
                     uploadData.incomingContentType = file_content_type;
                     
                     request.setAttribute(name,uploadData);
                     
                     params.put(name, filename);
                     
                     if (_deleteFiles)
                     {
                         //file.deleteOnExit();
                         ArrayList<UploadFileData> files = (ArrayList<UploadFileData>)request.getAttribute(FILES);
                         if (files==null)
                         {
                             files=new ArrayList<UploadFileData>();
                             request.setAttribute(FILES,files);
                         }
                         files.add(uploadData);
                     }
                     
                 }
                 else
                     out=new ByteArrayOutputStream();
                 
                 int state=-2;
                 int c;
                 boolean cr=false;
                 boolean lf=false;
                 
                 // loop for all lines`
                 while(true)
                 {
                     int b=0;
                     while((c=(state!=-2)?state:in.read())!=-1)
                     {
                         state=-2;
                         // look for CR and/or LF
                         if(c==13||c==10)
                         {
                             if(c==13)
                                 state=in.read();
                             break;
                         }
                         // look for boundary
                         if(b>=0&&b<byteBoundary.length&&c==byteBoundary[b])
                             b++;
                         else
                         {
                             // this is not a boundary
                             if(cr)
                                 out.write(13);
                             if(lf)
                                 out.write(10);
                             cr=lf=false;
                             if(b>0)
                                 out.write(byteBoundary,0,b);
                             b=-1;
                             out.write(c);
                         }
                     }
                     // check partial boundary
                     if((b>0&&b<byteBoundary.length-2)||(b==byteBoundary.length-1))
                     {
                         if(cr)
                             out.write(13);
                         if(lf)
                             out.write(10);
                         cr=lf=false;
                         out.write(byteBoundary,0,b);
                         b=-1;
                     }
                     // boundary match
                     if(b>0||c==-1)
                     {
                         if(b==byteBoundary.length)
                             lastPart=true;
                         if(state==10)
                             state=-2;
                         break;
                     }
                     // handle CR LF
                     if(cr)
                         out.write(13);
                     if(lf)
                         out.write(10);
                     cr=(c==13);
                     lf=(c==10||state==10);
                     if(state==10)
                         state=-2;
                 }
             }
             finally
             {
            	 if (out != null) { 
            		 	out.close();
            	 }
             }
             
             if (file==null)
             {
                 bytes = ((ByteArrayOutputStream)out).toByteArray();
                 params.add(name,bytes);
             }
         }
     
         // handle request
         chain.doFilter(new Wrapper(srequest,params),response);
     }
     catch (IOException e) { 
    	 LOG.error("###Exception In Multipart:" + CCStringUtils.stringifyException(e));
    	 throw e;
     }
     finally
     {
    	 LOG.info("Deleting Files Here");
         deleteFiles(request);
     }
 }

 private void deleteFiles(ServletRequest request)
 {
     ArrayList files = (ArrayList)request.getAttribute(FILES);
     if (files!=null)
     {
         Iterator iter = files.iterator();
         while (iter.hasNext())
         {
             UploadFileData file=(UploadFileData)iter.next();
             try
             {
                 // file.incomingFile.delete();
             }
             catch(Exception e)
             {
                 _context.log("failed to delete "+file.incomingFile,e);
             }
         }
     }
 }
 /* ------------------------------------------------------------ */
 private String value(String nameEqualsValue)
 {
     String value=nameEqualsValue.substring(nameEqualsValue.indexOf('=')+1).trim();
     int i=value.indexOf(';');
     if(i>0)
         value=value.substring(0,i);
     if(value.startsWith("\""))
     {
         value=value.substring(1,value.indexOf('"',1));
     }
     else
     {
         i=value.indexOf(' ');
         if(i>0)
             value=value.substring(0,i);
     }
     return value;
 }

 /* ------------------------------------------------------------------------------- */
 /**
  * @see javax.servlet.Filter#destroy()
  */
 public void destroy()
 {
 }
 
 private static class Wrapper extends HttpServletRequestWrapper
 {
     String encoding="UTF-8";
     MultiMap map;
     
     /* ------------------------------------------------------------------------------- */
     /** Constructor.
      * @param request
      */
     public Wrapper(HttpServletRequest request, MultiMap map)
     {
         super(request);
         this.map=map;
     }
     
     /* ------------------------------------------------------------------------------- */
     /**
      * @see javax.servlet.ServletRequest#getContentLength()
      */
     public int getContentLength()
     {
         return 0;
     }
     
     /* ------------------------------------------------------------------------------- */
     /**
      * @see javax.servlet.ServletRequest#getParameter(java.lang.String)
      */
     public String getParameter(String name)
     {
         Object o=map.get(name);
         if (o instanceof byte[])
         {
             try
             {
                 String s=new String((byte[])o,encoding);
                 return s;
             }
             catch(Exception e)
             {
                 e.printStackTrace();
             }
         }
         else if (o instanceof String)
             return (String)o;
         return null;
     }
     
     /* ------------------------------------------------------------------------------- */
     /**
      * @see javax.servlet.ServletRequest#getParameterMap()
      */
     public Map getParameterMap()
     {
         return map;
     }
     
     /* ------------------------------------------------------------------------------- */
     /**
      * @see javax.servlet.ServletRequest#getParameterNames()
      */
     public Enumeration getParameterNames()
     {
         return Collections.enumeration(map.keySet());
     }
     
     /* ------------------------------------------------------------------------------- */
     /**
      * @see javax.servlet.ServletRequest#getParameterValues(java.lang.String)
      */
     public String[] getParameterValues(String name)
     {
         List l=map.getValues(name);
         if (l==null || l.size()==0)
             return new String[0];
         String[] v = new String[l.size()];
         for (int i=0;i<l.size();i++)
         {
             Object o=l.get(i);
             if (o instanceof byte[])
             {
                 try
                 {
                     v[i]=new String((byte[])o,encoding);
                 }
                 catch(Exception e)
                 {
                     e.printStackTrace();
                 }
             }
             else if (o instanceof String)
                 v[i]=(String)o;
         }
         return v;
     }
     
     /* ------------------------------------------------------------------------------- */
     /**
      * @see javax.servlet.ServletRequest#setCharacterEncoding(java.lang.String)
      */
     public void setCharacterEncoding(String enc) 
         throws UnsupportedEncodingException
     {
         encoding=enc;
     }
 }
}
