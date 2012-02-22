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


import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.util.internal.LogFileUtils;
/** 
 * 
 * A custom servlet used to serve up this crawler's cache request log  
 * 
 * @author rana
 *
 */
public class RequestLogServlet extends HttpServlet {

  public static final String servletPath = "/requestLog";
  private static final Log LOG = LogFactory.getLog(RequestLogServlet.class);
  
  
  private static final int DEFAULT_MAX_LINES = 25;
  
  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    
    int maxLines = DEFAULT_MAX_LINES;
    
    if (req.getParameter("maxLines") == null) {
      resp.sendRedirect(resp.encodeRedirectURL(servletPath+"?maxLines=25"));
    }
    else { 
      try { 
        maxLines = Integer.parseInt(req.getParameter("maxLines"));
      }
      catch(NumberFormatException e) { 
        
      }
      
      File requestLogFile = new File(ProxyServer.getSingleton().getLogDirectory(),ProxyServer.getRequestLogFileName());
      
      List<String> tailList = LogFileUtils.tail(requestLogFile,maxLines);
      
      resp.setContentType("text/plain");
      PrintWriter writer = resp.getWriter();
      try { 
        for (String line : tailList) { 
          writer.println(line);
        }
      }
      finally { 
        writer.flush();
        writer.close();
      }
    }
  }
  
}
