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

package org.commoncrawl.crawl.crawler;

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

import com.google.common.collect.ImmutableMap;

/**
 * An http servlet that serves up request logs on demand
 * 
 * @author rana
 *
 */
public class RequestLogServlet extends HttpServlet {

  public static final String servletPath = "/requestLog";
  private static final Log LOG = LogFactory.getLog(RequestLogServlet.class);
  
  
  private static final int DEFAULT_MAX_LINES = 25;
  private static final int MAX_MAX_LINES = 1000;
  
  private static ImmutableMap<String, String> logNameToFileName = new ImmutableMap.Builder()
  .put("successLog", "crawlerSuccess.log")
  .put("failureLog", "crawlerFailures.log")
  .put("dnsLog", "crawlerDNS.log")
  .put("dnsFailuresLog", "crawlerDNSFailures.log")
  .put("robotsLog", "robotsFetchLog.log")
  .put("cookieLog", "cookieLog.log")
  .build();
  
  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    
    int maxLines = DEFAULT_MAX_LINES;
    
    try { 
      String strMaxLines = req.getParameter("maxLines");
      if (strMaxLines != null) { 
        maxLines = Math.min(Integer.parseInt(req.getParameter("maxLines")),MAX_MAX_LINES);
      }
    }
    catch(NumberFormatException e) { 
      
    }
    
    String logName = req.getParameter("logName");
    String fileName = (logName != null) ? logNameToFileName.get(logName) : null;
    if (fileName != null) { 
      File requestLogFile = new File(CrawlerServer.getServer().getLogDirectory(),fileName);
      
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
