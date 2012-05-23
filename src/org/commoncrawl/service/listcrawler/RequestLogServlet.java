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
import org.commoncrawl.util.LogFileUtils;
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
