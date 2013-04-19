package org.commoncrawl.service.crawlmaster;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.Semaphore;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.commoncrawl.server.AsyncWebServerRequest;

public class Servlets {
  
	@SuppressWarnings("serial")
  public static class ModifyMasterCrawlState extends HttpServlet { 
    
    @Override
    protected void doPost(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {      
      AsyncWebServerRequest request = new AsyncWebServerRequest("modifystate") {

        @Override
        public boolean handleRequest(final Semaphore completionSemaphore)throws IOException {
          CrawlDBServer.getSingleton().explicitlySetMasterCrawlState(Integer.parseInt(req.getParameter("state")));
          return false;
        }
        
      };
      // ok this call will block ... 
      request.dispatch(CrawlDBServer.getSingleton().getEventLoop());      
      
    }
  }
  
	@SuppressWarnings("serial")
  public static class ModifyCrawlNumber extends HttpServlet { 
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)throws ServletException, IOException {
      final int desiredCrawlNumber = Integer.parseInt(req.getParameter("crawlnumber"));
      final String target = req.getParameter("target");

      AsyncWebServerRequest request = new AsyncWebServerRequest("modifycrawl_number") {

        @Override
        public boolean handleRequest(final Semaphore completionSemaphore)throws IOException {
          if (target.equals("history")) { 
            CrawlDBServer.getSingleton().setHistoryServerCrawlNumber(desiredCrawlNumber);
          }
          else if (target.equals("crawler")) { 
            CrawlDBServer.getSingleton().setCrawlerCrawlNumber(desiredCrawlNumber);
          }
          // called within async event thread context ...
          return false;
        }
      };
      // ok this call will block ... 
      request.dispatch(CrawlDBServer.getSingleton().getEventLoop());      
    }
  }
  
  @SuppressWarnings("serial")
  public static class SetHistoryServerTransitionState extends HttpServlet { 
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)throws ServletException, IOException {

      AsyncWebServerRequest request = new AsyncWebServerRequest("set_transition") {

        @Override
        public boolean handleRequest(final Semaphore completionSemaphore)throws IOException {
          CrawlDBServer.getSingleton().setHistoryServerTransitionState();

          // called within async event thread context ...
          return false;
        }
      };
      // ok this call will block ... 
      request.dispatch(CrawlDBServer.getSingleton().getEventLoop());      
    }
  }
  
  @SuppressWarnings("serial")
  public static class GetCrawlerNamesServlet extends HttpServlet { 
    
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)throws ServletException, IOException {
      PrintWriter writer = resp.getWriter();
      
      for (String crawlerName : CrawlDBServer.getSingleton().getCrawlerNames()) { 
        writer.println(crawlerName);
      }
    }
  }
  
}
