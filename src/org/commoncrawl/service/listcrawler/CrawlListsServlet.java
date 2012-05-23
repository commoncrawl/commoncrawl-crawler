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


import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.commoncrawl.service.listcrawler.CrawlListDatabaseRecord;
import org.commoncrawl.service.listcrawler.CrawlListMetadata;
import org.commoncrawl.util.RPCStructIntrospector;
import org.mortbay.log.Log;

@SuppressWarnings("serial")
/** 
 * servlet that serves up lists status 
 * 
 * @author rana
 *
 */
public class CrawlListsServlet extends HttpServlet {

	String metadataProperties[] = { 
			"urlCount",
			"http200Count",
			"http301Count",
			"http403Count",
			"http404Count",
			"http500Count",
			"httpOtherCount",
			"RobotsExcludedCount",
			"TimeoutErrorCount",
			"IOExceptionCount",
			"InCacheCount",
			"OtherErrorCount",
			"redirectHttp200Count",
			"redirectHttp301Count",
			"redirectHttp403Count",
			"redirectHttp404Count",
			"redirectHttp500Count",
			"redirectHttpOtherCount",
			"redirectRobotsExcludedCount",
			"redirectTimeoutErrorCount",
			"redirectIOExceptionCount",
			"redirectInCacheCount",
			"redirectOtherErrorCount",
			"queuedCount"
	};
	
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)throws ServletException, IOException {
		String customerId = req.getParameter("customerId");
		
		ProxyServer server = ProxyServer.getSingleton();
		CrawlHistoryManager manager = server.getCrawlHistoryManager();
		
		if (customerId != null) {
			
			// get data 
			Map<Long,CrawlListDatabaseRecord> databaseRecords = server.getListInfoForCustomerId(customerId);
			Map<Long,CrawlListMetadata> metadataList = manager.collectListMetadata(databaseRecords.keySet());

			
			resp.setContentType("text/html");
			
			PrintWriter writer = resp.getWriter();
			
			writer.println("<HTML>");
			writer.println("<TABLE BORDER=1>");
			
			// ok iterate and dump out stats
			RPCStructIntrospector introspector = new RPCStructIntrospector(CrawlListMetadata.class);
			
			for (CrawlListDatabaseRecord record : databaseRecords.values()) { 

				writer.println("<TR>");
				writer.println("<TD>");
				writer.println("<CODE>");
				writer.println("<TABLE BORDER=0 cellpadding=0 cellspacing=0>");
				writer.println("<TR><TD><B>List Id</B>:<TD>" + record.getListId() + "</TR>");
				writer.println("<TR><TD><B>Description:</B>:<TD>" + record.getListName() + "</TR>");
				
				CrawlListMetadata metadata = metadataList.get(record.getListId());
				
				if (metadata != null) { 
					writer.println("<TR><TD colspan=2>&nbsp;</TR>");
					for (String property : metadataProperties) {
						Log.info("Retrieving value for Property:" + property);
						writer.println("<TR><TD>" + property + ":<TD>" + introspector.getStringValueGivenName(metadata, property) + "</TR>");
					}
				}
				writer.println("</TABLE>");
				writer.println("</TR>");
			}
			writer.println("</TABLE>");
		}
	}
}
