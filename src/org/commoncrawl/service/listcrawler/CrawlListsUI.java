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
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.Semaphore;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.server.AsyncWebServerRequest;
import org.commoncrawl.server.CommonCrawlServer;
import org.commoncrawl.service.listcrawler.CrawlListDatabaseRecord;
import org.commoncrawl.service.listcrawler.CrawlListDomainItem;
import org.commoncrawl.service.listcrawler.CrawlListMetadata;
import org.commoncrawl.service.listcrawler.CrawlList.QueueState;
import org.commoncrawl.util.CCStringUtils;

import com.google.gson.stream.JsonWriter;

@SuppressWarnings("serial")
/** 
 * Servlet used to support the crawl lists ui
 * 
 * @author rana
 *
 */
public class CrawlListsUI  extends HttpServlet {

	public static final Log LOG = LogFactory.getLog(CrawlListsUI.class);
	
	static String salt = "#$@!1Z";
	static byte secretKey[] = {(byte)0xcd,(byte)0xe7,(byte)0xe9,(byte)0x9d,(byte)0xb4,(byte)0x84,(byte)0xc5,0x2f,0x49,(byte)0xee,0x16,(byte)0xb1,0x12,(byte)0xa6,(byte)0xef,(byte)0xb7};

	
	public static String decryptUserKey(String userKey) {
	  if (userKey.length() % 2 != 0) {
	    return null;
	  }
	  byte keyAsHex[] = hexStringToByteArray(userKey);
	  if (keyAsHex != null) {
	    SecretKeySpec skeySpec = new SecretKeySpec(secretKey, "AES");
	    try {
	      Cipher cipher = Cipher.getInstance("AES");
	      cipher.init(Cipher.DECRYPT_MODE, skeySpec);
	      byte[] original =  cipher.doFinal(keyAsHex);
	      String originalString = new String(original);

	      if (originalString.startsWith(salt)) {
	        return originalString.substring(salt.length());
	      }

	    } catch (NoSuchAlgorithmException e) {
	      // TODO Auto-generated catch block
	      e.printStackTrace();
	    } catch (NoSuchPaddingException e) {
	      // TODO Auto-generated catch block
	      e.printStackTrace();
	    } catch (InvalidKeyException e) {
	      // TODO Auto-generated catch block
	      e.printStackTrace();
	    } catch (IllegalBlockSizeException e) {
	      // TODO Auto-generated catch block
	      e.printStackTrace();
	    } catch (BadPaddingException e) {
	      // TODO Auto-generated catch block
	      e.printStackTrace();
	    }
	  }
	  return null;
	}
	
	public static byte[] hexStringToByteArray(String s) {
	  int len = s.length();
	  byte[] data = new byte[len / 2];
	  for (int i = 0; i < len; i += 2) {
	    data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
	        + Character.digit(s.charAt(i+1), 16));
	  }
	  return data;
	}
	
	public static class HttpResult { 
		public int _resultCode = HttpServletResponse.SC_OK;
		public String _resultDesc = "";
	}
	
	@Override
	protected void doGet(final HttpServletRequest req, final HttpServletResponse resp)throws ServletException, IOException {
		
		String listIdStr = req.getParameter("listId");
		String reqType = req.getParameter("reqType");
		HttpResult result = new HttpResult();
		
		result._resultCode = HttpServletResponse.SC_BAD_REQUEST;
		
		resp.setContentType("application/json");
		
		if (listIdStr != null && reqType != null) { 
			
			long listId = Long.parseLong(listIdStr);
			if (reqType.equals("listLists")) { 
				String encCustomerId = req.getParameter("customerId");
				String customerId = decryptUserKey(encCustomerId);
				
				if (customerId != null) { 
					getListsForCustomer(customerId,resp,result);
				}
			}
			else if (reqType.equals("subDomainCount")) {
              String encCustomerId = req.getParameter("customerId");
              String customerId = decryptUserKey(encCustomerId);

              if (customerId != null) {
				getSubDomainCount(customerId,listId,resp,result);
              }
			}
			else if (reqType.equals("subDomainList")) { 
				String offset = req.getParameter("offset");
				String count  = req.getParameter("count");
                String encCustomerId = req.getParameter("customerId");
                String customerId = decryptUserKey(encCustomerId);
				if (offset != null && count != null && customerId != null) {
					getDomainListForListId(customerId,listId, Integer.parseInt(offset),Integer.parseInt(count),resp,result);
				}
			}
			else if (reqType.equals("listDetails")) {
              String encCustomerId = req.getParameter("customerId");
              String customerId = decryptUserKey(encCustomerId);
              if (customerId != null) {
                getListDetails(customerId,listId,resp,result);
              }
			}
			else if (reqType.equals("domainDetails")) { 
				String domainId = req.getParameter("domainId");
                String encCustomerId = req.getParameter("customerId");
                String customerId = decryptUserKey(encCustomerId);
				
				if (domainId != null && customerId != null) { 
					getDomainDetailForDomain(customerId,listId,domainId,resp,result);
				}
			}
		}
		
		if (result._resultCode != HttpServletResponse.SC_OK) { 
			resp.sendError(result._resultCode, result._resultDesc);
		}
	}
	
	
	private static CrawlListDomainItem buildListSummary(CrawlListMetadata metadata) {
		
		CrawlListDomainItem domainItem = new CrawlListDomainItem();
				
		
		int robotsExcludedItemsCount =0;
		int errorItemsCount =0;
		int inCacheItems = 0;
		int processedItemsCount = 0;
		int http200Count = 0;
		
		http200Count += metadata.getHttp200Count();

		robotsExcludedItemsCount += metadata.getRobotsExcludedCount();

		errorItemsCount += metadata.getTimeoutErrorCount();
		errorItemsCount += metadata.getIOExceptionCount();
		errorItemsCount += metadata.getDNSErrorCount();
		errorItemsCount += metadata.getOtherErrorCount();
		
		processedItemsCount += metadata.getHttp200Count();
		processedItemsCount += metadata.getHttp403Count();
		processedItemsCount += metadata.getHttp404Count();
		processedItemsCount += metadata.getHttp500Count();
		processedItemsCount += metadata.getHttpOtherCount();
		 
		domainItem.setUrlCount(metadata.getUrlCount());
		domainItem.setUrlsCrawled(processedItemsCount);
		domainItem.setHttp200Count(http200Count);
		domainItem.setInCacheItemsCount(0);
		domainItem.setRobotsExcludedCount(robotsExcludedItemsCount);
		domainItem.setErrorCount(errorItemsCount);
		domainItem.setQueuedCount(metadata.getQueuedItemCount());
		
		return domainItem;
	}

	
	public static void getDomainDetailForDomain(final String customerId,final long listId,final String domainName,final HttpServletResponse resp,final HttpResult result)throws IOException {

	  final CommonCrawlServer server = CommonCrawlServer.getServerSingleton();

	  server.dispatchAsyncWebRequest(new AsyncWebServerRequest("",resp.getWriter()) {

	    @Override
	    public boolean handleRequest(Semaphore completionSemaphore) throws IOException {

	      ProxyServer proxyServer = (ProxyServer)server;

	      if (!proxyServer.doesListBelongToCustomer(listId, customerId)) {
	        resp.sendError(HttpServletResponse.SC_FORBIDDEN);
	      }
	      else {

	        CrawlList list = proxyServer.getCrawlHistoryManager().getList(listId);
	        if (list != null && list.isListLoaded()) {
	          CrawlListMetadata metadata = list.getSubDomainMetadataByDomain(domainName);

	          if (metadata != null) {

	            CrawlListDomainItem item = buildListSummary(metadata);


	            PrintWriter writer = resp.getWriter();
	            JsonWriter jsonWriter = new JsonWriter(writer);

	            try {
	              jsonWriter.beginObject();
	              jsonWriter.name("items");
	              jsonWriter.beginArray();

	              if (item.getHttp200Count() != 0)	
	                jsonWriter.beginArray().value("http200").value(item.getHttp200Count()).endArray();
	              int http403Count = metadata.getHttp403Count() + metadata.getRedirectHttp403Count();
	              if (http403Count != 0)
	                jsonWriter.beginArray().value("http403").value(http403Count).endArray();
	              int http404Count = metadata.getHttp404Count() + metadata.getRedirectHttp404Count();
	              if (http404Count != 0)
	                jsonWriter.beginArray().value("http404").value(http404Count).endArray();
	              int http500Count = metadata.getHttp500Count() + metadata.getRedirectHttp500Count();
	              if (http500Count != 0)
	                jsonWriter.beginArray().value("http500").value(http500Count).endArray();
	              int httpOtherCount = metadata.getHttpOtherCount() + metadata.getRedirectHttpOtherCount();
	              if (httpOtherCount != 0)
	                jsonWriter.beginArray().value("httpOther").value(httpOtherCount).endArray();
	              if (item.getInCacheItemsCount() != 0)
	                jsonWriter.beginArray().value("inCache").value(item.getInCacheItemsCount()).endArray();
	              if (item.getRobotsExcludedCount() != 0)
	                jsonWriter.beginArray().value("robotsExcluded").value(item.getRobotsExcludedCount()).endArray();

	              // caculate errors 
	              int timeoutErrorCount = metadata.getTimeoutErrorCount() + metadata.getRedirectTimeoutErrorCount();
	              int ioexceptionErrorCount = metadata.getIOExceptionCount() + metadata.getRedirectIOExceptionCount();
	              int otherErrorCount  = metadata.getOtherErrorCount();

	              if (timeoutErrorCount != 0)
	                jsonWriter.beginArray().value("timeouts").value(timeoutErrorCount).endArray();

	              if (ioexceptionErrorCount != 0)
	                jsonWriter.beginArray().value("exceptions").value(ioexceptionErrorCount).endArray();


	              /*
							if (otherErrorCount != 0)
								jsonWriter.beginArray().value("other errors").value(otherErrorCount).endArray();
	               */


	              // calculate remaining items 
	              int remainingItems = metadata.getUrlCount();
	              // take off http counts
	              remainingItems -= item.getHttp200Count();
	              remainingItems -= http403Count;
	              remainingItems -= http404Count;
	              remainingItems -= http500Count;
	              remainingItems -= httpOtherCount;
	              remainingItems -= item.getRobotsExcludedCount();
	              remainingItems -= (timeoutErrorCount + ioexceptionErrorCount);
	              if (remainingItems > 0) { 
	                jsonWriter.beginArray().value("remaining").value(remainingItems).endArray();
	              }

	              jsonWriter.endArray();
	              jsonWriter.endObject();

	              result._resultCode = HttpServletResponse.SC_OK;

	            } catch (Exception e) {
	              throw new IOException(e);
	            }


	          }
	        }
	      }
	      return false;
	    }
	  });
	}
	
	private static void getListsForCustomer(final String customerId,final HttpServletResponse resp,final HttpResult result) throws IOException {
		
		final CommonCrawlServer server = CommonCrawlServer.getServerSingleton();

		server.dispatchAsyncWebRequest(new AsyncWebServerRequest("",resp.getWriter()) {

			@Override
      public boolean handleRequest(final Semaphore completionSemaphore)throws IOException {
				
				final ProxyServer proxyServer = (ProxyServer)server;
				
				LOG.info("Getting List for Customer:" + customerId);
				final Collection<CrawlListDatabaseRecord> recordSet = proxyServer.getListInfoForCustomerId(customerId).values();
				
				final ArrayList<CrawlListDatabaseRecord> sortedSet = new ArrayList<CrawlListDatabaseRecord>();
				
				sortedSet.addAll(recordSet);
				
				// sort by timestamp
				Collections.sort(sortedSet,new Comparator<CrawlListDatabaseRecord>() {

					@Override
          public int compare(CrawlListDatabaseRecord o1,CrawlListDatabaseRecord o2) {
	          return (o1.getListId() > o2.getListId()) ? -1 : 1;
          } 
					
				});
				
				LOG.info("Found:" +  sortedSet.size() + " Lists for Customer:" + customerId);
				
				if (sortedSet.size() != 0) { 
					
					Thread thread = new Thread(new Runnable() {

						@Override
            public void run() {
							LOG.info("Running Worker Thread");
							try { 
								PrintWriter writer = resp.getWriter();
								
								JsonWriter jsonWriter = new JsonWriter(writer);
								
								jsonWriter.beginObject();
								jsonWriter.name("items");
								jsonWriter.beginArray();
																
								for (CrawlListDatabaseRecord listRecord : sortedSet) { 
									
									
									// get the list 
									CrawlList list = proxyServer.getCrawlHistoryManager().getList(listRecord.getListId());
									if (list == null) { 
										LOG.error("DID NOT Find List Object for List:" + listRecord.getListId() + " Name:" + listRecord.getListName() + " FileName:" + listRecord.getSourceFileName() + " TempFile:" + listRecord.getTempFileName());
									}
									if (list != null) {
										String queueState = "W";
										if (list.isListLoaded()) {
											
										    if (list.getQueuedState() == QueueState.QUEUEING) 
										      queueState = "Q";
										    else if (list.getQueuedState() == QueueState.QUEUED)
										      queueState = "L";
										    else if (list.getQueuedState() == QueueState.ERROR)
										      queueState = "L";
										    else 
										      queueState = "?";
											CrawlListMetadata metadata = list.getMetadata();
											CrawlListDomainItem summary = buildListSummary(metadata);
											// populate identification info 
											summary.setListId(list.getListId());
											summary.setListName(listRecord.getListName());
											
											jsonWriter.beginArray();
											jsonWriter.value(summary.getListId());
											jsonWriter.value(queueState);
											jsonWriter.value(summary.getListName());
											jsonWriter.value(list.getSubDomainItemCount());
											jsonWriter.value(summary.getUrlCount());
											jsonWriter.value(summary.getUrlsCrawled());
											jsonWriter.value(summary.getHttp200Count());
											jsonWriter.value(summary.getRobotsExcludedCount());
											jsonWriter.value(summary.getErrorCount());
											jsonWriter.value(summary.getQueuedCount());
											
											jsonWriter.endArray();
										}
										else if (list.getLoadState() == CrawlList.LoadState.QUEUED_FOR_LOADING) { 
											jsonWriter.beginArray();
											jsonWriter.value(list.getListId());
											jsonWriter.value(queueState);
											jsonWriter.value("<B>Queued:</B>" + listRecord.getListName());
											jsonWriter.value(0);
											jsonWriter.value(0);
											jsonWriter.value(0);
											jsonWriter.value(0);
											jsonWriter.value(0);
											jsonWriter.value(0);
											jsonWriter.value(0);

																						
											jsonWriter.endArray();
										}
										else if (list.getLoadState() == CrawlList.LoadState.REALLY_LOADING) { 
											jsonWriter.beginArray();
											jsonWriter.value(list.getListId());
											jsonWriter.value("<B>Loading:</B>" + listRecord.getListName());
											jsonWriter.value(0);
											jsonWriter.value(0);
											jsonWriter.value(0);
											jsonWriter.value(0);
											jsonWriter.value(0);
											jsonWriter.value(0);
											jsonWriter.value(0);

											jsonWriter.endArray();
										}
										else if (list.getLoadState() == CrawlList.LoadState.ERROR) { 
											jsonWriter.beginArray();
											jsonWriter.value(list.getListId());
											jsonWriter.value("ERR");
											jsonWriter.value(0);
											jsonWriter.value(0);
											jsonWriter.value(0);
											jsonWriter.value(0);
											jsonWriter.value(0);
											jsonWriter.value(0);
											jsonWriter.value(0);
											 
											jsonWriter.endArray();
										}
									}
								}
								
								jsonWriter.endArray();
								jsonWriter.endObject();
								
								LOG.info("Done");
								
								result._resultCode = HttpServletResponse.SC_OK;
								
							}
							catch (IOException e) {
								LOG.error(CCStringUtils.stringifyException(e));
							} catch (Exception e) {
								LOG.error(CCStringUtils.stringifyException(e));
							}
							finally { 
								LOG.error("DONE");
								completionSemaphore.release();
							}
            } 
						
					});
					
					LOG.info("Spawning Worker Thread");
					thread.start();
					
					return true;
				}
				return false;
      }

		});
		
	}
	public static void getListDetails(final String customerId,final long listId,final HttpServletResponse resp,final HttpResult result) throws IOException { 
	  final CommonCrawlServer server = CommonCrawlServer.getServerSingleton();

	  server.dispatchAsyncWebRequest(new AsyncWebServerRequest("",resp.getWriter()) {

	    @Override
	    public boolean handleRequest(Semaphore completionSemaphore) throws IOException {

	      ProxyServer proxyServer = (ProxyServer)server;

	      if (!proxyServer.doesListBelongToCustomer(listId, customerId)) {
	        resp.sendError(HttpServletResponse.SC_FORBIDDEN);
	      }
	      else {

	        CrawlList list = proxyServer.getCrawlHistoryManager().getList(listId);

	        if (list != null && list.isListLoaded()) {

	          CrawlListMetadata metadata = list.getMetadata();
	          CrawlListDomainItem item = buildListSummary(metadata);

	          PrintWriter writer = resp.getWriter();

	          writer.println(
	              "{ "  
	              + "total:"+item.getUrlCount() +","
	              + "crawled:"+item.getUrlsCrawled() +","
	              + "http200:"+item.getHttp200Count() +","
	              + "inCache:"+item.getInCacheItemsCount() +","
	              + "robotsExcluded:"+item.getRobotsExcludedCount() +","
	              + "error:"+item.getErrorCount() 
	              + "queued:" + item.getQueuedCount() + "}");


	          result._resultCode = HttpServletResponse.SC_OK;

	        }
	      }
	      return false;
	    }
	  });
	}
	
	public static void getSubDomainCount(final String customerId,final long listId,final HttpServletResponse resp,final HttpResult result) throws IOException { 
		
	    final CommonCrawlServer server = CommonCrawlServer.getServerSingleton();

		server.dispatchAsyncWebRequest(new AsyncWebServerRequest("",resp.getWriter()) {
			
			@Override
			public boolean handleRequest(Semaphore completionSemaphore) throws IOException {
				
				ProxyServer proxyServer = (ProxyServer)server;
				
				if (!proxyServer.doesListBelongToCustomer(listId, customerId)) {
				  resp.sendError(HttpServletResponse.SC_FORBIDDEN);
				}
				else {
    				CrawlList list = proxyServer.getCrawlHistoryManager().getList(listId);
    				if (list != null && list.isListLoaded()) {
    					PrintWriter writer = resp.getWriter();
    					writer.println(
    							"{ "  
    								+ "itemCount:" + list.getSubDomainItemCount()
    						+ "}");
    				
    					result._resultCode = HttpServletResponse.SC_OK;
    
    				}
				}
  				
  				return false;
			}
		});
		
	}
	
	public static void getDomainListForListId(final String customerId,final long listId,final int offset,final int count,final HttpServletResponse resp,final HttpResult result) throws IOException {

	  final CommonCrawlServer server = CommonCrawlServer.getServerSingleton();

	  server.dispatchAsyncWebRequest(new AsyncWebServerRequest("",resp.getWriter()) {

	    @Override
	    public boolean handleRequest(Semaphore completionSemaphore) throws IOException {

	      ProxyServer proxyServer = (ProxyServer)server;

	      if (!proxyServer.doesListBelongToCustomer(listId, customerId)) {
	        resp.sendError(HttpServletResponse.SC_FORBIDDEN);
	      }
	      else {
	        CrawlList list = proxyServer.getCrawlHistoryManager().getList(listId);
	        if (list != null && list.isListLoaded()) {
	          PrintWriter writer = resp.getWriter();
	          JsonWriter jsonWriter= new JsonWriter(writer);

	          try {
	            jsonWriter.beginObject();
	            jsonWriter.name("items");
	            jsonWriter.beginArray();

	            int urlCount = 0;
	            for (CrawlListDomainItem item : list.getSubDomainList(offset,count)) {

	              jsonWriter.beginArray();
	              jsonWriter.value(item.getDomainName());
	              jsonWriter.value(item.getUrlCount());
	              urlCount += item.getUrlCount();
	              jsonWriter.value(item.getUrlsCrawled());
	              jsonWriter.value(item.getQueuedCount());
	              jsonWriter.value(item.getHashCode());
	              jsonWriter.endArray();
	            }

	            jsonWriter.endArray();
	            jsonWriter.name("remainingItems").value(list.getMetadata().getUrlCount() - urlCount);
	            jsonWriter.endObject();

	            result._resultCode = HttpServletResponse.SC_OK;
	          }
	          catch (Exception e) { 
	            throw new IOException(e);
	          }
	        }
	        else { 
	          resp.getWriter().print("Crawl List NULL!!");
	        }
	      }
	      return false;
	    }
	  });
	}
  
}
