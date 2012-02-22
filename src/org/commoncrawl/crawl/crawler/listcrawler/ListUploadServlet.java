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


import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.Shell;
import org.commoncrawl.crawl.proxy.CrawlListDatabaseRecord;
import org.commoncrawl.util.shared.CCStringUtils;

import com.google.common.collect.ImmutableSet;

/** 
 * Servlet used to upload crawl lists to a crawler server 
 * 
 * @author rana
 *
 */
@SuppressWarnings("serial")
public class ListUploadServlet extends HttpServlet {

  public static final Log LOG = LogFactory.getLog(ListUploadServlet.class);

  
	public static class ListRequeueServlet extends HttpServlet { 
		
	  public static final Log LOG = LogFactory.getLog(ListRequeueServlet.class);
	  
		@Override
		protected void doGet(HttpServletRequest req, HttpServletResponse resp)throws ServletException, IOException {
		    
			String listId = req.getParameter("listId");
			String listFileName = req.getParameter("urlFile");
			File listFile = new File(ProxyServer.getSingleton().getCrawlHistoryDataDir(),listFileName);
			
			LOG.info("###LISTUPLOADER: Requeue Request- ListId:" + listId + " listFileName:" + listFileName);
			
			if (listFile.exists()) { 
				ProxyServer.getSingleton().requeueList(Long.parseLong(listId), listFile);
			}
		}
	}
	
	public static class RequeueBrokenListsServlet extends HttpServlet { 
	  
	     public static final Log LOG = LogFactory.getLog(RequeueBrokenListsServlet.class);

		@Override
		protected void doGet(HttpServletRequest req, HttpServletResponse resp)throws ServletException, IOException {
			long idsToFix[] = { 
					1286466854056L,1286733537313L,
					1286467182139L,1286733537315L,
					1286467448576L,1286733537316L,
					1286467734918L,1286733537318L,
					1286468071056L,1286733537319L,
					1286468376989L,1286733537321L,
					1286468673896L,1286733537322L,
					1286469018206L,1286733537324L,
					1286469408437L,1286733537327L,
					1286469703877L,1286733537329L,
					1286469965566L,1286733537331L,
					1286470262212L,1286733537332L,
					1286470558900L,1286733537334L,
					1286470853220L,1286733537360L 					
			};

			for (int i=0;i<idsToFix.length;) { 
				File listFile = new File(ProxyServer.getSingleton().getCrawlHistoryDataDir(),"listURLS-" + idsToFix[i++]);
				if (listFile.exists()) { 
					LOG.info("Reloading List File:" + listFile.getAbsolutePath());
					ProxyServer.getSingleton().requeueList(idsToFix[i++], listFile);
				}
			}
		}
	}
	
	public static class ListUploadForm extends HttpServlet { 
		
	    public static final Log LOG = LogFactory.getLog(ListUploadForm.class);

	  
		@Override
		protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
			
			resp.setContentType("text/html");
		  PrintWriter writer = resp.getWriter();
		  
		  writer.println("<HTML>");
		  
		  writer.println("<form method='post' action='/ListUploader' enctype='multipart/form-data'>");
		  writer.println("<table border=0>");
		  writer.println("<tr><td>CutomerId:<td><input name='customerId' type='text' width=20 /></tr>");
		  writer.println("<tr><td>List Name:<td><input name='listName' type='text' width=100 /></tr>");
		  writer.println("<tr><td>List File:<td><input name='listFile' type='file' /></tr>");
		  writer.println("<tr><td colspan=2>&nbsp</tr>");
		  writer.println("<tr><td colspan=2><input type='submit' /></tr>");
		  writer.println("</table>");

		  writer.println("</HTML>");
		  writer.flush();
		}
	}
	
	private final static String FILES ="org.mortbay.servlet.MultiPartFilter.files";

	
	private static Set<String> customers 
	  = new ImmutableSet.Builder<String>()
	    .add("foobar")
	    .build();
	
	@Override
	protected void doPut(HttpServletRequest req, HttpServletResponse resp)
	  throws ServletException, IOException {
	  
      String customerId = req.getParameter("customerId");
      String listName = req.getParameter("listName");
      String incomingFileName = req.getParameter("fileName");
      
      LOG.info("###LISTUPLOADER: GOT PUT Customer Id:" + customerId + " ListName:" + listName + " FileName:" + incomingFileName);
      if (customerId == null || !customers.contains(customerId) || listName == null || listName.length() == 0) { 
        LOG.error("###LISTUPLOADER:No Customer Id or Invalid Customer Id:" + customerId + " ListId:" + listName);
        resp.sendError(500,"Invalid Customer Id or Invalid List Name!" + customerId + ":" + listName);
        return;
      }
      else if (incomingFileName == null || incomingFileName.length() == 0) {
        LOG.error("###LISTUPLOADER:No IncomingFilename");
        resp.sendError(500,"Invalid Filename");
        return;
      }
      else { 
        // get the server ... 
        ProxyServer server = ProxyServer.getSingleton();
        // get the crawl history data directory ...
        File dataDir = server.getCrawlHistoryDataDir();
        // create import file ... 
        File importFile = new File(dataDir,incomingFileName + "-" + System.currentTimeMillis());
        LOG.info("###LISTUPLOADER:Filename:" + incomingFileName + " Customer:" + customerId + " List:" + listName + " outputFile:" + importFile.getAbsolutePath());
        // open a handle to it 
        BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(importFile),1 << 20);
        // allocate a buffer ... 
        byte incomingBuffer[] = new byte[ 1 << 19 ];
        int bytesRead = -1;
        int totalBytesRead = 0;
        // get input stream 
        InputStream input = req.getInputStream();
        try { 
          try { 
            while ((bytesRead = input.read(incomingBuffer)) != -1) { 
              LOG.info("Read:" + bytesRead + " bytes from:" + incomingFileName);
              outputStream.write(incomingBuffer,0,bytesRead);
              totalBytesRead += bytesRead;
            }
          }
          finally  {
            outputStream.flush();
            outputStream.close();
          }
          LOG.info("###LISTUPLOADER:List:" + listName + " Finished download filename:" + incomingFileName + " TotalBytesRead:" + totalBytesRead + "-Inserting Record");
          // won't reach here unless write succeeded ... 
          // create a database record 
          CrawlListDatabaseRecord databaseRecord = new CrawlListDatabaseRecord();
          
          databaseRecord.setListName(listName);
          databaseRecord.setCustomerName(customerId);
          databaseRecord.setSourceFileName(incomingFileName);
          databaseRecord.setTempFileName(importFile.getName());
          
          
          long listId = server.queueListImportRequest(databaseRecord);
          
          LOG.info("###LISTUPLOADER:Queueing List:" + listName + " ListID:"+ listId);

          
          if (listId == -1) {
              LOG.error("###LISTUPLOADER:Queueing For List:" + listName + " Failed!");
              resp.sendError(500,"Queue Request Failed!");
          }
          else { 
              resp.setContentType("text/plain");
              resp.getWriter().print(Long.toString(listId));
              resp.getWriter().flush();
          }          
        }
        catch (IOException e) { 
          LOG.error("###LISTUPLOADER: IOException processing List:" + listName);
          LOG.error(CCStringUtils.stringifyException(e));
          importFile.delete();
        }
      }
	}
	
	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

		String customerId = req.getParameter("customerId");
		String listName = req.getParameter("listName");
		
		LOG.info("###LISTUPLOADER: GOT POST CustomerId:" + customerId + " ListName:" + listName);
		if (customerId == null || !customers.contains(customerId) || listName == null || listName.length() == 0) { 
			resp.sendError(500,"Invalid Customer Id or Invalid List Name!" + customerId + ":" + listName);
		}
		else { 
			ArrayList<MultiPartFilter.UploadFileData> files= (ArrayList<MultiPartFilter.UploadFileData>) req.getAttribute(FILES);

			if (files == null || files.size() == 0) {
			  LOG.error("###LISTUPLOADER: CustomerId:" + customerId + " ListName:" + listName + " No Files in Mutlipart Body!");
			  resp.sendError(500,"No File Selected!");
			  return;
			}
			else { 
				MultiPartFilter.UploadFileData uploadData = files.get(0);
				if (uploadData.incomingContentType == null || !uploadData.incomingContentType.equals("text/plain")) { 
				  LOG.error("###LISTUPLOADER: CustomerId:" + customerId 
				      + " ListName:" + listName 
				      + " incoming MimeType:" 
				      + uploadData.incomingContentType 
				      + " NOT text/plain!");
				  resp.sendError(500,"Only Text Files Supported For Now :-(");
				  return;
				}
				else{ 
					// get the server ... 
					ProxyServer server = ProxyServer.getSingleton();
					// get the crawl history data directory ...
					File dataDir = server.getCrawlHistoryDataDir();
					LOG.info("###LISTUPLOADER: CustomerId:" + customerId 
	                      + " ListName:" + listName 
	                      + "Incoming FileName is:" + uploadData.incomingFile.getAbsolutePath());					

					// move the file 
					File importFile = new File(dataDir,uploadData.incomingFilename + "-" + System.currentTimeMillis());
                    LOG.info("###LISTUPLOADER: CustomerId:" + customerId 
                        + " ListName:" + listName 
                        +"Renaming Incoming File to:" + importFile.getAbsolutePath());                   
					
					
					int retryCount = 0;
					boolean renameFailed = false;
					
					
					while (!importFile.exists()) {
	                    LOG.info("###LISTUPLOADER: CustomerId:" + customerId 
	                        + " ListName:" + listName 
	                        +" Moving Temp File");                   
					  
						Shell.execCommand(new String[] {"mv", uploadData.incomingFile.getAbsolutePath(),importFile.getAbsolutePath() } );

						
						if (!importFile.exists()) {
							if (++retryCount == 10) {
								renameFailed = true;
	                            LOG.error("###LISTUPLOADER: CustomerId:" + customerId 
	                                    + " ListName:" + listName 
	                                    +" Rename Failed. Bailing!");                   
								
								break;
							}
		                     LOG.error("###LISTUPLOADER: CustomerId:" + customerId 
		                            + " ListName:" + listName 
		                            +" Rename Failed. Retrying");                   

							try {
		            Thread.sleep(1000);
	            } catch (InterruptedException e) {
		            // TODO Auto-generated catch block
		            e.printStackTrace();
	            }
						}
						else { 
							break;
						}
					}
					if (renameFailed) {
                        LOG.error("###LISTUPLOADER: CustomerId:" + customerId 
                          + " ListName:" + listName 
                          +" Gave Up Trying to Move File!");                   
					  
						resp.sendError(500,"Failed to Copy Temp File!");
						return;
					}
					
                    LOG.info("###LISTUPLOADER: CustomerId:" + customerId 
                        + " ListName:" + listName 
                        +" Queueing Database Record");                   
					
					// create a database record 
					CrawlListDatabaseRecord databaseRecord = new CrawlListDatabaseRecord();
					
					databaseRecord.setListName(listName);
					databaseRecord.setCustomerName(customerId);
					databaseRecord.setSourceFileName(uploadData.incomingFilename);
					databaseRecord.setTempFileName(importFile.getName());
					
					long listId = server.queueListImportRequest(databaseRecord);
					
					if (listId == -1) {
	                    LOG.error("###LISTUPLOADER: CustomerId:" + customerId 
	                        + " ListName:" + listName 
	                        +" List Queueing Failed!");                   
						resp.sendError(500,"Queue Request Failed!");
					}
					else {
					    LOG.info("###LISTUPLOADER: CustomerId:" + customerId 
                          + " ListName:" + listName 
                          +" ListId:" + listId);                   
					  
						resp.setContentType("text/plain");
						resp.getWriter().print(Long.toString(listId));
						resp.getWriter().flush();
					}
				}
			}
		}
	}
	
}
