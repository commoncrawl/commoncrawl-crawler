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
package org.commoncrawl.service.queryserver.master;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.file.tfile.TFile;
import org.commoncrawl.util.CrawlDatum;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.protocol.ArchiveInfo;
import org.commoncrawl.protocol.CrawlDatumAndMetadata;
import org.commoncrawl.protocol.CrawlURLMetadata;
import org.commoncrawl.protocol.SubDomainMetadata;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.protocol.shared.ArcFileHeaderItem;
import org.commoncrawl.protocol.shared.ArcFileItem;
import org.commoncrawl.service.queryserver.ClientQueryInfo;
import org.commoncrawl.service.queryserver.DomainListQueryInfo;
import org.commoncrawl.service.queryserver.DomainURLListQueryInfo;
import org.commoncrawl.service.queryserver.InlinkingDomainInfo;
import org.commoncrawl.service.queryserver.InlinksByDomainQueryInfo;
import org.commoncrawl.service.queryserver.URLLinkDetailQueryInfo;
import org.commoncrawl.service.queryserver.index.DatabaseIndexV2.MasterDatabaseIndex.MetadataOut;
import org.commoncrawl.service.queryserver.master.MasterServer.BlockingQueryResult;
import org.commoncrawl.service.queryserver.query.DomainListQuery;
import org.commoncrawl.service.queryserver.query.DomainURLListQuery;
import org.commoncrawl.service.queryserver.query.InverseLinksByDomainQuery;
import org.commoncrawl.service.queryserver.query.QueryResult;
import org.commoncrawl.service.queryserver.query.QueryResultRecord;
import org.commoncrawl.service.queryserver.query.URLLinksQuery;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.FlexBuffer;
import org.commoncrawl.util.GoogleURL;
import org.commoncrawl.util.MurmurHash;
import org.commoncrawl.util.ProtocolStatus;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLUtils;

import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.DefaultServlet;

import com.google.gson.stream.JsonWriter;

/**
 * 
 * @author rana
 *
 */
public class QueryServerFE {

  private static final Log LOG = LogFactory.getLog(QueryServerFE.class);

  private static MasterServer _server;
  private File   _webAppRoot;

  
  public static MasterServer getServer() { return _server; }
  
  
  public QueryServerFE(MasterServer server, File webAppRoot) throws IOException {
    _server = server;
    _webAppRoot = webAppRoot;
    _server.getWebServer().addServlet("domainListQuery","/domainListQuery.jsp",DomainListQueryServlet.class);
    _server.getWebServer().addServlet("domainDetail","/domainDetail.jsp",DomainDataQueryServlet.class);
    _server.getWebServer().addServlet("urlDetail","/urlDetail.jsp",URLDetailServlet.class);
    _server.getWebServer().addServlet("linkDetail","/linkDetail.jsp",LinkDetailsServlet.class);
    _server.getWebServer().addServlet("contentDetail","/getCachedContent.jsp",URLContentServlet.class);
    _server.getWebServer().addServlet("getCrawlList","/getCrawlList.jsp",CrawlListServlet.class);
    LOG.info("Adding GetInverseByDomain Servet");
    _server.getWebServer().addServlet("getInverseByDomain","/getInverseLinksByDomain.jsp",InverseURLListByRootDomainQueryServlet.class);
    
    
    Context staticContext = new Context(_server.getWebServer().getContextHandlerCollection(),"/img");
    staticContext.setResourceBase(_webAppRoot.getAbsolutePath() + "/img/");
    staticContext.addServlet(DefaultServlet.class, "/");
    
    staticContext = new Context(_server.getWebServer().getContextHandlerCollection(),"/css");
    staticContext.setResourceBase(_webAppRoot.getAbsolutePath() + "/css/");
    staticContext.addServlet(DefaultServlet.class, "/");
    
    staticContext = new Context(_server.getWebServer().getContextHandlerCollection(),"/js");
    staticContext.setResourceBase(_webAppRoot.getAbsolutePath() + "/js/");
    staticContext.addServlet(DefaultServlet.class, "/");
    
  
  }
  
  
  @SuppressWarnings("serial")
  public static class DomainListQueryServlet extends HttpServlet { 
    
    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response)throws ServletException, IOException {
      
      //LOG.info("Received Request:" +request.toString());
      
      try { 
        String pattern = request.getParameter("pattern");
        int page_no = Integer.parseInt(request.getParameter("page_no")) - 1;
        int page_size = Integer.parseInt(request.getParameter("page_size"));
        String sortBy = request.getParameter("sort_by");
        String sortOrder = request.getParameter("sort_order");
        String renderType = request.getParameter("render_type");
        

        if (pattern == null) { 
        	throw new IOException("Invalid Search Pattern Specified");
        }
        
        // ok see if this is a valid url ...
        GoogleURL urlObject = new GoogleURL(pattern);
        if (urlObject.isValid()) { 
        	// ok, detected a direct url query ...
        	// formulate a specific response ... 
					PrintWriter writer = response.getWriter();
					JsonWriter jsonWriter = new JsonWriter(writer);
					
					try {
            jsonWriter.beginObject();
            jsonWriter.name("isURL");        	
            jsonWriter.value(1);
            jsonWriter.name("domainName");
            jsonWriter.value(urlObject.getHost());
            jsonWriter.endObject();
            return;
	        } catch (Exception e) {
	        	LOG.error(CCStringUtils.stringifyException(e));
	          throw new IOException(e);
	        }
        }
        
        // build domain query info 
        DomainListQueryInfo queryInfo = new DomainListQueryInfo();
        
        // set search pattern parameter 
        queryInfo.setSearchPattern(pattern);
        
        // initialize paging info 
        ClientQueryInfo clientQueryInfo = new ClientQueryInfo();
        
        clientQueryInfo.setSortByField(sortBy);
        clientQueryInfo.setPageSize(page_size);
        clientQueryInfo.setPaginationOffset(page_no);
        if (sortOrder.equalsIgnoreCase("ASC"))
          clientQueryInfo.setSortOrder(ClientQueryInfo.SortOrder.ASCENDING);
        else 
          clientQueryInfo.setSortOrder(ClientQueryInfo.SortOrder.DESCENDING);
        
        DomainListQuery query = new DomainListQuery(queryInfo);

        
        try {
        
        	BlockingQueryResult<Text, SubDomainMetadata> result  = null;
        	
        	// ok first see if the search pattern is actually a domain name 
        	if (URLUtils.isValidDomainName(pattern)) {
        		LOG.info("Pattern like a valid domain:" + pattern);
        		// ok in this case, do a direct query to get domain metadata ...  
        		SubDomainMetadata metadata = _server.getDatabaseIndex().queryDomainMetadataGivenDomainName(pattern);
      			// ok construct a result object 
      			QueryResult<Text, SubDomainMetadata> resultInner = new QueryResult<Text, SubDomainMetadata>();
      			result = new BlockingQueryResult<Text, SubDomainMetadata>(resultInner);
      			result.querySucceeded = true;
        		if (metadata != null) { 
        			LOG.info("Found Metadata for Domain:" + pattern);
        			// populate it 
        			resultInner.getResults().add(new QueryResultRecord<Text, SubDomainMetadata>(new Text(pattern), metadata));
        			resultInner.setTotalRecordCount(1);
        		}
        		else { 
        			LOG.info("Failed to find Metadata for Domain:" + pattern);
        		}
        	}
        	else {
        		// ok ... see if this is a regular expression 
        		try { 
        			Pattern.compile(pattern);
        		}
        		catch (PatternSyntaxException e) { 
        			LOG.error(CCStringUtils.stringifyException(e));
        			throw new IOException("Invalid Regular Expression Syntax in Search Pattern!");
        		}
        		
        		// ok at this point assume pattern is good .. do a parallel query ... 
        		result = _server.blockingQueryRequest(query,clientQueryInfo);
        	}
          
          if (result.querySucceeded) { 
            OutputStream outStream;
            try {
              outStream = response.getOutputStream();
              PrintWriter writer = new PrintWriter(outStream);
              
              if (renderType != null && renderType.equalsIgnoreCase("x-json")) { 
                response.setContentType("application/x-json");
              }
              else { 
              	response.setContentType("text/plain");
              }
              	
              
              writer.write("{\"isURL\":0,\"total\":"+result.resultObject.getTotalRecordCount() +",");
              writer.write("\"page\":"+ (page_no + 1) +",");
              writer.write("\"rows\":[");
              int count = 0;
              for (QueryResultRecord<Text,SubDomainMetadata> record : result.resultObject.getResults()) {
                if (count++ != 0) 
                  writer.write(",");
                writer.write("{ cell:[");
                writer.write(quote(record.getKey().toString()) + ",");
                writer.print(record.getValue().getUrlCount());
                writer.write("] }\n");
              }
              writer.append("]}");
              writer.flush();

              outStream.close();
            }
            catch (IOException e) { 
              LOG.error(CCStringUtils.stringifyException(e));
            }
          }
          else { 
          	
            OutputStream outStream;
            try {
              outStream = response.getOutputStream();
              PrintWriter writer = new PrintWriter(outStream);

              response.setContentType("text/plain");

              writer.append("Query Failed with Error:\n");
              writer.append(result.errorString);
              
              writer.flush();
              outStream.close();
            }
            catch (IOException e) { 
              LOG.error(CCStringUtils.stringifyException(e));
            }
          }
          
        } catch (IOException e) {
          LOG.error("Query Failed with Exception:" + CCStringUtils.stringifyException(e));
          throw e;
        }
      }
      catch (Exception e) { 
        throw new IOException(CCStringUtils.stringifyException(e));
      }
    }
  }
  
  private static final HashMap<Integer, String> codeToName =
    new HashMap<Integer, String>();
  static {
    codeToName.put(ProtocolStatus.FAILED, "failed");
    codeToName.put(ProtocolStatus.GONE, "gone");
    codeToName.put(ProtocolStatus.MOVED, "moved");
    codeToName.put(ProtocolStatus.TEMP_MOVED, "temp_moved");
    codeToName.put(ProtocolStatus.NOTFOUND, "notfound");
    codeToName.put(ProtocolStatus.RETRY, "retry");
    codeToName.put(ProtocolStatus.EXCEPTION, "exception");
    codeToName.put(ProtocolStatus.ACCESS_DENIED, "access_denied");
    codeToName.put(ProtocolStatus.ROBOTS_DENIED, "robots_denied");
    codeToName.put(ProtocolStatus.REDIR_EXCEEDED, "redir_exceeded");
    codeToName.put(ProtocolStatus.NOTFETCHING, "notfetching");
    codeToName.put(ProtocolStatus.NOTMODIFIED, "notmodified");
  }
  
  public static String getStatusStringFromMetadata(CrawlDatumAndMetadata metadata) { 
  	String status = CrawlDatum.getStatusName(metadata.getStatus()).substring("db_".length());
  	if (metadata.isFieldDirty(CrawlDatumAndMetadata.Field_PROTOCOLSTATUS)) { 
  		if (metadata.getProtocolStatus() > ProtocolStatus.SUCCESS) { 
  			String protocolStatus = codeToName.get((int)metadata.getProtocolStatus());
  			if (protocolStatus != null) { 
  				status += "-" + protocolStatus;
  			}
  		}
  	}
  	return status;
  }
  
  @SuppressWarnings("serial")
  public static class DomainDataQueryServlet extends HttpServlet { 
    
    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response)throws ServletException, IOException {
      
      LOG.info("Received Request:" +request.toString());
      
      String errorResult = null;

      
      try { 
        String requestType = request.getParameter("request_type");
        String domainName = request.getParameter("name");
        String renderType = request.getParameter("render_type");
        String domainIdParam   = request.getParameter("id");
        
        long domainId = -1;
        
        if (domainIdParam != null) { 
        	domainId = Long.parseLong(domainIdParam);
        }
          
        if (requestType.equalsIgnoreCase("domainStats")) { 

        	if (domainId == -1) { 
        		// get domain name and map it to a domain id 
        		domainId = _server.getDatabaseIndex().queryDomainIdGivenDomain(domainName);
        	}
        	
        	// get domain metadata 
        	SubDomainMetadata metadata = _server.getDatabaseIndex().queryDomainMetadataGivenDomainId(domainId);
        	
          if (metadata != null) { 
          
            OutputStream outStream = response.getOutputStream();
            PrintWriter writer = new PrintWriter(outStream);

            try {

              if (renderType == null || renderType.equalsIgnoreCase("x-json")) {
                response.setContentType("application/x-json");
              }
              else {
              	response.setContentType("text/plain");
              }
              	
                //response.setContentType("text/plain");
                
              
              writer.write("{\"name\":"+ quote(metadata.getDomainText()) +",");
              writer.write("\"urls\":"+ metadata.getUrlCount() +",");
              writer.write("\"fetched\":"+ metadata.getFetchedCount() +",");
              writer.write("\"gone\":"+ metadata.getGoneCount() +",");
              writer.write("\"redirectsPerm\":"+ metadata.getRedirectPermCount() +",");
              writer.write("\"redirectsTemp\":"+ metadata.getRedirectTemporaryCount() +",");
              writer.write("\"pageRank\":"+ metadata.getHasPageRankCount() +",");
              writer.write("\"arcfileInfo\":"+ metadata.getHasArcFileInfoCount() +",");
              writer.write("\"outlinkData\":"+ metadata.getHasLinkListCount() +",");
              writer.write("\"inlinkData\":"+ metadata.getHasInverseLinkListCount() +",");
              
              writer.write("}");
              writer.flush();
              outStream.close();
            }
            catch (IOException e) { 
            	errorResult = CCStringUtils.stringifyException(e); 
            	LOG.error(errorResult);
            	throw e;
            }
          }
          else {
          	errorResult = "Unable to locate metadata from Domain:" + domainName;
          	LOG.error(errorResult);
            throw new IOException(errorResult);
          }
        }
        else if (requestType.equalsIgnoreCase("urlList")) {

          // build domain query info 
          final DomainURLListQueryInfo queryInfo = new DomainURLListQueryInfo();
          
        	if (domainId == -1) { 
        		// get domain name and map it to a domain id 
        		domainId = _server.getDatabaseIndex().queryDomainIdGivenDomain(domainName);
        	}
          
        	// set it into the query info 
          queryInfo.setDomainId(domainId);
          
          // create query object ... 
          DomainURLListQuery query = new DomainURLListQuery(queryInfo);

          
          // initialize pagination info 
          int page_no = -1;
          int page_size = 0;
          String sortBy = "";
          String sortOrder = "";
          
          // initialize paging info 
          ClientQueryInfo clientQueryInfo = new ClientQueryInfo();
          
          if (request.getParameter("page_no") == null  
          		|| request.getParameter("page_size") == null 
          		|| request.getParameter("sort_by") == null
          		|| request.getParameter("sort_order") == null) { 
          	
          	errorResult = "Invalid Pagination Data";
          	LOG.error(errorResult);
            throw new IOException(errorResult);          	
          }

          page_no = Integer.parseInt(request.getParameter("page_no")) - 1;
          page_size = Math.min(Math.max(1,Integer.parseInt(request.getParameter("page_size"))),1000);
          sortBy = request.getParameter("sort_by");
          sortOrder = request.getParameter("sort_order");
          
          
          
          clientQueryInfo.setSortByField(sortBy);
          clientQueryInfo.setPageSize(page_size);
          clientQueryInfo.setPaginationOffset(page_no);
          
          if (sortOrder.equalsIgnoreCase("ASC"))
            clientQueryInfo.setSortOrder(ClientQueryInfo.SortOrder.ASCENDING);
          else 
            clientQueryInfo.setSortOrder(ClientQueryInfo.SortOrder.DESCENDING);

          // issue a blocking query request ... 
          BlockingQueryResult<URLFPV2, CrawlDatumAndMetadata> result = _server.blockingQueryRequest(query,clientQueryInfo);

          if (result.querySucceeded) { 
        
            OutputStream outStream = response.getOutputStream();
            PrintWriter writer = new PrintWriter(outStream);
  
            try {
  
              if (renderType != null && renderType.equalsIgnoreCase("x-json")) {
                response.setContentType("application/x-json");
              }
              else { 
              	response.setContentType("text/plain");
              }
              	
                
              writer.write("{\"total\":"+result.resultObject.getTotalRecordCount() +",");
              writer.write("\"page\":"+ (page_no + 1) +",");
              writer.write("\"rows\":[");
              int recordCount = 0;
              for (QueryResultRecord<URLFPV2,CrawlDatumAndMetadata> record : result.resultObject.getResults()) {
                if (recordCount++ != 0) 
                  writer.write(",");
                writer.write("{ cell:[");                  
                //URL
                writer.write(quote(record.getValue().getUrl())+",");
                //STATUS
                writer.write(quote(getStatusStringFromMetadata(record.getValue()))+",");
                // FETCHTIME
                if (record.getValue().getMetadata().isFieldDirty(CrawlURLMetadata.Field_LASTFETCHTIMESTAMP)) { 
                  writer.write(quote(new Date(record.getValue().getMetadata().getLastFetchTimestamp()).toString())+",");
                }
                else { 
                  writer.write(",");
                }
                // PAGE RANK
                writer.print(record.getValue().getMetadata().getPageRank());
                writer.write(",");
                //CRAWLNO
                writer.print(record.getValue().getMetadata().getCrawlNumber());
                writer.write(",");
                // PARSENO
                writer.print(record.getValue().getMetadata().getParseNumber());
                writer.write(",");
                // UPLOADNO
                writer.print(record.getValue().getMetadata().getUploadNumber());
                writer.write("]}\n");  
              }
              writer.write("]}");
              writer.flush();
              outStream.close();
            }
            catch (IOException e) { 
                errorResult = CCStringUtils.stringifyException(e);
                LOG.error(errorResult);
            }
          }
          else { 
            errorResult = result.errorString;
          }
        }
        
        
        if (errorResult != null) {
          
          response.setContentType("text/plain");
          OutputStream outStream = response.getOutputStream();
          PrintWriter writer = new PrintWriter(outStream);
          writer.write(errorResult);
          writer.close();
          outStream.flush();
        }
      }
      catch (Exception e) { 
        throw new IOException(CCStringUtils.stringifyException(e));
      }
    }
  }
  
  private static SimpleDateFormat S3_TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy/MM/dd/");

  private static String hdfsNameToS3ArcFileName(long arcFileDate,int arcFilePartNo) {
    
    String arcFileName = Long.toString(arcFileDate) + "_" +  arcFilePartNo + ".arc.gz"; 
    
    synchronized (S3_TIMESTAMP_FORMAT) {
      return S3_TIMESTAMP_FORMAT.format(new Date(arcFileDate))  +arcFilePartNo + "/" +  arcFileName;
    }
  }

  
  @SuppressWarnings("serial")
  public static class URLDetailServlet extends HttpServlet { 
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)throws ServletException, IOException {
      
      LOG.info("Received Request:" +request.toString());
      
      try { 
        String urlName = request.getParameter("name");
        
        
        if (urlName == null) { 
          throw new IOException("name parameter not specified!");
        }
        else { 
        	urlName = URLDecoder.decode(urlName,"UTF-8");
        }

        // compute fingerprint for given url 
        URLFPV2 fingerprint = URLUtils.getURLFPV2FromURL(urlName);
        
        // ok if null error out 
        if (fingerprint == null) { 
        	throw new IOException("Invalid URL:" + urlName);
        }
        
        String renderType = request.getParameter("render_type");
        
        try {
          
        	// ok query the master index for the metadata related to the url 
        	MetadataOut metadataOut = _server.getDatabaseIndex().queryMetadataAndURLGivenFP(fingerprint);
        	
        	//TODO:HACK - REMOVE LATER!!!
        	if (metadataOut == null) { 
        		HackMetadataOut hackedMetadata  = hackTryAlternativeRouteToGetMetadata(urlName,fingerprint);
        		if (hackedMetadata != null) { 
        			metadataOut = hackedMetadata.metadataOut;
        			urlName = hackedMetadata.alternativeURL;
        		}
        	}
        	
          if (metadataOut != null && metadataOut.datumAndMetadataBytes.getLength() != 0) {
            
          	DataInputBuffer readerStream = new DataInputBuffer();
          	CrawlDatumAndMetadata realMetadataObject = new CrawlDatumAndMetadata();
          	readerStream.reset(metadataOut.datumAndMetadataBytes.getBytes(),metadataOut.datumAndMetadataBytes.getOffset(),metadataOut.datumAndMetadataBytes.getLength());
          	realMetadataObject.readFields(readerStream);
          	
            OutputStream outStream;
            
            try {
              outStream = response.getOutputStream();
              PrintWriter writer = new PrintWriter(outStream);
              
              if (renderType != null && renderType.equalsIgnoreCase("x-json")) { 
                response.setContentType("application/x-json");
              }
              else { 
              	response.setContentType("text/plain");
              }
              	
              writer.write("{\n");

              
              // DOMAIN HASH 
              writer.write("domainHash:");
              writer.write(quote(Long.toString(fingerprint.getDomainHash()))+"\n,");
              
              // FINGERPRINT 
              writer.write("urlHash:");
              writer.write(quote(Long.toString(fingerprint.getUrlHash()))+"\n,");
              
              // CANONICAL URL 
              writer.write("canonicalURL:");
              writer.write(quote(urlName)+"\n,");
              
              
              //STATUS
              writer.write("status:");
              writer.write(quote(getStatusStringFromMetadata(realMetadataObject))+"\n,");
              
              //CONTENT LOCATION 
              //writer.write("contentLocation:");
              //writer.write(quote(data.getMetadata().getContentFileNameAndPos())+"\n,");
              
              writer.write("hasArcFileData:");
              writer.print(realMetadataObject.getMetadata().getArchiveInfo().size() != 0 ? 1 : 0 ); 
              writer.write("\n,");	
              
              if (realMetadataObject.getMetadata().getArchiveInfo().size() != 0) { 
              
                Collections.sort(realMetadataObject.getMetadata().getArchiveInfo(), new Comparator<ArchiveInfo> () {

                  @Override
                  public int compare(ArchiveInfo o1, ArchiveInfo o2) {
                    return (o1.getArcfileDate() < o2.getArcfileDate()) ? -1 : (o1.getArcfileDate() > o2.getArcfileDate()) ? 1 : 0; 
                  } 
                  
                });
                
                ArchiveInfo info = realMetadataObject.getMetadata().getArchiveInfo().get(realMetadataObject.getMetadata().getArchiveInfo().size()-1);
                
               
                //ARC FILE DATE
                writer.write("arcFileDate:");
                writer.print(info.getArcfileDate());
                writer.write("\n,");
                
                //ARC FILE INDEX
                writer.write("arcFileIndex:");
                writer.print(info.getArcfileDate());
                writer.write("\n,");
              
                //ARC FILE OFFSET
                writer.write("arcFileOffset:");
                writer.print(info.getArcfileOffset());
                writer.write("\n,");
                
                // ARC FILE PATH 
                writer.write("arcFilePath:");
                writer.print(quote(hdfsNameToS3ArcFileName(info.getArcfileDate(), info.getArcfileIndex())));
                writer.write("\n,");
                
                // ARC FILE SIZE 
                writer.write("arcFileCompressedSize:");
                writer.print(info.getCompressedSize());
                writer.write("\n,");                
              }
              

              //CONTENT TYPE 
              writer.write("contentType:");
              writer.write("\"" + realMetadataObject.getMetadata().getContentType() + "\"");
              writer.write("\n,");

              
              writer.write("hasFetchMetadata:");
              writer.print(realMetadataObject.getMetadata().isFieldDirty(CrawlURLMetadata.Field_LASTFETCHTIMESTAMP) ? 1 : 0 ); 
              writer.write("\n,");
              
              if (realMetadataObject.getMetadata().isFieldDirty(CrawlURLMetadata.Field_LASTFETCHTIMESTAMP)) { 
	              // FETCHTIME
	              writer.write("lastFetchTime:");
	              writer.print(realMetadataObject.getMetadata().getLastFetchTimestamp());
	              writer.write("\n,");
	              
	              //LAST FETCH SIZE  
	              writer.write("lastFetchSize:");
	              writer.print(realMetadataObject.getMetadata().getLastFetchSize());
	              writer.write("\n,");
              }
              
              // PAGE RANK
              writer.write("pageRank:");
              writer.print(realMetadataObject.getMetadata().getPageRank());
              writer.write("\n,");
              
              if (realMetadataObject.getRedirectLocation().length() !=0) { 
              	writer.write("RedirectLocation:");
              	writer.print(quote(realMetadataObject.getRedirectLocation()));
              	writer.write("\n,");

              	
              	String hostName = URLUtils.fastGetHostFromURL(realMetadataObject.getRedirectLocation());
              	if (hostName == null)
              		hostName = "";
              	
              	writer.write("RedirectDomain:");
              	writer.print(quote(hostName));
              	writer.write("\n,");
              	
              }

              if (realMetadataObject.getModifiedTime() > 0) { 
                writer.write("metadataLastModified:");
                writer.print(realMetadataObject.getModifiedTime()); 
                writer.write("\n,");	
              }
              
              if (realMetadataObject.getMetadata().isFieldDirty(CrawlURLMetadata.Field_HTTPDATE)) { 
                writer.write("httpDate:");
                writer.print(realMetadataObject.getMetadata().getHttpDate()); 
                writer.write("\n,");	
              }

              if (realMetadataObject.getMetadata().isFieldDirty(CrawlURLMetadata.Field_LASTMODIFIEDTIME)) { 
                writer.write("httpLastModified:");
                writer.print(realMetadataObject.getMetadata().getLastModifiedTime()); 
                writer.write("\n,");	
              }
              
              if (realMetadataObject.getMetadata().isFieldDirty(CrawlURLMetadata.Field_MAXAGE)) { 
                writer.write("maxAge:");
                writer.print(realMetadataObject.getMetadata().getMaxAge()); 
                writer.write("\n,");	
              }

              if (realMetadataObject.getMetadata().isFieldDirty(CrawlURLMetadata.Field_EXPIRES)) { 
                writer.write("httpExpires:");
                writer.print(realMetadataObject.getMetadata().getExpires()); 
                writer.write("\n,");	
              }

              if (realMetadataObject.getMetadata().isFieldDirty(CrawlURLMetadata.Field_ETAG)) { 
                writer.write("httpETag:");
                writer.print(quote(realMetadataObject.getMetadata().getETag())); 
                writer.write("\n,");	
              }

              if (realMetadataObject.getMetadata().isFieldDirty(CrawlURLMetadata.Field_CACHECONTROLFLAGS) && realMetadataObject.getMetadata().getCacheControlFlags() != 0 ) { 
                writer.write("httpCacheFlags:");
                String flags = "";
                if ((CrawlURLMetadata.CacheControlFlags.NO_CACHE & realMetadataObject.getMetadata().getCacheControlFlags()) != 0) {
                	flags += "nocache ";
                }
                if ((CrawlURLMetadata.CacheControlFlags.NO_STORE & realMetadataObject.getMetadata().getCacheControlFlags()) != 0) {
                	flags += "nostore ";
                }
                if ((CrawlURLMetadata.CacheControlFlags.VARY & realMetadataObject.getMetadata().getCacheControlFlags()) != 0) {
                	flags += "vary ";
                }
                if ((CrawlURLMetadata.CacheControlFlags.MUST_REVALIDATE & realMetadataObject.getMetadata().getCacheControlFlags()) != 0) {
                	flags += "must_revalidate ";
                }
                if ((CrawlURLMetadata.CacheControlFlags.PRIVATE & realMetadataObject.getMetadata().getCacheControlFlags()) != 0) {
                	flags += "private ";
                }
                writer.print(quote(flags)); 
                writer.write("\n,");	
              }
              
              writer.write("hasOutlinkData:");
              writer.print(realMetadataObject.getMetadata().isFieldDirty(CrawlURLMetadata.Field_LINKDBOFFSET) ? 1 : 0 ); 
              writer.write("\n,");	
              
              if (realMetadataObject.getMetadata().isFieldDirty(CrawlURLMetadata.Field_LINKDBOFFSET)) { 
	              // LINKDBTIMESTAMP
	              writer.write("linkdbTS:");
	              writer.print(realMetadataObject.getMetadata().getLinkDBTimestamp());
	              writer.write("\n,");
	
	              // LINKDBTIMEFILENO
	              writer.write("linkdbFileNo:");
	              writer.print(realMetadataObject.getMetadata().getLinkDBFileNo());
	              writer.write("\n,");
	              
	              // LINKDBPOFFSET
	              writer.write("linkdbOffset:");
	              writer.print(realMetadataObject.getMetadata().getLinkDBOffset());
	              writer.write("\n,");
              }
              
              writer.write("hasInlinkData:");
              writer.print(realMetadataObject.getMetadata().isFieldDirty(CrawlURLMetadata.Field_INVERSEDBOFFSET) ? 1 : 0 ); 
              writer.write("\n");	

              if (realMetadataObject.getMetadata().isFieldDirty(CrawlURLMetadata.Field_INVERSEDBOFFSET)) { 
              	writer.write(",");
	              // INVLINKDBTIMESTAMP
	              writer.write("InverseLinkdbTS:");
	              writer.print(realMetadataObject.getMetadata().getInverseDBTimestamp());
	              writer.write("\n,");
	
	              // LINKDBTIMEFILENO
	              writer.write("InverseLinkdbFileNo:");
	              writer.print(realMetadataObject.getMetadata().getInverseDBFileNo());
	              writer.write("\n,");
	              
	              // LINKDBPOFFSET
	              writer.write("InverseLinkdbOffset:");
	              writer.print(realMetadataObject.getMetadata().getInverseDBOffset());
              }
              
              
              writer.write("}\n");
              writer.flush();

              outStream.close();
            }
            catch (IOException e) { 
              LOG.error(CCStringUtils.stringifyException(e));
            }
          }
          else { 
            
            OutputStream outStream;
            try {
              outStream = response.getOutputStream();
              PrintWriter writer = new PrintWriter(outStream);

              response.setContentType("text/plain");

              writer.append("NO METADATA AVAILABLE!:\n");
              
              writer.flush();
              outStream.close();
            }
            catch (IOException e) { 
              LOG.error(CCStringUtils.stringifyException(e));
            }
          }
          
        } catch (IOException e) {
          LOG.error("Query Failed with Exception:" + CCStringUtils.stringifyException(e));
          throw e;
        }
      }
      catch (Exception e) { 
        throw new IOException(CCStringUtils.stringifyException(e));
      }
    }
  }
  
  @SuppressWarnings("serial")
  public static class LinkDetailsServlet extends HttpServlet { 
      
    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response)throws ServletException, IOException {
      
      LOG.info("Received Request:" +request.toString());
      
      String errorResult = null;

      try { 
        String urlName = request.getParameter("name");
        String renderType = request.getParameter("render_type");
        String queryTypeStr  = request.getParameter("query_type");
        
      	int queryType = -1;
      	
      	if (queryTypeStr != null) { 
	      	if (queryTypeStr.equalsIgnoreCase(URLLinkDetailQueryInfo.QueryType.toString(URLLinkDetailQueryInfo.QueryType.LINKS_QUERY))) { 
	      		queryType = URLLinkDetailQueryInfo.QueryType.LINKS_QUERY;
	      	}
	      	else if (queryTypeStr.equalsIgnoreCase(URLLinkDetailQueryInfo.QueryType.toString(URLLinkDetailQueryInfo.QueryType.INVERSE_QUERY))) { 
	      		queryType = URLLinkDetailQueryInfo.QueryType.INVERSE_QUERY;
	      	}
	      	else if (queryTypeStr.equalsIgnoreCase(URLLinkDetailQueryInfo.QueryType.toString(URLLinkDetailQueryInfo.QueryType.INVERSE_BY_DOMAIN_QUERY))) {
	      		queryType = URLLinkDetailQueryInfo.QueryType.INVERSE_BY_DOMAIN_QUERY;
	      	}
	      	else if (queryTypeStr.equalsIgnoreCase(URLLinkDetailQueryInfo.QueryType.toString(URLLinkDetailQueryInfo.QueryType.INVERSE_BY_DOMAIN_DETAIL_QUERY))) { 
	      		queryType = URLLinkDetailQueryInfo.QueryType.INVERSE_BY_DOMAIN_DETAIL_QUERY;
	      	}
      	}
        
        // build domain query info 
        final URLLinkDetailQueryInfo queryInfo = new URLLinkDetailQueryInfo();
  
        // set query type 
        queryInfo.setQueryType(queryType);
  
        if (urlName == null || urlName.length() ==0 || queryType == -1) {
          LOG.error("urlName:" + urlName + " query_type:" + queryTypeStr);
        	response.setStatus(404);
          return;
        }
        
        urlName = URLDecoder.decode(urlName,"UTF-8");
        
      	URLFPV2 fingerprint = URLUtils.getURLFPV2FromURL(urlName);
      	
      	if (fingerprint == null) { 
      		throw new IOException("Invalid URL Passed Into Query: " + urlName);
      	}
      	
        queryInfo.setTargetURLFP(fingerprint);            

        
        if (queryType != URLLinkDetailQueryInfo.QueryType.INVERSE_BY_DOMAIN_DETAIL_QUERY) {
        	
  	      // we need metadata location information ... 
        	
          // check to see the link db information is present in the request 
          String linkdbTS = request.getParameter("linkdbTS");
          String linkdbFileNo = request.getParameter("linkdbFileNo");
          String linkdbOffset = request.getParameter("linkdbOffset");
          
	        if (linkdbTS == null || linkdbFileNo == null || linkdbOffset == null) {
	          //LOG.info("LinkDB Location Hint not present in query. Issuing URL Detail Query to obtain information");
	          // need to query the local master index for the metadata 
	          
	        	MetadataOut metadataOut = _server.getDatabaseIndex().queryMetadataAndURLGivenFP(fingerprint);
	        	//TODO: HACK - REMOVE LATER !!!
	        	if (metadataOut == null) { 
          		HackMetadataOut hacked = hackTryAlternativeRouteToGetMetadata(urlName,fingerprint);
          		if (hacked != null) { 
          			metadataOut = hacked.metadataOut;
          		}
	        	}
	        	
	          if (metadataOut == null || metadataOut.datumAndMetadataBytes.getLength() == 0) {
	            //LOG.info("Unable to Obtain URLDetail for source url:" + urlName);
	            response.setStatus(404);
	            return;
	          }
	          else { 
	
	          	DataInputBuffer inputBuffer = new DataInputBuffer();
	          	inputBuffer.reset(
	          			metadataOut.datumAndMetadataBytes.getBytes(),
	          			metadataOut.datumAndMetadataBytes.getOffset(),
	          			metadataOut.datumAndMetadataBytes.getLength());
	          	
	          	CrawlDatumAndMetadata metadata = new CrawlDatumAndMetadata();
	          	metadata.readFields(inputBuffer);
	          	
	          	if (queryType == URLLinkDetailQueryInfo.QueryType.LINKS_QUERY) { 
		            if (metadata.getMetadata().isFieldDirty(CrawlURLMetadata.Field_LINKDBFILENO)) { 
		              linkdbTS = Long.toString(metadata.getMetadata().getLinkDBTimestamp());
		              linkdbFileNo = Integer.toString(metadata.getMetadata().getLinkDBFileNo());
		              linkdbOffset = Long.toString(metadata.getMetadata().getLinkDBOffset());
		            }
	          	}
	          	else { 
		            if (metadata.getMetadata().isFieldDirty(CrawlURLMetadata.Field_INVERSEDBFILENO)) { 
		              linkdbTS = Long.toString(metadata.getMetadata().getInverseDBTimestamp());
		              linkdbFileNo = Integer.toString(metadata.getMetadata().getInverseDBFileNo());
		              linkdbOffset = Long.toString(metadata.getMetadata().getInverseDBOffset());
		            }
	          	}
	          }
	        }
	        
	        if (linkdbTS == null || linkdbFileNo == null || linkdbOffset == null) {
	          LOG.error("No LinkDB Information found for URL:" + urlName);
	          response.setStatus(404);
	          return;
	        }
	        
	        queryInfo.setLinkDBTS(Long.parseLong(linkdbTS));
	        queryInfo.setLinkDBFileNo(Integer.parseInt(linkdbFileNo));
	        queryInfo.setLinkDBOffset(Long.parseLong(linkdbOffset));
	        
        }
        else if (queryType == URLLinkDetailQueryInfo.QueryType.INVERSE_BY_DOMAIN_DETAIL_QUERY) {
        	
        	String urlCount = request.getParameter("urlCount");
        	String urlDataOffset = request.getParameter("urlDataOffset");
        	
	        if (urlCount == null || urlDataOffset == null) {
	          LOG.error("No Inverse Link Metadata Found for:" + urlName);
	          response.setStatus(404);
	          return;
	        }

	        queryInfo.setInlinkDomainURLCount(Integer.parseInt(urlCount));
	        queryInfo.setUrlDataOffset(Long.parseLong(urlDataOffset));
        }

        //LOG.info("URLFP is:" + searchFingerprint.getUrlHash());
        

        // initialize paging info 
        ClientQueryInfo clientQueryInfo = new ClientQueryInfo();
        
        int page_no = Integer.parseInt(request.getParameter("page_no")) - 1;
        int page_size = Integer.parseInt(request.getParameter("page_size"));
        String sortBy = request.getParameter("sort_by");
        String sortOrder = request.getParameter("sort_order");
        if (sortOrder == null) { 
        	sortOrder="ASC";
        }
        
        clientQueryInfo.setSortByField(sortBy);
        clientQueryInfo.setPageSize(page_size);
        clientQueryInfo.setPaginationOffset(page_no);
        
        if (sortOrder.equalsIgnoreCase("ASC"))
          clientQueryInfo.setSortOrder(ClientQueryInfo.SortOrder.ASCENDING);
        else 
          clientQueryInfo.setSortOrder(ClientQueryInfo.SortOrder.DESCENDING);
       
        // allocate query object ... 
        URLLinksQuery query = new URLLinksQuery(queryInfo);
        
        //LOG.info("Starting Blocking URLLinksQuery request");
        // and send it through .... 
        BlockingQueryResult<Writable,Writable> result = _server.blockingQueryRequest(query,clientQueryInfo);
        
        if (result.querySucceeded) { 

      		try {
	        	if (queryType == URLLinkDetailQueryInfo.QueryType.LINKS_QUERY 
	        			|| queryType == URLLinkDetailQueryInfo.QueryType.INVERSE_QUERY) { 
        	
	        		produceLinksQueryResults(request,response,renderType,result,page_no);
	        		
	        	}
	        	else if (queryType == URLLinkDetailQueryInfo.QueryType.INVERSE_BY_DOMAIN_QUERY) { 
	        		produceInverseLinksByDomainQueryResults(request,response,renderType,result,page_no);
	        	}
	        	else if (queryType == URLLinkDetailQueryInfo.QueryType.INVERSE_BY_DOMAIN_DETAIL_QUERY) { 
	        		produceLinksQueryResults(request,response,renderType,result,page_no);
	        	}
      		}
          catch (IOException e) { 
            errorResult = CCStringUtils.stringifyException(e);
            LOG.error(errorResult);
          }
        }
        else { 
          LOG.error("Blocking URLLinksQuery request failed");
          errorResult = result.errorString;
        }
      }
      catch (Exception e) { 
        errorResult = CCStringUtils.stringifyException(e);        
      }
      if (errorResult != null) {
        
        LOG.error("LinkDetailQuery failed with error:" + errorResult);
        response.sendError(500, errorResult);
      }
    }

		private void produceInverseLinksByDomainDetailQueryResults(
        HttpServletRequest request, HttpServletResponse response,
        String renderType, BlockingQueryResult<Writable, Writable> result,
        int pageNo)throws IOException {
			
						
	    
    }

		private void produceInverseLinksByDomainQueryResults(
        HttpServletRequest request, HttpServletResponse response,
        String renderType, BlockingQueryResult<Writable, Writable> result,
        int page_no)throws IOException {
	    
	    OutputStream outStream = response.getOutputStream();
	    PrintWriter writer = new PrintWriter(outStream);

	    if (renderType != null && renderType.equalsIgnoreCase("x-json")) {
	      response.setContentType("application/x-json");
	    }
	    else { 
	      response.setContentType("text/plain");
	    }
	      
	    writer.write("{\"total\":"+result.resultObject.getTotalRecordCount() +",");
	    writer.write("\"page\":"+ (page_no + 1) +",");
	    writer.write("\"rows\":[");
	    int recordCount = 0;
	    
	    for (QueryResultRecord<Writable,Writable> record : result.resultObject.getResults()) {
	    	
	      if (recordCount++ != 0) 
	        writer.write(",");
	      writer.write("{ cell:[");                  
	      //URL
	      writer.write(quote(record.getKey().toString())+",");
	      InlinkingDomainInfo domainInfo = (InlinkingDomainInfo)record.getValue();
	      
	      writer.write(domainInfo.getUrlCount()+",");
	      writer.write(Long.toString(domainInfo.getUrlDataPos()));

	      writer.write("]}\n");  
	    }
	    writer.write("]}");
	    writer.flush();
	    outStream.close();	    
    }
  }

  private static void produceLinksQueryResults(final HttpServletRequest request, final HttpServletResponse response,String renderType,BlockingQueryResult<Writable,Writable> result,int page_no)throws IOException { 
    //LOG.info("Blocking URLLinksQuery request succeeded");
    
    OutputStream outStream = response.getOutputStream();
    PrintWriter writer = new PrintWriter(outStream);

    if (renderType != null && renderType.equalsIgnoreCase("x-json")) {
      response.setContentType("application/x-json");
    }
    else { 
      response.setContentType("text/plain");
    }
      
    writer.write("{\"total\":"+result.resultObject.getTotalRecordCount() +",");
    writer.write("\"page\":"+ (page_no + 1) +",");
    writer.write("\"rows\":[");
    int recordCount = 0;
    
    for (QueryResultRecord<Writable,Writable> record : result.resultObject.getResults()) {
    	
      if (recordCount++ != 0) 
        writer.write(",");
      writer.write("{ cell:[");                  
      //URL
      writer.write(quote(((CrawlDatumAndMetadata)record.getValue()).getUrl())+",");
      
      float pageRank = ((CrawlDatumAndMetadata)record.getValue()).getMetadata().getPageRank();
      // PAGE RANK
      writer.print(pageRank);
      writer.print(",");
       
      // WRITE STATUS 
      writer.print(quote(getStatusStringFromMetadata(((CrawlDatumAndMetadata)record.getValue()))));
      writer.print(",");

      //WRITE DOMAIN NAME 
      String hostName = URLUtils.fastGetHostFromURL(((CrawlDatumAndMetadata)record.getValue()).getUrl());
      
    	writer.print(quote((hostName!=null? hostName : "")));
      

      writer.write("]}\n");  
    }
    writer.write("]}");
    writer.flush();
    outStream.close();
  	
  }
    
  public static String reverseCanonicalizeURL(GoogleURL urlObject)throws MalformedURLException {

    StringBuilder urlOut = new StringBuilder();
    
    urlOut.append(urlObject.getScheme());
    urlOut.append("://");
    
    if (urlObject.getUserName() != GoogleURL.emptyString) { 
      urlOut.append(urlObject.getUserName());
      if (urlObject.getPassword() != GoogleURL.emptyString) { 
        urlOut.append(":");
        urlOut.append(urlObject.getPassword());
      }
      urlOut.append("@");
    }
    
    String host = urlObject.getHost();
    if (host.endsWith(".")) { 
    	host = host.substring(0,host.length() -1);
    }
    
  	if (!host.startsWith("www.")) {
  		// ok now. one nasty hack ... :-(
  		// if root name is null or root name does not equal full host name ...  
  		String rootName = URLUtils.extractRootDomainName(host);
  		if (rootName != null && rootName.equals(host)) { 
  			// striping the www. prefix  
  			host = "www." + host;
  		}
  	}
    urlOut.append(host);
    
    if (urlObject.getPort() != GoogleURL.emptyString && !urlObject.getPort().equals("80")) {
      urlOut.append(":");
      urlOut.append(urlObject.getPort());
    }
    if (urlObject.getPath() != GoogleURL.emptyString) { 
    	int indexOfSemiColon = urlObject.getPath().indexOf(';');
    	if (indexOfSemiColon != -1) { 
    		urlOut.append(urlObject.getPath().substring(0,indexOfSemiColon));
    	}
    	else { 
    		urlOut.append(urlObject.getPath());
    	}
    }
    if (urlObject.getQuery() != GoogleURL.emptyString) { 
      urlOut.append("?");
      urlOut.append(urlObject.getQuery());
    }
    
    String canonicalizedURL = urlOut.toString();
        
    return canonicalizedURL;
  }

  static class HackMetadataOut {
  	
  	HackMetadataOut(String alternativeURL,MetadataOut metadataOut) { 
  		this.alternativeURL = alternativeURL;
  		this.metadataOut = metadataOut;
  	}
  	
  	String alternativeURL = null;
  	MetadataOut metadataOut = null; 
  }
  public static HackMetadataOut hackTryAlternativeRouteToGetMetadata(String urlName,URLFPV2 fingerprint)throws IOException {
    // ok lookup metadata given index 
  	MetadataOut metadataOut = null;

		// try canonical form with leading www stripped :-(
		String canonicalURL = URLUtils.canonicalizeURL(urlName, true);
		// alternative fp ... 
		URLFPV2 canonicalFP = URLUtils.getURLFPV2FromCanonicalURL(canonicalURL);
		
		if (canonicalFP != null && canonicalFP.compareTo(fingerprint) != 0) { 
			// try retrieving metadata from this version 
			metadataOut = _server.getDatabaseIndex().queryMetadataAndURLGivenFP(canonicalFP);
			
			if (metadataOut != null) { 
				return new HackMetadataOut(canonicalURL, metadataOut);
			}
		}
		
		// ok if metadata still is bad ... 
		if (metadataOut == null) {
			GoogleURL urlObject = new GoogleURL(urlName);
			String reverseCanonical = reverseCanonicalizeURL(urlObject);
			if (reverseCanonical.compareTo(canonicalURL) != 0) { 
				URLFPV2 reverseCanonicalFP = URLUtils.getURLFPV2FromCanonicalURL(reverseCanonical);
    		if (reverseCanonicalFP != null && reverseCanonicalFP.compareTo(fingerprint) != 0) { 
    			// try retrieving metadata from this version 
    			metadataOut = _server.getDatabaseIndex().queryMetadataAndURLGivenFP(reverseCanonicalFP);
    			
    			if (metadataOut != null) { 
    				new HackMetadataOut(reverseCanonical, metadataOut);
    			}
    		}
				
			}
		}
		return null;
  }
  
  @SuppressWarnings("serial")
  public static class URLContentServlet extends HttpServlet { 

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)throws ServletException, IOException {
      
      LOG.info("Received Request:" +request.toString());
      
      try { 
        String urlName = request.getParameter("name");
        
        if (urlName == null) { 
          throw new IOException("name parameter not specified!");
        }
        
        // ok parse the url 
        URLFPV2 fingerprint = URLUtils.getURLFPV2FromURL(urlName);
        
        if (fingerprint == null) { 
        	throw new IOException("Invalid URL:" + urlName);
        }
        
        // ok lookup metadata given index 
        MetadataOut metadataOut = _server.getDatabaseIndex().queryMetadataAndURLGivenFP(fingerprint);
        
      	//TODO:HACK - REMOVE LATER!!!
      	if (metadataOut == null) {
      		HackMetadataOut hacked = hackTryAlternativeRouteToGetMetadata(urlName,fingerprint);
      		if (hacked != null) { 
      			metadataOut = hacked.metadataOut;
      		}
      	}
        
        if (metadataOut == null || metadataOut.datumAndMetadataBytes.getLength() == 0) { 
        	response.sendError(404);
        	return;
        }

      	DataInputBuffer inputBuffer = new DataInputBuffer();
      	inputBuffer.reset(
      			metadataOut.datumAndMetadataBytes.getBytes(),
      			metadataOut.datumAndMetadataBytes.getOffset(),
      			metadataOut.datumAndMetadataBytes.getLength());
      	
      	CrawlDatumAndMetadata metadata = new CrawlDatumAndMetadata();
      	metadata.readFields(inputBuffer);
      	
      	if (metadata.getMetadata().getArchiveInfo().size() == 0) { 
        	response.sendError(404);
        	return;
      	}
      	
  			ArchiveInfo infoToUse = null;
  			for (ArchiveInfo info : metadata.getMetadata().getArchiveInfo()) {
  				LOG.info("***Found INFO:" + info.getArcfileDate()+"-"+info.getArcfileIndex()+"-"+info.getArcfileOffset());
  				if (infoToUse == null || infoToUse.getArcfileDate() < info.getArcfileDate()) { 
  					infoToUse = info;
  				}
  			}
        
  			if (infoToUse == null) { 
  				response.sendError(404);
  				return;
  			}
  			
        try {
          
    			ArcFileItem item = S3Helper.retrieveArcFileItem(infoToUse, _server.getEventLoop());
          
          if (item != null) {

          	// write out headers 
          	for (ArcFileHeaderItem headerItem : item.getHeaderItems()) {
          		if (headerItem.getItemKey().length() != 0) {
          			if (headerItem.getItemKey().equalsIgnoreCase("content-encoding") || headerItem.getItemKey().equalsIgnoreCase("transfer-encoding")
          					|| headerItem.getItemKey().equalsIgnoreCase("content-length")
          					) { 
          				LOG.info("*** Skipping Content Encoding Header Field:" + headerItem.getItemValue());
          			}
          			else { 
          				response.addHeader(headerItem.getItemKey(),headerItem.getItemValue());
          			}
          		}
          	}
          	
          	response.addHeader("content-length", Integer.toString(item.getContent().getCount()));
          	
          	OutputStream outStream;
            
            try {
              outStream = response.getOutputStream();
              outStream.write(item.getContent().getReadOnlyBytes(),item.getContent().getOffset(),item.getContent().getCount());

              /*
              writer.println("Item:" + item.getUri());
              writer.println("Uncompressed Size:" + item.getContent().getCount());
              writer.println("***** Header Data:");
              for (ArcFileHeaderItem headerItem : item.getHeaderItems()) { 
                writer.println("Header Item:" + headerItem.getItemKey() + " Value:" + headerItem.getItemValue());
              }
              
              
              NIOHttpHeaders headers = ArcFileItemUtils.buildHeaderFromArcFileItemHeaders(item.getHeaderItems());
              CrawlURLMetadata metadataTemp = new CrawlURLMetadata();
              HttpHeaderInfoExtractor.parseHeaders(headers, metadataTemp);
              
              String charset = metadataTemp.getCharset();
              
              if (charset.length() !=0 ) { 
                writer.println("***** Charset(via HttpHeaders):" + charset);
              }
              else { 
                charset = CharsetUtils.sniffCharacterEncoding(item.getContent().getReadOnlyBytes());
                if (charset != null) { 
                  writer.println("***** Charset(via HTML MetaTag):" + charset);
                }
              }
              if (charset == null || charset.length() == 0) { 
                charset = "ASCII";
                writer.println("***** Charset(NotFount-UsingDefault):ASCII");                
              }
              

              Charset charsetObj = Charset.forName(charset);
              
              if (charsetObj == null) { 
                writer.println("***** Could Not Create CharsetDecoder for charset:" + charset);
                LOG.info("Unable to create Charsetcharset. Using ASCII");
                charsetObj = Charset.forName("ASCII");
              }
              
              writer.println("***** Content:");
              
              BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(item.getContent().getReadOnlyBytes(),0,item.getContent().getCount()),charsetObj));
              
              try { 
                String line = null;
                while ((line = bufferedReader.readLine()) != null) { 
                  writer.println(line);
                }
              }
              finally { 
                bufferedReader.close();
              }
              writer.flush();
              */
              outStream.flush();
              outStream.close();
            }
            catch (IOException e) { 
              LOG.error(CCStringUtils.stringifyException(e));
              throw e;
            }
          }
          else { 
          	response.sendError(404);
          	return;
          }
        } catch (IOException e) {
          LOG.error("Query Failed with Exception:" + CCStringUtils.stringifyException(e));
          throw e;
        }
      }
      catch (Exception e) { 
        throw new IOException(CCStringUtils.stringifyException(e));
      }
    }
  }
  
	private static long findLatestDatabaseTimestamp(Path rootPath)throws IOException { 
		FileSystem fs = CrawlEnvironment.getDefaultFileSystem();
		
		FileStatus candidates[] = fs.globStatus(new Path(rootPath,"*"));
		
		long candidateTimestamp = -1L;
		
		for (FileStatus candidate : candidates) {
			LOG.info("Found Seed Candidate:" + candidate.getPath());
			long timestamp = Long.parseLong(candidate.getPath().getName());
			if (candidateTimestamp == -1 || candidateTimestamp < timestamp) { 
				candidateTimestamp = timestamp;
			}
		}
		LOG.info("Selected Candidate is:"+ candidateTimestamp);
		return candidateTimestamp;
	}

  
  @SuppressWarnings("serial")
  public static class CrawlListServlet extends HttpServlet {
  	
	  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
	  static {
	    NUMBER_FORMAT.setMinimumIntegerDigits(5);
	    NUMBER_FORMAT.setGroupingUsed(false);
	  } 

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)throws ServletException, IOException {
      
    	// hack 
    	String crawlerNames[] = { 
    			"ccc01-01",
    			"ccc01-02",
    			"ccc02-01",
    			"ccc02-02",
    			"ccc03-01",
    			"ccc03-02",
    			"ccc04-01",
    			"ccc04-02"    			
    	};
    	
      LOG.info("Received Request:" +request.toString());
      
      try { 
        String domainName = request.getParameter("name");
        
        
        if (domainName == null) { 
          throw new IOException("name parameter not specified!");
        }
        else { 
        	domainName = URLDecoder.decode(domainName,"UTF-8");
        }

        // compute fingerprint for given url 
        long domainId = _server.getDatabaseIndex().queryDomainIdGivenDomain(domainName);
        
        // figure out shard id ... 
        //TODO: FIX HACKED SHARD EXTRACTION
        int shardId = (MurmurHash.hashLong(domainId,1) & Integer.MAX_VALUE) % CrawlEnvironment.NUM_DB_SHARDS;
        
        int   crawlerIndex = Math.abs((int)(domainId  % crawlerNames.length));
        
        // default generator path 
        Path generatorPath = new Path("crawl/generator/prgenerator");
        //get latest timestamp 
        long timestamp = findLatestDatabaseTimestamp(generatorPath);
        if (timestamp == -1) { 
        	throw new IOException("Timestamp Not Found!");
        }
        // build path to sharded data ...
        Path shardedDataLocation = new Path("crawl/generator/prgenerator/" + timestamp + "/analysis/part-" + NUMBER_FORMAT.format(shardId));
        LOG.info("Looking Up CrawlAnalysis File At:" + shardedDataLocation);
        
        
        response.setContentType("text/plain");
        PrintWriter writer = new PrintWriter(response.getWriter());
        URLFPV2 queryFP = new URLFPV2();
        
        queryFP.setDomainHash(domainId);
        
        // ok open file 
        FSDataInputStream indexInputStream = CrawlEnvironment.getDefaultFileSystem().open(shardedDataLocation);
        
				try { 
    			TFile.Reader reader = new TFile.Reader(indexInputStream,CrawlEnvironment.getDefaultFileSystem().getFileStatus(shardedDataLocation).getLen(),CrawlEnvironment.getHadoopConfig());
    			try { 
    				TFile.Reader.Scanner scanner = reader.createScanner();

    				try { 
	    				DataOutputBuffer keyBuffer = new DataOutputBuffer();
	    				keyBuffer.writeLong(domainId);
	  					if (scanner.seekTo(keyBuffer.getData(),0,keyBuffer.getLength())) {
	  						DataInputStream valueStream = scanner.entry().getValueStream();
	  						
	  						TreeMap<Integer,MetadataOut> sortedMap = new TreeMap<Integer,MetadataOut>();
	  						
	  						while (valueStream.available() != 0) { 
	  							int position = WritableUtils.readVInt(valueStream);
	  							long urlHash = valueStream.readLong();
	  							
	  							queryFP.setUrlHash(urlHash);
	  							
	  							MetadataOut metadata = _server.getDatabaseIndex().queryMetadataAndURLGivenFP(queryFP);
	  							sortedMap.put(position, metadata);
	  						}
	  						
  							for (Map.Entry<Integer,MetadataOut> entry : sortedMap.entrySet()) { 
	  							writer.print("Queue: [" + crawlerNames[crawlerIndex] +  "]POS:" + entry.getKey());
  								writer.print(" ,URL:" + entry.getValue().url.toString());
  								writer.print(" ,PageRank:" + entry.getValue().pageRank);
  								writer.print(" ,FetchStatus:" + CrawlDatum.getStatusName(entry.getValue().fetchStatus));
  								writer.print("\n");
  							}
	  					}
	  					else { 
	  						writer.print("ERROR:Unable to Locate Data for Domain:" + domainName + " DH:" + domainId + "\n");
	  					}
    				}
						finally { 
							scanner.close();
						}
						
					}
					finally { 
						reader.close();
					}
        }
        finally { 
        	indexInputStream.close();
        	writer.flush();
        }
      }
      catch (Exception e) { 
        throw new IOException(CCStringUtils.stringifyException(e));
      }
    }
  }
  
  @SuppressWarnings("serial")
  public static class InverseURLListByRootDomainQueryServlet extends HttpServlet { 
    
    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response)throws ServletException, IOException {
      
      //LOG.info("Received Request:" +request.toString());
    	if (request.getParameter("domain") == null || request.getParameter("page_no") == null || 
    			request.getParameter("page_size") == null && request.getParameter("sort_order") == null) { 
    		
    		throw new IOException("Missing Required Parameters");
    	}
      
      try { 
        String domain = request.getParameter("domain");
        int page_no = Integer.parseInt(request.getParameter("page_no")) - 1;
        int page_size = Integer.parseInt(request.getParameter("page_size"));
        String sortOrder = request.getParameter("sort_order");

        // build domain query info 
        InlinksByDomainQueryInfo queryInfo = new InlinksByDomainQueryInfo();
        
        if (URLUtils.isValidDomainName(domain)) { 
        	String rootDomain = URLUtils.extractRootDomainName(domain);
        	if (rootDomain != null) { 
  	        // set search pattern parameter 
  	        queryInfo.setDomainName(rootDomain);
        	}
        }
        if (queryInfo.getDomainName().length() == 0) { 
        	throw new IOException("Invalid Domain Name:" + domain);
        }
        
        // initialize paging info 
        ClientQueryInfo clientQueryInfo = new ClientQueryInfo();
        
        clientQueryInfo.setPageSize(page_size);
        clientQueryInfo.setPaginationOffset(page_no);
        if (sortOrder.equalsIgnoreCase("ASC"))
          clientQueryInfo.setSortOrder(ClientQueryInfo.SortOrder.ASCENDING);
        else 
          clientQueryInfo.setSortOrder(ClientQueryInfo.SortOrder.DESCENDING);
        
        InverseLinksByDomainQuery query = new InverseLinksByDomainQuery(queryInfo);

        
        try {
        	
        	BlockingQueryResult<FlexBuffer, URLFPV2> result  = _server.blockingQueryRequest(query, clientQueryInfo);
        		          
          if (result.querySucceeded) { 
            OutputStream outStream;
            try {
              outStream = response.getOutputStream();
              PrintWriter writer = new PrintWriter(outStream);
              
              //response.setContentType("application/x-json");
              response.setContentType("text/plain");

              writer.write("{\"total\":"+result.resultObject.getTotalRecordCount() +",");
              writer.write("\"page\":"+ (page_no + 1) +",");
              writer.write("\"rows\":[");
              int count = 0;
              
              DataInputBuffer inputReader = new DataInputBuffer();
              TextBytes text = new TextBytes();
              
              for (QueryResultRecord<FlexBuffer, URLFPV2> record : result.resultObject.getResults()) {
                if (count++ != 0) 
                  writer.write(",");
                // initialize the stream reader 
                inputReader.reset(record.getKey().get(),record.getKey().getOffset(),record.getKey().getCount());
                // skip target fp 
                inputReader.readLong();
                // capture rank 
                float pageRank = inputReader.readFloat();
                // capture incoming url ... 
                int textSize = WritableUtils.readVInt(inputReader);
                // initialize text 
                text.set(record.getKey().get(),inputReader.getPosition(),textSize);
                
                writer.write("[");
                writer.print(quote(text.toString()));
                writer.print(',');
                writer.print(pageRank);
                writer.print(',');
                MetadataOut metadata = null;
                try { 
                  metadata = _server.getDatabaseIndex().queryMetadataAndURLGivenFP(record.getValue());
                }
                catch (IOException e ) { 
                  LOG.error(CCStringUtils.stringifyException(e));
                }
                
                if (metadata != null) { 
                	writer.print(quote(metadata.url.toString()));
                }
                else { 
                	writer.print(quote("<<BAD URL>>"));
                }
                writer.write("]\n");
              }
              writer.append("]}");
              writer.flush();

              outStream.close();
            }
            catch (IOException e) { 
              LOG.error(CCStringUtils.stringifyException(e));
            }
          }
          else { 
          	
            OutputStream outStream;
            try {
              outStream = response.getOutputStream();
              PrintWriter writer = new PrintWriter(outStream);

              response.setContentType("text/plain");

              writer.append("Query Failed with Error:\n");
              writer.append(result.errorString);
              
              writer.flush();
              outStream.close();
            }
            catch (IOException e) { 
              LOG.error(CCStringUtils.stringifyException(e));
            }
          }
          
        } catch (IOException e) {
          LOG.error("Query Failed with Exception:" + CCStringUtils.stringifyException(e));
          throw e;
        }
      }
      catch (Exception e) { 
        throw new IOException(CCStringUtils.stringifyException(e));
      }
    }
  }
  
  /**
   * Produce a string in double quotes with backslash sequences in all the
   * right places. A backslash will be inserted within </, allowing JSON
   * text to be delivered in HTML. In JSON text, a string cannot contain a
   * control character or an unescaped quote or backslash.
   * @param string A String
   * @return  A String correctly formatted for insertion in a JSON text.
   */
  public static String quote(String string) {
      if (string == null || string.length() == 0) {
          return "\"\"";
      }

      char         b;
      char         c = 0;
      int          i;
      int          len = string.length();
      StringBuffer sb = new StringBuffer(len + 4);
      String       t;

      sb.append('"');
      for (i = 0; i < len; i += 1) {
          b = c;
          c = string.charAt(i);
          switch (c) {
          case '\\':
          case '"':
              sb.append('\\');
              sb.append(c);
              break;
          case '/':
              if (b == '<') {
                  sb.append('\\');
              }
              sb.append(c);
              break;
          case '\b':
              sb.append("\\b");
              break;
          case '\t':
              sb.append("\\t");
              break;
          case '\n':
              sb.append("\\n");
              break;
          case '\f':
              sb.append("\\f");
              break;
          case '\r':
              sb.append("\\r");
              break;
          default:
              if (c < ' ' || (c >= '\u0080' && c < '\u00a0') ||
                             (c >= '\u2000' && c < '\u2100')) {
                  t = "000" + Integer.toHexString(c);
                  sb.append("\\u" + t.substring(t.length() - 4));
              } else {
                  sb.append(c);
              }
          }
      }
      sb.append('"');
      return sb.toString();
  }
  
}
