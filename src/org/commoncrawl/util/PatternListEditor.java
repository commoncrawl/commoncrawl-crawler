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

package org.commoncrawl.util;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.Semaphore;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.mapred.BlackListRecord;
import org.commoncrawl.mapred.BlackListSimilarityMatch;
import org.commoncrawl.mapred.BlackListURLPattern;
import org.commoncrawl.mapred.PatternMatchDetails;
import org.commoncrawl.db.RecordStore;
import org.commoncrawl.server.AsyncWebServerRequest;
import org.commoncrawl.server.CommonCrawlServer;
import org.commoncrawl.util.URLPattern.URLPatternBuilder;
import org.commoncrawl.util.URLPattern.URLPatternMatcher;

import com.google.gson.stream.JsonWriter;

public class PatternListEditor extends CommonCrawlServer {

	@Override
  protected String getDefaultDataDir() {
	  return "data";
  }

	@Override
  protected String getDefaultHttpInterface() {
	  return "localhost";
  }

	@Override
  protected int getDefaultHttpPort() {
	  return 8033;
  }

	@Override
  protected String getDefaultLogFileName() {
	  return "patternEditor.log";
  }

	@Override
  protected String getDefaultRPCInterface() {
	  return "localhost";
  }

	@Override
  protected int getDefaultRPCPort() {
	  return 8034;
  }

	@Override
  protected String getWebAppName() {
	  return null;
  }

	static PatternListEditor _server;
	RecordStore _recordStore;
	RecordStore _oldRecordStore;
	
	@Override
  protected boolean initServer() {
		_server = this;
		
		try {
	    
			getWebServer().addServlet("validatePatterns", "/validate", ValidatePatternServlet.class);
			getWebServer().addServlet("bulkValidate", "/bulk", BulkValidate.class);
			getWebServer().addServlet("importOld", "/importOld", LoadFromOldDatabase.class);
			getWebServer().addServlet("genFile", "/genFile", GenerateFilterFile.class);
			
			_webServer.start();
	    
	    _recordStore = new RecordStore();
	    _oldRecordStore = new RecordStore();
	    
	    File patternDB = new File(getDataDirectory(),"pattern.db");
	    File oldPatternDB = new File(getDataDirectory(),"blacklist.db");
	    
	    _recordStore.initialize(patternDB, null);
	    _oldRecordStore.initialize(oldPatternDB, null);
	    
	    Vector<Long> recordIds = _recordStore.getChildRecordsByParentId("patterns");
	    
	    if (recordIds.size() == 0) {
	    	initializePatternDB();
	    }
	    else {
	    	/*
	    	LOG.info("There are:" + recordIds.size() + " patterns in the database");
	    	for (long recordId : recordIds) { 
	    		PatternMatchDetails detail = (PatternMatchDetails) _recordStore.getRecordById(recordId);
	    		if (detail.getStatus() == 0) { 
	    			LOG.info("Pattern:" + detail.getRegEx() + " is unmodified. Adding to Queue");
	    			_unmodifiedPatterns.add(recordId);
	    		}
	    		else { 
	    			LOG.info("Skipping Modified Pattern:" + detail.getRegEx() + " with status:" + PatternMatchDetails.Status .toString(detail.getStatus()));
	    		}
	    	}
	    	*/
	    }
	    
	    

	    
    } catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
    }
	  return true;
  }
	
	void initializePatternDB() throws IOException { 
		
		File sourcePath = new File(getDataDirectory(),"pattern.db.source");
		File destPath = new File(getDataDirectory(),"pattern.db");
		SequenceFile.Reader reader = new Reader(FileSystem.getLocal(CrawlEnvironment.getHadoopConfig()),
				new Path(sourcePath.getAbsolutePath()),CrawlEnvironment.getHadoopConfig());
		
		
		
		Text key = new Text();
		PatternMatchDetails details = new PatternMatchDetails();
		
		_recordStore.beginTransaction();
		while (reader.next(key,details)) { 
			
			LOG.info("Inserting Pattern:" + key.toString());
			_recordStore.insertRecord("patterns", key.toString(), details);
			
		}
		_recordStore.commitTransaction();
	}

	@Override
  protected boolean parseArguements(String[] argv) {
	  return true;
  }

	@Override
  protected void printUsage() {
	  // TODO Auto-generated method stub
	  
  }

	@Override
  protected boolean startDaemons() {
	  // TODO Auto-generated method stub
	  return false;
  }

	@Override
  protected void stopDaemons() {
	  // TODO Auto-generated method stub
  }

	
	public static class GenerateFilterFile extends HttpServlet { 
		
		private static class PatternMatch extends URLPatternMatcher {
			
			public String sourceExpression;
			public int matchCount = 0;
			public int attributionCount = 0;
			
			public PatternMatch(String regularExpression) throws PatternSyntaxException {
	      super(regularExpression);
	      sourceExpression = regularExpression;
      } 
			
		}
		private static class DomainRecord {
			
			public DomainRecord(String domainName) { 
				this.domainName = domainName; 
			}
			
			ArrayList<PatternMatch> patternList = new ArrayList<PatternMatch>();
			TreeSet<String> urls = new TreeSet<String>();
			String domainName;
		}
		
	  private static final String PARENT_REC_ID = "PARENT_REC_ID";
	  private static final String BlackListRecordPrefix = "BlackListRecordPrefix_";
		
		private static void populatePatternsFromOld(TreeMap<String,DomainRecord> domainMap,RecordStore recordStore) throws IOException { 
			
			for (long recordId : recordStore.getChildRecordsByParentId(PARENT_REC_ID)) {
				
				BlackListRecord blackListRecord = (BlackListRecord)recordStore.getRecordById(recordId);
				
				String domain = blackListRecord.getDomainName();
				
				String rootDomain = URLUtils.extractRootDomainName(domain);
				
				DomainRecord domainObject = domainMap.get(rootDomain);
				if (domainObject == null) { 
					domainObject = new DomainRecord(rootDomain);
					domainMap.put(rootDomain, domainObject);
				}
				
				if (blackListRecord.getStatus() == BlackListRecord.Status.blacklisted) {
					PatternMatch matchObject = new PatternMatch("http://[^/]*.*");
					matchObject.matchCount = Integer.MAX_VALUE;
					matchObject.attributionCount = Integer.MAX_VALUE;
					
					domainObject.patternList.add(matchObject);
				}
				else { 
					for (BlackListURLPattern pattern : blackListRecord.getPatterns()) { 
						if (pattern.getStatus() == BlackListURLPattern.Status.blacklist) { 
							// collect urls ... 
							Set<String> urlSet = new HashSet<String>();
							
							for (BlackListSimilarityMatch match : pattern.getMatches()) { 
								urlSet.add(match.getDocument1URL());
								urlSet.add(match.getDocument2URL());
							}
							
							URLPatternBuilder builder = new URLPatternBuilder();
							for (String url : urlSet) {
		  					builder.addPath(url);
		  					if (url.contains(";www.") || url.contains(";http")) { 
		  						domainObject.urls.add(url);
		  					}
							}
							builder.consolidatePatterns();
		
							int origSetSize = urlSet.size();
							
							for (URLPattern patternObj : builder.getPatterns()) { 
								
								String regularExpresion = patternObj.generateRegEx();
								URLPatternMatcher matcher = new URLPatternMatcher(regularExpresion);
								
								int matchCount = 0;
								Set<String> mismatches = new HashSet<String>();
								Set<String> matches = new HashSet<String>();
								// ok now validate against collected urls 
								for (String url : urlSet) {
									if (matcher.matches(url)) { 
										++matchCount;
										matches.add(url);
									}
									else { 
										mismatches.add(url);
									}
								}
								
								urlSet.clear();
								urlSet.addAll(mismatches);
								
								if (matchCount != 0) { 
									domainObject.patternList.add(new PatternMatch(regularExpresion));
								}
							}
						}
					}
				}
			}
		}
		
		private static void populatePatternsFromNew(TreeMap<String,DomainRecord> domainMap,RecordStore recordStore)throws IOException { 
			Vector<Long> recordIds = recordStore.getChildRecordsByParentId("patterns");
	    
    	LOG.info("There are:" + recordIds.size() + " patterns in the database");
    	
			    	
    	for (long recordId : recordIds) { 
    		PatternMatchDetails detail = (PatternMatchDetails) recordStore.getRecordById(recordId);

    		GoogleURL urlObject = new GoogleURL(detail.getUrls().get(0).toString());
  			String rootDomain = URLUtils.extractRootDomainName(urlObject.getHost());
  			
  			
  			DomainRecord domainObject = domainMap.get(rootDomain);
  			if (domainObject == null) { 
  				domainObject = new DomainRecord(rootDomain);
  				domainMap.put(rootDomain, domainObject);
  			}
    		
    		try { 
					
    			Pattern pattern = Pattern.compile(detail.getRegEx());
    			
    			domainObject.patternList.add(new PatternMatch(detail.getRegEx()));
    			
	  			for (TextBytes urlBytes : detail.getUrls()) {
	  				String url = urlBytes.toString();
	  				if (url.contains(";www.") || url.contains(";http")) { 
	  					LOG.error("Skipping BAD URL:" + url);
	  				}
	  				else {
	  					domainObject.urls.add(url);
	  				}
	  			}
    		}
  			catch (PatternSyntaxException e) { 
  				LOG.error(CCStringUtils.stringifyException(e));
  			}
    	}
			
		}
		
		@Override
		protected void doGet(HttpServletRequest req, HttpServletResponse resp)throws ServletException, IOException {
			RecordStore newRecordStore = _server._recordStore;
			RecordStore oldRecordStore = _server._oldRecordStore;

			TreeMap<String,DomainRecord> domainMap = new TreeMap<String,DomainRecord>();
			populatePatternsFromOld(domainMap,oldRecordStore);
			populatePatternsFromNew(domainMap,newRecordStore);

			// ok for each domain now ... 
			for (DomainRecord domainObject : domainMap.values()) { 
				// two passes 
				for (int pass=0;pass<2;++pass) { 
					if (pass == 0) { 
						for (String url : domainObject.urls){ 
							for (PatternMatch pattern : domainObject.patternList) { 
								if (pattern.matches(url)) {
									// increment match count
									if (pattern.matchCount != Integer.MAX_VALUE) { 
										pattern.matchCount++;
									}
								}
							}
						}
					}
					else { 
						// sort patterns by match count
						Collections.sort(domainObject.patternList,new Comparator<PatternMatch>() {

							@Override
							public int compare(PatternMatch o1, PatternMatch o2) {
								return ((Integer)o2.matchCount).compareTo(o1.matchCount);
							} 
						});
						// ok now iterate urls ... 
						for (String url : domainObject.urls) { 
							for (PatternMatch pattern : domainObject.patternList) { 
								if (pattern.matches(url)) { 
									// increment count
									if (pattern.attributionCount != Integer.MAX_VALUE) { 
										pattern.attributionCount++;
									}
									break;
								}
							}
						}

						Collections.sort(domainObject.patternList,new Comparator<PatternMatch>() {

							@Override
							public int compare(PatternMatch o1, PatternMatch o2) {
								return ((Integer)o2.attributionCount).compareTo(o1.attributionCount);
							} 
						});
					}
				}
			}
			PrintWriter writer = new PrintWriter(new File(System.currentTimeMillis() + "-patterns.txt"), "UTF-8");
			// ok time to write things out ...
			for (DomainRecord domainObj : domainMap.values()) { 
				for (PatternMatch match : domainObj.patternList) { 
					if (match.attributionCount != 0) { 
						writer.println(domainObj.domainName +"," + match.sourceExpression);
					}
				}
			}
			writer.flush();
			writer.close();
		}
	}
	
	public static class LoadFromOldDatabase extends HttpServlet { 
	  private static final String PARENT_REC_ID = "PARENT_REC_ID";
	  private static final String BlackListRecordPrefix = "BlackListRecordPrefix_";

	  @Override
	  protected void doGet(HttpServletRequest req, HttpServletResponse resp)throws ServletException, IOException {
	  	for (Object parameter : req.getParameterMap().keySet()) { 
	  		System.out.println("Param:" + parameter.toString());
	  	}
			RecordStore recordStore = _server._oldRecordStore;
			PrintWriter writer = resp.getWriter();
			
			writer.println("<HTML>");
			writer.println("<TABLE border=1>");
			for (long recordId : recordStore.getChildRecordsByParentId(PARENT_REC_ID)) { 
				BlackListRecord blackListRecord = (BlackListRecord)recordStore.getRecordById(recordId);
				
				if (blackListRecord.getStatus() == BlackListRecord.Status.blacklisted) {
					writer.println("<TR><TD colspan=3 style='background-color:red'>");
					writer.println("Domain:" + blackListRecord.getDomainName());
					writer.println("</TR>");
				}
				else { 
					for (BlackListURLPattern pattern : blackListRecord.getPatterns()) { 
						if (pattern.getStatus() == BlackListURLPattern.Status.blacklist) { 
							// collect urls ... 
							Set<String> urlSet = new HashSet<String>();
							
							for (BlackListSimilarityMatch match : pattern.getMatches()) { 
								urlSet.add(match.getDocument1URL());
								urlSet.add(match.getDocument2URL());
							}
							
							URLPatternBuilder builder = new URLPatternBuilder();
							for (String url : urlSet) { 
								builder.addPath(url);
							}
							builder.consolidatePatterns();

							int origSetSize = urlSet.size();
							
							for (URLPattern patternObj : builder.getPatterns()) { 
								
								URLPatternMatcher matcher = new URLPatternMatcher(patternObj.generateRegEx());
								
								int matchCount = 0;
								Set<String> mismatches = new HashSet<String>();
								Set<String> matches = new HashSet<String>();
								// ok now validate against collected urls 
								for (String url : urlSet) {
									if (matcher.matches(url)) { 
										++matchCount;
										matches.add(url);
									}
									else { 
										mismatches.add(url);
									}
								}
								
								urlSet.clear();
								urlSet.addAll(mismatches);
								
								writer.println("<TR><TD>" + blackListRecord.getDomainName());
								writer.println("<TD>" + matchCount + "/" + urlSet.size());
								writer.println("<TD>" + patternObj.generateRegEx());
								if (matches.size() != 0) { 
									writer.println("<BR>" + matches.iterator().next());
								}
								writer.println("</TR>");
							}
							
							if (urlSet.size() != 0) {
								writer.println("<TR><TD>Mismatches:");
								writer.println("<TD colspan=2>");
								writer.println("<TABLE BORDER=1>");
								for (String url : urlSet) { 
									writer.println("<TR><TD>" + url);
								}
								writer.println("</TABLE>");
								writer.println("</TR>");
							}
						}
					}
				}
			}
	  }
	  
	}
	
	public static class BulkValidate extends HttpServlet {
		
		@Override
		protected void doPost(final HttpServletRequest req, final HttpServletResponse resp)
		    throws ServletException, IOException {
			
			AsyncWebServerRequest request = new AsyncWebServerRequest("") {

				@Override
        public boolean handleRequest(Semaphore completionSemaphore) throws IOException {
					String action = req.getParameter("action");
					if (action.equals("apply")) { 
						
						String mode = req.getParameter("mode");
						
						if (mode == null || mode.equals("")) { 
							mode = "apply";
						}
						
						RecordStore recordStore = _server._recordStore;

						
						
						for (Object parameterName : req.getParameterMap().keySet()) { 
							String param = parameterName.toString();
							if (param.startsWith("PatternId_")) {
								recordStore.beginTransaction();
								long patternId = Long.parseLong(param.substring("PatternId_".length()));
								PatternMatchDetails detail = (PatternMatchDetails) recordStore.getRecordById(patternId);
								if (mode.equals("apply")) { 
									detail.setStatus(PatternMatchDetails.Status.Apply);
								}
								else if (mode.equals("ignore")) { 
									detail.setStatus(PatternMatchDetails.Status.Ignore);
								}
								else if (mode.equals("sessionid")) { 
									detail.setStatus(PatternMatchDetails.Status.SessionID);
								}
								LOG.info("Setting Pattern:" + detail.getRegEx() + " to:" + PatternMatchDetails.Status.toString(detail.getStatus()));
								recordStore.updateRecordById(patternId, detail);
								recordStore.commitTransaction();
							}
						}
						
						resp.sendRedirect("/bulk?action=submit&PATTERN=" + req.getParameter("PATTERN")+"&mode=" + req.getParameter("mode"));
					}
					return false;

				} 
				
			};

			request.dispatch(_server.getEventLoop());
			
		}
		
		@Override
		protected void doGet(final HttpServletRequest req, final HttpServletResponse resp)
		    throws ServletException, IOException {
			
			AsyncWebServerRequest request = new AsyncWebServerRequest("") {
				
				@Override
				public boolean handleRequest(Semaphore completionSemaphore)throws IOException {
					
					String action = req.getParameter("action");
					String mode = req.getParameter("mode");
					PrintWriter writer = resp.getWriter();
					if (action == null) {
						writer.println("<HTML>");
						writer.println("<script src='http://ajax.googleapis.com/ajax/libs/jquery/1.3.2/jquery.min.js' type='text/javascript'></script>");
						writer.println("<FORM action='/bulk' method=GET>");
						writer.println("<INPUT TYPE=HIDDEN NAME='action' VALUE='submit'></INPUT>");
						writer.println("Search for Pattern:<INPUT TYPE=TEXT NAME=PATTERN> </INPUT>");
						writer.println("Mode:<SELECT NAME=mode>");
						writer.println("<OPTION value='apply' " + ((mode == null || mode.equals("apply")) ? "selected" : "") + ">apply</OPTION>");
						writer.println("<OPTION value='ignore' " + ((mode != null && mode.equals("ignore")) ? "selected" : "") + ">ignore</OPTION>");
						writer.println("<OPTION value='sessionid' " + ((mode != null && mode.equals("sessionid")) ? "selected" : "") + ">sessionid</OPTION>");
						writer.println("</SELECT>");
						writer.println("<INPUT TYPE=SUBMIT>");
						writer.println("</FORM>");
						writer.println("</HTML>");
					}
					else if (action.equals("submit")) {
						String selectionPattern = req.getParameter("PATTERN");
						Pattern selectionPatternObj = Pattern.compile(selectionPattern);
						writer.println("<HTML>");
						writer.println("<script src='http://ajax.googleapis.com/ajax/libs/jquery/1.3.2/jquery.min.js' type='text/javascript'></script>");
						writer.println("<FORM action='/bulk' method=GET>");
						writer.println("<INPUT TYPE=HIDDEN NAME='action' VALUE='submit'></INPUT>");
						writer.println("Search for Pattern:<INPUT TYPE=TEXT NAME=PATTERN VALUE='" + selectionPattern +  "'> </INPUT>");
						writer.println("Mode:<SELECT id=modeSelector NAME=mode>");
						writer.println("<OPTION value='apply' " + ((mode == null || mode.equals("apply")) ? "selected" : "") + ">apply</OPTION>");
						writer.println("<OPTION value='ignore' " + ((mode != null && mode.equals("ignore")) ? "selected" : "") + ">ignore</OPTION>");
						writer.println("<OPTION value='sessionid' " + ((mode != null && mode.equals("sessionid")) ? "selected" : "") + ">sessionid</OPTION>");
						writer.println("</SELECT>");
						writer.println("<INPUT TYPE=SUBMIT>");
						writer.println("</FORM>");
						writer.println("<script>");
						writer.println("$(document).ready(function() { $('#modeSelector').change(function() { ");
						writer.println("$('#modeSelector option:selected').each(function () {");
						writer.println("$('#hiddenMode').val($(this).val()); } ); } ); } );");
						writer.println("</script>");
						
						writer.println("<FORM action='/bulk' method=POST>");
						writer.println("<INPUT TYPE=HIDDEN NAME='action' VALUE='apply'></INPUT>");
						writer.println("<INPUT TYPE=HIDDEN NAME='PATTERN' value='" + selectionPattern + "'> </INPUT>");
						writer.println("<INPUT id='hiddenMode' TYPE=HIDDEN NAME='mode' value='" + mode + "'> </INPUT>");
						writer.println("<INPUT TYPE=SUBMIT></INPUT>");
						writer.println("<TABLE BORDER=1>");
						RecordStore recordStore = _server._recordStore;

						int unmodifiedCount = 0;
						int selectedCount = 0;
						int badPatternCount=0;
						
						for (long patternId : recordStore.getChildRecordsByParentId("patterns")) {
							
			    		PatternMatchDetails detail = (PatternMatchDetails) recordStore.getRecordById(patternId);
			    		
			    		if (detail.getStatus() == PatternMatchDetails.Status.UnModified) { 
			    			unmodifiedCount++;
			    			
			    			Matcher selectionMatcher = selectionPatternObj.matcher(detail.getUrls().get(0).toString().toLowerCase());
			    			if (selectionMatcher.find()) { 
				    			URLPatternMatcher matcher = new URLPatternMatcher(detail.getRegEx());
				    			
				    			if (matcher.matches(detail.getUrls().get(0).toString())) {
				    				selectedCount++;
					    			writer.println("<TR><TD><INPUT class='applyCheckbox' TYPE=CHECKBOX NAME='PatternId_" + patternId + "' CHECKED></INPUT></TD>");
					    			writer.println("<TD> <a target=newwindow href='" + detail.getUrls().get(0)+"'>" + detail.getUrls().get(0)+ "</a>");
					    			writer.println("</TR>");
				    			}
				    			else { 
				    				badPatternCount++;
				    			}
				    		}
			    		}
						}
						writer.println("</TABLE>");
						writer.println("<INPUT TYPE=SUBMIT></INPUT>");
						writer.println("<P><a href=\"javascript:$('.applyCheckbox').attr('checked',false);\">UnCheckAll</a>&nbsp;<a href=\"javascript:$('.applyCheckbox').attr('checked',true);\">CheckAll</a>");
						writer.println("</FORM>");
						writer.println("<P><B>UnmodifiedCount:" + unmodifiedCount + " SelectedCount:" + selectedCount + " BadCount:" + badPatternCount );
						writer.println("</HTML>");
					}
					
					return false;
				}
			};
			
			request.dispatch(_server.getEventLoop());
		}
	}
	
	public static class ValidatePatternServlet extends HttpServlet {
		
		@Override
		protected void doGet(final HttpServletRequest req, final HttpServletResponse resp)
		    throws ServletException, IOException {
	    
			RecordStore recordStore = _server._recordStore;
			
			Vector<Long> recordIds = recordStore.getChildRecordsByParentId("patterns");
	    
    	LOG.info("There are:" + recordIds.size() + " patterns in the database");
    	for (long recordId : recordIds) { 
    		PatternMatchDetails detail = (PatternMatchDetails) recordStore.getRecordById(recordId);
    		LOG.info("Validating Pattern:" + detail.getRegEx());
  			boolean isValid;
  			int passNumber = 0;
  			do
  			{
  				isValid=false;
    			try { 
	  				Pattern pattern = Pattern.compile(detail.getRegEx());
	  				isValid=(passNumber != 0);
		  			for (TextBytes urlBytes : detail.getUrls()) {
		  				String url = urlBytes.toString();
		  				if (url.contains(";www.")) { 
		  					LOG.error("Skipping BAD URL:" + url);
		  				}
		  				else { 
			  				url = URLPattern.normalizeQueryURL(url);
			  				if (pattern.matcher(url).matches()) { 
			  					LOG.info("URL:" + url + " matches!");
			  				}
			  				else { 
			  					LOG.error("URL:" + url + " does not match pattern:" + detail.getRegEx());
			  					isValid = false;
			  					break;
			  				}
		  				}
		  			}
    			}
    			catch (PatternSyntaxException e) { 
    				LOG.error(CCStringUtils.stringifyException(e));
    			}
	  			
	  			if (!isValid) { 
	  				if (passNumber++ == 0) { 
		  				URLPatternBuilder builder = new URLPatternBuilder();
		  				for (TextBytes url : detail.getUrls()) { 
		  					builder.addPath(url.toString());
		  				}
		  				builder.consolidatePatterns();
		  				URLPattern patternObj = builder.getPatterns().get(0);
		  				// generate regular expression 
		  				detail.setRegEx(patternObj.generateRegEx());
	  				}
	  				else { 
	  					LOG.info("Failed to fix pattern:" + detail.getRegEx() + " on second pass. Marking Bad");
		  				detail.setPatternIsBad(true);
	  				}
	  				recordStore.beginTransaction();
	  				recordStore.updateRecordById(detail.getRecordId(), detail);
	  				recordStore.commitTransaction();
	  				
	  			}
  			} while (!isValid && !detail.getPatternIsBad());
    	}
		}
	}
	
	public static class BlackListDatabaseServlet extends HttpServlet {

		
	  private static RecordStore _recordStore;
	  private static TreeMap<String,BlackListRecord> _recordMap = new TreeMap<String,BlackListRecord>();
	  private static final String PARENT_REC_ID = "PARENT_REC_ID";
	  private static final String BlackListRecordPrefix = "BlackListRecordPrefix_";
	  
	  public static final Log LOG = LogFactory.getLog(BlackListDatabaseServlet.class);

	  
	  public static void initialize(PatternListEditor server)throws IOException { 
	  	File databaseFile = new File(server.getDataDirectory(),"blacklist.db");
	  	_recordStore = new RecordStore();
	  	_recordStore.initialize(databaseFile, null);
	  	intializeDatabase();
	  }
		
		@Override
		protected void doGet(HttpServletRequest req, HttpServletResponse resp)throws ServletException, IOException {
			URL url = new URL(req.getRequestURL().toString());
			LOG.info("Incoming URL:" + url.toString());
			Pattern pattern = Pattern.compile(".*/blackList/(.*)");
			Matcher matcher = pattern.matcher(url.getPath());
			
			if (matcher.matches()){
				resp.setCharacterEncoding("UTF-8");
				
				String action = matcher.group(1);
				
				if (action.equals("loadData")) { 
					LOG.info("Merging Databases");
					mergeDatabase();
					LOG.info("Done Merging Databases");
					resp.getWriter().print("Merge Complete");
				}
				else if (action.equals("getRecordSet")) {
					LOG.info("Generating RecordSet. Request:" + req.toString());
					generateBigDataset(req,resp,resp.getWriter());
				}
				else if (action.equals("updateDomainStatus")) { 
					LOG.info("Received UpdateDomainStatus Request:" + req.toString());
					updateDomainStatus(req,resp,resp.getWriter());
				}
				else if (action.equals("updatePatternStatus")) { 
					LOG.info("Received UpdatePatternStatus Request:" + req.toString());
					updatePatternStatus(req,resp,resp.getWriter());
				}
				else if (action.equals("getMatchList")) { 
					LOG.info("Received getMatchList Request:" + req.toString());
					getMatchList(req,resp,resp.getWriter());
				}

				else { 
					resp.sendError(500, "Bad Request");
				}
			}
		}
		
		private static void intializeDatabase() throws IOException { 
			for (long recordId : _recordStore.getChildRecordsByParentId(PARENT_REC_ID)) { 
				BlackListRecord blackListRecord = (BlackListRecord)_recordStore.getRecordById(recordId);
				_recordMap.put(blackListRecord.getDomainName(), blackListRecord);
			}
		}
		
		private void mergeDatabase()throws IOException {
			
			FileSystem fs = CrawlEnvironment.getDefaultFileSystem();
			Path path = new Path("crawl/scratch/sample001Result/part-00000");
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, CrawlEnvironment.getHadoopConfig());
			
			try { 
				Text domainName = new Text();
				BlackListRecord newRecord = new BlackListRecord();
				
				while (reader.next(domainName, newRecord)) {
					
					LOG.info("Domain Name:" + domainName.toString() + " recordName:" + newRecord.getDomainName());
					
					BlackListRecord existingRecord = _recordMap.get(domainName.toString());
					
					if (existingRecord != null) { 
						// take status from existing record ... 
						newRecord.setStatus(existingRecord.getStatus());

						
						// create a map of patterns of new set 
						TreeMap<String,BlackListURLPattern> newPatterns = new TreeMap<String,BlackListURLPattern>();
						
						for (BlackListURLPattern newPattern : newRecord.getPatterns()) { 
							newPatterns.put(newPattern.getPattern(), newPattern);
						}
						
						// walk patterns in existing set 
						
						for (BlackListURLPattern existingPattern : existingRecord.getPatterns()) { 
							// if the pattern's status has been modified ... 
							if (existingPattern.getStatus() == BlackListURLPattern.Status.blacklist) { 
								BlackListURLPattern newPatternObj = newPatterns.get(existingPattern.getPattern());
								if (newPatternObj != null) { 
									// honor old stats 
									newPatternObj.setStatus(existingPattern.getStatus());
								}
								else { 
									// need to add this pattern to new set 
									newRecord.getPatterns().add(existingPattern);
								}
							}
						}
						// ok delete old record and insert new record ... 
						_recordStore.beginTransaction();
						_recordStore.updateRecordByKey(BlackListRecordPrefix+newRecord.getDomainName(), newRecord);
						_recordStore.commitTransaction();
					}
					else{ 
						_recordStore.beginTransaction();
						_recordStore.insertRecord(PARENT_REC_ID,BlackListRecordPrefix+newRecord.getDomainName(), newRecord);
						_recordStore.commitTransaction();
					}
					// either way updte map ... 
					_recordMap.put(newRecord.getDomainName(),newRecord);
					
					newRecord = new BlackListRecord();
				}
			}
			finally { 
				reader.close();
			}
		}
		
		private void getMatchList(HttpServletRequest req, HttpServletResponse resp,PrintWriter writer) throws IOException { 
			String domain = req.getParameter("domain");
			int    patternIdx = Integer.parseInt(req.getParameter("patternIdx"));
			resp.setContentType("application/x-javascript; charset=utf-8");
			
			writer.println(req.getParameter("callback") + "(\n");
			
			JsonWriter jsonWriter = new JsonWriter(writer);
			try { 
				jsonWriter.beginObject();
				jsonWriter.name("results");
				jsonWriter.beginArray();

				BlackListRecord record = _recordMap.get(domain);
				
				if (patternIdx >=0 && patternIdx < record.getPatterns().size()) { 
					BlackListURLPattern pattern = record.getPatterns().get(patternIdx);
					
					for (BlackListSimilarityMatch match : pattern.getMatches()) { 
						jsonWriter.beginObject();
						jsonWriter.name("matchURL1");
						jsonWriter.value(match.getDocument1URL());
						jsonWriter.name("matchURL2");
						jsonWriter.value(match.getDocument2URL());
						jsonWriter.endObject();
					}
				}
				jsonWriter.endArray();
				jsonWriter.endObject();
			}
			catch (Exception e) { 
				throw new IOException(e);
			}
	    writer.println(");\n");
		}
		private void updatePatternStatus(HttpServletRequest req, HttpServletResponse resp,PrintWriter writer) throws IOException {
			
			
			String domain = req.getParameter("domain");
			int    patternIdx = Integer.parseInt(req.getParameter("patternIdx"));
			String status = req.getParameter("status");

			resp.setContentType("application/x-javascript; charset=utf-8");
			
			writer.println(req.getParameter("callback") + "(\n");
			
			JsonWriter jsonWriter = new JsonWriter(writer);
			try { 
				
				boolean success = false;
				
				jsonWriter.beginObject();
						
				LOG.info("Domain:" + domain + " new status:" + status);
				BlackListRecord record = _recordMap.get(domain);
				if (record != null) {

					if (patternIdx >= 0 && patternIdx < record.getPatterns().size()) { 
						success = true;

						if (status.equalsIgnoreCase("unmodified")) { 
							record.getPatterns().get(patternIdx).setStatus(BlackListURLPattern.Status.unmodified);
						}
						else if (status.equalsIgnoreCase("blacklist")) { 
							record.getPatterns().get(patternIdx).setStatus(BlackListURLPattern.Status.blacklist);
						}
						else { 
							success = false;
						}
						
						if (success) { 
							LOG.info("Updating Record:" + domain);
							_recordStore.beginTransaction();
							_recordStore.updateRecordByKey(BlackListRecordPrefix+record.getDomainName(), record);
							_recordStore.commitTransaction();
							LOG.info("Updated Record:" + domain);
					
							jsonWriter.name("status");
							jsonWriter.value(BlackListURLPattern.Status.toString(record.getPatterns().get(patternIdx).getStatus()));
						}
					}
				}
				jsonWriter.name("success");
				jsonWriter.value(success);
				
				jsonWriter.endObject();
				
		    writer.println(");\n");

			}
			catch (Exception e) { 
				throw new IOException(e);
			}
		}	
		private void updateDomainStatus(HttpServletRequest req, HttpServletResponse resp,PrintWriter writer) throws IOException {
			
			
			String domain = req.getParameter("domain");
			String status = req.getParameter("status");

			resp.setContentType("application/x-javascript; charset=utf-8");
			
			writer.println(req.getParameter("callback") + "(\n");
			
			JsonWriter jsonWriter = new JsonWriter(writer);
			try { 
				
				jsonWriter.beginObject();
						
				LOG.info("Domain:" + domain + " new status:" + status);
				BlackListRecord record = _recordMap.get(domain);
				if (record != null) {
					jsonWriter.name("success");
					jsonWriter.value(true);
					
					if (status.equalsIgnoreCase("unmodified")) { 
						record.setStatus(BlackListRecord.Status.unmodified);
					}
					else if (status.equalsIgnoreCase("modified")) { 
						record.setStatus(BlackListRecord.Status.modified);
					}
					else if (status.equalsIgnoreCase("blacklisted")) {
						record.setStatus(BlackListRecord.Status.blacklisted);
					}
					LOG.info("Updating Record:" + domain);
					_recordStore.beginTransaction();
					_recordStore.updateRecordByKey(BlackListRecordPrefix+record.getDomainName(), record);
					_recordStore.commitTransaction();
					LOG.info("Updated Record:" + domain);
					
					jsonWriter.name("status");
					jsonWriter.value(BlackListRecord.Status.toString(record.getStatus()));
				}
				else { 
					jsonWriter.name("success");
					jsonWriter.value(false);
				}
				
				jsonWriter.endObject();
				
		    writer.println(")\n");

			}
			catch (Exception e) { 
				throw new IOException(e);
			}
		}
		
		private void generateBigDataset(HttpServletRequest req, HttpServletResponse resp,PrintWriter writer) throws IOException { 
			
			
			resp.setContentType("application/x-javascript; charset=utf-8");
			
			writer.println(req.getParameter("callback") + "(\n");
			
			JsonWriter jsonWriter = new JsonWriter(writer);
					
			try { 
		    jsonWriter.beginObject();
				jsonWriter.name("results");
				jsonWriter.beginArray();
		
				int recordCount =0;
				
				BlackListRecord records[] = _recordMap.values().toArray(new BlackListRecord[0]);
				
				Arrays.sort(records,new Comparator<BlackListRecord>() {

					@Override
	        public int compare(BlackListRecord o1, BlackListRecord o2) {
		        return ((Integer)o2.getUrlCount()).compareTo(o1.getUrlCount());
	        } 
					
				});
				
				for (BlackListRecord record : records) {
					
					// write json record 
					jsonWriter.beginObject();
					
					jsonWriter.name("name");
					jsonWriter.value(record.getDomainName());
					jsonWriter.name("href");
					jsonWriter.value(record.getDomainName() + "_Index.html");
					jsonWriter.name("logFileCount");
					jsonWriter.value(record.getLogFileCount());
					jsonWriter.name("urlCount");
					jsonWriter.value(record.getUrlCount());
					jsonWriter.name("status");
					jsonWriter.value(BlackListRecord.Status.toString(record.getStatus()));
					
					jsonWriter.name("patterns");
					
					jsonWriter.beginArray();
					for (BlackListURLPattern pattern : record.getPatterns()) { 
						jsonWriter.beginObject();
						
						jsonWriter.name("pattern");
						jsonWriter.value(pattern.getPattern());				
		
						jsonWriter.name("status");
						jsonWriter.value(BlackListURLPattern.Status.toString(pattern.getStatus()));
						
						jsonWriter.name("matchCount");
						jsonWriter.value(pattern.getTotalMatchCount());				
		
						jsonWriter.name("avgJSC");
						jsonWriter.value(pattern.getAvgJSC());				

						/*
						jsonWriter.key("matches");
						jsonWriter.array();
						
						for (BlackListSimilarityMatch match : pattern.getMatches()) { 
							jsonWriter.object();
		
							jsonWriter.key("doc1URL");
							jsonWriter.value(match.getDocument1URL());				
							jsonWriter.key("doc2URL");
							jsonWriter.value(match.getDocument2URL());				
							jsonWriter.key("doc1Location");
							jsonWriter.value(match.getDocument1Locaiton());				
							jsonWriter.key("doc2Location");
							jsonWriter.value(match.getDocument2Location());				
							jsonWriter.key("hammingDistance");
							jsonWriter.value(match.getHammingDistance());				
							jsonWriter.key("jsc");
							jsonWriter.value(match.getJsc());				
							
							jsonWriter.endObject();
						}
						
						jsonWriter.endArray();
						*/
						
						jsonWriter.endObject();
					}
					jsonWriter.endArray();
					
					jsonWriter.endObject();
				}
				
		    jsonWriter.endArray();
		    jsonWriter.endObject();
		    
		    writer.println(")\n");
			}
			catch (Exception e) { 
				LOG.error(CCStringUtils.stringifyException(e));
				throw new IOException(e);
			}
		}
	}
}
