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

package org.commoncrawl.service.parser.ec2;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.server.CommonCrawlServer;
import org.commoncrawl.util.CCStringUtils;
import org.iq80.leveldb.DB;

import com.amazonaws.*;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import com.google.gson.JsonObject;

/**
 * 
 * @author rana
 *
 */
public class EC2ParserMaster extends CommonCrawlServer implements Constants {

  public static final String ENTRY_DB = "parse_entry_db";
  private static final String s3AccessKeyId = "";
  private static final String s3SecretKey = "";
  private static final Log LOG = LogFactory.getLog(EC2ParserMaster.class);
  
  private DB entryDB;

  @Override
  protected String getDefaultDataDir() {
    return CrawlEnvironment.DEFAULT_DATA_DIR;
  }

  @Override
  protected String getDefaultHttpInterface() {
    return "10.0.20.21";
  }

  @Override
  protected int getDefaultHttpPort() {
    return CrawlEnvironment.DEFAULT_EC2MASTER_HTTP_PORT;
  }

  @Override
  protected String getDefaultLogFileName() {
    return "historyserver.log";
  }

  @Override
  protected String getDefaultRPCInterface() {
    return CrawlEnvironment.DEFAULT_RPC_INTERFACE;
  }

  @Override
  protected int getDefaultRPCPort() {
    return CrawlEnvironment.DEFAULT_EC2MASTER_RPC_PORT;
  }

  @Override
  protected String getWebAppName() {
    return CrawlEnvironment.DEFAULT_EC2MASTER_WEBAPP_NAME;
  }

  @Override
  protected boolean initServer() {
    try {
      doScan(true);
      getWebServer().addServlet("checkout", "/checkout", CheckoutServlet.class);
      getWebServer().addServlet("ping", "/ping", PingServlet.class);
      getWebServer().addServlet("checkin", "/checkin", CheckInServlet.class);
    } catch (IOException e) {
      LOG.error(CCStringUtils.stringifyException(e));
      return false;
    }
    return true;
  }

  @Override
  protected boolean parseArguements(String[] argv) {
    return true;
  }

  @Override
  protected void printUsage() {
    
  }

  @Override
  protected boolean startDaemons() {
    startScannerThread();
    return true;
  }

  @Override
  protected void stopDaemons() {
    LOG.info("Shutting down scanner thread");
    if (_scannerThread != null) { 
      shutdownFlag.set(true);
      _scannerThread.interrupt();
      try {
        _scannerThread.join();
      } catch (InterruptedException e) {
      }
      _scannerThread = null;
    }
  }
  
  Thread _scannerThread = null;
  public static final int SCAN_INTERVAL = 5 * 60 * 1000;

  
  private Set<String> _complete = new HashSet<String>();
  private Multimap<Long,ParseCandidate> _candidates = TreeMultimap.create();
  private Map<ParseCandidate,ActiveHostRequest> _active = new TreeMap<ParseCandidate,ActiveHostRequest>();
  
  /***
   * 
   */
  private static class ParseCandidate implements Comparable<ParseCandidate> {
    public String _crawlLogName;
    public long _timestamp;
    public long _lastValidPos = 0;
    public long _size=0;
    
    public static ParseCandidate candidateFromBucketEntry(String bucketEntry) throws IOException  { 
      try { 
        Matcher m = crawlLogPattern.matcher(bucketEntry);
        if (m.matches() && m.groupCount() == 1) { 
          ParseCandidate candidate = new ParseCandidate();
          candidate._crawlLogName = m.group(1);
          Matcher timesampMatcher = timestampExtractorPattern.matcher(candidate._crawlLogName);
          if (timesampMatcher.matches()) { 
            candidate._timestamp = Long.parseLong(timesampMatcher.group(1));
          }
          else {
            throw new IOException("Invalid CrawlLog");
          }
          return candidate;
        }
      }
      catch (Exception e) { 
        LOG.error(CCStringUtils.stringifyException(e));
      }
      return null;
    }
    
//    public static ParseCandidate candidateFromDoneMatcher(Matcher m) throws IOException  { 
//      try { 
//        ParseCandidate candidate = new ParseCandidate();
//        candidate._crawlLogName = m.group(1);
//        Matcher timesampMatcher = timestampExtractorPattern.matcher(candidate._crawlLogName);
//        if (timesampMatcher.matches()) { 
//          candidate._timestamp = Long.parseLong(timesampMatcher.group(1));
//        }
//        else {
//          throw new IOException("Invalid CrawlLog");
//        }
//        candidate._lastValidPos = 
//      }
//      catch (Exception e) { 
//        LOG.error(CCStringUtils.stringifyException(e));
//      }
//      return null;
//    }
    
    @Override
    public String toString() {
      return _crawlLogName + ":" + _timestamp;
    }
    @Override
    public int compareTo(ParseCandidate o) {
      return _crawlLogName.compareTo(o._crawlLogName);
    }
    
    public static class Comparator implements java.util.Comparator<ParseCandidate> {

      @Override
      public int compare(ParseCandidate o1, ParseCandidate o2) {
        int result = (o1._timestamp < o2._timestamp) ? -1 : (o1._timestamp > o2._timestamp) ? 1: 0;
        if (result == 0) { 
          result = o1._crawlLogName.compareTo(o2._crawlLogName);
        }
        return result;
      } 
      
    }
  }
  
  static Pattern crawlLogPattern = Pattern.compile(".*(CrawlLog_ccc[0-9]{2}-[0-9]{2}_[0-9]*)");
  static Pattern timestampExtractorPattern = Pattern.compile("CrawlLog_ccc[0-9]{2}-[0-9]{2}_([0-9]*)");
  static Pattern doneFilePattern = Pattern.compile(".*(CrawlLog_ccc[0-9]{2}-[0-9]{2}_[0-9]*)_([0-9]*)_([0-9]*)"+ DONE_SUFFIX);
  
  AtomicBoolean shutdownFlag = new AtomicBoolean();
  
  
  private boolean doScan(boolean initialScan)throws IOException { 
    try { 
      LOG.info("Scanner Thread Starting");
      AmazonS3Client s3Client = new AmazonS3Client(new BasicAWSCredentials(s3AccessKeyId,s3SecretKey));
      
      ObjectListing response = s3Client.listObjects(new ListObjectsRequest().withBucketName("aws-publicdatasets").withPrefix(CC_BUCKET_ROOT+CC_CRAWLLOG_SOURCE));
      
      do { 
        
        LOG.info("Response Key Count:" + response.getObjectSummaries().size());
        
        for (S3ObjectSummary entry : response.getObjectSummaries()) { 
          
          Matcher matcher = crawlLogPattern.matcher(entry.getKey());
          if (matcher.matches()) {
            ParseCandidate candidate = ParseCandidate.candidateFromBucketEntry(entry.getKey());
            if (candidate == null) { 
              LOG.error("Failed to Parse Candidate for:" + entry.getKey());
            }
            else { 
              LOG.info("Candidate is:" + candidate);
              synchronized (this) {
                if (_complete.contains(candidate._crawlLogName)) { 
                  LOG.info("Skipping completed Candidate:" + candidate);
                }
                else {
                  if (!_candidates.containsEntry(candidate._timestamp, candidate) && !_active.containsKey(candidate)) {
                    // update candidate size here ... 
                    candidate._size = entry.getSize();
                    LOG.info("New Candidate:" + candidate._crawlLogName + " Found");
                    _candidates.put(candidate._timestamp,candidate);
                  }
                  else { 
                    LOG.info("Skipping Existing Candidate:" + candidate._crawlLogName);
                  }
                }
              }
            }
          }
        }
        
        if (response.isTruncated()) { 
          response = s3Client.listNextBatchOfObjects(response);
        }
        else { 
          break;
        }
      }
      while (!shutdownFlag.get());
      
      if (initialScan) { 
        // search for completions 
        synchronized(this) {
          scanForCompletions();
        }
      }
      
      return true;
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      return false;
    }
  }
  
  private static class ActiveHostRequest implements Comparable<ActiveHostRequest> { 
    public String hostName;
    public String uuid;
    public ParseCandidate candidate;
    public long   startTime;
    
    public ActiveHostRequest(String hostName,String  uuid,ParseCandidate candidate) { 
      this.hostName = hostName;
      this.uuid = uuid;
      this.candidate = candidate;
      this.startTime = System.currentTimeMillis();
    }

    @Override
    public int compareTo(ActiveHostRequest o) {
      int result = hostName.compareTo(o.hostName);
      if (result == 0) 
        result = uuid.compareTo(o.uuid);
      if (result == 0)
        result = candidate.compareTo(o.candidate);
      return result;
    }
  }
  
  
  public static class PingServlet extends HttpServlet { 
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)throws ServletException, IOException {
      String hostName = req.getParameter("host");
      String uuid     = req.getParameter("uuid");
      String logName  = req.getParameter("activeFile");
      String  pos      = req.getParameter("pos");
      if (hostName == null || uuid == null || logName == null || pos == null) {
        LOG.error("Invalid Request from Host:" + req.getRemoteAddr());
        resp.sendError(500,"Invalid Parameters");
      }
      else { 
        LOG.info("Got PING Request from Host:" + hostName + " uuid:" + uuid + " address:" + req.getRemoteAddr());
        EC2ParserMaster server = getServer();
        ParseCandidate candidate = ParseCandidate.candidateFromBucketEntry(logName);
        if (candidate == null) { 
          LOG.error("Unable to Parse Candidate given Name:" + logName + "from Host:" + hostName + " uuid:" + uuid + " address:" + req.getRemoteAddr());
          resp.sendError(500);
        }
        else { 

          boolean sendFailure =true;
          synchronized (server) {
            ActiveHostRequest request = server._active.get(candidate);
            if (request == null) { 
              LOG.error("Unable to Find ParseCandidate:" + candidate._crawlLogName + " in ActiveList from Host:" + hostName + " uuid:" + uuid + " address:" + req.getRemoteAddr());              
            }
            else { 
              if (!request.hostName.equals(hostName) || !request.uuid.equals(uuid)) { 
                // ok this is pad
                LOG.error("Host Mismatch for candidate:" + candidate._crawlLogName
                    + " We show:" + request.hostName + ":" + request.uuid + " We Got:"
                    +  hostName + ":" + uuid + " from:" + req.getRemoteAddr());
              }
              else { 
                long newPos = Long.parseLong(pos);
                LOG.info("Updating Candidate:" + request.candidate._crawlLogName + " with new Pos:"+ newPos);
                request.candidate._lastValidPos = newPos;
                request.startTime = System.currentTimeMillis();
                sendFailure = false;
              }
            }
          }
          if (sendFailure) { 
            resp.sendError(500);
          }
          else { 
            resp.setStatus(200);
          }
        }
      }
    }
  }
  
  public static class CheckInServlet extends HttpServlet { 
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)throws ServletException, IOException {
      String hostName = req.getParameter("host");
      String uuid     = req.getParameter("uuid");
      String logName  = req.getParameter("activeFile");
      String  pos      = req.getParameter("pos");
      if (hostName == null || uuid == null || logName == null || pos == null) {
        LOG.error("Invalid Request from Host:" + req.getRemoteAddr());
        resp.sendError(500,"Invalid Parameters");
      }
      else { 
        LOG.info("Got PING Request from Host:" + hostName + " uuid:" + uuid + " address:" + req.getRemoteAddr());
        EC2ParserMaster server = getServer();
        ParseCandidate candidate = ParseCandidate.candidateFromBucketEntry(logName);
        if (candidate == null) { 
          LOG.error("Unable to Parse Candidate given Name:" + logName + "from Host:" + hostName + " uuid:" + uuid + " address:" + req.getRemoteAddr());
          resp.sendError(500);
        }
        else { 

          boolean sendFailure =true;
          synchronized (server) {
            ActiveHostRequest request = server._active.get(candidate);
            if (request == null) { 
              LOG.error("Unable to Find ParseCandidate:" + candidate._crawlLogName + " in ActiveList from Host:" + hostName + " uuid:" + uuid + " address:" + req.getRemoteAddr());              
            }
            else { 
              if (!request.hostName.equals(hostName) || !request.uuid.equals(uuid)) { 
                // ok this is pad
                LOG.error("Host Mismatch for candidate:" + candidate._crawlLogName
                    + " We show:" + request.hostName + ":" + request.uuid + " We Got:"
                    +  hostName + ":" + uuid + " from:" + req.getRemoteAddr());
              }
              else { 
                long newPos = Long.parseLong(pos);
                LOG.info("Updating Candidate:" + request.candidate._crawlLogName + " Pos:"+ newPos);
                request.candidate._lastValidPos = newPos;
                if (request.candidate._lastValidPos == request.candidate._size) { 
                  LOG.info("MARKING Candidate:" + request.candidate._crawlLogName + " As COMPLETE");
                  // ok now mark this candidate as complete... 
                  server._active.remove(request.candidate);
                  server._complete.add(request.candidate._crawlLogName);
                }
                else {
                  LOG.info("Making Active Candidate " + request.candidate._crawlLogName + " AVAILABLE");
                  server._active.remove(request.candidate);
                  server._candidates.put(request.candidate._timestamp, request.candidate);
                }
                sendFailure = false;
              }
            }
          }
          if (sendFailure) { 
            resp.sendError(500);
          }
          else { 
            resp.setStatus(200);
          }
        }
      }
    }
  }
  
  public static class CheckoutServlet extends HttpServlet { 
    
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      String hostName = req.getParameter("host");
      String uuid     = req.getParameter("uuid");
      if (hostName == null || uuid == null) { 
        LOG.error("Invalid Request from Host:" + req.getRemoteAddr());
        resp.sendError(500,"Invalid Parameters");
      }
      else { 
        LOG.info("Got Request from Host:" + hostName + " uuid:" + uuid + " address:" + req.getRemoteAddr());
        EC2ParserMaster server = getServer();
        ParseCandidate candidate = null;
        synchronized (server) {
          if (server._candidates.size() != 0) {
            candidate = Iterables.getFirst(server._candidates.values(), null);
            if (candidate != null) {
              LOG.info("Assigning candidate:" + candidate._crawlLogName + " to Host:" + hostName + " uuid:" + uuid);
              server._candidates.remove(candidate._timestamp,candidate);
              // create a host request object ... 
              ActiveHostRequest request = new ActiveHostRequest(hostName, uuid, candidate);
              server._active.put(candidate, request);
            }
          }
        }
        if (candidate != null) { 
          JsonObject objectOut = new JsonObject();
          objectOut.addProperty("name",candidate._crawlLogName);
          objectOut.addProperty("lastPos",candidate._lastValidPos);
          objectOut.addProperty("size",candidate._size);
          
          resp.setContentType("text/plain");
          resp.getWriter().append(objectOut.toString());
          resp.getWriter().flush();
        }
        else { 
          resp.sendError(404,"No Valid Candidate Found");
        }
      }
    }
  }
  
  public static EC2ParserMaster getServer() { 
    return (EC2ParserMaster) CommonCrawlServer.getServerSingleton();
  }
  
  private void startScannerThread() { 
    _scannerThread = new Thread(new Runnable() {
      
      @Override
      public void run() {
        while (!shutdownFlag.get()) { 
          
          LOG.info("Sleeping.... ");
          try {
            if (!shutdownFlag.get())
              Thread.sleep(SCAN_INTERVAL);
          } catch (InterruptedException e) {
          }
          
          if(!shutdownFlag.get()) { 
            try {
              doScan(false);
            } catch (IOException e1) {
              LOG.error(CCStringUtils.stringifyException(e1));
            }
          }
        }
      }
    });
    _scannerThread.start();
  }
  
  public void scanForCompletions() throws IOException { 
    AmazonS3Client s3Client = new AmazonS3Client(new BasicAWSCredentials(s3AccessKeyId,s3SecretKey));
    
    ObjectListing response = s3Client.listObjects(new ListObjectsRequest().withBucketName("aws-publicdatasets").withPrefix(CC_BUCKET_ROOT+CC_PARSER_INTERMEDIATE));
    
    do {

      LOG.info("Response Key Count:" + response.getObjectSummaries().size());
      
      for (S3ObjectSummary entry : response.getObjectSummaries()) { 
        Matcher matcher = doneFilePattern.matcher(entry.getKey());
        if (matcher.matches()) {
          ParseCandidate candidate = ParseCandidate.candidateFromBucketEntry(entry.getKey());
          if (candidate == null) { 
            LOG.error("Failed to Parse Candidate for:" + entry.getKey());
          }
          else {
            long partialTimestamp = Long.parseLong(matcher.group(2));
            long position = Long.parseLong(matcher.group(3));
            LOG.info("Found completion for Log:" + candidate._crawlLogName + " TS:" + partialTimestamp + " Pos:" + position);
            candidate._lastValidPos = position;
            
            // ok lookup existing entry if present ... 
            ParseCandidate existingCandidate = Iterables.find(_candidates.get(candidate._timestamp),Predicates.equalTo(candidate));
            // if existing candidate found 
            if (existingCandidate != null) { 
              LOG.info("Found existing candidate with last pos:" + existingCandidate._lastValidPos);
              if (candidate._lastValidPos > existingCandidate._lastValidPos) { 
                existingCandidate._lastValidPos = candidate._lastValidPos;
                if (candidate._lastValidPos == candidate._size) { 
                  LOG.info("Found last pos == size for candidate:" + candidate._crawlLogName + ".REMOVING FROM ACTIVE - MOVING TO COMPLETE");
                  _candidates.remove(candidate._timestamp, candidate);
                  _complete.add(candidate._crawlLogName);
                }
              }
            }
            else { 
              LOG.info("Skipping Completion for CrawlLog:" + candidate._crawlLogName + " because existing candidate was not found.");
            }
          }
        }
      }      
      if (response.isTruncated()) { 
        response = s3Client.listNextBatchOfObjects(response);
      }
      else { 
        break;
      }
    }
    while (true);
  }
  
  public static void main(String[] args) throws IOException {

    Multimap<String,String> options = TreeMultimap.create();
    for (int i=0;i<args.length;++i) { 
      String optionName = args[i];
      if (++i != args.length) { 
        String optionValue = args[i];
        options.put(optionName, optionValue);
      }
    }
    options.removeAll("--server");
    options.put("--server",EC2ParserMaster.class.getName());
    
    Collection<Entry<String,String>> entrySet = options.entries();
    String finalArgs[] = new String[entrySet.size() * 2];
    int index = 0;
    for (Entry entry : entrySet) { 
      finalArgs[index++] = (String)entry.getKey();
      finalArgs[index++] = (String)entry.getValue();
    }
    
    try {
      CommonCrawlServer.main(finalArgs);
    } catch (Exception e) {
      LOG.error(CCStringUtils.stringifyException(e));
    }    
  }
}
