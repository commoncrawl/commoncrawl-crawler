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
 */



package org.commoncrawl.service.crawler;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;

import javax.servlet.jsp.JspWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.async.Timer;
import org.commoncrawl.common.Environment;
import org.commoncrawl.protocol.CrawlSegmentHost;
import org.commoncrawl.protocol.CrawlSegmentURL;
import org.commoncrawl.protocol.URLFP;
import org.commoncrawl.protocol.CrawlURL.FailureReason;
import org.commoncrawl.service.statscollector.CrawlerStats;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.RuntimeStatsCollector;
import org.commoncrawl.util.URLUtils;

import com.google.common.collect.ImmutableSet;

/**
 * A queue that manages a set of Crawlable Hosts
 * 
 * @author rana
 *
 */
public final class CrawlQueue {

    /** constants **/
    private static final int SCHEDULER_QUEUE_INITIAL_SIZE = 10000;
    
    private static final int SCHEDULER_SCAN_INTERVAL = 1000;
    
    
    private static final int IDLE_SCAN_INTERVAL = 30000;
       
    /** logging **/
    private static final Log LOG = LogFactory.getLog(CrawlQueue.class);

    public enum Protocol { 
        UNKNOWN,
        HTTP,
        HTTPS,
        FTP
    }
    
    private Protocol _protocol;
    private Fetcher   _fetcher;
    private boolean _active = false;
    private Timer _schedulerTimer = null;
    private long      _lastIdleCheckTime = -1;
    private long      _purgedHostCount = 0;

    /** active host map **/
    private TreeMap<Integer,CrawlQueueHost> _activeHosts    = new TreeMap<Integer,CrawlQueueHost>();
    /** idle host map **/
    private TreeMap<Integer,CrawlQueueHost> _idleHosts    = new TreeMap<Integer,CrawlQueueHost>();
    
    /** scheduled host priority queue **/
    private PriorityQueue<CrawlQueueHost> _schedulerQueue = new PriorityQueue<CrawlQueueHost>(SCHEDULER_QUEUE_INITIAL_SIZE, 
        
        new Comparator<CrawlQueueHost>() {

          public int compare(CrawlQueueHost host1, CrawlQueueHost host2) {
            if (host1.getWaitTime() < host2.getWaitTime()) { 
              return -1;
            }
            else if (host1.getWaitTime() > host2.getWaitTime()) { 
              return 1;
            }
            return 0;
          } 
        });

    public CrawlQueue(Protocol protocol,Fetcher fetcher) { 
      
      _protocol = protocol;
      _fetcher   = fetcher;
    }

    private void setScheduleTimer() { 
      
      _schedulerTimer = new Timer(SCHEDULER_SCAN_INTERVAL,true,new Timer.Callback() {

        public void timerFired(Timer timer) {
          
          // list of hosts that have cleared the wait state ... 
          LinkedList<CrawlQueueHost> readyList = new LinkedList<CrawlQueueHost>();
          
          CrawlQueueHost  item = null;
          
          Long currentTime = System.currentTimeMillis();
          
          while ( (item = _schedulerQueue.peek()) != null) { 
            
            // check to see if this host's timer has expired ... 
            if (currentTime >= item.getWaitTime()) {
              
              // remove from queue ... 
              _schedulerQueue.remove();

              // add to ready list ... 
              readyList.add(item);
            }
            else { 
              // break out of loop
              break;
            }
          }
          
          // now walk ready list and clear the host's wait state 
          for (CrawlQueueHost readyHost : readyList) { 
            readyHost.clearWaitState();
          }
          
          if (_lastIdleCheckTime == -1 || System.currentTimeMillis() - _lastIdleCheckTime >= IDLE_SCAN_INTERVAL) { 
            
            _lastIdleCheckTime = System.currentTimeMillis();
            
            // do idle scan ... 
            purgeIdleHosts();
          }
        }
      });
      getEngine().getServer().getEventLoop().setTimer(_schedulerTimer);
      
    }

   
    private void purgeIdleHosts() {
      
      LinkedList<CrawlQueueHost> purgeCandidates = new LinkedList<CrawlQueueHost>();
      
      long currentTime = System.currentTimeMillis();

      // walk active hosts ... 
      for (CrawlQueueHost host : _activeHosts.values()) { 
        
        if (currentTime - host.getLastModifiedTime() >= getEngine().getServer().getHostIdleFlushThreshold()) { 
          if (host.noActiveLists()) { 
            // LOG.info("BUG: Host:" + host.getIPAddressAsString() + " shows NOT_IDLE but is really IDLE");
            purgeCandidates.add(host);
          }
        }
      }
      
      // move inactive but not idled hosts into idle host bucket ... 
      for (CrawlQueueHost host : purgeCandidates) { 
        idleHost(host);
      }
      
      
      // walk idle hosts ... 
      for (CrawlQueueHost host : _idleHosts.values()) {
        if (host.noActiveLists()) { 
          if (currentTime - host.getLastModifiedTime() >= getEngine().getServer().getHostIdleFlushThreshold()) { 
            purgeCandidates.add(host);
          }
        }
      }
      
      if (purgeCandidates.size() != 0) { 
        if (Environment.detailLogEnabled())
          LOG.info("Purging " + purgeCandidates.size() + " IDLE Hosts");
      }
      for (CrawlQueueHost host : purgeCandidates) { 
        if (Environment.detailLogEnabled())
          LOG.info("Purging IDLE Host:" + host.getIPAddressAsString());
        
        // clear the host ... 
        host.purgeReferences();
        // and remove it from the map ... 
        _idleHosts.remove(host.getIPAddress());
        // increment stats ... 
        ++_purgedHostCount;
      }
      
      purgeCandidates.clear();
      
      
    }
    private void stopScheduleTimer() { 
      if (_schedulerTimer != null) {
        getEngine().getServer().getEventLoop().cancelTimer(_schedulerTimer);
      }
      _schedulerTimer = null;
    }
    
    /** get access to the engine object **/
    CrawlerEngine getEngine() { return CrawlerServer.getEngine(); }
    
    /** get the protocol associated with this queue **/
    public Protocol getProtocol() { return _protocol; }

    /** identify the protocol associated with the given host ... **/
    public static Protocol identifyProtocol(String  url) { 
      if (url.startsWith("http://") || url.startsWith("HTTP://")) { 
        return Protocol.HTTP;
      }
      return Protocol.UNKNOWN;
    }
    
    /** create a robots url given a specified host (based on the queue's protocol) **/
    URL getRobotsURL(String host) { 
      
      URL urlOut = null;
      
      // we only support http for now ...
      if (_protocol == Protocol.HTTP) { 
        try {
          urlOut = new URL("http",host,"/robots.txt");
        } catch (MalformedURLException e) {
          LOG.error(CCStringUtils.stringifyException(e));
        }
      }
      return urlOut;
    }

    /** get / set active state **/
    public final boolean isActive() { return _active; }

    public void startCrawl(boolean isRestart) { 
      // start the fetcher ... 
      _fetcher.start();
      
      // start schedule timer 
      setScheduleTimer();
      
      _active = true;
      
      if (!isRestart) { 
        // reschedule hosts ... 
        for (CrawlQueueHost host: _activeHosts.values()) { 
          if (!host.isIdled()) { 
            host.feedQueue();
          }
        }
      }
    }
    
    public void stopCrawl() { 

      stopScheduleTimer();
      
      // stop fetcher first ... it will retain all urls in its queue ... 
      _fetcher.stop();
    }
    
    public void pauseCrawl() {
      if (_fetcher != null) { 
        _fetcher.pause();
      }
    }
    
    public void resumeCrawl() {
      if (_fetcher != null) { 
        _fetcher.resume();
      }
    }
   
    private CrawlQueueHost getCrawlHost(int serverIP) {
      
      boolean activeHostCountIncreased = false;
      
      // get a host based on ip address ... 
      CrawlQueueHost crawlHost = _activeHosts.get(serverIP);

      // if null. first check idled hosts ... 
      if (crawlHost == null) { 
        crawlHost = _idleHosts.get(serverIP);
        // if we found an idled host ... mark it as active ... 
        if (crawlHost != null) { 
          _idleHosts.remove(crawlHost.getIPAddress());
          crawlHost.setIdle(false);
          _activeHosts.put(crawlHost.getIPAddress(),crawlHost);
          activeHostCountIncreased = true;
        }
      }
      
      // if crawl host is still null... allocate a new host ... 
      if (crawlHost == null) { 
        crawlHost = new CrawlHostImpl(this,serverIP);
        _activeHosts.put(serverIP,crawlHost);
        activeHostCountIncreased = true;
      }
      
      if (activeHostCountIncreased) { 
        getEngine().incDecActiveHostCount(1);
      }
      
      return crawlHost;
    }
    
    public void queueExternalURLRequest(String url,int listId,long fingerprint,String hostName,int resolvedIPAddress,long ipAddressTTL,boolean highPriorityRequest,CrawlItemStatusCallback callback) { 
      
      // get crawl host based on ip address
      CrawlQueueHost crawlHost = getCrawlHost(resolvedIPAddress);
      
      CrawlList crawlList = crawlHost.getCrawlList(listId);

      // lock it ... 
      synchronized (crawlList) {
        // update the list's dns cache 
        crawlList.cacheDNSEntry(hostName,resolvedIPAddress, ipAddressTTL);
        
        // update it's disposition change time ...
        crawlList.updateLastModifiedTime(System.currentTimeMillis());
        
        // allocate a new crawl target ... 
        CrawlTarget target = new CrawlTarget(-1,crawlList,url,fingerprint,callback);
        
        // set target's ip address and ttl based on original host 
        target.setServerIP(resolvedIPAddress);
        target.setServerIPTTL(ipAddressTTL);
        
        // and add to the domain's list ... 
        crawlList.addCrawlTarget(target,highPriorityRequest);
      }
    }
    
        
    /** add the specified url to the queue **/
    //TODO: OPTIMIZE THIS ROUTINE ....
    public int queueHost(int segmentId,int listId, CrawlSegmentHost host,CrawlItemStatusCallback callback) { 
      
      int newItemsQueued = 0;
      int originalDomainQueuedCount= 0;
      // int originalHostQueuedCount = 0;
      
      // get crawl host based on ip address
      CrawlQueueHost crawlHost = getCrawlHost(host.getIpAddress());
      // update last mod time
      crawlHost.updateLastModifiedTime(System.currentTimeMillis());
      
      // and extract host name ... 
      String hostName = host.getHostName();

      // get crawl list for host name ... 
      CrawlList crawlList = crawlHost.getCrawlList(listId);
 
      // lock it ... 
      synchronized (crawlList) {
        // update the list's dns cache 
        crawlList.cacheDNSEntry(hostName,host.getIpAddress(), host.getTtl());
        
        // update it's disposition change time ...
        crawlList.updateLastModifiedTime(System.currentTimeMillis());
        // get the domain initial queued count ... 
        originalDomainQueuedCount = crawlList.getPendingURLCount() + crawlList.getOfflineURLCount();
  
        // and next ... walk the targets ... 
        for (CrawlSegmentURL segmentURL : host.getUrlTargets()) { 
          
          //NOTE:(AHAD) REMOVED QUEUE SIZE RESTRICTION  
          //if (originalDomainQueuedCount + newItemsQueued >= MAX_DOMAIN_QUEUE_SIZE /*|| originalHostQueuedCount + newItemsQueued >= MAX_HOST_QUEUE_SIZE*/)
          //  break;
          
          // get fp for url 
          URLFP urlFingerprint = URLUtils.getURLFPFromURL(segmentURL.getUrl(),false);
          if (urlFingerprint != null) { 
            // check to see if already crawled ... 
            if (CrawlerServer.getEngine().getLocalBloomFilter().isPresent(urlFingerprint)) { 
              // CrawlTarget.failURL(CrawlTarget.allocateCrawlURLFromSegmentURL(host.getSegmentId(),host,segmentURL,false),null, CrawlURL.FailureReason.UNKNOWN,"Item Already Crawled");
              CrawlTarget.logFailureDetail(
                  getEngine(),
                  CrawlTarget.allocateCrawlURLFromSegmentURL(host.getSegmentId(),host,segmentURL,false),
                  null,
                  FailureReason.UNKNOWN, 
                  "Aready Crawled URL");
            }
            else { 
              
              // allocate a new crawl target ... 
              CrawlTarget target = new CrawlTarget(segmentId,crawlList,host,segmentURL);
              
              // set target's ip address and ttl based on original host 
              target.setServerIP(host.getIpAddress());
              target.setServerIPTTL(host.getTtl());
              
              // set optional callback
              target.setCompletionCallback(callback);
              
              // and add to the domain's list ... 
              crawlList.addCrawlTarget(target,false);
              // increment items Queued 
              newItemsQueued++;
            }
          }
          else { 
            CrawlTarget.logFailureDetail(
                getEngine(),
                CrawlTarget.allocateCrawlURLFromSegmentURL(host.getSegmentId(),host,segmentURL,false),
                null,
                FailureReason.MalformedURL, 
                "Failed to Convert to URLFP");            
          }
        }
        
      }
     
      return newItemsQueued;
    }
    
    void fetchItem(CrawlTarget target) { 
      _fetcher.queueURL(target);
    }
    
    void setTimer(CrawlQueueHost host,long timeoutTime) { 
      _schedulerQueue.add(host);
    }
    
    void killTimer(CrawlQueueHost host) { 
      _schedulerQueue.remove(host);
    }

    /** clear / reset queue **/
    public void clear() { 
      _fetcher.clearQueues();
      for (CrawlQueueHost host: _idleHosts.values()) { 
        host.purgeReferences();
      }
      _idleHosts.clear();
      
      for (CrawlQueueHost host: _activeHosts.values()) { 
        host.purgeReferences();
      }
      
      // inform engine of the change ...
      getEngine().incDecActiveHostCount(-_activeHosts.size());
      
      _activeHosts.clear();
      _schedulerQueue.clear();
    }
    
    void shutdown() { 
      // clear data structures first .... 
      clear();
      _protocol = null;
      _fetcher.shutdown();
      _fetcher = null;
      _schedulerTimer = null;
      
    }
    
   void idleHost(CrawlQueueHost host) {
     host.setIdle(true);
     _activeHosts.remove(host.getIPAddress());
     _idleHosts.put(host.getIPAddress(),host);
     // inform engine of stat change 
     getEngine().incDecActiveHostCount(-1);
   }
    
    
    void collectStats(CrawlerStats crawlerStats,RuntimeStatsCollector stats) { 
    
      stats.setIntValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.CrawlQueue_ActiveHostsCount, _activeHosts.size());
      stats.setIntValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.CrawlQueue_ScheduledHostsCount, _schedulerQueue.size());
      stats.setIntValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.CrawlQueue_IdledHostsCount,_idleHosts.size());
      stats.setLongValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.CrawlQueue_PurgedHostsCount,_purgedHostCount);

      synchronized(crawlerStats) { 
        crawlerStats.setActiveHosts(_activeHosts.size());
        crawlerStats.setScheduledHosts(_schedulerQueue.size());
        crawlerStats.setIdledHosts(_idleHosts.size());
      }
        
        //sb.append("*****ACTIVE-HOST-DUMP*****\n");
/*  
        sb.append(String.format("%1$20.20s ", "NAME"));
        sb.append(String.format("%1$10.10s ", "ROBOTS"));
        sb.append(String.format("%1$10.10s ", "REDIRECTS"));
        sb.append(String.format(" %1$7.7s ", "PENDING"));
        sb.append(String.format("%1$9.9s ", "SCHEDULED"));
        sb.append(String.format("%1$6.6s ", "ACTIVE"));
        sb.append(String.format("%1$10.10s ", "DISPOSITON"));
        sb.append(String.format("%1$8.8s\n", "HOSTFAIL"));
        
        
        for (CrawlHost host : _hosts.values()) { 
          sb.append(String.format("%1$20.20s ", host.getHostName()));
          sb.append(String.format("%1$10.10s ", ((Boolean)host.robotsRetrieved()).toString()));
          sb.append(String.format("%1$10.10s ",host.getTotalRedirects()));
          sb.append(String.format(" %1$7.7s ", host.getPendingURLCount()));
          sb.append(String.format("%1$9.9s ", host.isScheduled()));
          sb.append(String.format("%1$6.6s ", host.getActiveURLCount()));
          sb.append(String.format("%1$10.10s ", host.getDisposition()));
          sb.append(String.format("%1$8.8s\n", host.isFailedHost()));
        }
*/        
  /*        
        sb.append("*****IDLE-HOST-DUMP*****\n");
  
        sb.append(String.format("%1$20.20s\n", "NAME"));
        
        for (CrawlHost host : _idledHosts.values()) { 
          sb.append(String.format("%1$20.20s\n", host.getHostName()));
        }
    */
        
      _fetcher.collectStats(crawlerStats,stats);
    }
    
    private static class HostLoadInfo  implements Comparable { 
      
      public HostLoadInfo(CrawlQueueHost host) { 
        this.host = host;
        this.loadCount = host.getQueuedURLCount();
      }
      public CrawlQueueHost host;
      public int loadCount;
      
      public int compareTo(Object o) {
        HostLoadInfo other = (HostLoadInfo)o;
        
        if (loadCount < other.loadCount) { 
          return 1;
        }
        else if (loadCount > other.loadCount) { 
          return -1;
        }
        return 0;
      }
    }
    void dumpDetailsToHTML(JspWriter out) throws IOException { 
     
      if (_activeHosts.size() != 0) { 

        HostLoadInfo loadInfoVector[] = new HostLoadInfo[_activeHosts.size()];
        
        int index=0;
        int queuedAmount = 0;
        for (CrawlQueueHost host: _activeHosts.values()) {
          
          loadInfoVector[index] = new HostLoadInfo(host);
          
          queuedAmount += loadInfoVector[index].loadCount;
          
          ++index;
        }
        
        // sort the list by load ... 
        Arrays.sort(loadInfoVector);
        
        out.write("<B>Queued Item Count:" + queuedAmount + "</B></BR>" );
        out.write("<table border=1>");
        out.write("<tr><td>Host</td><td>Details</td></tr>");
        
        for (HostLoadInfo hostInfo : loadInfoVector) { 
          
          CrawlQueueHost host = hostInfo.host;
          
          out.write("<tr><td><a href=\"showHostDetails.jsp?hostId="+host.getIPAddress()+"\">" + host.getIPAddressAsString() + "</a></td><td><pre>");
          out.write("loadCount:<B>("+hostInfo.loadCount +") </B>");
          out.write("idled:" + host.isIdled());
          out.write(" activeList:" + ((host.getActiveList() != null) ? host.getActiveList().getListName() : "NULL"));
          out.write(" disp:<b>" + ((host.getActiveList() != null) ? host.getActiveList().getDisposition().toString() : "NULL") + "</b>");
          out.write(" lastChange(MS):");
          long lastChangeTimeMS = System.currentTimeMillis() - host.getLastModifiedTime();
          boolean colorRed = false;
          if (lastChangeTimeMS > 60000) { 
            colorRed = true;
            out.write("<FONT color=RED>");
          }
          out.write(Long.toString(lastChangeTimeMS));
          if (colorRed) { 
            out.write("</FONT>");
          }
          
          out.write("</pre></td></tr>");
        }
        out.write("</table>");
        }
    }

    public void dumpHostDetailsToHTML(JspWriter out, int hostIP)throws IOException {
      
      CrawlQueueHost host = _activeHosts.get(hostIP);
      
      if (host != null) { 
        out.write("<h2>Host Details for Host:" + host.getIPAddressAsString() + "</h2><BR>");
        out.write("<pre>");
        host.dumpDetails(out);
        out.write("</pre>");
      }
            
    }
    
    public static String protocolToScheme(Protocol protocol) { 
    	switch (protocol) { 
    	case FTP: return "ftp";
    	case HTTP: return "http";
    	case HTTPS: return "https";
    	default: return null;
    	}
    	
    }
    
    public Set<Integer> getActiveHostIPs() {
    	return ImmutableSet.copyOf(_activeHosts.keySet()); 
    }
}
