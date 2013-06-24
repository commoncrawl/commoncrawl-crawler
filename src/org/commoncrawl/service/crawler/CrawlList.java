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


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.record.Buffer;
import org.commoncrawl.util.GZIPUtils;
import org.commoncrawl.async.ConcurrentTask;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.common.Environment;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.io.NIOHttpConnection;
import org.commoncrawl.io.NIOHttpHeaders;
import org.commoncrawl.protocol.CrawlURL;
import org.commoncrawl.protocol.CrawlURLMetadata;
import org.commoncrawl.service.crawler.PersistentCrawlTarget;
import org.commoncrawl.service.crawler.RobotRulesParser.RobotRuleSet;
import org.commoncrawl.service.crawler.filters.FilterResults;
import org.commoncrawl.service.statscollector.CrawlerStats;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.FileUtils;
import org.commoncrawl.util.IPAddressUtils;
import org.commoncrawl.util.IntrusiveList;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLFingerprint;
import org.commoncrawl.util.URLUtils;
import org.commoncrawl.util.GZIPUtils.UnzipResult;
import org.commoncrawl.util.IntrusiveList.IntrusiveListElement;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * CrawlList - a collection of CrawlTargets (disk backed)
 * 
 * @author rana
 *
 */
public final class CrawlList extends IntrusiveList.IntrusiveListElement<CrawlList> {

	/** offline (disk) storage support **/
  private static int DISK_FLUSH_THRESHOLD = 50;
  private static int DISK_LOAD_THRESHOLD = 10;
  private static int IDEAL_TARGET_COUNT = 25;
  private static final int MAX_ROBOTS_EXCLUSION_IN_LOOP = 3;
  private static final int MAX_FAILED_TARGETS_IN_LOOP   = 50;
  private static final int IOEXCEPTION_TIMEOUT_BOOST = 60000;
  private static final int PAUSE_STATE_RETRY_DELAY = 10 * 60000; // 10 minutes
  
 
  static final AtomicLong seq = new AtomicLong();
  
  public static class DiskQueueEntry  implements Comparable<DiskQueueEntry> {
    
    final long seqNum;
    final CrawlList entry;
    final boolean isLoadRequest;
    
    public DiskQueueEntry(CrawlList item,boolean isLoadRequest) {
      this.seqNum = seq.getAndIncrement();
      this.entry = item;
      this.isLoadRequest = isLoadRequest;
    }
    
    public CrawlList getListItem() { return entry; }

    @Override
    public int compareTo(DiskQueueEntry o) {
      if (entry == null && o.entry != null) 
        return -1;
      else if (o.entry == null && entry != null) 
        return 1;
      else { 
        if (isLoadRequest && !o.isLoadRequest)
          return -1;
        else if (!isLoadRequest && o.isLoadRequest)
          return 1;
        else { 
          return ((Long)seqNum).compareTo(o.seqNum);
        }
      }
    }
  }  
  
  private static PriorityBlockingQueue<DiskQueueEntry> _diskOperationQueue = new PriorityBlockingQueue<DiskQueueEntry>();
  private static Thread  _diskOperationThread = null;
  private static boolean _diskOpThreadShuttingDown = false;
  private static long    _diskHeaderActiveVersionTimestamp = System.currentTimeMillis();
  
    
  /** logging **/
  private static final Log LOG = LogFactory.getLog(CrawlList.class);
  
  
  /** server repsonsible for servicing this domain **/
  private CrawlListHost  _host;
  /** host name **/
  private String _listName;
  /** host metadata */
  private int   _baseListId;
  /** unique list id **/
  private long   _uniqueListId;
  /** next crawl interface used to service this list **/
  private int    _nextCrawlInterface = 0;
  /** cumilative list of crawl targets associated with this queue ...*/ 
  private IntrusiveList<CrawlTarget> _pending = new IntrusiveList<CrawlTarget>();
  /** list of crawl targets directly scheduled for disk queue */
  private IntrusiveList<CrawlTarget> _queued = new IntrusiveList<CrawlTarget>();
  
  /** offline item count  - the set of crawl targets that are stored offline on disk **/
  private int _offlineTargetCount = 0;
  /** disk request pending **/
  private boolean _diskRequestPending = false;
  
  /** currently scheduled item **/
  private CrawlTarget _scheduled = null;
  /** fetch start time **/
  private long _fetchStartTime = -1;
  /** fetch end time **/
  private long _fetchEndTime = -1;
  /** last  successful fetch time (total time in milliseconds)**/
  private int  _lastRequestDownloadTime = -1;
  /** last successful request redirect count **/
  private int  _lastRequestRedirectCount = 0;
  
  /** active connection **/
  private NIOHttpConnection _activeConnection;

  /** SubDomain Stats and Robots State Information Structure **/
  private static class DomainInfo extends IntrusiveListElement<DomainInfo>{ 
    public String  _domainName;
    public boolean _domainFailed = false;
    public boolean _domainBlackListed = false;
    public long    _robotsCRC = -1;
    public long    _lastTouched;
    /** host retry counter **/
    public byte   _domainRetryCounter = 0;
    /** total 400 errors **/
    public int    _HTTP400Count = 0;
    /** total 500 errors **/
    public int    _HTTP500Count = 0;
    /** total 200 status code count**/
    public int    _HTTP200Count = 0;
    /** sequential failure count **/
    public short    _SequentialHTTPFailuresCount =0;
    
    public boolean   _robotsReturned400;
    
    public boolean   _robotsReturned403;
  }
  /** domain info map **/
  private IntrusiveList<DomainInfo> _domainInfo = new IntrusiveList<DomainInfo>();
  /** active domain info **/
  private DomainInfo _activeDomainInfo;
  
  /** the active robots rule set to apply robots policy for this host **/
  private RobotRuleSet _ruleSet = null;
  /** robots returned 400 **/
  private boolean   _robotsReturned400;
  private boolean   _robotsReturned403;

  /** the crc value for the active rule set - computed at pre-parse time**/
  private long _robotsCRC = 0;
  /** robots file retrieved */
  private boolean   _robotsRetrieved;
  /** robots host name **/
  private String    _robotsHostName;
  /** last fetched robots host Name**/
  private String    _lastFetchedRobotsHostName;
  /** last fetched robots data **/
  private String    _lastFetchedRobotsData;
  /** crc calculator **/
  private static CRC32 _crc32 = new CRC32();
  /** last request was io exception **/
  private boolean _lastRequestWasIOException = false;
  
  
  private static final int MAX_ITEM_RETRY = 2;
  
  private static final int MAX_HOST_RETRY = 7;
  
  private static final int DEFAULT_ITEM_RETRY_WAIT_TIME = 20000;
  
  private static final int DEFAULT_HOST_RETRY_WAIT_TIME = 20000;
  
  private static final int MIN_CRAWL_DELAY = 1;
  private static final int MAX_CRAWL_DELAY = 3500;
  
  private static int   STATS_CHECK_CODE_SAMPLE_THRESHOLD = 50; // don't do anything util we have retireved at least 50 urls
  
  private static float BAD_URL_TO_TOTAL_URL_FAILURE_THRESHOLD = .80f; // if 85% of urls are bad, fail the domain 
  
  // private static int   SEQUENTIAL_FAILURES_ON_403_ROBOTS_TRIGGER = 500;
  private static int   SEQUENTIAL_FAILURES_NO_200_TRIGGER = 10; // if we get 20 sequential failures we bail
  // private static int   SEQUENTIAL_FAILURES_SOME_200_TRIGGER = 1000;
  
  
  private static final int MAX_DNS_CACHE_ITEMS = 100;
  private static int MAX_DOMAIN_CACHE_ENTIRES = 1000;

  /** the reference to the singleton server object **/
  private static CrawlerServer _server = null;
  
  
   
  public enum Disposition { 
    ItemAvailable,
    WaitingOnCompletion,
    WaitingOnTime,
    QueueEmpty
  }
  
  /** domain's dipsition(state) **/
  private Disposition _disposition;
  
  
  private static byte WWWRULE_Remove  = 1 << 0;
  private static byte WWWRULE_Add     = 1 << 1;
    
  /** www rewrite rule patterns **/
  static class WWWReWriteItem extends IntrusiveList.IntrusiveListElement<WWWReWriteItem> {
    
    public WWWReWriteItem(String domainName,byte ruleType) { 
      _wwwRuleDomain = domainName;
      _wwwRuleType = ruleType;
      _lastUpdateTime = System.currentTimeMillis();
    }
    
    public String  _wwwRuleDomain = null;
    public long    _lastUpdateTime = -1;    
    public byte    _wwwRuleType = 0;
  };
  
  private static final int MAX_REWRITE_ITEMS = 5;
  
  IntrusiveList<WWWReWriteItem> _rewriteItemList = null;

  
  static class DNSCacheItem extends IntrusiveList.IntrusiveListElement<DNSCacheItem> {
    
    public DNSCacheItem(String hostName,int ipAddress,long ttl) { 
      _hostName = hostName;
      _ipAddress = ipAddress;
      _ttl       = ttl;
      _lastAccessTime = System.currentTimeMillis();
    }
    
    public String _hostName;
    public int    _ipAddress;
    public long   _ttl;
    public long   _lastAccessTime = -1;
  }

  IntrusiveList<DNSCacheItem> _dnsCacheItem = new IntrusiveList<DNSCacheItem>();
  
  public void cacheDNSEntry(String hostName,int ipAddress,long ttl) { 
    DNSCacheItem oldestItem = null;
    DNSCacheItem found = null;
    for (DNSCacheItem item : _dnsCacheItem) { 
      if (item._hostName.equals(hostName)) { 
        item._ipAddress = ipAddress;
        item._ttl = ttl;
        item._lastAccessTime = System.currentTimeMillis();
        found = item;
      }
      oldestItem = (oldestItem == null) ? item : (oldestItem._lastAccessTime > item._lastAccessTime) ? item : oldestItem;
    }
    
    if (found == null) { 
      if (_dnsCacheItem.size() == MAX_DNS_CACHE_ITEMS) {
        //LOG.info("###DNS Cache Full for Host:" + getListName() + " Flushing Host:" + oldestItem._hostName);
        _dnsCacheItem.removeElement(oldestItem);
      }
      _dnsCacheItem.addHead(new DNSCacheItem(hostName,ipAddress,ttl));
    }
    else { 
      _dnsCacheItem.removeElement(found);
      _dnsCacheItem.addHead(found);
    }
  }
  
  private void addWWWReWriteItem(String originalItem,byte itemType) { 
    WWWReWriteItem oldestItem = null;
    WWWReWriteItem found = null;
    if (_rewriteItemList == null) { 
      _rewriteItemList = new IntrusiveList<WWWReWriteItem>();
    }
    
    for (WWWReWriteItem item : _rewriteItemList) { 
      if (item._wwwRuleDomain.equals(originalItem)) { 
        item._lastUpdateTime = System.currentTimeMillis();
        found = item;
      }
      oldestItem = (oldestItem == null) ? item : (oldestItem._lastUpdateTime > item._lastUpdateTime) ? item : oldestItem;
    }
    if (found == null) { 
      if (_rewriteItemList.size() == MAX_REWRITE_ITEMS) { 
        _rewriteItemList.removeElement(oldestItem);
      }
      _rewriteItemList.addHead(new WWWReWriteItem(originalItem,itemType));
    }
    if (found != null && found != _rewriteItemList.getHead()) { 
      _rewriteItemList.removeElement(found);
      _rewriteItemList.addHead(found);
    }
  }
  
  
  public CrawlList(CrawlListHost crawlHost,int baseListId) { 
	  _host = crawlHost;
	  _baseListId = baseListId;
	  _uniqueListId = (((long)crawlHost.getIPAddress()) << 32) | _baseListId;
	  _listName = "List:" + _baseListId + " For:" + IPAddressUtils.IntegerToIPAddressString(crawlHost.getIPAddress());
	  _robotsRetrieved = false;
	  _disposition = Disposition.QueueEmpty;
  }
  
  /** host access **/
  public CrawlListHost getHost() { 
    return _host;
  }
  
  /** server access **/
  static CrawlerServer getServerSingleton() { 
  	return _server;
  }
  
  static void setServerSingleton(CrawlerServer server) { 
  	_server = server;
  }
  
  public int getListId() { 
    return _baseListId;
  }
  
  public long getUniqueListId() { 
    return _uniqueListId;
  }
  
  /** host name **/
  public String getListName() { 
    return _listName;
  }
  
  /** disposition **/
  Disposition getDisposition() {
    return _disposition;
  }
  
  void updateLastModifiedTime(long time) {
    _host.updateLastModifiedTime(time);  
  }
  
  /** get next crawl interface used to service this list 
   * 
   */
  public int getNextCrawlInterface() { 
    return _nextCrawlInterface;
  }
  
  /** set next crawl interface to use for this list 
   * 
   */
  public void setNextCrawlInterface(int crawlInterface) { 
    _nextCrawlInterface = crawlInterface;
  }
  
  /** get the pending urls count **/
  synchronized int getPendingURLCount() {
    return _pending.size();
  }
  
  /** get the offline url count 
   */
  synchronized int getOfflineURLCount() { 
    return _offlineTargetCount;
  }
  
  boolean isScheduled() { 
    return _scheduled != null;
  }
  
  int getActiveURLCount() { 
    return (_scheduled != null) ? 1 : 0;
  }
  
  void updateLastFetchStartTime(long newTime) { 
    _fetchStartTime = newTime;
    getHost().updateLastFetchStartTime(newTime);
  }
  
  long getLastFetchStartTime() { 
    return _fetchStartTime;
  }
  
  /** get fetch time in milliseconds for last request **/
  int getLastRequestFetchTime() { 
    if (_fetchStartTime != -1 && _fetchEndTime != -1){ 
      return (int) Math.max(0,_fetchEndTime - _fetchStartTime);
    }
    return 0;
  }
  
  int getLastSuccessfulDownloadTime() { 
    return _lastRequestDownloadTime;
  }
  
  public synchronized void stopCrawl() { 
     
    // add anything scheduled to pending ... 
    if (_scheduled != null) {
      _pending.addTail(_scheduled);
      _scheduled = null;
    }
    if (_pending.size() != 0 || !_robotsRetrieved) {
      _disposition = Disposition.ItemAvailable;
    }
    else { 
      _disposition = Disposition.QueueEmpty;
    }
  }
  
  
  /** add a new crawl target to the host queue **/
  public synchronized void addCrawlTarget(CrawlTarget target,boolean toFrontOfQueue) {
    
    // LOG.info("DOMAIN:" + this.getHostName() + " ADDING TGT:" + target.getOriginalURL());
    
    Disposition oldDisposition = _disposition;
     
    if (toFrontOfQueue) {
      target.setFlags(target.getFlags() | CrawlURL.Flags.IsHighPriorityURL);
      _pending.addHead(target);
    }
    else {
      // if offline != 0 add to pending 
      if (_offlineTargetCount == 0) { 
        _pending.addTail(target);
      }
      // otherwise add to queued 
      else {  
        //LOG.info("### QUEUED Adding to Queued List for CrawlList:" + getListName());
        _queued.addTail(target);
      }
    }
    
    if (_disposition == Disposition.QueueEmpty) { 
      _disposition = Disposition.ItemAvailable;
    }

    if (oldDisposition != _disposition && getHost().getActiveList() == this) { 
      getHost().listDispositionChanged(this, oldDisposition, _disposition);
    }
    
    if (_pending.size() >= DISK_FLUSH_THRESHOLD || _queued.size() != 0) {
      if (!_diskRequestPending) {  
        _diskRequestPending = true;
        _diskOperationQueue.add(new DiskQueueEntry(this,false));
      }
    }
  }
     
  private boolean activeDomainRequiresRobotsFetch(String activeDomainName) {
    
    if (_robotsRetrieved && _robotsHostName != null && _robotsHostName.equalsIgnoreCase(activeDomainName)) {
      // no, the active robots file matches the active domain name. no need to fetch anything ... 
      return false;
    }
    else {
      
      DomainInfo domainInfo = getDomainInfoFromDomain(activeDomainName);
      
      // get the cached crc for the active domain if it exists ... 
      long cachedRobotsCRC = (domainInfo == null) ? -1 : domainInfo._robotsCRC;
      
      // if cached crc found ...   
      if (cachedRobotsCRC != -1) { 
        // if cached robots file matches the actvie robots file's crc ... 
        if (_robotsRetrieved && cachedRobotsCRC == _robotsCRC) {
          //LOG.info("### Skipping Robots Fetch. Cached CRC == robotsCRC");
          // no need to refetch 
          return false;
        }
        // otherwise, check the host's cache ... 
        else { 
          // special case for the empty rule set 
          if (cachedRobotsCRC == 0) {
            
            _robotsCRC = cachedRobotsCRC;
            _robotsHostName = activeDomainName;
            _robotsReturned400 = domainInfo._robotsReturned400;
            _robotsReturned403 = domainInfo._robotsReturned403;
            _ruleSet = RobotRulesParser.getEmptyRules();
            _robotsRetrieved = true;
            if (Environment.detailLogEnabled())
              LOG.info("### Skipping Robots Fetch. Cached CRC is Zero, indicating empty rule set.");
            
            return false;
          }
          else {
            
            // check the rule set cache in the host (by crc)
            RobotRuleSet ruleSet = _host.getCachedRobotsEntry(cachedRobotsCRC);
            // if cached object found .... 
            if (ruleSet != null) { 
              _robotsCRC = cachedRobotsCRC;
              _robotsHostName = activeDomainName;
              _robotsReturned400 = domainInfo._robotsReturned400;
              _robotsReturned403 = domainInfo._robotsReturned403;
              _ruleSet = ruleSet;
              _robotsRetrieved = true;
              
              if (Environment.detailLogEnabled())
                LOG.info("### Skipping Robots Fetch. Cached CRC is Non-Zero and cached rule-set found via host.");
              
              return false;
            }
          }
        }
      }
    }
    return true;
  }
  
  public void setActiveDomainName(String hostName) {
    _activeDomainInfo = getDomainInfoFromDomain(hostName);
  }
  
  public DomainInfo getActiveDomain() { 
    return _activeDomainInfo;
  }

  private DomainInfo getDomainInfoFromDomain(String domainName) { 
    DomainInfo oldestItem = null;
    DomainInfo found = null;
    for (DomainInfo item : _domainInfo) { 
      if (item._domainName.equals(domainName)) {
      	if (getServerSingleton() != null) { 
	        if (item._lastTouched < getServerSingleton().getFilterUpdateTime()) {
	        	if (CrawlerServer.getEngine() != null) { 
	        		item._domainBlackListed = CrawlerServer.getEngine().isBlackListedHost(domainName);
	        	}
	        	else { 
	        		item._domainBlackListed = false;
	        	}
	        }
      	}
        item._lastTouched = System.currentTimeMillis();
        found = item;
      }
      oldestItem = (oldestItem == null) ? item : (oldestItem._lastTouched > item._lastTouched) ? item : oldestItem;
    }
    
    if (found == null) { 
      if (_domainInfo.size() == MAX_DOMAIN_CACHE_ENTIRES) { 
        _domainInfo.removeElement(oldestItem);
      }
      found = new DomainInfo();
      
      found._domainName = domainName;
      found._lastTouched = System.currentTimeMillis();
      if (getServerSingleton() != null) { 
      	if (CrawlerServer.getEngine() != null) { 
      		found._domainBlackListed = CrawlerServer.getEngine().isBlackListedHost(domainName);
      	}
      	else { 
      		found._domainBlackListed = false;
      	}
      }
      _domainInfo.addHead(found);
    }    
    return found;
  }
  
  private long checkDomainCacheForRobotsCRC(String hostName){ 
    for (DomainInfo aliasInfo : _domainInfo) { 
      if (aliasInfo._domainName.equalsIgnoreCase(hostName)) {
        aliasInfo._lastTouched = System.currentTimeMillis();
        _domainInfo.removeElement(aliasInfo);
        _domainInfo.addHead(aliasInfo);
        if (aliasInfo._robotsCRC != -1) { 
          if (Environment.detailLogEnabled())
            LOG.info("### Found Robots Match in Cache for host:" + hostName);          
        }
        return aliasInfo._robotsCRC;
      }
    }
    return -1;
  }
  
  
  
  private void updateRobotsCRCForDomain(long crc,String domainName,boolean robotsReturned400,boolean robotsReturned403) {
    getDomainInfoFromDomain(domainName)._robotsCRC = crc;
    getDomainInfoFromDomain(domainName)._robotsReturned400 = robotsReturned400;
    getDomainInfoFromDomain(domainName)._robotsReturned403 = robotsReturned403;
  }
  
  private void resetRobotsState() { 
    // flip robots status ... 
    _robotsRetrieved = false;
    _robotsReturned400 = false;
    _robotsReturned403 = false;
    _robotsHostName = null;
    _robotsCRC = 0;
    _ruleSet = RobotRulesParser.getEmptyRules();
  }    
  
  private CrawlTarget buildRobotsRequest(String hostName) { 

    CrawlTarget targetOut = null;
    
    // reset the robots state ... 
    resetRobotsState();
    
    // log the situation 
    // LOG.info("####Robots-fetching robots for host:" + hostName);
    
    // and set up some initial robots state 
    _robotsHostName = hostName;

    //build a robots.txt url
    URL robotsURL = null;
    try {
      robotsURL = new URL(getHost().getScheme(),_robotsHostName,"/robots.txt");
    } catch (MalformedURLException e) {
      
    }
        
    if (robotsURL == null) {
      if (Environment.detailLogEnabled())
        LOG.error("####Robots Unable to fetch Robots for host:"+ _robotsHostName);
      // cheat 
      _robotsRetrieved = true;
      // and update the robot info in the alias map 
      updateRobotsCRCForDomain(_robotsCRC, _robotsHostName,_robotsReturned400,_robotsReturned403);
    }
    else {
      // ok , the robots url is good 
      targetOut = new CrawlTarget(0,this);
      // set the url ... 
      targetOut.setOriginalURL(robotsURL.toString());
      // and mark the target as a robots get 
      targetOut.setFlags(CrawlURL.Flags.IsRobotsURL);
      
      CrawlerStats crawlerStats = CrawlerServer.getEngine().getCrawlerStats();
      
      synchronized (crawlerStats) { 
        crawlerStats.setActvieRobotsRequests(crawlerStats.getActvieRobotsRequests() + 1);
      }
      
    }
    return targetOut;
  }
  
  public boolean populateIPAddressForTarget(String hostName,CrawlTarget target) { 
    for (DNSCacheItem item : _dnsCacheItem) { 
      if (item._hostName.equalsIgnoreCase(hostName)) { 
        if (item._ttl >= System.currentTimeMillis()) { 
          //LOG.info("###Using Cached IP Address for target:" + target.getActiveURL() + " Cached IP:" + item._ipAddress + " TTL:" + item._ttl);
          target.setServerIP(item._ipAddress);
          target.setServerIPTTL(item._ttl);
          return true;
        }
        return false;
      }
    }
    return false;
  }
  
  private void applyRewriteRulesToTarget(String hostName,CrawlTarget target) {
    
    if(_rewriteItemList != null) { 
      for (WWWReWriteItem item : _rewriteItemList) { 
        if (item._wwwRuleDomain.equalsIgnoreCase(hostName)) { 
          if ((item._wwwRuleType & WWWRULE_Add) != 0) { 
            target.setOriginalURL(target.getOriginalURL().replaceFirst(hostName, "www." + hostName));
          }
          else { 
            target.setOriginalURL(target.getOriginalURL().replaceFirst(hostName, hostName.substring(4)));
          }
          break;
        }
      }
    }
  }
  
  static CrawlURLMetadata rewriteTestMetadata = new CrawlURLMetadata();
  static FilterResults rewriteFilterResults = new FilterResults();
  
  /** get next crawl candidate */
  public synchronized CrawlTarget getNextTarget() { 

    if (_scheduled != null) { 
      throw new RuntimeException("Scheduled Not Null and getNextTarget called!");
    }
    
    int maxRobotsExclusionInLoop = (CrawlerServer.getServer() != null) ? CrawlerServer.getServer().getMaxRobotsExlusionsInLoopOverride() : -1;
    if (maxRobotsExclusionInLoop == -1) { 
      maxRobotsExclusionInLoop = MAX_ROBOTS_EXCLUSION_IN_LOOP;
    }
    
    // target out is currently null
    CrawlTarget targetOut = null;
    
    Disposition oldDisposition = _disposition;
    
    int robotsExcludedCount = 0;
    int failedTargetsCount = 0;
    
    String domainName = "";
    
    while (targetOut == null && getNextPending(false) != null && getDisposition() == CrawlList.Disposition.ItemAvailable) { 
      
      // pop the next target off of the queue ... 
      CrawlTarget potentialTarget = getNextPending(true);
      
      // mark request start time 
      potentialTarget.setRequestStartTime(System.currentTimeMillis());
      
      
      // get the host name (fast)
      domainName = URLUtils.fastGetHostFromURL(potentialTarget.getActiveURL());
      
      // if not valid ... fail explicitly 
      if (domainName == null || domainName.length() == 0) { 
        // explicitly fail this url ... 
        CrawlTarget.failURL(potentialTarget.createFailureCrawlURLObject(CrawlURL.FailureReason.MalformedURL, null),potentialTarget, CrawlURL.FailureReason.MalformedURL,null);
      }
      else {

        /*
        // potentially rewrite domain name 
        if (getServer().getDNSRewriteFilter() != null) { 
          synchronized (rewriteTestMetadata) { 
            if (getServer().getDNSRewriteFilter().filterItem(domainName, "", rewriteTestMetadata, rewriteFilterResults) == FilterResult.Filter_Modified) {
              LOG.info("### FILTER Rewrote DomainName:" + domainName + " To:" + rewriteFilterResults.getRewrittenDomainName());
              domainName = rewriteFilterResults.getRewrittenDomainName();
            }
          }
        }
        */
        
        // set the active host name 
        setActiveDomainName(domainName);
        
        // check to see if the domain has been marked as failed or the host has been marked as failed ...
        if (!getActiveDomain()._domainFailed && !getActiveDomain()._domainBlackListed && !_host.isFailedServer()) { 
        
          // if the the active target does not match the current robots file ...  
          if (activeDomainRequiresRobotsFetch(domainName)) {
            
            // add the target back to the head of the queue... 
            _pending.addHead(potentialTarget);
  
            // and build a robots request ... 
            targetOut = buildRobotsRequest(domainName);
            
          }
          // otherwise ... go ahead try to fetch the next url in the queue 
          else {
            
            // now if disposition is still item available ... 
            if (potentialTarget != null && _disposition == Disposition.ItemAvailable) { 
                  
              targetOut = potentialTarget;
              
              URL theTargetURL = null;
              
              try {
                theTargetURL = new URL(targetOut.getOriginalURL());
              } catch (MalformedURLException e) {
                theTargetURL = null;
                LOG.error("Error parsing URL:"+targetOut.getOriginalURL() + " for Host:"+ domainName);
              }
              
              if (theTargetURL == null) { 
                // explicitly fail this url ... 
                CrawlTarget.failURL(targetOut.createFailureCrawlURLObject(CrawlURL.FailureReason.MalformedURL, null), targetOut,CrawlURL.FailureReason.MalformedURL,null);
                
                // and set target out to null!!
                targetOut = null;
              }
              else { 
                
                boolean robotsExcluded = !_ruleSet.isAllowed(theTargetURL);
                boolean serverExcluded = false;
                if (!robotsExcluded) { 
                  serverExcluded = CrawlerServer.getServer().isURLInBlockList(theTargetURL);
                }
                
                // validate against the robots file ...
                if (robotsExcluded || serverExcluded) { 
                  //track number of robots exclusion in this loop 
                  ++robotsExcludedCount;
                  // inform host 
                  _host.incrementCounter(CrawlListHost.CounterId.RobotsExcludedCount, 1);
                  // explicitly fail this url ...
                  if (robotsExcluded) {
                    if (Environment.detailLogEnabled())
                      LOG.info("### ROBOTS Excluded URL:" + theTargetURL + " via Robots File");
                    CrawlTarget.failURL(targetOut.createFailureCrawlURLObject(CrawlURL.FailureReason.RobotsExcluded, null),targetOut, CrawlURL.FailureReason.RobotsExcluded,null);
                  }
                  else {  
                    if (Environment.detailLogEnabled())
                      LOG.info("### ROBOTS Excluded URL:" + theTargetURL + " via Blacklist");
                    CrawlTarget.failURL(targetOut.createFailureCrawlURLObject(CrawlURL.FailureReason.BlackListedURL, null),targetOut, CrawlURL.FailureReason.BlackListedURL,null);
                  }

                  // and set target out to null
                  targetOut = null;
                  
                  
                  // if robots processed in loop exceeds maximum 
                  if (robotsExcludedCount >= maxRobotsExclusionInLoop) { 
                    
                    if (_pending.size() != 0 || _offlineTargetCount != 0) { 
                      // wait on time ...
                      _disposition = Disposition.WaitingOnTime;
                    }
                    else { 
                      _disposition = Disposition.QueueEmpty;
                    }
                    // and break out
                    break;
                  }
                }
                //
              }
            }
          }
        }
        // otherwise ... if the domain has failed ... 
        else {
          if (potentialTarget != null) {
            int failureReason = CrawlURL.FailureReason.TooManyErrors;
            String failureDesc = "Host Failed due to too many errors";
            if (getActiveDomain()._domainBlackListed) { 
              failureReason = CrawlURL.FailureReason.BlackListedURL;
              failureDesc = "Host Black Listed";
            }
            else if (getHost().isBlackListedHost()) { 
              failureReason = CrawlURL.FailureReason.BlackListedHost;
              failureDesc = "Host Black Host";
            }
            
            // fail the url and move on ...
            //TODO: DISABLING THIS BECAUSE FAILING FOR ABOVE REASONS IS NOT REALLY A PERSISTENT FAILURE ATTRIBUTABLE TO THE URL 
            // CrawlTarget.failURL(potentialTarget.createFailureCrawlURLObject(failureReason, failureDesc),potentialTarget, failureReason,null);
          }
          // set 
          targetOut = null;
          // increment failed item count 
          failedTargetsCount++;
          // now, if failed count exceeds max failures in loop 
          if (failedTargetsCount >= MAX_FAILED_TARGETS_IN_LOOP) { 
            
            if (_pending.size() != 0 || _offlineTargetCount != 0) { 
              // wait on time ...
              _disposition = Disposition.WaitingOnTime;
            }
            else { 
              _disposition = Disposition.QueueEmpty;
            }
            // and break out
            break;
          }
        }
      }
    }

    // ok, if we have a target ... fetch it ... 
    if (targetOut != null) { 
    	
    	// ok before we can fetch this guy, we need to check to see if the associated host is in a paused state ... 
    	if (_host.isPaused()) { 
    		LOG.info("***getNextItem for Host:" + domainName + " is Paused!!");
    		// null target out, which will set us in a waiting on time state again 
    		targetOut = null;
    	}
    	
    	// now again, if target out is not null 
    	if (targetOut != null) { 
    	
	      if (Environment.detailLogEnabled())
	        LOG.info("getNextItem for Host:" + domainName + " Returned URL:" + targetOut.getOriginalURL() + " object:" + targetOut.toString());
	      // set scheduled item pointer ... 
	      _scheduled = targetOut;
	
	      // set the active host name 
	      setActiveDomainName(domainName);
	      // get ip address info (if available)
	      populateIPAddressForTarget(domainName,_scheduled);
	      
	      // change disposition ... 
	      _disposition = Disposition.WaitingOnCompletion;
	
	      //set initial fetch start time ... 
	      updateLastFetchStartTime(System.currentTimeMillis());
    	}
    }

    
    // if target out is null, and pending size is zero but there are offline targets ... 
    if (targetOut == null) {
      
      if (_pending.size() != 0 || _offlineTargetCount != 0) {
        // then set disposition to waiting on time ... 
        _disposition = CrawlList.Disposition.WaitingOnTime;
      }
      else { 
        _disposition = CrawlList.Disposition.QueueEmpty;
      }
    }
    
    // check to see if we need to load more items from disk 
    potentiallyQueueDiskLoad();
    
    if (targetOut != null) {
      // finally rewrite target url if necessary 
      applyRewriteRulesToTarget(domainName,targetOut);
    }
    
    if (targetOut != null) { 
      // LOG.info("### getNextIem for Host:" + domainName + " Returned URL:" + targetOut.getActiveURL());
    }
    return targetOut;
  }
  
  
  void fetchStarting(CrawlTarget target,NIOHttpConnection connection) { 
    _activeConnection = connection;
  }
  
  
  /** fetch started callback - called from CrawlTarget **/
  void fetchStarted(CrawlTarget target) { 

    // record fetch start time ...  
    updateLastFetchStartTime(System.currentTimeMillis());
    
    // and notify host as well 
    _host.updateLastFetchStartTime(getLastFetchStartTime());
    
    if (_scheduled == target) { 

      if (Environment.detailLogEnabled())  
        LOG.info("Fetch Started URL:" + target.getOriginalURL());
    }
    else {
      if (_scheduled == null) { 
        LOG.error("fetchStarted - scheduled target is null and fetch started target is:" + target.getOriginalURL().toString() + " list:" + target.getSourceList().getListName() );
      }
      else { 
        LOG.error ( "fetchStarted - scheduled target is: " + _scheduled.getOriginalURL().toString() +" list:" +_scheduled.getSourceList().getListName() + " and fetch started target is:" + target.getOriginalURL().toString() + " list:" + target.getSourceList().getListName() );
      }
    }
    
  }
  
  /** if in memory queue is exhausted or below threshold and there offline targets, queue up a load from disk for this domain **/
  private void potentiallyQueueDiskLoad() { 
    if (_pending.size() <= DISK_LOAD_THRESHOLD && (!_diskRequestPending || _pending.size() ==0) && _offlineTargetCount != 0) { 
      _diskRequestPending = true;
      _diskOperationQueue.add(new DiskQueueEntry(this,true));
    }
  }
  
    
  static class RobotRuleResult { 
    public RobotRuleSet ruleSet;
    public long         crcValue;
  };
  
  /** fetch succeeded **/
  void fetchSucceeded(final CrawlTarget target,int downloadTime,final NIOHttpHeaders httpHeaders,final Buffer contentBuffer) { 
   
    _lastRequestWasIOException = false;
    _lastRequestDownloadTime  = downloadTime;
    _lastRequestRedirectCount = target.getRedirectCount();
    _fetchEndTime = System.currentTimeMillis();
    
    _activeConnection = null;
    
    getHost().incrementCounter(CrawlListHost.CounterId.SuccessfullGetCount,1);        
        
    // reset host's io error count
    _host.resetCounter(CrawlListHost.CounterId.ConsecutiveIOErrorCount);
    
    if (getActiveDomain() != null)
      getActiveDomain()._domainRetryCounter = 0;
    
    Disposition oldDisposition = _disposition;
    
    final String originalHost  = URLUtils.fastGetHostFromURL(target.getOriginalURL());
    final String activeHost    = URLUtils.fastGetHostFromURL(target.getActiveURL());
    
    if (originalHost != null && activeHost != null) { 
	    // update our server ip information from information contained within crawl target ...
	    cacheDNSEntry(activeHost,target.getServerIP(),target.getServerIPTTL());
	    // if the target was redirected ... cache the original ip address and ttl as well ... 
	    if (target.isRedirected()) { 
	      if (target.getOriginalRequestData() != null) { 
	        cacheDNSEntry(originalHost,target.getOriginalRequestData()._serverIP,target.getOriginalRequestData()._serverIPTTL);
	      }
	    }
    }
    
    final int resultCode = NIOHttpConnection.getHttpResponseCode(httpHeaders);
    
    if (resultCode == 200){
      
      getHost().incrementCounter(CrawlListHost.CounterId.Http200Count,1);        

      if (getActiveDomain() != null) {  
        getActiveDomain()._HTTP200Count++;
        getActiveDomain()._SequentialHTTPFailuresCount = 0;
      }
      
      // validate www rewrite rule if not set and target was redirected ... 
      if (target.isRedirected()) { 
        /* this is broken for the new list design
        if (!originalHost.equalsIgnoreCase(activeHost)) { 
          // if redirect strips the www then ... 
          if ((originalHost.startsWith("www.") || originalHost.startsWith("WWW.")) && activeHost.equalsIgnoreCase(originalHost.substring(4))) { 
            addWWWReWriteItem(originalHost,WWWRULE_Remove);
          }
          // else if redirect adds the www then ...
          else if ((activeHost.startsWith("www.") || activeHost.startsWith("WWW.")) && originalHost.equalsIgnoreCase(activeHost.substring(4))) {
            addWWWReWriteItem(originalHost,WWWRULE_Add);
          }
        }
        */
      }
    }
    else if (resultCode >= 400 && resultCode < 500) {
      if (resultCode == 403) { 
        // inform host for stats tracking purposes 
        _host.incrementCounter(CrawlListHost.CounterId.Http403Count,1);
      }
      if (getActiveDomain() != null) 
        getActiveDomain()._SequentialHTTPFailuresCount++;
    }
    else if (resultCode >=500 && resultCode < 600) {
      if (getActiveDomain() != null) {
        getActiveDomain()._SequentialHTTPFailuresCount++;
      }
    }
    
    if (_scheduled != target) { 
      if (_scheduled == null)
        LOG.error("List:" + getHost().getIPAddressAsString() + " List:" + getListName() + " fetchSucceed Target is:" + target.getOriginalURL() + " ActiveTarget is NULL!");
      else 
        LOG.error("List:" + getHost().getIPAddressAsString() + " List:" + getListName() + " fetchSucceed Target is:" + 
            target.getOriginalURL() + " " + target.toString() + " ActiveTarget is:" + _scheduled.getOriginalURL() + " " + _scheduled.toString());
    }
    else { 
      
      // clear active ... 
      _scheduled = null;
      
      // if this is the robots target ... 
      if ( (target.getFlags() & CrawlURL.Flags.IsRobotsURL) == 1) { 
        
        final CrawlerStats crawlerStats = CrawlerServer.getEngine().getCrawlerStats();
        
        
        // process the robots data if any ... 
        // check for null queue (in case of unit test);
        if (resultCode == 200) { 
  
          _robotsRetrieved = true;
          
          synchronized (crawlerStats) { 
            crawlerStats.setRobotsRequestsSucceeded(crawlerStats.getRobotsRequestsSucceeded() + 1);
            crawlerStats.setRobotsRequestsQueuedForParse(crawlerStats.getRobotsRequestsQueuedForParse() + 1);
          }

          LOG.info("### Scheduling Robots Parse for:"+target.getActiveURL());
          
          // transition to a waiting on completion disposition ... 
          _disposition = Disposition.WaitingOnCompletion;

          
          
          if (getServerSingleton() != null) { 
            // schedule a robots parser parse attempt ... 
          	getServerSingleton().registerThreadPool("robots", 5).execute(new ConcurrentTask<RobotRuleResult>(getServerSingleton().getEventLoop(), 
                
                new Callable<RobotRuleResult>() {
        
                  public RobotRuleResult call() throws Exception {
                    
                    try {
                      
                      TextBytes contentData = new TextBytes(contentBuffer.get());
                      
                      String contentEncoding = httpHeaders.findValue("Content-Encoding");
                      
                      if (contentEncoding != null && contentEncoding.equalsIgnoreCase("gzip")) {
  
                        if (Environment.detailLogEnabled())
                          LOG.info("GZIP Encoding Detected for Robots File For:"+activeHost);
  
                        UnzipResult result = GZIPUtils.unzipBestEffort(contentData.getBytes(),CrawlEnvironment.CONTENT_SIZE_LIMIT);
  
                        if (result == null) {
                          contentData = null;
                          if (Environment.detailLogEnabled())
                            LOG.info("GZIP Decoder returned NULL for Robots File For:"+activeHost);
                        }
                        else { 
                          contentData.set(result.data.get(),result.data.getOffset(),result.data.getCount());
                        }
                      }

                      try {
                        if (contentData != null) { 
                          String robotsTxt = contentData.toString().trim().toLowerCase();
                          if (robotsTxt.startsWith("<html") || robotsTxt.startsWith("<!doctype html")) { 
                            contentData = null;

                            CrawlerServer.getEngine().logRobots(System.currentTimeMillis(),_robotsHostName, 
                                resultCode, null,CrawlerEngine.RobotsLogEventType.HTTP_GET_Complete,CrawlerEngine.RobotsParseFlag_ContentWasHTML);
                          }
                          else { 
                            CrawlerServer.getEngine().logRobots(System.currentTimeMillis(),_robotsHostName, 
                                resultCode, robotsTxt,CrawlerEngine.RobotsLogEventType.HTTP_GET_Complete,0);
                            
                            synchronized (this) { 
                              _lastFetchedRobotsData = robotsTxt;
                              _lastFetchedRobotsHostName = _robotsHostName;
                            }
                          }
                        }
                        else { 
                          CrawlerServer.getEngine().logRobots(System.currentTimeMillis(),_robotsHostName, 
                              resultCode, null,CrawlerEngine.RobotsLogEventType.HTTP_GET_Complete,
                              CrawlerEngine.RobotsParseFlag_ContentDecodeFailed);
                        }
                      }
                      catch (Exception e) { 
                        LOG.error(CCStringUtils.stringifyException(e));
                        CrawlerServer.getEngine().logRobots(System.currentTimeMillis(),_robotsHostName, 
                            resultCode, null, CrawlerEngine.RobotsLogEventType.HTTP_GET_Complete,
                            CrawlerEngine.RobotsParseFlag_ContentDecodeFailed);
                      }
                      
                      if (Environment.detailLogEnabled())
                        LOG.info("Parsing Robots File for Host:"+activeHost);
                      RobotRuleResult result = new RobotRuleResult();
                      
                      if (contentData != null) { 
                        synchronized (_crc32) { 
                          _crc32.reset();
                          _crc32.update(contentData.getBytes(),contentData.getOffset(),contentData.getLength());
                          result.crcValue = _crc32.getValue();
                        }
                        RobotRulesParser parser = new RobotRulesParser(getServerSingleton().getConfig());
                        result.ruleSet = parser.parseRules(contentData.getBytes(),contentData.getOffset(),contentData.getLength()); 
                      }
                      else {
                        result.ruleSet = RobotRulesParser.getEmptyRules();
                        result.crcValue = 0;
                      }
                      return result;
                    }
                    catch (Exception e) { 
                      LOG.error(CCStringUtils.stringifyException(e));
                      throw e;
                    }
                  }
                }, 
        
                new ConcurrentTask.CompletionCallback<RobotRuleResult>() {
        
                  public void taskComplete(RobotRuleResult  loadResult) {

                    synchronized (crawlerStats) { 
                      crawlerStats.setRobotsRequestsQueuedForParse(crawlerStats.getRobotsRequestsQueuedForParse() - 1);
                    }
                    
                    if (loadResult != null) { 

                      boolean disallowsAll = !_ruleSet.isAllowed("/");
                      boolean robotsHadCrawlDelay = _ruleSet.getCrawlDelay() != -1;
                      boolean explicitMention   = _ruleSet.explicitMention;
                      
                      int logFlags = 0;
                      if (disallowsAll)
                        logFlags |= CrawlerEngine.RobotsParseFlag_ExcludesAll;
                      if (explicitMention)
                        logFlags |= CrawlerEngine.RobotsParseFlag_ExplicitMention;
                      if (robotsHadCrawlDelay)
                        logFlags |= CrawlerEngine.RobotsParseFlag_HasCrawlDelay;
                      
                      synchronized (crawlerStats) { 
                        crawlerStats.setRobotsRequestsSuccessfullParse(crawlerStats.getRobotsRequestsSuccessfullParse() + 1);
                        if (disallowsAll) { 
                          crawlerStats.setRobotsFileExcludesAllContent(crawlerStats.getRobotsFileExcludesAllContent() + 1);
                          if (explicitMention)
                            crawlerStats.setRobotsFileExplicitlyExcludesAll(crawlerStats.getRobotsFileExplicitlyExcludesAll() + 1);
                        }
                        if (explicitMention) { 
                          crawlerStats.setRobotsFileHasExplicitMention(crawlerStats.getRobotsFileHasExplicitMention() + 1);
                        }
                        if (robotsHadCrawlDelay) { 
                          crawlerStats.setRobotsFileHadCrawlDelay(crawlerStats.getRobotsFileHadCrawlDelay() + 1);
                        }
                      }
                      
                      CrawlerServer.getEngine().logRobots(System.currentTimeMillis(),_robotsHostName, 
                          0 , null,CrawlerEngine.RobotsLogEventType.Parse_Succeeded,logFlags);

                      
                      _ruleSet    = loadResult.ruleSet;
                      _robotsCRC  = loadResult.crcValue;
                      
                      _host.cacheRobotsFile(_ruleSet, _robotsCRC);
                    }
                    else {

                      CrawlerServer.getEngine().logRobots(System.currentTimeMillis(),_robotsHostName, 
                          0 , null,CrawlerEngine.RobotsLogEventType.Parse_Failed,0);
                      
                      synchronized (crawlerStats) { 
                        crawlerStats.setRobotsRequestsFailedParse(crawlerStats.getRobotsRequestsFailedParse() + 1);
                      }
                      
                      // LOG.error("####Robots parsing for host:" + activeHost + " failed.");
                      _ruleSet    = RobotRulesParser.getEmptyRules();
                      _robotsCRC  = 0;
                    }
                    
                    //if (Environment.detailLogEnabled())
                      LOG.info("####Robots RETRIEVED for Host:"+activeHost + " CrawlDelay IS:" + getCrawlDelay(false));
                    
                    if (originalHost != null && activeHost != null) { 
	                    updateRobotsCRCForDomain(_robotsCRC, originalHost,_robotsReturned400,_robotsReturned403);
	                    if (activeHost.compareToIgnoreCase(originalHost) != 0) { 
	                      updateRobotsCRCForDomain(_robotsCRC, activeHost,_robotsReturned400,_robotsReturned403);
	                    }
                    }
                      
                    Disposition oldDisposition = _disposition;
                    
                    if (getNextPending(false) != null) {
                      _disposition = Disposition.ItemAvailable;
                    }
                    else { 
                      _disposition = Disposition.WaitingOnTime;
                    }
                    
                    if (oldDisposition != _disposition) { 
                      // notify queue 
                      getHost().listDispositionChanged(CrawlList.this, oldDisposition, _disposition);
                    }
                  }
    
                  public void taskFailed(Exception e) {
                    if (Environment.detailLogEnabled())
                      LOG.error("####Robots parsing for host:" + _robotsHostName +" failed with exception" + e);
                    _ruleSet = RobotRulesParser.getEmptyRules();
                    
                    Disposition oldDisposition = _disposition;
                    
                    if (getNextPending(false) != null) {
                      _disposition = Disposition.ItemAvailable;
                    }
                    else { 
                      _disposition = Disposition.WaitingOnTime;
                    }
                    
                    if (oldDisposition != _disposition) { 
                      // notify queue 
                      getHost().listDispositionChanged(CrawlList.this, oldDisposition, _disposition);
                    }                
                  } 
                }));
          }
          // explitly return here ( inorder to wait for the async completion event)
          return;
        }
        //otherwise ... 
        else {
         
          synchronized (crawlerStats) { 
            crawlerStats.setRobotsRequestsFailed(crawlerStats.getRobotsRequestsFailed() + 1);
          }
          
          
          CrawlerServer.getEngine().logRobots(System.currentTimeMillis(),_robotsHostName, 
              resultCode, null,CrawlerEngine.RobotsLogEventType.HTTP_GET_Failed,0);
          
          _robotsCRC = 0;
          if (Environment.detailLogEnabled())
            LOG.info("####Robots GET for Host:" + activeHost + "FAILED With Result Code:" + resultCode);
          //TODO: MAKE THIS MORE ROBUST ... 
          // clear robots flag ... 
          _robotsRetrieved = true;
          // see if result code was a 403 
          if (resultCode >= 400 && resultCode <= 499) {
            _robotsReturned400 = true;
            if (resultCode == 403)
              _robotsReturned403 = true;
              
          }
          // for now, assume no robots rules for any error conditions ... 
          _ruleSet = RobotRulesParser.getEmptyRules();
          
          if (originalHost != null && activeHost != null) { 
	          updateRobotsCRCForDomain(_robotsCRC, originalHost,_robotsReturned400,_robotsReturned403);
	          if (activeHost.compareToIgnoreCase(originalHost) != 0) { 
	            updateRobotsCRCForDomain(_robotsCRC, activeHost,_robotsReturned400,_robotsReturned403);
	          }
          }
          
        }
      }
  
      if (getServerSingleton() != null && getServerSingleton().failHostsOnStats()) { 
        // update active host stats and check for failure ... 
        checkActiveHostStatsForFailure();
      }
        
      // if there are no more items in the queue 
      if (getNextPending(false) == null) {
        // if offline count is zero then mark this domain's queue as empty
        if (_offlineTargetCount == 0) { 
          _disposition = Disposition.QueueEmpty;
        }
        // otherwise put us in a wait state and potentially queue up a disk load 
        else { 
          _disposition = Disposition.WaitingOnTime;
          // potentially queue up a disk load 
          potentiallyQueueDiskLoad();
        }
      }
      else { 
        // if we are ready to fetch the next item ... 
        if (calculateNextWaitTime() < System.currentTimeMillis()) { 
          _disposition = Disposition.ItemAvailable;
        }
        else { 
        // transition to a new wait state ... 
          _disposition = Disposition.WaitingOnTime;
        }
      }
        
      if (oldDisposition != _disposition) { 
        // either way ... notify queue 
        getHost().listDispositionChanged(this, oldDisposition, _disposition);
      }
    }
  }
  
  /** get total request count **/
  private final int getTotalFailureCount() {
    if (getActiveDomain() != null)
      return getActiveDomain()._HTTP400Count + getActiveDomain()._HTTP500Count;
    return 0;
  }
  
  /** check to see if we should fail the host based on collected stats **/
  private boolean checkActiveHostStatsForFailure() { 

    boolean failHost = false;
    
    if (getActiveDomain() != null) { 
      String errorReason = null;

      /*
      if (getActiveDomain()._SequentialHTTPFailuresCount >= SEQUENTIAL_FAILURES_ON_403_ROBOTS_TRIGGER && _robotsReturned403) { 
        errorReason  ="Too Many Sequential Errors AFTER Robots Returned 403. HTTP200 Count:" + getActiveDomain()._HTTP200Count;
        failHost =true;
      }
      */
      if ((getActiveDomain()._HTTP200Count == 0 && getActiveDomain()._SequentialHTTPFailuresCount >= SEQUENTIAL_FAILURES_NO_200_TRIGGER)) { 
        errorReason  ="Too Many Sequential Errors. RobotsReturned400:" + _robotsReturned400 + " 400 Count:" + getActiveDomain()._HTTP400Count + " 500 Count:" + getActiveDomain()._HTTP500Count + " 200 Count:" + getActiveDomain()._HTTP200Count;
        failHost =true;
      }
      else { 
  
        int totalFailureCount = getTotalFailureCount();
        int totalRequestCount = totalFailureCount + getActiveDomain()._HTTP200Count;
        
        if (totalRequestCount != 0 && totalRequestCount >= STATS_CHECK_CODE_SAMPLE_THRESHOLD) {
          
          float badToGoodPercent = (float)totalFailureCount / (float)totalRequestCount;
          if (badToGoodPercent >= BAD_URL_TO_TOTAL_URL_FAILURE_THRESHOLD) { 
            failHost = true;
            errorReason  ="Bad To Good URL Pct:" + badToGoodPercent +" exceeded Threshold:" +
            BAD_URL_TO_TOTAL_URL_FAILURE_THRESHOLD + " RobotsReturned400:" + _robotsReturned400 + 
            " 400 Count:" + getActiveDomain()._HTTP400Count + " 500 Count:" + getActiveDomain()._HTTP500Count + " 200 Count:" + getActiveDomain()._HTTP200Count;
          }
        }
      }
      
      if (failHost) { 
        failActiveDomain(CrawlURL.FailureReason.TooManyErrors, errorReason);
        LOG.error("#### HOST FAILURE - List:" + getListName() + "Host: " + getActiveDomain()._domainName +" Reason:" + errorReason);
      }
    }    
    return failHost;
  }
  
  private static final int FAIL_STRATEGY_RETRY_ITEM = 0;   // increment the failure count on the item and retry 
  private static final int FAIL_STRATEGY_RETRY_HOST = 1; //  increment the failure count on the host and retry ... 
  private static final int FAIL_STRATEGY_FAIL_ITEM = 2;     //  immediately fail the item ... 
//  private static final int FAIL_STRATEGY_FAIL_HOST  = 3;    // immediately fail the host ... 
    
  private static final int failureCodeStrategyTable[] = { 
    
    FAIL_STRATEGY_RETRY_ITEM, // UNKNOWN            - result: Inc Fail Count on Item, potentially reschedule 
    FAIL_STRATEGY_FAIL_ITEM,// UknownProtocol  - result: Immediately Fail Item  
    FAIL_STRATEGY_FAIL_ITEM,// MalformedURL     - result: Immediately Fail Item  
    FAIL_STRATEGY_RETRY_ITEM,// Timeout                - result: Inc Fail Count on Host, potentially reschedule
    FAIL_STRATEGY_FAIL_ITEM,// DNSFailure            -result: reschedule, set waitstate for Host
    FAIL_STRATEGY_RETRY_HOST,// ResolverFailure     -result: Inc Fail Count on Host, potentially reschedule
    FAIL_STRATEGY_RETRY_ITEM,// IOException          -result: Inc Fail Count on Item, potentially reschedule 
    FAIL_STRATEGY_FAIL_ITEM, // RobotsExcluded
    FAIL_STRATEGY_FAIL_ITEM,// NoData            = 9;
    FAIL_STRATEGY_RETRY_ITEM,// RobotsParseError  = 10;
    FAIL_STRATEGY_FAIL_ITEM,// RedirectFailed    = 11;
    FAIL_STRATEGY_RETRY_ITEM,// RuntimeError      = 12;
    FAIL_STRATEGY_RETRY_HOST,// ConnectTimeout    = 13;
    FAIL_STRATEGY_FAIL_ITEM,//BlackListedHost   = 14;
    FAIL_STRATEGY_FAIL_ITEM,//BlackListedURL    = 15;
    FAIL_STRATEGY_FAIL_ITEM,//TooManyErrors     = 16;
    FAIL_STRATEGY_FAIL_ITEM,//InCache           = 17;
    FAIL_STRATEGY_FAIL_ITEM// InvalidResponseCode = 18;
    
  };
  
  private void failURL(CrawlTarget target,int failureReason,String errorDescription) {
    
    // explicitly fail the item ... 
    CrawlTarget.failURL(target.createFailureCrawlURLObject(failureReason,errorDescription),target,failureReason,errorDescription);
  }
  
  
  private synchronized void failActiveDomain(int  failureReason,String errorDescription) { 
    
    if (getActiveDomain() != null) { 
     
      LOG.error("### Failing Active Domain:" + getActiveDomain()._domainName + " in List:" + getListName() + " ReasonCode:" + failureReason + " Description:" + errorDescription);
      
      // _disposition = Disposition.QueueEmpty;
      getActiveDomain()._domainFailed = true;
      
      /*
      // fail scheduled url  ... 
      if (_scheduled != null) { 
        _scheduled = null;
      }
      // just remove all pending urls from list for now ...
      _pending.removeAll();
      // reset offline count... 
      _offlineTargetCount = 0;
      // reset disk operation pending indiciator ... 
      _diskRequestPending = false;
      */
      getHost().incrementCounter(CrawlListHost.CounterId.FailedDomainCount,1);
      
      CrawlerServer.getEngine().failDomain(getActiveDomain()._domainName);
    }
  }
  
  synchronized void fetchFailed(CrawlTarget target, int failureReason,String description) { 

    _activeConnection = null;
    _lastRequestRedirectCount = target.getRedirectCount();
    _fetchEndTime = System.currentTimeMillis();
    
    getHost().incrementCounter(CrawlListHost.CounterId.FailedGetCount,1);
    
    if (getActiveDomain() != null) {  
      getActiveDomain()._SequentialHTTPFailuresCount++;
    }
    
    _lastRequestWasIOException = false;
    
    //check to see if the error is an io exception or a timeout 
    if (failureReason == CrawlURL.FailureReason.IOException || failureReason == CrawlURL.FailureReason.Timeout) { 
      // increment host failure counter ... 
      _host.incrementCounter(CrawlListHost.CounterId.ConsecutiveIOErrorCount,1);
      _lastRequestWasIOException = true;
    }
    
    // the rest is similar to a host retry strategy ... 
    Disposition oldDisposition = _disposition;
    
    if (_scheduled != target) { 
      if (_scheduled == null)
        LOG.error("Host:" + getHost().getIPAddressAsString() + " List:" + getListName() + " fetchFailed Target is:" + target.getOriginalURL() + " ActiveTarget is NULL!");
      else 
        LOG.error("Host:" + getHost().getIPAddressAsString() + " List:" + getListName() + " fetchFailed Target is:" + target.getOriginalURL() + " ActiveTarget is:" + _scheduled.getOriginalURL());
    }
    else { 
    
      // reset active and scheduled ... 
      _scheduled = null;
      
      // if we failed on the robots get ... 
      if ((target.getFlags() & CrawlURL.Flags.IsRobotsURL) == 1) { 
  
        CrawlerStats crawlerStats = CrawlerServer.getEngine().getCrawlerStats();
        
        synchronized (crawlerStats) { 
          crawlerStats.setRobotsRequestsFailed(crawlerStats.getRobotsRequestsFailed() + 1);
        }
        
        //TODO: FIGURE THIS OUT LATER ... FOR NOW .. ON A FAILURE OF ROBOTS.TXT GET, WE ASSUME NO ROBOTS.TXT FILE ... 
  
        //LOG.warn("Robots Fetch for host:"+getHostName() + " Failed with Reason:" + failureReason +" Desc:" + description);
        //LOG.warn("Assuming NO-ROBOTS FILE");
  
        CrawlerServer.getEngine().logRobots(System.currentTimeMillis(),_robotsHostName, 
            0 , null,CrawlerEngine.RobotsLogEventType.HTTP_GET_Failed,0);
        
        target.logFailure(CrawlerServer.getEngine(),failureReason,description);
        
        _robotsRetrieved = true;
        _ruleSet = RobotRulesParser.getEmptyRules();
  
        // and clear scheduled ... 
        _scheduled = null;
  
        updateLastFetchStartTime(-1);
        
        // and transition to wait state .... 
        _disposition = Disposition.WaitingOnTime;
      }
      // otherwise pass on to underlying crawl target handler ... 
      else { 
  
        // default failure strategy ... 
        int failureStrategy = FAIL_STRATEGY_RETRY_ITEM;
            
        // if failure code is within known failure codes ... 
        if (failureReason >= CrawlURL.FailureReason.UNKNOWN && failureReason <= CrawlURL.FailureReason.InvalidResponseCode) { 
          // use table to map strategy ... 
          failureStrategy = failureCodeStrategyTable[failureReason-1];
        }
        
        switch (failureStrategy) { 
        
          case FAIL_STRATEGY_RETRY_HOST:  
          case FAIL_STRATEGY_RETRY_ITEM: { 

            // increment retry counter ... 
            getActiveDomain()._domainRetryCounter ++;
            // increment retry counter on target ... 
            target.incrementRetryCounter();
            // IFF server failed ... 
            // OR retry count on item exceeded ... 
            // OR this item is a high priority dispatch item ...
            // THEN immediately fail this item 
            // ELSE queue up this item for subsequent retry
            if (_host.isFailedServer() || target.getRetryCount() >= MAX_ITEM_RETRY || ((target.getFlags() & CrawlURL.Flags.IsHighPriorityURL) != 0) ) {
              failURL(target,failureReason,description);
            }
            else { 
              // and add it back to the pending list ... 
              _pending.addTail(target);
            }
          }break;
    
          case FAIL_STRATEGY_FAIL_ITEM: { 
            failURL(target,failureReason,description);
          }break;
  
  /*        
          case FAIL_STRATEGY_FAIL_HOST: { 
            // just put the entire host in a fail state ...
            failDomain(failureReason,description);
          }break;
  */        
        }
              
        switch (failureStrategy) { 
          
          case FAIL_STRATEGY_RETRY_ITEM: 
          case FAIL_STRATEGY_FAIL_ITEM: 
          case FAIL_STRATEGY_RETRY_HOST: { 
            
            // check to see if there are items in the pending queue .... 
            if (_pending.size() == 0 && _offlineTargetCount == 0) { 
              // if not... transition to Queue Empty 
              _disposition = Disposition.QueueEmpty;
            }
            else {
              long waitTime = calculateNextWaitTime();
              // if we can fetch the next item ... 
              if (waitTime <= System.currentTimeMillis()) { 
                if (_pending.size() != 0)
                  // shift to an available disposition ... 
                  _disposition = Disposition.ItemAvailable;
                else 
                  // shift to waiting on time disposition (to wait for disk queue load).
                  _disposition = Disposition.WaitingOnTime;
              }
              else { 
                // wait on time ...
                _disposition = Disposition.WaitingOnTime;
              }
            }
          }
          break;
        }
        
        if (description == null)
          description = "";
        if (Environment.detailLogEnabled())
          LOG.error("Fetch Failed for URL:"+target.getOriginalURL() + " Reason:"+failureReason + " Description:" + description + " Strategy:"+failureStrategy + " OldDisp:" + oldDisposition + " NewDisp:" + _disposition);
      }
      
      
      if (_disposition == Disposition.WaitingOnCompletion) { 
        LOG.error("### BUG Fetch Faile for URL:" + target.getOriginalURL() +" failed to transition List to proper disposition!");
      }
      
      if (getServerSingleton() != null && getServerSingleton().failHostsOnStats()) { 
        // update active host stats and check for failure ... 
        checkActiveHostStatsForFailure();
      }
      
      // notify queue if disposition changed ... 
      if (_disposition != oldDisposition) { 
        getHost().listDispositionChanged(this,oldDisposition,_disposition);
      }
    }
  }
  
  
  
  /** clear a pre-existing wait state **/
  synchronized void  clearWaitState() { 
    
    Disposition oldDisposition = _disposition;
    
    // if robots retrieval is pending ... 
    if (_robotsRetrieved == false ) { 
      
     // LOG.debug("clearWaitState called on Host:"+getHostName()+ " after initial robots fetch");
      
      // explicitly transition to availabel (to retry robots fetch... )
      _disposition = Disposition.ItemAvailable;
    }
    // otherwise if pending queue size is zero or host has failed ... 
    else if ((_pending.size() == 0 && _offlineTargetCount  == 0 && _queued.size() == 0)) { 
      // LOG.debug("clearWaitState called on Host:"+getHostName()+ " and queue is empty. transitioning to QueueEmpty");
      // transition to queue empty disposition
      _disposition = Disposition.QueueEmpty;
    }
    else {

      // if active request size < max simulatenous requests ... 
      if (_scheduled == null) {
        // if there are items to be read from the in memory list ... 
        if (_pending.size() != 0) { 
          // LOG.error("clearWaitState called Host:"+getHostName()+ " and getNextPendingReturned object. transitioning to ItemAvailable");
          // immediately transition to an available state ... 
          _disposition = Disposition.ItemAvailable;
        }
        else { 
          if (!_diskRequestPending) { 
            _diskRequestPending = true;
            _diskOperationQueue.add(new DiskQueueEntry(this,true));
          }
          _disposition = Disposition.WaitingOnTime;
        }
      }
      // otherwise... we are waiting on completion now ... 
      else {
        LOG.warn("clearWaitState called on already scheduled list:"+getListName());
        _disposition = Disposition.WaitingOnCompletion;
      }
    }
    
    getHost().listDispositionChanged(this,oldDisposition,_disposition);
  }
  
  /** calculateRetryWaitTime */
  public long calculateNextWaitTime() {
  	
  	// ok check to see if the related host is paused ...
  	if (_host.isPaused()) { 
  		LOG.info("*** host is paused. pausing crawl for: " + PAUSE_STATE_RETRY_DELAY + " milliseconds");
  		// ok suspend for pause delay 
  		return System.currentTimeMillis() + PAUSE_STATE_RETRY_DELAY;
  	}
  	
    if (_fetchStartTime == -1) {
      return System.currentTimeMillis();
    }
    else {
      // first calculate crawl delay based on robots delay value * number of hops to service last request 
      //int crawlDelay = (getCrawlDelay(true) * (_lastRequestRedirectCount+1));
      int crawlDelay = getCrawlDelay(true);
      
      // if the crawl delay is the default host crawl delay 
      if (crawlDelay == _host.getCrawlDelay()) { 
        // see if fetch time is available 
        int lastDocFetchTime = getLastRequestFetchTime();
        
        if (lastDocFetchTime != 0) { 
          // calculate alternate crawl delay based on fetch time ... 
          int alternateCrawlDelay = lastDocFetchTime * 4;
          if (alternateCrawlDelay > crawlDelay) { 
            crawlDelay = alternateCrawlDelay;
            if (Environment.detailLogEnabled())
              LOG.info("### CRAWLDELAY Using Alternate Crawl Delay of:" + alternateCrawlDelay + " for URL:" + getNextPending(false));
          }
        }
      }
      
      /*
      if (_lastRequestDownloadTime != -1) { 
        // next see if host took more than crawl delay millseconds to respond
        if (_lastRequestDownloadTime >= getCrawlDelay()) { 
          // add request time to crawl delay 
          crawlDelay += _lastRequestDownloadTime;
        }
        // add one second for every 2 seconds of request time 
        else { 
          crawlDelay += 1000 * (_lastRequestDownloadTime / 2000);
        }
      }
      */
      // ok ... adjust crawl delay by the number of hops it took to get the result
      if (_fetchStartTime != -1) { 
      	return _fetchStartTime + crawlDelay;
      }
      return System.currentTimeMillis() + crawlDelay;
    }
  }
  
  /** getNextPending */
  private synchronized CrawlTarget getNextPending(boolean removeItem) {

    CrawlTarget targetOut = null;
    
    if (_pending.size() != 0) { 
      targetOut = _pending.getHead();
     if (removeItem && targetOut != null) { 
       _pending.removeElement(targetOut);
      }
    }
    return targetOut;
  }

  /** indicates if robots file need to be retrieved for the specified host */
  public boolean robotsRetrieved() { return _robotsRetrieved; }
 
  /** */
  private final int getCrawlDelay(boolean checkForOverride) { 
    
    if (checkForOverride) { 
      CrawlTarget potentialTarget = getNextPending(false);
      
      if (potentialTarget != null) { 
  
        try {
          URL targetURL = new URL(potentialTarget.getActiveURL());
          // validate against the server for crawl delay
          //LOG.info("Checking Crawl Delay for url:" + targetURL.toString());
          int overridenCrawlDelay = CrawlerServer.getServer().checkForCrawlRateOverride(targetURL);
          if (overridenCrawlDelay != -1) {
            if (Environment.detailLogEnabled())
              LOG.info("### CRAWLDELAY - Overriding Crawl Delay for URL:" + targetURL + " Delay is:" + overridenCrawlDelay );
            return overridenCrawlDelay;
          }
        } catch (MalformedURLException e) {
          
        }
      }
    }
    
    int crawlDelayOut = 0;
    
    if (_ruleSet == null || _ruleSet.getCrawlDelay() == -1) { 
      crawlDelayOut += getHost().getCrawlDelay();
    }
    else { 
      crawlDelayOut += (int)Math.min(_ruleSet.getCrawlDelay(),MAX_CRAWL_DELAY);
      crawlDelayOut = Math.max(MIN_CRAWL_DELAY, crawlDelayOut);
      
    }
    
    if (_lastRequestWasIOException) { 
      crawlDelayOut += IOEXCEPTION_TIMEOUT_BOOST;
    }
    
    return crawlDelayOut;
  }

  /** clear the host's state **/
  public synchronized void clear() { 
    _pending.removeAll();
    _scheduled = null;
    _diskRequestPending = false;
    _offlineTargetCount = 0;
  }

  public synchronized void dumpDetailsToHTML(StringBuffer sb){
//    synchronized (_pending) { 
      sb.append("ListName:" + getListName() + "\n");
      sb.append("RobotsRetrieved:" + _robotsRetrieved + "\n");
      sb.append("Disposition:" + _disposition + "\n");
      sb.append("Scheduled:" + ((_scheduled != null)?_scheduled.getOriginalURL() : "null") + "\n");
      sb.append("PendingCount:" + _pending.size() + "\n");
      sb.append("QueuedCount:" + _queued.size() + "\n");
      sb.append("OfflineCount:" + _offlineTargetCount +"\n");
      sb.append("ActiveConnection:" + _activeConnection +"\n");
      sb.append("LastFetchedRobotsHost:" + _lastFetchedRobotsHostName +"\n");
      
      if (_pending.size() != 0) {
        
        sb.append("next 100 scheduled urls:\n");
        
        int itemCount =0;
        CrawlTarget target = _pending.getHead();
        while (target != null) { 
          
          sb.append("["+(itemCount++)+"]:<a href='" + target.getOriginalURL() +"'>" + target.getOriginalURL()  + "</a>\n");
          
          target = target.getNext();
        }
      }
      
      sb.append("\n\nLastFetchedRobotsData:\n\n");
      if (_lastFetchedRobotsData != null) { 
        sb.append(_lastFetchedRobotsData);
        sb.append("\n");
      }
      
//    }
  }
  
  /**************
   * Disk Operation Support 
   */
    
  
    public static int getPendingDiskOperationCount() { 
      return _diskOperationQueue.size();
    }
  
    public static void stopDiskQueueingThread() { 
      
      if (_diskOperationThread != null) {
        _diskOpThreadShuttingDown = true;
        LOG.info("shutting down Disk Queue Thread - sending null item to queue");
        _diskOperationQueue.add(new DiskQueueEntry(null,false));
        try {
          LOG.info("Waiting for Disk Queue Thread to Die");
          _diskOperationThread.join();
          LOG.info("Done Waiting for Disk Queue Thread");
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        
        _diskOpThreadShuttingDown = false;
        _diskOperationThread = null;
      }
    }
    public static void startDiskQueueingThread(final EventLoop serverEventLoop,final File baseStoragePath) { 
      
      // figure out 
      
      // and finally start the blocking writer thread ...
      _diskOperationThread = new Thread(new Runnable() {
    
        public void run() {
          
          for (;;) { 
            try {
              
              DiskQueueEntry entry = _diskOperationQueue.take();

              // if buffer item is null... this is considered an eof condition ... break out ... 
              if (entry.getListItem() == null) {
                LOG.info("### DiskThread:Received Null Item ... Shutting down CrawlDomain Disk Queue Thread");
                // now matter what ... break out ... 
                break;
              }
              // otherwise .. figure out what to do with the domain ... 
              else { 

                if (_diskOpThreadShuttingDown == false) {
                  
                  final CrawlList domain = entry.getListItem();
                  
                  try {
                    if (Environment.detailLogEnabled())
                      LOG.info("### DiskThread: Got List:" + domain.getListName());
                    // build a hierarchichal path for the given domain id ... 
                    File logFilePath = null;
                    String listName = null;
                    synchronized(domain) { 
                      logFilePath = FileUtils.buildHierarchicalPathForId(baseStoragePath,domain.getUniqueListId());
                      listName = domain.getListName();
                    }
                    // get the immediate parent directory ... 
                    File parentDirectory = logFilePath.getParentFile();
                    // and recursively create the directory chain (if necessary).
                    parentDirectory.mkdirs();
                    
                    IntrusiveList<CrawlTarget> flushList = null;
                    
                    int desiredLoadAmount = 0;

                    boolean truncateFile = false;
                    
                    synchronized(domain) { 
                      if (domain._offlineTargetCount == 0) { 
                        truncateFile = true;
                      }
                    }
                    
                    if (truncateFile && logFilePath.exists()) { 
                      
                      if (Environment.detailLogEnabled())
                        LOG.info("### DiskThread: Truncating Existing Log File for List:" + listName);
                      
                      LogFileHeader header = new LogFileHeader();
                           
                      RandomAccessFile file = new RandomAccessFile(logFilePath,"rw"); 
                      
                      try { 
                          writeLogFileHeader(file,header);
                      }
                      finally { 
                        file.close();
                      }
                    }
                    
                    // now lock access to the domain's pending queue
                    synchronized(domain) { 
                      
                      // if a disk request was pending ...
                      if (domain._diskRequestPending) { 
                      
                        // reset disk request pending flag here to prevent race condition ...
                        domain._diskRequestPending = false;
                        
                        // figure out what action to take with respect to the domain ...
                        
                        // if list count exceeds flush threshold 
                        if (domain._pending.size() >= DISK_FLUSH_THRESHOLD || domain._queued.size() != 0) {
                         
                          if (domain._queued.size() == 0) { 
                            LinkedList<CrawlTarget> candidates = new LinkedList<CrawlTarget>();
                            for (CrawlTarget candidate : domain._pending) { 
                              if ((candidate.getFlags() & CrawlURL.Flags.IsHighPriorityURL) == 0) { 
                                // add candidates in proper order ... 
                                candidates.add(candidate);
                              }
                            }
                            
                            // if there are low priority candidates we can flush ... 
                            if (candidates.size() != 0) {
                              // create a new flush list ... 
                              flushList = new IntrusiveList<CrawlTarget>();
                              
                              // reverse candidate list and start removing items from pending 
                              for (CrawlTarget candidate : Lists.reverse(candidates)) { 
                                domain._pending.removeElement(candidate);
                                flushList.addHead(candidate);
                                // if we are back to ideal target count bail ... 
                                if (domain._pending.size() <= IDEAL_TARGET_COUNT)
                                  break;
                              }
                              
                              if (Environment.detailLogEnabled())
                                LOG.info("### DiskThread: List:" + domain.getListName() + " Created FetchList FROM PENDING of Size:" + flushList.size());
                              
                              // increment offline target count ...
                              domain._offlineTargetCount += flushList.size();
                            }
                          }
                          else { 
                            flushList = domain._queued.detach(domain._queued.getHead());
                            
                            if (Environment.detailLogEnabled())
                              LOG.info("### DiskThread: List:" + domain.getListName() + " Created FetchList FROM QUEUED of Size:" + flushList.size());

                            // increment offline target count ...
                            domain._offlineTargetCount += flushList.size();
                            
                          }
                          

                          /*
                          // walk one past IDEAL target item count... 
                          int i=0;
                          CrawlTarget target = domain._pending.getHead();  
                          while (i<IDEAL_TARGET_COUNT) { 
                            target = target.getNext();
                            ++i;
                          }
                          
                          // and extract a sub-list starting at the target ...  
                          flushList = domain._pending.detach(target);
                          */
                          //and immediately update offline target count in domain ... 
                          //domain._offlineTargetCount += flushList.size();
                        }
                        // otherwise ... 
                        else { 
                          // check queued size ... 
                          if (domain._queued.size() != 0) {
                            // if pending size <= DISK_LOAD_THRESHOLD 
                            if (domain._offlineTargetCount == 0 && domain._pending.size() <= DISK_LOAD_THRESHOLD) {
                              if (Environment.detailLogEnabled())
                                LOG.info("### DiskThread: Moving Items from Queued List to Pending List for CrawlList:" + domain.getListName());
                              // move over items from queued to pending 
                              
                              while (domain._queued.getHead() != null) { 
                                domain._pending.addTail(domain._queued.removeHead());
                                if (domain._pending.size() == (DISK_FLUSH_THRESHOLD - 1))
                                  break;
                              }
                            }
                            
                            //now if domain queue exceeds flush threshold ... 
                            if (domain._queued.size() >= IDEAL_TARGET_COUNT) { 
                              if (Environment.detailLogEnabled())
                                LOG.info("### DiskThread: Queued Size Exceed Flush Threshold. Flushing to Disk for CrawlList:" + domain.getListName());
                              // extract a sub-list starting at head of queued list   
                              flushList = domain._queued.detach(domain._queued.getHead());
                              //and immediately update offline target count in domain ... 
                              domain._offlineTargetCount += flushList.size();
                            }
                          }
                          
                          // check to see if a load is desired ...
                          if (domain._pending.size() <= DISK_LOAD_THRESHOLD) { 
                            // calculate load amount ... 
                            desiredLoadAmount = IDEAL_TARGET_COUNT - domain._pending.size();
                          }
                        }
                      }
                      else { 
                        if (Environment.detailLogEnabled())
                          LOG.info("### DiskThread: Skipping List:" + domain.getListName());
                      }
                    }
                    
                    // now figure out what to do ... 
                    if (flushList != null) {
                      if (Environment.detailLogEnabled())
                        LOG.info("### DiskThread: Flushing"+ flushList.size() + " Items To Disk for Domain:" + domain.getListName());
                      // flush crawl targets to disk ... 
                      appendTargetsToLogFile(logFilePath,flushList);
                      // clear list ... 
                      flushList.removeAll();
                    }
                    // ... if load is desired ... 
                    if (desiredLoadAmount  != 0) { 
                      
                      IntrusiveList<CrawlTarget> loadList = new IntrusiveList<CrawlTarget>();

                      int loadCount = readTargetsFromLogFile(domain,logFilePath,desiredLoadAmount,loadList);
                      
                      // if (Environment.detailLogEnabled())
                        LOG.info("### DiskThread:Disk Queue Loaded: " + loadCount + "Items To Disk for Domain:" + domain.getListName());
                      
                      if (loadCount != 0) { 
                        // time to lock domain again ... 
                        synchronized(domain) { 
                          // and reduce offline count ... 
                          domain._offlineTargetCount -= loadList.size(); 
                          // load new items into domain's list ... 
                          domain._pending.attach(loadList);
                        }
                      }
                    }
                  }
                  catch (IOException e) { 
                    LOG.error("### DiskThread:" + CCStringUtils.stringifyException(e));
                  }
                }
              }
            } catch (InterruptedException e) {
    
            }
            catch (Exception e) { 
              LOG.fatal("### DiskThread: Encountered Unhandled Exception:" + CCStringUtils.stringifyException(e));
            }
          }
         
          LOG.info("### DiskThread: Exiting CrawlDomain Disk Queue Thread");
        } 
      });
      // launch the writer thread ... 
      _diskOperationThread.start();
    }
    
    private static class LogFileHeader {
      
      public static final int LogFileHeaderBytes = 0xCC00CC00;
      public static final int LogFileVersion         = 1;
      
      public LogFileHeader() { 
        _readPos = 0;
        _writePos = 0;
        _itemCount = 0;
      }
      
      public long _readPos;
      public long _writePos;
      public int   _itemCount;
      
      public void writeHeader(DataOutput stream) throws IOException { 
        stream.writeInt(LogFileHeaderBytes);
        stream.writeInt(LogFileVersion);
        stream.writeLong(_diskHeaderActiveVersionTimestamp);
        stream.writeLong(_readPos);
        stream.writeLong(_writePos);
        stream.writeInt(_itemCount);
      }
      
      public void readHeader(DataInput stream) throws IOException { 
        int headerBytes = stream.readInt();
        int version         = stream.readInt();
        long timestamp = stream.readLong();
        
        if (headerBytes != LogFileHeaderBytes && version !=LogFileVersion) { 
          throw new IOException("Invalid CrawlLog File Header Detected!");
        }
        
        _readPos     = stream.readLong();
        _writePos     = stream.readLong();
        _itemCount  = stream.readInt();
        
        // if timestamps don't match ... 
        if (timestamp != _diskHeaderActiveVersionTimestamp) {
          // then reset cursors .. eveything in the file is invalid ... 
          _writePos = 0;
          _readPos = 0;
          _itemCount =0;
        }
      }
    }
    
    
    private static final class CustomByteArrayOutputStream extends ByteArrayOutputStream { 
      public CustomByteArrayOutputStream(int initialSize) { 
        super(initialSize);
      }
      public byte[] getBuffer() { return buf; }
    }
    
    private static void appendTargetsToLogFile(File logFileName,IntrusiveList<CrawlTarget> list)throws IOException { 
      
      LogFileHeader header = new LogFileHeader();
      
      boolean preExistingHeader = logFileName.exists();
            
      RandomAccessFile file = new RandomAccessFile(logFileName,"rw"); 
      
      try { 
        long headerOffset = 0;
        
        if(preExistingHeader) {
          
          headerOffset = readLogFileHeader(file, header);
          
          if (header._writePos == 0) { 
            file.seek(headerOffset);
          }
          else {
            // seelk to appropriate write position 
            file.seek(header._writePos);
          }
        }
        else { 
          headerOffset = writeLogFileHeader(file,header);
        }
        
  
        CustomByteArrayOutputStream bufferOutputStream = new CustomByteArrayOutputStream(1 << 17);
        DataOutputStream dataOutputStream = new DataOutputStream(bufferOutputStream);
        CRC32 crc = new CRC32();
        
        for (CrawlTarget target : list) { 
          
          PersistentCrawlTarget persistentTarget = target.createPersistentTarget();
          
          bufferOutputStream.reset();
          // write to intermediate stream ... 
          persistentTarget.write(dataOutputStream);
          // and crc the data ... 
          crc.reset();
          crc.update(bufferOutputStream.getBuffer(),0,bufferOutputStream.size());
          // write out length first 
          file.writeInt(bufferOutputStream.size());
          //crc next
          long computedValue = crc.getValue();
          //TODO: waste of space - write 32 bit values as long because having problems with java sign promotion rules during read...
          file.writeLong(computedValue);
          // and then the data 
          file.write(bufferOutputStream.getBuffer(),0,bufferOutputStream.size());
        }
        
        // now update header ... 
        header._itemCount += list.size();
        header._writePos   = file.getFilePointer();
        
        // now write out header anew ... 
        writeLogFileHeader(file,header);
        
      }
      finally { 
        if (file != null) { 
          file.close();
        }
      }
    }
    
    private static int readTargetsFromLogFile(CrawlList domain,File logFileName,int desiredReadAmount,IntrusiveList<CrawlTarget> targetsOut)throws IOException { 
     
      int itemsRead = 0;
      
      if (logFileName.exists()) { 
       
        RandomAccessFile file = new RandomAccessFile(logFileName,"rw");
        
        LogFileHeader header = new LogFileHeader();     
        
        try { 
          
          long headerOffset = readLogFileHeader(file, header);
            
          // seelk to appropriate write position 
          if (header._readPos != 0)
            file.seek(header._readPos);
          
          int itemsToRead  = Math.min(desiredReadAmount, header._itemCount);

          PersistentCrawlTarget persistentTarget = new PersistentCrawlTarget();
          CRC32 crc = new CRC32();
          CustomByteArrayOutputStream buffer = new CustomByteArrayOutputStream(1 << 16);
          
          for (int i=0;i<itemsToRead;++i) { 
            // read length ... 
            int urlDataLen     = file.readInt();
            long urlDataCRC = file.readLong();

            buffer.reset();
            
            if (urlDataLen > buffer.getBuffer().length) { 
              buffer = new  CustomByteArrayOutputStream( ((urlDataLen / 65536) + 1) * 65536 );
            }
            file.read(buffer.getBuffer(), 0, urlDataLen);
            crc.reset();
            crc.update(buffer.getBuffer(), 0, urlDataLen);
            
            long computedValue = crc.getValue();
            
            // validate crc values ... 
            if (computedValue != urlDataCRC) { 
              throw new IOException("Crawl Target Log File Corrupt");
            }
            else { 
              //populate a persistentTarget from the (in memory) data stream
              DataInputStream bufferReader = new DataInputStream(new ByteArrayInputStream(buffer.getBuffer(),0,urlDataLen));
              
              persistentTarget.clear();
              persistentTarget.readFields(bufferReader);

              //populate a new crawl target structure ... 
              CrawlTarget newTarget = new CrawlTarget(domain,persistentTarget);
              
              targetsOut.addTail(newTarget);
            }
          }
          
          itemsRead = itemsToRead;
          
          // now update header ... 
          header._itemCount -= itemsRead;
          // now if item count is non zero ... 
          if (header._itemCount != 0) { 
            // set read cursor to next record location 
            header._readPos     = file.getFilePointer();
          }
          // otherwise ... 
          else { 
            // reset both cursors ... 
            header._readPos = 0;
            header._writePos = 0;
          }
          
          // now write out header anew ... 
          writeLogFileHeader(file,header);
        }
        catch (IOException e) { 
          LOG.fatal("Encountered Exception Reading From Offline Queue for LogFile:" + logFileName + ". Truncating Queue! \n" + CCStringUtils.stringifyException(e));
          header._itemCount = 0;
          header._readPos = 0;
          header._writePos = 0;
          
          writeLogFileHeader(file,header);
        }
        
        finally { 
          if (file != null) { 
            file.close();
          }
        }
      }
      return itemsRead;
    }

    
    private static long writeLogFileHeader(RandomAccessFile file, LogFileHeader header )throws IOException {
      
      // set the position at zero .. 
      file.seek(0);
      // and write header to disk ... 
      header.writeHeader(file);
      
      //took sync out because it was becoming a sever bottleneck
      // file.getFD().sync();
      
      return file.getFilePointer();
    }
    
    private static long readLogFileHeader(RandomAccessFile file,LogFileHeader header) throws IOException {
      
      file.seek(0);
      
      header.readHeader(file);
      
      return file.getFilePointer();
    }
    
  
    
    public static class CrawlDomainTester { 

      
      public static void main(String[] args) {
        try {
          testGetNextItemCode();
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
      @Test
      public static void testGetNextItemCode() throws Exception { 
        
      	/*
        CrawlListHost host = new CrawlListHost(null,1);
        CrawlList list = host.getCrawlList(1);
        
        
        list.addCrawlTarget(CrawlTarget.createTestCrawlTarget(list,"http://www.redirecttest.com"),false);
        list.addCrawlTarget(CrawlTarget.createTestCrawlTarget(list,"http://www.redirecttest.com/foobar"),false);
        list.addCrawlTarget(CrawlTarget.createTestCrawlTarget(list,"http://www.blogger.com"),false);
        list.addCrawlTarget(CrawlTarget.createTestCrawlTarget(list,"http://blogger.com"),false);
        list.addCrawlTarget(CrawlTarget.createTestCrawlTarget(list,"http://www.blogger.com"),false);
        list.addCrawlTarget(CrawlTarget.createTestCrawlTarget(list,"http://foo.blogger.com"),false);
        list.addCrawlTarget(CrawlTarget.createTestCrawlTarget(list,"http://failed.domain/bar"),false);
        list.addCrawlTarget(CrawlTarget.createTestCrawlTarget(list,"http://failed.domain/zzz"),false);
        list.addCrawlTarget(CrawlTarget.createTestCrawlTarget(list,"http://####/foo/zzz"),false);
        list.addCrawlTarget(CrawlTarget.createTestCrawlTarget(list,"garbage"),false);
        list.addCrawlTarget(CrawlTarget.createTestCrawlTarget(list,""),false);
        list._offlineTargetCount = 100;
        
        CrawlTarget target = null;
        NIOHttpHeaders headers = new NIOHttpHeaders();
        headers.add(null, "HTTP1.1 200 OK");

        while (list.getDisposition() != CrawlList.Disposition.QueueEmpty) {
          if (list.getDisposition() == CrawlList.Disposition.ItemAvailable) { 
            target = list.getNextTarget();
            if (target == null)
              System.out.println("Target:NULL");
            else 
              System.out.println("Target:" + target.getOriginalURL());
          }
          else if (list.getDisposition() == CrawlList.Disposition.WaitingOnCompletion) { 
            if (target != null) {
              if (target.getActiveURL().startsWith("http://www.redirecttest.com")) { 
                target.setRedirectURL(target.getActiveURL().replaceFirst("http://www.redirecttest.com", "http://redirecttest.com"));
                target.setFlags(target.getFlags() | CrawlURL.Flags.IsRedirected);

              }
              list.fetchStarted(target);
              if (target.getActiveURL().startsWith("http://failed.domain")) { 
                list.getActiveDomain()._domainFailed = true;
              }
              list.fetchSucceeded(target,0, headers, null);
              

              target = null;
            }
            else { 
              list._disposition = Disposition.WaitingOnTime;
            }
          }
          else if (list.getDisposition() == CrawlList.Disposition.WaitingOnTime) {
            System.out.println("clearing WaitState");
            list.clearWaitState();
          }
        }
        */
      }
      
      
      //@Test
      public void testDiskQueue() throws Exception { 
        
        String hostName = "poodleskirtcentral.com";
        long domainFP = URLFingerprint.generate64BitURLFPrint(hostName);
        File logFilePath  =FileUtils.buildHierarchicalPathForId(new File("/foo"),domainFP); 
        
        System.out.println(logFilePath.getAbsolutePath());
      }
      
      
      /*
      //@Test
      public void testDiskWriter() throws Exception {
        // initialize ...
        Configuration conf = new Configuration();
        
        conf.addResource("nutch-default.xml");
        conf.addResource("nutch-site.xml");
        conf.addResource("hadoop-default.xml");
        conf.addResource("hadoop-site.xml");
        conf.addResource("commoncrawl-default.xml");
        conf.addResource("commoncrawl-site.xml");
        
        CrawlEnvironment.setHadoopConfig(conf);
        CrawlEnvironment.setDefaultHadoopFSURI("file:///");
        CrawlEnvironment.setCrawlSegmentDataDirectory("./tests/crawlSegmentSamples/");
  
        EventLoop eventLoop = new EventLoop();
        eventLoop.start();
        
        DNSCache cache = new DNSCache() { 
          public DNSResult resolveName(CrawlSegmentHost host) {
            return null;
          } 
        };

        CrawlList.DISK_FLUSH_THRESHOLD = 5;
        CrawlList.DISK_LOAD_THRESHOLD = 2;
        CrawlList.IDEAL_TARGET_COUNT = 3;
        
        File basePath = new File("./data/diskQueueTest");
        basePath.mkdir();
        CrawlList.startDiskQueueingThread(eventLoop,basePath);

        CrawlSegmentDetail detailCC06 = SegmentLoader.loadCrawlSegment(1,1, "cc06",null, cache,null,null);
        
        CrawlListHost host = new CrawlListHost(null,0);
        
        int domainCount = 0;
        CrawlList firstDomain = null;
        
        for (CrawlSegmentHost segmentHost: detailCC06.getHosts()) { 
          CrawlList domain = new CrawlList(host,segmentHost.getListId());
          if (domainCount==0)
            firstDomain = domain;
          if (domainCount ==0)
            System.out.println("Domain:" + domain.getListName() + " FP:" + domain.getListId());
          for (CrawlSegmentURL segmentURL : segmentHost.getUrlTargets()) { 
            if (domainCount ==0)
              System.out.println("\tAdding Target::" + segmentURL.getUrl());
            CrawlTarget target = new CrawlTarget(1,domain,segmentHost,segmentURL);
            domain.addCrawlTarget(target, false);
          }
          
          if (++domainCount == 10) 
            break;
        }
        while (true) { 
          synchronized (firstDomain) { 
            if (firstDomain._pending.size() < CrawlList.DISK_FLUSH_THRESHOLD)
              break;
            Thread.sleep(5000);
          }
        }
        
        while (true) { 
          if (firstDomain.getDisposition() == CrawlList.Disposition.ItemAvailable) { 
            CrawlTarget nextTarget = null;
            while ((nextTarget = firstDomain.getNextTarget()) != null) { 
              System.out.println("Domain: "+ firstDomain.getListName() + " Got Target:" + nextTarget.getOriginalURL());
              firstDomain.fetchStarted(nextTarget);
              firstDomain.fetchSucceeded(nextTarget, 0,null, null);
              if (firstDomain.getDisposition() == CrawlList.Disposition.WaitingOnTime) { 
                firstDomain.clearWaitState();
              }
            }
          }
          else { 
            System.out.println("Domain Queue Empty ... Waiting");
            Thread.sleep(5000);
          }
        }
        //CrawlDomain._diskOperationThread.join();
      }
      */
    }
    
    @Override
    public String toString() {
      return "List Id:" + _baseListId + " Name:" + _listName;      
    }
}

