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


package org.commoncrawl.crawl.crawler;


import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.common.Environment;
import org.commoncrawl.crawl.crawler.RobotRulesParser.RobotRuleSet;
import org.commoncrawl.crawl.filters.IPAddressBlockFilter;
import org.commoncrawl.crawl.filters.Filter.FilterResult;
import org.commoncrawl.protocol.CrawlURLMetadata;
import org.commoncrawl.util.internal.HttpCookieUtils.CookieStore;
import org.commoncrawl.util.shared.CCStringUtils;
import org.commoncrawl.util.shared.IPAddressUtils;
import org.commoncrawl.util.shared.IntrusiveList;
import org.commoncrawl.util.shared.IntrusiveList.IntrusiveListElement;

/**
 * 
 * @author rana
 *
 * The common CrawlHost implementation shared by CrawlList and CrawlQueue
 */
public final class CrawlHostImpl implements CrawlListHost, CrawlQueueHost {

  private static final int DEFAULT_CRAWL_DELAY = 2000;
  
  private static final int SUPER_HOST_THRESHOLD_1 = 25;
  private static final int SUPER_HOST_THRESHOLD_2 = 100;
  private static final int SUPERHOST_CRAWL_DELAY_1 = 3000;
  private static final int SUPERHOST_CRAWL_DELAY_2 = 2000;
  
  /** fail the host after 5 consecutive io errors **/
  private static final int HOST_FAIL_ON_CONSECUTIVE_IO_ERRORS_THRESHOLD = 100;
  /** time to keep a host in failed state after an io error condition is detected **/
  private static final int HOST_FAIL_RESET_TIMER = 10 * 60 * 1000; // 10 minutes .. 
  

  /** logging **/
  private static final Log LOG = LogFactory.getLog(CrawlListHost.class);
  
  
  private int       _ipAddress; // host's ip address ...
  
  private IntrusiveList<CrawlList> _crawlLists = new IntrusiveList<CrawlList>(); 
  
  private CrawlQueue  _queue;
  private boolean     _idle = false;
  private boolean     _zombie= false;
  private boolean     _isBlackListedHost = false;
  /** paused by a master crawl controller **/
  private boolean 		_isPaused = false;
  /** pause state timestamp **/
  private int 		  _pauseStateTimestamp = -1;
  
  private long        _blackListStatusUpdateTime = -1;
  private long        _lastFetchStartTime = -1;
  private long        _lastDispositionChangeTime = -1;
  private long        _waitTime = -1;
  private int         _failedDomainCount = 0;
  private int         _uniqueDomainCount = 0;
  private int         _successfulGETCount = 0;
  private int         _http200Count = 0;
  private int         _robotsExcludedCount =0;
  private int         _403Count = 0;
  private int         _failedGETCount = 0;
  boolean             _inFeedQueue  = false;
  private short       _consecutiveIOErrors = 0;
  private long        _lastIOErrorTime = -1;
  private CookieStore _cookieStore = new CookieStore();
  
  // private boolean       _skipRobots = false;
  // private String            _resolvedHostName = null;
  
  private static int MAX_ROBOTS_CACHE_ENTIRES = 20;
  
  private static class RobotRuleSetCacheItem extends IntrusiveListElement<RobotRuleSetCacheItem> { 
    public long         _crc;
    public RobotRuleSet _ruleSetObject;
    public long         _lastTouched;
  }

  /** robots rule set cache **/
  private IntrusiveList<RobotRuleSetCacheItem> _robotsRuleSetCache = new IntrusiveList<RobotRuleSetCacheItem>();

  public CrawlHostImpl(CrawlQueue queue,int ipAddress) { 
    _ipAddress = ipAddress;
    _queue = queue;
  }
  
  public CrawlerServer getServer() { 
    return getQueue().getEngine().getServer();
  }
  
 
  public CrawlList getActiveList() { 
    return getHead();
  }
  
  /** get access to the cookie store associated with this host 
   *  may be null 
   * **/
  public CookieStore getCookieStore() { 
    return _cookieStore;
  }
  
  public boolean noActiveLists() { 
    // if there are no lists present then, yes there are no active lists ... 
    if (getHead() == null) { 
      return true;
    }
    else { 
      // otherwise ... special case, only one list present and it is the high priority list ... 
      if (_crawlLists.size() == 1 && getHead().getListId() == CrawlerServer.getServer().getHighPriorityListId()) { 
        // check to see if it's disposition is QueueEmpty 
        return getHead().getDisposition() == CrawlList.Disposition.QueueEmpty;
      }
      // otherwise ... no, there are active lists present 
      return false;
    }
  }  
  
  String getActiveListName() { 
    if (getHead() != null) 
      return getHead().getListName();
    return "null";
  }
  
  String getActiveListDisposition() { 
    if (getHead() != null) 
      return getHead().getDisposition().toString();
    return "null";
  }

  @Override
  public String getIPAddressAsString() { 
    String ipAddress = "UNKNOWN";
    try {
      ipAddress = IPAddressUtils.IntegerToInetAddress(getIPAddress()).toString();
    } catch (UnknownHostException e) {
    }
    return  ipAddress;
  }
  
  public int      getIPAddress() { return _ipAddress; } 
  
  public CrawlQueue getQueue() { return _queue; }

  public int getQueuedURLCount() { 
    int countOut = 0;
    for (CrawlList list : _crawlLists) { 
      synchronized (list) { 
        countOut += list.getPendingURLCount() + list.getOfflineURLCount();
      }
    }
    return countOut;
  }
  
  public CrawlList getCrawlList(int listId) { 
    CrawlList domainOut = null;
    
    for (CrawlList domain : _crawlLists) { 
      if (domain.getListId() == listId) { 
        domainOut = domain;
        break;
      }
    }
    
    if (domainOut == null) { 
      domainOut = new CrawlList(this,listId);
      _crawlLists.addTail(domainOut);
      // increment unique domain count ... 
      _uniqueDomainCount++;
    }
    
    return domainOut;
  }
  
  

  
  public boolean isTimerActive() { 
    return _waitTime != -1;
  }
  
  public void setTimer(long expireTime) { 
    _idle = false;
    _waitTime = expireTime;
    // check for null for unit test scenario
    if (getQueue() != null) { 
      getQueue().setTimer(this,expireTime);
    }
  }
  
  public void killTimer() {
    if (_waitTime != -1) { 
      _waitTime = -1;
      // check for null for unit test scenario
      if (getQueue() != null) { 
        getQueue().killTimer(this);
      }
    }
  }
  
  @Override
  public void updateLastFetchStartTime(long newFetchStartTime) { 
    _lastFetchStartTime = newFetchStartTime;
  }
  
  void incFailedDomainCount() { 
    _failedDomainCount++;
  }
  
  /** increments the consecutive io errors counter for this host 
   *  used to track failed servers 
   * **/ 
  public void incConsecutiveIOErrorCount() { 
    _consecutiveIOErrors++;
    _lastIOErrorTime = System.currentTimeMillis();
    if (_consecutiveIOErrors >= HOST_FAIL_ON_CONSECUTIVE_IO_ERRORS_THRESHOLD) { 
      LOG.error("### HOST: Failed Host:" + getIPAddressAsString() + " due to too many consecutive IO Errors!");
    }
  }
  
  /** reset IOError Counter ... whenever we succesfully retrieve a document from this host **/
  public void resetIOErrorCount() { 
    _consecutiveIOErrors = 0;
  }
  
  /** is this a blacklisted host **/
  public boolean isBlackListedHost() { 
    // check blacklisted host status ... 
    if (_blackListStatusUpdateTime < getServer().getFilterUpdateTime()) {
      
      // if black list status out of date ... validate blacklist status 
      IPAddressBlockFilter filter = getServer().getIPAddressFilter();
      
      if (filter != null) { 
        CrawlURLMetadata metadata = new CrawlURLMetadata();
        metadata.setServerIP(getIPAddress());
        _isBlackListedHost =  filter.filterItem(null,null, null, metadata, null) == FilterResult.Filter_Reject;
        
        if (_isBlackListedHost) { 
          if (Environment.detailLogEnabled())
            LOG.info("### FILTER Host:" + getIPAddressAsString() + " has been blacklisted.");
        }
      }
      
      _blackListStatusUpdateTime = getServer().getFilterUpdateTime();
      
    }
    
    return _isBlackListedHost; 
  }
  
  /** check to see if we have marked this server as a failed host (too many consecutive io errors) **/
  public boolean isFailedServer() {
    
    if (isBlackListedHost()) { 
      return true;
    }
    else { 
      if (_consecutiveIOErrors >= HOST_FAIL_ON_CONSECUTIVE_IO_ERRORS_THRESHOLD) { 
        if (System.currentTimeMillis() - _lastIOErrorTime < HOST_FAIL_RESET_TIMER) {
          return true;
        }
      }
      return false;
    }
  }
  
  @Override
  public int getCrawlDelay() { 
/*
    if (_uniqueDomainCount >= SUPER_HOST_THRESHOLD_1 && _uniqueDomainCount < SUPER_HOST_THRESHOLD_2)
      return SUPERHOST_CRAWL_DELAY_1;
    else if (_uniqueDomainCount >= SUPER_HOST_THRESHOLD_2)
      return SUPERHOST_CRAWL_DELAY_2;
    else
*/    
      return DEFAULT_CRAWL_DELAY;
  }
  
  /*
  boolean skipRobots() { 
    return _skipRobots ;
  }
  */
  
  long getDomainTransitionWaitTime() { 
    if (_lastFetchStartTime != -1) { 
      return _lastFetchStartTime + getCrawlDelay();
    }
    return -1;
  }
  
  public long getLastFetchStartTime() { 
    return _lastFetchStartTime;
  }
  
  
  private final CrawlList getHead() { 
    return _crawlLists.getHead();
  }
  
  public void feedQueue() { 

    if (getQueue() != null) { 
      
      if (!_inFeedQueue) {
        
        if (getHead() != null && (getHead().getDisposition() == CrawlList.Disposition.WaitingOnCompletion || getHead().getDisposition() == CrawlList.Disposition.WaitingOnTime)) { 
          LOG.warn("FeedQueue called on Host with Active List:" + getHead().toString());
          return;
        }
        
        _inFeedQueue = true;
        
        boolean exitLoop = false;
        
        while (!exitLoop && getHead() != null) { 
  
          CrawlList currentList = getHead();
          
          setIdle(false);
           
          CrawlTarget target = null;

          if (currentList.getDisposition() != CrawlList.Disposition.QueueEmpty) { 
            // get the next target for the current domain ...
            target = currentList.getNextTarget();
          }
            
          if (target != null) {
            // log it for now ...
            if (Environment.detailLogEnabled())
              LOG.info("Host: " + getIPAddressAsString() + " FeedQueue returned Target:" + target.getOriginalURL());
            // and queue it up with the fetcher 
            getQueue().fetchItem(target);
            
            // break out here ... 
            exitLoop = true;
          }
          else {
            
            // figure out what to do next ... 
            if (currentList.getDisposition() == CrawlList.Disposition.WaitingOnTime) {
              if (Environment.detailLogEnabled())
                LOG.info("Feed Queue for List:"+currentList.getListName() + " Returned WaitingOnTime");
              setTimer(currentList.calculateNextWaitTime());
              exitLoop = true;
            }
            else if (currentList.getDisposition() == CrawlList.Disposition.QueueEmpty) {
              
              if (currentList.getListId() != CrawlerServer.getServer().getHighPriorityListId()) { 
                if (Environment.detailLogEnabled())
                  LOG.info("Feed Detected List:"+currentList.getListName() + " is in QueueEmpty state - removing...");
                // remove the domain from the list ... 
                _crawlLists.removeElement(currentList);
              }
              else { 
                // remove the list to the tail end of the queue ... 
                _crawlLists.removeElement(currentList);
                _crawlLists.addTail(currentList);
                if (_crawlLists.getHead() == currentList) {
                  if (Environment.detailLogEnabled())
                    LOG.info("FeedQueue Detected HighPriority List:" + currentList.getListName() + " is IDLE. Exiting.");
                  // and exit loop 
                  exitLoop = true;
                }
              }
              
              // if there is a next domain ... 
              if (!exitLoop && getHead() != null) { 
                // setup a transition timer ... 
                setTimer(getDomainTransitionWaitTime());
                // and exit loop 
                exitLoop = true;
              }
              
            }
            else {
              
              
              StringWriter writer = new StringWriter();
              
              writer.write("Invalid Domain State Encountered for List:" + currentList.getListName() + " Disposition:" + currentList.getDisposition()+"\n");
              try {
                dumpDetails(writer);
              } catch (IOException e) {
                LOG.error(CCStringUtils.stringifyException(e));
              }
              LOG.fatal(writer.toString());
              throw new RuntimeException(writer.toString());
            }
          }
        }
        
        if (getHead() == null) { 
          setIdle(true);
        }
        _inFeedQueue = false;
      }
    }
  }
  
  @Override
  public void listDispositionChanged(CrawlList list,CrawlList.Disposition oldDisposition,CrawlList.Disposition newDisposition) {
    
    boolean originallyIdle = isIdled();
    
    updateLastModifiedTime(System.currentTimeMillis());
    list.updateLastModifiedTime(System.currentTimeMillis());
    
    // if this is the active domain ... 
    if (getHead() == list) {
      
      // if we were previously waiting on time ... 
      if (oldDisposition == CrawlList.Disposition.WaitingOnTime) {
        if (Environment.detailLogEnabled())
          LOG.info("Host:" + getIPAddressAsString() +" List:" + list.getListName() + " timer FIRED");
        if (isTimerActive()) { 
          killTimer();
        }
      }
    
      if (newDisposition == CrawlList.Disposition.WaitingOnTime) { 
        if (Environment.detailLogEnabled())
          LOG.info("Host:" + getIPAddressAsString() +" List:" + list.getListName() + " timer SET");
        setTimer(list.calculateNextWaitTime());
      }
      else if (newDisposition == CrawlList.Disposition.ItemAvailable  || newDisposition == CrawlList.Disposition.QueueEmpty) {
        if (Environment.detailLogEnabled())
          LOG.info("Host:" + getIPAddressAsString() +" List:" + list.getListName() + " triggered feedQueue");
        feedQueue(); 
      }
    }
    // otherwise if state change occured on a non-active domain ... 
    else {
      throw new RuntimeException("List Disposition Change Happened in non Active List:" + list.getListName() + " CurrentList:" + getHead());
    }
    
    // if we became idled as a result of this state transition ... 
    if (isIdled() && !originallyIdle) { 
      if (_queue != null) { 
        _queue.idleHost(this);
      }
    }
  }

  public void clearWaitState() { 

    if (_waitTime == -1) { 
      LOG.error("Host: " + getIPAddressAsString() + " clearWaitState called while _waitTime == -1");
      return;
    }
    
    _waitTime = -1;
    
    // check to see if the active domain is in a time wait state  
    if (getHead() != null) {
      // clear the item's wait state ... 
      getHead().clearWaitState();
    }
    else {
      throw new RuntimeException("clearWaitState called while head was null!");
    }
  }
  
  @Override
  public void purgeReferences() {
    if (_waitTime != -1) { 
      killTimer();
    } 
    
    while (_crawlLists.getHead() != null) { 
      CrawlList headElement = _crawlLists.removeHead();
      headElement.clear();
    }
  }

  @Override
  public void setIdle(boolean isIdle) { 
    _idle = isIdle;
  }
  
  public boolean isIdled() { 
    return _idle;
  }

  @Override
  public void updateLastModifiedTime(long time) { 
    _lastDispositionChangeTime = time;
    _zombie = false;
  }
  
  @Override
  public long getLastModifiedTime() { 
    return _lastDispositionChangeTime; 
  }
  
  public long getWaitTime() { 
    return _waitTime;
  }
  
  void incSuccessfulGETCount() { ++_successfulGETCount; }
  void incHttp200Count() { ++_http200Count; }
  void incFailedGETCount()       { ++_failedGETCount; }
  void incRobotsExcludedCount() { ++_robotsExcludedCount; }
  void inc403Count()            { ++_403Count; }
  
  
  
 
  private static SimpleDateFormat _formatter = new SimpleDateFormat("yyyy.MM.dd 'at' hh:mm:ss z");
  
  private static final String dateStringFromTimeValue(long timeValue) { 
    
    if (timeValue != -1) { 
      Date theDate = new Date(timeValue);
      return _formatter.format(theDate);
    }
    return "";
  }
  
  /** cache a robots file by crc **/ 
  public void cacheRobotsFile(RobotRuleSet ruleSet,long robotsCRC) { 
    RobotRuleSetCacheItem oldestItem = null;
    boolean found = false;
    for (RobotRuleSetCacheItem item : _robotsRuleSetCache) { 
      if (item._crc == robotsCRC) { 
        item._lastTouched = System.currentTimeMillis();
        found = true;
      }
      oldestItem = (oldestItem == null) ? item : (oldestItem._lastTouched > item._lastTouched) ? item : oldestItem;
    }
    
    if (!found) { 
      if (_robotsRuleSetCache.size() == MAX_ROBOTS_CACHE_ENTIRES) { 
        _robotsRuleSetCache.removeElement(oldestItem);
      }
      RobotRuleSetCacheItem cacheItem = new RobotRuleSetCacheItem();
      
      cacheItem._crc = robotsCRC;
      cacheItem._lastTouched = System.currentTimeMillis();
      cacheItem._ruleSetObject = ruleSet;
      _robotsRuleSetCache.addHead(cacheItem);
    }    
  }
  
  /** check for a cached robots entry via the given crc value **/  
  public RobotRuleSet getCachedRobotsEntry(long crcValue) { 
    for (RobotRuleSetCacheItem cacheItem : _robotsRuleSetCache) { 
      if (cacheItem._crc == crcValue) 
        return cacheItem._ruleSetObject;
    }
    return null;
  }
  
  @Override
  public void dumpDetails(Writer out)throws IOException { 
  
    StringBuffer sb = new StringBuffer();
    
    sb.append("Failed:" + isFailedServer() + "\n");
    sb.append("Idle:" + _idle + "\n");
    sb.append("Paused:" + _isPaused +"\n");
    sb.append("Zombie:" + _zombie+ "\n");
    sb.append("LastFetchTime:" +dateStringFromTimeValue(_lastFetchStartTime) + "\n");
    sb.append("LastDispChangeTime:" +dateStringFromTimeValue(_lastDispositionChangeTime) + "\n");
    sb.append("WaitTime:" +dateStringFromTimeValue(_waitTime) + "\n");
    sb.append("CrawlDelay:" + ((_lastFetchStartTime != -1) ? (Math.max(0,_waitTime - _lastFetchStartTime)) : 0) + "\n");
    sb.append("UniqueDomainCount:" +_uniqueDomainCount + "\n");
    sb.append("SuccessfulGETs:" +_successfulGETCount + "\n");
    sb.append("HTTP-200-Count:" +_http200Count + "\n");
    sb.append("HTTP-403-Count:" +_403Count + "\n");
    sb.append("RobotsExcludedCount:" +_robotsExcludedCount + "\n");
    sb.append("FailedGETs:" +_failedGETCount + "\n");
    sb.append("ActiveList:" + ((getHead() != null) ? getHead().getListName() : "NULL" + "\n"));
    sb.append("ActiveList-LastFetchTime:" + ((getHead() != null) ? getHead().getLastRequestFetchTime() : 0) +"\n");
    sb.append("ActiveList-NextCrawlInterface:" + ((getHead() != null) ? getHead().getNextCrawlInterface() : 0) +"\n");
    sb.append("\n\n<b>ActiveDomain Details:</b>\n");
    if (getHead() != null) {
      getHead().dumpDetailsToHTML(sb);
    }
    if (_crawlLists.size() > 1) { 
      sb.append("\n\n<B>Other Domains:</B>\n");
      for (CrawlList domain : _crawlLists) { 
        if (domain != getHead()) { 
          domain.dumpDetailsToHTML(sb);
          sb.append("\n");
        }
      }
    }
    out.write(sb.toString());
  }

  @Override
  public String getScheme() {
    return CrawlQueue.protocolToScheme(_queue.getProtocol());
  }

	@Override
  public void incrementCounter(CounterId counter, int amount) {
		switch (counter) { 
			case RobotsExcludedCount: _robotsExcludedCount += amount;break;
			case SuccessfullGetCount: _successfulGETCount += amount;break;
			case Http200Count:			 	_http200Count += amount;break;
			case Http403Count:				_403Count += amount;break; 
			case FailedDomainCount:		_failedDomainCount += amount;break;
			case FailedGetCount: 			_failedGETCount += amount;break;
			case ConsecutiveIOErrorCount: _consecutiveIOErrors += amount;break;
		}
  }

	@Override
  public void resetCounter(CounterId counter) {
		switch (counter) { 
		case RobotsExcludedCount: _robotsExcludedCount =0;break;
		case SuccessfullGetCount: _successfulGETCount =0;break;
		case Http200Count:			 	_http200Count =0;break;
		case Http403Count:				_403Count =0;break; 
		case FailedDomainCount:		_failedDomainCount =0;break;
		case FailedGetCount: 			_failedGETCount =0;break;
		case ConsecutiveIOErrorCount: _consecutiveIOErrors =0;break;
		}
  }

	@Override
  public boolean isPaused() {
	  // check to see if our pause state needs to be updated ...
		int currentServerPauseStateTimestamp = CrawlerServer.getServer().getPauseStateSerialTimestamp(); 
		if (currentServerPauseStateTimestamp != _pauseStateTimestamp) { 
			// yes it does ... 
			// ask server for current state
			_isPaused = CrawlerServer.getServer().isHostPaused(this);
			// and update our timestamp ... 
			_pauseStateTimestamp = currentServerPauseStateTimestamp;
		}
		return _isPaused;
  }
}
