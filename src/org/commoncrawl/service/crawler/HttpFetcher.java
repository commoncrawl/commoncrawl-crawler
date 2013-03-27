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
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.Locale;
import java.util.SimpleTimeZone;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.async.Timer;
import org.commoncrawl.common.Environment;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.io.NIOBufferList;
import org.commoncrawl.io.NIODNSResolver;
import org.commoncrawl.io.NIOHttpConnection;
import org.commoncrawl.io.NIOSocketSelector;
import org.commoncrawl.io.NIOHttpConnection.State;
import org.commoncrawl.protocol.CrawlURL;
import org.commoncrawl.protocol.URLFP;
import org.commoncrawl.service.statscollector.CrawlerStats;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.IPAddressUtils;
import org.commoncrawl.util.MovingAverage;
import org.commoncrawl.util.RuntimeStatsCollector;
import org.commoncrawl.util.SmoothedAverage;
import org.commoncrawl.util.URLUtils;

/**
 * 
 * @author rana
 *
 */
public final class HttpFetcher implements Fetcher , NIOHttpConnection.Listener {

  /** constants **/
  private static long TIMEOUT_TIMER_INTERVAL = 1000; // every 1 second ... 

  private static int   DOWNLOAD_LIMIT = CrawlEnvironment.CONTENT_SIZE_LIMIT;

  private static final int MAX_REDIRECTS = 6;


  /** logging **/
  private static final Log LOG = LogFactory.getLog(HttpFetcher.class);
  /** running state variable **/
  private boolean 		   				_running = false;
  /** paused state **/
  private boolean              _paused = false;
  /** max open sockets variable **/
  private int								   _maxSockets;
  /** selector reference **/
  NIOSocketSelector 	_selector;
  /** resolver reference **/
  NIODNSResolver 		_resolver;
  /** timeout timer object **/
  Timer 							_timeoutTimer;
  /** rotating snapshot timer **/
  int  _snapshotNumber;
  /** fail connections mode **/
  boolean _failConnections = false;
  /** crawler name **/
  String _crawlerName;

  /** stats **/
  private int       connectionCount = 0;
  private int       finishCount = 0;
  private int       successCount = 0;
  private int       failureCount  = 0;
  private int       resolvingCount = 0;	
  private int 		   connectingCount = 0;
  private int 		   sendingCount = 0;
  private int		   receivingCount = 0;

  private long cumulativeURLCount = 0;
  private long    firstSnapShotTime = -1;
  private int       snapShotURLCount = 0;
  private int       snapShotConnectionCount = 0;
  private long    snapShotTime = -1;
  private long cumulativeURLSSEC = 0;

  private int       snapShotCount = 0;
  private long    snapShotDownloadAmt = 0;
  private long cumulativeDownloadPerSec = 0;

  private MovingAverage _urlsPerSecMovingAverage;
  private MovingAverage _kbPerSecMovingAverage;
  private SmoothedAverage _urlsPerSecSmoothed;
  private SmoothedAverage _kbPerSecSmoothed;
  private MovingAverage  _avgDownloadSize;

  private InetSocketAddress _crawlInterfaces[];


  /** CrawlContext **/
  private static class CrawlContext { 

    CrawlTarget _url;
    int				 _index;

    public CrawlContext(CrawlTarget url, int index) { 
      _url = url;
      _index = index;
    }

    public CrawlTarget getURL() {
      return _url;
    }

    public int getIndex() {
      return _index;
    }
  }

  /** active connections **/
  private NIOHttpConnection _active[] 				= null;
  /** active connection versions **/
  private short _activeVersions[] = null;
  /** trailing connection versions **/
  private short _trailingVersions[] = null;
  /** pending URLs **/
  private LinkedList<CrawlTarget> _pending 	= new LinkedList<CrawlTarget>();

  /** pause support **/
  public boolean isPaused() { return _paused; }
  public void pause() { _paused = true; }
  public void resume() { 
    if (_paused) { 
      _paused = false;
      if (_running)
        fillQueue(true);
    }
  }

  /** if-modified-since support **/
  private SimpleDateFormat http_date_format;


  public HttpFetcher(int maxOpenSockets,InetSocketAddress[] crawlInterfaceList,String crawlerName) { 
    _maxSockets = maxOpenSockets;
    _active 			= new NIOHttpConnection[_maxSockets];
    _activeVersions = new short[_maxSockets];
    _trailingVersions = new short[_maxSockets];

    _selector			= CrawlerServer.getServer().getEventLoop().getSelector();
    _resolver			= CrawlerServer.getServer().getDNSServiceResolver();

    _urlsPerSecMovingAverage = new MovingAverage(200);
    _kbPerSecMovingAverage = new MovingAverage(200);
    _avgDownloadSize = new MovingAverage(200);
    _urlsPerSecSmoothed =  new SmoothedAverage(.25);
    _kbPerSecSmoothed = new SmoothedAverage(.25);

    _crawlInterfaces = crawlInterfaceList;
    _crawlerName = crawlerName;
    http_date_format = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.US);
    http_date_format.setTimeZone(new SimpleTimeZone(0, "GMT"));

    // set the default ccbot user agent string 
    NIOHttpConnection.setDefaultUserAgentString("CCBot/1.0 (+http://www.commoncrawl.org/bot.html)");
  }


  public void clearQueues() {
    if (!_running) { 
      for (int i=0;i<_active.length;++i) { 
        _active[i] = null;
      }
      _pending.clear();
    }
    else {
      throw new IllegalStateException();
    }
  }

  public void shutdown() { 
    clearQueues();
  }


  public void queueURLs(LinkedList<CrawlTarget> urlList) {
    for (CrawlTarget url : urlList) {
      //LOG.debug("Adding URL:"+url.getURL() + " to Fetcher Queue");
      _pending.add(url);
    }
    // fillQueue(false);
  }


  public void queueURL(CrawlTarget target) {
    _pending.add(target);
    //fillQueue(false);
  }	

  public void start() {

    // reset stats ... 
    finishCount = 0;
    successCount = 0;
    failureCount  = 0;
    // flip running bit ...
    _running = true;

    fillQueue(false);

    _timeoutTimer = new Timer(TIMEOUT_TIMER_INTERVAL,true, new Timer.Callback() {

      public void timerFired(Timer timer) {

        fillQueue(true);

      }
    });

    // register timeout timer ... 
    CrawlerServer.getServer().getEventLoop().setTimer(_timeoutTimer);
  }

  public void stop() {
    // flip running bit ..
    _running = false;

    // first step .. cancel timer ... 
    if (_timeoutTimer != null) { 
      CrawlerServer.getServer().getEventLoop().cancelTimer(_timeoutTimer);
      _timeoutTimer = null;
    }
    // next cancel all active connections ... 
    for (int i=0;i<_active.length;++i){ 

      if (_active[i] != null) {

        CrawlContext context = (CrawlContext) _active[i].getContext();

        _active[i].setContext(null);
        // close the connection 
        _active[i].close();
        // null out the slot ... 
        _active[i] = null;
        // and add the connection back to the pending queue ... 
        // add the item back to pending list ... 
        _pending.addFirst(context.getURL());
      }
    }
    // clear appropriate summary fields ...
    connectionCount = 0;
    resolvingCount = 0;
    connectingCount = 0;
    sendingCount = 0;
    receivingCount = 0;
  }

  private void logGET(CrawlTarget url, int index) { 

    if (Environment.detailLogEnabled()) { 
      //	  if ( (url.getFlags() & CrawlURL.Flags.IsRobotsURL) == 0) { 

      StringBuffer sb = new StringBuffer();


      sb.append(String.format("%1$20.20s ",CCStringUtils.dateStringFromTimeValue(System.currentTimeMillis())));
      sb.append(String.format("%1$4.4s ",url.getResultCode()));
      sb.append(String.format("%1$4.4s ",url.getRetryCount()));
      sb.append(String.format("%1$4.4s ",url.getRedirectCount()));
      sb.append(url.getOriginalURL());
      sb.append(" ");
      if ((url.getFlags() & CrawlURL.Flags.IsRedirected) != 0) { 
        sb.append(url.getRedirectURL());
      }
      CrawlerServer.getEngine().getGETLog().info(sb.toString());
      //	  }
    }
  }


  /** figure out which ip address(source) to use for the specified crawl target
   * 
   * @return index number of the interface to use
   */
  private int getCrawlInterfaceForCrawlTarget(CrawlTarget target) { 

    if (target.getCrawlInterface() != -1) { 
      return target.getCrawlInterface();
    }

    if (target.getSourceList() != null) {
      // save current interface 
      int nextCrawlInterface = target.getSourceList().getNextCrawlInterface();
      // set next interface 
      target.getSourceList().setNextCrawlInterface((nextCrawlInterface+1) % _crawlInterfaces.length);
      // set affinity in target 
      target.setCrawlInterface(nextCrawlInterface);

      return nextCrawlInterface;
    }
    else { 
      return 0;
    }
  }

  private boolean fillSlot(int index,CrawlTarget optionalTarget) {

    // dont fill slot in paused state ... 
    if (!isPaused() || optionalTarget != null) { 

      if (_active[index] != null) { 
        LOG.error("fill Slot Called on Non-Empty Slot:"+ index + " With URL:" + _active[index].getURL());
      }
      // if there are pending urls ... 	
      if (optionalTarget != null || _pending.size() != 0) { 
        // pop a url off of the queue ... or use the optionally passed in target 
        CrawlTarget crawlTarget = (optionalTarget != null) ? optionalTarget : _pending.removeFirst();

        try {
          URL fetchURL = new URL(crawlTarget.getActiveURL());
          URL originalURL = (crawlTarget.getRedirectCount() == 0) ? fetchURL : new URL(crawlTarget.getOriginalURL());


          // open a new connection and assign it to the available slot ...
          if (_crawlInterfaces!= null) { 
            _active[index] = new NIOHttpConnection(fetchURL,_crawlInterfaces[getCrawlInterfaceForCrawlTarget(crawlTarget)],_selector,_resolver,crawlTarget.getCookieStore());
          }
          else {  
            _active[index] = new NIOHttpConnection(fetchURL,_selector,_resolver,crawlTarget.getCookieStore());
          }

          // LOG.info("### FETCHER Alloc HTTPConnect to:" + fetchURL + " Slot:" + index);

          //TODO: MAJOR HACK
          // disable proxy requests for robots
          if ((crawlTarget.getFlags() & CrawlURL.Flags.IsRobotsURL) == 0) {
            if (CrawlerServer.getServer().getProxyAddress() != null) {
              // check to see if we should be using a proxy server 
              _active[index].setProxyServer(CrawlerServer.getServer().getProxyAddress());
            }
          }

          // add in special source header
          _active[index].getRequestHeaders().setIfNotSet("x-cc-id",_crawlerName);
          // add in cache tests if present 
          if (crawlTarget.getRedirectCount() == 0) { 
            if (crawlTarget.getLastModifiedTime() != -1) { 
              _active[index].getRequestHeaders().setIfNotSet(
                  "If-Modified-Since",http_date_format.format(new Date(crawlTarget.getLastModifiedTime())));

            }
            if (crawlTarget.getETag() != null) { 
              _active[index].getRequestHeaders().setIfNotSet(
                  "If-None-Match",crawlTarget.getETag());

            }
          }

          _activeVersions[index] = (short) ((_activeVersions[index] + 1) % 10); 

          long currentTime = System.currentTimeMillis();


          if (crawlTarget.getRedirectCount() != 0) {
            String newHost = fetchURL.getHost();
            String originalHost = originalURL.getHost();
            if (newHost != null && originalHost != null && newHost.equalsIgnoreCase(originalHost)) { 
              crawlTarget.getSourceList().populateIPAddressForTarget(fetchURL.getHost(),crawlTarget);
            }
          }
          // IFF NOT Redirect 
          else {
            // if the cached ip is still valid based on stored TTL ... 
            if (crawlTarget.getServerIP() != 0 && crawlTarget.getServerIPTTL() >= System.currentTimeMillis()) { 
              // then set the resolved address data members (thus enabling us to bypass dns lookup)
              _active[index].setResolvedAddress(IPAddressUtils.IntegerToInetAddress(crawlTarget.getServerIP()),crawlTarget.getServerIPTTL(),null);
            }
            else { 
              if (Environment.detailLogEnabled())
              {
                if (crawlTarget.getServerIP() == 0)
                  LOG.info("#### IP Address for Host:" + fetchURL.getHost() + " Not Set. Will require DNS Resolution");
                else 
                  LOG.info("#### TTL of Cached IP Expired for Host:" + fetchURL.getHost() + ". Will require DNS Resolution");
              }
            }
          }

          _active[index].setListener(this);
          _active[index].setContext(new CrawlContext(crawlTarget,index));
          _active[index].setDownloadMax(DOWNLOAD_LIMIT);

          if (!_failConnections) { 
            _active[index].open();
            //LOG.info("### FETCHER called open on connection to:" + fetchURL + " slot:" + index);
          }
          else {  
            throw new IOException("Deliberately Skipped Open and FAILED Connection");
          }

          snapShotConnectionCount++;
          connectionCount++;
          if (Environment.detailLogEnabled())
            LOG.info("Filled SLOT:"+index + " With URL:" + crawlTarget.getActiveURL());

          // inform the target of the status change 
          crawlTarget.fetchStarting(_active[index]);
          // LOG.info("### FETCHER called fetchStarting for:" + fetchURL + " slot:" + index);

          // log it ... 
          logGET(crawlTarget,index);
          // and construct the http request ...

        }
        catch (UnknownHostException e) { 
          //TODO: CLEAR SLOT BEFORE CALLING fetchFailed!!!!
          if (_active[index] != null) { 
            _active[index].setContext(null);
            _active[index].close();
          }
          _active[index] = null;
          if (Environment.detailLogEnabled())
            LOG.error("Maformed URL Exception Processing URL:" + crawlTarget.getActiveURL());
          crawlTarget.fetchFailed(CrawlURL.FailureReason.MalformedURL,e.toString());

          failureCount++;
        }
        catch (MalformedURLException e) { 

          //TODO: CLEAR SLOT BEFORE CALLING fetchFailed!!!!
          if (_active[index] != null) { 
            _active[index].setContext(null);
            _active[index].close();
          }
          _active[index] = null;
          if (Environment.detailLogEnabled())
            LOG.error("Maformed URL Exception Processing URL:" + crawlTarget.getActiveURL());
          crawlTarget.fetchFailed(CrawlURL.FailureReason.MalformedURL,e.toString());

          failureCount++;
        }
        catch (IOException e2){
          if (Environment.detailLogEnabled())   
            LOG.error("IOException Processing URL:" + crawlTarget.getActiveURL() + " Details:" + e2.getMessage());

          //TODO: WATCH IT!!! - always clear slot FIRST because fetchFailed calls back into fillSlot!!!!
          if (_active[index] != null) { 
            _active[index].setContext(null);
            _active[index].close();
          }
          _active[index] = null;

          // LOG.debug("Fetch FAILED URL:"+ context.getURL().getURL() + " Code:"+ failureCode);
          // notify url of failure ... 
          //TODO: Investigate if it is SANE!!! to call back into fillSlot from fetchFailed !!!
          crawlTarget.fetchFailed(CrawlURL.FailureReason.IOException,e2.getMessage());

          failureCount++;
        }
        catch (Exception e) { 

          LOG.error("Runtime Exception Processing URL:" + crawlTarget.getActiveURL() + " Details:" + e.getMessage());

          //TODO: WATCH IT!!! - always clear slot FIRST because fetchFailed calls back into fillSlot!!!!
          if (_active[index] != null) { 
            _active[index].setContext(null);
            _active[index].close();
          }
          _active[index] = null;

          // LOG.debug("Fetch FAILED URL:"+ context.getURL().getURL() + " Code:"+ failureCode);
          // notify url of failure ... 
          //TODO: Investigate if it is SANE!!! to call back into fillSlot from fetchFailed !!!
          crawlTarget.fetchFailed(CrawlURL.FailureReason.RuntimeError,e.getMessage());

          failureCount++;

        }
      }
    }
    return _active[index] != null;	
  }

  private void fillQueue(boolean checkForTimeout) { 

    // LOG.debug("fillQueue BEGIN- activeCount:"+connectionCount + " pendingCount:" + _pending.size());

    for (int index=0;index<_active.length;++index) { 

      if (_active[index] != null && checkForTimeout) { 

        if (_active[index].hasTimedOut()) { 

          CrawlContext context = (CrawlContext)_active[index].getContext();

          NIOHttpConnection theTimedOutConnection = _active[index];

          if (context != null) {
            if (Environment.detailLogEnabled())
              LOG.error("Fetch TimedOut for Original URL:"+context.getURL().getOriginalURL() + " ActiveURL:" + context.getURL().getActiveURL());

            switch (theTimedOutConnection.getTimeoutState()) { 

              case AWAITING_RESOLUTION: 
                // reduce resolving count if necessary ... 
                resolvingCount--;
                break;

              case AWAITING_CONNECT:  
                connectingCount--;
                break;

              case SENDING_REQUEST: 
                sendingCount--;
                break;

              case RECEIVING_HEADERS: 
                receivingCount--;
                break;

            }

            //TODO: DO ALL SLOT OPERATIONS BEFORE CALLING fetchFailed since it is calling back into fillQueue!!! BAD!!!
            _active[index].setContext(null);
            _active[index].close();
            _active[index] = null;
            connectionCount--;
            failureCount++;

            if (theTimedOutConnection.getTimeoutState() == NIOHttpConnection.State.AWAITING_CONNECT) { 
              context.getURL().fetchFailed(CrawlURL.FailureReason.ConnectTimeout, "TimedOut in Fill Queue AWAITING_CONNECT");
            }
            else if (theTimedOutConnection.getTimeoutState() == NIOHttpConnection.State.AWAITING_RESOLUTION){
              context.getURL().fetchFailed(CrawlURL.FailureReason.DNSFailure, "TimedOut in Fill Queue AWAITING_RESOLUTION");
            }
            else { 
              context.getURL().fetchFailed(CrawlURL.FailureReason.Timeout, "TimedOut in Fill Queue RECEIVING_DATA");
            }
          }
          else { 
            LOG.error("Context NULL in fillQueue call");
            throw new RuntimeException("Context Should NOT be NULL");
          }
        }
      }

      // if the current slot is empty .... 
      if (_active[index] == null) {

        // try to fill the slot ... 
        if (!fillSlot(index,null)) { 
          // if failed to fill slot, either break out or continue (if checking for timeouts)
          if (!checkForTimeout)
            break;
        }
      }
    }

    // LOG.debug("fillQueue END- activeCount:"+connectionCount + " pendingCount:" + _pending.size());
  }


  /** NIOHttpConnection.Listener overloads **/
  // @Override
  public void HttpConnectionStateChanged(NIOHttpConnection theConnection,State oldState, State state) {

    if (Environment.detailLogEnabled())
      LOG.info("URL:"+theConnection.getURL() + " OldState:"+ oldState + " NewState:"+state);

    // only process events if we are in a running state ... 
    if (_running)  { 

      if (oldState == State.AWAITING_RESOLUTION) {
        // reduce resolving count if necessary ... 
        resolvingCount--;
      }
      else if (oldState == State.AWAITING_CONNECT) { 
        connectingCount--;
      }
      else if (oldState == State.SENDING_REQUEST) { 
        sendingCount--;
      }
      else if (oldState == State.RECEIVING_HEADERS) { 
        receivingCount--;
      }


      if (state == State.DONE || state == State.ERROR) {

        // log it ... 
        if (Environment.detailLogEnabled())
          LOG.debug("ConnectionState for URL:" + theConnection.getURL() + " Changed from:" + oldState + " to:" + state);

        // get context 
        CrawlContext context = (CrawlContext)theConnection.getContext();

        if (context == null) { 
          LOG.error("Context is NULL for Connection to URL:"+theConnection.getURL() + " Connection State:" + state);
        }
        else { 

          //TODO: RELEASE SLOT UPFRONT !!!
          if (Environment.detailLogEnabled())
            LOG.info("Releasing SLOT:" + context.getIndex() + " URL:" + _active[context.getIndex()].getURL());
          // either way, this connection is now dead ... 
          _active[context.getIndex()].setContext(null);
          _active[context.getIndex()].close();

          _active[context.getIndex()] = null;
          // decrement active count 
          connectionCount--;

          // increment finish count no matter what ... 
          finishCount++;

          if (state == State.DONE) {
            URLFP urlFingerprint = URLUtils.getURLFPFromURL(
                (context.getURL().getRedirectCount() == 0) ? 
                    context.getURL().getOriginalURL()
                    : context.getURL().getRedirectURL(), false);
            // update local history bloom filer
            CrawlerServer.getEngine().getLocalBloomFilter().add(urlFingerprint);
            
            try { 
              // increment success count and process results ... 
              successCount++;

              if (snapShotTime != -1) { 
                // increment snapshot stats ... 
                snapShotURLCount++;
                // increment cumulative number
                cumulativeURLCount++;
              }

              // handle redirects ... 
              if (theConnection.isRedirectResponse()) {

                // if redirect count == 0, preserve original data... 
                if (context.getURL().getRedirectCount() == 0) { 
                  context.getURL().cacheOriginalRequestData(theConnection);
                }
                // increment redirect counter ...
                context.getURL().incRedirectCount();
                // if either max redirect limit exceeded or location is null ...
                if (context.getURL().getRedirectCount() > MAX_REDIRECTS || theConnection.getRedirectLocation() == null) { 
                  String errorDescription = null;
                  if (context.getURL().getRedirectCount() > MAX_REDIRECTS)
                    errorDescription = "Max Redirect Count Exceeded";
                  else 
                    errorDescription = "Location not found in Redirect Headers";
                  // fail the url ... 
                  context.getURL().fetchFailed(CrawlURL.FailureReason.RedirectFailed, errorDescription);
                }
                // otherwise, silently re-queue process the redirect ... 
                else { 

                  try { 
                    URL originalURL = new URL(context.getURL().getOriginalURL());
                    String redirectLocation = theConnection.getRedirectLocation().toLowerCase();
                    URL redirectURL = (redirectLocation.startsWith("http://") || redirectLocation.startsWith("https://")) 
                        ? new URL(theConnection.getRedirectLocation()) : new URL(originalURL,theConnection.getRedirectLocation());  
                        String redirectURLStr = redirectURL.toString();

                        // by default process the url ... 
                        boolean processRedirect = true;

                        if ((context.getURL().getFlags() & CrawlURL.Flags.IsRobotsURL) == 0) { 
                          // but if url is different from original url ... 
                          if (!redirectURLStr.equals(context.getURL().getOriginalURL())) {

                            URLFP redirectFingerprint = URLUtils.getURLFPFromURL(redirectURLStr, false);

                            if (redirectFingerprint != null) { 
                              // validate the url against the bloom filter to see that we have not visited it before ...  
                              if (CrawlerServer.getEngine().getLocalBloomFilter().isPresent(redirectFingerprint)) { 
                                // yes we have ... fail the url ... 
                                LOG.info("!!!!Rejecting redirect. from:" + originalURL + " to:" + redirectURL +". Already Visited Target URL");
                                context.getURL().fetchFailed(CrawlURL.FailureReason.RedirectFailed, "Alread Visited Redirect Location:" + theConnection.getRedirectLocation());
                                processRedirect = false;
                              }
                            }
                            else { 
                              LOG.error("!!!!Rejecting redirect. from:" + originalURL + " to:" + redirectURL +". Redirect Fingerprint returned Null Fingerprint! RedirectString:" + theConnection.getRedirectLocation());
                            }
                          }
                        }

                        if (processRedirect) { 
                          if (Environment.detailLogEnabled())
                            LOG.info("Redirecting request:" + originalURL + " to:" + redirectURL);
                          // set up redirect metdata ... 
                          context.getURL().setFlags(context.getURL().getFlags() | CrawlURL.Flags.IsRedirected);
                          context.getURL().setRedirectURL(redirectURLStr);
                          // refill slot ... 
                          fillSlot(context.getIndex(), context.getURL());
                        }
                        //}
                        //else { 
                        // circular redirect fail case 
                        //  context.getURL().fetchFailed(CrawlURL.FailureReason.RedirectFailed, "Circular Redirect detected:" + theConnection.getRedirectLocation());
                        //}
                  }
                  catch (MalformedURLException e) { 
                    // invalid url fail case ... 
                    context.getURL().fetchFailed(CrawlURL.FailureReason.RedirectFailed, "Malformed URL:" + theConnection.getRedirectLocation());
                  }
                }
              }
              else { 
                // ok before passing things on ... check to see if this was a successful get as a result of a redirect ...
                if (context.getURL().getRedirectCount() != 0 && context.getURL().getActiveURL() != null) {
                  URLFP fingerprint = URLUtils.getURLFPFromURL(context.getURL().getActiveURL(),false);
                  if (fingerprint == null) { 
                    LOG.error("####!!!! getURLFPFromURL Returned NULL FOR URL" + context.getURL().getActiveURL());
                  }
                  else { 
                    CrawlerServer.getEngine().getLocalBloomFilter().add(fingerprint);
                  }

                }

                _avgDownloadSize.addSample((double)theConnection.getContentBuffer().available());
                // process this as a successful get
                context.getURL().fetchSucceeded(theConnection,theConnection.getResponseHeaders(),theConnection.getContentBuffer());
              }
            }
            catch (Exception e) { 
              LOG.error("Exception processing HttpConnectionStateChange-DONE:" + CCStringUtils.stringifyException(e));
              context.getURL().fetchFailed(CrawlURL.FailureReason.RuntimeError, "Exception:" + CCStringUtils.stringifyException(e));
            }

          }
          else if (state == State.ERROR) { 

            // increment failure count ... 
            failureCount++;

            int failureCode = CrawlURL.FailureReason.UNKNOWN;
            // generate accurate failure reason ... 
            switch (theConnection.getErrorType()) { 
              case RESOLVER_FAILURE: 			failureCode = CrawlURL.FailureReason.ResolverFailure;break;
              case DNS_FAILURE:					failureCode = CrawlURL.FailureReason.DNSFailure;break;
              case IOEXCEPTION:					failureCode = CrawlURL.FailureReason.IOException;break;
              case TIMEOUT:							failureCode = CrawlURL.FailureReason.Timeout;break;
            }

            // LOG.debug("Fetch FAILED URL:"+ context.getURL().getURL() + " Code:"+ failureCode);
            // notify url of failure ... 
            context.getURL().fetchFailed(failureCode,(theConnection.getErrorDesc() != null) ? theConnection.getErrorDesc() : "ERROR During Connection State Change");
          }

          // repopulate slot (if possible)
          if (_active[context.getIndex()] == null) { 
            fillSlot(context.getIndex(),null);
          }
        }
      }
      else if (state == State.AWAITING_RESOLUTION) { 
        resolvingCount++;
      }
      else if (state == State.AWAITING_CONNECT) { 
        connectingCount++;
      }
      else if (state == State.SENDING_REQUEST) { 
        // get context 
        CrawlContext context = (CrawlContext)theConnection.getContext();

        // if context is valid ... send the crawl target a fetchStarted event ... 
        if (context != null) { 
          context.getURL().fetchStarted();
        }
        else { 
          LOG.error("SENDING_REQUEST STATE TRIGGERED W/ NULL CONTEXT URL:" + theConnection.getURL());
        }
        sendingCount++;
      }
      else if (state == State.RECEIVING_HEADERS) { 
        receivingCount++;
      }
    }
  }

  //@Override
  public void HttpContentAvailable(NIOHttpConnection theConnection,NIOBufferList contentBuffer) { 
    // NOOP
  }



  public void collectStats(CrawlerStats crawlerStats,RuntimeStatsCollector stats) { 

    _snapshotNumber++;

    long curTime = System.currentTimeMillis();

    stats.setIntValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.HTTPFetcher_ActiveConnections,connectionCount );
    stats.setIntValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.HTTPFetcher_FetcherQueueSize,_pending.size());
    stats.setIntValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.HTTPFetcher_TotalSuccessfulConnects,successCount );
    stats.setIntValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.HTTPFetcher_TotalFailedConnects,failureCount );
    stats.setIntValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.HTTPFetcher_ConnectionsInResolvingState,resolvingCount );
    stats.setIntValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.HTTPFetcher_ConnectionsInConnectingState,connectingCount );
    stats.setIntValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.HTTPFetcher_ConnectionsInSendingState,sendingCount );
    stats.setIntValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.HTTPFetcher_ConnectionsInRecevingState,receivingCount );

    if (snapShotTime != -1) {
      stats.setIntValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.HTTPFetcher_TimeDeltaBetweenSnapshots, (int)(curTime - snapShotTime));
    }

    double urlsPerSecond= 0;
    int bytesSnapShot = 0;
    double bytesPerSec = 0;

    // if last snap shot time is set ... 
    if (snapShotTime != -1) { 
      // calculate urls / sec 
      int millisecondsElapsed = (int)(curTime - snapShotTime);
      urlsPerSecond =( (double)snapShotURLCount / ((double)millisecondsElapsed / 1000.00));

      _urlsPerSecMovingAverage.addSample(urlsPerSecond);
      _urlsPerSecSmoothed.addSample(urlsPerSecond);

      cumulativeURLSSEC += urlsPerSecond;
      snapShotCount += 1;

      bytesSnapShot = (int) (NIOHttpConnection.getCumulativeBytesRead() - this.snapShotDownloadAmt);
      snapShotDownloadAmt = NIOHttpConnection.getCumulativeBytesRead();

      bytesPerSec =( (double)bytesSnapShot / ((double)millisecondsElapsed / 1000.00));

      _kbPerSecMovingAverage.addSample(bytesPerSec / 1000.00);
      _kbPerSecSmoothed.addSample(bytesPerSec / 1000.00);

      cumulativeDownloadPerSec += bytesPerSec;
    }
    snapShotTime = curTime;
    if (firstSnapShotTime == -1)
      firstSnapShotTime = snapShotTime;
    snapShotURLCount = 0;
    snapShotConnectionCount = 0;


    synchronized(crawlerStats) { 
      crawlerStats.setUrlsPerSecond((float)_urlsPerSecMovingAverage.getAverage());
      crawlerStats.setMbytesDownPerSecond((float)(_kbPerSecMovingAverage .getAverage() / 1000.00 ));
      crawlerStats.setBytesDownloaded(crawlerStats.getBytesDownloaded() + bytesSnapShot);
      crawlerStats.setAverageDownloadSize((float)_avgDownloadSize.getAverage());
    }

    stats.setDoubleValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.HTTPFetcher_SnapshotURLSPerSecond,urlsPerSecond);
    stats.setDoubleValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.HTTPFetcher_MovingAverageURLSPerSecond,_urlsPerSecMovingAverage.getAverage());
    stats.setDoubleValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.HTTPFetcher_SmoothedURLSPerSecond,_urlsPerSecSmoothed.getAverage());

    stats.setDoubleValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.HTTPFetcher_SnapshotKBPerSec,bytesPerSec/1000.00);
    stats.setDoubleValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.HTTPFetcher_MovingAverageKBPerSec,_kbPerSecMovingAverage .getAverage());
    stats.setDoubleValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.HTTPFetcher_SmoothedKBPerSec,_kbPerSecSmoothed.getAverage());

    int active = 0;
    StringBuffer sb = new StringBuffer();
    int MAX_LINE_LEN = 20;

    sb.append("[");
    for (int i=0;i<MAX_LINE_LEN;++i) { 
      sb.append(i%10);
    }
    sb.append("]\n[");
    long currentTime = System.currentTimeMillis();

    // iterate connections ...
    int i=0;
    for (;i<_active.length;) { 

      stats.setArrayValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.HTTPFetcher_LaggingConnectionDetailArray, _active.length, i,null);

      if (_active[i] != null) {

        if (_activeVersions[i] != _trailingVersions[i]) {
          sb.append("<FONT color=red>");
        }
        if (_active[i].getOpenTime() == -1) { 
          sb.append("?");
        }
        else if (_active[i].getState() == NIOHttpConnection.State.AWAITING_RESOLUTION) { 
          if (currentTime - _active[i].getOpenTime() <= 60000)
            sb.append("r");
          else 
            sb.append("R");
        }
        else if (currentTime - _active[i].getOpenTime() <= 60000) { 
          sb.append(_activeVersions[i]);
        }
        else if (currentTime - _active[i].getOpenTime() <= 120000) { 
          sb.append("!");
          stats.setArrayValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.HTTPFetcher_LaggingConnectionDetailArray, _active.length, i,"[!]["+(currentTime - _active[i].getOpenTime())+"]" + _active[i].getURL().toString()); 
        }
        else { 
          sb.append("$");
          stats.setArrayValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.HTTPFetcher_LaggingConnectionDetailArray, _active.length, i,"[$]["+(currentTime - _active[i].getOpenTime())+"]" + _active[i].getURL().toString()); 
        }
        active++;
      }
      else {
        sb.append("-");
      }
      if (_activeVersions[i] != _trailingVersions[i]) {
        sb.append("</FONT>");
        _trailingVersions[i] = _activeVersions[i]; 
      }
      if (++i%MAX_LINE_LEN == 0) { 
        sb.append("]\n[");
      }
    }
    for (;i%MAX_LINE_LEN != 0;++i) { 
      sb.append(" ");
    }
    sb.append("]");

    stats.setStringValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.HTTPFetcher_ConnectionMap,sb.toString());
    stats.setLongValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.HTTPFetcher_CumilativeKBytesIN, NIOHttpConnection.getCumulativeBytesRead() / 1000);
    stats.setLongValue(CrawlerEngineStats.ID,CrawlerEngineStats.Name.HTTPFetcher_CumilativeKBytesOUT, NIOHttpConnection.getCumulativeBytesWritten() / 1000);

  }

}
