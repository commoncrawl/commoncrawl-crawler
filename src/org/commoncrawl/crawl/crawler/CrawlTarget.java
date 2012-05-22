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

package org.commoncrawl.crawl.crawler;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.record.Buffer;
import org.apache.hadoop.util.StringUtils;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.common.Environment;
import org.commoncrawl.io.internal.NIOBufferList;
import org.commoncrawl.io.internal.NIOHttpConnection;
import org.commoncrawl.io.shared.NIOHttpHeaders;
import org.commoncrawl.protocol.CrawlSegmentHost;
import org.commoncrawl.protocol.CrawlSegmentURL;
import org.commoncrawl.protocol.CrawlURL;
import org.commoncrawl.server.CommonCrawlServer;
import org.commoncrawl.util.internal.HttpCookieUtils.CookieStore;
import org.commoncrawl.util.shared.CCStringUtils;
import org.commoncrawl.util.shared.IPAddressUtils;
import org.commoncrawl.util.shared.IntrusiveList;

/**
 * class encapsulates a target url and related state
 * 
 * @author rana
 *
 */
public final class CrawlTarget extends
    IntrusiveList.IntrusiveListElement<CrawlTarget> {

  /** logging **/
  private static final Log        LOG               = LogFactory
                                                        .getLog(CrawlTarget.class);

  private int                     _segmentId;
  private CrawlList               _sourceList;
  // private CrawlURL _urlData;
  private byte                    _crawlInterface   = -1;
  private long                    _urlFP;
  private String                  _url;
  private String                  _redirectURL      = null;
  // private Buffer _crawlDatum = null;
  private long                    _hostFP           = -1;
  private long                    _requestStartTime = -1;
  private int                     _hostIPAddress    = 0;
  private long                    _hostIPTTL;
  private byte                    _retryCount       = 0;
  private byte                    _redirectCount    = 0;
  private byte                    _flags            = 0;
  private long                    _lastModifiedTime = -1;
  private String                  _etag             = null;
  private String                  _crawlDirectiveJSON = null;

  // optional crawl completion callback
  private CrawlItemStatusCallback _callback;

  public static class HTTPData {

    public HTTPData() {

    }

    public HTTPData(String headers, short resultCode, int serverIPAddress,
        long serverIPTTL) {
      _headers = headers;
      _resultCode = resultCode;
      _serverIP = serverIPAddress;
      _serverIPTTL = serverIPTTL;
    }

    public String _headers;
    public short  _resultCode = 0;
    public int    _serverIP;
    public long   _serverIPTTL;
  }

  private HTTPData _originalRequestData     = null;
  private String   _activeRequestHeaders    = null;
  private short    _activeRequestResultCode = 0;

  public CrawlTarget(int segmentId, CrawlList sourceList) {
    _sourceList = sourceList;
    _segmentId = segmentId;
  }

  public CrawlTarget(int segmentId, CrawlList sourceList,
      CrawlSegmentHost segmentHost, CrawlSegmentURL segmentURL) {
    _sourceList = sourceList;
    _segmentId = segmentId;
    _urlFP = segmentURL.getUrlFP();
    _url = segmentURL.getUrl();

    _hostFP = segmentHost.getHostFP();
    _lastModifiedTime = (segmentURL
        .isFieldDirty(CrawlSegmentURL.Field_LASTMODIFIEDTIME)) ? segmentURL
        .getLastModifiedTime() : -1;
    _etag = (segmentURL.isFieldDirty(CrawlSegmentURL.Field_ETAG)) ? segmentURL
        .getEtag() : null;
    if (segmentURL.isFieldDirty(CrawlSegmentURL.Field_CRAWLDIRECTIVEJSON)) { 
      _crawlDirectiveJSON = segmentURL.getCrawlDirectiveJSON();
    }
  }

  public CrawlTarget(int segmentId, CrawlList sourceList, String url,
      long fingerprint, CrawlItemStatusCallback callback) {
    _sourceList = sourceList;
    _segmentId = segmentId;
    _url = url;
    _urlFP = fingerprint;
    _callback = callback;
  }

  public CrawlTarget(CrawlList sourceList, PersistentCrawlTarget target) {
    _sourceList = sourceList;

    _segmentId = target.getSegmentId();
    _urlFP = target.getUrlFP();
    _url = target.getUrl();
    _redirectURL = target.getRedirectURL();
    // _crawlDatum = null; // target.getCrawlDatum();
    _hostFP = target.getHostFP();
    _hostIPAddress = target.getHostIPAddress();
    _hostIPTTL = target.getHostIPTTL();
    _retryCount = target.getRetryCount();
    _redirectCount = target.getRedirectCount();
    _flags = target.getFlags();

    if (target.getActiveRequestData().isFieldDirty(
        CrawlTargetHTTPData.Field_HEADERS))
      _activeRequestHeaders = target.getActiveRequestData().getHeaders();
    if (target.getActiveRequestData().isFieldDirty(
        CrawlTargetHTTPData.Field_RESULTCODE))
      _activeRequestResultCode = (short) target.getActiveRequestData()
          .getResultCode();

    if (target.isFieldDirty(PersistentCrawlTarget.Field_ORIGINALREQUESTDATA)) {
      _originalRequestData = new HTTPData();
      _originalRequestData._headers = target.getActiveRequestData()
          .getHeaders();
      _originalRequestData._resultCode = (short) target.getActiveRequestData()
          .getResultCode();
      _originalRequestData._serverIP = target.getActiveRequestData()
          .getServerIP();
      _originalRequestData._serverIPTTL = target.getActiveRequestData()
          .getServerIPTTL();
    }

    _lastModifiedTime = target
        .isFieldDirty(PersistentCrawlTarget.Field_LASTMODIFIEDTIME) ? target
        .getLastModifiedTime() : -1;
    _etag = target.isFieldDirty(PersistentCrawlTarget.Field_ETAG) ? target
        .getEtag() : null;
    _crawlDirectiveJSON = null;    
    if (target.isFieldDirty(PersistentCrawlTarget.Field_CRAWLDIRECTIVEJSON)) { 
      _crawlDirectiveJSON = target.getCrawlDirectiveJSON();
    }
  }

  private CrawlTarget(CrawlList sourceList) {
    _sourceList = sourceList;
  }

  public static CrawlTarget createTestCrawlTarget(CrawlList domain, String url) {
    CrawlTarget target = new CrawlTarget(domain);

    target._segmentId = 1;
    target._url = url;

    return target;
  }

  public PersistentCrawlTarget createPersistentTarget() {

    PersistentCrawlTarget targetOut = new PersistentCrawlTarget();

    targetOut.setSegmentId(_segmentId);
    targetOut.setUrlFP(_urlFP);
    targetOut.setUrl(_url);
    // targetOut.setCrawlDatum(_crawlDatum);
    targetOut.setHostFP(_hostFP);
    targetOut.setHostIPAddress(_hostIPAddress);
    targetOut.setHostIPTTL(_hostIPTTL);
    targetOut.setRedirectURL((_redirectURL == null) ? "" : _redirectURL);
    targetOut.setRetryCount(_retryCount);
    targetOut.setRedirectCount(_redirectCount);
    targetOut.setFlags(_flags);

    if (_activeRequestHeaders != null)
      targetOut.getActiveRequestData().setHeaders(_activeRequestHeaders);
    if (_activeRequestResultCode != 0)
      targetOut.getActiveRequestData().setResultCode(_activeRequestResultCode);

    if (_originalRequestData != null) {
      targetOut.getOriginalRequestData().setHeaders(
          _originalRequestData._headers);
      targetOut.getOriginalRequestData().setResultCode(
          _originalRequestData._resultCode);
      targetOut.getOriginalRequestData().setServerIP(
          _originalRequestData._serverIP);
      targetOut.getOriginalRequestData().setServerIPTTL(
          _originalRequestData._serverIPTTL);
    }

    if (_lastModifiedTime != -1) {
      targetOut.setLastModifiedTime(_lastModifiedTime);
    }
    if (_etag != null) {
      targetOut.setEtag(_etag);
    }
    if (_crawlDirectiveJSON != null) { 
      targetOut.setCrawlDirectiveJSON(_crawlDirectiveJSON);
    }
    
    return targetOut;
  }

  /**
   * set the crawl completion callback
   * 
   */
  public void setCompletionCallback(CrawlItemStatusCallback callback) {
    _callback = callback;
  }

  /**
   * get the completion callback (if specified)
   * 
   * @return callback object
   */
  public CrawlItemStatusCallback getCompletionCallback() {
    return _callback;
  }

  /**
   * get the source list which is managing this crawl target
   * 
   * @return CrawlList object
   */
  public CrawlList getSourceList() {
    return _sourceList;
  }

  /** get crawl host **/
  public CrawlListHost getCrawlHost() {
    return _sourceList.getHost();
  }

  /** get cookie store associated with this target **/
  public CookieStore getCookieStore() {
    CrawlListHost host = getCrawlHost();
    if (host != null) {
      return host.getCookieStore();
    }
    return null;
  }

  /**
   * set the source list that owns this target object
   * 
   * @param listObject
   */
  public void setSourceList(CrawlList listObject) {
    _sourceList = listObject;
  }

  /**
   * get the last modified time for this url (if previously set)
   * 
   * @return last modified time if set or -1 if not
   */
  public long getLastModifiedTime() {
    return _lastModifiedTime;
  }

  /**
   * 
   * @return crawl interface associated with this target or -1
   */
  public int getCrawlInterface() {
    return _crawlInterface;
  }

  /**
   * set the crawl interface associated with this target
   * 
   * @param crawlInterface
   *          - the index of the crawl interface to use with this target
   */
  public void setCrawlInterface(int crawlInterface) {
    _crawlInterface = (byte) crawlInterface;
  }

  /**
   * get the etag value for this url (if previously set)
   * 
   * @return etag for given target or null if not set
   */
  public String getETag() {
    return _etag;
  }

  /**
   * get the url fingerprint for this crawl target
   * 
   * @return
   */
  public long getFingerprint() {
    return _urlFP;
  }

  /**
   * get the host fingerprint for this crawl target
   * 
   * @return host fingerprint id
   */
  public long getHostFP() {
    return _hostFP;
  }

  /*
   * set the host fingerprint for this crawl target
   */
  public void setHostFP(long hostFingerprint) {
    _hostFP = hostFingerprint;
  }

  public int getResultCode() {
    return _activeRequestResultCode;
  }

  /** retrieve the orignal request data **/
  public HTTPData getOriginalRequestData() {
    return _originalRequestData;
  }

  /*
   * public Buffer getCrawlDatum() { return _crawlDatum; }
   */

  public boolean isRedirected() {
    return (_flags & CrawlURL.Flags.IsRedirected) != 0;
  }

  public String getActiveURL() {
    // if this is a redirected target ...
    if ((_flags & CrawlURL.Flags.IsRedirected) != 0) {
      // return the redirect url ...
      return _redirectURL;
    }
    // otherwise return the primary url ...
    return _url;
  }

  public String getOriginalURL() {
    return _url;
  }

  public void setOriginalURL(String url) {
    _url = url;
  }

  public String getRedirectURL() {
    return _redirectURL;
  }

  public void setRedirectURL(String url) {
    _redirectURL = url;
  }

  public int getSegmentId() {
    return _segmentId;
  }

  public int getRetryCount() {
    return _retryCount;
  }

  public int getRedirectCount() {
    return _redirectCount;
  }

  public void incRedirectCount() {
    _redirectCount++;
  }

  public int getFlags() {
    return _flags;
  }

  public void setFlags(int flags) {
    _flags = (byte) flags;
  }

  public long getServerIPTTL() {
    return _hostIPTTL;
  }

  public void setServerIPTTL(long ttl) {
    _hostIPTTL = ttl;
  }

  public int getServerIP() {
    return _hostIPAddress;
  }

  public void setServerIP(int ipAddress) {
    _hostIPAddress = ipAddress;
  }

  /**
   * set request start time
   * 
   */
  public void setRequestStartTime(long time) {
    _requestStartTime = time;
  }

  /**
   * get request start time
   * 
   */
  public long getRequestStartTime() {
    return _requestStartTime;
  }

  public void incrementRetryCounter() {
    _retryCount++;
  }

  public void cacheOriginalRequestData(NIOHttpConnection connection) {
    InetAddress address = connection.getResolvedAddress();
    int ipAddress = 0;
    if (address == null || address.getAddress() == null) {
      if (address == null) {
        LOG.error("### BUG resolved Adddress is null in cacheOriginalRequest! for Target:"
            + getOriginalURL());
      } else {
        LOG.error("### BUG resolved Adddress.getAddress returned null in cacheOriginalRequest! for Target:"
            + getOriginalURL());
      }
    } else {
      ipAddress = IPAddressUtils.IPV4AddressToInteger(address.getAddress());
    }

    _originalRequestData = new CrawlTarget.HTTPData(connection
        .getResponseHeaders().toString(),
        (short) connection.getHttpResponseCode(), ipAddress,
        connection.getResolvedAddressTTL());
  }

  private static String failureDescFromReason(int failureReason) {
    return CrawlURL.FailureReason.toString(failureReason);
  }

  public void logFailure(final CrawlerEngine engine, int failureReason,
      String errorDescription) {
    StringBuffer sb = new StringBuffer();

    if (errorDescription == null)
      errorDescription = "";

    sb.append(String.format("%1$20.20s ",
        CCStringUtils.dateStringFromTimeValue(System.currentTimeMillis())));
    sb.append(String.format("%1$15.15s ",
        engine.getCrawlInterfaceGivenIndex(getCrawlInterface())));
    sb.append(String.format("%1$15.15s ", failureDescFromReason(failureReason)));
    sb.append(String.format("%1s ", errorDescription));
    if ((getFlags() & CrawlURL.Flags.IsRedirected) != 0) {
      sb.append(getRedirectURL());
      sb.append(" ");
    }
    sb.append(getActiveURL());

    if (engine != null) {
      engine.getFailureLog().error(sb.toString());
    } else {
      System.out.println(sb.toString());
    }
  }

  public static void logFailureDetail(final CrawlerEngine engine,
      CrawlURL url, CrawlTarget optionalTarget, int failureReason,
      String errorDescription) {

    StringBuffer sb = new StringBuffer();

    if (errorDescription == null)
      errorDescription = "";

    sb.append(String.format("%1$20.20s ",
        CCStringUtils.dateStringFromTimeValue(System.currentTimeMillis())));
    sb.append(String.format(
        "%1$15.15s ",
        (optionalTarget != null) ? engine
            .getCrawlInterfaceGivenIndex(optionalTarget.getCrawlInterface())
            : null));
    sb.append(String.format("%1$15.15s ", failureDescFromReason(failureReason)));
    sb.append(String.format("%1s ", errorDescription));
    if ((url.getFlags() & CrawlURL.Flags.IsRedirected) != 0) {
      sb.append(url.getRedirectURL());
      sb.append(" ");
    }
    sb.append(url.getUrl());

    if (engine != null) {
      engine.getFailureLog().error(sb.toString());
    } else {
      System.out.println(sb.toString());
    }
  }

  public static void failURL(CrawlURL urlData, CrawlTarget optionalTarget,
      int failureReason, String errorDescription) {

    if (Environment.detailLogEnabled())
      LOG.info("Fetch Failed URL:" + urlData.getUrl() + " reason:"
          + failureReason);

    // and log this event to the custom failure log ...
    logFailureDetail(CrawlerServer.getEngine(), urlData, optionalTarget,
        failureReason, errorDescription);

    // if not a robots request
    if ((urlData.getFlags() & CrawlURL.Flags.IsRobotsURL) == 0) {
      // add in failure info ...
      urlData.setLastAttemptFailureReason((byte) failureReason);
      if (errorDescription != null) {
        urlData.setLastAttemptFailureDetail(errorDescription);
      }

      // and update segment progress logs ...
      if (CrawlerServer.getEngine() != null) {
        CrawlerServer.getEngine().crawlComplete(null, urlData, optionalTarget,
            false);
      }
    }
  }

  public void fetchFailed(int failureReason, String description) {
    _sourceList.fetchFailed(this, failureReason, description);
  }

  public void fetchStarting(NIOHttpConnection connection) {
    CrawlerServer.getEngine().fetchStarting(this, connection);
    // inform source list of the change ...
    _sourceList.fetchStarting(this, connection);
  }

  public void fetchStarted() {
    _sourceList.fetchStarted(this);
  }

  private final EventLoop getEventLoop() {
    return getServer().getEventLoop();
  }

  private final CommonCrawlServer getServer() {
    return getEngine().getServer();
  }

  private final CrawlerEngine getEngine() {
    return CrawlerServer.getEngine();
  }

  private String getRedirectLocation(int responseCode,
      NIOHttpHeaders httpHeaders, NIOBufferList nioContentBuffer) {

    String redirectLocation = null;

    if (responseCode >= 300 && responseCode < 400) {

      switch (responseCode) {

      // multiple choices ?
        case 300:
          // permanent
        case 301:
          // use proxy ...
        case 305:
          // temporary
        case 302:
          // redirect after post
        case 303:
          // temporary redirect
        case 307: {

          // attempt to extract location from headers ...
          int key = httpHeaders.getKey("Location");
          if (key == -1) {
            // attempt lowercase version ...
            key = httpHeaders.getKey("location");
          }
          if (key != -1) {
            redirectLocation = httpHeaders.getValue(key);
            if (Environment.detailLogEnabled())
              LOG.info("Redirect detected for target:" + getOriginalURL()
                  + " .New Location:" + redirectLocation);
          }
        }
          break;
      }
    }
    return redirectLocation;
  }

  /**
   * check final http response code against list of acceptable response code for
   * a successfull fetch
   * 
   */
  private static boolean isAcceptableSuccessResponseCode(int responseCode) {
    if ((responseCode >= 200 && responseCode < 300) || responseCode == 304
        || (responseCode >= 400 && responseCode < 500)) {
      return true;
    }
    return false;
  }

  public void fetchSucceeded(NIOHttpConnection connection,
      NIOHttpHeaders httpHeaders, NIOBufferList nioContentBuffer) {

    boolean failure = false;
    int failureReason = CrawlURL.FailureReason.UNKNOWN;
    Exception failureException = null;
    String failureDescription = "";

    // revalidate ip address here ...
    if (getRedirectCount() == 0) {
      // check to see if ip address go reresolved ...
      if (connection.getResolvedAddress() != null) {

        InetAddress address = connection.getResolvedAddress();

        int ipAddress = 0;

        if (address.getAddress() != null) {
          // if so, update url data information ...
          ipAddress = IPAddressUtils.IPV4AddressToInteger(address.getAddress());
        } else {
          LOG.error("### BUG int Address getAddress returned Null for target:"
              + getActiveURL());
        }

        // LOG.info("IP Address for URL:" + getActiveURL() + " is:" + ipAddress
        // + " ttl is:" + connection.getResolvedAddressTTL());
        setServerIP(ipAddress);
        setServerIPTTL(connection.getResolvedAddressTTL());
      }
    }

    Buffer contentBuffer = new Buffer();
    byte data[] = new byte[nioContentBuffer.available()];

    int responseCode = -1;

    try {
      responseCode = NIOHttpConnection.getHttpResponseCode(httpHeaders);

      if (!isAcceptableSuccessResponseCode(responseCode)) {
        failure = true;
        failureReason = CrawlURL.FailureReason.InvalidResponseCode;
        failureDescription = "URL:" + getOriginalURL()
            + " returned invalid responseCode:" + responseCode;
      }
    } catch (Exception e) {
      failure = true;
      failureReason = CrawlURL.FailureReason.RuntimeError;
      failureException = e;
      failureDescription = "getHTTPResponse Threw:"
          + StringUtils.stringifyException(e) + " for URL:" + getOriginalURL();
    }

    if (!failure) {
      // populate a conventional buffer object with content data ...

      try {
        // read data from nio buffer into byte array
        nioContentBuffer.read(data);
        // and reset source buffer .... (releasing memory )...
        nioContentBuffer.reset();
        // set byte buffer into buffer object ...
        contentBuffer.set(data);

      } catch (IOException e) {

        failure = true;
        failureReason = CrawlURL.FailureReason.IOException;
        failureException = e;
        failureDescription = "Unable to read Content Buffer from successfull Fetch for URL:"
            + getOriginalURL();
      }
    }

    if (!failure) {
      // populate crawl url data
      _activeRequestHeaders = httpHeaders.toString();
      _activeRequestResultCode = (short) NIOHttpConnection
          .getHttpResponseCode(httpHeaders);
      ;
    }

    if (failure) {
      if (failureException != null) {
        if (Environment.detailLogEnabled())
          LOG.error(StringUtils.stringifyException(failureException));
      }
      fetchFailed(failureReason, failureDescription);
    } else {

      // call host ...
      _sourceList.fetchSucceeded(this, connection.getDownloadTime(),
          httpHeaders, contentBuffer);

      // Add to CrawlLog for both content gets and robots gets
      // create a crawl url object
      CrawlURL urlData = createCrawlURLObject(CrawlURL.CrawlResult.SUCCESS,
          contentBuffer);
      // set truncation flag if content truncation during download
      if (connection.isContentTruncated()) {
        urlData.setFlags(urlData.getFlags()
            | CrawlURL.Flags.TruncatedDuringDownload);
      }
      // and update segment progress logs ...
      getEngine().crawlComplete(connection, urlData, this, true);

      /*
       * if ((getFlags() & CrawlURL.Flags.IsRobotsURL) != 0) {
       * getEngine().logSuccessfulRobotsGET(connection, this); }
       */
    }
  }

  public CrawlURL createFailureCrawlURLObject(int failureReason,
      String errorDescription) {
    CrawlURL urlData = createCrawlURLObject(CrawlURL.CrawlResult.FAILURE, null);
    urlData.setLastAttemptFailureReason((byte) failureReason);
    return urlData;
  }

  public CrawlURL createCrawlURLObject(int result, Buffer contentBuffer) {

    // build a crawl url object ...
    CrawlURL crawlURL = new CrawlURL();

    long currentTime = System.currentTimeMillis();

    // original request fingerprint ...
    crawlURL.setFingerprint(getFingerprint());
    // original request url ...
    crawlURL.setUrl(getOriginalURL());

    // skip datum for now ...
    // crawlURL.setCrawlDatumData(getCrawlDatum());
    // original list id
    crawlURL.setListId(_sourceList.getListId());
    // original segment id
    crawlURL.setCrawlSegmentId(getSegmentId());
    // original host fingerprint ...
    crawlURL.setHostFP(getHostFP());

    // set the host ip in the crawl target ...

    // latest server ip information
    crawlURL.setServerIP(getServerIP());
    crawlURL.setServerIPTTL(getServerIPTTL());

    if (_originalRequestData != null) {
      // original request data if present ...
      crawlURL.setOriginalResultCode(_originalRequestData._resultCode);
      crawlURL.setOriginalHeaders(_originalRequestData._headers);
      crawlURL.setOriginalServerIP(_originalRequestData._serverIP);
      // url.setOriginalContentRaw(url.getOriginalContentRaw());
    }

    // set last crawl info ...
    // url.setLastAttemptCrawlerId();
    crawlURL.setLastAttemptTime(currentTime);
    // final disposition
    crawlURL.setLastAttemptResult((byte) result);
    // url.setLastCrawlTime(currentTime);

    // current result details ...
    if (_activeRequestHeaders != null)
      crawlURL.setHeaders(_activeRequestHeaders);
    if (_activeRequestResultCode != 0)
      crawlURL.setResultCode(_activeRequestResultCode);

    // current result content ...
    if (contentBuffer != null) {
      crawlURL.setFieldDirty(CrawlURL.Field_CONTENTRAW);
      crawlURL.setContentRaw(contentBuffer);
    }

    // finally, most importantly ... if redirected ...
    if ((getFlags() & CrawlURL.Flags.IsRedirected) != 0) {
      // check to see if urls match
      if (!getOriginalURL().equals(getActiveURL())) {
        crawlURL.setFlags(crawlURL.getFlags() | CrawlURL.Flags.IsRedirected);
        crawlURL.setRedirectURL(getActiveURL());
      }
    }

    // if robots, mark it so in the crawlURL object
    if ((getFlags() & CrawlURL.Flags.IsRobotsURL) != 0) {
      crawlURL.setFlags(crawlURL.getFlags() | CrawlURL.Flags.IsRobotsURL);
    }
    if (_crawlDirectiveJSON != null) { 
      crawlURL.setCrawlDirectiveJSON(_crawlDirectiveJSON);
    }
    return crawlURL;
  }

  public static CrawlURL allocateCrawlURLFromSegmentURL(int segmentId,
      CrawlSegmentHost host, CrawlSegmentURL segmentURL, boolean populateIPInfo) {

    // build a crawl url object ...
    CrawlURL crawlURL = new CrawlURL();

    crawlURL.setFingerprint(segmentURL.getUrlFP());
    crawlURL.setUrl(segmentURL.getUrl());

    // TODO: TRICKY BUFFER ASSIGNMENT BUT WORKS
    // crawlURL.setCrawlDatumData(new
    // Buffer(segmentURL.getCrawlDatumData().getReadOnlyBytes()));
    crawlURL.setCrawlSegmentId(segmentId);
    crawlURL.setListId(host.getListId());
    crawlURL.setHostFP(host.getHostFP());

    if (populateIPInfo) {
      // set the host ip in the crawl target ...
      crawlURL.setServerIP(host.getIpAddress());
      crawlURL.setServerIPTTL(host.getTtl());
    }

    return crawlURL;
  }

  public static CrawlURL allocateCrawlURLForFailure(String url,
      long fingerprint, int failureCode, String failureDetail) {
    // build a crawl url object ...
    CrawlURL crawlURL = new CrawlURL();

    crawlURL.setFingerprint(fingerprint);
    crawlURL.setUrl(url);
    crawlURL.setLastAttemptResult((byte) CrawlURL.CrawlResult.FAILURE);
    crawlURL.setLastAttemptFailureReason((byte) failureCode);
    crawlURL.setLastAttemptFailureDetail(failureDetail);

    return crawlURL;
  }

}
