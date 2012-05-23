package org.commoncrawl.service.statscollector;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.record.Buffer;
import org.commoncrawl.async.CallbackWithResult;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.async.ConcurrentTask.CompletionCallback;
import org.commoncrawl.rpc.base.shared.BinaryProtocol;
import org.commoncrawl.service.statscollector.CrawlerStats;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.time.Hour;

import com.google.common.collect.ImmutableSortedMap;

public class CrawlerStatsCollection extends StatsCollection<CrawlerStats> {

  public static final String GROUP_KEY = "crawlerStats"; 
  
  public CrawlerStatsCollection(StatsLogManager logFileManager,String uniqueKey) throws IOException {
    super(logFileManager, GROUP_KEY, uniqueKey);
  }

  @Override
  public CrawlerStats bufferToValueType(Buffer incomingBuffer) throws IOException {
    DataInputBuffer buffer = new DataInputBuffer();
    buffer.reset(incomingBuffer.get(),0, incomingBuffer.getCount());
    CrawlerStats recordOut = new CrawlerStats();
    recordOut.deserialize(buffer, new BinaryProtocol());
    return recordOut;
  }

  @Override
  public void combineHourlyValues(CrawlerStats sourceValue, CrawlerStats otherValue) {
    sourceValue.setUrlsProcessed(sourceValue.getUrlsProcessed() + otherValue.getUrlsProcessed());
    sourceValue.setUrlsSucceeded(sourceValue.getUrlsSucceeded() + otherValue.getUrlsSucceeded());
    sourceValue.setUrlsFailed(sourceValue.getUrlsFailed() + otherValue.getUrlsFailed());
    sourceValue.setHttp200Count(sourceValue.getHttp200Count() + otherValue.getHttp200Count());
    sourceValue.setHttp300Count(sourceValue.getHttp300Count() + otherValue.getHttp300Count());
    sourceValue.setHttp301Count(sourceValue.getHttp301Count() + otherValue.getHttp301Count());
    sourceValue.setHttp302Count(sourceValue.getHttp302Count() + otherValue.getHttp302Count());
    sourceValue.setHttp304Count(sourceValue.getHttp304Count() + otherValue.getHttp304Count());
    sourceValue.setHttp400Count(sourceValue.getHttp400Count() + otherValue.getHttp400Count());
    sourceValue.setHttp403Count(sourceValue.getHttp403Count() + otherValue.getHttp403Count());
    sourceValue.setHttp404Count(sourceValue.getHttp404Count() + otherValue.getHttp404Count());
    sourceValue.setHttp500Count(sourceValue.getHttp500Count() + otherValue.getHttp500Count());
    sourceValue.setHttpOtherCount(sourceValue.getHttpOtherCount() + otherValue.getHttpOtherCount());
    sourceValue.setHttpErrorUNKNOWN(sourceValue.getHttpErrorUNKNOWN() + otherValue.getHttpErrorUNKNOWN());
    sourceValue.setHttpErrorUnknownProtocol(sourceValue.getHttpErrorUnknownProtocol() + otherValue.getHttpErrorUnknownProtocol());
    sourceValue.setHttpErrorMalformedURL(sourceValue.getHttpErrorMalformedURL() + otherValue.getHttpErrorMalformedURL());
    sourceValue.setHttpErrorTimeout(sourceValue.getHttpErrorTimeout() + otherValue.getHttpErrorTimeout());
    sourceValue.setHttpErrorDNSFailure(sourceValue.getHttpErrorDNSFailure() + otherValue.getHttpErrorDNSFailure());
    sourceValue.setHttpErrorResolverFailure(sourceValue.getHttpErrorResolverFailure() + otherValue.getHttpErrorResolverFailure());
    sourceValue.setHttpErrorIOException(sourceValue.getHttpErrorIOException() + otherValue.getHttpErrorIOException());
    sourceValue.setHttpErrorRobotsExcluded(sourceValue.getHttpErrorRobotsExcluded() + otherValue.getHttpErrorRobotsExcluded());
    sourceValue.setHttpErrorNoData(sourceValue.getHttpErrorNoData() + otherValue.getHttpErrorNoData());
    sourceValue.setHttpErrorRobotsParseError(sourceValue.getHttpErrorRobotsParseError() + otherValue.getHttpErrorRobotsParseError());
    sourceValue.setHttpErrorRedirectFailed(sourceValue.getHttpErrorRedirectFailed() + otherValue.getHttpErrorRedirectFailed());
    sourceValue.setHttpErrorRuntimeError(sourceValue.getHttpErrorRuntimeError() + otherValue.getHttpErrorRuntimeError());
    sourceValue.setHttpErrorConnectTimeout(sourceValue.getHttpErrorConnectTimeout() + otherValue.getHttpErrorConnectTimeout());
    sourceValue.setHttpErrorBlackListedHost(sourceValue.getHttpErrorBlackListedHost() + otherValue.getHttpErrorBlackListedHost());
    sourceValue.setHttpErrorBlackListedURL(sourceValue.getHttpErrorBlackListedURL() + otherValue.getHttpErrorBlackListedURL());
    sourceValue.setHttpErrorTooManyErrors(sourceValue.getHttpErrorTooManyErrors() + otherValue.getHttpErrorTooManyErrors());
    sourceValue.setHttpErrorInCache(sourceValue.getHttpErrorInCache() + otherValue.getHttpErrorInCache());
    sourceValue.setHttpErrorInvalidResponseCode(sourceValue.getHttpErrorInvalidResponseCode() + otherValue.getHttpErrorInvalidResponseCode());
    sourceValue.setHttpErrorBadRedirectData(sourceValue.getHttpErrorBadRedirectData() + otherValue.getHttpErrorBadRedirectData());
    sourceValue.setAverageDownloadSize((sourceValue.getAverageDownloadSize() + otherValue.getAverageDownloadSize()) / 2.0f);
    sourceValue.setUrlsPerSecond((sourceValue.getUrlsPerSecond() + otherValue.getUrlsPerSecond())/2.0f);
    sourceValue.setMbytesDownPerSecond((sourceValue.getMbytesDownPerSecond() + otherValue.getMbytesDownPerSecond()) / 2.0f);
    sourceValue.setBytesDownloaded(sourceValue.getBytesDownloaded() + otherValue.getBytesDownloaded());
    sourceValue.setCrawlerMemoryUsedRatio((sourceValue.getCrawlerMemoryUsedRatio() + otherValue.getCrawlerMemoryUsedRatio()) / 2.0f);
    sourceValue.setUrlsInFetcherQueue(otherValue.getUrlsInFetcherQueue());
    sourceValue.setUrlsInLoaderQueue(otherValue.getUrlsInLoaderQueue());
    sourceValue.setActvieRobotsRequests(otherValue.getActvieRobotsRequests());
    sourceValue.setRobotsRequestsSucceeded(sourceValue.getRobotsRequestsSucceeded() + otherValue.getRobotsRequestsSucceeded());
    sourceValue.setRobotsRequestsFailed(sourceValue.getRobotsRequestsFailed() + otherValue.getRobotsRequestsFailed());
    sourceValue.setRedirectResultAfter1Hops(sourceValue.getRedirectResultAfter1Hops() + otherValue.getRedirectResultAfter1Hops());
    sourceValue.setRedirectResultAfter2Hops(sourceValue.getRedirectResultAfter2Hops() + otherValue.getRedirectResultAfter2Hops());
    sourceValue.setRedirectResultAfter3Hops(sourceValue.getRedirectResultAfter3Hops() + otherValue.getRedirectResultAfter3Hops());
    sourceValue.setRedirectResultAfterGT3Hops(sourceValue.getRedirectResultAfterGT3Hops() + otherValue.getRedirectResultAfterGT3Hops());
    sourceValue.setActiveHosts(otherValue.getActiveHosts());
    sourceValue.setScheduledHosts(otherValue.getScheduledHosts());
    sourceValue.setIdledHosts(otherValue.getIdledHosts());
    sourceValue.setActiveDNSRequests(otherValue.getActiveDNSRequests());
    sourceValue.setQueuedDNSRequests(otherValue.getQueuedDNSRequests());
    sourceValue.setFailedDNSRequests(sourceValue.getFailedDNSRequests() + otherValue.getFailedDNSRequests());
    sourceValue.setSuccessfullDNSRequests(sourceValue.getSuccessfullDNSRequests() + otherValue.getSuccessfullDNSRequests());
    sourceValue.setRobotsRequestsQueuedForParse(otherValue.getRobotsRequestsQueuedForParse());
    sourceValue.setRobotsRequestsSuccessfullParse(sourceValue.getRobotsRequestsSuccessfullParse() + otherValue.getRobotsRequestsSuccessfullParse());
    sourceValue.setRobotsRequestsFailedParse(sourceValue.getRobotsRequestsFailedParse() + otherValue.getRobotsRequestsFailedParse());
    sourceValue.setRobotsFileExcludesAllContent(sourceValue.getRobotsFileExcludesAllContent() + otherValue.getRobotsFileExcludesAllContent());
    sourceValue.setRobotsFileHadCrawlDelay(sourceValue.getRobotsFileHadCrawlDelay() + otherValue.getRobotsFileHadCrawlDelay());
    sourceValue.setRobotsFileHasExplicitMention(sourceValue.getRobotsFileHasExplicitMention() + otherValue.getRobotsFileHasExplicitMention());
    sourceValue.setRobotsFileExplicitlyExcludesAll(sourceValue.getRobotsFileExplicitlyExcludesAll() + otherValue.getRobotsFileExplicitlyExcludesAll());
  }

  
  public static CrawlerStats combineValues(Collection<CrawlerStats> collection) { 

    CrawlerStats valueOut = new CrawlerStats();
    

    // accumulate stats into daily value first ... 
    for (CrawlerStats item : collection) { 
      
      valueOut.setUrlsProcessed(valueOut.getUrlsProcessed() + item.getUrlsProcessed());
      valueOut.setUrlsSucceeded(valueOut.getUrlsSucceeded() + item.getUrlsSucceeded());
      valueOut.setUrlsFailed(valueOut.getUrlsFailed() + item.getUrlsFailed());
      valueOut.setHttp200Count(valueOut.getHttp200Count() + item.getHttp200Count());
      valueOut.setHttp300Count(valueOut.getHttp300Count() + item.getHttp300Count());
      valueOut.setHttp301Count(valueOut.getHttp301Count() + item.getHttp301Count());
      valueOut.setHttp302Count(valueOut.getHttp302Count() + item.getHttp302Count());
      valueOut.setHttp304Count(valueOut.getHttp304Count() + item.getHttp304Count());
      valueOut.setHttp400Count(valueOut.getHttp400Count() + item.getHttp400Count());
      valueOut.setHttp403Count(valueOut.getHttp403Count() + item.getHttp403Count());
      valueOut.setHttp404Count(valueOut.getHttp404Count() + item.getHttp404Count());
      valueOut.setHttp500Count(valueOut.getHttp500Count() + item.getHttp500Count());
      valueOut.setHttpOtherCount(valueOut.getHttpOtherCount() + item.getHttpOtherCount());
      valueOut.setHttpErrorUNKNOWN(valueOut.getHttpErrorUNKNOWN() + item.getHttpErrorUNKNOWN());
      valueOut.setHttpErrorUnknownProtocol(valueOut.getHttpErrorUnknownProtocol() + item.getHttpErrorUnknownProtocol());
      valueOut.setHttpErrorMalformedURL(valueOut.getHttpErrorMalformedURL() + item.getHttpErrorMalformedURL());
      valueOut.setHttpErrorTimeout(valueOut.getHttpErrorTimeout() + item.getHttpErrorTimeout());
      valueOut.setHttpErrorDNSFailure(valueOut.getHttpErrorDNSFailure() + item.getHttpErrorDNSFailure());
      valueOut.setHttpErrorResolverFailure(valueOut.getHttpErrorResolverFailure() + item.getHttpErrorResolverFailure());
      valueOut.setHttpErrorIOException(valueOut.getHttpErrorIOException() + item.getHttpErrorIOException());
      valueOut.setHttpErrorRobotsExcluded(valueOut.getHttpErrorRobotsExcluded() + item.getHttpErrorRobotsExcluded());
      valueOut.setHttpErrorNoData(valueOut.getHttpErrorNoData() + item.getHttpErrorNoData());
      valueOut.setHttpErrorRobotsParseError(valueOut.getHttpErrorRobotsParseError() + item.getHttpErrorRobotsParseError());
      valueOut.setHttpErrorRedirectFailed(valueOut.getHttpErrorRedirectFailed() + item.getHttpErrorRedirectFailed());
      valueOut.setHttpErrorRuntimeError(valueOut.getHttpErrorRuntimeError() + item.getHttpErrorRuntimeError());
      valueOut.setHttpErrorConnectTimeout(valueOut.getHttpErrorConnectTimeout() + item.getHttpErrorConnectTimeout());
      valueOut.setHttpErrorBlackListedHost(valueOut.getHttpErrorBlackListedHost() + item.getHttpErrorBlackListedHost());
      valueOut.setHttpErrorBlackListedURL(valueOut.getHttpErrorBlackListedURL() + item.getHttpErrorBlackListedURL());
      valueOut.setHttpErrorTooManyErrors(valueOut.getHttpErrorTooManyErrors() + item.getHttpErrorTooManyErrors());
      valueOut.setHttpErrorInCache(valueOut.getHttpErrorInCache() + item.getHttpErrorInCache());
      valueOut.setHttpErrorInvalidResponseCode(valueOut.getHttpErrorInvalidResponseCode() + item.getHttpErrorInvalidResponseCode());
      valueOut.setHttpErrorBadRedirectData(valueOut.getHttpErrorBadRedirectData() + item.getHttpErrorBadRedirectData());
      valueOut.setAverageDownloadSize(valueOut.getAverageDownloadSize() + item.getAverageDownloadSize());
      valueOut.setUrlsPerSecond(valueOut.getUrlsPerSecond() + item.getUrlsPerSecond());
      valueOut.setMbytesDownPerSecond(valueOut.getMbytesDownPerSecond() + item.getMbytesDownPerSecond());
      valueOut.setBytesDownloaded(valueOut.getBytesDownloaded() + item.getBytesDownloaded());
      valueOut.setCrawlerMemoryUsedRatio(valueOut.getCrawlerMemoryUsedRatio() + item.getCrawlerMemoryUsedRatio());
      valueOut.setUrlsInFetcherQueue(valueOut.getUrlsInFetcherQueue() + item.getUrlsInFetcherQueue());
      valueOut.setUrlsInLoaderQueue(valueOut.getUrlsInLoaderQueue() + item.getUrlsInLoaderQueue());
      valueOut.setActvieRobotsRequests(valueOut.getActvieRobotsRequests() + item.getActvieRobotsRequests());
      valueOut.setRobotsRequestsSucceeded(valueOut.getRobotsRequestsSucceeded() + item.getRobotsRequestsSucceeded());
      valueOut.setRobotsRequestsFailed(valueOut.getRobotsRequestsFailed() + item.getRobotsRequestsFailed());
      valueOut.setRedirectResultAfter1Hops(valueOut.getRedirectResultAfter1Hops() + item.getRedirectResultAfter1Hops());
      valueOut.setRedirectResultAfter2Hops(valueOut.getRedirectResultAfter2Hops() + item.getRedirectResultAfter2Hops());
      valueOut.setRedirectResultAfter3Hops(valueOut.getRedirectResultAfter3Hops() + item.getRedirectResultAfter3Hops());
      valueOut.setRedirectResultAfterGT3Hops(valueOut.getRedirectResultAfterGT3Hops() + item.getRedirectResultAfterGT3Hops());
      valueOut.setActiveHosts(valueOut.getActiveHosts() + item.getActiveHosts());
      valueOut.setScheduledHosts(valueOut.getScheduledHosts() + item.getScheduledHosts());
      valueOut.setIdledHosts(valueOut.getIdledHosts() + item.getIdledHosts());
      valueOut.setActiveDNSRequests(valueOut.getActiveDNSRequests() + item.getActiveDNSRequests());
      valueOut.setQueuedDNSRequests(valueOut.getQueuedDNSRequests() + item.getQueuedDNSRequests());
      valueOut.setFailedDNSRequests(valueOut.getFailedDNSRequests() + item.getFailedDNSRequests());
      valueOut.setSuccessfullDNSRequests(valueOut.getSuccessfullDNSRequests() + item.getSuccessfullDNSRequests());
      valueOut.setRobotsRequestsQueuedForParse(valueOut.getRobotsRequestsQueuedForParse() + item.getRobotsRequestsQueuedForParse());
      valueOut.setRobotsRequestsSuccessfullParse(valueOut.getRobotsRequestsSuccessfullParse() + item.getRobotsRequestsSuccessfullParse());
      valueOut.setRobotsRequestsFailedParse(valueOut.getRobotsRequestsFailedParse() + item.getRobotsRequestsFailedParse());
      valueOut.setRobotsFileExcludesAllContent(valueOut.getRobotsFileExcludesAllContent() + item.getRobotsFileExcludesAllContent());
      valueOut.setRobotsFileHadCrawlDelay(valueOut.getRobotsFileHadCrawlDelay() + item.getRobotsFileHadCrawlDelay());
      valueOut.setRobotsFileHasExplicitMention(valueOut.getRobotsFileHasExplicitMention() + item.getRobotsFileHasExplicitMention());
      valueOut.setRobotsFileExplicitlyExcludesAll(valueOut.getRobotsFileExplicitlyExcludesAll() + item.getRobotsFileExplicitlyExcludesAll());
    }
    
    // next average non-cumilative stats
    valueOut.setAverageDownloadSize(valueOut.getAverageDownloadSize() / collection.size());
    valueOut.setUrlsPerSecond(valueOut.getUrlsPerSecond() / collection.size());
    valueOut.setMbytesDownPerSecond(valueOut.getMbytesDownPerSecond() / collection.size());
    valueOut.setCrawlerMemoryUsedRatio(valueOut.getCrawlerMemoryUsedRatio()/ collection.size());
    valueOut.setUrlsInFetcherQueue(valueOut.getUrlsInFetcherQueue() / collection.size());
    valueOut.setUrlsInLoaderQueue(valueOut.getUrlsInLoaderQueue() / collection.size());
    valueOut.setActvieRobotsRequests(valueOut.getActvieRobotsRequests() / collection.size());
    valueOut.setRobotsRequestsQueuedForParse(valueOut.getRobotsRequestsQueuedForParse() / collection.size());
    
    
    return valueOut;
  }
  
  
  @Override
  public CrawlerStats createDailyValue(Set<Entry<Hour, CrawlerStats>> hourlyValueSet) {
    
    CrawlerStats dailyValueOut = new CrawlerStats();
    
    // accumulate stats into daily value first ... 
    for (Entry<Hour,CrawlerStats> entry : hourlyValueSet) { 
      
      CrawlerStats hourlyValue = entry.getValue();

      dailyValueOut.setUrlsProcessed(dailyValueOut.getUrlsProcessed() + hourlyValue.getUrlsProcessed());
      dailyValueOut.setUrlsSucceeded(dailyValueOut.getUrlsSucceeded() + hourlyValue.getUrlsSucceeded());
      dailyValueOut.setUrlsFailed(dailyValueOut.getUrlsFailed() + hourlyValue.getUrlsFailed());
      dailyValueOut.setHttp200Count(dailyValueOut.getHttp200Count() + hourlyValue.getHttp200Count());
      dailyValueOut.setHttp300Count(dailyValueOut.getHttp300Count() + hourlyValue.getHttp300Count());
      dailyValueOut.setHttp301Count(dailyValueOut.getHttp301Count() + hourlyValue.getHttp301Count());
      dailyValueOut.setHttp302Count(dailyValueOut.getHttp302Count() + hourlyValue.getHttp302Count());
      dailyValueOut.setHttp304Count(dailyValueOut.getHttp304Count() + hourlyValue.getHttp304Count());
      dailyValueOut.setHttp400Count(dailyValueOut.getHttp400Count() + hourlyValue.getHttp400Count());
      dailyValueOut.setHttp403Count(dailyValueOut.getHttp403Count() + hourlyValue.getHttp403Count());
      dailyValueOut.setHttp404Count(dailyValueOut.getHttp404Count() + hourlyValue.getHttp404Count());
      dailyValueOut.setHttp500Count(dailyValueOut.getHttp500Count() + hourlyValue.getHttp500Count());
      dailyValueOut.setHttpOtherCount(dailyValueOut.getHttpOtherCount() + hourlyValue.getHttpOtherCount());
      dailyValueOut.setHttpErrorUNKNOWN(dailyValueOut.getHttpErrorUNKNOWN() + hourlyValue.getHttpErrorUNKNOWN());
      dailyValueOut.setHttpErrorUnknownProtocol(dailyValueOut.getHttpErrorUnknownProtocol() + hourlyValue.getHttpErrorUnknownProtocol());
      dailyValueOut.setHttpErrorMalformedURL(dailyValueOut.getHttpErrorMalformedURL() + hourlyValue.getHttpErrorMalformedURL());
      dailyValueOut.setHttpErrorTimeout(dailyValueOut.getHttpErrorTimeout() + hourlyValue.getHttpErrorTimeout());
      dailyValueOut.setHttpErrorDNSFailure(dailyValueOut.getHttpErrorDNSFailure() + hourlyValue.getHttpErrorDNSFailure());
      dailyValueOut.setHttpErrorResolverFailure(dailyValueOut.getHttpErrorResolverFailure() + hourlyValue.getHttpErrorResolverFailure());
      dailyValueOut.setHttpErrorIOException(dailyValueOut.getHttpErrorIOException() + hourlyValue.getHttpErrorIOException());
      dailyValueOut.setHttpErrorRobotsExcluded(dailyValueOut.getHttpErrorRobotsExcluded() + hourlyValue.getHttpErrorRobotsExcluded());
      dailyValueOut.setHttpErrorNoData(dailyValueOut.getHttpErrorNoData() + hourlyValue.getHttpErrorNoData());
      dailyValueOut.setHttpErrorRobotsParseError(dailyValueOut.getHttpErrorRobotsParseError() + hourlyValue.getHttpErrorRobotsParseError());
      dailyValueOut.setHttpErrorRedirectFailed(dailyValueOut.getHttpErrorRedirectFailed() + hourlyValue.getHttpErrorRedirectFailed());
      dailyValueOut.setHttpErrorRuntimeError(dailyValueOut.getHttpErrorRuntimeError() + hourlyValue.getHttpErrorRuntimeError());
      dailyValueOut.setHttpErrorConnectTimeout(dailyValueOut.getHttpErrorConnectTimeout() + hourlyValue.getHttpErrorConnectTimeout());
      dailyValueOut.setHttpErrorBlackListedHost(dailyValueOut.getHttpErrorBlackListedHost() + hourlyValue.getHttpErrorBlackListedHost());
      dailyValueOut.setHttpErrorBlackListedURL(dailyValueOut.getHttpErrorBlackListedURL() + hourlyValue.getHttpErrorBlackListedURL());
      dailyValueOut.setHttpErrorTooManyErrors(dailyValueOut.getHttpErrorTooManyErrors() + hourlyValue.getHttpErrorTooManyErrors());
      dailyValueOut.setHttpErrorInCache(dailyValueOut.getHttpErrorInCache() + hourlyValue.getHttpErrorInCache());
      dailyValueOut.setHttpErrorInvalidResponseCode(dailyValueOut.getHttpErrorInvalidResponseCode() + hourlyValue.getHttpErrorInvalidResponseCode());
      dailyValueOut.setHttpErrorBadRedirectData(dailyValueOut.getHttpErrorBadRedirectData() + hourlyValue.getHttpErrorBadRedirectData());
      dailyValueOut.setAverageDownloadSize(dailyValueOut.getAverageDownloadSize() + hourlyValue.getAverageDownloadSize());
      dailyValueOut.setUrlsPerSecond(dailyValueOut.getUrlsPerSecond() + hourlyValue.getUrlsPerSecond());
      dailyValueOut.setMbytesDownPerSecond(dailyValueOut.getMbytesDownPerSecond() + hourlyValue.getMbytesDownPerSecond());
      dailyValueOut.setBytesDownloaded(dailyValueOut.getBytesDownloaded() + hourlyValue.getBytesDownloaded());
      dailyValueOut.setCrawlerMemoryUsedRatio(dailyValueOut.getCrawlerMemoryUsedRatio() + hourlyValue.getCrawlerMemoryUsedRatio());
      dailyValueOut.setUrlsInFetcherQueue(dailyValueOut.getUrlsInFetcherQueue() + hourlyValue.getUrlsInFetcherQueue());
      dailyValueOut.setUrlsInLoaderQueue(dailyValueOut.getUrlsInLoaderQueue() + hourlyValue.getUrlsInLoaderQueue());
      dailyValueOut.setActvieRobotsRequests(dailyValueOut.getActvieRobotsRequests() + hourlyValue.getActvieRobotsRequests());
      dailyValueOut.setRobotsRequestsSucceeded(dailyValueOut.getRobotsRequestsSucceeded() + hourlyValue.getRobotsRequestsSucceeded());
      dailyValueOut.setRobotsRequestsFailed(dailyValueOut.getRobotsRequestsFailed() + hourlyValue.getRobotsRequestsFailed());
      dailyValueOut.setRedirectResultAfter1Hops(dailyValueOut.getRedirectResultAfter1Hops() + hourlyValue.getRedirectResultAfter1Hops());
      dailyValueOut.setRedirectResultAfter2Hops(dailyValueOut.getRedirectResultAfter2Hops() + hourlyValue.getRedirectResultAfter2Hops());
      dailyValueOut.setRedirectResultAfter3Hops(dailyValueOut.getRedirectResultAfter3Hops() + hourlyValue.getRedirectResultAfter3Hops());
      dailyValueOut.setRedirectResultAfterGT3Hops(dailyValueOut.getRedirectResultAfterGT3Hops() + hourlyValue.getRedirectResultAfterGT3Hops());
      dailyValueOut.setActiveHosts(dailyValueOut.getActiveHosts() + hourlyValue.getActiveHosts());
      dailyValueOut.setScheduledHosts(dailyValueOut.getScheduledHosts() + hourlyValue.getScheduledHosts());
      dailyValueOut.setIdledHosts(dailyValueOut.getIdledHosts() + hourlyValue.getIdledHosts());
      dailyValueOut.setActiveDNSRequests(dailyValueOut.getActiveDNSRequests() + hourlyValue.getActiveDNSRequests());
      dailyValueOut.setQueuedDNSRequests(dailyValueOut.getQueuedDNSRequests() + hourlyValue.getQueuedDNSRequests());
      dailyValueOut.setFailedDNSRequests(dailyValueOut.getFailedDNSRequests() + hourlyValue.getFailedDNSRequests());
      dailyValueOut.setSuccessfullDNSRequests(dailyValueOut.getSuccessfullDNSRequests() + hourlyValue.getSuccessfullDNSRequests());
      dailyValueOut.setRobotsRequestsQueuedForParse(dailyValueOut.getRobotsRequestsQueuedForParse() + hourlyValue.getRobotsRequestsQueuedForParse());
      dailyValueOut.setRobotsRequestsSuccessfullParse(dailyValueOut.getRobotsRequestsSuccessfullParse() + hourlyValue.getRobotsRequestsSuccessfullParse());
      dailyValueOut.setRobotsRequestsFailedParse(dailyValueOut.getRobotsRequestsFailedParse() + hourlyValue.getRobotsRequestsFailedParse());
      dailyValueOut.setRobotsFileExcludesAllContent(dailyValueOut.getRobotsFileExcludesAllContent() + hourlyValue.getRobotsFileExcludesAllContent());
      dailyValueOut.setRobotsFileHadCrawlDelay(dailyValueOut.getRobotsFileHadCrawlDelay() + hourlyValue.getRobotsFileHadCrawlDelay());
      dailyValueOut.setRobotsFileHasExplicitMention(dailyValueOut.getRobotsFileHasExplicitMention() + hourlyValue.getRobotsFileHasExplicitMention());
      dailyValueOut.setRobotsFileExplicitlyExcludesAll(dailyValueOut.getRobotsFileExplicitlyExcludesAll() + hourlyValue.getRobotsFileExplicitlyExcludesAll());
    }
    
    // next average non-cumilative stats
    dailyValueOut.setAverageDownloadSize(dailyValueOut.getAverageDownloadSize() / hourlyValueSet.size());
    dailyValueOut.setUrlsPerSecond(dailyValueOut.getUrlsPerSecond() / hourlyValueSet.size());
    dailyValueOut.setMbytesDownPerSecond(dailyValueOut.getMbytesDownPerSecond() / hourlyValueSet.size());
    dailyValueOut.setCrawlerMemoryUsedRatio(dailyValueOut.getCrawlerMemoryUsedRatio()/ hourlyValueSet.size());
    dailyValueOut.setUrlsInFetcherQueue(dailyValueOut.getUrlsInFetcherQueue() / hourlyValueSet.size());
    dailyValueOut.setUrlsInLoaderQueue(dailyValueOut.getUrlsInLoaderQueue() / hourlyValueSet.size());
    dailyValueOut.setActvieRobotsRequests(dailyValueOut.getActvieRobotsRequests() / hourlyValueSet.size());
    dailyValueOut.setRobotsRequestsQueuedForParse(dailyValueOut.getRobotsRequestsQueuedForParse() / hourlyValueSet.size());
    
    return dailyValueOut;
  }

  @Override
  public Buffer valueTypeToBuffer(CrawlerStats value) throws IOException {
    DataOutputBuffer bufferOut = new DataOutputBuffer();
    value.serialize(bufferOut,new BinaryProtocol());
    return new Buffer(bufferOut.getData(),0,bufferOut.getLength());
  }
  
  
  public static void main(String[] args) {
    EventLoop eventLoop = new EventLoop();
    eventLoop.start();
    try {
      StatsLogManager logManager = new StatsLogManager(eventLoop, new File("/Users/rana"));
      CrawlerStatsCollection collection = new CrawlerStatsCollection(logManager,"ccn01-PROXY-Prod");

      collection.dumpHourlyToJSON(System.out);
      
      System.out.println();
      
      final Semaphore blockingSemaphore = new Semaphore(0);
      
      collection.dumpDailyToJSON(System.out, new CallbackWithResult<Boolean>() {
        
        @Override
        public void execute(Boolean result) {
          blockingSemaphore.release();
        }
      });
      
      blockingSemaphore.acquireUninterruptibly();

      logManager.shutdown();

    } catch (IOException e) {
      e.printStackTrace();
    }
    eventLoop.stop();
  }

  @Override
  public void setUniqueKeyInValue(CrawlerStats value) {
    value.setCrawlerName(_uniqueKey);
  }

  @Override
  public CrawlerStats allocateValueType() {
    return new CrawlerStats();
  }
}
