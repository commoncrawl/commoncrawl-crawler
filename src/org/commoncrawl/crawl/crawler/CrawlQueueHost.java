package org.commoncrawl.crawl.crawler;

import java.io.IOException;
import java.io.Writer;

/**
 * 
 * @author rana
 *
 * interface defining a CrawlList container as seen by the CrawlQueue
 */
public interface CrawlQueueHost extends CrawlHost {

	/** clear wait state on this host **/
	public void clearWaitState();

	/** feed items to the queue **/
	public void feedQueue();

	/** get or create a new CrawlList given list id **/
	public CrawlList getCrawlList(int listId);

	/** get queued url count for this host **/
	public int getQueuedURLCount();

	/** get wait time for this host, if in a wait state **/
	public long getWaitTime();

	/** purge all object refrences to help garbage collection process */
	public void purgeReferences();
	
	/** returns true if no more active lists are present **/
	public boolean noActiveLists();
	
	
	/** dump debug information **/
	public void dumpDetails(Writer writer) throws IOException;

	
}
