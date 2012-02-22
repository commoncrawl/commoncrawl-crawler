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

package org.commoncrawl.crawl.crawler.tests;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.crawl.crawler.CrawlerEngine;
import org.commoncrawl.protocol.CrawlerService;
import org.commoncrawl.rpc.base.internal.AsyncClientChannel;
import org.commoncrawl.rpc.base.internal.AsyncClientChannel.ConnectionCallback;

/** 
 * A unit test framework around the crawler server
 *  
 * 
 * @author rana
 *
 */
public abstract class CrawlerUnitTest implements ConnectionCallback {

  /** logging **/
  private static final Log LOG = LogFactory.getLog(CrawlerUnitTest.class);

  
  CrawlerEngine           _engine = null;
  EventLoop _eventLoop = new EventLoop();
  AsyncClientChannel _channel;
  CrawlerService.AsyncStub _crawlerService;
  
  public boolean initialize(CrawlerEngine engine) { 
    _engine = engine;
    // start the event loop ... 
    _eventLoop.start();
    // open a connection to the crawler ... 
    try {
      _channel = new AsyncClientChannel(_eventLoop,_engine.getServer().getServerAddress(),_engine.getServer().getServerAddress(),this);
      _channel.open();
      _crawlerService = new CrawlerService.AsyncStub(_channel);
    } catch (IOException e) {
      LOG.error("IOException thrown while initializing Unit Test");
      e.printStackTrace();
      return false;
    }
    return true;
  }

  public void OutgoingChannelConnected(AsyncClientChannel channel) {
    LOG.error("Connected to Crawler RPC - running Unit Test Code");
    run();
  }

  public boolean OutgoingChannelDisconnected(AsyncClientChannel channel) {
    LOG.error("Diconnected from Crawler RPC");
    return false;
  }
  
  public abstract void run();
  
  
  
  
}
