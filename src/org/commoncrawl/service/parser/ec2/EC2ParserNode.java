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
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.protocol.CrawlURL;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.Tuples.Pair;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;

public class EC2ParserNode implements Runnable, Constants {

  public static final Log LOG = LogFactory.getLog(EC2ParserNode.class);

  
  Configuration _conf;
  Thread _thread;
  FileSystem _fs;
  UUID  _uuid;
  String _masterHost;
  String _hostName;
  
  public EC2ParserNode(String hostName,Configuration conf)throws IOException, URISyntaxException { 
    _conf = conf;
    _conf.set("fs.s3n.awsAccessKeyId", "079HD5ZAQSKEY542V7R2");
    _conf.set("fs.s3n.awsSecretAccessKey", "g4Ow3MSj77mqEw3uf4fZ22QPXuH991YP/rak8FJX");
    _fs = FileSystem.get(new URI("s3n://aws-publicdatasets/"), _conf);
    _uuid = UUID.randomUUID();
    _masterHost = "10.0.20.21";
    _hostName = hostName;
    startThread();
  }
  
  
  private static class QueueItem { 
    public QueueItem(Path path) {
      crawlLogPath = path;
    }
    
    Path crawlLogPath;
  }
  
  LinkedBlockingQueue<QueueItem> _queue = new LinkedBlockingQueue<QueueItem>();
  
  private void startThread() { 
    _thread = new Thread(this);
    _thread.start();
  }
  
  public void stop() { 
    if (_thread != null) { 
      try {
        LOG.info("Stopping Thread");
        _queue.put(new QueueItem(null));
        LOG.info("Waiting for Thread to Die");
        _thread.join();
        LOG.info("Thread dead");
        _thread = null;
      } catch (InterruptedException e) {
      }
    }
  }
  
  public void addToQueue(Path path)throws IOException { 
    _queue.add(new QueueItem(path));
  }
  
  
  static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
  
  GenericUrl buildCheckoutURL() { 
    GenericUrl url = new GenericUrl();
    url.setScheme("http");
    url.setHost(_masterHost);
    url.setPort(CrawlEnvironment.DEFAULT_EC2MASTER_HTTP_PORT);
    url.setPathParts(Lists.newArrayList("","checkout"));
    url.put("host",_hostName);
    url.put("uuid",_uuid.toString());
    LOG.info(url.build());
    return url;
  }

  GenericUrl buildPingURL(String activeFile,long pos) { 
    GenericUrl url = new GenericUrl();
    url.setScheme("http");
    url.setHost(_masterHost);
    url.setPort(CrawlEnvironment.DEFAULT_EC2MASTER_HTTP_PORT);
    url.setPathParts(Lists.newArrayList("","ping"));
    url.put("host",_hostName);
    url.put("uuid",_uuid.toString());
    url.put("activeFile", activeFile);
    url.put("pos",pos);
    
    LOG.info(url.build());
    return url;
  }

  GenericUrl buildCheckInURL(String activeFile,long pos) { 
    GenericUrl url = new GenericUrl();
    url.setScheme("http");
    url.setHost(_masterHost);
    url.setPort(CrawlEnvironment.DEFAULT_EC2MASTER_HTTP_PORT);
    url.setPathParts(Lists.newArrayList("","checkin"));
    url.put("host",_hostName);
    url.put("uuid",_uuid.toString());
    url.put("activeFile", activeFile);
    url.put("pos",pos);
    LOG.info(url.build());
    return url;
  }
  
  
  AtomicBoolean _shutdownActive = new AtomicBoolean();
  HttpRequestFactory factory = HTTP_TRANSPORT.createRequestFactory();
  
  private Pair<String,Long> checkoutFile()throws IOException { 
    GenericUrl url = buildCheckoutURL();
    
    HttpRequest request = factory.buildGetRequest(url);
    
    HttpResponse response = request.execute();
    
    if (response.getStatusCode() == 200) {
      JsonParser parser = new JsonParser();
      JsonObject e = parser.parse(new JsonReader(new InputStreamReader(response.getContent(),Charset.forName("UTF-8")))).getAsJsonObject();
      String logName = e.get("name").getAsString();
      long lastPos = e.get("lastPos").getAsLong();
      LOG.info("Got Name:" + logName + " Pos:" + lastPos);
      
      return new Pair<String,Long>(logName,lastPos);
    }
    return null;
  }
  
  public static final Path buildCrawlLogPath(String logName) { 
    return new Path("/" + CC_BUCKET_ROOT + CC_CRAWLLOG_SOURCE+logName);
  }
  
  public static final Path buildCrawlLogCheckpointPath(String logName,long timestamp,long position) { 
    return new Path("/" + CC_BUCKET_ROOT + CC_PARSER_INTERMEDIATE+logName+ "_"+timestamp+"_"+position + DONE_SUFFIX);
  }
  
  private static final int CHECKPOINT_INTERVAL = 1 * 5 * 1000;
  @Override
  public void run() {


    while (!_shutdownActive.get()) { 

      if (!_shutdownActive.get()) {

        try { 
          Pair<String,Long> checkoutInfo = checkoutFile();
          if (checkoutInfo != null) { 
            Path logPath = buildCrawlLogPath(checkoutInfo.e0);  
            LOG.info("Opening File At LogPath:" + logPath);

            SequenceFile.Reader reader 
            = new SequenceFile.Reader(_fs,logPath,_conf);

            long lastPos = checkoutInfo.e1;

            LOG.info("Seeking to Pos:" + lastPos);
            
            if (lastPos != 0) { 
              reader.seek(lastPos);
            }
            try { 
              Text key = new Text();
              CrawlURL urlData = new CrawlURL();
              long lastCheckpointTime = System.currentTimeMillis();
              while (reader.next(key,urlData)) {
                if (reader.getPosition() != lastPos) {
                  if (System.currentTimeMillis() - lastCheckpointTime  >= CHECKPOINT_INTERVAL) {  
                    doCheckpoint(checkoutInfo.e0,lastCheckpointTime,reader.getPosition(),false);
                    lastPos = reader.getPosition();
                    lastCheckpointTime = System.currentTimeMillis();
                  }
                }
                LOG.info("Pos:" + reader.getPosition() + " Key:" + key.toString() + " ValueLen:" + urlData.getContentRaw().getCount());
              }
              doCheckpoint(checkoutInfo.e0,lastCheckpointTime,reader.getPosition(),true);
            }
            finally {
              reader.close();
            }
          }
        } catch (IOException e) {
          LOG.error(CCStringUtils.stringifyException(e));
        }
      }

    }
  }
  
  private void doCheckpoint(String logFileName,long timestamp,long position, boolean isFinalCommit)throws IOException { 
    Path checkpointFile = buildCrawlLogCheckpointPath(logFileName, timestamp, position);
    FSDataOutputStream outputStream = _fs.create(checkpointFile);
    try { 
      outputStream.write(1);
      outputStream.flush();
    }
    finally { 
      outputStream.close();
    }
    
    HttpRequest request = factory.buildGetRequest(
        (isFinalCommit) ? buildCheckInURL(logFileName,position) : buildPingURL(logFileName, position));
    HttpResponse response = request.execute();
    
    LOG.info("Checkpointing log:"+ logFileName + " position:" + position);
    if (response.getStatusCode() == 200) { 
      LOG.info("Checkpointing for log:"+ logFileName + " position:" + position + " SUCCEEDED");
    }
    else { 
      LOG.error("Checkpoint for log:"
          + logFileName 
          + " position:" 
          + position 
          + " FAILED WITH ERROR: " 
          + response.getStatusCode() 
          + " " 
          + response.getStatusMessage());
    }
  }
  
  public static void main(String[] args) {
    Configuration conf = new Configuration();
    try {
      EC2ParserNode parser = new EC2ParserNode("test-host",conf);
      parser.addToQueue(new Path("/common-crawl/crawl-intermediate/CrawlLog_ccc01-01_1328300149459"));
      parser.stop();
    } catch (IOException e) {
      LOG.error(CCStringUtils.stringifyException(e));
    } catch (URISyntaxException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
