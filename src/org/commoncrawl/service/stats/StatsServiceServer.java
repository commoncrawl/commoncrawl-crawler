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

package org.commoncrawl.service.stats;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.record.Buffer;
import org.commoncrawl.async.ConcurrentTask;
import org.commoncrawl.async.ConcurrentTask.CompletionCallback;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.rpc.base.internal.AsyncClientChannel;
import org.commoncrawl.rpc.base.internal.AsyncContext;
import org.commoncrawl.rpc.base.internal.AsyncServerChannel;
import org.commoncrawl.rpc.base.internal.AsyncRequest.Status;
import org.commoncrawl.rpc.base.shared.RPCException;
import org.commoncrawl.server.CommonCrawlServer;
import org.commoncrawl.service.stats.ReadStatsRecordResponse;
import org.commoncrawl.service.stats.ReadStatsRecordsRequest;
import org.commoncrawl.service.stats.StatsRecord;
import org.commoncrawl.service.stats.StatsService;
import org.commoncrawl.service.stats.WriteStatsRecordRequest;
import org.commoncrawl.service.stats.WriteStatsRecordResponse;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.TimeSeriesDataFile;
import org.commoncrawl.util.TimeSeriesDataFile.KeyValueTuple;

/**
 * 
 * @author rana
 *
 */
public class StatsServiceServer 
  extends CommonCrawlServer 
  implements StatsService,
             AsyncServerChannel.ConnectionCallback {

  
  
  private FileSystem _fileSystem = null;
  private File       _localDataDir = null;
  private static final String HDFS_ROOT = CrawlEnvironment.HDFS_CrawlDBBaseDir + "/" + CrawlEnvironment.STATS_SERVICE_HDFS_ROOT;
  private static final String LOCAL_DATA_ROOT = "stats_root";
  private static final Log LOG = LogFactory.getLog(StatsServiceServer.class);
  private Map<String,TimeSeriesDataFile<BytesWritable>> _groupToFileMap = new TreeMap<String,TimeSeriesDataFile<BytesWritable>>();
  
  
  public FileSystem getFileSystem() { 
    return _fileSystem;
  }
  
  private File getLocalRoot() {
    return _localDataDir;
  }
  
  @Override
  protected String getDefaultHttpInterface() {
    return CrawlEnvironment.DEFAULT_HTTP_INTERFACE;
  }

  @Override
  protected int getDefaultHttpPort() {
    return CrawlEnvironment.STATS_SERVICE_HTTP_PORT;
  }

  @Override
  protected String getDefaultLogFileName() {
    return "stats_service.log";
  }

  @Override
  protected String getDefaultRPCInterface() {
    return CrawlEnvironment.DEFAULT_RPC_INTERFACE;
  }

  @Override
  protected int getDefaultRPCPort() {
    return CrawlEnvironment.STATS_SERVICE_RPC_PORT;
  }

  @Override
  protected String getWebAppName() {
    return CrawlEnvironment.STATS_SERVICE_WEBAPP_NAME;
  }

  @Override
  protected boolean initServer() {
    
    try { 
      _fileSystem = CrawlEnvironment.getDefaultFileSystem();
      _localDataDir = new File(getDataDirectory(),LOCAL_DATA_ROOT);
      _localDataDir.mkdirs();
 
      
      // create server channel ... 
      AsyncServerChannel channel = new AsyncServerChannel(this, this.getEventLoop(), this.getServerAddress(),this);
      
      // register RPC services it supports ... 
      registerService(channel,StatsService.spec);
      
      return true;
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
    }
    return false;
  }

  @Override
  protected boolean parseArguments(String[] argv) {
    return true;
  }
  
  @Override
  protected void overrideConfig(Configuration conf) { 
    
  }
  
  @Override
  protected void printUsage() {
   
    
  }

  @Override
  protected boolean startDaemons() {
    return true;
  }

  @Override
  protected void stopDaemons() {
    
  }



  @Override
  protected String getDefaultDataDir() {
    // TODO Auto-generated method stub
    return null;
  }

  private File makeGroupFilePath(String groupName) { 
    return new File(getLocalRoot(),groupName);
  }
  
  private TimeSeriesDataFile<BytesWritable> getFileGivenGroupName(String groupName) { 
    TimeSeriesDataFile<BytesWritable> file = _groupToFileMap.get(groupName);
    if (file == null) { 
      file = new TimeSeriesDataFile<BytesWritable>(makeGroupFilePath(groupName),BytesWritable.class);
      _groupToFileMap.put(groupName,file);
    }
    return file;
  }
  
  @Override
  public void readStatsRecord(final AsyncContext<ReadStatsRecordsRequest, ReadStatsRecordResponse> rpcContext)throws RPCException {
    
    LOG.info("Received ReadRequest from:" + rpcContext.getClientChannel().toString() +" Group:" + rpcContext.getInput().getRecordGroup());
      
      final TimeSeriesDataFile<BytesWritable> file = getFileGivenGroupName(rpcContext.getInput().getRecordGroup());
      
      getDefaultThreadPool().execute(new ConcurrentTask<ArrayList< KeyValueTuple<Long,BytesWritable> >>(getEventLoop(), 
          
          new Callable<ArrayList< KeyValueTuple<Long,BytesWritable> >>() {

            @Override
            public ArrayList< KeyValueTuple<Long,BytesWritable> > call() throws Exception {
      
              if (!rpcContext.getInput().isFieldDirty(ReadStatsRecordsRequest.Field_IFLASTKEYNOT) || file.getLastRecordKey() != rpcContext.getInput().getIfLastKeyNot()) {
                return file.readFromTail(rpcContext.getInput().getRecordCount(),
                    rpcContext.getInput().isFieldDirty(ReadStatsRecordsRequest.Field_STOPIFKEYLESSTHAN) ? 
                        rpcContext.getInput().getStopIfKeyLessThan() : -1);
              }
              else { 
                return new ArrayList< KeyValueTuple<Long,BytesWritable>>(); 
              }
            }
        }
        ,
        new CompletionCallback<ArrayList< KeyValueTuple<Long,BytesWritable> >>() {

        @Override
        public void taskComplete(ArrayList<KeyValueTuple<Long, BytesWritable>> loadResult) {
          // ok fetch the require number of records ... 
          for (KeyValueTuple<Long,BytesWritable> item : loadResult) { 
            StatsRecord recordOut = new StatsRecord();

            recordOut.setTimestamp(item.key);
            recordOut.setRecordPosition(item.recordPos);
            recordOut.setData(new Buffer(item.value.getBytes()));
            
            rpcContext.getOutput().getRecordsOut().add(recordOut);
          }
          LOG.info("Returning " + rpcContext.getOutput().getRecordsOut().size() +" Records to:" + rpcContext.getClientChannel().toString());
          try {
            rpcContext.completeRequest();
          } catch (RPCException e1) {
            LOG.error(CCStringUtils.stringifyException(e1));
          }          
        }

        @Override
        public void taskFailed(Exception e) {
          LOG.error(CCStringUtils.stringifyException(e));
          rpcContext.setStatus(Status.Error_RequestFailed);
          rpcContext.setErrorDesc(CCStringUtils.stringifyException(e));
          try {
            rpcContext.completeRequest();
          } catch (RPCException e1) {
            LOG.error(CCStringUtils.stringifyException(e1));
          }          
        }
        
      }
    ));
      

  }

  @Override
  public void writeStatsRecord(final AsyncContext<WriteStatsRecordRequest, WriteStatsRecordResponse> rpcContext)throws RPCException {
    LOG.info("Received WriteRequest from:" + rpcContext.getClientChannel().toString() +" Group:" + rpcContext.getInput().getRecordGroup());

      
    final TimeSeriesDataFile<BytesWritable> file = getFileGivenGroupName(rpcContext.getInput().getRecordGroup());
    
    getDefaultThreadPool().execute(new ConcurrentTask<Long>(getEventLoop(), 
        
        new Callable<Long>() {

          @Override
          public Long call() throws Exception {
            
            BytesWritable data = new BytesWritable(rpcContext.getInput().getRecord().getData().getReadOnlyBytes());
            data.setSize(rpcContext.getInput().getRecord().getData().getCount());
            long recordPositionOut = file.appendRecordToLogFile(rpcContext.getInput().getRecord().getTimestamp(),data);
            LOG.info("Wrote Record with Timestamp:" + rpcContext.getInput().getRecord().getTimestamp() + " to Group:" + rpcContext.getInput().getRecordGroup());
            
            return recordPositionOut;
          }
        },
    
        new CompletionCallback<Long>() {

          @Override
          public void taskComplete(Long loadResult) {
            try {
              rpcContext.getOutput().setRecordPositon(loadResult);
              rpcContext.completeRequest();
            } catch (RPCException e) {
              LOG.error(CCStringUtils.stringifyException(e));
            }
          }

          @Override
          public void taskFailed(Exception e) {
            LOG.error(CCStringUtils.stringifyException(e));
            rpcContext.setStatus(Status.Error_RequestFailed);
            rpcContext.setErrorDesc(CCStringUtils.stringifyException(e));
            try {
              rpcContext.completeRequest();
            } catch (RPCException e1) {
              LOG.error(CCStringUtils.stringifyException(e1));
            }
          }
      
        }));
  }

  @Override
  public void IncomingClientConnected(AsyncClientChannel channel) {
    
  }

  @Override
  public void IncomingClientDisconnected(AsyncClientChannel channel) {
    
  }
}
