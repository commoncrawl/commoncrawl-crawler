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

package org.commoncrawl.util;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.commoncrawl.async.Callback;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.async.Timer;
import org.commoncrawl.common.Environment;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;

import com.hadoop.compression.lzo.LzoCodec;


/** used to push stats to hdfs in a consistent manner 
 *  stats are written to a local temp file in form of a 
 *  hadoop sequence file, and are flushed to hdfs when 
 *  the writer is closed. Each stat entry consists of a 
 *  key and a value pair. The key has to implement WritableComparable,
 *  and value has to implement Writable
 * @author rana
 *
 */
public class MapReduceJobStatsWriter<KeyType extends WritableComparable,ValueType extends Writable > {

  private static final Log LOG = LogFactory.getLog(MapReduceJobStatsWriter.class);
  
  /** log family type **/
  private String _logFamily;
  /** grouping key **/
  private String _groupingKey;
  /** log file key **/
  private long   _uniqueKey;
  
  /** the temp file stats writer object **/ 
  private SequenceFile.Writer _writer = null;
  /** remote file system instance **/
  FileSystem _remoteFileSystem;
  /** temp file name **/
  private File _tempFileName;
  /** output stream sequence file is writing to **/
  private FSDataOutputStream _outputStream;
  /** hadoop config **/
  Configuration _config;
  /** last log write exception **/
  private IOException _lastLogWriteException;
  /** log file entry count **/
  private int _entryCount = 0;
 

  /** internal class used to queue up log file write requests **/
  private static class LogFileItem<KeyType extends WritableComparable,ValueType extends Writable> {
    
    LogFileItem(KeyType key,ValueType value) { 
      _key = key;
      _value = value;
    }
    
    LogFileItem() { 
      _key = null;
      _value = null;
    }
    
    public KeyType    _key;
    public ValueType  _value;
  }
  
  /** the log writer thread event loop **/
  EventLoop _eventLoop = new EventLoop();
  
  
  /** Constructor
   * 
   * @param keyClass      key type
   * @param valueClass    value type
   * @param familyKey         
   * @param groupingKey
   * @param uniqueKey
   */
  public MapReduceJobStatsWriter(FileSystem remoteFileSystem,
                  Configuration config,
                  Class<KeyType> keyClass,
                  Class<ValueType> valueClass,
                  String familyKey,
                  String groupingKey,
                  long uniqueKey) throws IOException {
    
    _logFamily = familyKey;
    _groupingKey = groupingKey;
    _uniqueKey  = uniqueKey;
    _remoteFileSystem = remoteFileSystem;
    _config = config;
    // temp file 
    _tempFileName = File.createTempFile("statsWriter", "seq");
    // create output stream that sequence file writer will output to
    _outputStream = FileSystem.getLocal(_config).create(new Path(_tempFileName.getAbsolutePath()));
    
    LzoCodec codec = new LzoCodec();
    // create sequencefile writer 
    _writer = SequenceFile.createWriter(config,_outputStream,keyClass,valueClass,CompressionType.BLOCK,codec);
    // start event loop
    _eventLoop.start();
  }
  
  
  /** append an item to the log file **/
  public void appendLogEntry(final KeyType key,final ValueType value) throws IOException {
    if (_lastLogWriteException == null) { 
      // send async message to the writer thread ... 
      _eventLoop.setTimer(new Timer(0,false,new Timer.Callback() {
  
        @Override
        public void timerFired(Timer timer) {
          // this executes in the writer thread's context ... 
          try {
            _writer.append(key, value);
            ++_entryCount;
          } catch (IOException e) {
            LOG.error("Failed to Write to Log File Entry for:" + 
                _logFamily +"/" + _groupingKey +"/" + Long.toString(_uniqueKey) + 
                "Exception:" + CCStringUtils.stringifyException(e));
            
            _lastLogWriteException = e;
          }
        } 
      }));
    }
    else { 
      IOException e = _lastLogWriteException;
      _lastLogWriteException = null;
      throw e;
    }
  }
  
  /** close and flush the log file **/
  public void close(final Callback optionalAsyncCallback){ 

    if (_eventLoop != null) { 
      // allocate a blocking semaphore in case async callback was not specified 
      final Semaphore blockingCallSemaphore = new Semaphore(0);
  
      // perform shutdown in worker thread ... 
      _eventLoop.setTimer(new Timer(0,false,new Timer.Callback() {
  
        @Override
        public void timerFired(Timer timer) {
  
          try { 
            try { 
              if (_writer != null) { 
                _writer.close();
              }
            }
            catch (IOException e){ 
              LOG.error(CCStringUtils.stringifyException(e));
              _lastLogWriteException = e;
            }
            finally {
              _writer = null;
              
              try { 
                if (_outputStream != null) { 
                  _outputStream.flush();
                  _outputStream.close();
                }
              }
              catch (IOException e){ 
                LOG.error(CCStringUtils.stringifyException(e));
                _lastLogWriteException = e;
              }
              finally {
                _outputStream = null;
              }
            }
            
            // now figure out if everything went smoothly or not 
            if (_entryCount != 0 && _lastLogWriteException == null) { 
              // ok so far so good... time to copy the local log file to hdfs ... 
              Path hdfsPath = new Path(Environment.HDFS_LOGCOLLECTOR_BASEDIR,_logFamily+"/"+_groupingKey+"/" + Long.toString(_uniqueKey));
  
              try { 
                
                // delete the remote file if it exists
                _remoteFileSystem.delete(hdfsPath,false);
                // ensure parent path 
                _remoteFileSystem.mkdirs(hdfsPath.getParent());
                // now if the local file exists and has data 
                if (_tempFileName.exists() && _tempFileName.length() != 0) { 
                  // copy the file to hdfs 
                  _remoteFileSystem.copyFromLocalFile(new Path(_tempFileName.getAbsolutePath()), hdfsPath);
                }
              }
              catch (IOException e) {
                LOG.error(CCStringUtils.stringifyException(e));
                _lastLogWriteException = e;
              }
            } 
          }
          finally { 
            // always delete the temp file ... 
            _tempFileName.delete();
            
            // release semaphore 
            blockingCallSemaphore.release();
            
            // if callback was specified , call it now 
            if (optionalAsyncCallback != null) { 
              optionalAsyncCallback.execute();
            }
            
            // stop the event loop ... 
            _eventLoop.stop();
            _eventLoop = null;
          }
        }
      }));
      
      // now if callback was not specified... wait for blocking semaphore to signal ... 
      if (optionalAsyncCallback == null) { 
        blockingCallSemaphore.acquireUninterruptibly();
      }
    }
  }
  
  
  public static void main(String[] args) {
    LOG.info("Initializing Hadoop Config");

    Configuration conf = new Configuration();
    
    conf.addResource("nutch-default.xml");
    conf.addResource("nutch-site.xml");
    conf.addResource("hadoop-default.xml");
    conf.addResource("hadoop-site.xml");
    conf.addResource("commoncrawl-default.xml");
    conf.addResource("commoncrawl-site.xml");
    
    CrawlEnvironment.setHadoopConfig(conf);
    CrawlEnvironment.setDefaultHadoopFSURI("hdfs://ccn01:9000/");
    
    // test the stats Writer ... 
    try {
      
      LOG.info("Opening Stats Writer");
      MapReduceJobStatsWriter<IntWritable,Text> statsWriter = new MapReduceJobStatsWriter<IntWritable,Text>(
          CrawlEnvironment.getDefaultFileSystem(),
          conf,
          IntWritable.class,Text.class,
          "test","group1",12345L);
      
      LOG.info("Writing Entries");
      for (int i=0;i<1000;++i){ 
        statsWriter.appendLogEntry(new IntWritable(i), new Text("Log Entry #" + i));
      }
      LOG.info("Flushing / Closing");
      final Semaphore blockingSempahore = new Semaphore(0);
      statsWriter.close(new Callback() {

        @Override
        public void execute() {
          LOG.info("Completion Callback Triggered");
          blockingSempahore.release();
        } 
        
      });
      LOG.info("Waiting on Semaphore");
      blockingSempahore.acquireUninterruptibly();
      LOG.info("Acquired Semaphore");
      
      LOG.info("Closed");
      
      Path hdfsPath = new Path(Environment.HDFS_LOGCOLLECTOR_BASEDIR,"test"+"/"+"group1"+"/" + Long.toString(12345L));

      LOG.info("Opening Reader");
      SequenceFile.Reader reader = new SequenceFile.Reader(CrawlEnvironment.getDefaultFileSystem(),hdfsPath,conf);
      IntWritable key = new IntWritable();
      Text value = new Text();
      while (reader.next(key, value)) { 
        LOG.info("Key:" + key.get() + " Value:" + value.toString());
      }
      reader.close();
      
    } catch (IOException e) {
      LOG.error(CCStringUtils.stringifyException(e));
    }
        
        
  }
}
