package org.commoncrawl.service.crawlhistoryV2;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.commoncrawl.async.CallbackWithResult;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.URLFPBloomFilter;

public class ShardThread implements Runnable {

  private static final Log LOG = LogFactory.getLog(ShardThread.class);
  
  private static final int FSYNC_INTERVAL = 100;
  private static final int ROLL_INTERVAL = 1000000;
  CrawlHistoryServer _server;
  int                _shardId;
  FileSystem         _fs;
  Configuration      _conf;
  LinkedBlockingDeque<Request> _requestQueue = new LinkedBlockingDeque<Request>();
  SequenceFile.Writer _logWriter;
  long                _logFileId;
  int                 _logEntries;
  Path                _tlogBasePath;
  URLFPBloomFilter    _filter;  
  final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  

  public static class Request { 
    enum RequestType { 
      ROLL_LOG,
      SINGLE_FP_UPDATE,
      MULTIPLE_FP_UPDATE,
      SHUTDOWN
    }
    
    RequestType _type;
    long        _requestTime;
    boolean     _complete;
    IOException _lastError;
    
    
    public URLFPV2     _singleRequestFP;
    public DataOutputBuffer _multiReqBuffer = null;
    CallbackWithResult<Request> _completionCallback;
    
    public Request(RequestType requestType,long requestTime,CallbackWithResult<Request> completionCallback) { 
      _type = requestType;
      _requestTime = requestTime;
      _completionCallback = completionCallback;
    }
  }
  
  
  public ShardThread(CrawlHistoryServer server,FileSystem fs,Configuration conf,Path tlogBasePath,int shardId,URLFPBloomFilter filter) throws IOException { 
    _server = server;
    _fs = fs;
    _conf = conf;
    _shardId = shardId;
    _filter = filter;
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
    _tlogBasePath = tlogBasePath;
  }
  
  @Override
  public void run() {
    LOG.info(getLogPrefix()+"Thread Started");
    outer:
    while (true) { 
      try { 
        Request request = _requestQueue.take();
        try { 
          switch (request._type) { 
            case ROLL_LOG: { 
              try { 
                LOG.info(getLogPrefix()+"GOT ROLL_LOG CMD");
                rollTransactionLog();
                request._complete = true;
                LOG.info(getLogPrefix()+"FINISHED ROLL_LOG CMD");
              }
              catch (IOException e) { 
                LOG.error(getLogPrefix()+"Failed to RollLog with Exception:" + CCStringUtils.stringifyException(e));
              }
            }
            break;
            
            case SINGLE_FP_UPDATE: { 
              try {
                _filter.add(request._singleRequestFP);
                appendLogFileRecord(request._singleRequestFP,request._requestTime);
                request._complete = true;
              }
              catch (IOException e) { 
                LOG.error(getLogPrefix()+ "SINGLE FP REQUEST FAILED with Exception:" + CCStringUtils.stringifyException(e));
                request._lastError = e;
              }
            }
            break;
            
            case MULTIPLE_FP_UPDATE: { 
              try { 
                if (request._multiReqBuffer != null) { 
                  DataInputBuffer inputBuffer = new DataInputBuffer();
                  inputBuffer.reset(
                      request._multiReqBuffer.getData(),
                      0,
                      request._multiReqBuffer.getLength());
                  
                  int items = inputBuffer.readInt();
                  
                  URLFPV2 fp = new URLFPV2();
                  
                  for (int i=0;i<items;++i) { 

                    
                  }
                }
              }
              catch (IOException e) { 
                
              }
            }
            break;
            
            case SHUTDOWN: { 
              try { 
                LOG.info(getLogPrefix()+"GOT SHUTDOWN ROLLING LOG");
                rollTransactionLog();
                LOG.info(getLogPrefix()+"GOT SHUTDOWN ROLLED LOG");
              }
              catch (IOException e){
                LOG.error(getLogPrefix()+"Failed to RollLog with Exception:" + CCStringUtils.stringifyException(e));
              }
              break outer;
            }
          }
        }
        finally { 
          request._completionCallback.execute(request);
        }
      }
      catch (Exception e) { 
        LOG.error(getLogPrefix() + "UnhandledException: " +CCStringUtils.stringifyException(e));
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e1) {
        }
      }
    }
    LOG.info(getLogPrefix()+"THREAD EXITING");
  }
  
  void appendLogFileRecords(DataInputBuffer stream,long timestamp) throws IOException {
    URLFPV2 fp = new URLFPV2();
    int recordCount = stream.readInt();
    
    { 
      for (int i=0;i<recordCount;++i) {
        
        fp.setRootDomainHash(stream.readLong());
        fp.setDomainHash(stream.readLong());
        fp.setUrlHash(stream.readLong());
        
        try {
          
          SequenceFile.Writer writer = ensureWriter();
          
          if (writer != null) { 
            writer.append(fp, timestamp);
            _logEntries++;
            if (_logEntries >= ROLL_INTERVAL) { 
              rollTransactionLog();
            }
            else if (_logEntries % FSYNC_INTERVAL == 0) { 
              try { 
                writer.syncFs();
              }
              catch (IOException e) { 
                LOG.error(getLogPrefix() + "Failed to FSYNC File:" + _logFileId  + " with Exception:"
                    + CCStringUtils.stringifyException(e));
                // force roll the log 
                rollTransactionLog();
              }
            }
          }
          else { 
            
          }
        }
        catch (IOException e) { 
          LOG.error(getLogPrefix()+"APPEND FAILURE " 
                + " RH: " + fp.getRootDomainHash() 
                + " DH:" + fp.getDomainHash()
                + " UH:" + fp.getUrlHash());
          LOG.error(getLogPrefix()+"APPEND Exception: " + CCStringUtils.stringifyException(e));
          
          rollTransactionLog();
          
        }
      }
    }
  }
  
  void appendLogFileRecord(URLFPV2 fp,long timestamp) throws IOException { 
    SequenceFile.Writer writer = ensureWriter();
    if (writer != null) { 
      try { 
        writer.append(fp, timestamp);
        _logEntries++;
        if (_logEntries >= ROLL_INTERVAL) { 
          rollTransactionLog();
        }
        else if (_logEntries % FSYNC_INTERVAL == 0) { 
          try { 
            writer.syncFs();
          }
          catch (IOException e) { 
            LOG.error(getLogPrefix() + "Failed to FSYNC File:" + _logFileId  + " with Exception:"
                + CCStringUtils.stringifyException(e));
            // force roll the log 
            rollTransactionLog();
            
            throw e;
          }
        }
      }
      catch (IOException e) { 
        LOG.error(getLogPrefix()+"APPEND FAILURE " 
              + " RH: " + fp.getRootDomainHash() 
              + " DH:" + fp.getDomainHash()
              + " UH:" + fp.getUrlHash());
        LOG.error(getLogPrefix()+"APPEND Exception: " + CCStringUtils.stringifyException(e));
        
        rollTransactionLog();
        
        throw e;
      }
    }
  }
  
  SequenceFile.Writer ensureWriter()throws IOException { 
    if (_logWriter == null) { 
      long fileId = System.currentTimeMillis();
      _logWriter = new SequenceFile.Writer(_fs,_conf,getTLogFilePathGivenId(fileId),URLFPV2.class,LongWritable.class);
      _logFileId = fileId;
      _logEntries = 0;
    }
    return _logWriter;
  }
  
  Path getTLogFilePathGivenId(long fileId) { 
    return new Path(_tlogBasePath,NUMBER_FORMAT.format(_shardId)+"-"+fileId);
  }
  
  void rollTransactionLog()throws IOException { 
    if (_logWriter != null) {
      try { 
        _logWriter.close();
      }
      catch (IOException e) { 
        LOG.error(getLogPrefix() + " Threw Exception during close:" + CCStringUtils.stringifyException(e));
      }
      _logWriter = null;
      _logFileId = -1;
      _logEntries = 0;
    }
  }
  
  String getLogPrefix() { 
    return "SHARD[" + NUMBER_FORMAT.format(_shardId)+"]";
  }
  
}
