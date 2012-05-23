package org.commoncrawl.service.statscollector;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.util.TimeSeriesDataFile;

public class StatsLogManager {
  
  
  private static class DiskIORequest { 
    
    DiskIORequest(Runnable runnable) { 
      _runnable = runnable;
    }
    
    public Runnable _runnable;
  }
  
  private EventLoop _eventLoop;
  private File      _workingDirectory;
  private Map<String,TimeSeriesDataFile<BytesWritable>> _nameToFileMap = new TreeMap<String,TimeSeriesDataFile<BytesWritable>>();
  private Thread    _diskThread = null;
  private LinkedBlockingQueue<DiskIORequest> _requestQueue = new LinkedBlockingQueue<DiskIORequest>();
  private static final Log LOG = LogFactory.getLog(StatsLogManager.class);

  
  /**
   * Constructor 
   * 
   * @param serverEventLoop
   * @param workingDirectory
   */
  public StatsLogManager(EventLoop serverEventLoop,File workingDirectory) throws IOException { 
    _eventLoop = serverEventLoop;
    _workingDirectory = workingDirectory;
    _diskThread = new Thread(new Runnable() {

      @Override
      public void run() {
        
        while (true) { 
          try {
            DiskIORequest request = _requestQueue.take();
            if (request._runnable != null) { 
              request._runnable.run();
            }
            else { 
              break;
            }
          } catch (InterruptedException e) {
            
          }
        }
      } 
    });
    _diskThread.start();
  }
  
  /**
   * initiate a proper shtudown 
   */
  public void shutdown() { 
    if (_diskThread != null) { 
      try {
        _requestQueue.put(new DiskIORequest(null));
        _diskThread.join();
        _diskThread = null;
      } catch (InterruptedException e) {
      }
    }
  }
  
  
  /** queue a disk io request 
   * 
   */
  public void queueDiskIORequest(Runnable runnable) { 
    try {
      _requestQueue.put(new DiskIORequest(runnable));
    } catch (InterruptedException e) {
    }
  }
 
  
  
  /**
   * 
   * @param groupKey
   * @param uniqueKey
   * @param fileType
   * @return TimeSeriesDataFile object associated with the given composite key 
   */
  public synchronized TimeSeriesDataFile<BytesWritable> getFileGivenName(String groupKey,String uniqueKey,String fileType) { 
    File filePath = makePath(groupKey,uniqueKey,fileType);
    TimeSeriesDataFile<BytesWritable> fileOut = _nameToFileMap.get(filePath.getName());
    if (fileOut == null) { 
      fileOut = new TimeSeriesDataFile<BytesWritable>(filePath,BytesWritable.class);
      _nameToFileMap.put(filePath.getName(),fileOut);
    }
    return fileOut;
  }
  
  /**
   * Make a unique collection name given a group key and a unique collection key
   * 
   * @param groupKey  the group key
   * @param uniqueKey the unique key 
   * @return
   */
  public static String makeCollectionName(String groupKey,String uniqueKey) { 
    return groupKey+"-"+uniqueKey;
  }
  
  public static String getUniqueKeyGivenName(String collectionName) { 
    int indexOfDash = collectionName.indexOf('-');
    return collectionName.substring(indexOfDash + 1);
  }
  
  public EventLoop getEventLoop() { 
    return _eventLoop;
  }
  
  private File makePath(String groupKey,String uniqueKey,String fileType) { 
    String compositeName = makeCollectionName(groupKey,uniqueKey) +"." + fileType;
    return new File(_workingDirectory,compositeName);
  }
  
}
