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

package org.commoncrawl.crawl.crawler.listcrawler;



import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.record.Buffer;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.protocol.CacheItem;
import org.commoncrawl.util.shared.CCStringUtils;

/** 
 * Thread that flushes crawled content to HDFS 
 * 
 * @author rana
 *
 */
final class HDFSFlusherThread implements Runnable {
  
  public static final Log LOG = LogFactory.getLog(HDFSFlusherThread.class);
  
  
  CacheManager _manager;
  
  public HDFSFlusherThread(CacheManager manager) { 
    _manager = manager;
  }
  
  
  private long generateSequenceFileAndIndex(int itemFlushLimit,RandomAccessFile sourceLogFile,long startPos,long endPos, byte[] syncBytes,SequenceFile.Writer writer,DataOutput indexStreamOut,ArrayList<FingerprintAndOffsetTuple> tupleListOut) throws IOException {
    
    byte [] syncCheck = new byte[syncBytes.length];
    
    // and create a list to hold fingerprint / offset information
    Vector<FingerprintAndOffsetTuple> fpOffsetList = new Vector<FingerprintAndOffsetTuple>();
    
    long currentPos   = startPos; 
    
    LOG.info("Flushing Entries Starting up to offset:" + endPos);
    CacheItemHeader itemHeader = new CacheItemHeader();
    
    int itemsProcessed = 0;
    
    boolean ignoreFlushLimit = false;
    
    // start read 
    while (currentPos < endPos) {

      if ((endPos - currentPos) < LocalLogFileHeader.SYNC_BYTES_SIZE) 
        break;
      
      // seek to current position ... 
      sourceLogFile.seek(currentPos);
      
      boolean headerLoadFailed = false;
      
      try { 
        // read the item header ... assuming things are good so far ... 
        itemHeader.readHeader(sourceLogFile);
      }
      catch (IOException e) { 
        CacheManager.LOG.error("### Item Header Load At Position:" + currentPos +" Failed With Exception:" + CCStringUtils.stringifyException(e));
        headerLoadFailed = true;
      }
      
      if (headerLoadFailed) {
        CacheManager.LOG.error("### Item File Corrupt at position:" + currentPos +" Seeking Next Sync Point");
        currentPos += LocalLogFileHeader.SYNC_BYTES_SIZE;
      }
      
      // if header sync bytes don't match .. then seek to next sync position ... 
      if (headerLoadFailed || !Arrays.equals(itemHeader._sync, syncBytes)) {

        CacheManager.LOG.error("### Item File Corrupt at position:" + currentPos +" Seeking Next Sync Point");
        
        // reseek to current pos 
        sourceLogFile.seek(currentPos);
        // read in a sync.length buffer amount 
        sourceLogFile.readFully(syncCheck);
      
        int syncLen = syncBytes.length;
      
        // start scan for next sync position ...
        for (int i = 0; sourceLogFile.getFilePointer() < endPos; i++) {
          int j = 0;
          for (; j < syncLen; j++) {
            if (syncBytes[j] != syncCheck[(i+j)%syncLen])
              break;
          }
          if (j == syncLen) {
            sourceLogFile.seek(sourceLogFile.getFilePointer() - LocalLogFileHeader.SYNC_BYTES_SIZE);     // position before sync
            break;
          }
          syncCheck[i%syncLen] = sourceLogFile.readByte();
        }
        // whatever, happened file pointer is at current pos 
        currentPos = sourceLogFile.getFilePointer();
        
        if (currentPos < endPos) { 
          CacheManager.LOG.info("### Item Loader Found another sync point at:" + currentPos);
        }
        else { 
          CacheManager.LOG.error("### No more sync points found!");
        }
      }
      else {
        CacheManager.LOG.info("WritingItem with FP:" + itemHeader._fingerprint + " Pos Is:" + writer.getLength());
        // track offset information for index building purposes   
        fpOffsetList.add(new FingerprintAndOffsetTuple(itemHeader._fingerprint,writer.getLength()));
        // read item data ...
        CacheItem cacheItem = new CacheItem();
        cacheItem.readFields(sourceLogFile);
        // now read content length 
        int contentLength = sourceLogFile.readInt();
        // and if content present... allocate buffer 
        if (contentLength != 0) { 
          // allocate content buffer 
          byte[] contentBuffer = new byte[contentLength];
          // read it from disk 
          sourceLogFile.readFully(contentBuffer);
          // and set content into cache item 
          cacheItem.setContent(new Buffer(contentBuffer));
        }
        CacheManager.LOG.info("Adding to Sequence File Item with URL:" + cacheItem.getUrl());
        // write to sequence file ... 
        writer.append(new Text(cacheItem.getUrl()),cacheItem);
        // now seek past data
        currentPos += CacheItemHeader.SIZE + itemHeader._dataLength + CacheManager.ITEM_RECORD_TRAILING_BYTES;
        // increment item count 
        itemsProcessed++;
        
      }
      
      
      if (!ignoreFlushLimit && itemsProcessed >= itemFlushLimit) {
      	// ok this gets tricky now ...
      	// figure out how many bytes of data were required to get to flush limit 
      	long approxCheckpointSize = currentPos - startPos;
      	// compute a  threshold number 
      	long bytesThreshold = (long) (approxCheckpointSize * .70);
      	// compute bytes remaining in checkpoint file ... 
      	long bytesRemaining = endPos - currentPos;
      	
      	// ok if bytes remaining are less than threshold number then go ahead and gobble
      	// everything up in a single pass (to prevent smaller subsequent index 
      	if (bytesRemaining <= bytesThreshold) { 
      		// ignore the flush limit and keep on rolling to the end ...  
      		ignoreFlushLimit = true;
      		LOG.warn("*****Bytes Remaining:" + bytesRemaining + " less than % of last whole chkpt size:" + approxCheckpointSize + ". Bypassing Flush Limit");
      	}
      	else { 
      		LOG.info("Reached Flush Item Limit:" + itemsProcessed + " Breaking Out");
      		break;
      	}
      	
      }
    }
    
    LOG.info("Writing Index");
    // ok now build the index file ... 
    HDFSFileIndex.writeIndex(fpOffsetList,indexStreamOut);
    LOG.info("Done Writing Index. Total Items Written:" + fpOffsetList.size());
    // copy offset list into tuple list
    tupleListOut.addAll(fpOffsetList);
    
    return currentPos;
  }
  
  static class IndexDataFileTriple { 
  	public Path _dataFilePath = null;
  	public Path _indexFilePath = null;
  	public File _localIndexFilePath = null;
  }
  
  @Override
  public void run() {
    
      
    boolean shutdown = false;
    
    while(!shutdown) { 
    	
      try {
        
        final CacheFlushRequest request = _manager.getHDFSFlushRequestQueue().take();
        
        switch (request._requestType) { 
        
          case ExitThreadRequest: { 
            // shutdown condition ... 
            CacheManager.LOG.info("Cache Flusher Thread Received Shutdown. Exiting!");
            shutdown = true;
          }
          break;
          
          case FlushRequest: {
            
            LOG.info("Received Flush Request");
            
            ArrayList<IndexDataFileTriple> tempFiles = new ArrayList<IndexDataFileTriple>();
            ArrayList<FingerprintAndOffsetTuple> tuplesOut = new ArrayList<FingerprintAndOffsetTuple>();
            
            // flag to track request status at end .. 
            boolean requestFailed = false;
            
            long logStart   = LocalLogFileHeader.SIZE; 
            long logEnd     = logStart + request._bytesToFlush;
            
            // create a hdfs temp file for data (and index)
            long generateTime = System.currentTimeMillis();
            Path tempDir = new Path(CrawlEnvironment.getHadoopConfig().get("mapred.temp.dir", ".") + "/flusher-temp-"+ generateTime);

            // mkdir ... 
            try {
	            _manager.getRemoteFileSystem().mkdirs(tempDir);
            } catch (IOException e1) {
            	LOG.error(CCStringUtils.stringifyException(e1));
            	requestFailed = true;
            }

            int iterationNumber = 0;
            
            while (logStart != logEnd && !requestFailed) { 

            	
            	
	            Path tempDataFile = new Path(tempDir,CacheManager.PROXY_CACHE_FILE_DATA_PREFIX + "-" + iterationNumber);
	            Path tempIndexFile = new Path(tempDir,CacheManager.PROXY_CACHE_FILE_INDEX_PREFIX + "-" + iterationNumber);
	            
            	LOG.info("FlushRequest Pass#:" + iterationNumber + " DataPath:"+tempDataFile + " IndexPath:" + tempIndexFile);

	            
	            SequenceFile.Writer writer = null;
	            FSDataOutputStream indexOutputStream = null;
	            RandomAccessFile localLogFile = null;
	            
	            try {
	
	              LOG.info("Pass#:" + iterationNumber + " Opening SequenceFile for Output");
	              // open a temporary hdfs streams ...
	              writer = SequenceFile.createWriter(_manager.getRemoteFileSystem(),CrawlEnvironment.getHadoopConfig(),tempDataFile,Text.class,CacheItem.class,CompressionType.NONE);
	              
	              // opening index output stream ... 
	              LOG.info("Pass#:" + iterationNumber + " Opening Index Output Stream");
	              indexOutputStream = _manager.getRemoteFileSystem().create(tempIndexFile);
	              
	              LOG.info("Pass#:" + iterationNumber + " Opening Local Log");
	              localLogFile = new RandomAccessFile(_manager.getActiveLogFilePath(),"rw");
	 
	              // transfer log entries and generate index
	              logStart = generateSequenceFileAndIndex(_manager.getCacheFlushThreshold(),localLogFile,logStart,logEnd,_manager.getLocalLogSyncBytes(),writer,indexOutputStream,tuplesOut);
	            }
	            catch (IOException e) { 
	              CacheManager.LOG.error(CCStringUtils.stringifyException(e));
	              requestFailed = true;
	            }
	            finally {
	              if (writer != null) { 
	                try {
	                  writer.close();
	                } catch (IOException e) {
	                  LOG.error(CCStringUtils.stringifyException(e));
	                }
	              }
	              if (indexOutputStream != null) { 
	                try {
	                  indexOutputStream.close();
	                } catch (IOException e) {
	                  LOG.error(CCStringUtils.stringifyException(e));
	                }
	              }
	              if (localLogFile != null) { 
	                try {
	                  localLogFile.close();
	                } catch (IOException e) {
	                  LOG.error(CCStringUtils.stringifyException(e));
	                }
	              }
	            }
	            
	            if (requestFailed) { 
	              try {
	              	LOG.info("Pass#:" + iterationNumber+ " Failed. Deleting temp files");
	                _manager.getRemoteFileSystem().delete(tempDataFile, false);
	                _manager.getRemoteFileSystem().delete(tempIndexFile, false);
	              }
	              catch (IOException e){ 
	                LOG.error("Delete Failed During Failure! Potenital Orphan Files! : " + CCStringUtils.stringifyException(e));
	              }
	              break;
	            }
	            else {
	            	LOG.info("Pass#:" + iterationNumber+ " Finished. Adding files to tuple list");
	            	// add temp file tuple
	            	IndexDataFileTriple indexDataPair = new IndexDataFileTriple();
	            	
	            	indexDataPair._dataFilePath = tempDataFile;
	            	indexDataPair._indexFilePath = tempIndexFile;
	            	
	            	tempFiles.add(indexDataPair);
	            }
	            iterationNumber++;
            }
            
            LOG.info("All Passes Complete. Finalizing Commit");

            // ok if request failed ... 
            if (!requestFailed) { 
            	
            	int itemIndex = 0;
            	for (IndexDataFileTriple indexDataPair : tempFiles) { 
            		// generate final paths ... 
                Path finalOutputDir = _manager.getRemoteDataDirectory();
                
                Path finalDataFilePath  = new Path(finalOutputDir,CacheManager.PROXY_CACHE_FILE_DATA_PREFIX+ "-" + (generateTime + itemIndex));
                Path finalIndexFilePath  = new Path(finalOutputDir,CacheManager.PROXY_CACHE_FILE_INDEX_PREFIX+ "-" + (generateTime + itemIndex));
                
                try { 
                	LOG.info("Pass#:" + itemIndex + " Renaming Temp Files");
                  LOG.info("Pass#:" + itemIndex + " Final Data File Name is:" + finalDataFilePath);
                  LOG.info("Pass#:" + itemIndex + " Final Index File Name is:" + finalIndexFilePath);

	                // rename files ... 
	                _manager.getRemoteFileSystem().rename(indexDataPair._dataFilePath, finalDataFilePath);
	                indexDataPair._dataFilePath = finalDataFilePath;
	                _manager.getRemoteFileSystem().rename(indexDataPair._indexFilePath, finalIndexFilePath);
	                indexDataPair._indexFilePath = finalIndexFilePath;
                }
                catch (IOException e) { 
                	LOG.info("Pass#:" + itemIndex + " Rename Failed");
                	LOG.error(CCStringUtils.stringifyException(e));
                	requestFailed = true;
                	break;
                }
                
                try { 
	                // copy to local ...
	                indexDataPair._localIndexFilePath = new File(_manager.getLocalDataDirectory(),finalIndexFilePath.getName());
	                
	                LOG.info("Pass#:" + itemIndex + " Copying Remote Index File at:" + finalIndexFilePath + " to Local Directory:" + indexDataPair._localIndexFilePath.getAbsolutePath());
	                _manager.getRemoteFileSystem().copyToLocalFile(finalIndexFilePath, new Path(indexDataPair._localIndexFilePath.getAbsolutePath()));
	                LOG.info("Pass#:" + itemIndex + " Done Copying Remote Index File to Local");
                }
                catch (IOException e) { 
                	LOG.info("Pass#:" + itemIndex + " Local File Copy Failed with Exception:" + CCStringUtils.stringifyException(e));
                  requestFailed = true;
                  indexDataPair._localIndexFilePath = null;
                  break;
                }
                // inrement item index 
                itemIndex++;
            	}
            	// ok callback to manager if request succeeded 
            	if (!requestFailed) {
            		try { 
            			LOG.info("Flush Complete. Calling hdfsFlushComplete");
            			_manager.hdfsCacheFlushRequestComplete(request,tuplesOut,tempFiles);
            			LOG.info("Flush Complete. hdfsFlushComplete succeeded");
            		}
            		catch (IOException e) { 
            			
            			LOG.error("hdfsFlushComplete returned Exception:" + CCStringUtils.stringifyException(e));
            			requestFailed = true;
            		}
            	}
            	
            }
            
            if (requestFailed) {
            	LOG.info("Cache Manager Log Flush Failed. Deleteing files");
            	try { 
            		// delete temp file directory recursively 
            		_manager.getRemoteFileSystem().delete(tempDir, true);
            	}
            	catch (IOException e) { 
            		LOG.error(CCStringUtils.stringifyException(e));
            	}
            	// iterate temp file list 
            	for (IndexDataFileTriple triple : tempFiles) { 
            		try { 
	            		LOG.info("Deleteing:" + triple._dataFilePath);
	            		_manager.getRemoteFileSystem().delete(triple._dataFilePath,false);
	            		LOG.info("Deleteing:" + triple._indexFilePath);
	            		_manager.getRemoteFileSystem().delete(triple._indexFilePath,false);
	            		if (triple._localIndexFilePath != null) { 
	            			LOG.info("Deleteing LOCAL:" + triple._localIndexFilePath);
	            			triple._localIndexFilePath.delete();
	            		}
            		}
            		catch (IOException e) { 
            			LOG.error(CCStringUtils.stringifyException(e));
            		}
            	}
            	// callback to manager with the bad news ... 
            	_manager.hdfsCacheFlushRequestFailed(request);
            }
          }
          break;
        }
      } catch (InterruptedException e) {
        LOG.error("Unexpected Exception in HDFSFlusher Thread:" + CCStringUtils.stringifyException(e));
      }
    }
  }
}