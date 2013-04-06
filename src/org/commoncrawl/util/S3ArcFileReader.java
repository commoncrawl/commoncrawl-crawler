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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;
import org.commoncrawl.io.NIOBufferList;
import org.commoncrawl.io.NIOHttpConnection;
import org.commoncrawl.protocol.shared.ArcFileItem;

/**
 * 
 * @author rana
 *
 */
public class S3ArcFileReader implements S3Downloader.Callback {

  private static final int MAX_BUFFERS_ENQUEUED_PER_THREAD = 10000;
  private static final int MAX_DECODER_THREADS = 2;
  
  private static final Log LOG = LogFactory.getLog(S3ArcFileReader.class);
      
  
  private static class QueuedArcFileItem { 
    
    String _source = null;
    ArcFileItem _item = null;
    
    public QueuedArcFileItem(String source,ArcFileItem item) {
      _source = source;
      _item = item;
    }
  }
  
  private LinkedBlockingQueue<QueuedArcFileItem> _items = new LinkedBlockingQueue<QueuedArcFileItem>();
  private QueuedArcFileItem _currentItem = null;
  
  // the single download instance ... 
  private S3Downloader _downloader;

  private static class QueuedBufferItem { 
    
    public QueuedBufferItem(String key,ByteBuffer buffer) { 
      _key = key;
      _buffer = buffer;
    }
    
    public String _key;
    public ByteBuffer _buffer;
  }
  // the blocking input buffer queue 
  private LinkedBlockingQueue<QueuedBufferItem> _bufferQueues[] = new LinkedBlockingQueue[MAX_DECODER_THREADS];
  
  // the map from arcfile key to ArcFileState
  private Map<String,StreamingArcFileReader> _decodeStateMaps[] = new Map[MAX_DECODER_THREADS];
  
  //decoder thread ... 
  Thread _decoderThreads[] = new Thread[MAX_DECODER_THREADS];
  
  // active stream count 
  int _activeStreamCounts[] = new int[MAX_DECODER_THREADS];
  
  // item list 
  String _keys[] = null;
  
  public S3ArcFileReader(String bucketName,String s3AccessId, String s3SecretKey,String[] arcFileNames, int maxParallelStreams)throws IOException { 
    _downloader = new S3Downloader(bucketName,s3AccessId,s3SecretKey,false);
    _downloader.setMaxParallelStreams(maxParallelStreams);
    _keys = arcFileNames;
    
    for (int i=0;i<MAX_DECODER_THREADS;++i) { 
      _bufferQueues[i] = new LinkedBlockingQueue<QueuedBufferItem>(MAX_BUFFERS_ENQUEUED_PER_THREAD);
      _decodeStateMaps[i] = new HashMap<String,StreamingArcFileReader>();
      _activeStreamCounts[i] = 0;
    }
  }
  
  public void start() throws IOException { 
    
    //LOG.info("ArcFileReader->start");

    //LOG.info("ArcFileReader-> Download.start");
    _downloader.initialize(this);
    int keyCount =0;
    for (String key : _keys) { 
      try {
        //LOG.info("Fetching Key:" + key);
        
         
        // start fetch ...
        int itemId = _downloader.fetchItem(key);
        // set key to decoder affinity ...
        int threadIdx = itemId % MAX_DECODER_THREADS;
        // increment per thread stream count ... 
        _activeStreamCounts[threadIdx]++;
        // increment key count 
        keyCount++;
        
      } catch (IOException e) {
        LOG.error("Failed to Queue Item:" + key +" for Fetch");
      }
    }
    
    // start decoders ...
    for (int threadIdx=0;threadIdx<MAX_DECODER_THREADS;++threadIdx) {
      _decoderThreads[threadIdx] = startDecoderThread(threadIdx,_bufferQueues[threadIdx]);
    }
  }
  
  private Thread startDecoderThread(final int threadIdx,final LinkedBlockingQueue<QueuedBufferItem> attachedQueue) { 
    
    Thread decoderThread = new Thread( new Runnable() {

      public void run() {
        //LOG.info("Decoder Thread Start");
        while(true) { 
          
          try {
            QueuedBufferItem bufferItem = attachedQueue.take();
            
            
            if (bufferItem._key == null) {
              //this is our signal to shutdown and exit
              //LOG.info("Decoder Thread received shutdown signal");
              _items.add(new QueuedArcFileItem(null,null));
              return;
            }
            
            // LOG.info("Decoder Thread Got Buffer Item for Key:" + bufferItem._key);
            StreamingArcFileReader decoder = null;
            
            synchronized (_decodeStateMaps[threadIdx]) { 
              decoder = _decodeStateMaps[threadIdx].get(bufferItem._key);
            }
            if (decoder == null) { 
              LOG.error("No Decode State found for Key:" + bufferItem._key);
            }
            else {
              if (bufferItem._buffer != null) { 
                decoder.available(bufferItem._buffer);
              }
              else { 
                //LOG.info("Calling Decoder Finished For Key:" + bufferItem._key);
                decoder.finished();
              }
              
              try { 
              
                int itemsProduced = 0;
                
                StreamingArcFileReader.TriStateResult result = decoder.hasMoreItems();
                
                while (result == StreamingArcFileReader.TriStateResult.MoreItems) { 
                  ArcFileItem item = decoder.getNextItem();
                  if (item != null) { 
                    itemsProduced++;
                    //LOG.info("Decoder Got Item:" + item.getUri() + " For Key:" + bufferItem._key);
                    _items.offer(new QueuedArcFileItem(bufferItem._key,item));                    
                  }
                  else { 
                    break;
                  }
                }
                
                // LOG.info("Stream:" + bufferItem._key + " Bytes:" + bufferBytes + " Items:" + itemsProduced);
                
                if (result == StreamingArcFileReader.TriStateResult.NoMoreItems) {
                  decoder.resetState();
                  synchronized (_decodeStateMaps[threadIdx]) {
                    _decodeStateMaps[threadIdx].remove(bufferItem._key);                    
                  }
                }
              }
              catch (IOException e) {
                decoder.resetState();
                synchronized (_decodeStateMaps[threadIdx]) {
                  _decodeStateMaps[threadIdx].remove(bufferItem._key);                    
                }
                LOG.error(StringUtils.stringifyException(e));
              }
            }
          } catch (InterruptedException e) {
          }
        }
      } 
      
    });
    
    decoderThread.start();
    
    return decoderThread;
  }
  
  public void stop() throws IOException { 
    _downloader.shutdown();  
    for (int threadIdx=0;threadIdx < MAX_DECODER_THREADS;++threadIdx) { 
      _bufferQueues[threadIdx].clear();
      if (_decoderThreads[threadIdx]!= null) { 
        _bufferQueues[threadIdx].add(new QueuedBufferItem(null,null));
        try {
          _decoderThreads[threadIdx].join();
        } catch (InterruptedException e) {
        }
        _decoderThreads[threadIdx] = null;
      }
      _activeStreamCounts[threadIdx] = 0;
    }
  }
  
  public boolean hasMoreItems() throws IOException { 

    try {
      _currentItem = _items.take();
    } catch (InterruptedException e) {
    }
    return _currentItem._item != null;
  }
  
  public ArcFileItem getNextItem()throws IOException { 
    return _currentItem._item;
  }
  
  
  // @Override
  public boolean downloadStarting(int itemId,String itemKey,long contentLength) { 
    // set key to decoder affinity ...
    int threadIdx = itemId % MAX_DECODER_THREADS;
    
    synchronized(_decodeStateMaps[threadIdx]) { 
      _decodeStateMaps[threadIdx].put(itemKey, new StreamingArcFileReader(true));
    }
    return true;
  }
  
  // @Override
  public boolean contentAvailable(NIOHttpConnection connection,int itemId,String itemKey,NIOBufferList contentBuffer) {
    // set key to decoder affinity ...
    int threadIdx = itemId % MAX_DECODER_THREADS;

    boolean continueDownload = true;
    while (continueDownload && contentBuffer.available() != 0) { 
      try {
        ByteBuffer buffer = contentBuffer.read();
        try {
          _bufferQueues[threadIdx].put(new QueuedBufferItem(itemKey,buffer));
          // LOG.info("BQueue[" + threadIdx + "] Count:" +_bufferQueues[threadIdx].size() + " Stream:" + itemKey);
        } catch (InterruptedException e) {
        }
      } catch (IOException e) {
        LOG.error(StringUtils.stringifyException(e));
        continueDownload = false;
      }
    }
    return continueDownload;
  }
  
  //@Override
  public void downloadFailed(int itemId,String itemKey,String errorCode) { 
    // set key to decoder affinity ...
    int threadIdx = itemId % MAX_DECODER_THREADS;

    _activeStreamCounts[threadIdx]--;
    LOG.error("Download Failed for Item:" + itemKey + " ReasonCode:" + errorCode);
    // add item termination packet 
    try {
      _bufferQueues[threadIdx].put(new QueuedBufferItem(itemKey,null));
    } catch (InterruptedException e) {
    }
    // add thread termination packet 
    if (_activeStreamCounts[threadIdx] == 0) { 
      try {
        _bufferQueues[threadIdx].put(new QueuedBufferItem(null,null));
      } catch (InterruptedException e) {
      }
    }
  }
  
  //@Override
  public void downloadComplete(int itemId,String itemKey) {
    // set key to decoder affinity ...
    int threadIdx = itemId % MAX_DECODER_THREADS;

    _activeStreamCounts[threadIdx]--;
    // LOG.info("Download Complete for Item:" + itemKey);
    // add item termination packet 
    try {
      _bufferQueues[threadIdx].put(new QueuedBufferItem(itemKey,null));
    } catch (InterruptedException e) {
    }
    // add thread termination packet 
    if (_activeStreamCounts[threadIdx] == 0) { 
      try {
        _bufferQueues[threadIdx].put(new QueuedBufferItem(null,null));
      } catch (InterruptedException e) {
      }
    }
  }
}
