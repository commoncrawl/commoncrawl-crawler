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
 */

package org.commoncrawl.service.crawler;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.WritableName;
import org.apache.hadoop.io.WritableUtils;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.protocol.CrawlSegmentDetail;
import org.commoncrawl.protocol.CrawlSegmentDetailFPInfo;
import org.commoncrawl.protocol.CrawlSegmentHost;
import org.commoncrawl.protocol.CrawlSegmentHostFPInfo;
import org.commoncrawl.protocol.CrawlSegmentURL;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.service.crawler.CrawlSegmentLog.CrawlSegmentFPMap;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.TextBytes;

/**
 * 
 * @author rana
 *
 */
public class SegmentLoader {
  
  private static final Log LOG = LogFactory.getLog(SegmentLoader.class);

  public static class DNSResult { 
    public int ipAddress;
    public long ttl;
    public String cname;
  }
  public static interface DNSCache {
    public DNSResult resolveName(CrawlSegmentHost host);
  }
  
  public static interface LoadProgressCallback { 
    public boolean hostAvailable(final CrawlSegmentHost host,final int originalURLCount,final int completedURLCount);
  }
  
  public static interface CancelOperationCallback { 
    // return true to cancel the operation
    public boolean cancelOperation(); 
  }
  
  @SuppressWarnings("unchecked")
  public static CrawlSegmentFPMap  loadCrawlSegmentFPInfo(int listId,int segmentId,String crawlerName,CancelOperationCallback cancelCallback) throws IOException { 

    CrawlSegmentFPMap fpMap = new CrawlSegmentFPMap();
    
    WritableName.setName(CrawlSegmentHost.class, "org.crawlcommons.protocol.CrawlSegmentHost");
    
    // construct hdfs path to segment ... 
    Path hdfsPath;
    if (segmentId != -1)
      hdfsPath = new Path(CrawlEnvironment.getCrawlSegmentDataDirectory() + "/" + listId + "/" + segmentId + "/");
    else 
      hdfsPath = new Path(CrawlEnvironment.getCrawlSegmentDataDirectory() + "/");
    
    Path workUnitDetailPath = new Path(hdfsPath,crawlerName);
    
    SequenceFile.Reader reader = null;
    
    try { 
      FileSystem hdfs = CrawlEnvironment.getDefaultFileSystem();
      reader          = new SequenceFile.Reader(hdfs,workUnitDetailPath,CrawlEnvironment.getHadoopConfig());
      
      LongWritable      hostFP = new LongWritable();
      CrawlSegmentHost  segmentHost = new CrawlSegmentHost();  
      
      DataOutputBuffer outputBuffer = new DataOutputBuffer();
      
      int segmentUrlCount = 0;
      while (reader.next(hostFP, segmentHost) && cancelCallback.cancelOperation() == false) { 
        // and update url count ... 
        segmentUrlCount += segmentHost.getUrlTargets().size();
        
        // set the url vector to the appropriate size ... 
        for (CrawlSegmentURL url : segmentHost.getUrlTargets()) {

          WritableUtils.writeVLong(outputBuffer,segmentHost.getHostFP());
          WritableUtils.writeVLong(outputBuffer,url.getUrlFP());
        }
      }
      outputBuffer.flush();
      // ok set the urlfp stream 
      fpMap.setURLFPBuffer(segmentUrlCount,outputBuffer.getData(),outputBuffer.getLength());
      // now initialize the 
      
      if (cancelCallback.cancelOperation()) { 
        return null;
      }
      else { 
        return fpMap;
      }
    }
    finally { 
      if (reader != null)
        reader.close();
    }
  }
  
  public static class CrawlSegmentDetialLoadHintItem { 
      public static final int Is_Complete = 1 << 0;  // completely intact 
      public static final int Is_Partial    = 1 << 1; // partial item ... 
      public static final int Is_Empty    = 1 << 2; // completely exhausted ...
      
      public int      _flags = 0;
      public CrawlSegmentHostFPInfo _hostInfo = null;
  }
  
  public static class CrawlSegmentDetailLoadHint { 
    
    public Map<Long,CrawlSegmentDetialLoadHintItem> _hostItems = new TreeMap<Long,CrawlSegmentDetialLoadHintItem>();
    
    public static CrawlSegmentDetailLoadHint buildLoadHintFromDetailFPInfo(CrawlSegmentDetailFPInfo info) { 
      
      CrawlSegmentDetailLoadHint hintOut = new CrawlSegmentDetailLoadHint();
      
      for (CrawlSegmentHostFPInfo host : info.getHosts()) {
        
        CrawlSegmentDetialLoadHintItem hintItem = new CrawlSegmentDetialLoadHintItem();
                
        if (host.getUrlTargets().size() == 0) { 
          hintItem._flags = CrawlSegmentDetialLoadHintItem.Is_Empty;
        }
        else if (host.getUrlTargets().size() == host.getOriginalTargetCount()) { 
          hintItem._flags = CrawlSegmentDetialLoadHintItem.Is_Complete;
        }
        else { 
          hintItem._flags = CrawlSegmentDetialLoadHintItem.Is_Partial;
          hintItem._hostInfo = host;
        }
        
        hintOut._hostItems.put(host.getHostFP(),hintItem);
      }
      
      return hintOut;
    }
  }
  
  
  @SuppressWarnings("unchecked")
  public static CrawlSegmentDetail loadCrawlSegment(int listId,int segmentId,String crawlerName,CrawlSegmentFPMap loadHint,DNSCache cache,LoadProgressCallback callback,CancelOperationCallback incomingCancelCallback) throws IOException { 
   
    final CancelOperationCallback cancelCallback = (incomingCancelCallback != null) ? 
          incomingCancelCallback : new  CancelOperationCallback() {

            @Override
            public boolean cancelOperation() {
              return false;
            } 
          };
      
    
    WritableName.setName(CrawlSegmentHost.class, "org.crawlcommons.protocol.CrawlSegmentHost");
    
    // construct hdfs path to segment ... 
    Path hdfsPath;
    if (segmentId != -1)
      hdfsPath = new Path(CrawlEnvironment.getCrawlSegmentDataDirectory() + "/" + listId + "/" + segmentId + "/");
    else 
      hdfsPath = new Path(CrawlEnvironment.getCrawlSegmentDataDirectory() + "/" );
      
    Path workUnitDetailPath = new Path(hdfsPath,crawlerName);
    SequenceFile.Reader reader = null;
    try { 
    
      CrawlSegmentDetail    segmentOut  = new CrawlSegmentDetail();
      
      // initialize work unit detail ...
      segmentOut.setSegmentId(segmentId);
      
      FileSystem hdfs = CrawlEnvironment.getDefaultFileSystem();
      reader             = new SequenceFile.Reader(hdfs,workUnitDetailPath,CrawlEnvironment.getHadoopConfig());
      
      LongWritable          hostFP = new LongWritable();
      CrawlSegmentHost  segmentHost = new CrawlSegmentHost();  
      
      while (reader.next(hostFP, segmentHost) && !cancelCallback.cancelOperation()) {
        
        if (segmentHost.getHostFP() == 0) { 
          LOG.error("Host FP is Zero during reader.next");
        }
        
        //setup the segment id associated with this host (so that the host contains self sufficient context information).
        segmentHost.setSegmentId(segmentId);
        segmentHost.setListId(listId);

        // capture original item count 
        int originalURLCount = segmentHost.getUrlTargets().size();
        int completedURLCount = 0;
        
        // and update url count ... 
        segmentOut.setUrlCount(segmentOut.getUrlCount() + segmentHost.getUrlTargets().size());

        
        if (loadHint != null) { 
          // now walk remaining items (in hint) 
          for (int i=0;i<segmentHost.getUrlTargets().size();++i) { 

            CrawlSegmentURL segmentURL = segmentHost.getUrlTargets().get(i);
            
            URLFPV2 urlfp = new URLFPV2();
            
            urlfp.setDomainHash(segmentHost.getHostFP());
            urlfp.setUrlHash(segmentURL.getUrlFP());
            
            if (loadHint.wasCrawled(urlfp)) { 
              completedURLCount++;
              segmentHost.getUrlTargets().remove(i);
              --i;
              segmentOut.setUrlsComplete(segmentOut.getUrlsComplete() + 1);
            }
          }
        }
        // now ... if there are no more entries in the host ...  
        if (segmentHost.getUrlTargets().size() != 0) { 
          
          if (!segmentHost.isFieldDirty(CrawlSegmentHost.Field_IPADDRESS)) { 
            if (cache != null) { 
              // try to resolve the address up front 
              DNSResult dnsCacheResult = cache.resolveName(segmentHost);
              
              if (dnsCacheResult != null) { 
                segmentHost.setIpAddress(dnsCacheResult.ipAddress);
                segmentHost.setTtl(dnsCacheResult.ttl);
                if (dnsCacheResult.cname != null && dnsCacheResult.cname.length() != 0) { 
                  segmentHost.setCname(dnsCacheResult.cname);
                }
              }
            }
          }
          else { 
            if (!segmentHost.isFieldDirty(CrawlSegmentHost.Field_TTL)) { 
              segmentHost.setTtl(0);
            }
          }
        }
        
        // if a progress callback was specified, then call it with the load progress of this host ... 
        if (callback != null) { 
          // and initiate completion callaback
          boolean continueLoading = callback.hostAvailable(segmentHost,originalURLCount,completedURLCount);
          
          if (!continueLoading) {
            LOG.info("HostAvailable Callback returned false. Aborting Load");
             return null;
          }
          
        }
        // otherwise ... add the host to the segment detail ... 
        else { 
          segmentOut.getHosts().add(segmentHost);
        }
        
        // and allocate a new segment host for next read 
        segmentHost = new CrawlSegmentHost();
      }

      if (!cancelCallback.cancelOperation()) { 
        return segmentOut;
      }
      else { 
        return null;
      }
    }
    finally { 
      if (reader != null)
        reader.close();
    }
  }
  
  public static void main(String[] args) {
    
    if (args.length != 0) { 
      System.out.println("Usage: listId segmentId crawlerName");
    }
    
    int listId    = Integer.parseInt(args[0]);
    int segmentId = Integer.parseInt(args[1]);
    String crawlerName = args[2];
      
    // initialize ...
    Configuration conf = new Configuration();
    
    conf.addResource("nutch-default.xml");
    conf.addResource("nutch-site.xml");
    conf.addResource("hadoop-default.xml");
    conf.addResource("hadoop-site.xml");
    conf.addResource("commoncrawl-default.xml");
    conf.addResource("commoncrawl-site.xml");
    
    CrawlEnvironment.setHadoopConfig(conf);
    
    try { 
      
      System.out.println("Loading Crawl Segment Log for Segment:" + segmentId + " Crawler:" + crawlerName);
      // now do it one more time ... 
      CrawlSegmentDetail detail = SegmentLoader.loadCrawlSegment(listId,segmentId, crawlerName, null, null, null,null);
      
      System.out.println("Segment Detail - URL Count:" + detail.getUrlCount() + " Host Count:" + detail.getHosts().size());
      
      for (CrawlSegmentHost host : detail.getHosts()) { 
        System.out.println("    Host:" + host.getHostName());
        TextBytes textBytes = new TextBytes();
        
        for (CrawlSegmentURL url : host.getUrlTargets()) { 
          System.out.println("      URL2:" + url.getUrl());
        }
      }
    }
    catch (IOException e) { 
      System.out.println(CCStringUtils.stringifyException(e));
    }
  }
  
}


