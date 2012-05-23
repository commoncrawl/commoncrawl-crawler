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

package org.commoncrawl.crawl.filters;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.nio.charset.Charset;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.directoryservice.BlockingClient;

/** 
 * Utility functions used by various filters
 * 
 * @author rana
 *
 */
public class Utils {

  private static final Log LOG = LogFactory.getLog(Utils.class);
  
  static Path getPathForSession(JobConf job) { 
    return new Path(job.get("mapred.temp.dir", ".") + "/list-cache-" + getJobCacheSessionId(job));
  }
  
  static void setJobCacheSessionId(JobConf conf,long sessionId) { 
    conf.setLong("list.cache.session.id", sessionId);
  }
  
  static long getJobCacheSessionId(JobConf conf) { 
    return conf.getLong("list.cache.session.id",0);
  }
  
  
  public static void initializeCacheSession(JobConf job, long sessionId) throws IOException {
    if (job.getLong("list.cache.session.id", -1) == -1) { 
      setJobCacheSessionId(job,sessionId);
      FileSystem fs = FileSystem.get(job);
      LOG.info("Initialize Cache Session. Path is:" + getPathForSession(job));
      fs.mkdirs(getPathForSession(job));
    }
    else { 
      LOG.info("Initialize Cache Session - Session Already Initialized Previously");
    }
  }
  
  public static void publishListToCache(JobConf job,byte[] streamData,String itemPath)throws IOException { 
    FileSystem fs = FileSystem.get(job);
    Path fileSystemPath = null;
    if (itemPath.startsWith("/")) { 
      fileSystemPath = new Path(getPathForSession(job),itemPath.substring(1));
    }
    else { 
      fileSystemPath = new Path(getPathForSession(job),itemPath);
    }
    
    LOG.info("Publishing Filter at:" + itemPath + " to hdfs location:" + fileSystemPath);
    
    FSDataOutputStream outputStream = fs.create(fileSystemPath);
    try { 
      outputStream.write(streamData);
    }
    finally { 
      if (outputStream != null) { 
        outputStream.close();
      }
    }
    LOG.info("publishListToCache calling addCacheFile with path:" + fileSystemPath.toString());
    DistributedCache.addCacheFile(fileSystemPath.toUri(), job);
  }
  
  public static void loadFilterFromStream(InputStream inputStream,Filter filterObject) throws IOException { 

    // read filter items
    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
    String filterItemLine = null;
    
    while ((filterItemLine = reader.readLine()) != null) {
      // LOG.info("Got Filter Line:" + filterItemLine);
      if (!filterItemLine.startsWith("#")) { 
        filterObject.loadFilterItem(filterItemLine);
      }
    }
    
  }
  
  public static void loadFilterFromCache(JobConf job,String cacheFilePath,Filter filterObject)throws IOException { 
    LOG.info("Loading filter data from:" + cacheFilePath);
    byte data[] = Utils.loadListFromCache(job, cacheFilePath);
    
    // create an input stream based on the loaded data 
    ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
    
    // load from stream
    loadFilterFromStream(inputStream,filterObject);
  }
  
  public static void loadFilterFromMasterFile(JobConf job,Filter filterObject,String filterFilePath) throws IOException { 
    String directoryServiceIp = job.get(CrawlEnvironment.DIRECTORY_SERVICE_ADDRESS_PROPERTY, "10.0.20.21");
    
    ByteArrayInputStream inputStream = new ByteArrayInputStream(buildConsolidatedStreamFromMasterStream(job, filterFilePath));
    
    loadFilterFromStream(inputStream,filterObject);
  }

  public static void loadFilterFromPath(InetAddress directoryServer,Filter filterObject,String filterFilePath,boolean hasMasterFile) throws IOException { 
    ByteArrayInputStream inputStream = null;
    if (hasMasterFile) { 
      inputStream = new ByteArrayInputStream(buildConsolidatedStreamFromMasterStream(directoryServer, filterFilePath));
    }
    else { 
      inputStream = new ByteArrayInputStream(BlockingClient.loadDataFromPath(directoryServer,filterFilePath));
    }
    loadFilterFromStream(inputStream,filterObject);
  }

  
  public static byte[] buildConsolidatedStreamFromMasterStream(JobConf job,String masterFilePath) throws IOException {
    String directoryServiceIp = job.get(CrawlEnvironment.DIRECTORY_SERVICE_ADDRESS_PROPERTY, "10.0.20.21");
    return buildConsolidatedStreamFromMasterStream(InetAddress.getByName(directoryServiceIp),masterFilePath);
  }
  
  public static byte[] buildConsolidatedStreamFromMasterStream(InetAddress directoryServiceServer,String masterFilePath) throws IOException { 
    //create and output stream..
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    BufferedWriter   charWriter = new BufferedWriter(new OutputStreamWriter(outputStream,Charset.forName("UTF8")));

    LOG.info("Loading master file for filter at:" + masterFilePath + " ServerAddress:" + directoryServiceServer.toString());
    // load primary path via directory service client
    BufferedReader reader = BlockingClient.createReaderFromPath(directoryServiceServer,masterFilePath);
    
    try { 
      // load streams referenced in master stream
      String line = null;
      while ((line = reader.readLine()) != null) { 
        if (!line.startsWith("#")) {
          LOG.info("Loading referenced Filter file at:" + line);
          // load inner stream 
          BufferedReader innerReader = BlockingClient.createReaderFromPath(directoryServiceServer, line);
          
          try { 
            String innerLine = null;
            
            while ((innerLine = innerReader.readLine()) != null) { 
              charWriter.write(innerLine);
              charWriter.write('\n');
            }
          }
          finally { 
            if (innerReader != null) { 
              innerReader.close();
            }
          }
        }
      }
    }
    finally { 
      if (reader != null) { 
        reader.close();
      }
    }
    charWriter.flush();
    
    return outputStream.toByteArray();
  }
  
  public static void publishFilterToCache(JobConf job,Filter filterObject,String filterFilePath,boolean isMasterFile)throws IOException { 

    String directoryServiceIp = job.get(CrawlEnvironment.DIRECTORY_SERVICE_ADDRESS_PROPERTY, "10.0.20.21");
    
    byte dataBuffer[] = null; 
    if (isMasterFile) {
      LOG.info("Loading Master File at:" + filterFilePath + " Server:" + directoryServiceIp);
      dataBuffer =  buildConsolidatedStreamFromMasterStream(job,filterFilePath);
    }
    else { 
      LOG.info("Loading Single Filter File at:" + filterFilePath +" Server:" + directoryServiceIp);
      dataBuffer = BlockingClient.loadDataFromPath(InetAddress.getByName(directoryServiceIp),filterFilePath);
    }
    LOG.info("Publishing Filter at:" + filterFilePath + " to Cache");
    publishListToCache(job,dataBuffer,filterFilePath);
  }
  
  
  public static byte[] loadListFromCache(JobConf job,String itemPath) throws IOException {
    
    // tweak the item path to reflect the bizarre way in which distributed cache stores paths
    Path localPath = new Path(itemPath);
    //localPath = new Path(localPath,localPath.getName());
    itemPath = localPath.toString();
    
    Path paths[] = DistributedCache.getLocalCacheFiles(job);
    
    LOG.info("loadListFromCache returned path count:" + paths.length);
    
    for (Path path: paths) { 
      
      LOG.info("loadListFromCache - trying to match current path:" + path.toString() + " to target:" + itemPath);
      
      if (path.toString().endsWith(itemPath)) { 
        FileStatus fileStatus = FileSystem.getLocal(job).getFileStatus(path);
        
        LOG.info("match found! Loading Data");
        if (fileStatus != null && fileStatus.getLen() != 0) {
          
          byte data[] = new byte[(int)fileStatus.getLen()];
          FSDataInputStream dataInput = FileSystem.getLocal(job).open(path);
          try { 
            dataInput.read(data);
            return data;
          }
          finally { 
            if (dataInput != null) { 
              dataInput.close();
            }
          }
        }
        else { 
          LOG.error("Unable to load file at:" + itemPath);
          throw new IOException("Unable to load File at:" + itemPath);
        }
      }
    }
    LOG.error("Unable to locate target item:" + itemPath);
    throw new IOException("Unable to locate File:" + itemPath + " in local cache!");
  }
  
  public void destoryCache(JobConf job)throws IOException { 
    if (getJobCacheSessionId(job) != 0) { 
      FileSystem fs = FileSystem.get(job);
      fs.delete(getPathForSession(job), true);
    }
  }
  
}
