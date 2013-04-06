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
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.commoncrawl.io.NIOBufferList;
import org.commoncrawl.io.NIOHttpConnection;
import org.commoncrawl.util.Tuples.Pair;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.ImmutableList;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;

/**
 *  @author rana
 */

public class EC2MetadataTransferUtil implements S3Downloader.Callback {

  private static final Log LOG = LogFactory.getLog(EC2MetadataTransferUtil.class);
  
  S3Downloader _downloader;
  private static final String s3AccessKeyId = "";
  private static final String s3SecretKey = "";

  Configuration _conf;
  FileSystem    _fs;
  int _totalQueuedItemsCount;
  int _totalCompletedItemsCount = 0;
  
  EC2MetadataTransferUtil(String bucketName, JsonArray pathList)throws IOException {
    _conf = new Configuration();
    _fs   = FileSystem.get(_conf);
    LOG.info("Initializing Downloader");
    _downloader = new S3Downloader(bucketName,s3AccessKeyId,s3SecretKey,false);
    _downloader.setMaxParallelStreams(150);
    _downloader.initialize(this);
    
    LOG.info("Got JSON Array with:" + pathList.size() + " elements");
    for (int i=0;i<pathList.size();++i){
      LOG.info("Collection metadata files from path:" + pathList.get(i).toString());
      List<S3ObjectSummary> metadataFiles = getMetadataPaths(s3AccessKeyId, s3SecretKey,bucketName,pathList.get(i).getAsString());
      LOG.info("Got:" + metadataFiles.size() + " total files");
      for (S3ObjectSummary metadataFile : metadataFiles) {
        
        Matcher segmentNameMatcher = metadataInfoPattern.matcher(metadataFile.getKey());
        
        if (segmentNameMatcher.matches()) {
          
          String segmentId = segmentNameMatcher.group(1);
          String partExtension = segmentNameMatcher.group(2);
          Path finalSegmentPath = new Path(finalSegmentOutputDir,segmentId);
          Path finalPath = new Path(finalSegmentPath,"metadata-" + partExtension);
          
          FileStatus fileStatus = _fs.getFileStatus(finalPath);
          
          if (fileStatus != null && fileStatus.getLen() != metadataFile.getSize()) { 
            LOG.error(
                "SRC-DEST SIZE MISMATCH!! SRC:" + metadataFile 
                + " SRC-SIZE:" + metadataFile.getSize()
                + " DEST:" + finalPath 
                + " DEST-SIZE:" + fileStatus.getLen());
            
            // ok delete the destination 
            _fs.delete(finalPath,false);
            // null file status so that the item gets requeued ... 
            fileStatus = null;
          }
          
          if (fileStatus == null) { 
            LOG.info("Queueing Item:" + metadataFile);
            ++_totalQueuedItemsCount;
            _downloader.fetchItem(metadataFile.getKey());
          }
          else {
            LOG.info("Skipping Already Download Item:" + metadataFile + " Found at:" + finalPath);
          }
        }
      }
    }
    LOG.info("Waiting for shutdown event");
    _downloader.waitForCompletion();
  }
  
  
  public static List<S3ObjectSummary> getMetadataPaths(String s3AccessKeyId,String s3SecretKey,String bucketName,String segmentPath) throws IOException  { 
       
    AmazonS3Client s3Client = new AmazonS3Client(new BasicAWSCredentials(s3AccessKeyId,s3SecretKey));
    
        
    
    ImmutableList.Builder<S3ObjectSummary> listBuilder = new ImmutableList.Builder<S3ObjectSummary>();
    
    String metadataFilterKey = segmentPath +"metadata-";
    LOG.info("Prefix Search Key is:" + metadataFilterKey);
    
    ObjectListing response = s3Client.listObjects(new ListObjectsRequest().withBucketName(bucketName).withPrefix(metadataFilterKey));

    do {
      LOG.info("Response Key Count:" + response.getObjectSummaries().size());
      
      for (S3ObjectSummary entry : response.getObjectSummaries()) { 
        listBuilder.add(entry);
      }

      if (response.isTruncated()) { 
        response = s3Client.listNextBatchOfObjects(response);
      }
      else { 
        break;
      }
    }
    while (true);
    
    return listBuilder.build();
  }  
  

  
  ConcurrentSkipListMap<String, Pair<Path,FSDataOutputStream>> _pathToStreamMap = new ConcurrentSkipListMap<String, Pair<Path,FSDataOutputStream>>();
  
  @Override
  public boolean contentAvailable(NIOHttpConnection connection,int itemId, String itemKey,NIOBufferList contentBuffer) {
    
    Pair<Path,FSDataOutputStream> downloadTuple = _pathToStreamMap.get(itemKey);
    if (downloadTuple != null) {
      try { 
        while (contentBuffer.available() != 0) {
          ByteBuffer bufferForRead = contentBuffer.read();
          if (bufferForRead != null) { 
            //LOG.info("Writing: " + bufferForRead.remaining() + " bytes for Key:"+ itemKey);
            downloadTuple.e1.write(bufferForRead.array(),bufferForRead.position(),bufferForRead.remaining());
          }
        }
        return true;
      }
      catch (Exception e) { 
        LOG.error("Error during contentAvailable for Key:" + itemKey + " Exception:" + CCStringUtils.stringifyException(e));
      }
    }
    return false;
  }


  static Path finalSegmentOutputDir = new Path("crawl/ec2Import/segment");
  
  Pattern metadataInfoPattern = Pattern.compile(".*/([0-9]*)/metadata-([0-9]+)");
  
  @Override
  public void downloadComplete(int itemId, String itemKey) {
    LOG.info("Received Download Complete Event for Key:" + itemKey);
    Pair<Path,FSDataOutputStream> downloadTuple = _pathToStreamMap.remove(itemKey);
    boolean downloadSuccessful = false;
    
    if (downloadTuple == null) { 
      LOG.error("Excepected Download Tuple for key:" + itemKey + " GOT NULL!");
    }
    else {
      try { 
        // ok close the stream first ... 
        LOG.info("Flushing Stream for key:" + itemKey);
        downloadTuple.e1.flush();
        downloadTuple.e1.close();
        downloadTuple.e1 = null;
        // extract segment name parts 
        Matcher segmentNameMatcher = metadataInfoPattern.matcher(itemKey);
        
        if (segmentNameMatcher.matches()) {
          
          String segmentId = segmentNameMatcher.group(1);
          String partExtension = segmentNameMatcher.group(2);
          // construct final path 
          Path finalSegmentPath = new Path(finalSegmentOutputDir,segmentId);
          _fs.mkdirs(finalSegmentPath);
          Path finalPath = new Path(finalSegmentPath,"metadata-" + partExtension);
          LOG.info("Moving Temp File:" + downloadTuple.e0 + " to:" + finalPath);
          _fs.rename(downloadTuple.e0, finalPath);
          downloadSuccessful = true;
        }
        else { 
          LOG.error("Unable to match regular expression to itemKey:"+ itemKey);
        }
      }
      catch (Exception e) { 
        LOG.error("Error completing download for item:" + itemKey 
            + " Exception:"+ CCStringUtils.stringifyException(e));
      }
      finally { 
        if (downloadTuple.e1 != null) { 
          try { 
            downloadTuple.e1.close();
          }
          catch (IOException e) { 
            LOG.error(CCStringUtils.stringifyException(e));
          }
        }
      }
    }
    
    if (!downloadSuccessful) { 
      LOG.error("Download for Key:" + itemKey + " Unsuccessful. Requeueing");
      try {
        _downloader.fetchItem(itemKey);
      } catch (IOException e) {
        LOG.fatal("Failed to Requeue Item:" + itemKey);
      }
    }
  }


  @Override
  public void downloadFailed(int itemId, String itemKey, String errorCode) {
    LOG.info("Received Download Failed Event for Key:" + itemKey);
    Pair<Path,FSDataOutputStream> downloadTuple = _pathToStreamMap.remove(itemKey);
    boolean downloadSuccessful = false;
    
    if (downloadTuple == null) { 
      LOG.error("Excepected Download Tuple for Failed Download key:" + itemKey + " GOT NULL!");
    }
    else { 
      try { 
        if (downloadTuple.e1 != null) { 
          downloadTuple.e1.close();
          downloadTuple.e1 = null;
        }
        LOG.info("Deleting Temp File:" + downloadTuple.e0 + " for Key:" + itemKey);
        _fs.delete(downloadTuple.e0,false);
      }
      catch (IOException e) { 
        LOG.error(CCStringUtils.stringifyException(e));
      }
    }
    LOG.error("Download for Key:" + itemKey + " Unsuccessful. Requeueing");
    try {
      _downloader.fetchItem(itemKey);
    } catch (IOException e) {
      LOG.fatal("Failed to Requeue Item:" + itemKey);
    }
  }


  @Override
  public boolean downloadStarting(int itemId, String itemKey, long contentLength) {
    LOG.info("Received Download Start Event for Key:" + itemKey);
    
    Matcher segmentNameMatcher = metadataInfoPattern.matcher(itemKey);
    boolean continueDownload = false;
    
    if (segmentNameMatcher.matches()) {
      
      String segmentId = segmentNameMatcher.group(1);
      String partExtension = segmentNameMatcher.group(2);
      
      Path tempOutputDir = new Path(_conf.get("mapred.temp.dir", "."));
      Path tempSegmentDir = new Path(tempOutputDir,segmentId);
      Path metadataFilePath = new Path(tempSegmentDir,"metadata-" + partExtension);
      
      try {
        _fs.mkdirs(tempSegmentDir);
        Pair<Path,FSDataOutputStream> tupleOut = new Pair<Path, FSDataOutputStream>(metadataFilePath,_fs.create(metadataFilePath));
        LOG.info("Created Stream for Key:"+ itemKey +" temp Path:" + metadataFilePath);
        _pathToStreamMap.put(itemKey, tupleOut);
        continueDownload = true;
      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
      }
    }
    else {
      LOG.error("Unable to extract metadata filename parts from name:" + itemKey);
    }
    return continueDownload;
  }
  
  public static void main(String[] args)throws IOException {
    final String bucket = args[0];
    String paths  = args[1];
    
    System.out.println("Paths are:" + paths);
    JsonParser parser = new JsonParser();
    JsonReader reader  = new JsonReader(new StringReader(paths));
    reader.setLenient(true);
    final JsonArray  array  = parser.parse(reader).getAsJsonArray();
    int pathCount = array.size();
    
    EC2MetadataTransferUtil util = new EC2MetadataTransferUtil(bucket, array);
  }
  
  
  
}
