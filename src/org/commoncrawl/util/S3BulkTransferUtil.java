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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
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
import com.google.gson.JsonPrimitive;
import com.google.gson.stream.JsonReader;

/** 
 * Utility used to transfer data from S3 down to colo in bulk
 * 
 * @author rana
 *
 */
public class S3BulkTransferUtil implements S3Downloader.Callback {

  private static final Log LOG = LogFactory.getLog(S3BulkTransferUtil.class);
  
  S3Downloader _downloader;

  Configuration _conf;
  FileSystem    _fs;
  int _totalQueuedItemsCount;
  int _totalCompletedItemsCount = 0;
  ConcurrentSkipListMap<String,Path> _pathMapping = new ConcurrentSkipListMap<String, Path>();
  
  S3BulkTransferUtil(String bucketName, String s3AccessKeyId,String s3SecretKey, JsonArray pathList,final Path outputPath)throws IOException {
    _conf = new Configuration();
    _fs   = FileSystem.get(_conf);
    LOG.info("Initializing Downloader");
    _downloader = new S3Downloader(bucketName,s3AccessKeyId,s3SecretKey,false);
    _downloader.setMaxParallelStreams(150);
    _downloader.initialize(this);
    
    LOG.info("Got JSON Array with:" + pathList.size() + " elements");
    for (int i=0;i<pathList.size();++i){
      LOG.info("Collecting files from path:" + pathList.get(i).toString());
      List<S3ObjectSummary> metadataFiles = getPaths(s3AccessKeyId, s3SecretKey,bucketName,pathList.get(i).getAsString());
      LOG.info("Got:" + metadataFiles.size() + " total files");
      for (S3ObjectSummary metadataFile : metadataFiles) {
        
        Path s3Path = new Path("/" + metadataFile.getKey());
        Path finalPath = new Path(outputPath,s3Path.getName());
        
        FileStatus fileStatus = null;
        try { 
          fileStatus = _fs.getFileStatus(finalPath);
        }
        catch (Exception e) { 
          
        }
          
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
          _pathMapping.put(metadataFile.getKey(),finalPath);
          _downloader.fetchItem(metadataFile.getKey());
        }
        else {
          LOG.info("Skipping Already Download Item:" + metadataFile + " Found at:" + finalPath);
        }
      }
    }
    LOG.info("Waiting for shutdown event");
    _downloader.waitForCompletion();
  }
  
  
  public static List<S3ObjectSummary> getPaths(String s3AccessKeyId,String s3SecretKey,String bucketName,String segmentPath) throws IOException  { 
       
    AmazonS3Client s3Client = new AmazonS3Client(new BasicAWSCredentials(s3AccessKeyId,s3SecretKey));
    
        
    
    ImmutableList.Builder<S3ObjectSummary> listBuilder = new ImmutableList.Builder<S3ObjectSummary>();
        
    ObjectListing response = s3Client.listObjects(new ListObjectsRequest().withBucketName(bucketName).withPrefix(segmentPath));

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
    
  @Override
  public void downloadComplete(NIOHttpConnection connection,int itemId, String itemKey) {
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
        
        downloadSuccessful = true;
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
  public void downloadFailed(NIOHttpConnection connection,int itemId, String itemKey, String errorCode) {
    LOG.info("Received Download Failed Event for Key:" + itemKey);
    Pair<Path,FSDataOutputStream> downloadTuple = _pathToStreamMap.remove(itemKey);
    
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
  public boolean downloadStarting(NIOHttpConnection connection,int itemId, String itemKey, long contentLength) {
    LOG.info("Received Download Start Event for Key:" + itemKey);
    
    boolean continueDownload = false;

    Path outputFilePath = _pathMapping.get(itemKey);
    
    if (outputFilePath != null) {       
      try {
        _fs.mkdirs(outputFilePath.getParent());
        Pair<Path,FSDataOutputStream> tupleOut = new Pair<Path, FSDataOutputStream>(outputFilePath,_fs.create(outputFilePath));
        LOG.info("Created Stream for Key:"+ itemKey +" temp Path:" + outputFilePath);
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
  
  static Options options = new Options();
  static { 
    
    options.addOption(
        OptionBuilder.withArgName("awsKey").hasArg().withDescription("AWS Key").isRequired().create("awsKey"));
    
    options.addOption(
        OptionBuilder.withArgName("awsSecret").hasArg().withDescription("AWS Secret").isRequired().create("awsSecret"));

    options.addOption(
        OptionBuilder.withArgName("bucket").hasArg().withDescription("S3 bucket name").isRequired().create("bucket"));

    options.addOption(
        OptionBuilder.withArgName("outputPath").hasArg().isRequired().withDescription("HDFS output path").create("outputPath"));
    
    options.addOption(
        OptionBuilder.withArgName("path").hasArg().withDescription("S3 path prefix").create("path"));

    options.addOption(
        OptionBuilder.withArgName("paths").hasArg().withDescription("S3 paths as a JSON Array").create("paths"));

  }
  
  static void printUsage() { 
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( "S3BulkTransferUtil", options );
  }

  
  public static void main(String[] args)throws IOException {
    
    CommandLineParser parser = new GnuParser();

    try {
      // parse the command line arguments
      CommandLine cmdLine = parser.parse( options, args );
      
      String s3AccessKey = cmdLine.getOptionValue("awsKey");
      String s3Secret    = cmdLine.getOptionValue("awsSecret");
      String s3Bucket    = cmdLine.getOptionValue("bucket");
      Path hdfsOutputPath = new Path(cmdLine.getOptionValue("outputPath")); 
      JsonArray paths = new JsonArray();
      
      if (cmdLine.hasOption ("path")) { 
        String values[] = cmdLine.getOptionValues("path");
        for (String value : values) { 
          paths.add( new JsonPrimitive(value));
        }
      }
      if (cmdLine.hasOption("paths")) { 
        JsonParser jsonParser = new JsonParser();
        JsonReader reader  = new JsonReader(new StringReader(cmdLine.getOptionValue("paths")));
        reader.setLenient(true);
        JsonArray  array  = jsonParser.parse(reader).getAsJsonArray();
        if (array != null) { 
          paths.addAll(array);
        }
      }
      
      if (paths.size() == 0) { 
        throw new IOException("No Input Paths Specified!");
      }
      
      LOG.info("Bucket:" + s3Bucket + " Target Paths:" + paths.toString());
      
      S3BulkTransferUtil util = new S3BulkTransferUtil(s3Bucket,s3AccessKey,s3Secret,paths,hdfsOutputPath);
    }
    catch (Exception e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      printUsage();
    }
  }
  
  
  
}
