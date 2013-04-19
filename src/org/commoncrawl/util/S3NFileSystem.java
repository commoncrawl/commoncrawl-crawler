package org.commoncrawl.util;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;

/** 
 * Beginnings of a replacement for the S3N FileSystem 
 * 
 * @author rana
 *
 */
public class S3NFileSystem extends NativeS3FileSystem {

  AmazonS3Client _s3Client;

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    _s3Client = new AmazonS3Client(new BasicAWSCredentials(getConf().get("fs.s3n.awsAccessKeyId"),getConf().get("fs.s3n.awsSecretAccessKey")));
  }

  @Override
  public void close() throws IOException {
    super.close();
    _s3Client.shutdown();
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    // make absolute 
    if (!f.isAbsolute()) { 
      f = new Path(getWorkingDirectory(),f);
    }
    // qualify with 
    f = makeQualified(f);
    
    return new FSDataInputStream(
        new S3SeekableResilientInputStream(
            f.toUri(), 
            getConf().get("fs.s3n.awsAccessKeyId"), 
            getConf().get("fs.s3n.awsSecretAccessKey"), bufferSize, 100));
  }
}  