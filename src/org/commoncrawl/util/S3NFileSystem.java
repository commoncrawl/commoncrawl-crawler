package org.commoncrawl.util;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;
import org.apache.hadoop.util.Progressable;

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
  
  private Path makeAbsolute(Path path) {
    if (path.isAbsolute()) {
      return path;
    }
    return new Path(getWorkingDirectory(), path);
  }

  
  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {

    if (exists(f) && !overwrite) {
      throw new IOException("File already exists:"+f);
    }
    Path absolutePath = makeAbsolute(f);
    Path qualifiedPath = makeQualified(absolutePath);
    
    return new FSDataOutputStream(
        new S3MultipartUploadStream(
            qualifiedPath.toUri(), 
            getConf().get("fs.s3n.awsAccessKeyId"),
            getConf().get("fs.s3n.awsSecretAccessKey"),
            overwrite, null, 
            S3MultipartUploadStream.DEFAULT_UPLOADER_COUNT,
            S3MultipartUploadStream.DEFAULT_PART_BUFFER_SIZE, 
            Math.max(S3MultipartUploadStream.DEFAULT_MAX_QUEUED_COUNT, bufferSize / S3MultipartUploadStream.DEFAULT_PART_BUFFER_SIZE), 
            S3MultipartUploadStream.DEFAULT_MAX_RETRIES_PER_PART), statistics);
  }
}  