package org.commoncrawl.mapred.ec2.postprocess.crawldb;

import java.io.IOException;
import java.net.URI;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.MockReporter;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader.KeyAndValueData;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader.RawRecordValue;
import org.commoncrawl.util.S3SeekableResilientInputStream;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.Tuples.Pair;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.collect.Lists;

/** 
 * Final Merge Step is done using a non-shuffle reduce since all input segments have been pre-sorted and 
 * pre-sharded with the proper shard count.
 * 
 * @author rana
 *
 */
public class CrawlDBMergeSortReducer implements Reducer<IntWritable, Text ,TextBytes,TextBytes> {

  static final Log LOG = LogFactory.getLog(CrawlDBMergeSortReducer.class);
  
  JobConf _conf;
  AmazonS3Client _s3Client;

  @Override
  public void configure(JobConf job) {
    _conf = job;
    _s3Client = new AmazonS3Client(new BasicAWSCredentials(_conf.get("fs.s3n.awsAccessKeyId"),_conf.get("fs.s3n.awsSecretAccessKey")));
  }

  @Override
  public void close() throws IOException {
    _s3Client.shutdown();
  }

  static class RawValueIterator implements Iterator<TextBytes>  {

    TextBytes keyBytes  = new TextBytes();
    TextBytes valueBytes = new TextBytes();
    DataInputBuffer keyInputBuffer = new DataInputBuffer();
    DataInputBuffer inputBuffer = new DataInputBuffer();
    
    Iterator<RawRecordValue> rawIterator;
    void reset(Iterable<RawRecordValue> rawIterable) { 
      this.rawIterator = rawIterable.iterator();
    }
    
    @Override
    public boolean hasNext() {
      return rawIterator.hasNext();
    }

    @Override
    public TextBytes next(){
      try { 
        RawRecordValue nextRawValue = rawIterator.next();
        // read in text bytes key ... 
        keyInputBuffer.reset(nextRawValue.key.getData(),0,nextRawValue.key.getLength());
        inputBuffer.reset(nextRawValue.data.getData(),0,nextRawValue.data.getLength());
        int valueTextLen = WritableUtils.readVInt(inputBuffer);
        valueBytes.set(nextRawValue.data.getData(),inputBuffer.getPosition(),valueTextLen);
        int keyTextLen = WritableUtils.readVInt(keyInputBuffer);
        keyBytes.set(nextRawValue.key.getData(),keyInputBuffer.getPosition(),keyTextLen);
        
        System.out.println("NextKey:" + keyBytes.toString() + " Source:" + nextRawValue.source);
        return valueBytes;
      }
      catch (IOException e) { 
        LOG.error(CCStringUtils.stringifyException(e));
        throw new RuntimeException(e);
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("remove");
    } 
  }
    
  private FileSystem getS3NFileSystem()throws IOException {
    return new FileSystem() {

      @Override
      public URI getUri() {
        return null;
      }

      @Override
      public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        return new FSDataInputStream(
            new S3SeekableResilientInputStream(
                f.toUri(), 
                _conf.get("fs.s3n.awsAccessKeyId"), 
                _conf.get("fs.s3n.awsSecretAccessKey"), bufferSize, 100));
      }

      @Override
      public FSDataOutputStream create(Path f, FsPermission permission,
          boolean overwrite, int bufferSize, short replication, long blockSize,
          Progressable progress) throws IOException {
        return null;
      }

      @Override
      public FSDataOutputStream append(Path f, int bufferSize,
          Progressable progress) throws IOException {
        return null;
      }

      @Override
      public boolean rename(Path src, Path dst) throws IOException {
        return false;
      }

      @Override
      @Deprecated
      public boolean delete(Path f) throws IOException {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public boolean delete(Path f, boolean recursive) throws IOException {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public FileStatus[] listStatus(Path f) throws IOException {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public void setWorkingDirectory(Path new_dir) {
        // TODO Auto-generated method stub
        
      }

      @Override
      public Path getWorkingDirectory() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public FileStatus getFileStatus(Path f) throws IOException {
        // get uri from path ... 
        URI uri = f.toUri();
        // convert to s3 path .. 
        String key = uri.getPath().substring(1);
        System.out.println("***uri path:" +key );
        ObjectMetadata metadata = _s3Client.getObjectMetadata(uri.getHost(), key);
        if (metadata != null) { 
          FileStatus fileStatus = new FileStatus(metadata.getContentLength(),false,1,0,metadata.getLastModified().getTime(),0,FsPermission.getDefault(),"","",f);
          return fileStatus;
        }
        return null;
      } 
      
    };    
  }
  public void reduce(IntWritable key, Iterator<Text> values,OutputCollector<TextBytes, TextBytes> output, Reporter reporter) throws IOException {
    // collect all incoming paths first
    Vector<Path> incomingPaths = new Vector<Path>();
    
    Set<String> fsType = new HashSet<String>();
    
    while(values.hasNext()){ 
      String path = values.next().toString();
      LOG.info("Found Incoming Path:" + path);
      incomingPaths.add(new Path(path));
      // convert to uri ... 
      URI uri = new Path(path).toUri();
      // get scheme if present ... 
      String scheme = uri.getScheme();
      if (scheme == null || scheme.length() == 0) { 
        fsType.add("default");
      }
      else { 
        fsType.add(scheme);
      }
    }
    
    if (fsType.size() != 1) { 
      throw new IOException("Only One Input Scheme at a time supported!");
    }
    
    boolean isS3N = fsType.contains("s3n");


    // set up merge attributes
    Configuration localMergeConfig = new Configuration(_conf);
    // we don't want to use a grouping comparator because the we are using the reducer code from the intermediate 
    // merge 
    localMergeConfig.setClass(MultiFileInputReader.MULTIFILE_COMPARATOR_CLASS,CrawlDBKey.LinkKeyComparator.class, RawComparator.class);
    localMergeConfig.setClass(MultiFileInputReader.MULTIFILE_KEY_CLASS,TextBytes.class, WritableComparable.class);
    
    // spawn merger

    // pick filesystem based on path ... 
    FileSystem fs = null;
    if (!isS3N) { 
      fs = FileSystem.get(incomingPaths.get(0).toUri(),_conf);
    }
    else { 
      // use our custom s3n stub 
      fs = getS3NFileSystem();
    }
    LOG.info("FileSystem is:" + fs.toString());
    MultiFileInputReader<TextBytes> multiFileInputReader = new MultiFileInputReader<TextBytes>(fs, incomingPaths, localMergeConfig);

    // create crawl db writer, which is the actual reducer we want to use ...  
    CrawlDBMergingReducer crawlDBWriter = new CrawlDBMergingReducer();
    crawlDBWriter.configure(_conf);
    
    RawValueIterator rawValueIterator = new RawValueIterator();
    
    Pair<KeyAndValueData<TextBytes>,Iterable<RawRecordValue>> nextItem = null;
    // walk tuples and feed them to the actual reducer ...  
    while ((nextItem = multiFileInputReader.getNextItemIterator()) != null) {
      
      System.out.println("PKey:" + nextItem.e0._keyObject);
      rawValueIterator.reset(nextItem.e1);
      // output to reducer ... 
      crawlDBWriter.reduce(nextItem.e0._keyObject,rawValueIterator, output, reporter);
      
      reporter.progress();
    }
    // flush output 
    crawlDBWriter.close();

  }
  
  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }    

  
  private static List<Path> getIntermediateSegmentPaths(AmazonS3Client s3Client)throws IOException { 
    ArrayList<Path> listOut = Lists.newArrayList();
    ObjectListing response = s3Client.listObjects(new ListObjectsRequest()
      .withBucketName("aws-publicdatasets")
      .withPrefix("common-crawl/crawl-db/merge/intermediate/")
      .withDelimiter("/")
      );

    do {
      LOG.info("Response Key Count:" + response.getCommonPrefixes());
      
      for (String entry : response.getCommonPrefixes()) { 
        try { 
          Path s3nPath =new Path("s3n","aws-publicdatasets","/"+entry);
          //long timestamp = Long.parseLong(s3nPath.getName());
          listOut.add(s3nPath);
        }
        catch (Exception e) { 
          
        }
      }

      if (response.isTruncated()) { 
        response = s3Client.listNextBatchOfObjects(response);
      }
      else { 
        break;
      }
    }
    while (true);
    
    return listOut;
  }

  /** 
   * do a merge on a single shard for test purposes
   * @param args
   * @throws IOException
   */
  public static void main(String[] args)throws IOException {
    String s3AccessKey = args[0];
    String s3Secret    = args[1];
    int    partNumber  = Integer.parseInt(args[2]);
    
    AmazonS3Client s3Client = new AmazonS3Client(new BasicAWSCredentials(s3AccessKey,s3Secret));
    
    List<Path> segments = getIntermediateSegmentPaths(s3Client);
    
    String partName = "part-" + NUMBER_FORMAT.format(partNumber);
    
    List<Text> transformedPaths = Lists.newArrayList();
    
    for (Path path : segments) { 
      transformedPaths.add(new Text(new Path(path,partName).toUri().toString()));
      if (transformedPaths.size() >= 3)
        break;
    }
    
    CrawlDBMergeSortReducer finalMerge = new CrawlDBMergeSortReducer();
    
    JobConf conf = new JobConf();

    conf.set("fs.s3n.awsAccessKeyId",s3AccessKey);
    conf.set("fs.s3n.awsSecretAccessKey",s3Secret);
    
    finalMerge.configure(conf);
    finalMerge.reduce(new IntWritable(1), transformedPaths.iterator(), new OutputCollector<TextBytes, TextBytes>() {

      long lastValue = Long.MIN_VALUE;
      
      @Override
      public void collect(TextBytes key, TextBytes value) throws IOException {
        long domainHash = CrawlDBKey.getLongComponentFromKey(key, CrawlDBKey.ComponentId.DOMAIN_HASH_COMPONENT_ID);
        if (domainHash < lastValue) { 
          throw new IOException("LastValue:" + lastValue + " CurrentValue:" + domainHash + " " + value.toString());
        }
        lastValue = domainHash;
        System.out.println("OutputKey:"+ key.toString());
      }
    },new MockReporter());
        
  }
}
