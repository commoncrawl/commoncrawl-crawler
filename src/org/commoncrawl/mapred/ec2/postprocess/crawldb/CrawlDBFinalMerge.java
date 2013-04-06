package org.commoncrawl.mapred.ec2.postprocess.crawldb;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
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
import org.commoncrawl.util.S3SeekableResilientInputStream;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader.KeyAndValueData;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader.RawRecordValue;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.Tuples.Pair;

/** 
 * Final Merge Step is done using a non-shuffle reduce since all input segments have been pre-sorted and 
 * pre-sharded with the proper shard count.
 * 
 * @author rana
 *
 */
public class CrawlDBFinalMerge implements Reducer<IntWritable, Text ,TextBytes,TextBytes> {

  static final Log LOG = LogFactory.getLog(CrawlDBFinalMerge.class);
  
  JobConf _conf;
  @Override
  public void configure(JobConf job) {
    _conf = job;
  }

  @Override
  public void close() throws IOException {
  }

  static class RawValueIterator implements Iterator<TextBytes>  {

    TextBytes valueBytes = new TextBytes();
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
        inputBuffer.reset(nextRawValue.data.getData(),0,nextRawValue.data.getLength());
        int valueTextLen = WritableUtils.readVInt(inputBuffer);
        valueBytes.set(nextRawValue.data.getData(),inputBuffer.getPosition(),valueTextLen);
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
  
  public void reduce(IntWritable key, Iterator<Text> values,OutputCollector<TextBytes, TextBytes> output, Reporter reporter) throws IOException {
    // collect all incoming paths first
    Vector<Path> incomingPaths = new Vector<Path>();
    
    while(values.hasNext()){ 
      String path = values.next().toString();
      LOG.info("Found Incoming Path:" + path);
      incomingPaths.add(new Path(path));
    }


    // set up merge attributes
    Configuration localMergeConfig = new Configuration(_conf);
    // we don't want to use a grouping comparator because the we are using the reducer code from the intermediate 
    // merge 
    localMergeConfig.setClass(
        MultiFileInputReader.MULTIFILE_COMPARATOR_CLASS,
        CrawlDBKey.LinkKeyComparator.class, RawComparator.class);
    localMergeConfig.setClass(MultiFileInputReader.MULTIFILE_KEY_CLASS,TextBytes.class, WritableComparable.class);
    
    
    // spawn merger
    // Skip using s3n fs, as it seems to have a limitation on the number of parallel s3 streams it can open :-(
    // Hence the hacked FileSystem that uses the CC S3ReslientInputStream
    MultiFileInputReader<TextBytes> multiFileInputReader = new MultiFileInputReader<TextBytes>(new FileSystem() {

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
        FileSystem fs = FileSystem.get(f.toUri(),_conf);
        return fs.getFileStatus(f);
      } 
      
    }, incomingPaths, localMergeConfig);

    // create crawl db writer, which is the actual reducer we want to use ...  
    CrawlDBWriter crawlDBWriter = new CrawlDBWriter();
    crawlDBWriter.configure(_conf);
    
    RawValueIterator rawValueIterator = new RawValueIterator();
    
    Pair<KeyAndValueData<TextBytes>,Iterable<RawRecordValue>> nextItem = null;
    // walk tuples and feed them to the actual reducer ...  
    while ((nextItem = multiFileInputReader.getNextItemIterator()) != null) {
      rawValueIterator.reset(nextItem.e1);
      // output to reducer ... 
      crawlDBWriter.reduce(nextItem.e0._keyObject,rawValueIterator, output, reporter);
    }
    // flush output 
    crawlDBWriter.close();
  }

}
