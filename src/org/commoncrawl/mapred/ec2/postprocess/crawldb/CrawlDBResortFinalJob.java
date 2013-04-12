package org.commoncrawl.mapred.ec2.postprocess.crawldb;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.commoncrawl.hadoop.mergeutils.MergeSortSpillWriter;
import org.commoncrawl.hadoop.mergeutils.RawDataSpillWriter;
import org.commoncrawl.hadoop.mergeutils.RawKeyValueComparator;
import org.commoncrawl.hadoop.mergeutils.SequenceFileSpillWriter;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.MultiFileMergeUtils;
import org.commoncrawl.util.TextBytes;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;


/** 
 * The intermediate segments link graph data doesn't seem to be sorted properly, and 
 * the regeneration of this data would be costly, so to avoid this, we are going to resort 
 * and re-merge each shard individually. ... Temporary Fix :-(
 * 
 *  
 * @author rana
 *
 */
public class CrawlDBResortFinalJob implements Reducer<IntWritable, Text ,TextBytes,TextBytes> {
  
  static final Log LOG = LogFactory.getLog(CrawlDBResortFinalJob.class);
  
  public static void main(String[] args)throws IOException {
    Path existingMergeDBPath = new Path(args[0]);
    Path fixedMergedDBPath = new Path(args[1]);

    Configuration conf = new Configuration();
    
    
    // spin up the resort job ... 
    JobConf jobConf = new JobBuilder("Resort Final Merge Shards", conf)
    .inputs(Lists.newArrayList(existingMergeDBPath))
    .inputFormat(MultiFileMergeUtils.MultiFileMergeInputFormat.class)
    .mapperKeyValue(IntWritable.class, Text.class)
    .outputKeyValue(TextBytes.class, TextBytes.class)
    .outputFormat(SequenceFileOutputFormat.class)
    .reducer(CrawlDBResortFinalJob.class,false)
    .partition(MultiFileMergeUtils.MultiFileMergePartitioner.class)
    .numReducers(CrawlDBCommon.NUM_SHARDS)
    .speculativeExecution(true)
    .output(fixedMergedDBPath)
    .compressMapOutput(true)
    .compressor(CompressionType.BLOCK, GzipCodec.class)
    .maxMapAttempts(10)
    .maxReduceAttempts(4)
    .maxMapTaskFailures(1)
    .reuseJVM(1)
    .build();
    
    LOG.info("Starting JOB:" + jobConf);
    try { 
      JobClient.runJob(jobConf);
      LOG.info("Finished JOB:" + jobConf);
    }
    catch (IOException e) { 
      LOG.error("Failed to Execute JOB:" + jobConf + " Exception:\n" + CCStringUtils.stringifyException(e));
    }    
  }

  JobConf _conf;
  @Override
  public void configure(JobConf job) {
    _conf = job;
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub
    
  }

  // declare a keyvalue comparator that delegates to linkkey comparator ... 
  static class Comparator implements RawKeyValueComparator<TextBytes, TextBytes> {

    CrawlDBKey.LinkKeyComparator comparator = new CrawlDBKey.LinkKeyComparator();
    
    @Override
    public int compare(TextBytes key1, TextBytes value1, TextBytes key2,
        TextBytes value2) {
      return comparator.compare(key1, key2);
    }

    @Override
    public int compareRaw(byte[] key1Data, int key1Offset, int key1Length,
        byte[] key2Data, int key2Offset, int key2Length, byte[] value1Data,
        int value1Offset, int value1Length, byte[] value2Data,
        int value2Offset, int value2Length) throws IOException {
      return comparator.compare(key1Data, key1Offset, key1Length, key2Data, key2Offset, key2Length);
    } 
  }    

  
  @Override
  public void reduce(IntWritable key, final Iterator<Text> values,final OutputCollector<TextBytes, TextBytes> collector, final Reporter reporter)throws IOException {
    // we expect a single path per shard here ...
    
    
    // construct the CrawlDBWriter (merging reducer instance we going to delegate to) 
    final CrawlDBWriter crawlDBWriter = new CrawlDBWriter();
    crawlDBWriter.configure(_conf);
    
    // construct a raw data spill writer (required by merger) that delegates to the merging reducer 
    RawDataSpillWriter<TextBytes, TextBytes> spillWriter = new RawDataSpillWriter<TextBytes, TextBytes>() {
      
      TextBytes _key = new TextBytes();
      TextBytes _value = new TextBytes();
      DataInputBuffer _buffer = new DataInputBuffer();
      
      @Override
      public void spillRecord(TextBytes key, TextBytes value) throws IOException {
        // ok spill this to the final collector ... 
        crawlDBWriter.reduce(key, Iterators.forArray(value), collector, reporter);
      }
      
      @Override
      public void close() throws IOException {
        // flush the writer ... 
        crawlDBWriter.close();
      }
      
      @Override
      public void spillRawRecord(byte[] keyData, int keyOffset, int keyLength,
          byte[] valueData, int valueOffset, int valueLength) throws IOException {
        // we want to avoid any memory allocations here .. 
        // read key and data lengths, and reconstitute the key/val objects.
        _buffer.reset(keyData,keyOffset,keyLength);
        int realLength = WritableUtils.readVInt(_buffer);
        _key.set(keyData,_buffer.getPosition(), realLength);
        _buffer.reset(valueData,valueOffset,valueLength);
        realLength = WritableUtils.readVInt(_buffer);
        _value.set(valueData,_buffer.getPosition(),realLength);
        // delegate to typed spill method 
        spillRecord(_key, _value);
      }
    };
    
    // we need a custom config for the merger... since we want to use really big buffers to accomodate really big key/value pairs
    Configuration sortConf = new Configuration(_conf);
    // setup big buffer sizes for merge ... 
    sortConf.setInt(MergeSortSpillWriter.SPILL_INDEX_BUFFER_SIZE_PARAM, 250000000);
    sortConf.setInt(SequenceFileSpillWriter.SPILL_WRITER_BUFFER_SIZE_PARAM,250000000);
   
    // spawn merge sorter
    // it will sort incoming data in chunks and then spill to temp
    // finally, it will merge sort all chunks and spill to final output (spillWriter)
    MergeSortSpillWriter merger 
    = new MergeSortSpillWriter<TextBytes,TextBytes>(
        sortConf, 
        spillWriter, 
        FileSystem.getLocal(_conf),
        new Path("/mnt/tmp/"), 
        null,
        new Comparator(), 
        TextBytes.class, 
        TextBytes.class, 
        true, 
        null);
    
    try { 
      Path inputPath = new Path(Iterators.getNext(values, null).toString());
      // read unsorted file and feed data to merger ... 
      SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(inputPath.toUri(),_conf), inputPath, _conf);
        TextBytes inputKey = new TextBytes();
        TextBytes inputValue = new TextBytes();
        
        while (reader.next(inputKey,inputValue)) { 
          merger.spillRecord(inputKey,inputValue);
        }
    }
    finally { 
        merger.close();
        spillWriter.close();
    }
  }
}
