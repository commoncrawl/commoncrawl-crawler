package org.commoncrawl.mapred.ec2.postprocess.crawldb;

import java.io.IOException;
import static org.mockito.Mockito.mock;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.InputBuffer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Counters.Counter;
import org.commoncrawl.hadoop.mergeutils.MergeSortSpillWriter;
import org.commoncrawl.hadoop.mergeutils.RawDataSpillWriter;
import org.commoncrawl.hadoop.mergeutils.RawKeyValueComparator;
import org.commoncrawl.hadoop.mergeutils.SequenceFileSpillWriter;
import org.commoncrawl.util.TextBytes;

import com.google.common.collect.Iterators;

public class SortPartitionData {
  
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
  
  public static void main(String[] args) throws IOException {
    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);
    Configuration conf = new Configuration();
    
    final SequenceFile.Writer outputWriter = SequenceFile.createWriter(
        FileSystem.get(outputPath.toUri(),conf),
        conf,
        outputPath,
        TextBytes.class,
        TextBytes.class,
        CompressionType.BLOCK,
        new GzipCodec());
    
    final CrawlDBMergingReducer crawlDBWriter = new CrawlDBMergingReducer();
    crawlDBWriter.configure(new JobConf(conf));
    
    final OutputCollector<TextBytes,TextBytes> collector = new OutputCollector<TextBytes, TextBytes>() {
      
      @Override
      public void collect(TextBytes key, TextBytes value) throws IOException {
        outputWriter.append(key, value);
      }
    };
    
    RawDataSpillWriter<TextBytes, TextBytes> spillWriter = new RawDataSpillWriter<TextBytes, TextBytes>() {
      
      TextBytes _key = new TextBytes();
      TextBytes _value = new TextBytes();
      DataInputBuffer _buffer = new DataInputBuffer();
      
      @Override
      public void spillRecord(TextBytes key, TextBytes value) throws IOException {
        crawlDBWriter.reduce(key, Iterators.forArray(value), collector, mock(Reporter.class));
      }
      
      @Override
      public void close() throws IOException {
        crawlDBWriter.close();
        outputWriter.close();
      }
      
      @Override
      public void spillRawRecord(byte[] keyData, int keyOffset, int keyLength,
          byte[] valueData, int valueOffset, int valueLength) throws IOException {
        _buffer.reset(keyData,keyOffset,keyLength);
        int realLength = WritableUtils.readVInt(_buffer);
        _key.set(keyData, keyOffset + _buffer.getPosition(), realLength);
        _buffer.reset(valueData,valueOffset,valueLength);
        realLength = WritableUtils.readVInt(_buffer);
        _value.set(valueData,valueOffset + _buffer.getPosition(),realLength);
        spillRecord(_key, _value);
      }
    };
    
    //SequenceFileSpillWriter<TextBytes, TextBytes> finalWriter 
    //  = new SequenceFileSpillWriter<TextBytes, TextBytes>(FileSystem.get(outputPath.toUri(),conf), conf, outputPath, TextBytes.class, TextBytes.class, null, true);
    
    conf.setInt(MergeSortSpillWriter.SPILL_INDEX_BUFFER_SIZE_PARAM, 100000000);
    conf.setInt(SequenceFileSpillWriter.SPILL_WRITER_BUFFER_SIZE_PARAM,100000000);
    
    MergeSortSpillWriter merger 
    = new MergeSortSpillWriter<TextBytes,TextBytes>(
        conf, 
        spillWriter, 
        FileSystem.getLocal(conf),
        new Path("/tmp"), 
        null,
        new Comparator(), 
        TextBytes.class, 
        TextBytes.class, 
        true, 
        null);
    
    try { 
    SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(inputPath.toUri(),conf), inputPath, conf);
      TextBytes key = new TextBytes();
      TextBytes value = new TextBytes();
      
      while (reader.next(key, value)) { 
        merger.spillRecord(key, value);
      }
    }
    finally { 
      merger.close();
      spillWriter.close();
    }
  }
}
