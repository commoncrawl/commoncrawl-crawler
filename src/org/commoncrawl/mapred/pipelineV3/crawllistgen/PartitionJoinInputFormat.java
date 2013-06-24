package org.commoncrawl.mapred.pipelineV3.crawllistgen;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.hadoop.util.TextDatumInputSplit;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

public class PartitionJoinInputFormat implements InputFormat<IntWritable,Text> {

  public static final String PARTITION_COUNT_TEXT = "CustomFF.ParitionCount";
  public static final String PARTITION_ID_PREFIX = "CustomFF.ParitionID";

  /** 
   * multiple paths per partiition 
   * @param partitions
   * @param conf
   */
  public static void writePartitions(List<List<Path>> partitions,Configuration conf) { 
    conf.setInt(PARTITION_COUNT_TEXT,partitions.size());
    for (int partIndex=0;partIndex<partitions.size();++partIndex) { 
      conf.set(PARTITION_ID_PREFIX+partIndex,Joiner.on(',').join(partitions.get(partIndex)).toString());
    }
  }
  
  /** 
   * one path per partition 
   * @param partitions
   * @param conf
   */
  public static void writeSinglePathPerPartition(List<Path> partitions,Configuration conf) { 
    conf.setInt(PARTITION_COUNT_TEXT,partitions.size());
    for (int partIndex=0;partIndex<partitions.size();++partIndex) { 
      conf.set(PARTITION_ID_PREFIX+partIndex,partitions.get(partIndex).toString());
    }
  }
  
  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits)
      throws IOException {
    int numPartitions = job.getInt(PARTITION_COUNT_TEXT, -1);
    InputSplit splits[] = new InputSplit[numPartitions];
    for (int i=0;i<numPartitions;++i) { 
      splits[i] = new TextDatumInputSplit(Integer.toString(i) +"," + job.get(PARTITION_ID_PREFIX+i));
    }
    return splits;
  }

  @Override
  public RecordReader<IntWritable, Text> getRecordReader(final InputSplit split,
      JobConf job, Reporter reporter) throws IOException {
    
    final ArrayList<String> parts = Lists.newArrayList(Splitter.on(',')
    .trimResults()
    .omitEmptyStrings()
    .split(((TextDatumInputSplit)split).getDatum()));
    
    final int partitionId = Integer.parseInt(parts.remove(0));

    return new RecordReader<IntWritable,Text>() {

      int index=0;
      
      @Override
      public boolean next(IntWritable key, Text value) throws IOException {
        if (index < parts.size()) { 
          key.set(partitionId);
          value.set(parts.get(index));
          index++;
          return true;
        }
        return false;
      }

      @Override
      public IntWritable createKey() {
        return new IntWritable();
      }

      @Override
      public Text createValue() {
        return new Text();
      }

      @Override
      public long getPos() throws IOException {
        return 0;
      }

      @Override
      public void close() throws IOException {
        
      }

      @Override
      public float getProgress() throws IOException {
        return 0;
      }
    };
  } 
  
}