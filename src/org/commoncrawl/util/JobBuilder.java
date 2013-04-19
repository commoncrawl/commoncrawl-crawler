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
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapRunner;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;


/**
 * 
 * @author rana
 *
 */
public class JobBuilder {
  
  JobConf _jobConf;
  
  public JobBuilder(String jobName,Configuration conf) {
    _jobConf = new JobConf(conf);
    // legacy crap 
    _jobConf.addResource("nutch-default.xml");
    _jobConf.addResource("nutch-site.xml");
    //defaults 
    _jobConf.setNumTasksToExecutePerJvm(1000);
    _jobConf.setJobName(jobName);
  }

  /**
   * add inputs to the job config 
   * 
   * @param inputs
   * @return
   * @throws IOException
   */
  public JobBuilder inputs(List<Path> inputs)throws IOException { 
    for (Path input : inputs) { 
      FileInputFormat.addInputPath(_jobConf, input);
    }
    return this;
  }
  
  /**
   * add a single input file to the job config
   * @param input
   * @return
   * @throws IOException
   */
  public JobBuilder input(Path input)throws IOException { 
    FileInputFormat.addInputPath(_jobConf, input);
    return this;
  }
  
  public JobBuilder output(Path outputPath)throws IOException {
    FileOutputFormat.setOutputPath(_jobConf,outputPath); 
    return this;
  }
  
  /**
   * set input format 
   * 
   * @param inputFormat
   * @return
   * @throws IOException
   */
  public JobBuilder inputFormat(Class<? extends InputFormat> inputFormat)throws IOException { 
    _jobConf.setInputFormat(inputFormat);
    return this;
  }
  
  
  public JobBuilder inputIsSeqFile()throws IOException { 
    _jobConf.setInputFormat(SequenceFileInputFormat.class);
    return this;
  }

  /**
   * set output format 
   * 
   * @param inputFormat
   * @return
   * @throws IOException
   */
  public JobBuilder outputFormat(Class<? extends OutputFormat> outputFormat)throws IOException { 
    _jobConf.setOutputFormat(outputFormat);
    return this;
  }

  public JobBuilder outputIsSeqFile()throws IOException { 
    _jobConf.setOutputFormat(SequenceFileOutputFormat.class);
    return this;
  }

  
  public JobBuilder jarByClass(Class theClass)throws IOException { 
    _jobConf.setJarByClass(theClass);
    return this;
  }
  /**
   * 
   * @param mapper
   * @return
   * @throws IOException
   */
  public JobBuilder mapper(Class<? extends Mapper> mapper)throws IOException {
    if (mapper != IdentityMapper.class)
      _jobConf.setMapperClass(mapper);
    _jobConf.setJarByClass(mapper);
    return this;
  }
  
  public JobBuilder mapRunner(Class<? extends MapRunner> mapRunner)throws IOException {
    _jobConf.setMapRunnerClass(mapRunner);
    _jobConf.setJarByClass(mapRunner);
    return this;
  }
  
  public JobBuilder mapperKeyValue(Class<? extends WritableComparable> key,Class<? extends Writable> value)throws IOException { 
    _jobConf.setMapOutputKeyClass(key);
    _jobConf.setMapOutputValueClass(value);
    _jobConf.setOutputKeyClass(key);
    _jobConf.setOutputValueClass(value);
    
    return this;
  }
      
  public JobBuilder reducer(Class<? extends Reducer> reducer,boolean hasCombiner)throws IOException { 
    if (reducer != IdentityReducer.class)
      _jobConf.setReducerClass(reducer);
    if (hasCombiner)
      _jobConf.setCombinerClass(reducer);
    _jobConf.setJarByClass(reducer);
    return this;
  }

  public JobBuilder outputKeyValue(Class<? extends WritableComparable> key,Class<? extends Writable> value)throws IOException { 
    _jobConf.setOutputKeyClass(key);
    _jobConf.setOutputValueClass(value);
    return this;
  }

  public JobBuilder keyValue(Class<? extends WritableComparable> key,Class<? extends Writable> value)throws IOException { 
    _jobConf.setMapOutputKeyClass(key);
    _jobConf.setMapOutputValueClass(value);
    _jobConf.setOutputKeyClass(key);
    _jobConf.setOutputValueClass(value);
    return this;
  }
  
  public JobBuilder numMappers(int mappers)throws IOException { 
    _jobConf.setNumMapTasks(mappers);
    return this;
  }

  public JobBuilder numReducers(int reducers)throws IOException { 
    _jobConf.setNumReduceTasks(reducers);
    return this;
  }
  
  public JobBuilder compressMapOutput(boolean compress)throws IOException {
    _jobConf.setCompressMapOutput(compress);
    return this;
  }
  
  public JobBuilder compressor(CompressionType type,Class<? extends CompressionCodec> codec)throws IOException { 
    _jobConf.setBoolean("mapred.output.compress", true);
    _jobConf.set("mapred.output.compression.type", type.toString());
    _jobConf.setClass("mapred.output.compression.codec", codec,CompressionCodec.class);
    return this;
  }
  public JobBuilder compressType(CompressionType type)throws IOException {
    if (type == CompressionType.NONE) { 
      _jobConf.setBoolean("mapred.output.compress", false);
    }
    else { 
      _jobConf.setBoolean("mapred.output.compress", true);
    }
    _jobConf.set("mapred.output.compression.type", type.toString());
    return this;
  }
  
  
  public JobBuilder sort(Class<? extends RawComparator> comparator)throws IOException { 
    _jobConf.setOutputKeyComparatorClass(comparator);
    return this;
  }

  public JobBuilder group(Class<? extends RawComparator> comparator)throws IOException { 
    _jobConf.setOutputValueGroupingComparator(comparator);
    return this;
  }
  
  public JobBuilder partition(Class<? extends Partitioner> partitioner)throws IOException { 
    _jobConf.setPartitionerClass(partitioner);
    return this;
  }
  
  public JobBuilder speculativeExecution(boolean enabled)throws IOException { 
    _jobConf.setSpeculativeExecution(enabled);
    return this;
  }
  
  public JobBuilder speculativeMapExecution() throws IOException { 
    _jobConf.setMapSpeculativeExecution(true);
    return this;
  }
  
  public JobBuilder speculativeReducerExecution() throws IOException { 
    _jobConf.setReduceSpeculativeExecution(true);
    return this;
  }

  
  public JobBuilder maxMapAttempts(int maxAttempts)throws IOException { 
    _jobConf.setMaxMapAttempts(maxAttempts);
    return this;
  }
  
  public JobBuilder maxReduceAttempts(int maxAttempts)throws IOException { 
    _jobConf.setMaxReduceAttempts(maxAttempts);
    return this;
  }
  
  public JobBuilder delayReducersUntil(float pctMappersComplete)throws IOException { 
    _jobConf.setFloat("mapred.reduce.slowstart.completed.maps", pctMappersComplete);
    return this;
  }
  
  public JobBuilder maxMapTaskFailures(int percent)throws IOException { 
    _jobConf.setMaxMapTaskFailuresPercent(percent);
    return this;
  }

  public JobBuilder maxReduceTaskFailures(int percent)throws IOException { 
    _jobConf.setMaxReduceTaskFailuresPercent(percent);
    return this;
  }

  public JobBuilder setAffinity(Path affinityPath)throws IOException {
    if (affinityPath != null) { 
      // set node affinity ...
      String affinityMask = NodeAffinityMaskBuilder
          .buildNodeAffinityMask(FileSystem.get(_jobConf), affinityPath,null);
      
      NodeAffinityMaskBuilder.setNodeAffinityMask(_jobConf, affinityMask);
    }
    
    return this;
  }
  
  public JobBuilder reuseJVM(int numberOfTimes)throws IOException { 
    _jobConf.setNumTasksToExecutePerJvm(numberOfTimes);
    return this;
  }

  public JobBuilder setAffinity(Path affinityPath,Set<String> exclusionSet)throws IOException { 
    // set node affinity ...
    String affinityMask = NodeAffinityMaskBuilder
        .buildNodeAffinityMask(FileSystem.get(_jobConf), affinityPath,null,exclusionSet);
    
    NodeAffinityMaskBuilder.setNodeAffinityMask(_jobConf, affinityMask);
    
    return this;
  }

  public JobBuilder setAffinityNoBalancing(Path affinityPath,Set<String> exclusionSet)throws IOException { 
    // set node affinity ...
    String affinityMask = NodeAffinityMaskBuilder
        .buildNodeAffinityMask(FileSystem.get(_jobConf), affinityPath,null,exclusionSet,_jobConf.getInt("mapred.tasktracker.reduce.tasks.maximum", -1),true);
    
    NodeAffinityMaskBuilder.setNodeAffinityMask(_jobConf, affinityMask);
    
    return this;
  }
  
  public JobBuilder minSplitSize(long minSplitSize)throws IOException { 
    _jobConf.setLong("mapred.min.split.size",minSplitSize);
    return this;
  }
  
  public JobConf build()throws IOException { 
    return _jobConf;
  }
  
  public static Path tempDir(Configuration conf,String baseName) throws IOException {

    Path tempOutputDir = new Path(conf.get("mapred.temp.dir", ".")
            + "/"
            + baseName
            + "-"
            + System.currentTimeMillis());

    return tempOutputDir;
  }
  
  public JobBuilder set(String key,String value)throws IOException { 
    _jobConf.set(key, value);
    return this;
  }
  
  public JobBuilder set(String key,long value)throws IOException { 
    _jobConf.setLong(key, value);
    return this;
  }
  
}
