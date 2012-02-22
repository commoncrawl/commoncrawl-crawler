/**
 * Copyright 2008 - CommonCrawl Foundation
 * 
 * CommonCrawl licenses this file to you under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.commoncrawl.crawl.database.crawlpipeline;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.commoncrawl.util.internal.NodeAffinityMaskBuilder;

import com.google.common.collect.ImmutableList;

/**
 * JobConf Builder 
 * 
 * @author rana
 *
 */
public class JobConfig {
  
  JobConf _jobConf = new JobConf();
  
  public JobConfig(String jobName) { 
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
  public JobConfig inputs(ImmutableList<Path> inputs)throws IOException { 
    for (Path input : inputs) { 
      _jobConf.addInputPath(input);
    }
    return this;
  }
  
  /**
   * add a single input file to the job config
   * @param input
   * @return
   * @throws IOException
   */
  public JobConfig input(Path input)throws IOException { 
    _jobConf.addInputPath(input);
    return this;
  }
  
  public JobConfig output(Path outputPath)throws IOException { 
    _jobConf.setOutputPath(outputPath);
    return this;
  }
  
  /**
   * set input format 
   * 
   * @param inputFormat
   * @return
   * @throws IOException
   */
  public JobConfig inputFormat(Class<? extends InputFormat> inputFormat)throws IOException { 
    _jobConf.setInputFormat(inputFormat);
    return this;
  }
  
  
  public JobConfig inputSeqFile()throws IOException { 
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
  public JobConfig outputFormat(Class<? extends OutputFormat> outputFormat)throws IOException { 
    _jobConf.setOutputFormat(outputFormat);
    return this;
  }

  public JobConfig outputSeqFile()throws IOException { 
    _jobConf.setOutputFormat(SequenceFileOutputFormat.class);
    return this;
  }

  
  /**
   * 
   * @param mapper
   * @return
   * @throws IOException
   */
  public JobConfig mapper(Class<? extends Mapper> mapper)throws IOException { 
    _jobConf.setMapperClass(mapper);
    _jobConf.setJarByClass(mapper);
    return this;
  }
  
  public JobConfig mapperKeyValue(Class<? extends WritableComparable> key,Class<? extends Writable> value)throws IOException { 
    _jobConf.setMapOutputKeyClass(key);
    _jobConf.setMapOutputValueClass(value);
    return this;
  }
      
  public JobConfig reducer(Class<? extends Reducer> reducer,boolean hasCombiner)throws IOException { 
    _jobConf.setReducerClass(reducer);
    if (hasCombiner)
      _jobConf.setCombinerClass(reducer);
    _jobConf.setJarByClass(reducer);
    return this;
  }

  public JobConfig outputKeyValue(Class<? extends WritableComparable> key,Class<? extends Writable> value)throws IOException { 
    _jobConf.setOutputKeyClass(key);
    _jobConf.setOutputValueClass(value);
    return this;
  }

  public JobConfig numMappers(int mappers)throws IOException { 
    _jobConf.setNumMapTasks(mappers);
    return this;
  }

  public JobConfig numReducers(int reducers)throws IOException { 
    _jobConf.setNumReduceTasks(reducers);
    return this;
  }
  
  public JobConfig compressOutput(boolean compress)throws IOException { 
    _jobConf.setBoolean("mapred.output.compress", compress);
    return this;
  }
  
  public JobConfig sort(Class<? extends RawComparator> comparator)throws IOException { 
    _jobConf.setOutputKeyComparatorClass(comparator);
    return this;
  }

  public JobConfig group(Class<? extends RawComparator> comparator)throws IOException { 
    _jobConf.setOutputValueGroupingComparator(comparator);
    return this;
  }
  
  public JobConfig partition(Class<? extends Partitioner> partitioner)throws IOException { 
    _jobConf.setPartitionerClass(partitioner);
    return this;
  }

  public JobConfig setAffinity(Path affinityPath)throws IOException { 
    // set node affinity ...
    String affinityMask = NodeAffinityMaskBuilder
        .buildNodeAffinityMask(FileSystem.get(_jobConf), affinityPath,null);
    
    NodeAffinityMaskBuilder.setNodeAffinityMask(_jobConf, affinityMask);
    
    return this;
  }
  
  public JobConf build()throws IOException { 
    return _jobConf;
  }
  

}
