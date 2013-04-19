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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.SortedSet;
import java.util.TreeSet;
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
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.ValueBytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader.KeyAndValueData;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader.RawRecordValue;
import org.commoncrawl.util.Tuples.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Mockito.*;
import com.google.common.collect.*;


/**
 * 
 * @author rana
 *
 */
public class MultiFileMergeUtils {


  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }    


  private static final Log LOG = LogFactory.getLog(MultiFileMergeUtils.class);


  public static class MultiFileMergeSplit extends org.apache.hadoop.mapreduce.InputSplit implements InputSplit {
    public void write(DataOutput out) throws IOException { }
    public void readFields(DataInput in) throws IOException { }
    public long getLength() { return 0L; }
    public String[] getLocations() { return new String[0]; }
  }

  public static class MultiFileMergePartitioner extends org.apache.hadoop.mapreduce.Partitioner<IntWritable, Text> implements Partitioner<IntWritable, Text> {

    @Override
    public int getPartition(IntWritable key, Text value, int numPartitions) {
      return key.get();
    }
    @Override
    public void configure(JobConf job) {
    } 
  }


  public static class MultiFileMergeInputFormat extends org.apache.hadoop.mapreduce.InputFormat<IntWritable, Text> implements InputFormat<IntWritable,Text>{


    public MultiFileMergeInputFormat() { }


    @Override
    public RecordReader<IntWritable, Text> getRecordReader(org.apache.hadoop.mapred.InputSplit split, JobConf job,Reporter reporter) throws IOException {

      // ok step 1. get input paths 
      Path paths[] = FileInputFormat.getInputPaths(job);
      if (paths.length == 0) { 
        throw new IOException("No Input Paths Specified!");
      }
      else {

        FileSystem fs = FileSystem.get(job);

        // get job affinity mask 
        String nodeAffinityMask = NodeAffinityMaskBuilder.getNodeAffinityMask(job);

        if (nodeAffinityMask != null) { 
          // ok build a mapping 
          Map<Integer,String> rootAffinityMap = NodeAffinityMaskBuilder.parseAffinityMask(nodeAffinityMask);
          // ok validate that all input paths have a matching mapping 
          for (int i=0;i<paths.length;++i) { 

            Path nextPath = paths[i];

            String nextPathAffinityMask = NodeAffinityMaskBuilder.buildNodeAffinityMask(fs,nextPath,rootAffinityMap);

            if (nodeAffinityMask.compareTo(nextPathAffinityMask) != 0) { 
              LOG.error("Input Path:" + paths[i] + " had an incompatible NodeAffinityMask with root path.");
              LOG.error("Root Path:" + paths[0]);
              LOG.error("Root Mask:" + nodeAffinityMask);
              LOG.error("Current Mask:" + nextPathAffinityMask);
              // throw new IOException("NodeAffinity Masks Mismatch!");
            }
          }
        }

        // ok build an array of all possible paths
        final ArrayList<Path> pathList = new ArrayList<Path>();

        for (Path path : paths) { 
          FileSystem pathFS = FileSystem.get(path.toUri(),job);
          FileStatus parts[] = pathFS.globStatus(new Path(path,"part-*"));

          for (FileStatus part : parts) { 
            pathList.add(part.getPath());
          }
        }

        // ok good to go ... create input format ... 
        return new RecordReader<IntWritable,Text>() {

          private int position = 0;

          public boolean next(IntWritable keyOut,Text valueOut) throws IOException {
            if (position < pathList.size()) { 

              Path path = pathList.get(position++);
              String name = path.getName();
              int partitionNumber;
              try {
                if (name.startsWith("part-r")) { 
                  partitionNumber = NUMBER_FORMAT.parse(name.substring("part-r-".length())).intValue();
                }
                else { 
                  partitionNumber = NUMBER_FORMAT.parse(name.substring("part-".length())).intValue();
                }
              } catch (ParseException e) {
                throw new IOException("Invalid Part Name Encountered:" + name);
              }

              keyOut.set(partitionNumber);
              valueOut.set(path.toString());

              return true;
            }
            else { 
              return false;
            }
          }
          public IntWritable createKey() { return new IntWritable(); }
          public Text createValue() { return new Text(); }
          public long getPos() throws IOException { return 0L; }
          public void close() throws IOException { }
          public float getProgress() throws IOException { return 0.0f; }
        };
      }
    }


    @Override
    public InputSplit[] getSplits(JobConf job,int numSplits) throws IOException {
      MultiFileMergeSplit split = new MultiFileMergeSplit();
      InputSplit[] array = { split };
      return array;
    }


    @Override
    public org.apache.hadoop.mapreduce.RecordReader<IntWritable, Text> createRecordReader(
        org.apache.hadoop.mapreduce.InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
      // ok step 1. get input paths 
      Path paths[] = org.apache.hadoop.mapreduce.lib.input.FileInputFormat.getInputPaths(context);
      if (paths.length == 0) { 
        throw new IOException("No Input Paths Specified!");
      }
      else {

        FileSystem fs = FileSystem.get(context.getConfiguration());

        // get job affinity mask 
        String nodeAffinityMask = NodeAffinityMaskBuilder.getNodeAffinityMask(context.getConfiguration());
        // ok build a mapping 
        Map<Integer,String> rootAffinityMap = NodeAffinityMaskBuilder.parseAffinityMask(nodeAffinityMask);
        // ok validate that all input paths have a matching mapping 
        for (int i=0;i<paths.length;++i) { 

          Path nextPath = paths[i];

          String nextPathAffinityMask = NodeAffinityMaskBuilder.buildNodeAffinityMask(fs,nextPath,rootAffinityMap);

          if (nodeAffinityMask.compareTo(nextPathAffinityMask) != 0) { 
            LOG.error("Input Path:" + paths[i] + " had an incompatible NodeAffinityMask with root path.");
            LOG.error("Root Path:" + paths[0]);
            LOG.error("Root Mask:" + nodeAffinityMask);
            LOG.error("Current Mask:" + nextPathAffinityMask);
            // throw new IOException("NodeAffinity Masks Mismatch!");
          }

        }

        // ok build an array of all possible paths
        final ArrayList<Path> pathList = new ArrayList<Path>();

        for (Path path : paths) { 
          FileStatus parts[] = fs.globStatus(new Path(path,"part-*"));
          for (FileStatus part : parts) { 
            pathList.add(part.getPath());
          }
        }

        // ok good to go ... create input format ... 
        return new org.apache.hadoop.mapreduce.RecordReader<IntWritable, Text>() {

          private int position = 0;
          IntWritable keyOut = new IntWritable();
          Text valueOut = new Text();


          @Override
          public IntWritable getCurrentKey() throws IOException,
          InterruptedException {
            return keyOut;
          }
          @Override
          public Text getCurrentValue() throws IOException,
          InterruptedException {
            return valueOut;
          }
          @Override
          public void initialize(org.apache.hadoop.mapreduce.InputSplit split,
              TaskAttemptContext context) throws IOException,
              InterruptedException {
            // TODO Auto-generated method stub

          }
          @Override
          public boolean nextKeyValue() throws IOException,InterruptedException {
            if (position < pathList.size()) { 

              Path path = pathList.get(position++);
              String name = path.getName();
              int partitionNumber;
              try {
                if (name.startsWith("part-r-")) { 
                  partitionNumber = NUMBER_FORMAT.parse(name.substring("part-r-".length())).intValue();
                }
                else { 
                  partitionNumber = NUMBER_FORMAT.parse(name.substring("part-".length())).intValue();
                }
              } catch (ParseException e) {
                throw new IOException("Invalid Part Name Encountered:" + name);
              }

              keyOut.set(partitionNumber);
              valueOut.set(path.toString());

              return true;
            }
            else { 
              return false;
            }
          }

          @Override
          public void close() throws IOException {

          }

          @Override
          public float getProgress() throws IOException, InterruptedException {
            return 0;
          }
        };
      }
    }


    @Override
    public List<org.apache.hadoop.mapreduce.InputSplit> getSplits(
        JobContext context) throws IOException, InterruptedException {
      MultiFileMergeSplit split = new MultiFileMergeSplit();
      ArrayList<org.apache.hadoop.mapreduce.InputSplit> list = new ArrayList<org.apache.hadoop.mapreduce.InputSplit>();
      list.add(split);
      return list;
    }
  }




  public static class MultiFileInputReader<KeyClassType extends Writable> {

    public static final String MULTIFILE_COMPARATOR_CLASS = "mutlifile.compaarator.class";
    public static final String MULTIFILE_KEY_CLASS 				= "mutlifile.key.class";

    @SuppressWarnings("rawtypes")
    PriorityQueue<InputSource> _inputs = new PriorityQueue<InputSource>(1); 
    
    
    Configuration _conf;
    @SuppressWarnings("rawtypes")
    Comparator _comparator;
    RawComparator<Writable> _rawComparator;
    @SuppressWarnings("rawtypes")
    Class _keyClass;
    KeyClassType _keyObject;
    KeyAndValueData<KeyClassType> _keyAndValueData = new KeyAndValueData<KeyClassType>();
    DataInputBuffer _keyObjectReader = new DataInputBuffer();

    public static void setKeyClass(Configuration conf,Class<? extends Writable> theClass) { 
      conf.setClass(MULTIFILE_KEY_CLASS, theClass, Writable.class);
    }

    public static void setComparatorClass(Configuration conf,Class<? extends RawComparator<Writable>> theClass) { 
      conf.setClass(MULTIFILE_COMPARATOR_CLASS, theClass, Comparator.class);
    }

    public static class RawRecordValue {

      public Path source;

      public DataOutputBuffer key  = new DataOutputBuffer();
      public DataOutputBuffer data = new DataOutputBuffer();
    }

    public static class KeyAndValueData<KeyClassType> { 
      public KeyClassType _keyObject;
      public DataOutputBuffer _keyData;
      public ArrayList<RawRecordValue> _values = new ArrayList<RawRecordValue>();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public MultiFileInputReader(FileSystem fs,List<Path> inputPaths,Configuration conf) throws IOException { 

      _conf = conf;
      Class comparatorClass = conf.getClass(MULTIFILE_COMPARATOR_CLASS, null); 

      LOG.info("Constructing comparator of type:" + comparatorClass.getName());
      _comparator = (Comparator) ReflectionUtils.newInstance(comparatorClass, conf);
      LOG.info("Construced comparator of type:" + comparatorClass.getName());

      if (_comparator instanceof RawComparator) { 
        _rawComparator = (RawComparator) _comparator;
        LOG.info("Comparator implements RawComparator");
      }

      _keyClass = conf.getClass(MULTIFILE_KEY_CLASS, null);
      LOG.info("Constructing Key Object of Type:" + _keyClass.getName());
      _keyObject = (KeyClassType)ReflectionUtils.newInstance(_keyClass, conf); 
      LOG.info("Constructed Key Object Of Type:" + _keyClass.getName());

      for (Path path : inputPaths) {
        LOG.info("Adding Stream at Path:" + path);
        InputSource inputSource = new InputSource(fs, _conf, path,(_rawComparator == null) ? _keyClass : null);
        
        // advance to first item 
        if (inputSource.next() == false) {
          LOG.error("Stream At Path:" + path  + " contains zero entries!");
          inputSource.close();
        }
        else {
          LOG.info("Stream at Path:" + path + " is VALID");
          _inputs.add(inputSource);
        }
      }
      LOG.info("Finished With Initial Sort");
    }

    public class RawValueIterator implements Iterator<RawRecordValue>, Iterable<RawRecordValue> {
      int _streamIdx = 0;
      RawRecordValue _currentValue = null;

      RawValueIterator(RawRecordValue initialValue) { 
        _currentValue = initialValue;
      }

      @Override
      public boolean hasNext() {
        if (_currentValue == null) { 
          // peek at the top most item in the queue
          InputSource nextSource = _inputs.peek();
          if (nextSource != null) { 
            // ok now compare against next item to see if there is a match 
            int result = (_rawComparator != null) ? 
                _rawComparator.compare(_keyAndValueData._keyData.getData(),0,_keyAndValueData._keyData.getLength(),
                    nextSource._keyData.getData(),0,nextSource._keyData.getLength())
                    :
                      _comparator.compare(_keyAndValueData._keyObject,nextSource._keyObject);
                
            if (result == 0) { 
              // save the current value ... 
              _currentValue = nextSource._value;
              // pop source ... 
              _inputs.remove();
              // advance it (potentially) 
              try  {
                if (!nextSource.next()) {
                  // if no more data .. gracefully close the source ... 
                  nextSource.close();
                }
                // otherwise resinsert in priority queue 
                else { 
                  _inputs.add(nextSource);
                }
              }
              catch (IOException e) { 
                LOG.error(CCStringUtils.stringifyException(e));
                try { 
                  nextSource.close();
                }
                catch (Exception e2) { 
                  
                }
                return false;
              }
            }
          }
        }
        return _currentValue != null;
      }

      @Override
      public RawRecordValue next() {
        RawRecordValue temp = _currentValue;
        _currentValue = null;
        return temp;
      }

      @Override
      public void remove() {
        // NOOP 
      }

      @Override
      public Iterator<RawRecordValue> iterator() {
        return this;
      }
    }

    public Pair<KeyAndValueData<KeyClassType>,Iterable<RawRecordValue>> getNextItemIterator() throws IOException { 

      int newValidStreamCount=0;

      // pop the top most item in the queue
      InputSource nextSource = _inputs.poll();
      

      // if data available ... 
      if (nextSource != null) { 
        
        try { 
          //reset value array 
          _keyAndValueData._values.clear();
  
          // set key object ref if source has a key object ...  
          if (nextSource._keyObject != null) { 
            _keyAndValueData._keyObject =(KeyClassType) nextSource.detachKeyObject(); 
          }
          // otherwise ... deserialize from raw ... 
          else { 
            _keyObjectReader.reset(nextSource._keyData.getData(), nextSource._keyData.getLength());
            _keyObject.readFields(_keyObjectReader);
            _keyAndValueData._keyObject = (KeyClassType)_keyObject;
          }
          // and also grab key data from first input source
          _keyAndValueData._keyData = nextSource.detachKeyData();
  
          //LOG.info("readNextTarget - target is:" + target.target.getDomainHash() + ":" + target.target.getUrlHash());
          //LOG.info("readNextTarget - source is:" + _inputs[0].last().source.getDomainHash() + ":" + _inputs[0].last().source.getUrlHash());
  
          // save the initial value 
          RawRecordValue initialValue = nextSource._value;
  
          // add the first item to the list 
          _keyAndValueData._values.add(initialValue);
          
          //LOG.info("Using Input:" + _inputs[0]._path + " as primary key");

          // advance input zero 
          if (!nextSource.next()) {
            // if no more data .. gracefully close the source ... 
            nextSource.close();
          }
          // otherwise resinsert in priority queue 
          else { 
            _inputs.add(nextSource);
          }

          // return tuple ... 
          return new Pair<KeyAndValueData<KeyClassType>, Iterable<RawRecordValue>>(_keyAndValueData,new RawValueIterator(initialValue));
        }
        catch (IOException e) { 
          LOG.error(CCStringUtils.stringifyException(e));
          
          try {
            nextSource.close(); 
          }
          catch (Exception e2) { 
            
          }
          
          throw e;
        }
      }
      return null;
    }

    
    // collect next valid target and all related sources 
    public KeyAndValueData<KeyClassType> readNextItem() throws IOException {

      // pop the top most item in the queue
      InputSource nextSource = _inputs.poll();
      
      if (nextSource != null) { 
        try { 
          //reset value array 
          _keyAndValueData._values.clear();
  
          // set key object ref if source has a key object ...  
          if (nextSource._keyObject != null) { 
            _keyAndValueData._keyObject =(KeyClassType) nextSource.detachKeyObject(); 
          }
          // otherwise ... deserialize from raw ... 
          else { 
            _keyObjectReader.reset(nextSource._keyData.getData(), nextSource._keyData.getLength());
            _keyObject.readFields(_keyObjectReader);
            _keyAndValueData._keyObject = (KeyClassType)_keyObject;
          }
          // and also grab key data from first input source
          _keyAndValueData._keyData = nextSource.detachKeyData();

          //LOG.info("readNextTarget - target is:" + target.target.getDomainHash() + ":" + target.target.getUrlHash());
          //LOG.info("readNextTarget - source is:" + _inputs[0].last().source.getDomainHash() + ":" + _inputs[0].last().source.getUrlHash());

          // save the initial value 
          RawRecordValue initialValue = nextSource._value;

          //LOG.info("Using Input:" + _inputs[0]._path + " as primary key");
          // advance input zero 
          if (!nextSource.next()) {
            // if no more data .. gracefully close the source ... 
            nextSource.close();
          }
          // otherwise resinsert in priority queue 
          else { 
            _inputs.add(nextSource);
          }
          nextSource = null;
          // create an interator ... 
          RawValueIterator iterator = new RawValueIterator(initialValue);
          // iterator ...
          while (iterator.hasNext()) { 
            // add the first item to the list 
            _keyAndValueData._values.add(iterator.next());
          }
        }
        catch (IOException e) {
          LOG.error(CCStringUtils.stringifyException(e));
          if (nextSource != null) { 
            try {
              nextSource.close();
            }
            catch (Exception e2) { 
            }
          }
          throw e;
        }
        return _keyAndValueData;
      }
      else { 
        return null;
      }
    }

    public void close() {
      InputSource nextSource = null;
      while ((nextSource = _inputs.poll()) != null) { 
        try { 
          nextSource.close();
        }
        catch (Exception e) { 
          LOG.error(CCStringUtils.stringifyException(e));
        }
      }
    }

    public class InputSource<KeyClassType extends Writable> implements Comparable<InputSource> {

      boolean 					  eos = false;
      Path 								_path;
      SequenceFile.Reader _reader;
      DataOutputBuffer    _keyData = null;
      DataInputBuffer 	  _keyDataReader = new DataInputBuffer();
      Class				  _keyClass;
      private KeyClassType			  _keyObject;
      RawRecordValue 	  _value;
      ValueBytes       	  _valueBytes = null;

      public InputSource(FileSystem fs,Configuration conf,Path inputPath,Class optKeyClass) throws IOException { 
        _path = inputPath;
        _reader = new SequenceFile.Reader(fs,_path,conf);
        _valueBytes = _reader.createValueBytes();
        _keyClass = optKeyClass;
        if (_keyClass != null) { 
          _keyObject = (KeyClassType)ReflectionUtils.newInstance(_keyClass, conf);
        }
      }

      public void close() {
        if (_reader != null){ 
          try { 
            _reader.close();
          }
          catch (IOException e) { 
            LOG.error((CCStringUtils.stringifyException(e)));
          }
        }
      }


      public boolean next()throws IOException { 
        if (!eos) {
          _value = new RawRecordValue();
          _keyData = _value.key;
          _value.source = _path;

          eos = (_reader.nextRawKey(_keyData) == -1);
          if (!eos) { 
            if (_reader.nextRawValue(_valueBytes) != 0) { 
              _valueBytes.writeUncompressedBytes(_value.data);
            }
            // now if key object is present ... 
            if (_keyObject != null) { 
              _keyDataReader.reset(_keyData.getData(), _keyData.getLength());
              _keyObject.readFields(_keyDataReader);
            }
          }
        }
        return !eos;
      }

      public boolean isValid() { 
        return !eos;
      }

      public KeyClassType detachKeyObject() { 
        KeyClassType keyObjectOut = _keyObject;
        _keyObject = (KeyClassType)ReflectionUtils.newInstance(_keyClass, _conf);
        return keyObjectOut;
      }
      
      public DataOutputBuffer detachKeyData() { 
        DataOutputBuffer temp = _keyData;
        _keyData = new DataOutputBuffer();
        return temp;
      }

      @SuppressWarnings({ "rawtypes", "unchecked" })
      @Override
      public int compareTo(InputSource other) {
        int result;
        if (_rawComparator != null) { 
          result = _rawComparator.compare(
              this._keyData.getData(),0,this._keyData.getLength(),
              other._keyData.getData(),0,other._keyData.getLength());
        }
        else { 
          result = _comparator.compare(this._keyObject, other._keyObject);
        }
        return result;
      }
    }

  }

  // Test data: The first item in the tuple is the key
  // the second is a unique sequence number

  // There are two data sets. They should be sorted, and 
  // each item should have a unique sequence number. 

  // We will write the two datasets to Seq files and then 
  // run them through the merger. Then we will verify that 
  // the keys are still in sequence, and that we got the right 
  // number of unique tuples per unique key.

  ImmutableList<Pair<Integer,Integer>> dataSet1 
  = new ImmutableList.Builder<Pair<Integer,Integer>>()

  .add(new Pair<Integer,Integer>(1,1))
  .add(new Pair<Integer,Integer>(1,2))
  .add(new Pair<Integer,Integer>(2,3))
  .add(new Pair<Integer,Integer>(2,4))
  .add(new Pair<Integer,Integer>(2,5))
  .add(new Pair<Integer,Integer>(4,6))
  .add(new Pair<Integer,Integer>(4,7))

  .build();  

  ImmutableList<Pair<Integer,Integer>> dataSet2 
  = new ImmutableList.Builder<Pair<Integer,Integer>>()

  .add(new Pair<Integer,Integer>(1,8))
  .add(new Pair<Integer,Integer>(2,9))
  .add(new Pair<Integer,Integer>(3,10))
  .add(new Pair<Integer,Integer>(3,11))
  .add(new Pair<Integer,Integer>(4,12))
  .add(new Pair<Integer,Integer>(5,13))

  .build();  

  static void writeTestFile(FileSystem fs,Configuration conf,Path fileName,ImmutableList<Pair<Integer,Integer>> dataSet)throws IOException { 
    SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, fileName, IntWritable.class, IntWritable.class,CompressionType.NONE);
    for (Pair<Integer,Integer> datum : dataSet) { 
      writer.append(new IntWritable(datum.e0), new IntWritable(datum.e1));
    }
    writer.close();
  }

  public static class NonRawComparator implements Comparator<IntWritable> {

    @Override
    public int compare(IntWritable arg0, IntWritable arg1) {
      int thisValue = arg0.get();
      int thatValue = arg1.get();
      
      return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
    } 
    
  }
  
  @Test
  public void testMerge() throws Exception {

    Configuration conf = new Configuration();
    FileSystem fsMock = mock(FileSystem.class);
    final DataOutputBuffer stream1Buffer = new DataOutputBuffer();
    final DataOutputBuffer stream2Buffer = new DataOutputBuffer();
    final DataOutputBuffer stream3Buffer = new DataOutputBuffer();
    // the two mock input paths 
    Path mockPath1 = new Path("/mockPath1");
    Path mockPath2 = new Path("/mockPath2");
    // the mock output path ... 
    Path outputPath = new Path("/outputPath");
    // hook file system 
    when(fsMock.create(eq(mockPath1),anyBoolean(),anyInt(),anyShort(),anyLong(),(Progressable)any())).thenReturn(new FSDataOutputStream(stream1Buffer,null));
    when(fsMock.create(eq(mockPath2),anyBoolean(),anyInt(),anyShort(),anyLong(),(Progressable)any())).thenReturn(new FSDataOutputStream(stream2Buffer,null));
    when(fsMock.create(eq(outputPath),anyBoolean(),anyInt(),anyShort(),anyLong(),(Progressable)any())).thenReturn(new FSDataOutputStream(stream3Buffer,null));
    when(fsMock.getConf()).thenReturn(conf);
    // write test data in seq file format 
    writeTestFile(fsMock, conf, mockPath1, dataSet1);
    writeTestFile(fsMock, conf, mockPath2, dataSet2);
    // hook fs to return the mocked seq files 
    when(fsMock.getLength(eq(mockPath1))).thenReturn((long)stream1Buffer.getLength());
    when(fsMock.getLength(eq(mockPath2))).thenReturn((long)stream2Buffer.getLength());
    
    final List<FSDataInputStream> allocatedStreams = Lists.newArrayList();
    
    // spy on input stream to verify close 
    when(fsMock.open(eq(mockPath1),anyInt())).thenAnswer(new Answer<FSDataInputStream>() {

      @Override
      public FSDataInputStream answer(InvocationOnMock invocation)throws Throwable {
        FSDataInputStream stream = new FSDataInputStream(
            new FSByteBufferInputStream(ByteBuffer.wrap(stream1Buffer.getData(),0,stream1Buffer.getLength())));
        
        stream = spy(stream);
        allocatedStreams.add(stream);
        return stream;
      }
    });
    
    when(fsMock.open(eq(mockPath2),anyInt())).thenAnswer(new Answer<FSDataInputStream>() {

      @Override
      public FSDataInputStream answer(InvocationOnMock invocation)throws Throwable {
        FSDataInputStream stream = new FSDataInputStream(
            new FSByteBufferInputStream(ByteBuffer.wrap(stream2Buffer.getData(),0,stream2Buffer.getLength())));
        
        stream = spy(stream);
        allocatedStreams.add(stream);
        return stream;
      }
    });
      
    // two passes... one using raw comparator, one using simple Comparator
    // (code paths for two scenarios are different, so we need to validate both)
    for (int pass=0;pass<2;++pass) { 

      // setup merger ... 
      if (pass == 0) { 
        System.out.println("******Using Raw Comparator");
        conf.setClass(MultiFileInputReader.MULTIFILE_COMPARATOR_CLASS, IntWritable.Comparator.class,Comparator.class);
      }
      else { 
        System.out.println("******Using Non-Raw Comparator");
        conf.setClass(MultiFileInputReader.MULTIFILE_COMPARATOR_CLASS, NonRawComparator.class,Comparator.class);
      }
      conf.setClass(MultiFileInputReader.MULTIFILE_KEY_CLASS,IntWritable.class,WritableComparable.class);
      
      // instantiate merger... 
      MultiFileInputReader inputReader = new MultiFileInputReader<Writable>(
          fsMock, 
          new ImmutableList.Builder<Path>().add(mockPath1).add(mockPath2).build(),
          conf);
  
      // create multimap to merged data 
      TreeMultimap<Integer, Integer> mergedDataMap = TreeMultimap.create();
      // iterate 
      KeyAndValueData<IntWritable> kvData;
      DataInputBuffer inputBuffer = new DataInputBuffer();
      IntWritable valueTemp = new IntWritable();
      int lastKey = Integer.MIN_VALUE;
      while ((kvData= inputReader.readNextItem()) != null) {
        // check to keys are coming in order ...  
        Assert.assertTrue(kvData._keyObject.get() > lastKey);
  
        System.out.println("key:"+ kvData._keyObject.get());
        // iterate all merged values for specified key ... 
        for (RawRecordValue value : kvData._values) { 
          inputBuffer.reset(value.data.getData(),value.data.getLength());
          valueTemp.readFields(inputBuffer);
          // collect sequence numbers by key  
          mergedDataMap.put(kvData._keyObject.get(), valueTemp.get());
          System.out.println("value:"+ valueTemp);
        }
      }
      
      // close merger ... 
      inputReader.close();
      
      // validate all input streams were closed ... 
      for (FSDataInputStream allocatedStream : allocatedStreams) { 
        verify(allocatedStream).close();
      }
      
      // create a source data map ... 
      TreeMultimap<Integer, Integer> sourceDataMap = TreeMultimap.create();
      // populate it ... 
      for (Pair<Integer,Integer> tuple : dataSet1)
        sourceDataMap.put(tuple.e0, tuple.e1);
      for (Pair<Integer,Integer> tuple : dataSet2)
        sourceDataMap.put(tuple.e0, tuple.e1);
      // validate source and dest maps equate
      if (!sourceDataMap.equals(mergedDataMap)) { 
        System.out.println("Source And Dest Maps Mismatched!");
        System.out.println("Source Map:" + sourceDataMap);
        System.out.println("Dest Map:" + mergedDataMap);
        Assert.assertTrue(sourceDataMap.equals(mergedDataMap));
      }
    }
  }
}
