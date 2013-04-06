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
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;
import java.util.zip.CRC32;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.SequenceFile.ValueBytes;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader.KeyAndValueData;
import org.commoncrawl.util.Tuples.Pair;
import org.commoncrawl.util.URLUtils.URLFPV2RawComparator;


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

    @SuppressWarnings("unchecked")
    InputSource _inputs[] = null;
    int _validStreams = 0;
    Configuration _conf;
    @SuppressWarnings("unchecked")
    Comparator _comparator;
    RawComparator<Writable> _rawComparator;
    @SuppressWarnings("unchecked")
    Class _keyClass;
    KeyClassType _keyObject;
    KeyAndValueData<KeyClassType> _keyAndValueData = new KeyAndValueData<KeyClassType>();
    DataInputBuffer _keyObjectReader = new DataInputBuffer();

    public static void setKeyClass(Configuration conf,Class<? extends Writable> theClass) { 
      conf.setClass(MULTIFILE_KEY_CLASS, theClass, Writable.class);
    }

    @SuppressWarnings("unchecked")
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

    public MultiFileInputReader(FileSystem fs,Vector<Path> inputPaths,Configuration conf) throws IOException { 

      _conf = conf;
      _inputs = new InputSource[inputPaths.size()];  		
      Class comparatorClass = conf.getClass(MULTIFILE_COMPARATOR_CLASS, null); 

      LOG.info("Constructing comparator of type:" + comparatorClass.getName());
      _comparator = (RawComparator<Writable>)ReflectionUtils.newInstance(comparatorClass, conf);
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
        _inputs[_validStreams] = new InputSource(fs, _conf, path,(_rawComparator == null) ? _keyClass : null);
        // advance to first item 
        if (_inputs[_validStreams].next() == false) {
          LOG.error("Stream At Index:" + _validStreams + " contains zero entries!");
          _inputs[_validStreams].close();
        }
        else {
          LOG.info("Stream at Path:" + path + " is VALID");
          _validStreams++;
        }
      }
      // lastly sort streams
      LOG.info("Doing Initial Sort on Streams");
      sortStreams();
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
        if (_currentValue != null) { 
          return true;
        }
        else { 
          // ok enter a loop and collect all sources for current target ... 
          for (;_streamIdx<_validStreams;) { 
            if (!_inputs[_streamIdx].isValid()) { 
              // skip invalid streams 
              _streamIdx++;
            }
            else {
              // ok now compare against next item to see if there is a match 
              int result = (_rawComparator != null) ? 
                  _rawComparator.compare(_keyAndValueData._keyData.getData(),0,_keyAndValueData._keyData.getLength(),
                      _inputs[_streamIdx]._keyData.getData(),0,_inputs[_streamIdx]._keyData.getLength())
                      :
                        _comparator.compare(_keyAndValueData._keyObject,_inputs[_streamIdx]._keyObject);

              if (result != 0) {
                //LOG.info("Input:" + _inputs[streamIdx]._path + " did not match. skipping");
                // advance to next stream - values don't match 
                _streamIdx++;
              }
              else { 

                //LOG.info("Input:" + _inputs[streamIdx]._path + " did match. adding. previous count:" + _keyAndValueData._values.size());
                // grab this value  
                _currentValue = _inputs[_streamIdx]._value;

                // advance current stream ... 
                try {
                  _inputs[_streamIdx].next();
                  return true;
                } catch (IOException e) {
                  LOG.error(CCStringUtils.stringifyException(e));
                  throw new RuntimeException(e);
                }
              }
            }            
          }
        }
        
        return false;
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
      
      for (int currStreamIdx=0;currStreamIdx<_validStreams;++currStreamIdx) { 
        if (_inputs[currStreamIdx].isValid()) { 
          _inputs[newValidStreamCount++] = _inputs[currStreamIdx];
        }
        else {
          // close the stream ... 
          _inputs[currStreamIdx].close();
          // null it out ... 
          _inputs[currStreamIdx] = null;
        }
      }
      // resset valid stream count 
      _validStreams = newValidStreamCount;

      // ok now sort streams ...
      if (_validStreams != 0) {
        //LOG.info("Resorting Streams");
        sortStreams();
      }
      
      if (_validStreams != 0) { 
        //reset value array 
        _keyAndValueData._values.clear();

        // get key object if available ... 
        if (_inputs[0]._keyObject != null) { 
          _keyAndValueData._keyObject =(KeyClassType) _inputs[0]._keyObject; 
        }
        else { 
          _keyObjectReader.reset(_inputs[0]._keyData.getData(), _inputs[0]._keyData.getLength());
          _keyObject.readFields(_keyObjectReader);
          _keyAndValueData._keyObject = (KeyClassType)_keyObject;
        }
        // and also grab key data from first input source
        _keyAndValueData._keyData = _inputs[0].detachKeyData();

        //LOG.info("readNextTarget - target is:" + target.target.getDomainHash() + ":" + target.target.getUrlHash());
        //LOG.info("readNextTarget - source is:" + _inputs[0].last().source.getDomainHash() + ":" + _inputs[0].last().source.getUrlHash());

        // save the initial value 
        RawRecordValue initialValue = _inputs[0]._value;
        
        // add the first item to the list 
        _keyAndValueData._values.add(_inputs[0]._value);

        //              /LOG.info("Using Input:" + _inputs[0]._path + " as primary key");

        // advance input zero 
        _inputs[0].next();
        
        return new Pair<KeyAndValueData<KeyClassType>, Iterable<RawRecordValue>>(_keyAndValueData,new RawValueIterator(initialValue));
      }
      return null;
    }
    
    // collect next valid target and all related sources 
    public KeyAndValueData<KeyClassType> readNextItem() throws IOException {

      if (_validStreams != 0) { 
        //reset value array 
        _keyAndValueData._values.clear();

        // get key object if available ... 
        if (_inputs[0]._keyObject != null) { 
          _keyAndValueData._keyObject =(KeyClassType) _inputs[0]._keyObject; 
        }
        else { 
          _keyObjectReader.reset(_inputs[0]._keyData.getData(), _inputs[0]._keyData.getLength());
          _keyObject.readFields(_keyObjectReader);
          _keyAndValueData._keyObject = (KeyClassType)_keyObject;
        }
        // and also grab key data from first input source
        _keyAndValueData._keyData = _inputs[0].detachKeyData();

        //LOG.info("readNextTarget - target is:" + target.target.getDomainHash() + ":" + target.target.getUrlHash());
        //LOG.info("readNextTarget - source is:" + _inputs[0].last().source.getDomainHash() + ":" + _inputs[0].last().source.getUrlHash());

        // add the first item to the list 
        _keyAndValueData._values.add(_inputs[0]._value);

        //  			/LOG.info("Using Input:" + _inputs[0]._path + " as primary key");

        // advance input zero 
        _inputs[0].next();

        // ok enter a loop and collect all sources for current target ... 
        for (int streamIdx=0;streamIdx<_validStreams;) { 
          if (!_inputs[streamIdx].isValid()) { 
            // skip invalid streams 
            streamIdx++;
          }
          else {
            // ok now compare against next item to see if there is a match 
            int result = (_rawComparator != null) ? 
                _rawComparator.compare(_keyAndValueData._keyData.getData(),0,_keyAndValueData._keyData.getLength(),
                    _inputs[streamIdx]._keyData.getData(),0,_inputs[streamIdx]._keyData.getLength())
                    :
                      _comparator.compare(_keyAndValueData._keyObject,_inputs[streamIdx]._keyObject);

                if (result != 0) {
                  //LOG.info("Input:" + _inputs[streamIdx]._path + " did not match. skipping");
                  // advance to next stream - values don't match 
                  streamIdx++;
                }
                else { 

                  //LOG.info("Input:" + _inputs[streamIdx]._path + " did match. adding. previous count:" + _keyAndValueData._values.size());
                  // grab this value  
                  _keyAndValueData._values.add(_inputs[streamIdx]._value);

                  // advance current stream ... 
                  _inputs[streamIdx].next();
                }
          }
        }

        //LOG.info("Consolidating Streams");
        // ok now collect remaining valid streams 
        int newValidStreamCount=0;
        for (int currStreamIdx=0;currStreamIdx<_validStreams;++currStreamIdx) { 
          if (_inputs[currStreamIdx].isValid()) { 
            _inputs[newValidStreamCount++] = _inputs[currStreamIdx];
          }
          else {
            // close the stream ... 
            _inputs[currStreamIdx].close();
            // null it out ... 
            _inputs[currStreamIdx] = null;
          }
        }
        // resset valid stream count 
        _validStreams = newValidStreamCount;

        // ok now sort streams ...
        if (_validStreams != 0) {
          //LOG.info("Resorting Streams");
          sortStreams();
        }

        return _keyAndValueData;
      }
      else { 
        return null;
      }
    }

    public void close() { 
      for (int i=0;i<_validStreams;++i) { 
        _inputs[i].close();
        _inputs[i] = null;
      }
      _validStreams = 0;
    }

    private final void sortStreams() { 

      if (_rawComparator != null) { 
        Arrays.sort(_inputs,0,_validStreams,new Comparator<InputSource>() {

          @Override
          public int compare(InputSource o1, InputSource o2) {
            return _rawComparator.compare(o1._keyData.getData(), 0, o1._keyData.getLength(), o2._keyData.getData(), 0, o2._keyData.getLength());
          }
        });
      }
      else { 
        Arrays.sort(_inputs,0,_validStreams,new Comparator<InputSource>() {

          @Override
          public int compare(InputSource o1, InputSource o2) {
            return _comparator.compare(o1._keyObject, o2._keyObject);
          }
        });

      }
    }

    public static class InputSource<KeyClassType extends Writable> {

      boolean 					  eos = false;
      Path 								_path;
      SequenceFile.Reader _reader;
      DataOutputBuffer    _keyData = null;
      DataInputBuffer 	  _keyDataReader = new DataInputBuffer();
      Class				  _keyClass;
      Writable			  _keyObject;
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
          }
          // now if key object is present ... 
          if (_keyObject != null) { 
            _keyDataReader.reset(_keyData.getData(), _keyData.getLength());
            _keyObject.readFields(_keyDataReader);
          }
        }
        return !eos;
      }

      public boolean isValid() { 
        return !eos;
      }

      public DataOutputBuffer detachKeyData() { 
        DataOutputBuffer temp = _keyData;
        _keyData = new DataOutputBuffer();
        return temp;
      }
    }

  }


  public static void main(String[] args) {

    Path testPath = new Path(args[0]);

    LOG.info("Initializing Hadoop Config");

    Configuration conf = new Configuration();

    conf.addResource("nutch-default.xml");
    conf.addResource("nutch-site.xml");
    conf.addResource("mapred-site.xml");
    conf.addResource("hdfs-site.xml");
    conf.addResource("commoncrawl-default.xml");
    conf.addResource("commoncrawl-site.xml");

    conf.setClass(MultiFileInputReader.MULTIFILE_COMPARATOR_CLASS,URLFPV2RawComparator.class,RawComparator.class);
    conf.setClass(MultiFileInputReader.MULTIFILE_KEY_CLASS,URLFPV2.class,WritableComparable.class);

    CrawlEnvironment.setHadoopConfig(conf);
    CrawlEnvironment.setDefaultHadoopFSURI("hdfs://ccn01:9000/");

    try { 
      FileSystem fs = CrawlEnvironment.getDefaultFileSystem();


      Vector<Path> paths = new Vector<Path>();

      paths.add(new Path(testPath,"part-00000"));
      // paths.add(new Path(testPath,"part-00000"));
      paths.add(new Path(testPath,"part-00001"));

      TreeSet<URLFPV2> directReadSet = new TreeSet<URLFPV2>();
      TreeSet<URLFPV2> multiFileReadSet = new TreeSet<URLFPV2>();

      MultiFileInputReader<URLFPV2> inputReader = new MultiFileInputReader<URLFPV2>(fs, paths, conf);

      KeyAndValueData<URLFPV2> keyValueData = null;
      int multiFileKeyCount = 0;
      while ((keyValueData = inputReader.readNextItem()) != null) { 
        LOG.info("Got Key Domain:" + keyValueData._keyObject.getDomainHash() + " URLHash:" + keyValueData._keyObject.getUrlHash() + " Item Count:" + keyValueData._values.size() 
            + " Path[0]:" + keyValueData._values.get(0).source);

        if (keyValueData._values.size() > 1) { 
          LOG.error("Got more than one item");
          for (int i=0;i<keyValueData._values.size();++i) {
            CRC32 crc = new CRC32();
            crc.update(keyValueData._keyData.getData(),0,keyValueData._keyData.getLength());
            LOG.error("Item at[" + i + "] Path:" + keyValueData._values.get(i).source + " CRC:" + crc.getValue()); 
          }
        }
        if (multiFileKeyCount++ < 1000)
          multiFileReadSet.add((URLFPV2) keyValueData._keyObject.clone());
      }
      inputReader.close();

      addFirstNFPItemsToSet(fs,new Path(testPath,"part-00000"),conf,directReadSet,1000);
      addFirstNFPItemsToSet(fs,new Path(testPath,"part-00001"),conf,directReadSet,1000);

      Iterator<URLFPV2> directReadIterator = directReadSet.iterator();
      Iterator<URLFPV2> multiFileReadIterator = multiFileReadSet.iterator();

      for (int i=0;i<1000;++i) { 
        URLFPV2 directReadFP = directReadIterator.next();
        URLFPV2 multiFileReadFP = multiFileReadIterator.next();

        if (directReadFP.compareTo(multiFileReadFP) != 0) { 
          LOG.info("Mismatch at Index:" + i);
        }
      }

    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
    } catch (CloneNotSupportedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }

  static void scanToItemThenDisplayNext(FileSystem fs,Path path,Configuration conf, URLFPV2 targetItem) throws IOException { 
    DataOutputBuffer rawKey = new DataOutputBuffer();
    DataInputBuffer  keyDataStream = new DataInputBuffer();

    SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
    ValueBytes valueBytes = reader.createValueBytes();

    int i=0;
    while (reader.nextRawKey(rawKey) != -1) { 
      URLFPV2 keyObject = new URLFPV2();
      keyDataStream.reset(rawKey.getData(),0,rawKey.getLength());
      keyObject.readFields(keyDataStream);
      rawKey.reset();
      reader.nextRawValue(valueBytes);

      if (keyObject.compareTo(targetItem) == 0) {

        reader.nextRawKey(rawKey);
        URLFPV2 nextKeyObject = new URLFPV2();
        keyDataStream.reset(rawKey.getData(),0,rawKey.getLength());
        nextKeyObject.readFields(keyDataStream);
        LOG.info("Target Domain:" + targetItem.getDomainHash() + " FP:" + targetItem.getUrlHash() + " NextDomain:" + nextKeyObject.getDomainHash() + " NextHash:" + nextKeyObject.getUrlHash());
        break;
      }
    }
    reader.close();
  }

  static void addFirstNFPItemsToSet(FileSystem fs,Path path,Configuration conf,Set<URLFPV2> outputSet,int nItems) throws IOException { 
    DataOutputBuffer rawKey = new DataOutputBuffer();
    DataInputBuffer  keyDataStream = new DataInputBuffer();

    SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
    ValueBytes valueBytes = reader.createValueBytes();

    int i=0;
    while (reader.nextRawKey(rawKey) != -1) { 
      URLFPV2 keyObject = new URLFPV2();
      keyDataStream.reset(rawKey.getData(),0,rawKey.getLength());
      keyObject.readFields(keyDataStream);
      outputSet.add(keyObject);
      rawKey.reset();
      reader.nextRawValue(valueBytes);

      if (++i == nItems) { 
        break;
      }
    }
    reader.close();
  }

}
