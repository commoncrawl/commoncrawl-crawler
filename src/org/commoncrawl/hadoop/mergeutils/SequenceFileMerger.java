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
package org.commoncrawl.hadoop.mergeutils;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.SequenceFile.ValueBytes;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;
import org.commoncrawl.hadoop.mergeutils.OptimizedKeyGeneratorAndComparator.OptimizedKey;
import org.commoncrawl.util.IntrusiveList;
import org.commoncrawl.util.IntrusiveList.IntrusiveListElement;

/**
 * merge sort a pre-sorted set of sequence files and spill them to output
 * 
 * 
 * 
 * @author rana
 * 
 * @param <KeyType>
 * @param <ValueType>
 */
public class SequenceFileMerger<KeyType extends WritableComparable, ValueType extends Writable> {

  private static class MergeResultSegment<KeyType extends Writable, ValueType extends Writable> extends
      IntrusiveListElement<MergeResultSegment<KeyType, ValueType>> {

    private static final Class[] emptyArray = new Class[] {};

    SequenceFile.Reader reader = null;
    KeyType key = null;
    ValueType value = null;
    Constructor<KeyType> keyConstructor = null;
    Constructor<ValueType> valConstructor = null;
    boolean eos = false;
    Path path;
    int index = -1;
    boolean useRawMode = false;
    DataOutputBuffer rawKeyData = null;
    DataOutputBuffer rawValueData = null;
    ValueBytes valueBytes = null;
    OptimizedKeyGeneratorAndComparator<KeyType, ValueType> _optimizedGenerator = null;
    OptimizedKey _optimizedKey = null;

    public MergeResultSegment() {
      eos = true;
    }

    public MergeResultSegment(FileSystem fileSystem, Configuration conf, Path inputFile, Class<KeyType> keyClass,
        Class<ValueType> valueClass, boolean useRawMode,
        OptimizedKeyGeneratorAndComparator<KeyType, ValueType> optionalGenerator) throws IOException {
      try {
        this.useRawMode = useRawMode;
        this._optimizedGenerator = optionalGenerator;
        if (_optimizedGenerator != null) {
          _optimizedKey = new OptimizedKey(_optimizedGenerator.getGeneratedKeyType());
        }
        this.keyConstructor = keyClass.getDeclaredConstructor(emptyArray);
        this.keyConstructor.setAccessible(true);
        this.valConstructor = valueClass.getDeclaredConstructor(emptyArray);
        this.valConstructor.setAccessible(true);
        if (useRawMode) {
          rawKeyData = new DataOutputBuffer();
          rawValueData = new DataOutputBuffer();
        }
      } catch (SecurityException e) {
        LOG.error(StringUtils.stringifyException(e));
        throw new RuntimeException(e);
      } catch (NoSuchMethodException e) {
        LOG.error(StringUtils.stringifyException(e));
        throw new RuntimeException(e);
      }
      this.path = inputFile;
      reader = new SequenceFile.Reader(fileSystem, inputFile, conf);
      if (useRawMode) {
        valueBytes = reader.createValueBytes();
      }
      index = -1;
    }

    public void close() throws IOException {
      if (reader != null)
        reader.close();
    }

    int getIndex() {
      return this.index;
    }

    public KeyType getKey() throws IOException {
      if (useRawMode) {
        throw new IOException("getKey Unsupported in RawMode");
      }
      return key;
    }

    public String getName() {
      return "Seg:" + index + "(" + path.toString() + ")";
    }

    public OptimizedKey getOptimizedKey() {
      return _optimizedKey;
    }

    public Path getPath() {
      return path;
    }

    public DataOutputBuffer getRawKeyData() {
      return rawKeyData;
    }

    public DataOutputBuffer getRawValueData() {
      return rawValueData;
    }

    public ValueType getValue() throws IOException {
      if (useRawMode) {
        throw new IOException("getValue Unsupported in RawMode");
      }
      return value;
    }

    public boolean isNullSegment() {
      return reader == null;
    }

    public boolean next() throws IOException {
      if (!eos) {
        try {
          if (!useRawMode) {
            key = keyConstructor.newInstance();
            value = valConstructor.newInstance();
          } else {
            rawKeyData.reset();
            rawValueData.reset();
          }
        } catch (Exception e) {
          LOG.error("Failed to create key or value type with Exception:" + StringUtils.stringifyException(e));
          throw new RuntimeException(e);
        }
        if (!useRawMode) {
          eos = !reader.next(key, value);
        } else {
          eos = (reader.nextRawKey(this.rawKeyData) == -1);
          if (!eos) {
            if (reader.nextRawValue(valueBytes) != 0) {
              valueBytes.writeUncompressedBytes(rawValueData);
            }

            if (!eos && _optimizedGenerator != null) {
              _optimizedKey.initFromKeyValuePair(rawKeyData.getData(), 0, rawKeyData.getLength(), rawValueData
                  .getData(), 0, rawValueData.getLength());
            }
          }
        }
      }
      return !eos;
    }

    void setIndex(int index) {
      this.index = index;
    }

  }

  public static final Log LOG = LogFactory.getLog(SequenceFileMerger.class);
  // the set of input files (segments) to operate on
  IntrusiveList<MergeResultSegment<KeyType, ValueType>> _segmentList = new IntrusiveList<MergeResultSegment<KeyType, ValueType>>();
  // the output spill writer
  SpillWriter<KeyType, ValueType> _writer = null;
  // a reference to the raw writer interface if _writer implements
  // RawDataSpillWriter
  RawDataSpillWriter<KeyType, ValueType> _rawWriter = null;
  // basic key value comparator used to merge files
  KeyValuePairComparator<KeyType, ValueType> _comparator;
  // raw comparator if supported
  RawKeyValueComparator<KeyType, ValueType> _rawComparator = null;
  // optimized key generator interface
  OptimizedKeyGeneratorAndComparator<KeyType, ValueType> _optimizedKeyGenerator = null;
  // optional combiner interface
  SpillValueCombiner<KeyType, ValueType> _optionalCombiner = null;
  // input record counter
  long _inputRecordCount = 0;
  // merged record count
  long _mergedRecordCount = 0;

  // optimized key type
  int _optimizedKeyType = 0;

  /**
   * construct a specialized merger that uses an optimized key generator to
   * speed merges (used by merge sort spill writer)
   * 
   * this constructor is package private since it requires a special contract
   * between mergesortspillwriter and sequencefilemerger
   * 
   * @param fileSystem
   * @param conf
   * @param inputSegments
   * @param spillWriter
   * @param keyClass
   * @param valueClass
   * @param optionalKeyGenerator
   * @param optionalCombiner
   * @throws IOException
   */
  SequenceFileMerger(FileSystem fileSystem, Configuration conf, Vector<Path> inputSegments,
      SpillWriter<KeyType, ValueType> spillWriter, Class<KeyType> keyClass, Class<ValueType> valueClass,
      OptimizedKeyGeneratorAndComparator<KeyType, ValueType> keyGenerator) throws IOException {

    // initialize optimized key object
    _optimizedKeyType = keyGenerator.getGeneratedKeyType();
    // common init ...
    init(fileSystem, conf, inputSegments, spillWriter, keyClass, valueClass, null, keyGenerator, null);
  }

  /**
   * construct a merger that uses an raw comparator
   * 
   * this constructor is package private since it requires a special contract
   * between mergesortspillwriter and sequencefilemerger
   * 
   * @param fileSystem
   * @param conf
   * @param inputSegments
   * @param spillWriter
   * @param keyClass
   * @param valueClass
   * @param comparator
   * @throws IOException
   */
  public SequenceFileMerger(FileSystem fileSystem, Configuration conf, Vector<Path> inputSegments,
      SpillWriter<KeyType, ValueType> spillWriter, Class<KeyType> keyClass, Class<ValueType> valueClass,
      RawKeyValueComparator<KeyType, ValueType> comparator) throws IOException {

    // common init ...
    init(fileSystem, conf, inputSegments, spillWriter, keyClass, valueClass, comparator, null, null);
  }

  /**
   * construct a basic merger using a standard basic or raw comparator
   * 
   * @param fileSystem
   * @param conf
   * @param inputSegments
   * @param spillWriter
   * @param keyClass
   * @param valueClass
   * @param optionalCombiner
   * @param comparator
   * @throws IOException
   */
  public SequenceFileMerger(FileSystem fileSystem, Configuration conf, Vector<Path> inputSegments,
      SpillWriter<KeyType, ValueType> spillWriter, Class<KeyType> keyClass, Class<ValueType> valueClass,
      SpillValueCombiner<KeyType, ValueType> optionalCombiner, KeyValuePairComparator<KeyType, ValueType> comparator)
      throws IOException {
    // common init ...
    init(fileSystem, conf, inputSegments, spillWriter, keyClass, valueClass, comparator, null, optionalCombiner);
  }

  // do a binary search in the map to find the right value
  private final MergeResultSegment<KeyType, ValueType> _findInsertionPos(
      MergeResultSegment<KeyType, ValueType> searchSegment, MergeResultSegment<KeyType, ValueType>[] array,
      int arrayCount) throws IOException {

    int low = 0;
    int high = arrayCount - 1;

    while (low <= high) {
      int mid = low + ((high - low) / 2);

      MergeResultSegment<KeyType, ValueType> segment = array[mid];

      int compareResult = 0;
      if (_optimizedKeyGenerator != null) {
        if ((_optimizedKeyType & OptimizedKey.KEY_TYPE_LONG) != 0) {
          compareResult = (int) (segment.getOptimizedKey().getLongKeyValue() - searchSegment.getOptimizedKey()
              .getLongKeyValue());
        }
        if (compareResult == 0 && (_optimizedKeyType & OptimizedKey.KEY_TYPE_BUFFER) != 0) {
          // compare buffers ...
          compareResult = _optimizedKeyGenerator.compareOptimizedBufferKeys(segment.getOptimizedKey()
              .getBufferKeyValue().get(), segment.getOptimizedKey().getBufferKeyValue().getOffset(), segment
              .getOptimizedKey().getBufferKeyValue().getCount(), searchSegment.getOptimizedKey().getBufferKeyValue()
              .get(), searchSegment.getOptimizedKey().getBufferKeyValue().getOffset(), searchSegment.getOptimizedKey()
              .getBufferKeyValue().getCount());
        }
      } else if (_rawComparator != null) {
        compareResult = _rawComparator.compareRaw(segment.getRawKeyData().getData(), 0, segment.getRawKeyData()
            .getLength(), searchSegment.getRawKeyData().getData(), 0, searchSegment.getRawKeyData().getLength(),
            segment.getRawValueData().getData(), 0, segment.getRawValueData().getLength(), searchSegment
                .getRawValueData().getData(), 0, searchSegment.getRawValueData().getLength());

      } else {
        compareResult = _comparator.compare(segment.getKey(), segment.getValue(), searchSegment.getKey(), searchSegment
            .getValue());
      }

      // LOG.info("Compare Between" + segment.getKey().toString() + " and " +
      // searchSegment.getKey() + " returned:" + compareResult);
      if (compareResult > 0) {
        // LOG.info("Setting high to:" + (mid - 1));
        high = mid - 1;
      } else if (compareResult < 0) {
        // LOG.info("Setting low to:" + (mid + 1));
        low = mid + 1;
      } else {
        // LOG.info("Found match. returning item at:" + mid);
        return array[mid]; // found
      }

    }
    // not found ... return best insertion position ...
    if (high == -1) {
      // LOG.info("High == -1. Returning NULL");
      return null;
    } else {
      // LOG.info("Returning element at index:" + high);
      return array[high];
    }
  }

  /**
   * add merge segments to sort array
   * 
   * @param array
   * @param list
   */
  private final void addItemsToArray(MergeResultSegment<KeyType, ValueType>[] array,
      IntrusiveList<MergeResultSegment<KeyType, ValueType>> list) {
    MergeResultSegment<KeyType, ValueType> current = list.getHead();
    int pos = 0;
    while (current != null) {
      array[pos++] = current;
      current = current.getNext();
    }
  }

  /**
   * close and flush the merger
   * 
   * @throws IOException
   */
  public void close() throws IOException {
    for (MergeResultSegment<KeyType, ValueType> segment : _segmentList) {
      try {
        segment.close();
      } catch (IOException e) {
        LOG.error(StringUtils.stringifyException(e));
      }
    }
  }

  /**
   * internal init method
   * 
   * @param fileSystem
   * @param conf
   * @param inputSegments
   * @param spillWriter
   * @param keyClass
   * @param valueClass
   * @param comparator
   * @param optionalKeyGenerator
   * @param optionalCombiner
   * @throws IOException
   */
  private void init(FileSystem fileSystem, Configuration conf, Vector<Path> inputSegments,
      SpillWriter<KeyType, ValueType> spillWriter, Class<KeyType> keyClass, Class<ValueType> valueClass,
      KeyValuePairComparator<KeyType, ValueType> comparator,
      OptimizedKeyGeneratorAndComparator<KeyType, ValueType> optionalKeyGenerator,
      SpillValueCombiner<KeyType, ValueType> optionalCombiner

  ) throws IOException {

    _comparator = comparator;
    _optimizedKeyGenerator = optionalKeyGenerator;
    if (_comparator instanceof RawKeyValueComparator) {
      _rawComparator = (RawKeyValueComparator<KeyType, ValueType>) _comparator;
    }
    if (_rawComparator != null && _optimizedKeyGenerator != null) {
      throw new IOException("RawComparator not compatible with OptimizedKeyGenerator option!");
    }
    _optionalCombiner = optionalCombiner;

    try {

      Vector<MergeResultSegment<KeyType, ValueType>> segments = new Vector<MergeResultSegment<KeyType, ValueType>>();

      for (Path path : inputSegments) {
        // LOG.info("Loading QueryResultSegment:" + path);
        MergeResultSegment<KeyType, ValueType> resultSegment = new MergeResultSegment<KeyType, ValueType>(fileSystem,
            conf, path, keyClass, valueClass, _rawComparator != null || _optimizedKeyGenerator != null,
            _optimizedKeyGenerator);
        if (!resultSegment.next()) {
          // LOG.info("QueryResultSegment:" + path
          // +" returned EOS on initial next.Ignoring Segment");
          try {
            resultSegment.close();
          } catch (IOException e) {
            LOG.error("QueryResultSegment:" + path + " Threw Exception:" + StringUtils.stringifyException(e));
          }
        } else {
          _inputRecordCount++;
          segments.add(resultSegment);
        }
      }

      // create temporary array for sorting purposes ...
      MergeResultSegment<KeyType, ValueType> segmentArray[] = segments.toArray(new MergeResultSegment[0]);
      // sort the array ...
      Arrays.sort(segmentArray, new Comparator<MergeResultSegment<KeyType, ValueType>>() {

        @Override
        public int compare(MergeResultSegment<KeyType, ValueType> o1, MergeResultSegment<KeyType, ValueType> o2) {
          try {
            if (_optimizedKeyGenerator != null) {
              int result = 0;
              if ((_optimizedKeyType & OptimizedKey.KEY_TYPE_LONG) != 0) {
                result = (int) (o1.getOptimizedKey().getLongKeyValue() - o2.getOptimizedKey().getLongKeyValue());
              }
              if (result == 0 && ((_optimizedKeyType & OptimizedKey.KEY_TYPE_BUFFER) != 0)) {
                // compare buffers ...
                result = _optimizedKeyGenerator.compareOptimizedBufferKeys(o1.getOptimizedKey().getBufferKeyValue()
                    .get(), o1.getOptimizedKey().getBufferKeyValue().getOffset(), o1.getOptimizedKey()
                    .getBufferKeyValue().getCount(), o2.getOptimizedKey().getBufferKeyValue().get(), o2
                    .getOptimizedKey().getBufferKeyValue().getOffset(), o2.getOptimizedKey().getBufferKeyValue()
                    .getCount());

              }
              return result;
            } else if (_rawComparator != null) {
              return _rawComparator.compareRaw(o1.getRawKeyData().getData(), 0, o1.getRawKeyData().getLength(), o2
                  .getRawKeyData().getData(), 0, o2.getRawKeyData().getLength(), o1.getRawValueData().getData(), 0, o1
                  .getRawValueData().getLength(), o2.getRawValueData().getData(), 0, o2.getRawValueData().getLength());
            } else {
              return _comparator.compare(o1.getKey(), o1.getValue(), o2.getKey(), o2.getValue());
            }
          } catch (IOException e) {
            LOG.error(StringUtils.stringifyException(e));
            throw new RuntimeException(e);
          }
        }
      });
      // LOG.info("Initial sorted segment list is ....");
      // now store the segments in sorted order ...
      int index = 0;
      for (MergeResultSegment<KeyType, ValueType> segment : segmentArray) {
        segment.setIndex(index++);
        _segmentList.addTail(segment);
      }

      _writer = spillWriter;
      if (!(_writer instanceof RawDataSpillWriter)) {
        throw new IOException("Writer supplied with RawComparator does not implement RawDataSpillWriter");
      }
      _rawWriter = (RawDataSpillWriter<KeyType, ValueType>) _writer;

    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));

      for (MergeResultSegment<KeyType, ValueType> segment : _segmentList) {
        try {
          segment.close();
        } catch (IOException e2) {
          LOG.error(StringUtils.stringifyException(e2));
        }
      }
      throw e;
    }
  }

  /**
   * merge the inputs
   * 
   * @param reporter
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public void mergeAndSpill(Reporter reporter) throws IOException {
    long sortStartTime = System.currentTimeMillis();

    // allocate our sort array
    MergeResultSegment<KeyType, ValueType> sortArray[] = new MergeResultSegment[_segmentList.size() + 1];

    KeyType lastCombinerKey = null;
    Vector<ValueType> valueBuffer = new Vector<ValueType>();

    while (_segmentList.getHead() != null) {
      MergeResultSegment<KeyType, ValueType> spillSegment = null;
      try {
        // get the head element
        spillSegment = _segmentList.removeHead();
        // and spill its current key/value pair ...
        // LOG.info("Spilling Segment:" + spillSegment.getName() + " Key:" +
        // spillSegment.getKey().toString());
        // LOG.info("Spilling Segment:" + spillSegment.getName() + " Key:" +
        // spillSegment.getKey().toString());
        // if no combiner spill directly ...
        if (_optionalCombiner == null) {
          _mergedRecordCount++;
          // ok in the case of optimized keys ...
          if (_optimizedKeyGenerator != null) {
            // spill only the raw key, skipping the optimized key part ...

            /*
             * LOG.info("Spilling Record From Segment:" + spillSegment.getName()
             * + " OptKeyValue:" +
             * spillSegment.getOptimizedKey().getLongKeyValue() + " HeaderSize:"
             * + spillSegment.getOptimizedKey().getHeaderSize() + " KeySize:" +
             * (spillSegment.getRawKeyData().getLength() -
             * spillSegment.getOptimizedKey().getHeaderSize() - 4) +
             * " KeyDataLength:" + spillSegment.getRawKeyData().getLength() );
             */

            // ok segments with optimized keys have {optimized key header} +
            // {original-key-len} preceeding the actual key bytes
            // and optional buffer data at tail end of value

            _rawWriter.spillRawRecord(spillSegment.getRawKeyData().getData(), spillSegment.getOptimizedKey()
                .getHeaderSize() + 4, spillSegment.getRawKeyData().getLength()
                - spillSegment.getOptimizedKey().getHeaderSize() - 4, spillSegment.getRawValueData().getData(), 0,
                spillSegment.getRawValueData().getLength() - spillSegment.getOptimizedKey().getDataBufferSize());

          } else if (_rawComparator != null) {
            _rawWriter.spillRawRecord(spillSegment.getRawKeyData().getData(), 0, spillSegment.getRawKeyData()
                .getLength(), spillSegment.getRawValueData().getData(), 0, spillSegment.getRawValueData().getLength());
          } else {
            _writer.spillRecord(spillSegment.getKey(), spillSegment.getValue());
          }
        } else {
          if (valueBuffer.size() != 0 && lastCombinerKey.compareTo(spillSegment.getKey()) != 0) {

            // LOG.info("DEBUG:Spilling Combined Values for Key:" +
            // lastCombinerKey.toString() + " Value Count:" +
            // valueBuffer.size());
            // combine and flush last set of values ...
            _mergedRecordCount++;
            _writer.spillRecord(lastCombinerKey, _optionalCombiner.combineValues(lastCombinerKey, valueBuffer));
            // clear accumulation buffer
            valueBuffer.clear();
          }
          if (valueBuffer.size() == 0) {
            // set current key as lastKey
            lastCombinerKey = spillSegment.getKey();
          }
          // add value to buffer
          valueBuffer.add(spillSegment.getValue());
        }
        // and see if there is a next item for the spilled segment
        if (spillSegment.next()) {
          _inputRecordCount++;
          // yes, ok insert it back into the list at the appropriate position
          // ...
          if (_segmentList.size() == 0) {
            _segmentList.addHead(spillSegment);
          } else {
            // first convert existing list to array
            addItemsToArray(sortArray, _segmentList);
            // next find insertion position
            MergeResultSegment<KeyType, ValueType> insertionPos = _findInsertionPos(spillSegment, sortArray,
                _segmentList.size());
            // if null, add to head ...
            if (insertionPos == null) {
              // LOG.info("DEBUG:Adding Key:" + spillSegment.getKey().toString()
              // + " Before:" + _segmentList.getHead().getKey().toString());
              _segmentList.addHead(spillSegment);
            } else {
              // LOG.info("DEBUG:Adding Key:" + spillSegment.getKey().toString()
              // + " After:" + insertionPos.getKey().toString());
              _segmentList.insertAfter(insertionPos, spillSegment);
            }
          }
        }
        // otherwise ...
        else {
          // close the segment
          // LOG.info("Segment:" + spillSegment.getName() +
          // " Exhausted. Closing");
          try {
            spillSegment.close();
          } catch (IOException e) {
            LOG.error("Segment:" + spillSegment.getName() + " Exception:" + StringUtils.stringifyException(e));
          }
        }
      } catch (IOException e) {
        LOG.error(StringUtils.stringifyException(e));
        if (spillSegment != null) {
          LOG.error("Error during splill of segment:" + spillSegment.getName() + " Exception:"
              + StringUtils.stringifyException(e));
        }
      }

      if (reporter != null) {
        reporter.progress();
      }

      if (_mergedRecordCount % 100000 == 0) {
        LOG.info("Merged " + _mergedRecordCount + " Items");
      }
    }
    // now, if combiner is not null and there is a value buffered up ..
    if (_optionalCombiner != null && valueBuffer.size() != 0) {
      _mergedRecordCount++;
      // combine and flush last set of values ...
      _writer.spillRecord(lastCombinerKey, _optionalCombiner.combineValues(lastCombinerKey, valueBuffer));
      // clear combiner buffer ..
      valueBuffer.clear();
    }
    LOG.info("Merge took:" + (System.currentTimeMillis() - sortStartTime) + " InputRecordCount:" + _inputRecordCount
        + " MergedRecordCount:" + _mergedRecordCount);
  }
}
