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
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.StringUtils;

/**
 * An input source that reads from a SequenceFile
 * 
 * @author rana
 * 
 * @param <KeyType>
 * @param <ValueType>
 */
public class SequenceFileReader<KeyType extends WritableComparable, ValueType extends Writable> {

  public static final Log LOG = LogFactory.getLog(SequenceFileReader.class);

  FileSystem _sourceFileSystem;
  Configuration _config;
  SpillWriter<KeyType, ValueType> _writer = null;
  Vector<Path> _inputSegments = null;
  Constructor<KeyType> _keyConstructor = null;
  Constructor<ValueType> _valConstructor = null;
  long _recordCount = 0;
  private static final Class[] emptyArray = new Class[] {};

  public SequenceFileReader(FileSystem fileSystem, Configuration conf, Vector<Path> inputSegments,
      SpillWriter<KeyType, ValueType> spillWriter, Class<KeyType> keyClass, Class<ValueType> valueClass)
      throws IOException {

    _sourceFileSystem = fileSystem;
    _config = conf;
    _inputSegments = inputSegments;
    _writer = spillWriter;

    try {
      this._keyConstructor = keyClass.getDeclaredConstructor(emptyArray);
      this._keyConstructor.setAccessible(true);
      this._valConstructor = valueClass.getDeclaredConstructor(emptyArray);
      this._valConstructor.setAccessible(true);
    } catch (SecurityException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new RuntimeException(e);
    } catch (NoSuchMethodException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new RuntimeException(e);
    }

  }

  public void close() throws IOException {
  }

  @SuppressWarnings("unchecked")
  public void readAndSpill() throws IOException {
    long cumilativeReadTimeStart = System.currentTimeMillis();

    for (Path sequenceFilePath : _inputSegments) {

      long individualReadTimeStart = System.currentTimeMillis();

      // LOG.info("Reading Contents for File:" + sequenceFilePath);
      // allocate a reader for the current path
      SequenceFile.Reader reader = new SequenceFile.Reader(_sourceFileSystem, sequenceFilePath, _config);

      try {
        boolean eos = false;

        while (!eos) {

          KeyType key = null;
          ValueType value = null;

          try {
            key = _keyConstructor.newInstance();
            value = _valConstructor.newInstance();
          } catch (Exception e) {
            LOG.error("Failed to create key or value type with Exception:" + StringUtils.stringifyException(e));
            throw new RuntimeException(e);
          }

          eos = !reader.next(key, value);

          if (!eos) {
            _recordCount++;
            _writer.spillRecord(key, value);
          }
        }
        while (!eos)
          ;

        // LOG.info("Read and Spill of File:" + sequenceFilePath +" took:" +
        // (System.currentTimeMillis() - individualReadTimeStart));
      } finally {

        if (reader != null) {
          reader.close();
        }
      }
    }
    // LOG.info("Cumilative Read and Spill took:" + (System.currentTimeMillis()
    // - cumilativeReadTimeStart) + " Spilled RecordCount:" + _recordCount);
  }
}
