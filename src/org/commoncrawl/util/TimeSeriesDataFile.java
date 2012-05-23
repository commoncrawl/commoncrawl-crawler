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
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.zip.CRC32;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.record.Buffer;

/** quick and dirty (for now) way to write writable records to a local disk file 
 * 
 * @author rana
 *
 */
public class TimeSeriesDataFile<ValueType extends Writable> {
  
  private static final int SyncBytes = 0xCC00CC00;
  private CRC32 crc = new CRC32();
  private File  fileName=null;
  private static final int RECORD_HEADER_LENGTH = 4 * 4;
  private Class valueClass;

  
  public static class KeyValueTuple<KeyType,ValueType> { 
    
    public KeyValueTuple(KeyType key,ValueType value) { 
      this.key = key;
      this.value = value;
    }
    
    public KeyType key;
    public ValueType value;
    public long recordPos;
  }
  
  /** constructor
   * 
   * @param fileName the output path (file will be created if it doesn't exist)
   */
  public TimeSeriesDataFile(File fileName,Class valueClass) { 
    this.fileName = fileName;
    this.valueClass = valueClass;
  }
  

  /** append a record to the file ...  
   * 
   * @param key
   * @param value
   * @throws IOException
   */
  public synchronized long appendRecordToLogFile(long key,Writable value)throws IOException { 
    
    LogFileHeader header = new LogFileHeader();
    
    boolean preExistingHeader = fileName.exists();
          
    RandomAccessFile file = new RandomAccessFile(fileName,"rw");
    
    long recordPositionOut = -1;
    
    try { 
      
      if(preExistingHeader) {
        
        long headerOffset = readLogFileHeader(file, header);
        
        if (header._writePos == 0) { 
          recordPositionOut = headerOffset;
        }
        else {
          recordPositionOut = header._writePos;
        }
        // seelk to appropriate write position 
        file.seek(recordPositionOut);

      }
      else { 
        recordPositionOut = writeLogFileHeader(file,header);
      }
      

      DataOutputBuffer buffer = new DataOutputBuffer();

      // write out sync bytes ... 
      buffer.writeInt(SyncBytes);
      // write out placeholder for record length 
      buffer.writeInt(0);
      // write out placeholder for crc 
      buffer.writeLong(0);
      // write out key + value to buffer
      WritableUtils.writeVLong(buffer,key);
      // write out value ... 
      value.write(buffer);
      // write out trailing record size (4 bytes sync + 4 bytes record length + 4 bytes crc + key/value buffer +  
      buffer.writeInt(buffer.getLength());
      // reset crc 
      crc.reset();
      //calc crc 
      crc.update(buffer.getData(),RECORD_HEADER_LENGTH,buffer.getLength()-RECORD_HEADER_LENGTH);
      // ok fix up record ... 
      // write out record length
      // total length - sync bytes(4) - record length(4), at offset 4
      writeInt(buffer.getLength() - 8,4,buffer.getData());
      // and write out crc
      // at offset 8 (after sync(4) and length(4)
      writeLong(crc.getValue(),8,buffer.getData());
      
      // and then the data 
      file.write(buffer.getData(),0,buffer.getLength());
      
      // now update header ... 
      header._itemCount += 1;
      header._writePos   = file.getFilePointer();
      header._lastRecordLength = buffer.getLength() - 4;
      header._lastRecordKey = key;
      // now write out header anew ... 
      writeLogFileHeader(file,header);
      
    }
    finally { 
      if (file != null) { 
        file.close();
      }
    }
    
    return recordPositionOut;
  }
  /**
   * read given a position 
   * 
   * @param position file position to start read at 
   * @param maxNumberOfRecords maximum number of records to read 
   * @param optionalMinKeyValue optional min key value to limit read by or -1
   * @return a vector of KeyValueTuples
   * @throws IOException
   */
  public synchronized ArrayList <KeyValueTuple<Long,ValueType> > readFromPos(long position,int maxNumberOfRecords,long optionalMinKeyValue) throws IOException { 
    ArrayList< KeyValueTuple<Long,ValueType> > valuesOut = new ArrayList< KeyValueTuple<Long,ValueType> >();
    
    LogFileHeader header = new LogFileHeader();
    
    if (fileName.exists()) { 

      RandomAccessFile file = new RandomAccessFile(fileName,"r"); 
      
      try { 

        //read header ... 
        long headerOffset = readLogFileHeader(file, header);
  
        long endOfPrevRecord = position;
        
        if (position > headerOffset) { 
          
          file.seek(endOfPrevRecord - 4);
          
          // read previous record length 
          int currentRecordLength = file.readInt();
          
          // delegate to common read
          doCommonRead(valuesOut,file,headerOffset,endOfPrevRecord,currentRecordLength,maxNumberOfRecords,optionalMinKeyValue);
          
        }
      }
      finally { 
        if (file != null) { 
          file.close();
        }        
      }
    }
    return valuesOut;
  }
  
  /**
   * read from the tail end of the file 
   * 
   * @param maxNumberOfRecords the maximum number of records to read from the tail 
   * @return a list of records at the tail end of the file  
   * @throws IOException
   */
  public synchronized ArrayList< KeyValueTuple<Long,ValueType> > readFromTail(int maxNumberOfRecords,long optionalMinKeyValue)throws IOException { 
    
    ArrayList< KeyValueTuple<Long,ValueType> > valuesOut = new ArrayList< KeyValueTuple<Long,ValueType> >();
    
    LogFileHeader header = new LogFileHeader();
    
    if (fileName.exists()) { 
          
      RandomAccessFile file = new RandomAccessFile(fileName,"r"); 
      

      try { 
  
        //read header ... 
        long headerOffset = readLogFileHeader(file, header);
        // figure out how many records we can read ... 
        int recordsToRead = maxNumberOfRecords;
        
        if (recordsToRead != 0) { 
          
          long endOfPrevRecord = header._writePos;  

          // read in first record length ... 
          int currentRecordLength = header._lastRecordLength;
         
          // delegate to common read
          doCommonRead(valuesOut,file,headerOffset,endOfPrevRecord,currentRecordLength,recordsToRead,optionalMinKeyValue);
        }
      }
      finally { 
        if (file != null) { 
          file.close();
        }
      }
    }
    
    return valuesOut;
  }
  
  
  private void doCommonRead(
      ArrayList< KeyValueTuple<Long,ValueType> > valuesOut,
      RandomAccessFile file,
      long headerOffset,
      long endOfPrevRecord,
      int currentRecordLength,
      int recordsToRead,
      long optionalMinKeyValue) throws IOException {
    
    Buffer  recordBuffer = new Buffer();
    DataInputBuffer inputBuffer = new DataInputBuffer();
    
    // ok start walking backwards ... 
    while (recordsToRead != 0) { 
      // setup new previous record pos pointer  
      endOfPrevRecord = endOfPrevRecord - currentRecordLength - 4;
      // and seek to it endOfLastRecord - 4
      file.seek(endOfPrevRecord - 4);
       
      recordBuffer.setCapacity(currentRecordLength + 8);
      // read in proper amount of data ...
      file.read(recordBuffer.get(),0,currentRecordLength + 8);
      // ok initialize input buffer ... 
      inputBuffer.reset(recordBuffer.get(), currentRecordLength + 8);
      // now read next record length first ... 
      int nextRecordLength = inputBuffer.readInt();
      // next read sync bytes ... 
      int syncBytes = inputBuffer.readInt();
      // validate 
      if (syncBytes != SyncBytes) { 
        throw new IOException("Corrupt Record Detected!");
      }
      // ok read real record bytes ... 
      int realRecordBytes = inputBuffer.readInt();
      // read crc ... 
      long crcValue = inputBuffer.readLong();
      // ok validate crc ...  
      crc.reset();
      crc.update(inputBuffer.getData(),inputBuffer.getPosition(),realRecordBytes-8);
      if (crcValue != crc.getValue()) { 
        throw new IOException("CRC Mismatch!");
      }
      // ok now read key and value 
      try {
        long key = WritableUtils.readVLong(inputBuffer);
        
        if (optionalMinKeyValue != -1 && key < optionalMinKeyValue) { 
          break;
        }
        
        ValueType value = (ValueType) valueClass.newInstance();
        value.readFields(inputBuffer);
        KeyValueTuple tuple = new KeyValueTuple<Long, ValueType>(key, value);
        tuple.recordPos = endOfPrevRecord;
        valuesOut.add(0,tuple);
        
      } catch (Exception e) {
        throw new IOException(e);
      }

      currentRecordLength = nextRecordLength;
      
      recordsToRead--;
      
      if (endOfPrevRecord == headerOffset)
        break;
    }
  }
  
  /**
   * get the key value of the last record in the file 
   * @return record key as a long or -1 if zero records in file 
   * @throws IOException
   */
  public synchronized long getLastRecordKey() throws IOException{ 
    LogFileHeader header = new LogFileHeader();
    
    if (fileName.exists()) { 
          
      RandomAccessFile file = new RandomAccessFile(fileName,"r"); 
      
      Buffer  recordBuffer = new Buffer();
      DataInputBuffer inputBuffer = new DataInputBuffer();
      try { 
  
        //read header ... 
        long headerOffset = readLogFileHeader(file, header);
        
        return header._lastRecordKey;
      }
      finally { 
        if (file != null) { 
          file.close();
        }
      }
    }
    return -1;
  }
  
  /**
   * get the number of records in the file 
   * 
   * @return record count in file 
   * @throws IOException
   */
  public synchronized int getRecordCount() throws IOException { 
    
    LogFileHeader header = new LogFileHeader();
    
    if (fileName.exists()) { 
          
      RandomAccessFile file = new RandomAccessFile(fileName,"r"); 
      
      Buffer  recordBuffer = new Buffer();
      DataInputBuffer inputBuffer = new DataInputBuffer();
      try { 
  
        //read header ... 
        long headerOffset = readLogFileHeader(file, header);
        
        return header._itemCount;
      }
      finally { 
        if (file != null) { 
          file.close();
        }
      }
    }
    return 0;
  }
  
  private static class LogFileHeader {
    
    public static final int LogFileHeaderBytes = SyncBytes;
    public static final int LogFileVersion         = 1;
    
    public LogFileHeader() { 
      _writePos = 0;
      _itemCount = 0;
      _lastRecordLength = 0;
      _lastRecordKey = -1;
    }
    
    public long _writePos;
    public int  _itemCount;
    public int  _lastRecordLength;
    public long _lastRecordKey;
    
    public void writeHeader(DataOutput stream) throws IOException { 
      stream.writeInt(LogFileHeaderBytes);
      stream.writeInt(LogFileVersion);
      stream.writeLong(_writePos);
      stream.writeInt(_itemCount);
      stream.writeInt(_lastRecordLength);
      stream.writeLong(_lastRecordKey);
    }
    
    public void readHeader(DataInput stream) throws IOException { 
      int headerBytes = stream.readInt();
      int version     = stream.readInt();
      if (headerBytes != LogFileHeaderBytes && version !=LogFileVersion) { 
        throw new IOException("Invalid CrawlLog File Header Detected!");
      }
      _writePos         = stream.readLong();
      _itemCount        = stream.readInt();
      _lastRecordLength = stream.readInt();
      _lastRecordKey    = stream.readLong();
    }
  }  
  private static long writeLogFileHeader(RandomAccessFile file, LogFileHeader header )throws IOException {
    
    // set the position at zero .. 
    file.seek(0);
    // and write header to disk ... 
    header.writeHeader(file);
    
    //took sync out because it was becoming a sever bottleneck
    // file.getFD().sync();
    
    return file.getFilePointer();
  }
  
  private static long readLogFileHeader(RandomAccessFile file,LogFileHeader header) throws IOException {
    
    file.seek(0);
    
    header.readHeader(file);
    
    return file.getFilePointer();
  }

  static void writeInt(int value,int atOffset, byte[] intoBytes) throws IOException {
    intoBytes[atOffset + 0] = (byte) ((value >>> 24) & 0xFF);
    intoBytes[atOffset + 1] = (byte) ((value >>> 16) & 0xFF);
    intoBytes[atOffset + 2] = (byte) ((value >>> 8) & 0xFF);
    intoBytes[atOffset + 3] = (byte) ((value >>> 0) & 0xFF);
  }

  static void writeLong(long value,int atOffset, byte[] intoBytes) throws IOException {
    intoBytes[atOffset + 0] = (byte) ((value >>> 56) & 0xFF);
    intoBytes[atOffset + 1] = (byte) ((value >>> 48) & 0xFF);
    intoBytes[atOffset + 2] = (byte) ((value >>> 40) & 0xFF);
    intoBytes[atOffset + 3] = (byte) ((value >>> 32) & 0xFF);
    intoBytes[atOffset + 4] = (byte) ((value >>> 24) & 0xFF);
    intoBytes[atOffset + 5] = (byte) ((value >>> 16) & 0xFF);
    intoBytes[atOffset + 6] = (byte) ((value >>> 8) & 0xFF);
    intoBytes[atOffset + 7] = (byte) ((value >>> 0) & 0xFF);
  }
}
