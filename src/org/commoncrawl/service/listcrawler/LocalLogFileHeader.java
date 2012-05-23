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

package org.commoncrawl.service.listcrawler;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.rmi.server.UID;
import java.security.MessageDigest;

/** 
 * Header of a local cache log file
 * 
 * @author rana
 *
 */
class LocalLogFileHeader {
  
  public static final int LogFileHeaderBytes = 0xCC00CC00;
  public static final int LogFileVersion         = 1;
  public static final int SYNC_BYTES_SIZE = 16;
  public static final int SIZE = 4 /*header bytes*/ + 4 /*version*/ + SYNC_BYTES_SIZE + 8 /*file size*/ + 8 /*itemCount*/; 
  
  public LocalLogFileHeader() { 
    _fileSize = SIZE;
    _itemCount = 0;
  }
  public long _fileSize;
  public long _itemCount;
  /** sync bytes variable **/
  byte[] _sync;                          // 16 random bytes
  {
    try {                                       
      MessageDigest digester = MessageDigest.getInstance("MD5");
      long time = System.currentTimeMillis();
      digester.update((new UID()+"@"+time).getBytes());
      _sync = digester.digest();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  
  public void writeHeader(DataOutput stream) throws IOException { 
    stream.writeInt(LogFileHeaderBytes);
    stream.writeInt(LogFileVersion);
    stream.write(_sync);
    stream.writeLong(_fileSize);
    stream.writeLong(_itemCount);
  }
  
  public void readHeader(DataInput stream) throws IOException { 
    int headerBytes = stream.readInt();
    int version         = stream.readInt();
    
    if (headerBytes != LogFileHeaderBytes && version !=LogFileVersion) { 
      throw new IOException("Invalid CrawlLog File Header Detected!");
    }
    // read the sync bytes ... 
    stream.readFully(_sync);
    // and the record header information 
    _fileSize = stream.readLong();
    _itemCount = stream.readLong();
  }
}