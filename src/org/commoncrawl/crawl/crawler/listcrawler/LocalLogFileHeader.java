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

package org.commoncrawl.crawl.crawler.listcrawler;


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