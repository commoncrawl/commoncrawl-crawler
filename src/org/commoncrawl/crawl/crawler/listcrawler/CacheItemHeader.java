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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

import org.commoncrawl.util.shared.ByteStream;
import org.commoncrawl.util.shared.CRC16;

/**
 * header data used to represent a cache item on disk 
 *  
 * @author rana
 *
 */
class CacheItemHeader {
  
  public static int SIZE = LocalLogFileHeader.SYNC_BYTES_SIZE + 4 /*checksump*/ + 1 /*status*/ + 8 /*last access time*/ + 8 /*fingerprint*/+ 4 /*data length*/;
  public static int CHECKSUM_OFFSET = LocalLogFileHeader.SYNC_BYTES_SIZE;
  public static int CHECKSUM_SIZE = 4;
  public static int STATUS_BYTE_OFFSET = CHECKSUM_OFFSET + CHECKSUM_SIZE;  
  
  public static final int STATUS_ALIVE = 0;
  public static final int STATUS_PURGED = 1;
  
  
  public CacheItemHeader() {
    _sync = new byte[LocalLogFileHeader.SYNC_BYTES_SIZE];
  }
  
  public CacheItemHeader(byte[] syncBytes) { 
    _sync = syncBytes;
  }
  
  
  public byte[] _sync = null;
  public byte   _status;
  public long   _fingerprint;
  public long   _lastAccessTime;
  public int    _dataLength;
  
  public void writeHeader(DataOutput stream) throws IOException {
    
    stream.write(_sync);
    ByteStream baos = new ByteStream(SIZE-STATUS_BYTE_OFFSET);
    CheckedOutputStream checkedStream = new CheckedOutputStream(baos,new CRC16());
    DataOutputStream dataStream = new DataOutputStream(checkedStream);
    dataStream.write(_status);
    dataStream.writeLong(_lastAccessTime);
    dataStream.writeLong(_fingerprint);
    dataStream.writeInt(_dataLength);
    // now write out checksum first
    stream.writeInt((int)checkedStream.getChecksum().getValue());
    // and the remainder 
    stream.write(baos.getBuffer(), 0, baos.size());
  }
  
  public void readHeader(DataInput stream) throws IOException { 
    stream.readFully(_sync);
    // read checksum value 
    int checksumOnDisk = stream.readInt();
    // read reamining bytes 
    byte[] headerData = new byte[SIZE-STATUS_BYTE_OFFSET]; 
    // no open a checked input stream ... 
    stream.readFully(headerData);
    // and now read it in via checked input stream
    CheckedInputStream checkedInputStream = new CheckedInputStream(new ByteArrayInputStream(headerData),new CRC16());
    DataInputStream dataStream = new DataInputStream(checkedInputStream);
    
    _status = dataStream.readByte();
    _lastAccessTime = dataStream.readLong();
    _fingerprint = dataStream.readLong();
    _dataLength = dataStream.readInt();
    
    // now validate header checksum ... 
    if (checkedInputStream.getChecksum().getValue() != (long)checksumOnDisk) { 
      throw new IOException("Checksum failed while reading Item Header!");
    }
  }    
  
}