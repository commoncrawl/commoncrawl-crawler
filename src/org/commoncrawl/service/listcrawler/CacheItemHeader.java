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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

import org.commoncrawl.util.ByteStream;
import org.commoncrawl.util.CRC16;

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
    // read remaining bytes
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