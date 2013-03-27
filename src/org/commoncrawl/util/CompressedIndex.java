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

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.GzipCodec;

public class CompressedIndex {

  public static final Log LOG                  = LogFactory
                                                   .getLog(CompressedIndex.class);

  public static final int DATA_BLOCK_SIZE      = 256 * 1024;
  public static final int MAX_DATA_BUFFER_SIZE = 245 * 1024;

  public static class Builder {

    private FSDataOutputStream indexStream;
    private FSDataOutputStream dataStream;

    private static class BlockCompressor {

      int              desiredBlockSize = -1;

      DataOutputBuffer keyDataStream;
      DataOutputBuffer valueDataStream;
      DataOutputBuffer metadataStream;
      int              entryCount;
      int              lastKeyLength;
      int              lastDataLength;
      GzipCodec        codec            = new GzipCodec();
      Compressor       compressor       = null;

      DataOutputBuffer firstKeyBuffer   = new DataOutputBuffer();
      DataOutputBuffer lastKeyBuffer    = new DataOutputBuffer();
      DataOutputBuffer firstKey         = null;
      DataOutputBuffer lastKey          = null;

      public BlockCompressor(Configuration conf, int desiredBlockSize) {
        conf.setInt("io.file.buffer.size", DATA_BLOCK_SIZE);
        codec.setConf(conf);
        compressor = codec.createCompressor();
        this.desiredBlockSize = desiredBlockSize;
        reset();
      }

      private void reset() {
        compressor.reset();
        keyDataStream = new DataOutputBuffer();
        valueDataStream = new DataOutputBuffer();
        metadataStream = new DataOutputBuffer();
        entryCount = 0;
        lastKeyLength = 0;
        lastDataLength = 0;
        firstKey = null;
        lastKey = null;
      }

      public boolean addItem(FlexBuffer keyBytes, FlexBuffer dataBytes)
          throws IOException {
        if (firstKey == null) {
          firstKeyBuffer.reset();
          firstKeyBuffer.write(keyBytes.get(), keyBytes.getOffset(), keyBytes
              .getCount());
          firstKey = firstKeyBuffer;
        }
        if (lastKey == null) {
          lastKey = firstKeyBuffer;
        } else {
          lastKeyBuffer.reset();
          lastKeyBuffer.write(keyBytes.get(), keyBytes.getOffset(), keyBytes
              .getCount());
          lastKey = lastKeyBuffer;
        }

        // increment count
        entryCount++;
        // write out key length ...
        WritableUtils.writeVInt(metadataStream, keyBytes.getCount()
            - lastKeyLength);
        // update last Key length
        lastKeyLength = keyBytes.getCount();
        // write key to block stream
        keyDataStream.write(keyBytes.get(), keyBytes.getOffset(), keyBytes
            .getCount());
        // write out data length (delta)
        WritableUtils.writeVInt(metadataStream, dataBytes.getCount()
            - lastDataLength);
        // update last url data length
        lastDataLength = dataBytes.getCount();
        // write url data
        valueDataStream.write(dataBytes.get(), dataBytes.getOffset(), dataBytes
            .getCount());

        if (30 + metadataStream.getLength() + keyDataStream.getLength()
            + valueDataStream.getLength() >= desiredBlockSize) {
          return true;
        }
        return false;
      }

      public void flush(DataOutputBuffer indexPosStream,
          DataOutputBuffer indexDataStream, FSDataOutputStream finalDataStream)
          throws IOException {

        if (entryCount > 0) {
          // write out index position ...
          indexPosStream.writeLong(indexDataStream.getLength());
          // ok write out index
          WritableUtils.writeVInt(indexDataStream, firstKey.getLength());
          indexDataStream.write(firstKey.getData(), 0, firstKey.getLength());
          WritableUtils.writeVInt(indexDataStream, lastKey.getLength());
          indexDataStream.write(lastKey.getData(), 0, lastKey.getLength());
          indexDataStream.writeLong(finalDataStream.getPos());

          indexPosStream.flush();
          indexDataStream.flush();

          DataOutputBuffer dataStream = new DataOutputBuffer();
          // construct a crc object
          CRC32 crc = new CRC32();

          // ok write out url count ...
          WritableUtils.writeVInt(dataStream, entryCount);
          // and lengths stream size
          WritableUtils.writeVInt(dataStream, metadataStream.getLength());
          // write url data uncompressed length
          WritableUtils.writeVInt(dataStream, valueDataStream.getLength());
          // ok now url data stream
          dataStream.write(keyDataStream.getData(), 0, keyDataStream
              .getLength());
          // now lengths
          dataStream.write(metadataStream.getData(), 0, metadataStream
              .getLength());
          // now finally compress the url data
          DataOutputBuffer urlDataCompressed = new DataOutputBuffer();
          CompressionOutputStream compressionStream = codec.createOutputStream(
              urlDataCompressed, compressor);
          try {
            compressionStream.write(valueDataStream.getData(), 0,
                valueDataStream.getLength());
            compressionStream.flush();
          } finally {
            compressionStream.close();
          }
          // ok compute crc up to this point
          crc.update(dataStream.getData(), 0, dataStream.getLength());
          // next compute crc for compressed data
          crc.update(urlDataCompressed.getData(), 0, urlDataCompressed
              .getLength());

          // ok now pickup checksum
          finalDataStream.writeByte(0); // version
          // ok now pickup checksum
          finalDataStream.writeLong(crc.getValue());
          // write out data
          finalDataStream
              .write(dataStream.getData(), 0, dataStream.getLength());
          // and write out compressed data
          finalDataStream.write(urlDataCompressed.getData(), 0,
              urlDataCompressed.getLength());

          finalDataStream.flush();

        }

        reset();
      }
    }

    BlockCompressor  compressor           = null;
    DataOutputBuffer blockIndexPosStream  = new DataOutputBuffer();
    DataOutputBuffer blockIndexDataStream = new DataOutputBuffer();

    public Builder(FSDataOutputStream indexDataStream,
        FSDataOutputStream dataStream) {
      this.indexStream = indexStream;
      this.dataStream = dataStream;
      this.compressor = new BlockCompressor(new Configuration(),
          MAX_DATA_BUFFER_SIZE);
    }

    public void addItem(FlexBuffer key, FlexBuffer value) throws IOException {
      if (compressor.addItem(key, value)) {
        compressor.flush(blockIndexPosStream, blockIndexDataStream, dataStream);
      }
    }

    public void close() throws IOException {
      compressor.flush(blockIndexPosStream, blockIndexDataStream, dataStream);

      CRC32 crc = new CRC32();
      DataOutputStream checkedOutputStream = new DataOutputStream(
          new CheckedOutputStream(indexStream, new CRC32()));
      // write out cumulative length
      indexStream.writeInt(blockIndexPosStream.getLength()
          + blockIndexDataStream.getLength());
      // write out data via checked stream
      WritableUtils.writeVInt(checkedOutputStream, blockIndexPosStream
          .getLength());
      checkedOutputStream.write(blockIndexPosStream.getData(), 0,
          blockIndexPosStream.getLength());
      WritableUtils.writeVInt(checkedOutputStream, blockIndexDataStream
          .getLength());
      checkedOutputStream.write(blockIndexDataStream.getData(), 0,
          blockIndexDataStream.getLength());
      checkedOutputStream.flush();

      // write out crc at end
      indexStream.writeLong(crc.getValue());
      indexStream.flush();
      indexStream.close();
      dataStream.close();
    }
  }


}
