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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.TreeMap;
import java.util.zip.CRC32;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.zlib.ZlibDecompressor;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.Tuples.LongTextBytesTuple;
import org.junit.Assert;

/**
 * 
 * @author rana
 * 
 */
public class CompressURLListV2 {

  public static final Log LOG                          = LogFactory
                                                           .getLog(CompressURLListV2.class);

  public static final int INDEX_RECORD_SIZE            = 40;
  public static final int URL_DATA_FIXED_RECORD_HEADER = 41;
  public static final int DATA_BLOCK_SIZE              = 256 * 1024;
  public static final int MAX_DATA_BUFFER_SIZE         = 245 * 1024;
  public static final int URL_ID_RECORD_SIZE           = 8;

  public static class Builder {

    private FSDataOutputStream indexStream;
    private FSDataOutputStream dataStream;

    private static class BlockCompressor {

      int              desiredBlockSize = -1;

      DataOutputBuffer urlIDStream;
      DataOutputBuffer urlStream;
      DataOutputBuffer metadataStream;
      int              urlCount;
      int              lastURLDataLength;
      GzipCodec        codec            = new GzipCodec();
      Compressor       compressor       = null;

      URLFPV2          firstItem        = null;
      URLFPV2          lastItem         = null;

      public BlockCompressor(Configuration conf, int desiredBlockSize) {
        conf.setInt("io.file.buffer.size", DATA_BLOCK_SIZE);
        codec.setConf(conf);
        compressor = codec.createCompressor();
        this.desiredBlockSize = desiredBlockSize;
        reset();
      }

      private void reset() {
        compressor.reset();
        urlIDStream = new DataOutputBuffer();
        urlStream = new DataOutputBuffer();
        metadataStream = new DataOutputBuffer();
        urlCount = 0;
        lastURLDataLength = 0;
        firstItem = null;
        lastItem = null;
      }

      public boolean addItem(URLFPV2 fingerprint, TextBytes urlBytes)
          throws IOException {
        if (firstItem == null) {
          firstItem = new URLFPV2();
          firstItem.setDomainHash(fingerprint.getDomainHash());
          firstItem.setUrlHash(fingerprint.getUrlHash());
        }
        if (lastItem == null) {
          lastItem = new URLFPV2();
        }

        // update last item pointer
        lastItem.setDomainHash(fingerprint.getDomainHash());
        lastItem.setUrlHash(fingerprint.getUrlHash());

        // increment url count
        urlCount++;
        // write url id
        urlIDStream.writeLong(fingerprint.getUrlHash());
        // write out url length (delta)
        WritableUtils.writeVInt(metadataStream, urlBytes.getLength()
            - lastURLDataLength);
        // update last url data length
        lastURLDataLength = urlBytes.getLength();
        // write url data
        urlStream.write(urlBytes.getBytes(), 0, urlBytes.getLength());

        if (30 + metadataStream.getLength() + urlIDStream.getLength()
            + urlStream.getLength() >= desiredBlockSize) {
          return true;
        }
        return false;
      }

      public void flush(FSDataOutputStream indexStream,
          FSDataOutputStream finalDataStream) throws IOException {

        if (urlCount > 0) {
          // ok write out index
          indexStream.writeLong(firstItem.getDomainHash());
          indexStream.writeLong(firstItem.getUrlHash());
          indexStream.writeLong(lastItem.getDomainHash());
          indexStream.writeLong(lastItem.getUrlHash());
          indexStream.writeLong(finalDataStream.getPos());

          indexStream.flush();

          DataOutputBuffer dataStream = new DataOutputBuffer();
          // construct a crc object
          CRC32 crc = new CRC32();

          // and url data stream
          dataStream.writeLong(firstItem.getDomainHash());
          dataStream.writeLong(firstItem.getUrlHash());
          dataStream.writeLong(lastItem.getDomainHash());
          dataStream.writeLong(lastItem.getUrlHash());
          // ok write out url count ...
          WritableUtils.writeVInt(dataStream, urlCount);
          // and lengths stream size
          WritableUtils.writeVInt(dataStream, metadataStream.getLength());
          // write url data uncompressed length
          WritableUtils.writeVInt(dataStream, urlStream.getLength());
          // ok now url data stream
          dataStream.write(urlIDStream.getData(), 0, urlIDStream.getLength());
          // now lengths
          dataStream.write(metadataStream.getData(), 0, metadataStream
              .getLength());
          // now finally compress the url data
          DataOutputBuffer urlDataCompressed = new DataOutputBuffer();
          CompressionOutputStream compressionStream = codec.createOutputStream(
              urlDataCompressed, compressor);
          try {
            compressionStream.write(urlStream.getData(), 0, urlStream
                .getLength());
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

    BlockCompressor compressor = null;

    public Builder(FSDataOutputStream indexStream, FSDataOutputStream dataStream) {
      this.indexStream = indexStream;
      this.dataStream = dataStream;
      this.compressor = new BlockCompressor(new Configuration(),
          MAX_DATA_BUFFER_SIZE);
    }

    public void addItem(URLFPV2 fingerprint, TextBytes url) throws IOException {
      if (compressor.addItem(fingerprint, url)) {
        compressor.flush(indexStream, dataStream);
      }
    }

    public void close() throws IOException {
      compressor.flush(indexStream, dataStream);
      indexStream.close();
      dataStream.close();
    }
  }

  public static class IndexCursor {
    public IndexCursor() {

    }

    long dataOffset          = -1;
    byte decompressedBytes[] = null;

  }

  public static class Index {

    public static class IndexFile {
      public File       _localIndexFilePath;
      public File       _localDataFilePath;
      public ByteBuffer _indexDataBuffer;
      public int        _recordCount = -1;

      public IndexFile(File localIndexFilePath) throws IOException {
        _localIndexFilePath = localIndexFilePath;
        String baseName = _localIndexFilePath.getName();
        baseName = baseName.substring(0, baseName.lastIndexOf('.'));
        _localDataFilePath = new File(_localIndexFilePath.getParentFile(),
            baseName + ".data");

        // LOG.info("Index File:" + _localIndexFilePath + " DataFile:" +
        // _localDataFilePath + " Loading");
        loadIndex();
        // LOG.info("Index File:" + _localIndexFilePath + " DataFile:" +
        // _localDataFilePath + " Loaded");
      }

      private void loadIndex() throws IOException {
        _indexDataBuffer = ByteBuffer.allocate((int) _localIndexFilePath
            .length());
        // LOG.info("Loading Index Buffer From File:" + _localIndexFilePath);

        BufferedInputStream inputStream = new BufferedInputStream(
            new FileInputStream(_localIndexFilePath));
        try {
          for (int offset = 0, totalRead = 0; offset < _indexDataBuffer
              .capacity();) {
            int bytesToRead = Math.min(16384, _indexDataBuffer.capacity()
                - totalRead);
            inputStream.read(_indexDataBuffer.array(), offset, bytesToRead);
            offset += bytesToRead;
            totalRead += bytesToRead;
          }
          _recordCount = (int) _localIndexFilePath.length() / INDEX_RECORD_SIZE;
        } finally {
          if (inputStream != null) {
            inputStream.close();
          }
        }
      }

      final long findDataOffsetInIndex(URLFPV2 searchTerm) {

        ByteBuffer indexDataBuffer = _indexDataBuffer.asReadOnlyBuffer();

        URLFPV2 indexFPLow = new URLFPV2();
        URLFPV2 indexFPHigh = new URLFPV2();

        int low = 0;
        int high = _recordCount - 1;
        while (low <= high) {

          int mid = low + ((high - low) / 2);

          indexFPLow.setDomainHash(indexDataBuffer.getLong(mid
              * INDEX_RECORD_SIZE + (0 * 8)));
          indexFPLow.setUrlHash(indexDataBuffer.getLong(mid * INDEX_RECORD_SIZE
              + (1 * 8)));
          indexFPHigh.setDomainHash(indexDataBuffer.getLong(mid
              * INDEX_RECORD_SIZE + (2 * 8)));
          indexFPHigh.setUrlHash(indexDataBuffer.getLong(mid
              * INDEX_RECORD_SIZE + (3 * 8)));

          int result = indexFPLow.compareTo(searchTerm);

          if (result <= 0) {
            if (result == 0) {
              // LOG.info("Matched Index Record for (DH):" +
              // searchTerm.getDomainHash() + " (UH):" + searchTerm.getUrlHash()
              // + " index:" + mid);
              // LOG.info("fpLow is (DH):"+ indexFPLow.getDomainHash() + "(UH):"
              // + indexFPLow.getUrlHash());
              // LOG.info("fpHigh is (DH):"+ indexFPHigh.getDomainHash() +
              // "(UH):" + indexFPHigh.getUrlHash());

              return indexDataBuffer.getLong(mid * INDEX_RECORD_SIZE + (4 * 8));
            } else {
              result = indexFPHigh.compareTo(searchTerm);

              if (result >= 0) {
                // LOG.info("Matched Index Record for (DH):" +
                // searchTerm.getDomainHash() + " (UH):" +
                // searchTerm.getUrlHash() + " index:" + mid);
                // LOG.info("fpLow is (DH):"+ indexFPLow.getDomainHash() +
                // "(UH):" + indexFPLow.getUrlHash());
                // LOG.info("fpHigh is (DH):"+ indexFPHigh.getDomainHash() +
                // "(UH):" + indexFPHigh.getUrlHash());
                return indexDataBuffer.getLong(mid * INDEX_RECORD_SIZE
                    + (4 * 8));
              }
            }
          }

          if (result > 0)
            high = mid - 1;
          else if (result < 0)
            low = mid + 1;
        }
        // LOG.info("Failed to find Index Record for (DH):" +
        // searchTerm.getDomainHash() + " (UH):" + searchTerm.getUrlHash());
        // LOG.info("fpLow is (DH):"+ indexFPLow.getDomainHash() + "(UH):" +
        // indexFPLow.getUrlHash());
        // LOG.info("fpHigh is (DH):"+ indexFPHigh.getDomainHash() + "(UH):" +
        // indexFPHigh.getUrlHash());

        return -1; // not found
      }

      private static long readVLongFromByteBuffer(ByteBuffer source) {
        byte firstByte = source.get();
        int len = WritableUtils.decodeVIntSize(firstByte);
        if (len == 1) {
          return firstByte;
        }
        long i = 0;
        for (int idx = 0; idx < len - 1; idx++) {
          byte b = source.get();
          i = i << 8;
          i = i | (b & 0xFF);
        }
        return (WritableUtils.isNegativeVInt(firstByte) ? (i ^ -1L) : i);
      }

      private static byte[] decompressBytes(ByteBuffer buffer,
          int decompressedBufferSize) throws IOException {
        Configuration conf = new Configuration();
        conf.setInt("io.file.buffer.size", DATA_BLOCK_SIZE);
        GzipCodec codec = new GzipCodec();
        codec.setConf(conf);

        // ok time to decompress
        CompressionInputStream compressionInput = codec.createInputStream(
            new ByteBufferInputStream(buffer), new ZlibDecompressor(
                ZlibDecompressor.CompressionHeader.AUTODETECT_GZIP_ZLIB,
                DATA_BLOCK_SIZE));

        try {
          byte data[] = new byte[decompressedBufferSize];
          int bytesRead = compressionInput.read(data, 0, data.length);

          if (bytesRead != decompressedBufferSize) {
            LOG.error("Decompress. Expected Uncompressed Size of:"
                + decompressedBufferSize + " Got:" + bytesRead);
            throw new IOException("Expected Uncompressed Size of:"
                + decompressedBufferSize + " Got:" + bytesRead);
          }
          return data;
        } finally {
          compressionInput.close();
        }
      }

      private static TextBytes decodeURL(byte[] decompressedBytes,
          int urlDataPos, int urlDataLength) throws IOException {

        // allocate a buffer
        TextBytes txtBytesOut = new TextBytes();
        // copy corresponding url bytes into it
        txtBytesOut.set(decompressedBytes, urlDataPos, urlDataLength);
        // LOG.info("Returning Text Bytes At Pos:" + urlDataPos + " urlDataLen:"
        // + urlDataLength);
        // return the buffer
        return txtBytesOut;
      }

      public TextBytes mapURLFPToURL(URLFPV2 fingerprint, IndexCursor cursor)
          throws IOException {
        long dataOffset = findDataOffsetInIndex(fingerprint);
        // LOG.info("Data Offset for (DH):" + fingerprint.getDomainHash() +
        // " (UH):" + fingerprint.getUrlHash() + " is:" + dataOffset);
        // if found ...
        if (dataOffset != -1) {
          // ok seartch the url list ...

          // open the data file ...
          RandomAccessFile file = null;

          try {

            // LOG.info("Opening Data File At:" + _localDataFilePath);
            file = new RandomAccessFile(_localDataFilePath, "r");

            // LOG.info("Mapping Memory At:" + dataOffset);
            // map the proper location
            long mapSize = Math
                .min(file.length() - dataOffset, DATA_BLOCK_SIZE);
            MappedByteBuffer memoryBuffer = file.getChannel().map(
                MapMode.READ_ONLY, dataOffset, mapSize);

            // ok load the url list
            memoryBuffer.position(URL_DATA_FIXED_RECORD_HEADER);

            // LOG.info("Skipping Past:" + URL_DATA_FIXED_RECORD_HEADER +
            // " bytes");

            // read url count and metadata buffer length
            int urlCount = (int) readVLongFromByteBuffer(memoryBuffer);
            int metadataLength = (int) readVLongFromByteBuffer(memoryBuffer);
            int urlBufferLength = (int) readVLongFromByteBuffer(memoryBuffer);

            // LOG.info("URLCount:"+ urlCount + " metadataLength:" +
            // metadataLength + " urlBufferLength:" + urlBufferLength);

            memoryBuffer.mark();
            memoryBuffer.position(memoryBuffer.position() + URL_ID_RECORD_SIZE
                * urlCount);
            ByteBuffer metadataAndURLDataReader = memoryBuffer.slice();
            memoryBuffer.reset();

            int urlDataPos = 0;
            int lastURLDataLength = 0;
            // ok start reading ...
            for (int i = 0; i < urlCount; ++i) {
              long urlFPValue = memoryBuffer.getLong();
              int urlDataLength = lastURLDataLength
                  + (int) readVLongFromByteBuffer(metadataAndURLDataReader);
              int result = (urlFPValue < fingerprint.getUrlHash()) ? -1
                  : urlFPValue == fingerprint.getUrlHash() ? 0 : 1;
              if (result == 0) {
                // LOG.info("Found Match At Pos:"+ i + "urlDataLength:" +
                // urlDataLength + " offset:" + urlDataPos + " urlBufferLength:"
                // + urlBufferLength);
                // ok match found ...
                // time to decompress and return the result ...
                metadataAndURLDataReader.position(metadataLength);

                byte decompressedBytes[] = null;
                if (cursor != null && cursor.dataOffset == dataOffset) {
                  decompressedBytes = cursor.decompressedBytes;
                } else {
                  decompressedBytes = decompressBytes(metadataAndURLDataReader
                      .slice(), urlBufferLength);
                }

                if (cursor != null) {
                  cursor.decompressedBytes = decompressedBytes;
                  cursor.dataOffset = dataOffset;
                }

                return decodeURL(decompressedBytes, urlDataPos, urlDataLength);
              } else {
                // ok advance to next record ... but accumulate url data
                urlDataPos += urlDataLength;
                lastURLDataLength = urlDataLength;
              }
            }
          } finally {
            if (file != null) {
              file.close();
            }
          }
        }
        // not found ...
        return null;
      }
    }

    Path       _indexPaths[] = null;
    FileSystem _fileSystem   = null;

    public Index(FileSystem fileSystem, Path[] indexPaths) {
      _fileSystem = fileSystem;
      _indexPaths = indexPaths;

      // startScannerThread();
    }
  }
}
