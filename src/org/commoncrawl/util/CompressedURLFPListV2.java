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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.BasicConfigurator;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.BitUtils.BitStream;
import org.commoncrawl.util.BitUtils.BitStreamReader;

import com.google.common.collect.TreeMultimap;

/**
 * A compressed list of url fingerprints
 * 
 * @author rana
 * 
 */
public class CompressedURLFPListV2 {

  private static final Log LOG                        = LogFactory
                                                          .getLog(CompressedURLFPListV2.class);

  public static final int  FPList_Version             = 1;

  // store the segment id as part of the urlfingerprint stream
  public static final int  FLAG_ARCHIVE_SEGMENT_ID    = 1;
  // this stream contains a single permanent redirect url entry
  public static final int  FLAG_IS_PERMANENT_REDIRECT = 2;
  // serialize indvidual flag information per URLFPV2 if present
  public static final int  FLAG_SERIALIZE_URLFP_FLAGS = 4;
  // serialize link count variant
  public static final int  FLAG_SERIALIZE_LINK_COUNT  = 8;
  // serialize timestamp
  public static final int  FLAG_SERIALIZE_TIMESTAMP   = 16;

  /**
   * The builder, used to build the list
   * 
   * @author rana
   * 
   */
  public static class Builder {

    private TreeMultimap<Long, URLFPV2> _links = TreeMultimap.create();
    private int                         _flags = 0;

    /**
     * construct a link builder
     * 
     * @param archiveSegmentId
     *          - true if you want to preserve document versions in the list
     */

    public Builder(int flags) {
      _flags = flags;
    }

    /** add a fingerprint item to the builder **/
    public void addLink(URLFPV2 linkItem) {
      _links.get(linkItem.getRootDomainHash()).add(linkItem);
    }

    /** get the link map **/
    public TreeMultimap<Long, URLFPV2> getLinkMap() {
      return _links;
    }

    /** do we need to serialize URLFPV2 flags **/
    public boolean serializeURLFPFlags() {
      return (_flags & FLAG_SERIALIZE_URLFP_FLAGS) != 0;
    }

    /** serialize the fingerprint's timestamp value **/
    public boolean serializeTimestamp() {
      return (_flags & FLAG_SERIALIZE_TIMESTAMP) != 0;
    }

    private void flushRootDomainItems(long rootDomainHash,
        Collection<URLFPV2> items, DataOutputStream dataOut, BitStream bitStream)
        throws IOException {

      boolean allItemsRootItems = true;
      boolean allItemsBelongToSameDomain = true;
      URLFPV2 firstSubDomainItem = null;

      // fast scan for special edge cases
      for (URLFPV2 item : items) {

        if (firstSubDomainItem == null) {
          firstSubDomainItem = item;
        }

        if (allItemsRootItems) {
          if (item.getDomainHash() != rootDomainHash) {
            allItemsRootItems = false;
          }
        }
        if (allItemsBelongToSameDomain && item != firstSubDomainItem) {
          if (item.getDomainHash() != firstSubDomainItem.getDomainHash()) {
            allItemsBelongToSameDomain = false;
            allItemsRootItems = false;
          }
        }
      }

      if (allItemsRootItems) {

        bitStream.addbit(1); // bit indicating all items belong to root domain
                             // ..
        WritableUtils.writeVLong(dataOut, rootDomainHash);
        WritableUtils.writeVInt(dataOut, items.size());
        if (serializeURLFPFlags()) {
          serializeURLFPFlagStates(dataOut, items);
        }

        for (URLFPV2 item : items) {
          WritableUtils.writeVLong(dataOut, item.getUrlHash());
          if ((_flags & FLAG_ARCHIVE_SEGMENT_ID) != 0) {
            WritableUtils.writeVInt(dataOut, item.getParseSegmentId());
          }
          // serialize flags if necessary
          if (serializeURLFPFlags() && (item.getFlags() & Short.MAX_VALUE) != 0) {
            WritableUtils.writeVInt(dataOut,
                (item.getFlags() & Short.MAX_VALUE));
          }
          if (serializeTimestamp()) {
            WritableUtils.writeVInt(dataOut, item.getTimestampInSeconds());
          }
        }
      } else if (allItemsBelongToSameDomain) {
        bitStream.addbit(0); // bit indicating items belong to sub-domains ..
        WritableUtils.writeVLong(dataOut, rootDomainHash);
        WritableUtils.writeVInt(dataOut, 1);// sub domain count
        WritableUtils.writeVLong(dataOut, firstSubDomainItem.getDomainHash()); // sub
                                                                               // domain
                                                                               // hash
        WritableUtils.writeVInt(dataOut, items.size()); // url count
        if (serializeURLFPFlags()) {
          serializeURLFPFlagStates(dataOut, items);
        }
        for (URLFPV2 item : items) {
          WritableUtils.writeVLong(dataOut, item.getUrlHash());
          if ((_flags & FLAG_ARCHIVE_SEGMENT_ID) != 0) {
            WritableUtils.writeVInt(dataOut, item.getParseSegmentId());
          }
          // serialize flags if necessary
          if (serializeURLFPFlags() && (item.getFlags() & Short.MAX_VALUE) != 0) {
            WritableUtils.writeVInt(dataOut,
                (item.getFlags() & Short.MAX_VALUE));
          }
          if (serializeTimestamp()) {
            WritableUtils.writeVInt(dataOut, item.getTimestampInSeconds());
          }
        }
      } else {

        TreeMultimap<Long, URLFPV2> subDomainMap = TreeMultimap.create();

        // ok the long path ...
        for (URLFPV2 item : items) {
          // add to sorted multi-map
          subDomainMap.put(item.getDomainHash(), item);
        }

        bitStream.addbit(0); // bit indicating items belong to sub-domains ..
        WritableUtils.writeVLong(dataOut, rootDomainHash);
        Set<Map.Entry<Long, Collection<URLFPV2>>> entrySet = subDomainMap
            .asMap().entrySet();
        WritableUtils.writeVInt(dataOut, entrySet.size());// sub domain count
        // iterate set ...
        for (Map.Entry<Long, Collection<URLFPV2>> entry : entrySet) {
          WritableUtils.writeVLong(dataOut, entry.getKey()); // sub domain hash
          WritableUtils.writeVInt(dataOut, entry.getValue().size());
          if (serializeURLFPFlags()) {
            serializeURLFPFlagStates(dataOut, entry.getValue());
          }

          for (URLFPV2 fpItem : entry.getValue()) {
            WritableUtils.writeVLong(dataOut, fpItem.getUrlHash());
            if ((_flags & FLAG_ARCHIVE_SEGMENT_ID) != 0) {
              WritableUtils.writeVInt(dataOut, fpItem.getParseSegmentId());
            }
            // serialize flags if necessary
            if (serializeURLFPFlags()
                && (fpItem.getFlags() & Short.MAX_VALUE) != 0) {
              WritableUtils.writeVInt(dataOut,
                  (fpItem.getFlags() & Short.MAX_VALUE));
            }
            if (serializeTimestamp()) {
              WritableUtils.writeVInt(dataOut, fpItem.getTimestampInSeconds());
            }
          }
        }
      }
    }

    /** flush the link stream to the specified output stream **/
    public void flush(OutputStream os) throws IOException {

      TreeMultimap<Long, URLFPV2> subDomainMap = TreeMultimap.create();
      DataOutputBuffer dataOut = new DataOutputBuffer();
      BitStream bitStream = new BitStream();

      // get the root domain entry set
      Set<Map.Entry<Long, Collection<URLFPV2>>> entrySet = _links.asMap()
          .entrySet();

      WritableUtils.writeVInt(dataOut, entrySet.size());

      // iterate entires ...
      for (Map.Entry<Long, Collection<URLFPV2>> entry : entrySet) {
        // flush items in this root domain ...
        flushRootDomainItems(entry.getKey(), entry.getValue(), dataOut,
            bitStream);
      }

      DataOutputStream finalDataOut = new DataOutputStream(os);

      // write out header ...
      finalDataOut.writeByte(FPList_Version);
      // write out archive versions flag
      WritableUtils.writeVInt(finalDataOut, _flags);
      // flush bit control stream ...
      finalDataOut.writeInt(bitStream.nbits);
      finalDataOut.write(bitStream.bits, 0, (bitStream.nbits + 7) / 8);
      // and write out remaing data stream
      finalDataOut.write(dataOut.getData(), 0, dataOut.getLength());
    }

    private void serializeURLFPFlagStates(DataOutputStream dataOut,
        Collection<URLFPV2> itemList) throws IOException {
      // ok first pass see if there is a need to serialize flag states ...
      boolean serializeFlagStates = false;
      for (URLFPV2 item : itemList) {
        if ((item.getFlags() & Short.MAX_VALUE) != 0) {
          serializeFlagStates = true;
          break;
        }
      }

      if (!serializeFlagStates) {
        dataOut.write(0);
      } else {
        BitStream bitStream = new BitStream();
        // add first single bit to set a non-zero first byte
        bitStream.addbit(1);
        for (URLFPV2 item : itemList) {
          bitStream.addbit(((item.getFlags() & Short.MAX_VALUE) != 0) ? 1 : 0);
        }
        dataOut.write(bitStream.bits, 0, (bitStream.nbits + 7) / 8);
      }
    }
  }

  /**
   * reader - used to read compressed fingerprint list ...
   * 
   * @author rana
   * 
   */
  public static final class Reader {

    private DataInputStream _din                        = null;
    private int             _flags;
    private long            _currentRootDomainHash;
    private long            _currentSubDomainHash;
    private int             _currentRootIdx             = -1;
    private int             _rootIdxCount               = -1;
    private BitStream       _bitStream;
    private BitStreamReader _bitReader;
    private boolean         _currentDomainHasSubDomains = false;
    private int             _currentSubDomainIdx        = -1;
    private int             _currentSubDomainCount      = -1;
    private int             _currentURLIdx              = -1;
    private int             _currentURLCount            = -1;
    private BitStreamReader _flagStateBitStreamReader   = null;

    /**
     * initialize a reader to read and decode the link data stream
     * 
     * @param stream
     *          the previously encoded link data stream
     * @throws IOException
     */
    public Reader(InputStream stream) throws IOException {
      _din = new DataInputStream(stream);
    }

    public Reader(DataInputStream stream) throws IOException {
      _din = stream;
      // skip version
      _din.read();
      // read flags
      _flags = WritableUtils.readVInt(_din);
      // read bit stream ...
      _bitStream = new BitStream();
      _bitStream.nbits = _din.readInt();
      _bitStream.bits = new byte[(_bitStream.nbits + 7) / 8];
      _din.read(_bitStream.bits);
      _bitReader = new BitStreamReader(_bitStream);
      // read root index count...
      _rootIdxCount = WritableUtils.readVInt(_din);
      // reset current root index ...
      _currentRootIdx = -1;
    }

    /** get the stream flags **/
    public int getStreamFlags() {
      return _flags;
    }

    private void readURLFPFlagStates(int urlCount) throws IOException {
      // read first byte ...
      byte firstByte = _din.readByte();
      if (firstByte != 0) {
        // LOG.info("first byte non-zero - reading FPFlag States");
        // ok there is an embedded bit stream
        // figure out how many more bytes we need to read
        BitStream flagStateBitStream = new BitStream();
        flagStateBitStream.bits = new byte[((urlCount + 1 + 7) / 8)];
        flagStateBitStream.bits[0] = firstByte;
        if (flagStateBitStream.bits.length - 1 != 0) {
          // read remaining bytes ...
          _din.read(flagStateBitStream.bits, 1,
              flagStateBitStream.bits.length - 1);
        }
        // and initialize reader ...
        _flagStateBitStreamReader = new BitStreamReader(flagStateBitStream);
        // and skip first bit
        _flagStateBitStreamReader.getbit();
      } else {
        // LOG.info("first byte zero - skipping FPFlag States");
        _flagStateBitStreamReader = null;
      }
    }

    private void readLinkCountStates(int urlCount) throws IOException {
      // read first byte ...
      byte firstByte = _din.readByte();
      if (firstByte != 0) {
        // LOG.info("first byte non-zero - reading FPFlag States");
        // ok there is an embedded bit stream
        // figure out how many more bytes we need to read
        BitStream linkCountBitStream = new BitStream();
        linkCountBitStream.bits = new byte[((urlCount + 1 + 7) / 8)];
        linkCountBitStream.bits[0] = firstByte;
        if (linkCountBitStream.bits.length - 1 != 0) {
          // read remaining bytes ...
          _din.read(linkCountBitStream.bits, 1,
              linkCountBitStream.bits.length - 1);
        }
      } else {
        // LOG.info("first byte zero - skipping FPFlag States");
      }
    }

    /**
     * checks to see if there is more data in the stream
     * 
     * @return true if another item can be read from the stream
     * @throws IOException
     */
    public boolean hasNext() throws IOException {

      while (_currentRootIdx < _rootIdxCount) {

        if (++_currentURLIdx < _currentURLCount) {
          // LOG.info("urlIdx:" + _currentURLIdx + " Max:" + _currentURLCount);
          return true;
        } else if (++_currentSubDomainIdx < _currentSubDomainCount) {
          _currentURLIdx = -1;
          if (_currentDomainHasSubDomains) {
            // read sub domain hash
            _currentSubDomainHash = WritableUtils.readVLong(_din);
          } else {
            _currentSubDomainHash = _currentRootDomainHash;
          }
          _currentURLCount = WritableUtils.readVInt(_din);
          // LOG.info("subDomainIdx:" + _currentSubDomainIdx + " URLCount:" +
          // _currentURLCount);
          // if this stream has URLFPV2 flags
          if (hasURLFPFlags()) {
            readURLFPFlagStates(_currentURLCount);
          }
          if (hasLinkCount()) {
            readLinkCountStates(_currentURLCount);
          }
        } else {
          if (++_currentRootIdx < _rootIdxCount) {
            _currentSubDomainIdx = -1;
            _currentURLIdx = -1;
            _currentURLCount = -1;
            _currentDomainHasSubDomains = (_bitReader.getbit() == 0);
            _currentRootDomainHash = WritableUtils.readVLong(_din);
            if (_currentDomainHasSubDomains) {
              _currentSubDomainCount = WritableUtils.readVInt(_din);
            } else {
              _currentSubDomainCount = 1;
            }
          }
        }
      }
      return false;
    }

    /**
     * does this list have serialized flag information per url
     * 
     */
    private boolean hasURLFPFlags() {
      return (_flags & FLAG_SERIALIZE_URLFP_FLAGS) != 0;
    }

    private boolean hasLinkCount() {
      return (_flags & FLAG_SERIALIZE_LINK_COUNT) != 0;
    }

    private boolean hasTimestamp() {
      return (_flags & FLAG_SERIALIZE_TIMESTAMP) != 0;
    }

    /**
     * reads a nd returns the next URLFPV2 object in the data stream
     * 
     * @return URLFPV2 object
     * @throws IOException
     */

    public URLFPV2 next() throws IOException {
      URLFPV2 urlFPOut = new URLFPV2();
      if (!_currentDomainHasSubDomains) {
        urlFPOut.setRootDomainHash(_currentRootDomainHash);
        urlFPOut.setDomainHash(_currentRootDomainHash);
      } else {
        urlFPOut.setRootDomainHash(_currentRootDomainHash);
        urlFPOut.setDomainHash(_currentSubDomainHash);
      }

      urlFPOut.setUrlHash(WritableUtils.readVLong(_din));
      if ((_flags & FLAG_ARCHIVE_SEGMENT_ID) != 0) {
        urlFPOut.setParseSegmentId(WritableUtils.readVInt(_din));
      }
      // if stream has URLFPV2 flags && this subset has individual flag state
      // info
      if (hasURLFPFlags() && _flagStateBitStreamReader != null) {
        if (_flagStateBitStreamReader.getbit() == 1) {
          urlFPOut.setFlags(WritableUtils.readVInt(_din));
        }
      }
      if (hasTimestamp()) {
        urlFPOut.setTimestampInSeconds((int) (WritableUtils.readVInt(_din)));
      }

      return urlFPOut;
    }

    public void close() throws IOException {
      _din.close();
    }
  }

  private static URLFPV2 insertURLFPItem(TreeMultimap<Long, URLFPV2> map,
      String url, int parseSegmentId) {
    return insertURLFPItem(map, url, parseSegmentId, 0);
  }

  private static URLFPV2 insertURLFPItem(TreeMultimap<Long, URLFPV2> map,
      String url, int parseSegmentId, int flags) {

    URLFPV2 fpOut = URLUtils.getURLFPV2FromURL(url);

    if (fpOut != null) {

      fpOut.setParseSegmentId(parseSegmentId);
      fpOut.setFlags(flags);

      map.put(fpOut.getRootDomainHash(), fpOut);
    }

    return fpOut;
  }

  private static void addMapToBuilder(Builder builder,
      TreeMultimap<Long, URLFPV2> map) {
    for (Map.Entry<Long, URLFPV2> entry : map.entries()) {
      builder.addLink(entry.getValue());
    }
  }

  public static final String DOMAIN_1_SUBDOMAIN_1_URL_1 = "http://news.google.com/foo/bar/z";
  public static final String DOMAIN_1_SUBDOMAIN_1_URL_2 = "http://news.google.com/zzz";

  public static final String DOMAIN_2_SUBDOMAIN_1_URL_1 = "http://cnn.com/foo/bar/z";
  public static final String DOMAIN_2_SUBDOMAIN_1_URL_2 = "http://cnn.com/zzzz";

  public static final String DOMAIN_3_SUBDOMAIN_1_URL_1 = "http://news.abc.com/url1";
  public static final String DOMAIN_3_SUBDOMAIN_1_URL_2 = "http://news.abc.com/url2";
  public static final String DOMAIN_3_SUBDOMAIN_2_URL_1 = "http://cartoons.abc.com/url1";
  public static final String DOMAIN_3_SUBDOMAIN_2_URL_2 = "http://cartoons.abc.com/url2";

  public static void main(String[] args) {

    // initialize ...
    final Configuration conf = new Configuration();

    conf.addResource("nutch-default.xml");
    conf.addResource("nutch-site.xml");
    conf.addResource("core-site.xml");
    conf.addResource("hdfs-site.xml");
    conf.addResource("mapred-site.xml");

    BasicConfigurator.configure();
    CrawlEnvironment.setHadoopConfig(conf);

    try {
      FileSystem fs = CrawlEnvironment.getDefaultFileSystem();

      Path testFile = new Path(
          "crawl/linkdb/merged1282844121161/linkData/part-00000");
      SequenceFile.Reader reader = new SequenceFile.Reader(fs, testFile, conf);

      URLFPV2 fp = new URLFPV2();
      BytesWritable bytes = new BytesWritable();

      while (reader.next(fp, bytes)) {
        if (bytes.getLength() != 0) {
          DataInputBuffer inputStream = new DataInputBuffer();
          inputStream.reset(bytes.getBytes(), bytes.getLength());
          CompressedURLFPListV2.Reader listReader = new CompressedURLFPListV2.Reader(
              inputStream);
          while (listReader.hasNext()) {
            URLFPV2 nextFP = listReader.next();
            LOG.info("DH:" + nextFP.getDomainHash() + " UH:"
                + nextFP.getUrlHash());
          }
        } else {
          LOG.error("ZERO BYTE LIST!");
        }
      }

      reader.close();
    } catch (IOException e) {
      LOG.error(CCStringUtils.stringifyException(e));
    }

    if (1 == 1)
      return;

    validateDuplicateChecking();
    // validateReallyBigList();
    validateURLFPSerializationRootDomain();
    validateURLFPSerializationSingleSubDomain();
    validateURLFPSerializationMultiDomain();
    validateURLFPFlagSerializationRootDomain();
    validateURLFPFlagSerializationMultipleSubDomains();
    validateURLFPFlagSerializationOneSubDomain();
  }

  public static void validateReallyBigList() {
    Builder builder = new Builder(FLAG_ARCHIVE_SEGMENT_ID
        | FLAG_SERIALIZE_URLFP_FLAGS);
    for (int rootDomain = 0; rootDomain < 1000; ++rootDomain) {
      for (long urlfphash = 0; urlfphash < 1000; ++urlfphash) {
        URLFPV2 foo = new URLFPV2();
        foo.setDomainHash(rootDomain);
        foo.setRootDomainHash(rootDomain);
        foo.setUrlHash(urlfphash);

        builder.addLink(foo);
      }
    }

    ByteBufferOutputStream bufferOut = new ByteBufferOutputStream();
    try {
      builder.flush(bufferOut);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void validateDuplicateChecking() {
    TreeMultimap<Long, URLFPV2> sourceMap = TreeMultimap.create();
    TreeMultimap<Long, URLFPV2> destMap = TreeMultimap.create();

    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    Builder firstBuilder = new Builder(FLAG_ARCHIVE_SEGMENT_ID
        | FLAG_SERIALIZE_URLFP_FLAGS);

    insertURLFPItem(sourceMap, "http://www.google.com/hello", 1, 255);
    insertURLFPItem(sourceMap, "http://google.com/hello", 1, 255);
    insertURLFPItem(sourceMap, "http://google.com/foobar", 1, 255);
    insertURLFPItem(sourceMap, "http://www.google.com/hello", 1, 255);

    addMapToBuilder(firstBuilder, sourceMap);

    try {
      // flush to byte stream ...
      firstBuilder.flush(byteStream);
      // now set up to read the stream
      ByteArrayInputStream inputStream = new ByteArrayInputStream(byteStream
          .toByteArray(), 0, byteStream.size());
      Reader reader = new Reader(inputStream);

      while (reader.hasNext()) {
        URLFPV2 fp = reader.next();
        destMap.put(fp.getRootDomainHash(), fp);
      }
      reader.close();

      for (long rootDomain : sourceMap.keySet()) {
        for (URLFPV2 URLFPV2 : sourceMap.get(rootDomain)) {
          System.out.println("SourceFP Root:" + URLFPV2.getRootDomainHash()
              + " Domain:" + URLFPV2.getDomainHash() + " URL:"
              + URLFPV2.getUrlHash());
        }
      }
      for (long rootDomain : destMap.keySet()) {
        for (URLFPV2 URLFPV2 : destMap.get(rootDomain)) {
          System.out.println("DestFP Root:" + URLFPV2.getRootDomainHash()
              + " Domain:" + URLFPV2.getDomainHash() + " URL:"
              + URLFPV2.getUrlHash());
        }
      }

      Assert.assertTrue(sourceMap.equals(destMap));

    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  public static void validateURLFPFlagSerializationOneSubDomain() {
    TreeMultimap<Long, URLFPV2> sourceMap = TreeMultimap.create();
    TreeMultimap<Long, URLFPV2> destMap = TreeMultimap.create();

    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    Builder firstBuilder = new Builder(FLAG_ARCHIVE_SEGMENT_ID
        | FLAG_SERIALIZE_URLFP_FLAGS);

    insertURLFPItem(sourceMap, DOMAIN_1_SUBDOMAIN_1_URL_1 + "0", 1, 255);
    insertURLFPItem(sourceMap, DOMAIN_1_SUBDOMAIN_1_URL_1 + "1", 2, 255);
    insertURLFPItem(sourceMap, DOMAIN_1_SUBDOMAIN_1_URL_1 + "2", 3, 255);
    insertURLFPItem(sourceMap, DOMAIN_1_SUBDOMAIN_1_URL_1 + "3", 4, 0);
    insertURLFPItem(sourceMap, DOMAIN_1_SUBDOMAIN_1_URL_1 + "4", 5, 0);
    insertURLFPItem(sourceMap, DOMAIN_1_SUBDOMAIN_1_URL_1 + "5", 6, 0);
    insertURLFPItem(sourceMap, DOMAIN_1_SUBDOMAIN_1_URL_1 + "6", 7, 255);
    insertURLFPItem(sourceMap, DOMAIN_1_SUBDOMAIN_1_URL_1 + "7", 8, 255);
    insertURLFPItem(sourceMap, DOMAIN_1_SUBDOMAIN_1_URL_1 + "8", 9, 255);

    addMapToBuilder(firstBuilder, sourceMap);

    try {
      // flush to byte stream ...
      firstBuilder.flush(byteStream);
      // now set up to read the stream
      ByteArrayInputStream inputStream = new ByteArrayInputStream(byteStream
          .toByteArray(), 0, byteStream.size());
      Reader reader = new Reader(inputStream);

      while (reader.hasNext()) {
        URLFPV2 fp = reader.next();
        destMap.put(fp.getRootDomainHash(), fp);
      }
      reader.close();

      Assert.assertTrue(sourceMap.equals(destMap));

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void validateURLFPFlagSerializationRootDomain() {
    TreeMultimap<Long, URLFPV2> sourceMap = TreeMultimap.create();
    TreeMultimap<Long, URLFPV2> destMap = TreeMultimap.create();

    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    Builder firstBuilder = new Builder(FLAG_ARCHIVE_SEGMENT_ID
        | FLAG_SERIALIZE_URLFP_FLAGS);

    for (int i = 0; i < 12; ++i) {
      insertURLFPItem(sourceMap, DOMAIN_2_SUBDOMAIN_1_URL_1 + "0_" + i, 1,
          (255 | (65535 << 16)));
      insertURLFPItem(sourceMap, DOMAIN_2_SUBDOMAIN_1_URL_1 + "1_" + i, 2,
          (255 | (65535 << 16)));
      insertURLFPItem(sourceMap, DOMAIN_2_SUBDOMAIN_1_URL_1 + "2_" + i, 3,
          (255 | (65535 << 16)));
      insertURLFPItem(sourceMap, DOMAIN_2_SUBDOMAIN_1_URL_1 + "3_" + i, 4, 0);
      insertURLFPItem(sourceMap, DOMAIN_2_SUBDOMAIN_1_URL_1 + "4_" + i, 5, 0);
      insertURLFPItem(sourceMap, DOMAIN_2_SUBDOMAIN_1_URL_1 + "5_" + i, 6, 0);
      insertURLFPItem(sourceMap, DOMAIN_2_SUBDOMAIN_1_URL_1 + "6_" + i, 7,
          (255 | (65535 << 16)));
      insertURLFPItem(sourceMap, DOMAIN_2_SUBDOMAIN_1_URL_1 + "7_" + i, 8, 255);
    }
    insertURLFPItem(sourceMap, DOMAIN_2_SUBDOMAIN_1_URL_1 + "8", 8, 255);

    addMapToBuilder(firstBuilder, sourceMap);

    try {
      // flush to byte stream ...
      firstBuilder.flush(byteStream);
      // now set up to read the stream
      ByteArrayInputStream inputStream = new ByteArrayInputStream(byteStream
          .toByteArray(), 0, byteStream.size());
      Reader reader = new Reader(inputStream);

      while (reader.hasNext()) {
        URLFPV2 fp = reader.next();
        destMap.put(fp.getRootDomainHash(), fp);
      }
      reader.close();

      Assert.assertTrue(sourceMap.equals(destMap));

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void validateURLFPFlagSerializationMultipleSubDomains() {
    TreeMultimap<Long, URLFPV2> sourceMap = TreeMultimap.create();
    TreeMultimap<Long, URLFPV2> destMap = TreeMultimap.create();

    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    Builder firstBuilder = new Builder(FLAG_ARCHIVE_SEGMENT_ID
        | FLAG_SERIALIZE_URLFP_FLAGS);

    insertURLFPItem(sourceMap, DOMAIN_3_SUBDOMAIN_1_URL_1 + "0", 1, 255);
    insertURLFPItem(sourceMap, DOMAIN_3_SUBDOMAIN_2_URL_1 + "1", 2, 255);
    insertURLFPItem(sourceMap, DOMAIN_3_SUBDOMAIN_1_URL_1 + "2", 3, 255);
    insertURLFPItem(sourceMap, DOMAIN_3_SUBDOMAIN_2_URL_1 + "3", 4, 0);
    insertURLFPItem(sourceMap, DOMAIN_3_SUBDOMAIN_1_URL_1 + "4", 1, 255);
    insertURLFPItem(sourceMap, DOMAIN_3_SUBDOMAIN_2_URL_1 + "5", 2, 255);
    insertURLFPItem(sourceMap, DOMAIN_3_SUBDOMAIN_1_URL_1 + "6", 3, 255);
    insertURLFPItem(sourceMap, DOMAIN_3_SUBDOMAIN_2_URL_1 + "7", 4, 0);
    insertURLFPItem(sourceMap, DOMAIN_3_SUBDOMAIN_1_URL_1 + "8", 1, 255);
    insertURLFPItem(sourceMap, DOMAIN_3_SUBDOMAIN_2_URL_1 + "9", 2, 255);
    insertURLFPItem(sourceMap, DOMAIN_3_SUBDOMAIN_1_URL_1 + "10", 3, 255);
    insertURLFPItem(sourceMap, DOMAIN_3_SUBDOMAIN_2_URL_1 + "11", 4, 0);

    addMapToBuilder(firstBuilder, sourceMap);

    try {
      // flush to byte stream ...
      firstBuilder.flush(byteStream);
      // now set up to read the stream
      ByteArrayInputStream inputStream = new ByteArrayInputStream(byteStream
          .toByteArray(), 0, byteStream.size());
      Reader reader = new Reader(inputStream);

      while (reader.hasNext()) {
        URLFPV2 fp = reader.next();
        destMap.put(fp.getRootDomainHash(), fp);
      }
      reader.close();

      Assert.assertTrue(sourceMap.equals(destMap));

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void validateURLFPSerializationMultiDomain() {

    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    Builder firstBuilder = new Builder(FLAG_ARCHIVE_SEGMENT_ID);

    TreeMultimap<Long, URLFPV2> sourceMap = TreeMultimap.create();
    TreeMultimap<Long, URLFPV2> destMap = TreeMultimap.create();

    // single top level domain with one sub domain an two urls
    insertURLFPItem(sourceMap, DOMAIN_1_SUBDOMAIN_1_URL_1, 1);
    insertURLFPItem(sourceMap, DOMAIN_1_SUBDOMAIN_1_URL_2, 1);

    // top level domain with matching sub domain an two urls
    insertURLFPItem(sourceMap, DOMAIN_2_SUBDOMAIN_1_URL_1, 1);
    insertURLFPItem(sourceMap, DOMAIN_2_SUBDOMAIN_1_URL_2, 1);

    // two sub domains with two urls each
    insertURLFPItem(sourceMap, DOMAIN_3_SUBDOMAIN_1_URL_1, 1);
    insertURLFPItem(sourceMap, DOMAIN_3_SUBDOMAIN_1_URL_2, 1);
    insertURLFPItem(sourceMap, DOMAIN_3_SUBDOMAIN_2_URL_1, 1);
    insertURLFPItem(sourceMap, DOMAIN_3_SUBDOMAIN_2_URL_2, 1);

    addMapToBuilder(firstBuilder, sourceMap);

    try {
      // flush to byte stream ...
      firstBuilder.flush(byteStream);
      // now set up to read the stream
      ByteArrayInputStream inputStream = new ByteArrayInputStream(byteStream
          .toByteArray(), 0, byteStream.size());
      Reader reader = new Reader(inputStream);

      while (reader.hasNext()) {
        URLFPV2 fp = reader.next();
        destMap.put(fp.getRootDomainHash(), fp);
      }
      reader.close();

      // dump both lists
      for (long rootDomain : sourceMap.keySet()) {
        for (URLFPV2 URLFPV2 : sourceMap.get(rootDomain)) {
          System.out.println("SourceFP Root:" + URLFPV2.getRootDomainHash()
              + " Domain:" + URLFPV2.getDomainHash() + " URL:"
              + URLFPV2.getUrlHash());
        }
      }
      for (long rootDomain : destMap.keySet()) {
        for (URLFPV2 URLFPV2 : destMap.get(rootDomain)) {
          System.out.println("DestFP Root:" + URLFPV2.getRootDomainHash()
              + " Domain:" + URLFPV2.getDomainHash() + " URL:"
              + URLFPV2.getUrlHash());
        }
      }

      Assert.assertTrue(sourceMap.equals(destMap));

    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }

  public static void validateURLFPSerializationSingleSubDomain() {

    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    Builder firstBuilder = new Builder(FLAG_ARCHIVE_SEGMENT_ID);

    TreeMultimap<Long, URLFPV2> sourceMap = TreeMultimap.create();
    TreeMultimap<Long, URLFPV2> destMap = TreeMultimap.create();

    // single top level domain with one sub domain an two urls
    insertURLFPItem(sourceMap, DOMAIN_1_SUBDOMAIN_1_URL_1 + "0", 1);
    insertURLFPItem(sourceMap, DOMAIN_1_SUBDOMAIN_1_URL_2 + "1", 1);

    // single top level domain with one sub domain an two urls
    insertURLFPItem(sourceMap, DOMAIN_1_SUBDOMAIN_1_URL_1 + "2", 1);
    insertURLFPItem(sourceMap, DOMAIN_1_SUBDOMAIN_1_URL_2 + "3", 1);

    // single top level domain with one sub domain an two urls
    insertURLFPItem(sourceMap, DOMAIN_1_SUBDOMAIN_1_URL_1 + "4", 1);
    insertURLFPItem(sourceMap, DOMAIN_1_SUBDOMAIN_1_URL_2 + "5", 1);

    // single top level domain with one sub domain an two urls
    insertURLFPItem(sourceMap, DOMAIN_1_SUBDOMAIN_1_URL_1 + "6", 1);
    insertURLFPItem(sourceMap, DOMAIN_1_SUBDOMAIN_1_URL_2 + "7", 1);

    // single top level domain with one sub domain an two urls
    insertURLFPItem(sourceMap, DOMAIN_1_SUBDOMAIN_1_URL_1 + "8", 1);
    insertURLFPItem(sourceMap, DOMAIN_1_SUBDOMAIN_1_URL_2 + "9", 1);

    addMapToBuilder(firstBuilder, sourceMap);

    try {
      // flush to byte stream ...
      firstBuilder.flush(byteStream);
      // now set up to read the stream
      ByteArrayInputStream inputStream = new ByteArrayInputStream(byteStream
          .toByteArray(), 0, byteStream.size());
      Reader reader = new Reader(inputStream);

      while (reader.hasNext()) {
        URLFPV2 fp = reader.next();
        destMap.put(fp.getRootDomainHash(), fp);
      }
      reader.close();

      // dump both lists
      for (long rootDomain : sourceMap.keySet()) {
        for (URLFPV2 URLFPV2 : sourceMap.get(rootDomain)) {
          System.out.println("SourceFP Root:" + URLFPV2.getRootDomainHash()
              + " Domain:" + URLFPV2.getDomainHash() + " URL:"
              + URLFPV2.getUrlHash());
        }
      }
      for (long rootDomain : destMap.keySet()) {
        for (URLFPV2 URLFPV2 : destMap.get(rootDomain)) {
          System.out.println("DestFP Root:" + URLFPV2.getRootDomainHash()
              + " Domain:" + URLFPV2.getDomainHash() + " URL:"
              + URLFPV2.getUrlHash());
        }
      }

      Assert.assertTrue(sourceMap.equals(destMap));

    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }

  public static void validateURLFPSerializationRootDomain() {

    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    Builder firstBuilder = new Builder(FLAG_ARCHIVE_SEGMENT_ID);

    TreeMultimap<Long, URLFPV2> sourceMap = TreeMultimap.create();
    TreeMultimap<Long, URLFPV2> destMap = TreeMultimap.create();

    // top level domain with matching sub domain an two urls
    insertURLFPItem(sourceMap, DOMAIN_2_SUBDOMAIN_1_URL_1 + "0", 1);
    insertURLFPItem(sourceMap, DOMAIN_2_SUBDOMAIN_1_URL_2 + "1", 1);
    // top level domain with matching sub domain an two urls
    insertURLFPItem(sourceMap, DOMAIN_2_SUBDOMAIN_1_URL_1 + "2", 1);
    insertURLFPItem(sourceMap, DOMAIN_2_SUBDOMAIN_1_URL_2 + "3", 1);
    // top level domain with matching sub domain an two urls
    insertURLFPItem(sourceMap, DOMAIN_2_SUBDOMAIN_1_URL_1 + "4", 1);
    insertURLFPItem(sourceMap, DOMAIN_2_SUBDOMAIN_1_URL_2 + "5", 1);

    addMapToBuilder(firstBuilder, sourceMap);

    try {
      // flush to byte stream ...
      firstBuilder.flush(byteStream);
      // now set up to read the stream
      ByteArrayInputStream inputStream = new ByteArrayInputStream(byteStream
          .toByteArray(), 0, byteStream.size());
      Reader reader = new Reader(inputStream);

      while (reader.hasNext()) {
        URLFPV2 fp = reader.next();
        destMap.put(fp.getRootDomainHash(), fp);
      }
      reader.close();

      // dump both lists
      for (long rootDomain : sourceMap.keySet()) {
        for (URLFPV2 URLFPV2 : sourceMap.get(rootDomain)) {
          System.out.println("SourceFP Root:" + URLFPV2.getRootDomainHash()
              + " Domain:" + URLFPV2.getDomainHash() + " URL:"
              + URLFPV2.getUrlHash());
        }
      }
      for (long rootDomain : destMap.keySet()) {
        for (URLFPV2 URLFPV2 : destMap.get(rootDomain)) {
          System.out.println("DestFP Root:" + URLFPV2.getRootDomainHash()
              + " Domain:" + URLFPV2.getDomainHash() + " URL:"
              + URLFPV2.getUrlHash());
        }
      }

      System.out.println("Buffer Size:" + byteStream.size() + " URLCount:"
          + firstBuilder.getLinkMap().size());

      Assert.assertTrue(sourceMap.equals(destMap));

    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

}
