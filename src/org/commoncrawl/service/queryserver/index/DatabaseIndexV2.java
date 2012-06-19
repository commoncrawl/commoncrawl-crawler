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

package org.commoncrawl.service.queryserver.index;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.SequenceFile.ValueBytes;
import org.apache.hadoop.io.file.tfile.TFile;
import org.apache.log4j.BasicConfigurator;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.mapred.pipelineV1.MetadataIndexBuilderV2;
import org.commoncrawl.mapred.pipelineV1.InverseLinksByDomainDBBuilder.ComplexKeyComparator;
import org.commoncrawl.hadoop.mergeutils.MergeSortSpillWriter;
import org.commoncrawl.hadoop.mergeutils.RawKeyValueComparator;
import org.commoncrawl.hadoop.mergeutils.SequenceFileSpillWriter;
import org.commoncrawl.protocol.CrawlDatumAndMetadata;
import org.commoncrawl.protocol.CrawlURLMetadata;
import org.commoncrawl.protocol.SubDomainMetadata;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.service.queryserver.ShardIndexHostNameTuple;
import org.commoncrawl.service.queryserver.index.DatabaseIndexV2.MasterDatabaseIndex.MetadataOut;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.CompressURLListV2;
import org.commoncrawl.util.CompressedURLFPListV2;
import org.commoncrawl.util.CrawlDatum;
import org.commoncrawl.util.FileUtils;
import org.commoncrawl.util.FlexBuffer;
import org.commoncrawl.util.NodeAffinityMaskBuilder;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLUtils;
import org.commoncrawl.util.Tuples.TriTextBytesTuple;
import org.commoncrawl.util.URLUtils.URLFPV2RawComparator;

import com.google.common.collect.TreeMultimap;

public class DatabaseIndexV2 {

  private static final Log LOG = LogFactory.getLog(DatabaseIndexV2.class);

  public static class MasterDatabaseIndex {

    Configuration                                           _conf;
    FileSystem                                              _remoteFS;
    int                                                     _driveCount;
    long                                                    _databaseTimestamp;
    ArrayList<ShardIndexHostNameTuple>                      _urlfpIndex_ShardMap                           = null;
    ArrayList<ShardIndexHostNameTuple>                      _stringToSubDomainIndex_ShardMap               = null;

    public static String                                    INDEX_NAME_URLFPV2                             = "URLFP_INDEX";
    public static String                                    INDEX_NAME_DOMAIN_NAME_TO_METADATA             = "DOMAIN_NAME_TO_METADATA";
    public static String                                    INDEX_NAME_DOMAIN_ID_TO_METADATA               = "DOMAIN_ID_TO_METADATA";
    public static String                                    INDEX_NAME_DOMAIN_ID_TO_URLLIST_SORTED_BY_NAME = "DOMAIN_ID_TO_URLLIST_BY_NAME";
    public static String                                    INDEX_NAME_DOMAIN_ID_TO_URLLIST_SORTED_BY_PR   = "DOMAIN_ID_TO_URLLIST_BY_PR";
    public static String                                    INDEX_NAME_OUTLINK_DATA                        = "OUTLINK_DATA";
    public static String                                    INDEX_NAME_INLINK_DATA                         = "INLINK_DATA";

    private Map<String, ArrayList<ShardIndexHostNameTuple>> _indexToShardMapping                           = new TreeMap<String, ArrayList<ShardIndexHostNameTuple>>();

    public MasterDatabaseIndex(Configuration conf, FileSystem remoteFS,
        int driveCount, long databaseTimestamp,Set<String> slavesList) throws IOException {
      _conf = conf;
      _remoteFS = remoteFS;
      _driveCount = driveCount;
      _databaseTimestamp = databaseTimestamp;

      // ok populate affinity map indexes
      _indexToShardMapping.put(INDEX_NAME_URLFPV2,
          buildShardMapFromAffinityMapGivenRootPath(new Path(
              "crawl/linkdb/merged" + _databaseTimestamp + "/linkMetadata"),slavesList));
      _indexToShardMapping.put(INDEX_NAME_OUTLINK_DATA,
          buildShardMapFromAffinityMapGivenRootPath(new Path(
              "crawl/linkdb/merged" + _databaseTimestamp + "/linkData"),slavesList));
      _indexToShardMapping
          .put(INDEX_NAME_INLINK_DATA,
              buildShardMapFromAffinityMapGivenRootPath(new Path(
                  "crawl/inverse_linkdb/merged" + _databaseTimestamp
                      + "/linkData"),slavesList));
      _indexToShardMapping.put(INDEX_NAME_DOMAIN_NAME_TO_METADATA,
          buildShardMapFromAffinityMapGivenRootPath(new Path(
              "crawl/metadatadb/" + _databaseTimestamp + "/subDomainMetadata/"
                  + MetadataIndexBuilderV2.SUBDOMAIN_INDEX_NAME_TO_METADATA),slavesList));
      _indexToShardMapping.put(INDEX_NAME_DOMAIN_ID_TO_METADATA,
          buildShardMapFromAffinityMapGivenRootPath(new Path(
              "crawl/metadatadb/" + _databaseTimestamp + "/subDomainMetadata/"
                  + MetadataIndexBuilderV2.SUBDOMAIN_INDEX_ID_TO_METADATA),slavesList));
      _indexToShardMapping.put(INDEX_NAME_DOMAIN_ID_TO_URLLIST_SORTED_BY_NAME,
          buildShardMapFromAffinityMapGivenRootPath(new Path(
              "crawl/querydb/db/" + _databaseTimestamp + "/indexedByURL"),slavesList));
      _indexToShardMapping.put(INDEX_NAME_DOMAIN_ID_TO_URLLIST_SORTED_BY_PR,
          buildShardMapFromAffinityMapGivenRootPath(new Path(
              "crawl/querydb/db/" + _databaseTimestamp + "/indexedByPR"),slavesList));
    }

    static ThreadLocal<NumberFormat> _numberFormat = new ThreadLocal<NumberFormat>() {
                                                     protected NumberFormat initialValue() {

                                                       NumberFormat formatOut = NumberFormat
                                                           .getInstance();

                                                       formatOut
                                                           .setMinimumIntegerDigits(5);
                                                       formatOut
                                                           .setGroupingUsed(false);

                                                       return formatOut;
                                                     };
                                                   };

    /**
     * return an shardIdToHost mapping given an index name
     * 
     * @param indexName
     * @return
     * @throws IOException
     */
    public final ArrayList<ShardIndexHostNameTuple> mapShardIdsForIndex(
        String indexName) throws IOException {
      return _indexToShardMapping.get(indexName);
    }

    public final TextBytes queryURLGivenURLFP(URLFPV2 fingerprint)
        throws IOException {
      // establish parition id
      int paritionId = (fingerprint.hashCode() & Integer.MAX_VALUE)
          % CrawlEnvironment.NUM_DB_SHARDS;
      // estalish drive index ...
      int driveIndex = paritionId % _driveCount;
      // establish path ..
      Path indexPath = new Path("/data/" + driveIndex + "/urldb/"
          + _databaseTimestamp + "/part-"
          + _numberFormat.get().format(paritionId) + ".index");

      CompressURLListV2.Index.IndexFile index = new CompressURLListV2.Index.IndexFile(
          new File(indexPath.toString()));

      return index.mapURLFPToURL(fingerprint, null);
    }

    public static class MetadataOut {
      // the url component
      public TextBytes url                   = new TextBytes();
      // the optimized data components
      // page rank
      public float     pageRank              = 0.0f;
      // fetch status
      public byte      fetchStatus           = -1;
      // protocol status
      public byte      protocolStatus        = -1;
      // fetch time
      public long      lastFetchTime         = -1;
      // and finally, optionally, the datum and metadata structure
      public TextBytes datumAndMetadataBytes = new TextBytes();
    }

    public final MetadataOut queryMetadataAndURLGivenFP(URLFPV2 fingerprint)
        throws IOException {

      FileSystem localFileSystem = FileSystem.getLocal(_conf);

      DataOutputBuffer keyData = new DataOutputBuffer();

      keyData.writeLong(fingerprint.getDomainHash());
      keyData.writeLong(fingerprint.getUrlHash());

      // establish parition id
      int paritionId = (fingerprint.hashCode() & Integer.MAX_VALUE)
          % CrawlEnvironment.NUM_DB_SHARDS;
      // estalish drive index ...
      int driveIndex = paritionId % _driveCount;
      // establish path ..
      Path indexPath = new Path("/data/" + driveIndex + "/metadata/"
          + _databaseTimestamp + "/part-"
          + _numberFormat.get().format(paritionId) + ".index");

      CompressURLListV2.Index.IndexFile index = new CompressURLListV2.Index.IndexFile(
          new File(indexPath.toString()));

      TextBytes dataOut = index.mapURLFPToURL(fingerprint, null);

      if (dataOut != null) {

        DataInputBuffer readerStream = new DataInputBuffer();

        readerStream.reset(dataOut.getBytes(), dataOut.getOffset(), dataOut
            .getLength());

        MetadataOut metadataOut = new MetadataOut();

        // LOG.info("**Data Length:" + dataOut.getLength());
        int urlLength = WritableUtils.readVInt(readerStream);
        // LOG.info("**URL BYTES Length:" + urlLength + " ReaderPos:" +
        // readerStream.getPosition());
        // set text bytes
        metadataOut.url.set(dataOut.getBytes(), readerStream.getPosition(),
            urlLength);
        // advance past url bytes.
        readerStream.skip(urlLength);
        int otherBytes = WritableUtils.readVInt(readerStream);
        // LOG.info("**OTHER BYTES Length:" + otherBytes);
        // ok see if other bytes is valid
        if (otherBytes != 0) {
          // ok read optimized data
          metadataOut.pageRank = readerStream.readFloat();
          metadataOut.fetchStatus = readerStream.readByte();
          metadataOut.protocolStatus = readerStream.readByte();
          metadataOut.lastFetchTime = readerStream.readLong();
        }
        // ok read in metadata length
        int metadataBytes = WritableUtils.readVInt(readerStream);
        // LOG.info("**METADATA BYTES Length:" + metadataBytes);
        // IFF metadata is presnet and a full read is requested ...
        if (metadataBytes != 0) {
          // read metadata
          metadataOut.datumAndMetadataBytes.set(dataOut.getBytes(),
              readerStream.getPosition(), metadataBytes);
        }
        return metadataOut;
      }
      return null;
    }

    public void bulkQueryURLAndMetadataGivenInputStream(FileSystem remoteFS,
        Configuration conf, File tempFileDir, FlexBuffer linkDataBuffer,
        MergeSortSpillWriter<TextBytes, TriTextBytesTuple> merger)
        throws IOException {

      FileSystem localFS = FileSystem.getLocal(conf);
      // delete contents for temp dir
      localFS.delete(new Path(tempFileDir.getAbsolutePath()), true);
      // make it again
      tempFileDir.mkdir();

      // read link data input stream and populate map

      // create a multimap ... sort by shard id ...
      TreeMultimap<Integer, URLFPV2> shardedFingerprintList = TreeMultimap
          .create();

      // initialize stream ...
      DataInputBuffer linkDataInputStream = new DataInputBuffer();
      linkDataInputStream.reset(linkDataBuffer.get(), linkDataBuffer
          .getOffset(), linkDataBuffer.getCount());

      // initialize fingerprint list reader
      CompressedURLFPListV2.Reader fplistReader = new CompressedURLFPListV2.Reader(
          linkDataInputStream);

      try {
        // walk fingerprints
        while (fplistReader.hasNext()) {
          // read next fingerprint
          URLFPV2 nextFP = fplistReader.next();
          // ok compute shard index
          int shardId = ((nextFP.hashCode() & Integer.MAX_VALUE) % CrawlEnvironment.NUM_DB_SHARDS);
          // ok add it to multimap based on shard id
          shardedFingerprintList.put(shardId, nextFP);
        }
      } finally {
        // close reader
        fplistReader.close();
      }

      // ok now walk fingerprints sorted by shard id

      LOG.info("Walking fingerprints based on shard id ");
      // walk shard entries one at a time
      for (int shardId : shardedFingerprintList.keySet()) {

        // get fingerprints specific to this shard ...
        SortedSet<URLFPV2> fingerprintsForShard = shardedFingerprintList
            .get(shardId);

        // read url count for shard
        int urlCount = fingerprintsForShard.size();

        LOG.info("Shard Id:" + shardId + " URLCount:" + urlCount);

        // open up stream
        // estalish drive index ...
        int driveIndex = shardId % _driveCount;
        // establish path ..
        Path indexPath = new Path("/data/" + driveIndex + "/metadata/"
            + _databaseTimestamp + "/part-"
            + _numberFormat.get().format(shardId) + ".index");

        DataInputBuffer inputStream = new DataInputBuffer();

        CompressURLListV2.Index.IndexFile index = new CompressURLListV2.Index.IndexFile(
            new File(indexPath.toString()));
        CompressURLListV2.IndexCursor cursor = new CompressURLListV2.IndexCursor();

        TextBytes urlValueOut = new TextBytes();

        for (URLFPV2 fingerprint : fingerprintsForShard) {

          TextBytes dataOut = index.mapURLFPToURL(fingerprint, cursor);

          if (dataOut != null) {

            inputStream.reset(dataOut.getBytes(), dataOut.getOffset(), dataOut
                .getLength());

            // data is a tri-text byte tuple object ...
            TriTextBytesTuple tuple = new TriTextBytesTuple();
            // read tuple
            tuple.readFields(inputStream);
            // transfer url to key value
            urlValueOut.set(tuple.getFirstValue().getBytes(), 0, tuple
                .getFirstValue().getLength());
            // reset tuple's url value
            tuple.getFirstValue().clear();
            // and spill it to merger
            merger.spillRecord(urlValueOut, tuple);
          } else {
            LOG.error("Failed to retrieve Metadata for Shard:" + shardId
                + " FP:" + fingerprint.getUrlHash());
          }
        }
      }
    }

    /**
     * query subdomain metadata given domain id
     * 
     * @param domainId
     * @return
     * @throws IOException
     */
    public SubDomainMetadata queryDomainMetadataGivenDomainId(long domainId)
        throws IOException {
      // figure out shard id based on key
      int shardId = (((int) domainId) & Integer.MAX_VALUE)
          % CrawlEnvironment.NUM_DB_SHARDS;
      // write key to buffer
      DataOutputBuffer keyBuffer = new DataOutputBuffer();
      keyBuffer.writeLong(domainId);

      FlexBuffer key = queryDomainMetadataKeyAndIndex(keyBuffer,
          MetadataIndexBuilderV2.SUBDOMAIN_INDEX_ID_TO_METADATA, shardId);

      if (key != null) {
        DataInputBuffer inputStream = new DataInputBuffer();
        inputStream.reset(key.get(), 0, key.getCount());
        SubDomainMetadata metadataOut = new SubDomainMetadata();
        metadataOut.readFields(inputStream);
        return metadataOut;
      }
      return null;
    }

    /**
     * query subdomain metadata given domain name
     * 
     * @param domainName
     * @return
     * @throws IOException
     */
    public SubDomainMetadata queryDomainMetadataGivenDomainName(
        String domainName) throws IOException {

      long domainID = queryDomainIdGivenDomain(domainName);

      return queryDomainMetadataGivenDomainId(domainID);
    }

    private FlexBuffer queryDomainMetadataKeyAndIndex(DataOutputBuffer keyData,
        String indexName, int shardId) throws IOException {

      FileSystem localFS = FileSystem.getLocal(_conf);

      // figure out shard id ...
      // int shardId = (((int)domainId) & Integer.MAX_VALUE) %
      // CrawlEnvironment.NUM_DB_SHARDS;

      // figure out drive index ...
      int driveIndex = shardId % _driveCount;

      // construct index path ..
      Path filePath = new Path("/data/" + driveIndex + "/subDomain_"
          + indexName + "/" + _databaseTimestamp + "/part-"
          + _numberFormat.get().format(shardId));

      // open file
      FSDataInputStream inputStream = localFS.open(filePath);

      try {

        TFile.Reader reader = new TFile.Reader(inputStream, localFS
            .getFileStatus(filePath).getLen(), _conf);

        try {
          // scanner
          TFile.Reader.Scanner scanner = reader.createScanner();

          try {
            // seek to key
            if (scanner.seekTo(keyData.getData(), 0, keyData.getLength())) {
              BytesWritable dataOut = new BytesWritable();
              // ok return raw data
              scanner.entry().getValue(dataOut);
              // and return it
              return new FlexBuffer(dataOut.getBytes(), 0, dataOut.getLength());
            }
          } finally {
            scanner.close();
          }

        } finally {
          reader.close();
        }
      } finally {
        inputStream.close();
      }
      return null;
    }

    public final URLFPV2 queryFPGivenURL(String url) throws IOException {
      URLFPV2 fp = URLUtils.getURLFPV2FromURL(url);
      if (fp == null) {
        throw new IOException("Malformed URL Exception");
      }
      return fp;
    }

    public final int queryShardIdGivenFP(URLFPV2 fingerprint) {
      return (fingerprint.hashCode() & Integer.MAX_VALUE)
          % CrawlEnvironment.NUM_DB_SHARDS;
    }

    public final long queryDomainIdGivenDomain(String domain)
        throws IOException {
      String domainHack = "http://" + domain;
      URLFPV2 hackFP = URLUtils.getURLFPV2FromURL(domainHack);
      if (hackFP == null) {
        throw new IOException("Malformed Domain Name Exception");
      } else {
        return hackFP.getDomainHash();
      }
    }

    public final long queryDomainShardIdGivenDomain(long domain)
        throws IOException {
      return (((int) domain) & Integer.MAX_VALUE)
          % CrawlEnvironment.NUM_DB_SHARDS;
    }

    private ArrayList<ShardIndexHostNameTuple> buildShardMapFromAffinityMapGivenRootPath(
        Path rootPath,Set<String> optionalSlavesList) throws IOException {
      // Path linkDBPath = new Path("crawl/linkdb/merged" + _databaseTimestamp +
      // "/linkMetadata");
      String affinityMapStr = NodeAffinityMaskBuilder.buildNodeAffinityMask(
          _remoteFS, rootPath, null);
      if (affinityMapStr == null) {
        throw new IOException("Unable to create node affinity mask for path:"
            + rootPath);
      } else {

        Map<Integer, String> affinityMap = NodeAffinityMaskBuilder
            .parseAffinityMask(affinityMapStr);
        
        // ok if a slaves list is supplied ... 
        if (optionalSlavesList != null) {
          HashSet<String> excludedNodes = new HashSet<String>();
          for (Map.Entry<Integer, String> entry : affinityMap.entrySet()) { 
            if (!optionalSlavesList.contains(entry.getValue())) { 
              LOG.warn("Slave:" + entry.getValue() + " for parition:" + entry.getKey() + " not available!");
              excludedNodes.add(entry.getValue());
            }
          }
          // now if exclusion set is not empty, this means the affinity map 
          // contains nodes that are not available ... rebuild it again with an
          // exlusion list 
          if (excludedNodes.size() != 0) { 
            LOG.warn("Affinity map will be rebuilt with excluded nodes:" + excludedNodes.toString());
           
            affinityMapStr = NodeAffinityMaskBuilder.buildNodeAffinityMask(
                _remoteFS, rootPath, null,excludedNodes);
            if (affinityMapStr == null) {
              throw new IOException("Unable to create node affinity mask for path:"
                  + rootPath);
            } else {
              affinityMap = NodeAffinityMaskBuilder.parseAffinityMask(affinityMapStr);
            }
          }
        }

        // ok build host name to shard id tuple
        ArrayList<ShardIndexHostNameTuple> tupleListOut = new ArrayList<ShardIndexHostNameTuple>();

        // ok now build an actual affinity map
        for (Map.Entry<Integer, String> entry : affinityMap.entrySet()) {
          // create a tuple
          ShardIndexHostNameTuple tuple = new ShardIndexHostNameTuple();

          String hostName = entry.getValue();
          // strip everything except leading qualifier
          int indexOfDot = hostName.indexOf('.');
          if (indexOfDot != -1) {
            hostName = hostName.substring(0, indexOfDot);
          }
          tuple.setHostName(hostName);
          tuple.setShardId(entry.getKey());

          tupleListOut.add(tuple);
        }

        return tupleListOut;
      }
    }

    private static final NumberFormat                   NUMBER_FORMAT                      = NumberFormat
                                                                                               .getInstance();
    static {
      NUMBER_FORMAT.setMinimumIntegerDigits(5);
      NUMBER_FORMAT.setGroupingUsed(false);
    }

    static Map<Integer, PositionBasedSequenceFileIndex> _shardToInverseDomainQueryIndexMap = new TreeMap<Integer, PositionBasedSequenceFileIndex>();

    public long collectAllTopLevelDomainRecordsByDomain(FileSystem fs,
        Configuration conf, long targetRootDomainFP,
        FileSystem outputFileSystem, Path finalOutputPath) throws IOException {

      File tempFile = new File("/tmp/inverseLinksReport-"
          + System.currentTimeMillis());
      tempFile.mkdir();

      long recordCount = 0;

      try {
        LOG.info("Opening SpillWriter @:" + finalOutputPath
            + " using FileSystem:" + outputFileSystem.toString());
        // create the final output spill writer ...
        SequenceFileSpillWriter<FlexBuffer, URLFPV2> spillwriter = new SequenceFileSpillWriter<FlexBuffer, URLFPV2>(
            outputFileSystem, conf, finalOutputPath, FlexBuffer.class,
            URLFPV2.class,
            new PositionBasedSequenceFileIndex.PositionBasedIndexWriter(
                outputFileSystem, PositionBasedSequenceFileIndex
                    .getIndexNameFromBaseName(finalOutputPath)), true);

        try {

          MergeSortSpillWriter<FlexBuffer, URLFPV2> finalMerger = new MergeSortSpillWriter<FlexBuffer, URLFPV2>(
              conf, spillwriter, FileSystem.getLocal(conf), new Path(tempFile
                  .getAbsolutePath()), null, new ComplexKeyComparator(),
              FlexBuffer.class, URLFPV2.class, true, null);

          try {

            for (int targetShardId = 0; targetShardId < CrawlEnvironment.NUM_DB_SHARDS; ++targetShardId) {
              // 0. shard domain id to find index file location ...
              int indexShardId = (int) ((targetRootDomainFP & Integer.MAX_VALUE) % CrawlEnvironment.NUM_DB_SHARDS);
              // build path to index file
              Path indexFilePath = new Path("crawl/inverseLinkDB_ByDomain/"
                  + _databaseTimestamp + "/phase3Data/part-"
                  + NUMBER_FORMAT.format(indexShardId));
              LOG.info("rootDomain is:" + targetRootDomainFP + " ShardId:"
                  + indexShardId + " Index Path:" + indexFilePath);
              // 1. scan domainFP to index file first
              // 2. given index, scan index->pos file to find scan start
              // position
              // 3. given scan start position, scan forward until fp match is
              // found.
              // 4. collect all matching entries and output to a file ?

              FSDataInputStream indexDataInputStream = fs.open(indexFilePath);
              try {
                TFile.Reader reader = new TFile.Reader(indexDataInputStream, fs
                    .getFileStatus(indexFilePath).getLen(), conf);
                try {
                  TFile.Reader.Scanner scanner = reader.createScanner();

                  try {
                    // generate key ...
                    DataOutputBuffer keyBuffer = new DataOutputBuffer();
                    keyBuffer.writeLong(targetRootDomainFP);
                    if (scanner.seekTo(keyBuffer.getData(), 0, keyBuffer
                        .getLength())) {
                      // setup for value scan
                      DataInputStream valueStream = scanner.entry()
                          .getValueStream();
                      int dataOffsetOut = -1;
                      while (valueStream.available() > 0) {
                        // read entries looking for our specific entry
                        int shardIdx = valueStream.readInt();
                        int dataOffset = valueStream.readInt();
                        if (shardIdx == targetShardId) {
                          dataOffsetOut = dataOffset;
                          break;
                        }
                      }
                      LOG.info("Index Search Yielded:" + dataOffsetOut);
                      if (dataOffsetOut != -1) {
                        // ok create a data path
                        Path finalDataPath = new Path(
                            "crawl/inverseLinkDB_ByDomain/"
                                + _databaseTimestamp + "/phase2Data/data-"
                                + NUMBER_FORMAT.format(targetShardId));
                        Path finalDataIndexPath = new Path(
                            "crawl/inverseLinkDB_ByDomain/"
                                + _databaseTimestamp + "/phase2Data/data-"
                                + NUMBER_FORMAT.format(targetShardId)
                                + ".index");
                        // check to see if index is already loaded ...
                        PositionBasedSequenceFileIndex<FlexBuffer, TextBytes> index = null;
                        synchronized (_shardToInverseDomainQueryIndexMap) {
                          index = _shardToInverseDomainQueryIndexMap
                              .get(targetShardId);
                        }
                        if (index == null) {
                          LOG.info("Loading Index from Path:"
                              + finalDataIndexPath);
                          // load index
                          index = new PositionBasedSequenceFileIndex<FlexBuffer, TextBytes>(
                              fs, finalDataIndexPath, FlexBuffer.class,
                              TextBytes.class);
                          // put in cache
                          synchronized (_shardToInverseDomainQueryIndexMap) {
                            _shardToInverseDomainQueryIndexMap.put(
                                targetShardId, index);
                          }
                        }

                        LOG.info("Initializing Data Reader at Path:"
                            + finalDataPath);
                        // ok time to create a reader
                        SequenceFile.Reader dataReader = new SequenceFile.Reader(
                            fs, finalDataPath, conf);

                        try {
                          LOG.info("Seeking Reader to Index Position:"
                              + dataOffsetOut);
                          index.seekReaderToItemAtIndex(dataReader,
                              dataOffsetOut);

                          FlexBuffer keyBytes = new FlexBuffer();
                          URLFPV2 sourceFP = new URLFPV2();
                          DataInputBuffer keyReader = new DataInputBuffer();

                          // ok read to go ...
                          while (dataReader.next(keyBytes, sourceFP)) {
                            // initialize reader
                            keyReader.reset(keyBytes.get(), keyBytes
                                .getOffset(), keyBytes.getCount());

                            long targetFP = keyReader.readLong();

                            if (targetRootDomainFP == targetFP) {
                              finalMerger.spillRecord(keyBytes, sourceFP);
                              ++recordCount;
                            } else {
                              LOG.info("FP:" + targetFP + " > TargetFP:"
                                  + targetRootDomainFP
                                  + " Exiting Iteration Loop");
                              break;
                            }
                          }
                        } finally {
                          LOG.info("Closing Reader");
                          dataReader.close();
                        }
                      }
                    }
                  } finally {
                    LOG.info("Closing Scanner");
                    scanner.close();
                  }

                } finally {
                  LOG.info("Closing TFile Reader");
                  reader.close();
                }
              } finally {
                LOG.info("Closing InputStream");
                indexDataInputStream.close();
              }
            }
          } finally {
            LOG.info("Closing Final Merger");
            finalMerger.close();
          }
        } finally {
          LOG.info("Closing Final SpillWriter");
          spillwriter.close();
        }
      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
        FileUtils.recursivelyDeleteFile(tempFile);
        throw e;
      }
      return recordCount;
    }

  }

  public static class SlaveDatabaseIndex {

    Configuration                    _conf;
    FileSystem                       _fs;
    long                             _databaseTimestamp;
    int                              _shardIds[];

    static ThreadLocal<NumberFormat> _numberFormat = new ThreadLocal<NumberFormat>() {
                                                     protected NumberFormat initialValue() {

                                                       NumberFormat formatOut = NumberFormat
                                                           .getInstance();

                                                       formatOut
                                                           .setMinimumIntegerDigits(5);
                                                       formatOut
                                                           .setGroupingUsed(false);

                                                       return formatOut;
                                                     };
                                                   };

    public SlaveDatabaseIndex(Configuration conf, FileSystem remoteFS,
        long databaseTimestamp) throws IOException {
      _conf = conf;
      _fs = remoteFS;
      _databaseTimestamp = databaseTimestamp;
    }

    public FlexBuffer queryURLListSortedByName(long domainFP)
        throws IOException {
      return queryURLList(domainFP, "indexedByURL");
    }

    public FlexBuffer queryURLListSortedByPR(long domainFP) throws IOException {
      return queryURLList(domainFP, "indexedByPR");
    }

    private static final long MAX_SPILLBUFFER_ITEM_COUNT = (1 << 27) /8;
    private FlexBuffer queryURLList(long domainFP, String indexName)
        throws IOException {

      // figure out shard index ...
      int shardIndex = (((int) domainFP) & Integer.MAX_VALUE)
          % CrawlEnvironment.NUM_DB_SHARDS;

      // calculate paths ...
      Path indexPath = new Path("crawl/querydb/db/" + _databaseTimestamp + "/"
          + indexName + "/part-" + _numberFormat.get().format(shardIndex));
      Path indexDataPath = new Path("crawl/querydb/db/" + _databaseTimestamp
          + "/" + indexName + "/IndexData"
          + _numberFormat.get().format(shardIndex));

      FSDataInputStream indexInputStream = _fs.open(indexPath);
      try {
        FSDataInputStream indexDataInputStream = _fs.open(indexDataPath);
        try {
          TFile.Reader reader = new TFile.Reader(indexInputStream, _fs
              .getFileStatus(indexPath).getLen(), _conf);
          try {
            TFile.Reader.Scanner scanner = reader.createScanner();

            try {
              DataOutputBuffer keyBuffer = new DataOutputBuffer();
              keyBuffer.writeLong(domainFP);
              if (scanner.seekTo(keyBuffer.getData(), 0, keyBuffer.getLength())) {
                // establish data start ..
                long dataPosStart = scanner.entry().getValueStream().readLong();
                // now establish default end pos
                long dataPosEnd = _fs.getFileStatus(indexDataPath).getLen();
                // and if not last index item .. use next item as stop point
                if (scanner.advance()) {
                  dataPosEnd = scanner.entry().getValueStream().readLong();
                }
                // calculate size 
                long dataSize = dataPosEnd - dataPosStart;
                long itemCount = dataSize / 8;
                if (itemCount > MAX_SPILLBUFFER_ITEM_COUNT) { 
                  LOG.error("itemCount:"+ itemCount + " exceeds MAX_SPILLBUFFER_SIZE:" + MAX_SPILLBUFFER_ITEM_COUNT + " truncating.");
                  dataSize = MAX_SPILLBUFFER_ITEM_COUNT * 8;
                }
                // all right .. we are read to spill out the fingerprints ..
                
                FlexBuffer bufferOut = new FlexBuffer(
                    new byte[(int)dataSize]);
                // seek to proper location ...
                indexDataInputStream.seek(dataPosStart);
                // and read entire contents ..
                indexDataInputStream.read(bufferOut.get());

                return bufferOut;
              }
            } finally {
              scanner.close();
            }
          } finally {
            reader.close();
          }
        } finally {
          indexDataInputStream.close();
        }
      } finally {
        indexInputStream.close();
      }

      return null;
    }

    public FlexBuffer queryOutlinksByFP(URLFPV2 fingerprint, int shardId,
        long dataPos) throws IOException {
      return queryLinkDataByFP("linkdb", fingerprint, shardId, dataPos);
    }

    public FlexBuffer queryInlinksByFP(URLFPV2 fingerprint, int shardId,
        long dataPos) throws IOException {
      return queryLinkDataByFP("inverse_linkdb", fingerprint, shardId, dataPos);
    }

    private FlexBuffer queryLinkDataByFP(String indexName, URLFPV2 fingerprint,
        int shardId, long dataPos) throws IOException {
      // write out incoing fingerprint in a buffer
      DataOutputBuffer queryBuffer = new DataOutputBuffer();
      fingerprint.write(queryBuffer);
      URLFPV2RawComparator comparator = new URLFPV2RawComparator();

      // establish path ...
      Path linkDataPath = new Path("crawl/" + indexName + "/merged"
          + _databaseTimestamp + "/linkData/part-"
          + _numberFormat.get().format(shardId));

      LOG.info("linkDataPath:" + linkDataPath);
      for (int pass = 0; pass < 2; ++pass) {
        // open file
        SequenceFile.Reader reader = new SequenceFile.Reader(_fs, linkDataPath,
            _conf);

        try {

          if (pass == 1) {
            reader.sync(dataPos);
            LOG.info("New Position is:" + reader.getPosition());
          } else {
            reader.seek(dataPos);
          }
          LOG.info("Seek DataPos:" + dataPos + " NewPos:"
              + reader.getPosition());

          boolean eos = false;

          DataOutputBuffer keyBuffer = new DataOutputBuffer();
          ValueBytes valueBytes = reader.createValueBytes();

          int itemsRead = 0;

          boolean doSecondPass = false;

          while (!eos) {

            keyBuffer.reset();

            if (reader.nextRaw(keyBuffer, valueBytes) == -1) {
              LOG.error("Next Raw Failed");
              eos = true;
            } else {
              itemsRead++;
              // URLFPV2 currentKeyDbg = new URLFPV2();
              // DataInputBuffer inputTemp = new DataInputBuffer();
              // inputTemp.reset(keyBuffer.getData(),0,keyBuffer.getLength());
              // currentKeyDbg.readFields(inputTemp);
              // ok compare fingerprints ...
              int result = comparator.compare(queryBuffer.getData(), 0,
                  queryBuffer.getLength(), keyBuffer.getData(), 0, keyBuffer
                      .getLength());

              // LOG.info("***SEARCHING FOR DomainFP:" +
              // fingerprint.getDomainHash() + " URLFP:" +
              // fingerprint.getUrlHash()
              // + " Got: DomainFP:"+ currentKeyDbg.getDomainHash() + " URLFP:"
              // + currentKeyDbg.getUrlHash() + " CompareResult:"+ result);

              if (result == 0) {
                // ok match found !!!
                DataOutputBuffer valueDataOut = new DataOutputBuffer();
                valueBytes.writeUncompressedBytes(valueDataOut);
                // skip the first four bytes length field of the container
                // byteswritable
                return new FlexBuffer(valueDataOut.getData(), 4, valueDataOut
                    .getLength() - 4);
              } else if (result == -1) {

                DataInputBuffer inputStream = new DataInputBuffer();
                inputStream
                    .reset(keyBuffer.getData(), 0, keyBuffer.getLength());
                URLFPV2 otherFP = new URLFPV2();
                otherFP.readFields(inputStream);

                LOG.error("***Failed to Find Match. ItemsRead:" + itemsRead
                    + " QueryDH:" + fingerprint.getDomainHash() + " FP:"
                    + fingerprint.getUrlHash() + " lastDH:"
                    + otherFP.getDomainHash() + " FP:" + otherFP.getUrlHash());

                if (itemsRead == 1) {
                  if (dataPos != 0) {
                    dataPos = Math.max(0, dataPos
                        - _fs.getFileStatus(linkDataPath).getBlockSize());
                    LOG.info("Retrying with new data pos of:" + dataPos);
                    doSecondPass = true;
                  }
                }
                eos = true;
              }
            }
          }

          if (!doSecondPass) {
            break;
          } else {
            LOG.info("*** Doing Second Pass with BlockPos:" + dataPos);
          }
        } finally {
          reader.close();
        }
      }
      return null;
    }

    @SuppressWarnings("unchecked")
    public long queryDomainsGivenPattern(String searchPattern, int shardId,
        SequenceFileSpillWriter<Text, SubDomainMetadata> spillWriter)
        throws IOException {

      Path metadataDBPath = new Path("crawl/metadatadb/" + _databaseTimestamp
          + "/subDomainMetadata/"
          + MetadataIndexBuilderV2.SUBDOMAIN_INDEX_NAME_TO_METADATA + "/part-"
          + _numberFormat.get().format(shardId));

      Pattern patternObj = Pattern.compile(searchPattern);

      FSDataInputStream inputStream = _fs.open(metadataDBPath);

      long indexLength = _fs.getFileStatus(metadataDBPath).getLen();

      long recordCount = 0;

      try {

        TFile.Reader reader = new TFile.Reader(inputStream, indexLength, _conf);

        try {
          TFile.Reader.Scanner scanner = reader.createScanner();

          try {
            BytesWritable keyBytes = new BytesWritable();
            DataInputBuffer keyStream = new DataInputBuffer();
            TextBytes textBytes = new TextBytes();
            while (!scanner.atEnd()) {
              // get key bytes ...
              scanner.entry().getKey(keyBytes);
              // reset stream
              keyStream.reset(keyBytes.getBytes(), keyBytes.getLength());
              // read text bytes length
              int textBytesLength = WritableUtils.readVInt(keyStream);
              // initialize text bytes to remaining bytes
              textBytes.set(keyBytes.getBytes(), keyStream.getPosition(), keyStream
                  .getLength()
                  - keyStream.getPosition());
              // decode ...
              String domainName = textBytes.toString();
              // match
              if (patternObj.matcher(domainName).matches()) {
                // IFF MATCH, GET VALUE BYTES
                BytesWritable valueBytes = new BytesWritable();
                scanner.entry().getValue(valueBytes);
                // SPILL
                spillWriter.spillRawRecord(keyBytes.getBytes(), 0, keyBytes
                    .getLength(), valueBytes.getBytes(), 0, valueBytes.getLength());
                // increment record count
                recordCount++;

              }
              scanner.advance();
            }

          } finally {
            scanner.close();
          }
        } finally {
          reader.close();
        }
      } finally {
        inputStream.close();
      }
      return recordCount;

    }
  }

  private static void spillLinkDataIntoTempFileIndex(
      FileSystem remoteFileSystem, FileSystem localFileSystem,
      Configuration conf, DatabaseIndexV2.MasterDatabaseIndex index,
      File tempFilePath, Path outputFilePath, FlexBuffer linkData)
      throws IOException {

    SequenceFileSpillWriter<TextBytes, TriTextBytesTuple> outputWriter = new SequenceFileSpillWriter<TextBytes, TriTextBytesTuple>(
        localFileSystem, conf, outputFilePath, TextBytes.class,
        TriTextBytesTuple.class,
        new PositionBasedSequenceFileIndex.PositionBasedIndexWriter(
            localFileSystem, PositionBasedSequenceFileIndex
                .getIndexNameFromBaseName(outputFilePath)), true);

    try {
      // ok create merge sort spill writer ...
      MergeSortSpillWriter<TextBytes, TriTextBytesTuple> merger = new MergeSortSpillWriter<TextBytes, TriTextBytesTuple>(
          conf, outputWriter, localFileSystem, new Path(tempFilePath
              .getAbsolutePath()), null,
          new RawKeyValueComparator<TextBytes, TriTextBytesTuple>() {

            DataInputBuffer   stream1 = new DataInputBuffer();
            DataInputBuffer   stream2 = new DataInputBuffer();
            TriTextBytesTuple tuple1  = new TriTextBytesTuple();
            TriTextBytesTuple tuple2  = new TriTextBytesTuple();

            @Override
            public int compareRaw(byte[] key1Data, int key1Offset,
                int key1Length, byte[] key2Data, int key2Offset,
                int key2Length, byte[] value1Data, int value1Offset,
                int value1Length, byte[] value2Data, int value2Offset,
                int value2Length) throws IOException {

              stream1.reset(value1Data, value1Offset, value1Length);
              stream2.reset(value2Data, value2Offset, value2Length);

              // ok skip url
              int url1Length = WritableUtils.readVInt(stream1);
              stream1.skip(url1Length);
              int url2Length = WritableUtils.readVInt(stream2);
              stream2.skip(url2Length);
              // ok now read optimized page rank stuffed in second tuple
              WritableUtils.readVInt(stream1);
              WritableUtils.readVInt(stream2);
              // now read page rank
              float pageRank1 = stream1.readFloat();
              float pageRank2 = stream2.readFloat();

              return (pageRank1 == pageRank2) ? 0
                  : (pageRank1 < pageRank2) ? -1 : 1;

            }

            @Override
            public int compare(TextBytes key1, TriTextBytesTuple value1,
                TextBytes key2, TriTextBytesTuple value2) {
              stream1.reset(value1.getSecondValue().getBytes(), value1
                  .getSecondValue().getLength());
              stream2.reset(value2.getSecondValue().getBytes(), value2
                  .getSecondValue().getLength());

              try {
                float pr1 = stream1.readFloat();
                float pr2 = stream2.readFloat();

                return (pr1 == pr2) ? 0 : pr1 < pr2 ? -1 : 1;

              } catch (IOException e) {
                LOG.error(CCStringUtils.stringifyException(e));
                throw new RuntimeException();
              }
            }
          }, TextBytes.class, TriTextBytesTuple.class, false, null);

      try {
        long timeStart = System.currentTimeMillis();
        System.out.println(".Running Merger against to resolve tuple set ");
        index.bulkQueryURLAndMetadataGivenInputStream(remoteFileSystem, conf,
            tempFilePath, linkData, merger);
        long timeEnd = System.currentTimeMillis();
        LOG.info(".Merged Successfully in:" + (timeEnd - timeStart));
      } finally {
        LOG.info("Closing Merger");
        merger.close();
      }
    } finally {
      LOG.info("Closing Writer");
      outputWriter.close();
    }
  }

  public static void main(String[] args) {
    if (args.length != 3) {
      LOG.error("args: [candidate Timestamp] [drive count] [query string]");
    }

    // initialize ...
    final Configuration conf = new Configuration();

    conf.addResource("nutch-default.xml");
    conf.addResource("nutch-site.xml");
    conf.addResource("core-site.xml");
    conf.addResource("hdfs-site.xml");
    conf.addResource("mapred-site.xml");

    BasicConfigurator.configure();
    CrawlEnvironment.setHadoopConfig(conf);

    long candidateTS = Long.parseLong(args[0]);
    int driveCount = Integer.parseInt(args[1]);
    String queryString = args[2];

    try {
      FileSystem fs = CrawlEnvironment.getDefaultFileSystem();

      MasterDatabaseIndex masterIndex = new MasterDatabaseIndex(conf, fs,
          driveCount, candidateTS,null);
      SlaveDatabaseIndex slaveIndex = new SlaveDatabaseIndex(conf, fs,
          candidateTS);

      // ok hit the domain against the master index first ...
      LOG.info("Querying master index for DomainId Given DomainName:"
          + queryString);
      long domainId = masterIndex.queryDomainIdGivenDomain(queryString);

      LOG.info("Querying master index for DomainMetadata Given DomainId:"
          + domainId);
      SubDomainMetadata subDomainMeta = masterIndex
          .queryDomainMetadataGivenDomainId(domainId);

      if (subDomainMeta != null) {

        LOG.info("Metadata is present. Deserializing");
        // dump some fields ...
        LOG.info("Domain:" + subDomainMeta.getDomainText() + " URLCount:"
            + subDomainMeta.getUrlCount() + " FetchedCount:"
            + subDomainMeta.getFetchedCount() + " PageRankCount:"
            + subDomainMeta.getHasPageRankCount());

        // ok time to dive into a url list ...

        // query for a list of urls sorted by name
        LOG.info("Querying for URLList for Domain BY PR");
        FlexBuffer urlListBufferByPR = slaveIndex
            .queryURLListSortedByPR(domainId);

        if (urlListBufferByPR != null) {

          // read the list ...
          DataInputBuffer readerStream = new DataInputBuffer();
          readerStream.reset(urlListBufferByPR.get(), urlListBufferByPR
              .getCount());
          int totalItemCount = urlListBufferByPR.getCount() / 8;
          System.out.println("List BY  PR totalCount:" + totalItemCount);

          // initialize a fingerprint object to use for queries ...
          URLFPV2 queryFP = new URLFPV2();

          queryFP.setDomainHash(domainId);

          DataInputBuffer metadataReaderStream = new DataInputBuffer();
          // iterate the first N items ranked by page rank
          for (int i = 0; i < Math.min(10, totalItemCount); ++i) {

            queryFP.setUrlHash(readerStream.readLong());

            // and for metadata
            MetadataOut urlMetadata = masterIndex
                .queryMetadataAndURLGivenFP(queryFP);

            if (urlMetadata != null) {

              // decode the url
              String url = urlMetadata.url.toString();

              System.out.println("URL for FP:" + queryFP.getUrlHash() + " is:"
                  + url);
              if (urlMetadata.datumAndMetadataBytes.getLength() == 0) {
                System.out.println("URL for FP:" + queryFP.getUrlHash()
                    + " had no METADATA!!");
              } else {

                // explode metadata
                CrawlDatumAndMetadata metadataObject = new CrawlDatumAndMetadata();

                metadataReaderStream.reset(urlMetadata.datumAndMetadataBytes
                    .getBytes(), urlMetadata.datumAndMetadataBytes.getOffset(),
                    urlMetadata.datumAndMetadataBytes.getLength());
                metadataObject.readFields(metadataReaderStream);

                // ok at this point spit out stuff for this url
                StringBuilder urlInfo = new StringBuilder();

                urlInfo.append("    FetchStatus:"
                    + CrawlDatum.getStatusName(metadataObject.getStatus())
                    + "\n");
                urlInfo.append("    PageRank:"
                    + metadataObject.getMetadata().getPageRank() + "\n");
                urlInfo.append("    ContentType:"
                    + metadataObject.getMetadata().getContentType() + "\n");
                urlInfo.append("    ArcFileInfoCount:"
                    + metadataObject.getMetadata().getArchiveInfo().size());
                if (metadataObject.getMetadata().isFieldDirty(
                    CrawlURLMetadata.Field_LINKDBFILENO)) {
                  urlInfo.append("    HasLinkDataInfo:"
                      + metadataObject.getMetadata().getLinkDBFileNo() + ":"
                      + metadataObject.getMetadata().getLinkDBOffset());
                }
                if (metadataObject.getMetadata().isFieldDirty(
                    CrawlURLMetadata.Field_INVERSEDBFILENO)) {
                  urlInfo.append("    HasINVLinkDataInfo:"
                      + metadataObject.getMetadata().getInverseDBFileNo() + ":"
                      + metadataObject.getMetadata().getInverseDBOffset());
                }
                System.out.println(urlInfo.toString());

                // now if inverse link data is present ..
                if (metadataObject.getMetadata().isFieldDirty(
                    CrawlURLMetadata.Field_INVERSEDBFILENO)) {
                  // get it ...
                  System.out.println("Querying for Inlinks for FP:"
                      + queryFP.getUrlHash());
                  FlexBuffer inlinks = slaveIndex.queryInlinksByFP(queryFP,
                      metadataObject.getMetadata().getInverseDBFileNo(),
                      metadataObject.getMetadata().getInverseDBOffset());

                  if (inlinks != null) {
                    System.out.println("Found Inlink Buffer of Size:"
                        + inlinks.getCount());
                    FileSystem localFS = FileSystem.getLocal(conf);
                    File testDir = new File("/tmp/dbIndexTest");
                    File testFile = new File("/tmp/dbIndexTestFile");
                    localFS.delete(new Path(testDir.getAbsolutePath()), true);
                    localFS.delete(new Path(testFile.getAbsolutePath()), false);
                    localFS.mkdirs(new Path(testDir.getAbsolutePath()));

                    LOG.info("Creating Spill File of Inlinks");
                    spillLinkDataIntoTempFileIndex(fs, localFS, conf,
                        masterIndex, testDir, new Path(testFile
                            .getAbsolutePath()), inlinks);
                    LOG.info("Created Spill File of Inlinks");

                    LOG.info("Reading Inlinks");
                    // ok now open it up and dump the first few inlinks from the
                    // spill file
                    SequenceFile.Reader reader = new SequenceFile.Reader(
                        localFS, new Path(testFile.getAbsolutePath()), conf);

                    TextBytes key = new TextBytes();
                    TriTextBytesTuple value = new TriTextBytesTuple();
                    CrawlDatumAndMetadata metadata = new CrawlDatumAndMetadata();
                    DataInputBuffer inputBuffer = new DataInputBuffer();

                    try {
                      int itemCount = 0;

                      while (reader.next(key, value)) {

                        if (value.getThirdValue().getLength() != 0) {
                          inputBuffer.reset(value.getThirdValue().getBytes(),
                              0, value.getThirdValue().getLength());
                          metadata.readFields(inputBuffer);
                          System.out.println("INLINK:" + key.toString()
                              + " METADATA STATUS:"
                              + CrawlDatum.getStatusName(metadata.getStatus()));
                        } else {
                          System.out.println("INLINK:" + key.toString()
                              + " NOMETADATA");
                        }

                        if (++itemCount == 500) {
                          break;
                        }
                      }
                    } finally {
                      reader.close();
                    }

                    LOG.info("Done Reding Inlinks");
                  }
                }
              }
            } else {
              LOG.error("Query for FP:" + queryFP.getUrlHash()
                  + " returned NULL URL");
            }
          }
        }
      }

    } catch (IOException e) {
      LOG.error(CCStringUtils.stringifyException(e));
    }
  }

}