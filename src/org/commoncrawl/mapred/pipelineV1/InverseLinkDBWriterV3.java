package org.commoncrawl.mapred.pipelineV1;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.service.crawler.filters.SuperDomainFilter;
import org.commoncrawl.service.crawler.filters.Utils;
import org.commoncrawl.service.crawler.filters.Filter.FilterResult;
import org.commoncrawl.protocol.CrawlURLMetadata;
import org.commoncrawl.protocol.InlinkData;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.CompressedURLFPListV2;
import org.commoncrawl.util.NodeAffinityMaskBuilder;
import org.commoncrawl.util.CompressedURLFPListV2.Builder;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileMergeInputFormat;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileMergePartitioner;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader.KeyAndValueData;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader.RawRecordValue;
import org.commoncrawl.util.URLUtils.URLFPV2RawComparator;
import org.commoncrawl.util.ByteBufferOutputStream;
import org.commoncrawl.util.CCStringUtils;

import com.google.common.collect.TreeMultimap;

/**
 * Build the inverted link database from the current crawl database and current
 * link database
 * 
 * @author rana
 * 
 */
public class InverseLinkDBWriterV3 extends CrawlDBCustomJob {

  public static final int     ROOT_DOMAIN_INLINK_THRESHOLD             = 10000;
  public static final int     MAX_INLINKS                              = 1000000;
  public static final int     MAX_ROOT_DOMAINS_THRESHOLD               = 10000;

  public static final int     MAX_OUTLINKS                             = 5000;
  private static final String CLEAROUTLINKSFROMFP_FLAG                 = "ClearOutlinkCountFromURLFP";

  public static final int     URLFPStreamFlags_IsURLFPRecord           = 1;
  public static final int     URLFPStreamFlags_IsIntraDomainLinkRecord = 2;
  public static final int     URLFPStreamFlags_HasSegmentId            = 4;
  public static final int     URLFPStreamFlags_HasFlags                = 16;
  public static final int     URLFPStreamFlags_HasTimestamp            = 32;

  private static void writeURLFPToStream(URLFPV2 fingerprint,
      DataOutputStream stream) throws IOException {

    if ((fingerprint.getFlags() & URLFPV2.Flag.IsIntraDomainLink) != 0) {
      // stream.write(URLFPStreamFlags_IsIntraDomainLinkRecord);
      // WritableUtils.writeVInt(stream,fingerprint.getIntraDomainLinkCount());
    } else {
      int flags = URLFPStreamFlags_IsURLFPRecord;
      if (fingerprint.getParseSegmentId() != 0) {
        flags |= URLFPStreamFlags_HasSegmentId;
      }
      if ((fingerprint.getFlags() & ~URLFPV2.Flag.IsIntraDomainLink) != 0) {
        flags |= URLFPStreamFlags_HasFlags;
      }
      if (fingerprint.getTimestampInSeconds() != 0) {
        flags |= URLFPStreamFlags_HasTimestamp;
      }
      stream.write(flags);
      WritableUtils.writeVLong(stream, fingerprint.getDomainHash());
      WritableUtils.writeVLong(stream, fingerprint.getUrlHash());
      WritableUtils.writeVLong(stream, fingerprint.getRootDomainHash());
      if ((flags & URLFPStreamFlags_HasSegmentId) != 0) {
        WritableUtils.writeVInt(stream, fingerprint.getParseSegmentId());
      }
      if ((flags & URLFPStreamFlags_HasFlags) != 0) {
        WritableUtils.writeVInt(stream,
            (fingerprint.getFlags() & ~URLFPV2.Flag.IsIntraDomainLink));
      }
      if ((flags & URLFPStreamFlags_HasTimestamp) != 0) {
        WritableUtils.writeVInt(stream, fingerprint.getTimestampInSeconds());
      }
    }
  }

  private static URLFPV2 readURLFPFromStream(DataInputStream stream)
      throws IOException {
    URLFPV2 urlfpOut = new URLFPV2();

    int flags = stream.read();

    if ((flags & URLFPStreamFlags_IsIntraDomainLinkRecord) != 0) {
      urlfpOut.setFlags(URLFPV2.Flag.IsIntraDomainLink);
    } else {
      urlfpOut.setDomainHash(WritableUtils.readVLong(stream));
      urlfpOut.setUrlHash(WritableUtils.readVLong(stream));
      urlfpOut.setRootDomainHash(WritableUtils.readVLong(stream));
      if ((flags & URLFPStreamFlags_HasSegmentId) != 0) {
        urlfpOut.setParseSegmentId(WritableUtils.readVInt(stream));
      }
      if ((flags & URLFPStreamFlags_HasFlags) != 0) {
        urlfpOut.setFlags(WritableUtils.readVInt(stream));

      }
      if ((flags & URLFPStreamFlags_HasTimestamp) != 0) {
        urlfpOut.setTimestampInSeconds(WritableUtils.readVInt(stream));
      }
    }
    return urlfpOut;
  }

  private static final Log LOG = LogFactory.getLog(InverseLinkDBWriterV3.class);

  enum Counters {
    NO_DATUM_RECORD_BUT_LINK_RECORD, DUPLICATE_DATUM_FOUND,
    DUPLICATE_LINK_DATA_RECORD, NEXT_RECORD_IN_REDUCE_WAS_MERGE_DB_RECORD,
    NEXT_RECORD_IN_REDUCE_WAS_LINK_DB_RECORD, FINAL_RECORD_WAS_LINK_DB_RECORD,
    FINAL_RECORD_WAS_MERGE_DB_RECORD, NEXT_RECORD_WAS_CRAWLDATUM_RECORD,
    WROTE_PERMANENT_REDIRECT_LINK_STREAM, LINKDATARECORD_HAD_PART_NO,
    LINKDATARECORD_HAD_OFFSET, LINKDATARECORD_HAD_NONZERO_EXTRADOMAIN_COUNT,
    LINKDATARECORD_HAD_NONZERO_INTRADOMAIN_COUNT,
    GOT_COMBINER_URLFP_IN_REDUCER, GOT_INTERMEDIATERECORD_IN_REDUCER,
    GOT_INTRADOMAIN_LINK_IN_REDUCER,
    GOT_NONZERO_INTRADOMIAN_LINKCOUNT_IN_REDUCER,
    MAX_QUEUED_INLINS_LIMIT_REACHED, TOO_MANY_INLINKS_FROM_ROOT_DOMAIN,
    TOO_MANY_ROOT_DOMAINS, OUTLINK_BUFFER_GT_2MB, INLINKS_LTEQ_10,
    INLINKS_GT_10_LTEQ_100, INLINKS_GT_100_LTEQ_1K, INLINKS_GT_1K_LTEQ_10K,
    INLINKS_GT_10K_LTEQ_100K, INLINKS_GT_100K_LTEQ_1MILLION,
    GOT_OUTOFMEMORY_ERROR_IN_REDUCE, OUTLINK_BUFFER_GT_20MB
  }

  @Override
  public String getJobDescription() {
    return "Inverted Link Database Writer";
  }

  private static final int PARTS_TO_PROCESS_EVERY_ITERATION = CrawlEnvironment.NUM_DB_SHARDS / 4;

  private long findLatestDatabaseTimestamp(Path rootPath) throws IOException {
    FileSystem fs = CrawlEnvironment.getDefaultFileSystem();

    FileStatus candidates[] = fs.globStatus(new Path(rootPath, "*"));

    long candidateTimestamp = -1L;

    for (FileStatus candidate : candidates) {
      LOG.info("Found Seed Candidate:" + candidate.getPath());
      long timestamp = Long.parseLong(candidate.getPath().getName());
      if (candidateTimestamp == -1 || candidateTimestamp < timestamp) {
        candidateTimestamp = timestamp;
      }
    }
    LOG.info("Selected Candidate is:" + candidateTimestamp);
    return candidateTimestamp;
  }

  @Override
  public void runJob() throws IOException {

    FileSystem fs = CrawlEnvironment.getDefaultFileSystem();

    Configuration conf = CrawlEnvironment.getHadoopConfig();

    long seedTimestamp = findLatestDatabaseTimestamp(new Path(
        "crawl/crawldb_new"));
    LOG.info("Seed Timestamp is:" + seedTimestamp);
    Path seedDatabasePath = new Path("crawl/crawldb_new/" + seedTimestamp);

    Path linkDBBase = new Path(CrawlEnvironment.HDFS_CrawlDBBaseDir,
        CrawlEnvironment.HDFS_LinkDBDir);
    Path linkDBMerged = new Path("crawl/linkdb/merged" + seedTimestamp);
    long linkDBTimestamp = seedTimestamp;
    LOG.info("LinkDB Candidate is:" + linkDBMerged);

    // ok create a set of all paths for the current linkDBMerged
    ArrayList<Path> linkDBParts = new ArrayList<Path>();
    FileStatus linkDBPartsArray[] = fs.globStatus(new Path(linkDBMerged,
        "linkData/part-*"));
    for (FileStatus linkDBPart : linkDBPartsArray) {
      linkDBParts.add(linkDBPart.getPath());
    }

    // ok create an array of paths where we will generate temporary output ...
    ArrayList<Path> inverseLinkDBPaths = new ArrayList<Path>();
    {
      // ok now iterate candidate parts
      while (linkDBParts.size() != 0) {
        LOG.info(getJobDescription() + ": Starting.");

        JobConf job = new JobConf(conf);

        initializeDistributedCache(job);

        String affinityMask = NodeAffinityMaskBuilder.buildNodeAffinityMask(
            FileSystem.get(job), seedDatabasePath, null);
        NodeAffinityMaskBuilder.setNodeAffinityMask(job, affinityMask);

        job.setJobName(getJobDescription() + " - Phase 1");

        // add requisite number of parts ....
        int partCount = 0;
        while (linkDBParts.size() != 0) {
          Path pathToProcess = linkDBParts.remove(0);
          LOG.info("Processing Part:" + pathToProcess);
          FileInputFormat.addInputPath(job,pathToProcess);
          if (++partCount == PARTS_TO_PROCESS_EVERY_ITERATION) {
            break;
          }
        }

        // create a job output path
        long tempDirSeed = System.currentTimeMillis();
        Path tempOutputDir = new Path(CrawlEnvironment.getHadoopConfig().get(
            "mapred.temp.dir", ".")
            + "/generate-temp-" + tempDirSeed);
        // set output path ...
        FileOutputFormat.setOutputPath(job,tempOutputDir);
        // add it to output paths list ...
        inverseLinkDBPaths.add(tempOutputDir);

        // ok set more job specifics ...

        job.setInputFormat(SequenceFileInputFormat.class);
        job.setMapOutputKeyClass(URLFPV2.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setMapperClass(LinkInverter.class);
        job.setReducerClass(InvertedLinkDBReducer.class);
        job.setOutputFormat(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(URLFPV2.class);
        job.setOutputValueClass(BytesWritable.class);
        FileOutputFormat.setOutputPath(job,tempOutputDir);
        job.setNumReduceTasks(CrawlEnvironment.NUM_DB_SHARDS);
        job.setNumTasksToExecutePerJvm(1000);
        job.setCompressMapOutput(false);

        LOG.info("Running  " + job.getJobName());
        JobClient.runJob(job);
        LOG.info("Finished Running" + job.getJobName());

      }
    }

    /**
     * stage 2: consolidate output and write out metadata
     * 
     */

    // now merge the inverse link database information with the crawl database
    JobConf job = new JobConf(conf);

    job.setJobName(getJobDescription() + " - Phase 2 - Merge Databases");

    // add prior job outputs
    for (Path path : inverseLinkDBPaths) {
      FileInputFormat.addInputPath(job,path);
    }

    // set node affinity ...
    String affinityMask = NodeAffinityMaskBuilder.buildNodeAffinityMask(
        FileSystem.get(job), seedDatabasePath, null);
    NodeAffinityMaskBuilder.setNodeAffinityMask(job, affinityMask);

    // set output path
    long tempDirSeed = System.currentTimeMillis();
    Path tempOutputDir = new Path(CrawlEnvironment.getHadoopConfig().get(
        "mapred.temp.dir", ".")
        + "/generate-temp-" + tempDirSeed);

    // multi file merger
    job.setInputFormat(MultiFileMergeInputFormat.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setMapperClass(IdentityMapper.class);
    job.setReducerClass(Phase2LinkDataReducer.class);
    job.setOutputKeyClass(URLFPV2.class);
    job.setOutputValueClass(CrawlURLMetadata.class);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    job.setPartitionerClass(MultiFileMergePartitioner.class);
    FileOutputFormat.setOutputPath(job,tempOutputDir);
    job.setNumReduceTasks(CrawlEnvironment.NUM_DB_SHARDS);
    job.setNumTasksToExecutePerJvm(1000);

    LOG.info("Running  " + getJobDescription() + " OutputDir:" + tempOutputDir);
    JobClient.runJob(job);
    LOG.info("Finished Running " + getJobDescription() + " OutputDir:"
        + tempOutputDir);

    // construct final output paths
    Path inverseLinkDBMerged = new Path("crawl/inverse_linkdb/merged"
        + linkDBTimestamp);
    Path linkDataDirectory = new Path(inverseLinkDBMerged, "linkData");
    Path linkMetadataDirectory = new Path(inverseLinkDBMerged, "linkMetadata");

    // ensure they exist
    fs.mkdirs(inverseLinkDBMerged);
    fs.mkdirs(linkDataDirectory);
    fs.mkdirs(linkMetadataDirectory);

    LOG.info("Moving link database files to merged");
    // copy link db files across
    for (FileStatus linkDBFile : fs.globStatus(new Path(tempOutputDir,
        "linkData_*"))) {
      Path destinationPath = new Path(linkDataDirectory, "part-"
          + linkDBFile.getPath().getName().substring(
              "linkData_".length()));
      fs.rename(linkDBFile.getPath(), destinationPath);
      LOG.info("Moved " + linkDBFile.getPath() + " to: " + destinationPath);
    }
    // copy metadata db files across
    for (FileStatus linkDBFile : fs
        .globStatus(new Path(tempOutputDir, "part-*"))) {
      Path destinationPath = new Path(linkMetadataDirectory, linkDBFile
          .getPath().getName());
      fs.rename(linkDBFile.getPath(), destinationPath);
      LOG.info("Moved " + linkDBFile.getPath() + " to: " + destinationPath);
    }
  }

  private static SuperDomainFilter superDomainFilter = new SuperDomainFilter(
                                                         CrawlEnvironment.ROOT_SUPER_DOMAIN_PATH);

  public static void initializeDistributedCache(JobConf job) throws IOException {

    Utils.initializeCacheSession(job, System.currentTimeMillis());
    LOG.info("Publishing superDomainFilter to Cache");
    superDomainFilter.publishFilter(job);
  }

  public static class Phase1KeyComparator implements RawComparator {

    DataInputBuffer stream1 = new DataInputBuffer();
    DataInputBuffer stream2 = new DataInputBuffer();
    DataInputStream din1    = new DataInputStream(stream1);
    DataInputStream din2    = new DataInputStream(stream2);

    @Override
    public int compare(byte[] b1, int offset1, int length1, byte[] b2,
        int offset2, int length2) {
      stream1.reset(b1, offset1, length1);
      stream2.reset(b2, offset2, length2);

      try {
        din1.skipBytes(2);
        din2.skipBytes(2);

        int domainHash1 = din1.readInt();
        int domainHash2 = din2.readInt();

        int result = (domainHash1 < domainHash2) ? -1
            : (domainHash1 > domainHash2) ? 1 : 0;
        if (result != 0)
          return result;

        din1.skipBytes(2);
        din2.skipBytes(2);

        long urlFP1 = din1.readLong();
        long urlFP2 = din2.readLong();

        if (urlFP1 < urlFP2)
          return -1;
        else if (urlFP1 > urlFP2)
          return 1;
        else
          return 0;
      } catch (IOException e) {
        LOG.error(e);
        throw new RuntimeException(e);
      }
    }

    @Override
    public int compare(Object o1, Object o2) {
      return ((URLFPV2) o1).compareTo((URLFPV2) o2);
    }
  }

  /**
   * Run MultiFileMerger to produce single cohesive inverse link graph and
   * output metadata information
   */
  public static class Phase2LinkDataReducer implements
      Reducer<IntWritable, Text, URLFPV2, CrawlURLMetadata> {

    private SequenceFile.Writer       linkDBWriter;
    FileSystem                        _fs;
    Configuration                     _conf;

    private int                       partNumber;

    private static final NumberFormat NUMBER_FORMAT = NumberFormat
                                                        .getInstance();
    static {
      NUMBER_FORMAT.setMinimumIntegerDigits(5);
      NUMBER_FORMAT.setGroupingUsed(false);
    }

    @Override
    public void reduce(IntWritable key, Iterator<Text> values,
        OutputCollector<URLFPV2, CrawlURLMetadata> output, Reporter reporter)
        throws IOException {
      // collect all incoming paths first
      Vector<Path> incomingPaths = new Vector<Path>();

      while (values.hasNext()) {

        String path = values.next().toString();
        LOG.info("Found Incoming Path:" + path);
        incomingPaths.add(new Path(path));
      }

      // set up merge attributes
      JobConf localMergeConfig = new JobConf(_conf);

      localMergeConfig.setClass(
          MultiFileInputReader.MULTIFILE_COMPARATOR_CLASS,
          URLFPV2RawComparator.class, RawComparator.class);
      localMergeConfig.setClass(MultiFileInputReader.MULTIFILE_KEY_CLASS,
          URLFPV2.class, WritableComparable.class);

      // ok now spawn merger
      MultiFileInputReader<URLFPV2> multiFileInputReader = new MultiFileInputReader<URLFPV2>(
          _fs, incomingPaths, localMergeConfig);

      // now read one set of values at a time and output result
      KeyAndValueData<URLFPV2> keyValueData = null;
      // a metadata record ...
      CrawlURLMetadata metadata = new CrawlURLMetadata();

      DataOutputBuffer builderOutputBuffer = new DataOutputBuffer();

      while ((keyValueData = multiFileInputReader.readNextItem()) != null) {

        // create a set to hold
        HashSet<URLFPV2> inlinks = new HashSet<URLFPV2>();

        // create a compressed list writer
        Builder fpListBuilder = new Builder(
            CompressedURLFPListV2.FLAG_ARCHIVE_SEGMENT_ID
                | CompressedURLFPListV2.FLAG_SERIALIZE_URLFP_FLAGS
                | CompressedURLFPListV2.FLAG_SERIALIZE_TIMESTAMP);

        for (RawRecordValue rawRecord : keyValueData._values) {
          // iterate list
          ByteArrayInputStream inputStream = new ByteArrayInputStream(
              rawRecord.data.getData(), 4, rawRecord.data.getLength() - 4);

          CompressedURLFPListV2.Reader reader = new CompressedURLFPListV2.Reader(
              inputStream);

          while (reader.hasNext()) {
            URLFPV2 sourceFP = reader.next();
            inlinks.add(sourceFP);
          }
        }

        // ok now spit then out via the builder ...
        for (URLFPV2 inlink : inlinks) {
          fpListBuilder.addLink(inlink);
        }

        builderOutputBuffer.reset();
        fpListBuilder.flush(builderOutputBuffer);

        // construct a bytes writable ...
        BytesWritable linkDataOut = new BytesWritable(builderOutputBuffer
            .getData());
        linkDataOut.setSize(builderOutputBuffer.getLength());

        // mark link db file position
        long recordPosition = linkDBWriter.getLength();
        // write out link db record
        linkDBWriter.append(keyValueData._keyObject, linkDataOut);
        // and update urldb record ...
        metadata.clear();

        metadata.setInverseDBFileNo(partNumber);
        metadata.setInverseDBOffset(recordPosition);

        output.collect(keyValueData._keyObject, metadata);
      }
    }

    @Override
    public void configure(JobConf job) {
      _conf = job;
      try {
        _fs = FileSystem.get(job);
      } catch (IOException e1) {
        LOG.error(CCStringUtils.stringifyException(e1));
      }

      partNumber = job.getInt("mapred.task.partition", 0);
      // get the task's temporary file directory ...
      Path taskOutputPath = FileOutputFormat.getWorkOutputPath(job);
      // and create the appropriate path ...
      Path inverseLinkDBPath = new Path(taskOutputPath, "linkData_"
          + NUMBER_FORMAT.format(partNumber));
      // and create the writer ...
      try {
        linkDBWriter = SequenceFile.createWriter(_fs, job, inverseLinkDBPath,
            URLFPV2.class, BytesWritable.class, CompressionType.BLOCK);
      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
      }
    }

    @Override
    public void close() throws IOException {

      if (linkDBWriter != null) {
        linkDBWriter.close();
      }
    }
  }

  /** final reducer that combines previosuly combined inverted lists **/
  private static class InvertedLinkDBReducer implements
      Reducer<URLFPV2, BytesWritable, URLFPV2, BytesWritable> {

    private boolean                   _clearURLCounts = false;
    private DataInputBuffer           keyReader       = new DataInputBuffer();
    private DataInputBuffer           valueReader     = new DataInputBuffer();

    private int                       partNumber;

    private static final NumberFormat NUMBER_FORMAT   = NumberFormat
                                                          .getInstance();
    static {
      NUMBER_FORMAT.setMinimumIntegerDigits(5);
      NUMBER_FORMAT.setGroupingUsed(false);
    }

    private static class OutputRecord {

      public OutputRecord(URLFPV2 fingerprint, InlinkData inlinkData) {
        _target = fingerprint;
        _inlinkData = inlinkData;
      }

      public URLFPV2    _target;
      public InlinkData _inlinkData;
    }

    public boolean isCombiner() {
      return false;
    }

    private void addInlinkToFPList(TreeMultimap<Long, URLFPV2> map,
        URLFPV2 inlinkFP, Reporter reporter) {
      // if not already black listed ...
      if (map.size() < MAX_INLINKS) {
        // get existing collection of urlfps by root domain hash
        Collection<URLFPV2> fpByRootDomain = map.get(inlinkFP
            .getRootDomainHash());
        // check if too many inlinks from the same root domain ...

        if (fpByRootDomain.size() <= ROOT_DOMAIN_INLINK_THRESHOLD) {
          // add the item to the list ...
          fpByRootDomain.add(inlinkFP);
        }
        /*
         * if (map.keySet().size() >= MAX_ROOT_DOMAINS_THRESHOLD &&
         * maxInlinksPerRootDomain != 1) { maxInlinksPerRootDomain = 1;
         * reporter.incrCounter(Counters.TOO_MANY_ROOT_DOMAINS, 1); }
         */
      }
    }

    /*
     * finalInlinkData.setExternalDomainLinkCount(finalInlinkData.getExternalDomainLinkCount
     * () + 1); if (++queuedExtraDomainLinkCount < MAX_INLINKS) {
     * fpListBuilder.addLink(inlinkFP); }
     */
    @Override
    public void reduce(URLFPV2 targetFP, Iterator<BytesWritable> values,
        OutputCollector<URLFPV2, BytesWritable> output, Reporter reporter)
        throws IOException {

      // create a inlink data structure ...
      InlinkData finalInlinkData = new InlinkData();

      finalInlinkData
          .setRecordType((byte) InlinkData.RecordType.LinkListRecord);

      // count up queued inlinks
      int queuedExtraDomainLinkCount = 0;

      // create a compressed list writer
      Builder fpListBuilder = new Builder(
          CompressedURLFPListV2.FLAG_ARCHIVE_SEGMENT_ID
              | CompressedURLFPListV2.FLAG_SERIALIZE_URLFP_FLAGS
              | CompressedURLFPListV2.FLAG_SERIALIZE_TIMESTAMP);

      while (values.hasNext()) {

        BytesWritable intermediateRecord = values.next();

        valueReader.reset(intermediateRecord.getBytes(), 0, intermediateRecord
            .getLength());

        while (valueReader.getPosition() != valueReader.getLength()) {

          URLFPV2 inlinkFP = readURLFPFromStream(valueReader);

          if ((inlinkFP.getFlags() & URLFPV2.Flag.IsIntraDomainLink) != 0) {
            // reporter.incrCounter(Counters.GOT_INTRADOMAIN_LINK_IN_REDUCER,
            // 1);
            // finalInlinkData.setIntraDomainLinkCount(finalInlinkData.getIntraDomainLinkCount()
            // + inlinkFP.getIntraDomainLinkCount());
          } else {

            addInlinkToFPList(fpListBuilder.getLinkMap(), inlinkFP, reporter);
            finalInlinkData.setExternalDomainLinkCount(finalInlinkData
                .getExternalDomainLinkCount() + 1);
          }
          /*
           * if (sourceDomainIsSuperDomain) { if (inlinkFP.getDomainHash() ==
           * targetFP.getDomainHash()) {
           * reporter.incrCounter(Counters.GOT_INTRADOMAIN_LINK_IN_REDUCER, 1);
           * finalInlinkData
           * .setIntraDomainLinkCount(finalInlinkData.getIntraDomainLinkCount()
           * + 1); } else {
           * finalInlinkData.setExternalDomainLinkCount(finalInlinkData
           * .getExternalDomainLinkCount() + 1);
           * addInlinkToFPList(fpListBuilder.getLinkMap(),inlinkFP,reporter); }
           * } else { if (inlinkFP.getRootDomainHash() ==
           * targetFP.getRootDomainHash()) {
           * reporter.incrCounter(Counters.GOT_INTRADOMAIN_LINK_IN_REDUCER, 1);
           * finalInlinkData
           * .setIntraDomainLinkCount(finalInlinkData.getIntraDomainLinkCount()
           * + 1); } else {
           * finalInlinkData.setExternalDomainLinkCount(finalInlinkData
           * .getExternalDomainLinkCount() + 1);
           * addInlinkToFPList(fpListBuilder.getLinkMap(),inlinkFP,reporter); }
           * }
           */
        }
      }

      if (finalInlinkData.getExternalDomainLinkCount() != 0) {
        if (queuedExtraDomainLinkCount >= MAX_INLINKS) {
          reporter.incrCounter(Counters.MAX_QUEUED_INLINS_LIMIT_REACHED, 1);
        }
      }

      if (finalInlinkData.getExternalDomainLinkCount() <= 10) {
        reporter.incrCounter(Counters.INLINKS_LTEQ_10, 1);
      } else if (finalInlinkData.getExternalDomainLinkCount() <= 100) {
        reporter.incrCounter(Counters.INLINKS_GT_10_LTEQ_100, 1);
      } else if (finalInlinkData.getExternalDomainLinkCount() <= 1000) {
        reporter.incrCounter(Counters.INLINKS_GT_100_LTEQ_1K, 1);
      } else if (finalInlinkData.getExternalDomainLinkCount() <= 10000) {
        reporter.incrCounter(Counters.INLINKS_GT_1K_LTEQ_10K, 1);
      } else if (finalInlinkData.getExternalDomainLinkCount() <= 100000) {
        reporter.incrCounter(Counters.INLINKS_GT_10K_LTEQ_100K, 1);
      } else if (finalInlinkData.getExternalDomainLinkCount() <= 1000000) {
        reporter.incrCounter(Counters.INLINKS_GT_100K_LTEQ_1MILLION, 1);
      }

      try {
        // create a buffer to hold compressed fingerprint list
        ByteBufferOutputStream bufferStream = new ByteBufferOutputStream();
        DataOutputStream baos = new DataOutputStream(bufferStream);
        // flush the builder output into the stream
        fpListBuilder.flush(baos);

        // if buffer > 20MB ... NOTE it ...
        if (bufferStream._buffer.getCount() > 20000000) {
          reporter.incrCounter(Counters.OUTLINK_BUFFER_GT_20MB, 1);
          LOG.error("Too Many ExtraDomainLinks for DomainFP:"
              + targetFP.getDomainHash() + " URLFP:" + targetFP.getUrlHash()
              + " Count:" + finalInlinkData.getExternalDomainLinkCount()
              + " BufferSize:" + bufferStream._buffer.getCount()
              + " Available Memory:" + Runtime.getRuntime().freeMemory());
          /*
           * LOG.info("Dumping Entries for Source:" +
           * targetFP.getRootDomainHash() + "," + targetFP.getDomainHash() + ","
           * + targetFP.getUrlHash()); for (Map.Entry<Long,URLFPV2> entry :
           * fpListBuilder.getLinkMap().entries()) {
           * LOG.info(entry.getValue().getRootDomainHash() + "," +
           * entry.getValue().getDomainHash() + "," +
           * entry.getValue().getUrlHash()); } LOG.info("Done with Dump");
           */
        }

        BytesWritable dataOut = new BytesWritable(bufferStream._buffer.get());
        dataOut.setSize(bufferStream._buffer.getCount());

        output.collect(targetFP, dataOut);
      } catch (OutOfMemoryError e) {
        reporter.incrCounter(Counters.GOT_OUTOFMEMORY_ERROR_IN_REDUCE, 1);
        LOG.error("out of memory error writing FP:" + targetFP);
      }
    }

    @Override
    public void configure(JobConf job) {
      LOG.info("Loading superDomainFilter to Cache");
      try {
        superDomainFilter.loadFromCache(job);
      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
      }
      _clearURLCounts = job.getBoolean(CLEAROUTLINKSFROMFP_FLAG, false);

      FileSystem fileSystem = null;

      try {
        fileSystem = FileSystem.get(job);
      } catch (IOException e1) {
        LOG.error(CCStringUtils.stringifyException(e1));
      }

      partNumber = job.getInt("mapred.task.partition", 0);
    }

    @Override
    public void close() throws IOException {

    }
  }

  /** mapper that inverts an outlink list **/
  public static class LinkInverter implements
      Mapper<URLFPV2, BytesWritable, URLFPV2, BytesWritable> {

    DataOutputBuffer keyBuffer   = new DataOutputBuffer(1024);
    DataOutputBuffer valueBuffer = new DataOutputBuffer(1024);

    enum Counters {
      EMPTY_LINLK_LIST, HAD_INTRADOMAIN_LINKS, HAD_INTRADOMAIN_ROOT_LINKS,
      DUPLICATE_OUTLINK_FOUND
    }

    @Override
    public void map(URLFPV2 sourceFP, BytesWritable value,
        OutputCollector<URLFPV2, BytesWritable> output, Reporter reporter)
        throws IOException {
      if (value.getLength() == 0) {
        reporter.incrCounter(Counters.EMPTY_LINLK_LIST, 1);
      } else {

        // check to see if source root domain is a super domain
        boolean sourceDomainIsSuperDomain = (superDomainFilter
            .filterItemByHashIdV2(sourceFP.getRootDomainHash()) == FilterResult.Filter_Accept);

        // iterate list
        DataInputBuffer inputStream = new DataInputBuffer();
        inputStream.reset(value.getBytes(), 0, value.getLength());

        CompressedURLFPListV2.Reader reader = new CompressedURLFPListV2.Reader(inputStream);

        if ((reader.getStreamFlags() & CompressedURLFPListV2.FLAG_IS_PERMANENT_REDIRECT) != 0) {
          sourceFP.setFlags(URLFPV2.Flag.IsRedirect);
        }

        int intraDomainLinkCount = 0;
        int extraDomainCount = 0;

        while (reader.hasNext()) {

          URLFPV2 targetFP = reader.next();

          boolean isIntraDomainLink = false;

          if (sourceDomainIsSuperDomain) {
            if (targetFP.getDomainHash() == sourceFP.getDomainHash()) {
              isIntraDomainLink = true;
              ++intraDomainLinkCount;
            }
          } else {
            if (targetFP.getRootDomainHash() == sourceFP.getRootDomainHash()) {
              ++intraDomainLinkCount;
              isIntraDomainLink = true;
            }
          }

          if (!isIntraDomainLink) {
            valueBuffer.reset();

            writeURLFPToStream(sourceFP, valueBuffer);

            BytesWritable valueObject = new BytesWritable(valueBuffer.getData());
            valueObject.setSize(valueBuffer.getLength());

            output.collect(targetFP, valueObject);

            ++extraDomainCount;

            if (extraDomainCount == MAX_OUTLINKS) {
              break;
            }

          }
        }
      }
    }

    int _numPartitions;

    @Override
    public void configure(JobConf job) {

      _numPartitions = job.getNumReduceTasks();

      LOG.info("Loading superDomainFilter to Cache");
      try {
        superDomainFilter.loadFromCache(job);
      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
      }
    }

    @Override
    public void close() throws IOException {
      // TODO Auto-generated method stub

    }
  }

}
