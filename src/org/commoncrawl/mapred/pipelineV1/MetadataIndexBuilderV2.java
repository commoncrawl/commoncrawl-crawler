package org.commoncrawl.mapred.pipelineV1;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.util.Collections;
import java.util.Iterator;
import java.util.Vector;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.file.tfile.TFile;
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
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.protocol.ArchiveInfo;
import org.commoncrawl.protocol.CrawlDatumAndMetadata;
import org.commoncrawl.protocol.CrawlURLMetadata;
import org.commoncrawl.protocol.SubDomainMetadata;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.CompressURLListV2;
import org.commoncrawl.util.CrawlDatum;
import org.commoncrawl.util.FSByteBufferInputStream;
import org.commoncrawl.util.FileUtils;
import org.commoncrawl.util.GoogleURL;
import org.commoncrawl.util.NodeAffinityMaskBuilder;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLFingerprint;
import org.commoncrawl.util.URLUtils;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileMergeInputFormat;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileMergePartitioner;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader.KeyAndValueData;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader.RawRecordValue;
import org.commoncrawl.util.Tuples.LongTextBytesTuple;
import org.commoncrawl.util.Tuples.TriTextBytesTuple;
import org.commoncrawl.util.URLUtils.URLFPV2RawComparator;

public class MetadataIndexBuilderV2 extends CrawlDBCustomJob {

  private static final Log LOG = LogFactory.getLog(MetadataIndexBuilderV2.class);

  @Override
  public String getJobDescription() {
    return "Metadata Index Builder";
  }

  public static final String SUBDOMAIN_INDEX_ID_TO_METADATA = "idToMetadata";
  public static final String SUBDOMAIN_INDEX_ID_TO_NAME = "idToString";
  public static final String SUBDOMAIN_INDEX_NAME_TO_METADATA = "nameToMetadata";

  @Override
  public void runJob() throws IOException {

    Vector<Long> timestamps = gatherDatabaseTimestamps(new Path("crawl/crawldb_new"));

    if (timestamps.size() >= 2) {

      long crawlDBTimestamp = timestamps.lastElement();
      long candidateTimestamp = timestamps.get(timestamps.size() - 2);

      LOG.info("Using CrawlDB Candidate:" + crawlDBTimestamp);

      FileSystem fs = CrawlEnvironment.getDefaultFileSystem();

      // build relevant paths ...
      Path crawlDBPath = new Path("crawl/crawldb_new/" + crawlDBTimestamp);

      Path crawlDBMetadataTemp = new Path(CrawlEnvironment.getHadoopConfig().get("mapred.temp.dir", ".")
          + "/fptoMetadataIndexBuilder-crawlDBMetadata-" + candidateTimestamp);
      Path phase2OutputDir = new Path(CrawlEnvironment.getHadoopConfig().get("mapred.temp.dir", ".")
          + "/fptoMetadataIndexBuilder-consolidatedMetadata-" + candidateTimestamp);
      Path s3MetadataTemp = new Path(CrawlEnvironment.getHadoopConfig().get("mapred.temp.dir", ".")
          + "/s3Metadata-Temp");

      fs.mkdirs(new Path("crawl/metadatadb/" + candidateTimestamp + "/urlMetadata"));
      fs.mkdirs(new Path("crawl/metadatadb/" + candidateTimestamp + "/subDomainMetadata"));

      Path seedOutputDir = new Path("crawl/metadatadb/" + candidateTimestamp + "/urlMetadata/seed");
      Path indexOutputDir = new Path("crawl/metadatadb/" + candidateTimestamp + "/urlMetadata/index");
      Path subDomainStatsRaw = new Path("crawl/metadatadb/" + candidateTimestamp + "/subDomainMetadata/raw");
      Path subDomainIdToMetadataIndex = new Path("crawl/metadatadb/" + candidateTimestamp + "/subDomainMetadata/"
          + SUBDOMAIN_INDEX_ID_TO_METADATA);
      Path subDomainIdToNameIndex = new Path("crawl/metadatadb/" + candidateTimestamp + "/subDomainMetadata/"
          + SUBDOMAIN_INDEX_ID_TO_NAME);
      Path subDomainNameToMetadata = new Path("crawl/metadatadb/" + candidateTimestamp + "/subDomainMetadata/"
          + SUBDOMAIN_INDEX_NAME_TO_METADATA);
      Path s3Metadata = new Path("crawl/metadatadb/s3Metadata");

      // Phase 1: Pull in S3 Info
      if (!fs.exists(s3Metadata)) {
        LOG.info("Collecting S3Metadata");
        if (buildOldS3ArchiveInfo(candidateTimestamp, crawlDBPath, s3MetadataTemp)) {
          fs.rename(s3MetadataTemp, s3Metadata);
        }
      }

      // Phase 2: Consolidate Metadata from (PR Database,Link Databaase,Inverse
      // Link DB, and CrawlDB)
      if (!fs.exists(indexOutputDir)) {
        LOG.info("Running Metadata Index Builder");
        if (buildConsolidatedMetadataIndex(candidateTimestamp, crawlDBPath, s3Metadata, phase2OutputDir)) {
          fs.rename(phase2OutputDir, indexOutputDir);
          // delete subdomain stats dir
          fs.delete(subDomainStatsRaw, true);
          // recreate it
          fs.mkdirs(subDomainStatsRaw);
          // now move sub domain stats files
          FileStatus subDomainFiles[] = fs.globStatus(new Path(indexOutputDir, "*.domainMetadata"));
          for (FileStatus subDomainFile : subDomainFiles) {
            Path originalLoc = subDomainFile.getPath();
            Path newLocation = new Path(subDomainStatsRaw, originalLoc.getName().split("\\.")[0]);
            LOG.info("Moving: " + originalLoc + " to New Loc:" + newLocation);
            fs.rename(originalLoc, newLocation);
          }
        }
      }

      if (!fs.exists(subDomainIdToMetadataIndex)) {
        LOG.info("Generating Subdomain TFile");
        Path subDomainMetadataTemp = new Path(CrawlEnvironment.getHadoopConfig().get("mapred.temp.dir", ".")
            + "/subDomainMetadataTFILEGEN-" + candidateTimestamp);
        if (buildSubDomainIdToMetadataIndex(candidateTimestamp, subDomainStatsRaw, subDomainMetadataTemp)) {

          fs.delete(subDomainIdToNameIndex, true);
          LOG.info("Creating subDomainStringIndex file at:" + subDomainIdToNameIndex);
          fs.mkdirs(subDomainIdToNameIndex);
          FileStatus stringIndexFiles[] = fs.globStatus(new Path(subDomainMetadataTemp, "strings-part-*"));
          for (FileStatus stringIndexFile : stringIndexFiles) {
            LOG.info("Moving:" + stringIndexFile.getPath() + " to:" + subDomainIdToNameIndex);
            fs.rename(stringIndexFile.getPath(), new Path(subDomainIdToNameIndex, stringIndexFile.getPath().getName()
                .substring("strings-".length())));
          }
          LOG.info("Moving :" + subDomainMetadataTemp + " to:" + subDomainIdToMetadataIndex);
          fs.rename(subDomainMetadataTemp, subDomainIdToMetadataIndex);
        }
      }

      if (!fs.exists(subDomainNameToMetadata)) {
        LOG.info("Generating SubDomain Name to Metadata Index");
        Path subDomainMetadataTemp = new Path(CrawlEnvironment.getHadoopConfig().get("mapred.temp.dir", ".")
            + "/subDomainMetadataNAMETOMETADATA-" + candidateTimestamp);
        if (buildSubDomainNameToMetadataIndex(candidateTimestamp, subDomainStatsRaw, subDomainMetadataTemp)) {

          fs.delete(subDomainNameToMetadata, true);
          LOG.info("Creating subDomainStringIndex file at:" + subDomainNameToMetadata);
          fs.rename(subDomainMetadataTemp, subDomainNameToMetadata);
        }
      }

      /*
       * // Phase 3 .. build a list of domains sorted by subdomain Path
       * phase3OutputDir = new
       * Path(CrawlEnvironment.getHadoopConfig().get("mapred.temp.dir", ".") +
       * "/subDomains-Intermediate"+ candidateTimestamp); if
       * (!fs.exists(phase3OutputDir)) {
       * LOG.info("Running Phase 3 - Generate Intermediate Sub Domain List");
       * buildIntermediateDomainList(urlSeedPath,phase3OutputDir); }
       */
      Path phase4OutputDir = new Path(CrawlEnvironment.getHadoopConfig().get("mapred.temp.dir", ".")
          + "/subDomains-Consolidated" + candidateTimestamp);

    }

  }

  public static class TFileBugWorkaroundDomainHashAndURLHashComparator implements RawComparator<Object> {

    DataInputBuffer stream1 = new DataInputBuffer();
    DataInputBuffer stream2 = new DataInputBuffer();

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      if (l1 == 0 && l2 != 0)
        return -1;
      else if (l1 != 0 && l2 == 0)
        return 1;
      else {
        try {
          stream1.reset(b1, s1, l1);
          stream2.reset(b2, s2, l2);

          long domainHash1 = stream1.readLong();
          long domainHash2 = stream2.readLong();

          int result = (domainHash1 == domainHash2) ? 0 : (domainHash1 < domainHash2) ? -1 : 1;

          if (result == 0) {
            long urlHash1 = stream1.readLong();
            long urlHash2 = stream2.readLong();

            result = (urlHash1 == urlHash2) ? 0 : (urlHash1 < urlHash2) ? -1 : 1;
          }

          return result;
        } catch (IOException e) {
          LOG.error(CCStringUtils.stringifyException(e));
          throw new RuntimeException("Unexpected IOException in RawComparator!!");
        }
      }
    }

    @Override
    public int compare(Object o1, Object o2) {
      throw new RuntimeException("Not Supported!");
    }

  }

  public static class TFileBugWorkaroundLongWritableComparator extends LongWritable.Comparator {

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      if (l1 == 0 && l2 != 0)
        return -1;
      else if (l1 != 0 && l2 == 0)
        return 1;
      else
        return super.compare(b1, s1, l1, b2, s2, l2);
    }

  }

  private boolean buildOldS3ArchiveInfo(long candidateTimestamp, Path crawlDBPath, Path outputPath) {

    try {

      FileSystem fs = CrawlEnvironment.getDefaultFileSystem();
      Configuration conf = CrawlEnvironment.getHadoopConfig();

      fs.delete(outputPath, true);

      Path searchPattern = new Path("crawl/s3metadata/*/part-[0-9]*");

      JobConf job = new JobConf(conf);

      job.setJobName(getJobDescription() + " - Generte ArchiveInfo for S3Data");

      // set node affinity ...
      String affinityMask = NodeAffinityMaskBuilder.buildNodeAffinityMask(FileSystem.get(job), crawlDBPath, null);
      NodeAffinityMaskBuilder.setNodeAffinityMask(job, affinityMask);

      // add parts
      FileStatus candidates[] = fs.globStatus(searchPattern);
      for (FileStatus candidate : candidates) {
        if (!candidate.getPath().toString().endsWith(".log")) {
          LOG.info("Adding Path:" + candidate.getPath());
          FileInputFormat.addInputPath(job,candidate.getPath());
        }
      }

      job.setInputFormat(SequenceFileInputFormat.class);
      job.setMapOutputKeyClass(URLFPV2.class);
      job.setMapOutputValueClass(ArchiveInfo.class);
      job.setMapperClass(S3MetadataMapper.class);
      job.setOutputKeyClass(URLFPV2.class);
      job.setOutputValueClass(ArchiveInfo.class);
      job.setOutputFormat(SequenceFileOutputFormat.class);
      job.setReducerClass(IdentityReducer.class);
      job.setNumTasksToExecutePerJvm(1000);
      FileOutputFormat.setOutputPath(job,outputPath);
      job.setNumReduceTasks(CrawlEnvironment.NUM_DB_SHARDS);

      LOG.info("Running  " + getJobDescription() + " OutputDir:" + outputPath);
      JobClient.runJob(job);
      LOG.info("Finished Running " + getJobDescription() + " OutputDir:" + outputPath);

      return true;

    } catch (IOException e) {
      LOG.error(CCStringUtils.stringifyException(e));
      return false;
    }

  }

  public static class S3MetadataMapper implements Mapper<Text, CrawlURLMetadata, URLFPV2, ArchiveInfo> {

    enum Counters {
      BAD_URL, NO_FILENAME_IN_ARCHIVE_INFO
    }

    @Override
    public void map(Text key, CrawlURLMetadata metadata, OutputCollector<URLFPV2, ArchiveInfo> output, Reporter reporter)
        throws IOException {

      // map key to canoncial url fp
      URLFPV2 fp = URLUtils.getURLFPV2FromURL(key.toString());

      if (fp != null) {
        ArchiveInfo archiveInfo = new ArchiveInfo();

        archiveInfo.setArcfileOffset(metadata.getArcFileOffset());

        // grab date from arc file name
        if (metadata.isFieldDirty(CrawlURLMetadata.Field_ARCFILENAME)) {
          int indexOfForwardSlash = metadata.getArcFileName().lastIndexOf('/');
          int indexOfUnderscore = metadata.getArcFileName().indexOf('_');
          int indexOfDot = metadata.getArcFileName().indexOf('.');

          String timestampComponent = metadata.getArcFileName().substring(indexOfForwardSlash + 1, indexOfUnderscore);
          String idComponent = metadata.getArcFileName().substring(indexOfUnderscore + 1, indexOfDot);

          archiveInfo.setArcfileDate(Long.parseLong(timestampComponent));
          archiveInfo.setArcfileIndex(Integer.parseInt(idComponent));

          archiveInfo.setCrawlNumber(1);

          output.collect(fp, archiveInfo);
        } else {
          reporter.incrCounter(Counters.NO_FILENAME_IN_ARCHIVE_INFO, 1);
        }
      } else {
        reporter.incrCounter(Counters.BAD_URL, 1);
      }
    }

    @Override
    public void configure(JobConf job) {

    }

    @Override
    public void close() throws IOException {

    }

  }

  private boolean buildSubDomainIdToMetadataIndex(long candidateTimestamp, Path subDomainStatsRaw,
      Path subDomainFinalTemp) {

    try {

      FileSystem fs = CrawlEnvironment.getDefaultFileSystem();
      Configuration conf = CrawlEnvironment.getHadoopConfig();

      fs.delete(subDomainFinalTemp, true);

      JobConf job = new JobConf(conf);

      job.setJobName(getJobDescription() + " - Generate SubDomain TFile");

      // add seed db as input
      FileInputFormat.addInputPath(job,subDomainStatsRaw);

      job.setInputFormat(SequenceFileInputFormat.class);
      job.setMapOutputKeyClass(LongWritable.class);
      job.setMapOutputValueClass(SubDomainMetadata.class);
      job.setMapperClass(IdentityMapper.class);
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(NullWritable.class);
      job.setOutputFormat(NullOutputFormat.class);
      job.setReducerClass(SubDomainMetadataDomainIdToIndexWriter.class);
      job.setNumTasksToExecutePerJvm(1000);
      FileOutputFormat.setOutputPath(job,subDomainFinalTemp);
      job.setNumReduceTasks(CrawlEnvironment.NUM_DB_SHARDS);

      LOG.info("Running  " + getJobDescription() + " OutputDir:" + subDomainFinalTemp);
      JobClient.runJob(job);
      LOG.info("Finished Running " + getJobDescription() + " OutputDir:" + subDomainFinalTemp);
    } catch (IOException e) {
      LOG.error(CCStringUtils.stringifyException(e));
      return false;
    }
    return true;
  }

  private boolean buildSubDomainNameToMetadataIndex(long candidateTimestamp, Path subDomainStatsRaw,
      Path subDomainFinalTemp) {

    try {

      FileSystem fs = CrawlEnvironment.getDefaultFileSystem();
      Configuration conf = CrawlEnvironment.getHadoopConfig();

      fs.delete(subDomainFinalTemp, true);

      JobConf job = new JobConf(conf);

      job.setJobName(getJobDescription() + " - Generate SubDomain Name to Metadata Index");

      // add seed db as input
      FileInputFormat.addInputPath(job,subDomainStatsRaw);

      job.setInputFormat(SequenceFileInputFormat.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(SubDomainMetadata.class);
      job.setMapperClass(SubDomainStatsToNameMapper.class);
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(NullWritable.class);
      job.setOutputFormat(NullOutputFormat.class);
      job.setReducerClass(SubDomainMetadataNameToIndexWriter.class);
      job.setNumTasksToExecutePerJvm(1000);
      FileOutputFormat.setOutputPath(job,subDomainFinalTemp);
      job.setNumReduceTasks(CrawlEnvironment.NUM_DB_SHARDS);

      LOG.info("Running  " + getJobDescription() + " OutputDir:" + subDomainFinalTemp);
      JobClient.runJob(job);
      LOG.info("Finished Running " + getJobDescription() + " OutputDir:" + subDomainFinalTemp);
    } catch (IOException e) {
      LOG.error(CCStringUtils.stringifyException(e));
      return false;
    }
    return true;
  }

  public static class SubDomainStatsToNameMapper implements
      Mapper<LongWritable, SubDomainMetadata, Text, SubDomainMetadata> {

    @Override
    public void map(LongWritable key, SubDomainMetadata metadata, OutputCollector<Text, SubDomainMetadata> output,
        Reporter reporter) throws IOException {
      String domainName = metadata.getDomainText();
      String normalized = URLUtils.normalizeHostName(domainName, true);
      if (normalized != null) {
        domainName = normalized;
      }
      output.collect(new Text(domainName), metadata);
    }

    @Override
    public void configure(JobConf job) {

    }

    @Override
    public void close() throws IOException {
      // TODO Auto-generated method stub

    }

  }

  public static class SubDomainMetadataNameToIndexWriter implements
      Reducer<Text, SubDomainMetadata, NullWritable, NullWritable> {

    JobConf _conf;
    FileSystem _fs;
    TFile.Writer _writer = null;
    FSDataOutputStream _outputStream = null;
    DataOutputBuffer _keyStream = new DataOutputBuffer();
    DataOutputBuffer _valueStream = new DataOutputBuffer();

    private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
    static {
      NUMBER_FORMAT.setMinimumIntegerDigits(5);
      NUMBER_FORMAT.setGroupingUsed(false);
    }

    @Override
    public void reduce(Text key, Iterator<SubDomainMetadata> values,
        OutputCollector<NullWritable, NullWritable> output, Reporter reporter) throws IOException {

      if (_outputStream == null || _writer == null) {
        throw new IOException("Streams not initialized!");
      }
      SubDomainMetadata firstValue = null;
      try {
        firstValue = (SubDomainMetadata) values.next().clone();
      } catch (CloneNotSupportedException e) {
      }

      while (values.hasNext()) {
        SubDomainMetadata nextValue = values.next();

        if (firstValue.getDomainText().length() == 0 && nextValue.getDomainText().length() != 0) {
          firstValue.setDomainText(nextValue.getDomainText());
        }

        firstValue.setUrlCount(firstValue.getUrlCount() + nextValue.getUrlCount());
        firstValue.setUnfetchedCount(firstValue.getUnfetchedCount() + nextValue.getUnfetchedCount());
        firstValue.setFetchedCount(firstValue.getFetchedCount() + nextValue.getFetchedCount());
        firstValue.setGoneCount(firstValue.getGoneCount() + nextValue.getGoneCount());
        firstValue.setRedirectTemporaryCount(firstValue.getRedirectTemporaryCount()
            + nextValue.getRedirectTemporaryCount());
        firstValue.setRedirectPermCount(firstValue.getRedirectPermCount() + nextValue.getRedirectPermCount());
        firstValue.setUnmodifiedCount(firstValue.getUnmodifiedCount() + nextValue.getUnmodifiedCount());
        firstValue.setHasPageRankCount(firstValue.getHasPageRankCount() + nextValue.getHasPageRankCount());
        firstValue.setHasLinkListCount(firstValue.getHasLinkListCount() + nextValue.getHasLinkListCount());
        firstValue.setHasInverseLinkListCount(firstValue.getHasInverseLinkListCount()
            + nextValue.getHasInverseLinkListCount());
        firstValue.setHasArcFileInfoCount(firstValue.getHasArcFileInfoCount() + nextValue.getHasArcFileInfoCount());
        firstValue.setHasParseSegmentInfoCount(firstValue.getHasParseSegmentInfoCount()
            + nextValue.getHasParseSegmentInfoCount());
        firstValue.setHasSignatureCount(firstValue.getHasSignatureCount() + nextValue.getHasSignatureCount());
        firstValue.setLatestFetchTime(Math.max(firstValue.getLatestFetchTime(), nextValue.getLatestFetchTime()));

      }

      _keyStream.reset();
      _valueStream.reset();
      key.write(_keyStream);
      firstValue.write(_valueStream);
      _writer.append(_keyStream.getData(), 0, _keyStream.getLength(), _valueStream.getData(), 0, _valueStream
          .getLength());
    }

    @Override
    public void configure(JobConf job) {
      _conf = job;
      try {
        _fs = FileSystem.get(_conf);
        int partitionNumber = job.getInt("mapred.task.partition", -1);
        Path outputPath = new Path(FileOutputFormat.getWorkOutputPath(_conf), "part-"
            + NUMBER_FORMAT.format(partitionNumber));
        _outputStream = _fs.create(outputPath);
        _writer = new TFile.Writer(_outputStream, 64 * 1024, TFile.COMPRESSION_LZO, TFile.COMPARATOR_JCLASS
            + Text.Comparator.class.getName(), _conf);

      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
      }
    }

    @Override
    public void close() throws IOException {
      _writer.close();
      _outputStream.close();
    }

  }

  public static class SubDomainMetadataDomainIdToIndexWriter implements
      Reducer<LongWritable, SubDomainMetadata, NullWritable, NullWritable> {

    JobConf _conf;
    FileSystem _fs;
    TFile.Writer _writer = null;
    TFile.Writer _stringIndexWriter = null;
    FSDataOutputStream _outputStream = null;
    FSDataOutputStream _stringIndexOutputStream = null;
    DataOutputBuffer _keyStream = new DataOutputBuffer();
    DataOutputBuffer _valueStream = new DataOutputBuffer();
    DataOutputBuffer _stringsValueStream = new DataOutputBuffer();

    private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
    static {
      NUMBER_FORMAT.setMinimumIntegerDigits(5);
      NUMBER_FORMAT.setGroupingUsed(false);
    }

    @Override
    public void reduce(LongWritable key, Iterator<SubDomainMetadata> values,
        OutputCollector<NullWritable, NullWritable> output, Reporter reporter) throws IOException {

      if (_outputStream == null || _writer == null) {
        throw new IOException("Streams not initialized!");
      }
      SubDomainMetadata firstValue = null;
      try {
        firstValue = (SubDomainMetadata) values.next().clone();
      } catch (CloneNotSupportedException e) {
      }

      while (values.hasNext()) {
        SubDomainMetadata nextValue = values.next();

        if (firstValue.getDomainText().length() == 0 && nextValue.getDomainText().length() != 0) {
          firstValue.setDomainText(nextValue.getDomainText());
        }

        firstValue.setUrlCount(firstValue.getUrlCount() + nextValue.getUrlCount());
        firstValue.setUnfetchedCount(firstValue.getUnfetchedCount() + nextValue.getUnfetchedCount());
        firstValue.setFetchedCount(firstValue.getFetchedCount() + nextValue.getFetchedCount());
        firstValue.setGoneCount(firstValue.getGoneCount() + nextValue.getGoneCount());
        firstValue.setRedirectTemporaryCount(firstValue.getRedirectTemporaryCount()
            + nextValue.getRedirectTemporaryCount());
        firstValue.setRedirectPermCount(firstValue.getRedirectPermCount() + nextValue.getRedirectPermCount());
        firstValue.setUnmodifiedCount(firstValue.getUnmodifiedCount() + nextValue.getUnmodifiedCount());
        firstValue.setHasPageRankCount(firstValue.getHasPageRankCount() + nextValue.getHasPageRankCount());
        firstValue.setHasLinkListCount(firstValue.getHasLinkListCount() + nextValue.getHasLinkListCount());
        firstValue.setHasInverseLinkListCount(firstValue.getHasInverseLinkListCount()
            + nextValue.getHasInverseLinkListCount());
        firstValue.setHasArcFileInfoCount(firstValue.getHasArcFileInfoCount() + nextValue.getHasArcFileInfoCount());
        firstValue.setHasParseSegmentInfoCount(firstValue.getHasParseSegmentInfoCount()
            + nextValue.getHasParseSegmentInfoCount());
        firstValue.setHasSignatureCount(firstValue.getHasSignatureCount() + nextValue.getHasSignatureCount());
        firstValue.setLatestFetchTime(Math.max(firstValue.getLatestFetchTime(), nextValue.getLatestFetchTime()));

      }

      _keyStream.reset();
      _valueStream.reset();
      _stringsValueStream.reset();
      key.write(_keyStream);
      firstValue.write(_valueStream);
      String domainNameOut = firstValue.getDomainText();
      // on normalize it
      String normalizedName = URLUtils.normalizeHostName(domainNameOut, true);
      if (normalizedName != null) {
        domainNameOut = normalizedName;
      }
      _stringsValueStream.writeUTF(domainNameOut);

      LOG.info("Key:" + key.get());
      _writer.append(_keyStream.getData(), 0, _keyStream.getLength(), _valueStream.getData(), 0, _valueStream
          .getLength());
      _stringIndexWriter.append(_keyStream.getData(), 0, _keyStream.getLength(), _stringsValueStream.getData(), 0,
          _stringsValueStream.getLength());
    }

    @Override
    public void configure(JobConf job) {
      _conf = job;
      try {
        _fs = FileSystem.get(_conf);
        int partitionNumber = job.getInt("mapred.task.partition", -1);
        Path outputPath = new Path(FileOutputFormat.getWorkOutputPath(_conf), "part-"
            + NUMBER_FORMAT.format(partitionNumber));
        _outputStream = _fs.create(outputPath);
        _writer = new TFile.Writer(_outputStream, 64 * 1024, TFile.COMPRESSION_LZO, TFile.COMPARATOR_JCLASS
            + TFileBugWorkaroundLongWritableComparator.class.getName(), _conf);
        Path stringsOutputPath = new Path(FileOutputFormat.getWorkOutputPath(_conf), "strings-part-"
            + NUMBER_FORMAT.format(partitionNumber));
        _stringIndexOutputStream = _fs.create(stringsOutputPath);
        _stringIndexWriter = new TFile.Writer(_stringIndexOutputStream, 64 * 1024, TFile.COMPRESSION_LZO,
            TFile.COMPARATOR_JCLASS + TFileBugWorkaroundLongWritableComparator.class.getName(), _conf);

      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
      }
    }

    @Override
    public void close() throws IOException {
      _writer.close();
      _outputStream.close();
      _stringIndexWriter.close();
      _stringIndexOutputStream.close();
    }

  }

  private void buildIntermediateDomainList(Path urlSeedPath, Path outputPath) throws IOException {
    FileSystem fs = CrawlEnvironment.getDefaultFileSystem();
    Configuration conf = CrawlEnvironment.getHadoopConfig();

    // delete exisiting output directory if it exists ...
    fs.delete(outputPath, true);

    JobConf job = new JobConf(conf);

    job.setJobName(getJobDescription() + " - Generate Domain List Intermediate");

    // add seed db as input
    FileInputFormat.addInputPath(job,urlSeedPath);

    job.setInputFormat(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setMapperClass(SeedDatabaseToHostNameMapper.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);
    job.setCombinerClass(SeedDatabaseToHostNameCombiner.class);
    job.setReducerClass(SeedDatabaseToHostNameReducer.class);
    job.setNumTasksToExecutePerJvm(1000);
    FileOutputFormat.setOutputPath(job,outputPath);

    LOG.info("Running  " + getJobDescription() + " OutputDir:" + outputPath);
    JobClient.runJob(job);
    LOG.info("Finished Running " + getJobDescription() + " OutputDir:" + outputPath);

  }

  public static class SeedDatabaseToHostNameCombiner implements Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
        Reporter reporter) throws IOException {
      output.collect(key, values.next());
    }

    @Override
    public void configure(JobConf job) {

    }

    @Override
    public void close() throws IOException {

    }
  }

  public static class SeedDatabaseToHostNameReducer implements Reducer<Text, IntWritable, LongWritable, Text> {

    @Override
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<LongWritable, Text> output,
        Reporter reporter) throws IOException {
      long fingerprint = URLFingerprint.generate64BitURLFPrint(key.toString());
      output.collect(new LongWritable(fingerprint), key);
    }

    @Override
    public void configure(JobConf job) {
    }

    @Override
    public void close() throws IOException {
    }

  }

  public static class SeedDatabaseToHostNameMapper implements Mapper<URLFPV2, LongTextBytesTuple, Text, IntWritable> {

    IntWritable valueOut = new IntWritable(1);

    @Override
    public void map(URLFPV2 key, LongTextBytesTuple value, OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException {
      // ok get the url from value
      String url = value.getTextValueBytes().toString();
      // now extract components
      GoogleURL urlObject = new GoogleURL(url);
      // get host IF valid
      if (urlObject.isValid() && urlObject.getHost().length() != 0) {
        String host = urlObject.getHost();
        output.collect(new Text(host), valueOut);
      }
    }

    @Override
    public void configure(JobConf job) {

    }

    @Override
    public void close() throws IOException {

    }
  }

  /**
   * Build a consolidated Metadata Index
   * 
   * @param rootTimestamp
   * @param crawlDBMetadataLocation
   * @param pageRankMetadataLocation
   * @param linkDBDataLocation
   * @param inverseLinkDBDataLocation
   * @param outputPath
   * @return
   */
  private boolean buildConsolidatedMetadataIndex(long rootTimestamp, Path crawlDBPath, Path s3Metadata, Path outputPath) {
    try {

      FileSystem fs = CrawlEnvironment.getDefaultFileSystem();
      Configuration conf = CrawlEnvironment.getHadoopConfig();

      // delete exisiting output directory if it exists ...
      fs.delete(outputPath, true);

      JobConf job = new JobConf(conf);

      job.setJobName(getJobDescription() + " - Merge Metadata");

      // add prior job outputs
      // FileInputFormat.addInputPath(job,urldbSeedPath);
      FileInputFormat.addInputPath(job,crawlDBPath);
      FileInputFormat.addInputPath(job,s3Metadata);

      // set node affinity ...
      String affinityMask = NodeAffinityMaskBuilder.buildNodeAffinityMask(FileSystem.get(job), crawlDBPath, null);
      NodeAffinityMaskBuilder.setNodeAffinityMask(job, affinityMask);

      // set root timestamp in job ...
      job.setLong("root.timestamp", rootTimestamp);

      // multi file merger
      job.setInputFormat(MultiFileMergeInputFormat.class);
      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(Text.class);
      job.setMapperClass(IdentityMapper.class);
      job.setReducerClass(ConsolidatedMetadataIndexWriter.class);
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(NullWritable.class);
      job.setOutputFormat(NullOutputFormat.class);
      job.setPartitionerClass(MultiFileMergePartitioner.class);
      FileOutputFormat.setOutputPath(job,outputPath);
      job.setNumReduceTasks(92);
      job.setNumTasksToExecutePerJvm(1000);

      LOG.info("Running  " + getJobDescription() + " OutputDir:" + outputPath);
      JobClient.runJob(job);
      LOG.info("Finished Running " + getJobDescription() + " OutputDir:" + outputPath);

      return true;
    } catch (IOException e) {
      LOG.error(CCStringUtils.stringifyException(e));
      return false;
    }
  }

  static Text HACK_URL_KEY = new Text("HACK_URL_KEY");

  private void extractMetadataFromCrawlDB(Path affinityPath, Path outputPath) throws IOException {

    FileSystem fs = CrawlEnvironment.getDefaultFileSystem();

    Configuration conf = CrawlEnvironment.getHadoopConfig();

    JobConf job = new JobConf(conf);

    job.setJobName(getJobDescription() + " - Phase 1");

    Path crawlDBPath = new Path("crawl/crawldb/current");

    // add merged link db as input
    FileInputFormat.addInputPath(job,crawlDBPath);

    // set node affinity
    String nodeAffinityMask = NodeAffinityMaskBuilder.buildNodeAffinityMask(fs, affinityPath, null);
    LOG.info("Using NodeAffinityMask:" + nodeAffinityMask);
    NodeAffinityMaskBuilder.setNodeAffinityMask(job, nodeAffinityMask);

    job.setInputFormat(SequenceFileInputFormat.class);
    job.setMapperClass(FingerprintToDatumMapper.class);
    job.setMapOutputKeyClass(URLFPV2.class);
    job.setMapOutputValueClass(CrawlDatum.class);
    job.setReducerClass(IdentityReducer.class);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(URLFPV2.class);
    job.setOutputValueClass(CrawlDatum.class);
    FileOutputFormat.setOutputPath(job,outputPath);
    job.setNumReduceTasks(92);
    job.setNumTasksToExecutePerJvm(1000);
    job.setCompressMapOutput(false);

    LOG.info("Running  " + job.getJobName());
    JobClient.runJob(job);
    LOG.info("Finished Running" + job.getJobName());
  }

  public static class FingerprintToDatumMapper implements Mapper<Text, CrawlDatum, URLFPV2, CrawlDatum> {

    @Override
    public void map(Text key, CrawlDatum value, OutputCollector<URLFPV2, CrawlDatum> output, Reporter reporter)
        throws IOException {
      URLFPV2 fingerprint = URLUtils.getURLFPV2FromURL(key.toString());
      if (fingerprint != null) {
        value.getMetaData().put(HACK_URL_KEY, key);
        output.collect(fingerprint, value);
      }
    }

    @Override
    public void configure(JobConf job) {
      // TODO Auto-generated method stub

    }

    @Override
    public void close() throws IOException {
      // TODO Auto-generated method stub

    }

  }

  private Vector<Long> gatherDatabaseTimestamps(Path rootPath) throws IOException {

    Vector<Long> timestampsOut = new Vector<Long>();

    FileSystem fs = CrawlEnvironment.getDefaultFileSystem();

    FileStatus candidates[] = fs.globStatus(new Path(rootPath, "*"));

    for (FileStatus candidate : candidates) {
      LOG.info("Found Seed Candidate:" + candidate.getPath());
      long timestamp = Long.parseLong(candidate.getPath().getName());
      timestampsOut.add(timestamp);
    }

    Collections.sort(timestampsOut);

    return timestampsOut;
  }

  /** 
	 * 
	 */
  public static class ConsolidatedMetadataIndexWriter implements Reducer<IntWritable, Text, NullWritable, NullWritable> {

    JobConf _conf;
    FileSystem _fs;
    long _rootTimestamp;

    private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
    static {
      NUMBER_FORMAT.setMinimumIntegerDigits(5);
      NUMBER_FORMAT.setGroupingUsed(false);
    }

    enum Counters {
      HAD_ONLY_METADATA, HAD_ONLY_PR, HAD_ONLY_LINKDATA, HAD_ONLY_INVERSELINKDATA, HAD_METADATA_AND_PR,
      HAD_METADATA_AND_LINKDATA, HAD_METADATA_AND_INVERSELINKDATA, HAD_METADATA_AND_ALL_LINKDATA, HAD_ALL_DATA,
      HAD_METADATA_PR_AND_LINKDATA, HAD_METADATA_PR_AND_INVERSE_LINKDATA, HAD_LINKDATA_AND_INVERSE_LINKDATA,
      HAD_PR_LINKDATA_AND_INVERSE_LINKDATA, HAD_ONLY_URL, HAD_REDIRECT_LOCATION_IN_METADATA, FOUND_S3_ARCHIVEINFO,
      FOUND_CRAWLONE_ARCHIVE_INFO, FOUND_CURRENTCRAWL_ARCHIVE_INFO, FOUND_UNKNOWNCRAWL_ARCHIVE_INFO,
      URLDB_RECORD_WITHOUT_URL, HAD_URL, WROTE_SINGLE_RECORD, HAD_INV_LINKDB_DATA, HAD_LINKDB_DATA, HAD_PAGERANK

    }

    static final String FP_TO_PR_PATTERN = "fpToPR";
    static final String LINKDB_PATTERN = "linkdb/merged";
    static final String INVERSE_LINKDB_PATTERN = "inverse_linkdb/merged";
    static final String CRAWLDB_METADATA_PATTERN = "/urlMetadata/seed";
    static final String S3_METADATA_PATTERN = "/s3Metadata/";
    static final String URLDB_PATTERN = "/crawl/crawldb_new";

    static final int HAD_METADATA = 1 << 0;
    static final int HAD_PR = 1 << 1;
    static final int HAD_LINKDATA = 1 << 2;
    static final int HAD_INVERSE_LINKDATA = 1 << 3;
    static final int HAD_URL = 1 << 4;

    static final int MASK_HAD_METADATA_AND_PR = HAD_URL | HAD_METADATA | HAD_PR;
    static final int MASK_HAD_METADATA_AND_LINKDATA = HAD_URL | HAD_METADATA | HAD_LINKDATA;
    static final int MASK_HAD_METADATA_AND_INVERSE_LINKDATA = HAD_URL | HAD_METADATA | HAD_INVERSE_LINKDATA;
    static final int MASK_HAD_METADATA_AND_ALL_LINKDATA = HAD_URL | HAD_METADATA | HAD_LINKDATA | HAD_INVERSE_LINKDATA;
    static final int MASK_HAD_METADATA_PR_AND_LINKDATA = HAD_URL | HAD_METADATA | HAD_LINKDATA | HAD_PR;
    static final int MASK_HAD_METADATA_PR_AND_INVERSE_LINKDATA = HAD_URL | HAD_METADATA | HAD_INVERSE_LINKDATA | HAD_PR;
    static final int MASK_HAD_ALL_DATA = HAD_URL | HAD_METADATA | HAD_LINKDATA | HAD_INVERSE_LINKDATA | HAD_PR;
    static final int MASK_HAD_LINKDATA_AND_INVERSE_LINKDATA = HAD_LINKDATA | HAD_INVERSE_LINKDATA;
    static final int MASK_HAD_PR_AND_LINKDATA_AND_INVERSE_LINKDATA = HAD_PR | HAD_LINKDATA | HAD_INVERSE_LINKDATA;

    @Override
    public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<NullWritable, NullWritable> output,
        Reporter reporter) throws IOException {

      // collect all incoming paths first
      Vector<Path> incomingPaths = new Vector<Path>();

      while (values.hasNext()) {

        String path = values.next().toString();
        LOG.info("Found Incoming Path:" + path);
        incomingPaths.add(new Path(path));
      }

      // set up merge attributes
      JobConf localMergeConfig = new JobConf(_conf);

      localMergeConfig.setClass(MultiFileInputReader.MULTIFILE_COMPARATOR_CLASS, URLFPV2RawComparator.class,
          RawComparator.class);
      localMergeConfig.setClass(MultiFileInputReader.MULTIFILE_KEY_CLASS, URLFPV2.class, WritableComparable.class);

      // ok now spawn merger
      MultiFileInputReader<URLFPV2> multiFileInputReader = new MultiFileInputReader<URLFPV2>(_fs, incomingPaths,
          localMergeConfig);

      // now read one set of values at a time and output result
      KeyAndValueData<URLFPV2> keyValueData = null;

      DataOutputBuffer builderOutputBuffer = new DataOutputBuffer();

      // create output paths ...
      // Path outputDataFile = new
      // Path(FileOutputFormat.getWorkOutputPath(_conf),"part-" +
      // NUMBER_FORMAT.format(key.get()));
      Path outputDataFile = new Path(FileOutputFormat.getWorkOutputPath(_conf), "part-"
          + NUMBER_FORMAT.format(key.get()) + ".data");
      Path outputIndexFile = new Path(FileOutputFormat.getWorkOutputPath(_conf), "part-"
          + NUMBER_FORMAT.format(key.get()) + ".index");

      Path domainMetadataIndexFile = new Path(FileOutputFormat.getWorkOutputPath(_conf), "part-"
          + NUMBER_FORMAT.format(key.get()) + ".domainMetadata");

      LOG.info("Creating TFile Index at:" + outputDataFile);
      LOG.info("Creating DomainMetadata File at:" + domainMetadataIndexFile);

      // create output streams. ...
      // FSDataOutputStream dataStream = _fs.create(outputDataFile);
      FSDataOutputStream dataStream = _fs.create(outputDataFile);
      FSDataOutputStream indexStream = _fs.create(outputIndexFile);

      try {
        // and create tfile writer
        // TFile.Writer indexWriter = new TFile.Writer(dataStream,64 *
        // 1024,TFile.COMPRESSION_LZO,TFile.COMPARATOR_JCLASS +
        // TFileBugWorkaroundDomainHashAndURLHashComparator.class.getName(),
        // _conf);
        CompressURLListV2.Builder builder = new CompressURLListV2.Builder(indexStream, dataStream);

        try {
          // sub domain metadata writer
          SequenceFile.Writer domainMetadataWriter = SequenceFile.createWriter(_fs, _conf, domainMetadataIndexFile,
              LongWritable.class, SubDomainMetadata.class);

          try {

            DataOutputBuffer finalOutputBuffer = new DataOutputBuffer();
            DataInputBuffer inputBuffer = new DataInputBuffer();
            TriTextBytesTuple tupleOut = new TriTextBytesTuple();
            DataOutputBuffer fastLookupBuffer = new DataOutputBuffer();
            SubDomainMetadata metadata = null;
            // LongTextBytesTuple urlTuple = new LongTextBytesTuple();

            DataOutputBuffer datumStream = new DataOutputBuffer();
            DataOutputBuffer keyBuffer = new DataOutputBuffer();
            TextBytes textValueBytes = new TextBytes();

            // start reading merged values ...
            while ((keyValueData = multiFileInputReader.readNextItem()) != null) {

              // ok metadata we are going to write into
              CrawlDatumAndMetadata datumAndMetadata = new CrawlDatumAndMetadata();
              TextBytes urlBytes = null;
              Vector<ArchiveInfo> s3Items = new Vector<ArchiveInfo>();
              boolean dirty = false;

              int mask = 0;

              metadata = createOrFlushSubDomainMetadata(keyValueData._keyObject, metadata, domainMetadataWriter);

              // increment url count
              metadata.setUrlCount(metadata.getUrlCount() + 1);

              // walk values ...
              for (RawRecordValue value : keyValueData._values) {

                String path = value.source.toString();
                inputBuffer.reset(value.data.getData(), value.data.getLength());

                if (path.contains(S3_METADATA_PATTERN)) {
                  ArchiveInfo s3ArchiveInfo = new ArchiveInfo();
                  s3ArchiveInfo.readFields(inputBuffer);
                  s3Items.add(s3ArchiveInfo);
                  reporter.incrCounter(Counters.FOUND_S3_ARCHIVEINFO, 1);
                } else if (path.contains(URLDB_PATTERN)) {

                  mask |= HAD_METADATA;

                  datumAndMetadata.readFields(inputBuffer);

                  urlBytes = datumAndMetadata.getUrlAsTextBytes();

                  if (urlBytes != null && urlBytes.getLength() != 0) {

                    mask |= HAD_URL;

                    if (!metadata.isFieldDirty(SubDomainMetadata.Field_DOMAINTEXT)) {
                      String url = urlBytes.toString();
                      String domain = URLUtils.fastGetHostFromURL(url);
                      if (domain != null && domain.length() != 0) {
                        metadata.setDomainText(domain);
                      }
                    }

                    // update subdomain metadata
                    switch (datumAndMetadata.getStatus()) {
                      case CrawlDatum.STATUS_DB_UNFETCHED:
                        metadata.setUnfetchedCount(metadata.getUnfetchedCount() + 1);
                        break;
                      case CrawlDatum.STATUS_DB_FETCHED:
                        metadata.setFetchedCount(metadata.getFetchedCount() + 1);
                        break;
                      case CrawlDatum.STATUS_DB_GONE:
                        metadata.setGoneCount(metadata.getGoneCount() + 1);
                        break;
                      case CrawlDatum.STATUS_DB_REDIR_TEMP:
                        metadata.setRedirectTemporaryCount(metadata.getRedirectTemporaryCount() + 1);
                        break;
                      case CrawlDatum.STATUS_DB_REDIR_PERM:
                        metadata.setRedirectPermCount(metadata.getRedirectPermCount() + 1);
                        break;
                      case CrawlDatum.STATUS_DB_NOTMODIFIED:
                        metadata.setUnmodifiedCount(metadata.getUnmodifiedCount() + 1);
                        break;
                    }

                    // update fetch time stats
                    metadata.setLatestFetchTime(Math
                        .max(metadata.getLatestFetchTime(), datumAndMetadata.getFetchTime()));

                    CrawlURLMetadata metadataObj = (CrawlURLMetadata) datumAndMetadata.getMetadata();

                    // clear some invalid fields ...
                    metadataObj.setFieldClean(CrawlURLMetadata.Field_SIGNATURE);
                    metadataObj.setFieldClean(CrawlURLMetadata.Field_HOSTFP);
                    metadataObj.setFieldClean(CrawlURLMetadata.Field_URLFP);
                    metadataObj.setFieldClean(CrawlURLMetadata.Field_CONTENTFILESEGNO);
                    metadataObj.setFieldClean(CrawlURLMetadata.Field_CONTENTFILENAMEANDPOS);
                    metadataObj.setFieldClean(CrawlURLMetadata.Field_PARSEDATASEGNO);
                    metadataObj.setFieldClean(CrawlURLMetadata.Field_CRAWLNUMBER);
                    metadataObj.setFieldClean(CrawlURLMetadata.Field_PARSENUMBER);
                    metadataObj.setFieldClean(CrawlURLMetadata.Field_UPLOADNUMBER);
                    metadataObj.setFieldClean(CrawlURLMetadata.Field_ARCFILEDATE);
                    metadataObj.setFieldClean(CrawlURLMetadata.Field_ARCFILEINDEX);
                    metadataObj.setFieldClean(CrawlURLMetadata.Field_ARCFILENAME);
                    metadataObj.setFieldClean(CrawlURLMetadata.Field_ARCFILEOFFSET);
                    metadataObj.setFieldClean(CrawlURLMetadata.Field_ARCFILESIZE);
                    metadataObj.setFieldClean(CrawlURLMetadata.Field_LINKDBTIMESTAMP);
                    metadataObj.setFieldClean(CrawlURLMetadata.Field_INVERSEDBTIMESTAMP);
                    metadataObj.setFieldClean(CrawlURLMetadata.Field_INVERSEDBEXTRADOMAININLINKCOUNT);
                    metadataObj.setFieldClean(CrawlURLMetadata.Field_INVERSEDBINTRADOMAININLINKCOUNT);
                    metadataObj.setFieldClean(CrawlURLMetadata.Field_PAGERANKTIMESTAMP);
                    metadataObj.setFieldClean(CrawlURLMetadata.Field_PAGERANKVALUEOLD);
                    metadataObj.getParseSegmentInfo().clear();
                    // clear url field as we store it in a separate location ...
                    metadataObj.setFieldClean(CrawlDatumAndMetadata.Field_URL);

                    // update sub domain stats ...
                    if (metadataObj.getArchiveInfo().size() != 0) {
                      metadata.setHasArcFileInfoCount(metadata.getHasArcFileInfoCount() + 1);
                    }
                    if (metadataObj.getParseSegmentInfo().size() != 0) {
                      metadata.setHasParseSegmentInfoCount(metadata.getHasParseSegmentInfoCount() + 1);
                    }
                    if (metadataObj.isFieldDirty(CrawlURLMetadata.Field_PAGERANK)) {
                      reporter.incrCounter(Counters.HAD_PAGERANK, 1);
                      metadata.setHasPageRankCount(metadata.getHasPageRankCount() + 1);
                    }
                    if (metadataObj.isFieldDirty(CrawlURLMetadata.Field_LINKDBFILENO)) {
                      reporter.incrCounter(Counters.HAD_LINKDB_DATA, 1);
                      metadata.setHasLinkListCount(metadata.getHasLinkListCount() + 1);
                    }
                    if (metadataObj.isFieldDirty(CrawlURLMetadata.Field_INVERSEDBFILENO)) {
                      reporter.incrCounter(Counters.HAD_INV_LINKDB_DATA, 1);
                      metadata.setHasInverseLinkListCount(metadata.getHasInverseLinkListCount() + 1);
                    }

                    dirty = true;
                  } else {
                    reporter.incrCounter(Counters.URLDB_RECORD_WITHOUT_URL, 1);
                  }
                }
              }

              // URL IS VALID
              if (((mask & HAD_URL) != 0)) {

                reporter.incrCounter(Counters.HAD_URL, 1);
                // add any s3 archive information
                datumAndMetadata.getMetadata().getArchiveInfo().addAll(s3Items);

                // ok only keep last value archive info
                ArchiveInfo lastValidArchiveInfo = null;
                int archiveInfoCount = 0;
                for (ArchiveInfo archiveInfo : datumAndMetadata.getMetadata().getArchiveInfo()) {
                  if (lastValidArchiveInfo == null
                      || lastValidArchiveInfo.getArcfileDate() < archiveInfo.getArcfileDate()) {
                    lastValidArchiveInfo = archiveInfo;
                  }
                  ++archiveInfoCount;
                }

                if (lastValidArchiveInfo != null) {
                  // clear archive info
                  datumAndMetadata.getMetadata().getArchiveInfo().clear();
                  datumAndMetadata.getMetadata().getArchiveInfo().add(lastValidArchiveInfo);

                  if (lastValidArchiveInfo.getCrawlNumber() == 1) {
                    reporter.incrCounter(Counters.FOUND_CRAWLONE_ARCHIVE_INFO, 1);
                  } else if (lastValidArchiveInfo.getCrawlNumber() == CrawlEnvironment.getCurrentCrawlNumber()) {
                    reporter.incrCounter(Counters.FOUND_CURRENTCRAWL_ARCHIVE_INFO, 1);
                  } else {
                    reporter.incrCounter(Counters.FOUND_UNKNOWNCRAWL_ARCHIVE_INFO, 1);
                  }
                }

                // ok initialize tuple .. first value is url ...
                tupleOut.setFirstValue(new TextBytes(urlBytes));

                if (dirty) {
                  datumAndMetadata.setIsValid((byte) 1);
                  // second value is special
                  fastLookupBuffer.reset();
                  // write page rank value
                  fastLookupBuffer.writeFloat(datumAndMetadata.getMetadata().getPageRank());
                  // write fetch status
                  fastLookupBuffer.writeByte(datumAndMetadata.getStatus());
                  // protocol status
                  fastLookupBuffer.writeByte(datumAndMetadata.getProtocolStatus());
                  // write fetch time
                  fastLookupBuffer.writeLong(datumAndMetadata.getFetchTime());
                  // ok write this buffer into second tuple value
                  tupleOut.getSecondValue().set(fastLookupBuffer.getData(), 0, fastLookupBuffer.getLength());
                  // ok write out datum and metadata to stream
                  datumStream.reset();
                  datumAndMetadata.write(datumStream);
                  // set third value in output tuple
                  tupleOut.getThirdValue().set(datumStream.getData(), 0, datumStream.getLength());
                } else {
                  tupleOut.getSecondValue().clear();
                  tupleOut.getThirdValue().clear();
                }

                // reset composite buffer
                finalOutputBuffer.reset();
                // write tuple into it -- TODO: DOUBLE BUFFER COPIES SUCK!!!
                tupleOut.write(finalOutputBuffer);
                // write out key value
                keyBuffer.reset();
                keyBuffer.writeLong(keyValueData._keyObject.getDomainHash());
                keyBuffer.writeLong(keyValueData._keyObject.getUrlHash());

                textValueBytes.set(finalOutputBuffer.getData(), 0, finalOutputBuffer.getLength());
                // output final value to index builder ...
                /*
                 * indexWriter.append( keyBuffer.getData(), 0,
                 * keyBuffer.getLength(), finalOutputBuffer.getData(), 0,
                 * finalOutputBuffer.getLength());
                 */

                reporter.incrCounter(Counters.WROTE_SINGLE_RECORD, 1);
                builder.addItem(keyValueData._keyObject, textValueBytes);

                // update stats
                if (mask == HAD_URL) {
                  reporter.incrCounter(Counters.HAD_ONLY_URL, 1);
                } else if (mask == HAD_METADATA) {
                  reporter.incrCounter(Counters.HAD_ONLY_METADATA, 1);
                } else if (mask == HAD_PR) {
                  reporter.incrCounter(Counters.HAD_ONLY_PR, 1);
                } else if (mask == HAD_LINKDATA) {
                  reporter.incrCounter(Counters.HAD_ONLY_LINKDATA, 1);
                } else if (mask == HAD_INVERSE_LINKDATA) {
                  reporter.incrCounter(Counters.HAD_ONLY_INVERSELINKDATA, 1);
                } else if (mask == MASK_HAD_METADATA_AND_PR) {
                  reporter.incrCounter(Counters.HAD_METADATA_AND_PR, 1);
                } else if (mask == MASK_HAD_METADATA_AND_LINKDATA) {
                  reporter.incrCounter(Counters.HAD_METADATA_AND_LINKDATA, 1);
                } else if (mask == MASK_HAD_METADATA_AND_INVERSE_LINKDATA) {
                  reporter.incrCounter(Counters.HAD_METADATA_AND_INVERSELINKDATA, 1);
                } else if (mask == MASK_HAD_METADATA_AND_ALL_LINKDATA) {
                  reporter.incrCounter(Counters.HAD_METADATA_AND_ALL_LINKDATA, 1);
                } else if (mask == MASK_HAD_METADATA_PR_AND_LINKDATA) {
                  reporter.incrCounter(Counters.HAD_METADATA_PR_AND_LINKDATA, 1);
                } else if (mask == MASK_HAD_METADATA_PR_AND_INVERSE_LINKDATA) {
                  reporter.incrCounter(Counters.HAD_METADATA_PR_AND_INVERSE_LINKDATA, 1);
                } else if (mask == MASK_HAD_ALL_DATA) {
                  reporter.incrCounter(Counters.HAD_ALL_DATA, 1);
                } else if (mask == MASK_HAD_LINKDATA_AND_INVERSE_LINKDATA) {
                  reporter.incrCounter(Counters.HAD_LINKDATA_AND_INVERSE_LINKDATA, 1);
                } else if (mask == MASK_HAD_PR_AND_LINKDATA_AND_INVERSE_LINKDATA) {
                  reporter.incrCounter(Counters.HAD_PR_LINKDATA_AND_INVERSE_LINKDATA, 1);
                }
              }

              // report progress to keep reducer alive
              reporter.progress();
            }
            // flush trailing domain metadata entry ...
            if (metadata != null) {
              domainMetadataWriter.append(new LongWritable(metadata.getDomainHash()), metadata);
            }
          } finally {
            domainMetadataWriter.close();
          }
        } finally {
          // indexWriter.close();
          builder.close();
        }
      } finally {
        dataStream.close();
        indexStream.close();
      }
    }

    @Override
    public void configure(JobConf job) {
      _conf = job;
      try {
        _fs = FileSystem.get(_conf);
        _rootTimestamp = job.getLong("root.timestamp", -1);
      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
      }

    }

    @Override
    public void close() throws IOException {
      // TODO Auto-generated method stub

    }

    private static SubDomainMetadata createOrFlushSubDomainMetadata(URLFPV2 currentFP, SubDomainMetadata metadata,
        SequenceFile.Writer writer) throws IOException {
      if (metadata != null) {
        // if current fingerprint is for a different subdomain ..
        if (currentFP.getDomainHash() != metadata.getDomainHash()) {
          // flush the existing record
          writer.append(new LongWritable(metadata.getDomainHash()), metadata);
          // null out metadata
          metadata = null;
        }
      }
      // if metadata is null ...
      if (metadata == null) {
        // allocate a fresh new record for this new subdomain
        metadata = new SubDomainMetadata();
        metadata.setDomainHash(currentFP.getDomainHash());
        metadata.setRootDomainHash(currentFP.getRootDomainHash());
      }
      return metadata;
    }

  }

  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();

  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }

  public static class TFileHolder {
    public FSDataInputStream _inputStream;
    public TFile.Reader _reader;
    public TFile.Reader.Scanner _scanner;
  }

  public static final int OUTER_ITERATION_COUNT = 2;

  private static void doRegExTest(int driveCount, long candidateTS, Configuration conf, FileSystem fs,
      String queryString) throws IOException {
    FileSystem remoteFS = FileSystem.get(conf);

    Pattern pattern = Pattern.compile(queryString);

    SubDomainMetadata metadata = new SubDomainMetadata();

    for (int i = 0; i < 2; ++i) {
      int disk = i % driveCount;

      Path hdfsPath = new Path("crawl/metadatadb/" + candidateTS + "/subDomainMetadata/tfile/part-"
          + NUMBER_FORMAT.format(i));
      File filePath = new File("/data/" + disk + "/subDomainMetadata/" + candidateTS + "/part-"
          + NUMBER_FORMAT.format(i));
      for (int attempt = 0; attempt < 2; ++attempt) {

        FSDataInputStream inputStream = null;
        long length = 0;
        if (attempt == 0) {
          LOG.info("Scanning:" + filePath);
          byte[] dataBuffer = FileUtils.readFully(filePath);
          ByteBuffer wrapped = ByteBuffer.wrap(dataBuffer);
          inputStream = new FSDataInputStream(new FSByteBufferInputStream(wrapped));
          length = filePath.length();
          LOG.info("Using ByteBufferInputStream");
        } else {
          LOG.info("Scanning:" + hdfsPath);
          inputStream = remoteFS.open(hdfsPath);
          FileStatus status = remoteFS.getFileStatus(hdfsPath);
          if (status != null) 
            length = status.getLen();
          LOG.info("Using HDFS Stream");
        }

        long timeStart = System.currentTimeMillis();
        // FSDataInputStream inputStream = fs.open(new
        // Path(filePath.getAbsolutePath()));
        try {
          TFile.Reader reader = new TFile.Reader(inputStream, length, conf);
          try {
            TFile.Reader.Scanner scanner = reader.createScanner();

            try {
              while (!scanner.atEnd()) {
                DataInputStream stream = scanner.entry().getValueStream();
                metadata.readFields(stream);
                String domainName = metadata.getDomainText();
                if (pattern.matcher(domainName).matches()) {
                  LOG.info("Matched:" + domainName);
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
        long timeEnd = System.currentTimeMillis();
        LOG.info("Scan of File:" + filePath + " took:" + (timeEnd - timeStart) + " MS");
      }

    }

  }
}
