package org.commoncrawl.mapred.pipelineV3.domainmeta.crawlstats;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.mapred.ec2.postprocess.crawldb.CrawlDBCommon;
import org.commoncrawl.mapred.ec2.postprocess.crawldb.CrawlDBKey;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.DomainMetadataTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.crawlstats.NewCrawlStatsCollectorStep.CrawlDBStatsCollectingReducer.Counters;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader.KeyAndValueData;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader.RawRecordValue;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileMergeInputFormat;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileMergePartitioner;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.Tuples.Pair;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class TypeAndRelStatsCollectorStep extends CrawlPipelineStep  {

  private static final Log LOG = LogFactory.getLog(TypeAndRelStatsCollectorStep.class);
  
  public static final String OUTPUT_DIR_NAME = "typeAndRelStats";
  
  public TypeAndRelStatsCollectorStep(CrawlPipelineTask task) {
    super(task, "TypeAndRels Stats Collector", OUTPUT_DIR_NAME);
  }
  
  
  @Override
  public Log getLogger() {
    return LOG;
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {

    DomainMetadataTask rootTask = (DomainMetadataTask)getRootTask();
    ImmutableList<Path> paths = new ImmutableList.Builder<Path>().addAll(rootTask.getRestrictedMergeDBDataPaths()).build();

    Path tempPath = JobBuilder.tempDir(getConf(), OUTPUT_DIR_NAME);
    
    JobConf jobConf = new JobBuilder("Collect TypeAndRels", getConf())

    .inputs(paths)
    .inputFormat(MultiFileMergeInputFormat.class)
    .mapperKeyValue(IntWritable.class, Text.class)
    .outputKeyValue(TextBytes.class, IntWritable.class)
    .outputFormat(SequenceFileOutputFormat.class)
    .reducer(TypeAndRelStatsCollector.class, false)
    .partition(MultiFileMergePartitioner.class)
    .numReducers(CrawlEnvironment.NUM_DB_SHARDS)
    .maxReduceAttempts(4)
    .maxReduceTaskFailures(10)
    .speculativeExecution(true)
    .output(tempPath)
    .compressMapOutput(false).compressor(CompressionType.BLOCK, SnappyCodec.class)

    .build();

    LOG.info("Starting JOB");
    JobClient.runJob(jobConf);
    LOG.info("Finsihed JOB");

    Path stage1OutputPath = JobBuilder.tempDir(getConf(), OUTPUT_DIR_NAME);
    
    for (int pass=0;pass<2;++pass) { 
      
      Path passOutputPath = (pass ==0) ? stage1OutputPath : outputPathLocation;
      Path passInputPath  = (pass ==0) ? tempPath : stage1OutputPath;
      
      jobConf = new JobBuilder("Aggregate TypeAndRels", getConf())
  
      .input(passInputPath)
      .inputIsSeqFile()
      .keyValue(TextBytes.class,IntWritable.class)
      .outputFormat((pass ==0) ? SequenceFileOutputFormat.class : TextOutputFormat.class)
      .reducer(DropUniqueEntriesReducer.class, false)
      .numReducers((pass == 0) ? 20 : 1)
      .speculativeExecution(true)
      .output(passOutputPath)
      .compressType(CompressionType.NONE)
  
      .build();
  
      LOG.info("Starting JOB");
      JobClient.runJob(jobConf);
      LOG.info("Finsihed JOB");
    }
    
  }

  public static class DropUniqueEntriesReducer implements Reducer<TextBytes,IntWritable,TextBytes,IntWritable> {

    @Override
    public void configure(JobConf job) {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void close() throws IOException {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void reduce(TextBytes key, Iterator<IntWritable> values,OutputCollector<TextBytes, IntWritable> output, Reporter reporter)
        throws IOException {
      int count = 0;
      while (values.hasNext()) 
        count += values.next().get();
      if (count >= 10) { 
        output.collect(key, new IntWritable(count));
      }
    } 
    
  }
  
  public static class TypeAndRelStatsCollector implements Reducer<IntWritable,Text,TextBytes,IntWritable> {

    JobConf _jobConf;
    @Override
    public void configure(JobConf job) {
      _jobConf = job;
    }

    @Override
    public void close() throws IOException {
      
    }

    @Override
    public void reduce(IntWritable key, Iterator<Text> values,OutputCollector<TextBytes, IntWritable> output, Reporter reporter)throws IOException {
      // collect all incoming paths first
      Vector<Path> incomingPaths = new Vector<Path>();

      while (values.hasNext()) {
        String path = values.next().toString();
        LOG.info("Found Incoming Path:" + path);
        incomingPaths.add(new Path(path));
      }

      // set up merge attributes
      Configuration localMergeConfig = new Configuration(_jobConf);

      localMergeConfig.setClass(MultiFileInputReader.MULTIFILE_COMPARATOR_CLASS, CrawlDBKey.LinkKeyComparator.class,
          RawComparator.class);
      localMergeConfig.setClass(MultiFileInputReader.MULTIFILE_KEY_CLASS, TextBytes.class, WritableComparable.class);

      FileSystem fs = FileSystem.get(incomingPaths.get(0).toUri(),_jobConf);
      
      // ok now spawn merger
      MultiFileInputReader<TextBytes> multiFileInputReader 
        = new MultiFileInputReader<TextBytes>(fs, incomingPaths, localMergeConfig);

      try {
        
        Pair<KeyAndValueData<TextBytes>, Iterable<RawRecordValue>> nextItem = null;
    
        TextBytes valueText = new TextBytes();
        DataInputBuffer valueStream = new DataInputBuffer();
        JsonParser parser = new JsonParser();
        IntWritable one = new IntWritable(1);
        while ((nextItem = multiFileInputReader.getNextItemIterator()) != null) {
        
          long recordType = CrawlDBKey.getLongComponentFromKey(nextItem.e0._keyObject, CrawlDBKey.ComponentId.TYPE_COMPONENT_ID);
          if (recordType == CrawlDBKey.Type.KEY_TYPE_MERGED_RECORD.ordinal()) {
            reporter.incrCounter(Counters.GOT_MERGED_RECORD, 1);
            // walk records
            RawRecordValue rawValue = Iterators.getNext(nextItem.e1.iterator(),null);
            if (rawValue != null) {
              valueStream.reset(rawValue.data.getData(),0,rawValue.data.getLength());
              valueText.setFromRawTextBytes(valueStream);
              try {
                JsonObject mergeRecord = parser.parse(valueText.toString()).getAsJsonObject();
                if (mergeRecord.has(CrawlDBCommon.TOPLEVEL_LINKSTATUS_PROPERTY)) { 
                  JsonObject linkStatus = mergeRecord.getAsJsonObject(CrawlDBCommon.TOPLEVEL_LINKSTATUS_PROPERTY);
                  if (linkStatus.has(CrawlDBCommon.LINKSTATUS_TYPEANDRELS_PROPERTY)) { 
                    JsonArray typeAndRelsArray = linkStatus.getAsJsonArray(CrawlDBCommon.LINKSTATUS_TYPEANDRELS_PROPERTY);
                    for (JsonElement typeAndRel : typeAndRelsArray) {
                      output.collect(new TextBytes(typeAndRel.getAsString()), one);
                    }
                  }
                }
              }
              catch (Exception e) {
                reporter.incrCounter(Counters.HIT_EXCEPTION_PROCESSING_RECORD, 1);
                LOG.error(CCStringUtils.stringifyException(e));
              }
            }
          }
        }
      }
      finally { 
        multiFileInputReader.close();
      }      
    }       
    
    
  }
  
}
