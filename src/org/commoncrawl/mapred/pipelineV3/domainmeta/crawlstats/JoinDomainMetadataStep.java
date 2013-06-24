package org.commoncrawl.mapred.pipelineV3.domainmeta.crawlstats;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.hadoop.mergeutils.SequenceFileSpillWriter;
import org.commoncrawl.mapred.ec2.postprocess.crawldb.CrawlDBCommon;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.rank.GenDomainRankStep;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.JoinMapper;
import org.commoncrawl.util.JoinValue;
import org.commoncrawl.util.KeyBasedSequenceFileIndex;
import org.commoncrawl.util.TextBytes;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class JoinDomainMetadataStep extends CrawlPipelineStep {

  private static final Log LOG = LogFactory.getLog(JoinDomainMetadataStep.class);
  
  public static final String OUTPUT_DIR_NAME = "DomainMetadataJoined";
  
  public JoinDomainMetadataStep(CrawlPipelineTask task) {
    super(task, "Join Metadata Step", OUTPUT_DIR_NAME);
  }
  
  @Override
  public Log getLogger() {
    return LOG;
  }

  public static String TAG_RANKDATA = "1";
  public static String TAG_BLEKKODATA = "2";
  public static String TAG_CRAWLSTATS = "3";
  
  @Override
  public void runStep(Path outputPathLocation) throws IOException {

    ImmutableMap<Path, String> joinSourcesMap 
      = new ImmutableMap.Builder<Path, String>()
      .put(getOutputDirForStep(GenDomainRankStep.class), TAG_RANKDATA)
      .put(getOutputDirForStep(MergeNewDomainStatsStep.class), TAG_CRAWLSTATS)
      .put(new Path(CrawlDBCommon.BLEKKO_DOMAIN_METADATA_PATH), TAG_BLEKKODATA)
      .build();
    
    JobConf job = new JobBuilder("Final Domain Metadata Join Step", getConf())
    .inputIsSeqFile()
    .inputs(ImmutableList.copyOf(joinSourcesMap.keySet()))
    .mapper(JoinMapper.class)
    .mapperKeyValue(TextBytes.class, JoinValue.class)
    .outputKeyValue(TextBytes.class, TextBytes.class)
    .reducer(JoinMetadataReducer.class, false)
    .numReducers(CrawlEnvironment.NUM_DB_SHARDS)
    .outputFormat(NullOutputFormat.class)
    .output(outputPathLocation)
    .compressor(CompressionType.BLOCK, SnappyCodec.class)
    .speculativeReducerExecution(false)
    .build();
    
    JoinMapper.setPathToTagMapping(joinSourcesMap, job);
    
    
    JobClient.runJob(job);
    
  }
  
  public static class JoinMetadataReducer implements Reducer<TextBytes,JoinValue,TextBytes,TextBytes> {

    private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
    static {
      NUMBER_FORMAT.setMinimumIntegerDigits(5);
      NUMBER_FORMAT.setGroupingUsed(false);
    }    
    
    SequenceFileSpillWriter<TextBytes, TextBytes> _spillWriter;
    
    @Override
    public void configure(JobConf job) {
      // get the partition number for this reducer 
      int partitionNumber = job.getInt("mapred.task.partition", -1);

      // figure out output paths ... 
      // WORK OUTPUTPATH doesn't really work properly in EMR ??? 
      Path outputPath = new Path(FileOutputFormat.getOutputPath(job),"part-" + NUMBER_FORMAT.format(partitionNumber));
      Path indexOutputPath = new Path(FileOutputFormat.getOutputPath(job),"index-" + NUMBER_FORMAT.format(partitionNumber));
      
      
      try {
        // hence the need to delete and write directly to output dir ...
        FileSystem fs = FileSystem.get(outputPath.toUri(), job);
        if (fs.exists(outputPath)) 
          fs.delete(outputPath, false);
        if (fs.exists(indexOutputPath)) 
          fs.delete(indexOutputPath, false);

        
        // create index writer ... 
        KeyBasedSequenceFileIndex.IndexWriter<TextBytes,TextBytes> indexWriter = new KeyBasedSequenceFileIndex.IndexWriter<TextBytes,TextBytes>(job, indexOutputPath);
  
        // splill writer ... 
        _spillWriter 
          = new SequenceFileSpillWriter<TextBytes, TextBytes>(
            FileSystem.get(outputPath.toUri(),job),
            job, 
                outputPath, 
                TextBytes.class, 
                TextBytes.class, 
                indexWriter, true);
      }
      catch (IOException e) { 
        LOG.error(CCStringUtils.stringifyException(e));
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() throws IOException {
      if (_spillWriter != null) { 
        IOUtils.closeStream(_spillWriter);
      }
    }

    JsonParser parser = new JsonParser();
    
    enum Counters { 
      GOT_, JOINED_TUPLE_HAD_ONE_ELEMENT, JOINED_TUPLE_HAD_TWO_ELEMENTS, JOINED_TUPLE_HAD_THREE_ELEMENTS
    }
    @Override
    public void reduce(TextBytes key, Iterator<JoinValue> values,OutputCollector<TextBytes, TextBytes> output, Reporter reporter)throws IOException {
      JsonObject topLevelObject = new JsonObject();
      while (values.hasNext()) {
        JoinValue value = values.next();
        
        if (value.getTag().equals(TAG_RANKDATA)) { 
          topLevelObject.addProperty(CrawlStatsCommon.JOINEDMETDATA_PROPERTY_RANK,value.getDoubleValue());
        }
        else if (value.getTag().equals(TAG_BLEKKODATA)) { 
          topLevelObject.add(CrawlStatsCommon.JOINEDMETDATA_PROPERTY_BELKKO,parser.parse(value.getTextValue().toString()).getAsJsonObject());
        }
        else if (value.getTag().equals(TAG_CRAWLSTATS)) { 
          topLevelObject.add(CrawlStatsCommon.JOINEDMETDATA_PROPERTY_CRAWLSTATS,parser.parse(value.getTextValue().toString()).getAsJsonObject());
        }
        else { 
          throw new IOException("Unknown JoinValue. Tag:" + value.getTag() + " Type:" + value.getType());
        }
        
        value.getTextValue().clear();
      }
      
      if (topLevelObject.entrySet().size() == 1) { 
        reporter.incrCounter(Counters.JOINED_TUPLE_HAD_ONE_ELEMENT, 1);
      }
      else if (topLevelObject.entrySet().size() == 2) { 
        reporter.incrCounter(Counters.JOINED_TUPLE_HAD_TWO_ELEMENTS, 1);
      }
      else if (topLevelObject.entrySet().size() == 3) { 
        reporter.incrCounter(Counters.JOINED_TUPLE_HAD_THREE_ELEMENTS, 1);
      }
      _spillWriter.spillRecord(key, new TextBytes(topLevelObject.toString()));
    } 
  }

}
