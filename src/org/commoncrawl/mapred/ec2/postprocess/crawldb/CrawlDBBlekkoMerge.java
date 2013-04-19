package org.commoncrawl.mapred.ec2.postprocess.crawldb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.mapred.ec2.postprocess.crawldb.CrawlDBKey.CrawlDBKeyPartitioner;
import org.commoncrawl.mapred.ec2.postprocess.crawldb.CrawlDBKey.LinkKeyComparator;
import org.commoncrawl.mapred.ec2.postprocess.crawldb.CrawlDBKey.Type;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.MultiFileMergeUtils;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLUtils;
import org.junit.Assert;
import org.junit.Test;

import com.google.gson.JsonObject;

@SuppressWarnings("static-access")
public class CrawlDBBlekkoMerge {
  
  static final Log LOG = LogFactory.getLog(CrawlDBBlekkoMerge.class);
  static final String BLKEKKO_TIMESTAMP_PROPERTY = "blekko.timestamp";
  
  static Options options = new Options();
  
  static { 
    options.addOption(OptionBuilder.withArgName("op").hasArg(true).isRequired().withDescription("Operation (shard/import)").create("op"));
    options.addOption(OptionBuilder.withArgName("input").hasArg(true).isRequired().withDescription("Input Path").create("input"));
    options.addOption(OptionBuilder.withArgName("output").hasArg(true).isRequired().withDescription("Output Path").create("output"));
    options.addOption(OptionBuilder.withArgName("crawldb").hasArg(true).withDescription("CrawlDB Path").create("crawldb"));
    options.addOption(OptionBuilder.withArgName("timestamp").hasArg(true).withDescription("Metadata Timestamp").create("timestamp"));
    options.addOption(OptionBuilder.withArgName("shards").hasArg(true).withDescription("Shard Count (test only)").create("shards"));
  }
  
  public static void main(String[] args) throws Exception {

    CommandLineParser parser = new GnuParser();
    
    try { 
      // parse the command line arguments
      CommandLine cmdLine = parser.parse( options, args );
      
      if (cmdLine.getOptionValue("op").equalsIgnoreCase("shard")) { 
        runShardStep(cmdLine);
      }
      else if (cmdLine.getOptionValue("op").equalsIgnoreCase("import")) { 
        runImportStep(cmdLine);
      }
    }
    catch (Exception e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp( "CrawlDBBlekkoMerge", options );
      
      throw e;
    }
  }
  static Pattern METADATA_PATTERN = Pattern.compile("^rank=([0-9.]*)\\s*rank10=([0-9.]*)[ ]*([^\\s]*)");
  static String  BLEKKO_CRAWL_STATUS_CRAWLED = "crawled";
  static String  BLEKKO_CRAWL_STATUS_REDIRECT = "redir";
  
  static class BlekkoURLMetadataToJSONMapper implements Mapper<Text,Text,TextBytes,TextBytes> {

    enum Counters {
      NULL_FP, BAD_METADATA, EXCEPTION_DURING_PARSE, RANK10_LT_1, RANK10_LT_2, RANK10_LT_3, RANK10_GT_4, RANK10_LT_4, BLEKKO_CRAWLED 
      
    }
    long metadataTimestamp = -1L; 
    @Override
    public void configure(JobConf job) {
      metadataTimestamp = job.getLong(BLKEKKO_TIMESTAMP_PROPERTY, -1);
    }

    @Override
    public void close() throws IOException {
      
    }
    
    

    @Override
    public void map(Text key, Text value,OutputCollector<TextBytes, TextBytes> output, Reporter reporter)throws IOException {
      // map to fingerprint ... 
      URLFPV2 fp = URLUtils.getURLFPV2FromURL(key.toString());
      if (fp != null) { 
        // parse 
        Matcher m = METADATA_PATTERN.matcher(value.toString().trim());
        if (m.matches()) {
           try { 
            float rank10= Float.parseFloat(m.group(2));
            boolean crawled = m.group(3).equalsIgnoreCase("crawled");
            if (rank10>=0.0 && rank10 <1)
              reporter.incrCounter(Counters.RANK10_LT_1, 1);
            else if (rank10>=1.0 && rank10 <2)
              reporter.incrCounter(Counters.RANK10_LT_2, 1);
            else if (rank10>=2.0 && rank10 <3)
              reporter.incrCounter(Counters.RANK10_LT_3, 1);
            else if (rank10>=3.0 && rank10 <4)
              reporter.incrCounter(Counters.RANK10_LT_4, 1);
            else if (rank10>=4.0)
              reporter.incrCounter(Counters.RANK10_GT_4, 1);
            if (crawled) 
              reporter.incrCounter(Counters.BLEKKO_CRAWLED, 1);
            
            
            JsonObject jsonMetadata = new JsonObject();
            jsonMetadata.addProperty(CrawlDBMergingReducer.BLEKKO_METADATA_TIMESTAMP_PROPERTY,metadataTimestamp);
            jsonMetadata.addProperty(CrawlDBMergingReducer.BLEKKO_METADATA_RANK,Float.parseFloat(m.group(1)));
            jsonMetadata.addProperty(CrawlDBMergingReducer.BLEKKO_METADATA_RANK_10,Float.parseFloat(m.group(2)));
            jsonMetadata.addProperty(CrawlDBMergingReducer.BLEKKO_METADATA_STATUS,m.group(3));
            JsonObject topLevelObject = new JsonObject();
            topLevelObject.add(CrawlDBMergingReducer.TOPLEVEL_BLEKKO_METADATA_PROPERTY, jsonMetadata);
            topLevelObject.addProperty(CrawlDBMergingReducer.TOPLEVEL_SOURCE_URL_PROPRETY,key.toString());
  
            // get crawl db key format key 
            TextBytes keyOut = CrawlDBKey.generateKey(fp, Type.KEY_TYPE_MERGED_RECORD, metadataTimestamp);
            // emit
            output.collect(keyOut, new TextBytes(topLevelObject.toString()));
           }
           catch (Exception e) { 
             LOG.error(CCStringUtils.stringifyException(e));
             reporter.incrCounter(Counters.EXCEPTION_DURING_PARSE, 1);
           }
        }
        else { 
          reporter.incrCounter(Counters.BAD_METADATA, 1);
          LOG.info("Bad Metadata:" + value.toString() + " Len:" + value.getLength());
        }
        
      }
      else { 
        reporter.incrCounter(Counters.NULL_FP, 1);
        LOG.info("NULLFP:" + key.toString());
      }
    } 
  }
  
  @Test 
  public void testPattern() {
    String testDatum1 = "rank=0.0 rank10=1.00 crawled";
    String testDatum2 = "rank=2.0 rank10=3.01 redir";
    String testDatum3 = "rank=0.0 rank10=0.00 crawled\n";
    {
      Matcher m = METADATA_PATTERN.matcher(testDatum1);
      Assert.assertTrue(m.matches());
      Assert.assertEquals(m.group(1),"0.0");
      Assert.assertEquals(m.group(2),"1.00");
      Assert.assertEquals(m.group(3),"crawled");
    }

    {
      Matcher m= METADATA_PATTERN.matcher(testDatum2);
      Assert.assertTrue(m.matches());
      Assert.assertEquals(m.group(1),"2.0");
      Assert.assertEquals(m.group(2),"3.01");
      Assert.assertEquals(m.group(3),"redir");
    }

    {
      Matcher m= METADATA_PATTERN.matcher(testDatum3);
      Assert.assertTrue(m.matches());
      Assert.assertEquals(m.group(1),"0.0");
      Assert.assertEquals(m.group(2),"0.00");
      Assert.assertEquals(m.group(3),"crawled");
      Assert.assertEquals(m.groupCount(),3);
    }
    
  }
  
  
  static void runShardStep(CommandLine commandLine)throws IOException {
    
    if (!commandLine.hasOption("timestamp")) { 
      throw new IOException("Required timestamp parameter missing!");
    }
    
    Path inputPath = new Path(commandLine.getOptionValue("input"));
    Path outputPath = new Path(commandLine.getOptionValue("output"));
    
    Configuration conf = new Configuration();
    
    // set the timestamp property in the config ... 
    conf.setLong(BLKEKKO_TIMESTAMP_PROPERTY, Long.parseLong(commandLine.getOptionValue("timestamp")));
    
    
    JobConf jobConf = new JobBuilder("Shard Belkko URL Metadata", conf)
    .input(inputPath)
    .inputFormat(SequenceFileInputFormat.class)
    .mapper(BlekkoURLMetadataToJSONMapper.class)
    .mapperKeyValue(TextBytes.class, TextBytes.class)
    .outputKeyValue(TextBytes.class, TextBytes.class)
    .outputFormat(SequenceFileOutputFormat.class)
    .partition(CrawlDBKeyPartitioner.class)
    .sort(LinkKeyComparator.class)
    .numReducers(CrawlDBCommon.NUM_SHARDS)
    .speculativeExecution(true)
    .output(outputPath)
    .compressMapOutput(true)
    .maxMapAttempts(4)
    .maxReduceAttempts(3)
    .maxMapTaskFailures(1)
    .compressor(CompressionType.BLOCK, SnappyCodec.class)
    .build();    
    
    LOG.info("Starting JOB:" + jobConf);
    try { 
      JobClient.runJob(jobConf);
      LOG.info("Finished JOB:" + jobConf);
    }
    catch (Exception e) { 
      LOG.info("JOB Exec Failed for:" + jobConf);
      LOG.error(CCStringUtils.stringifyException(e));
    }
  }

  static void runImportStep(CommandLine commandLine)throws IOException { 
    
    if (!commandLine.hasOption("crawldb")) { 
      throw new IOException("CrawlDB required parameter missing!");
    }

    Path inputPath = new Path(commandLine.getOptionValue("input"));
    Path outputPath = new Path(commandLine.getOptionValue("output"));
    Path crawldbPath = new Path(commandLine.getOptionValue("crawldb"));
    
    int shardCount = CrawlDBCommon.NUM_SHARDS;
    if (commandLine.hasOption("shards")) { 
      shardCount = Integer.parseInt(commandLine.getOptionValue("shards"));
    }
    
    Configuration conf = new Configuration();
    
    // construct input paths ...  
    ArrayList<Path> inputPaths = new ArrayList<Path>();
    
    inputPaths.add(inputPath);
    inputPaths.add(crawldbPath);
    
    JobConf jobConf = new JobBuilder("Merge Blekko Data", conf)
    .inputs(inputPaths)
    .inputFormat(MultiFileMergeUtils.MultiFileMergeInputFormat.class)
    .mapperKeyValue(IntWritable.class, Text.class)
    .outputKeyValue(TextBytes.class, TextBytes.class)
    .outputFormat(SequenceFileOutputFormat.class)
    .reducer(CrawlDBMergeSortReducer.class,false)
    .partition(MultiFileMergeUtils.MultiFileMergePartitioner.class)
    .numReducers(shardCount)
    .speculativeExecution(true)
    .output(outputPath)
    .compressMapOutput(true)
    .compressor(CompressionType.BLOCK, GzipCodec.class)
    .maxMapAttempts(10)
    .maxReduceAttempts(4)
    .maxMapTaskFailures(1)
    .reuseJVM(1)
    .build();
    
    LOG.info("Starting JOB:" + jobConf);
    try { 
      JobClient.runJob(jobConf);
      LOG.info("Finished JOB:" + jobConf);
    }
    catch (Exception e) { 
      LOG.info("JOB Exec Failed for:" + jobConf);
      LOG.error(CCStringUtils.stringifyException(e));
    }
  }

  
  
}
