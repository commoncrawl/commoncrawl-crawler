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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.TextBytes;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonObject;

public class PrepareBlekkoDomainMetadata {

  static final Log LOG = LogFactory.getLog(CrawlDBIndexWriter.class);
  
  static Options options = new Options();
  
  static { 
    options.addOption(OptionBuilder.withArgName("input").hasArg(true).isRequired().withDescription("Input Path").create("input"));
  }
  
  
  
  public static void main(String[] args)throws Exception {
    CommandLineParser parser = new GnuParser();
    
    try { 
      // parse the command line arguments
      CommandLine cmdLine = parser.parse( options, args );
      
      // build the index... 
      importRawDomainMetadta(cmdLine);
    }
    catch (Exception e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp( "PrepareBlekkoDomainMetadata", options );
      throw e;
    }
  }
  
  static void importRawDomainMetadta(CommandLine commandLine) throws IOException { 
    Path inputPath = new Path(commandLine.getOptionValue("input"));
    
    Configuration conf = new Configuration();
    
    FileSystem fs = FileSystem.get(inputPath.toUri(),conf);
    
    ArrayList<Path> paths = new ArrayList<Path>();
    FileStatus[] files = fs.globStatus(new Path(inputPath,"*.seq"));
    for (FileStatus file : files)
      paths.add(file.getPath());
        
    JobConf job = new JobBuilder("Import Blekko Metadata", new Configuration())
      .keyValue(TextBytes.class, TextBytes.class)
      .mapper(MapBelkkoMetadata.class)
      .numReducers(CrawlEnvironment.NUM_DB_SHARDS/2)
      .inputs(paths)
      .output(new Path(CrawlDBCommon.BLEKKO_DOMAIN_METADATA_PATH))
      .inputIsSeqFile()
      .outputIsSeqFile()
      .compressor(CompressionType.BLOCK,  SnappyCodec.class)
      .build();
    JobClient.runJob(job);
  }

  
  
  
  public static class MapBelkkoMetadata implements Mapper<Text,Text,TextBytes,TextBytes> {

    @Override
    public void configure(JobConf job) {
      
    }

    @Override
    public void close() throws IOException {
    }

    enum Counters { 
      HAD_WWW_PREFIX, HIT_EXCEPTION, HAD_RANK10, HAD_IP, HAD_ISPORN, HAD_ISSPAM
    }
    
    static Pattern rank10Extractor = Pattern.compile("rank10=([0-9.]*)");
    static Pattern ipExtractor = Pattern.compile("ip=([0-9.]*)");
    static Pattern isPorn   = Pattern.compile("bool_porn=1");
    static Pattern isSpam   = Pattern.compile("bool_spam=1");
    
    @Override
    public void map(Text key, Text value,OutputCollector<TextBytes, TextBytes> output, Reporter reporter) throws IOException {
      try { 
        String domainName = key.toString();
        boolean hadWWWPrefix = false;
        if (domainName.startsWith("www.")) { 
          domainName = domainName.substring("www.".length());
          hadWWWPrefix = true;
          reporter.incrCounter(Counters.HAD_WWW_PREFIX, 1);
        }
        
        JsonObject metadataOut = new JsonObject();
        
        if (hadWWWPrefix) { 
          metadataOut.addProperty(CrawlDBCommon.BLEKKO_METADATA_WWW_PREFIX, true);
        }
        String metadata = value.toString();
        
        Matcher rank10Matcher = rank10Extractor.matcher(metadata);
        
        if (rank10Matcher.find()) {
          metadataOut.addProperty(CrawlDBCommon.BLEKKO_METADATA_RANK_10, Double.parseDouble(rank10Matcher.group(1)));
          reporter.incrCounter(Counters.HAD_RANK10, 1);
        }
        
        Matcher ipExtractorMatcher = ipExtractor.matcher(metadata);
        if (ipExtractorMatcher.find()) { 
          metadataOut.addProperty(CrawlDBCommon.BLEKKO_METADATA_IP,ipExtractorMatcher.group(1));
          reporter.incrCounter(Counters.HAD_IP, 1);
        }
        if (isPorn.matcher(metadata).find()) { 
          metadataOut.addProperty(CrawlDBCommon.BLEKKO_METADATA_ISPORN,1);
          reporter.incrCounter(Counters.HAD_ISPORN, 1);
        }
        if (isSpam.matcher(metadata).find()) { 
          metadataOut.addProperty(CrawlDBCommon.BLEKKO_METADATA_ISSPAM,1);
          reporter.incrCounter(Counters.HAD_ISSPAM, 1);
        }
        
        output.collect(new TextBytes(domainName),new TextBytes(metadataOut.toString()));
      }
      catch (Exception e) { 
        LOG.error(CCStringUtils.stringifyException(e));
        reporter.incrCounter(Counters.HIT_EXCEPTION, 1);
      }
    } 
    
  }
}
