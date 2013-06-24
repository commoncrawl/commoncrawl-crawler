package org.commoncrawl.mapred.ec2.postprocess.crawldb;

import java.io.IOException;
import java.net.URI;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.KeyBasedSequenceFileIndex;
import org.commoncrawl.util.MultiFileMergeUtils;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader.KeyAndValueData;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader.RawRecordValue;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.Tuples.Pair;

@SuppressWarnings("static-access")
public class CrawlDBIndexWriter {

  static final Log LOG = LogFactory.getLog(CrawlDBIndexWriter.class);
  
  static Options options = new Options();
  
  static { 
    options.addOption(OptionBuilder.withArgName("input").hasArg(true).isRequired().withDescription("Input Path").create("input"));
    options.addOption(OptionBuilder.withArgName("output").hasArg(true).isRequired().withDescription("Output Path").create("output"));
    options.addOption(OptionBuilder.withArgName("shards").hasArg(true).withDescription("Shard Count (test only)").create("shards"));
  }
  
  public static void main(String[] args)throws Exception {
    CommandLineParser parser = new GnuParser();
    
    try { 
      // parse the command line arguments
      CommandLine cmdLine = parser.parse( options, args );
      
      // build the index... 
      buildIndex(cmdLine);
    }
    catch (Exception e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp( "CrawlDBIndexWriter", options );
      throw e;
    }
 
  }

  static void buildIndex(CommandLine commandLine)throws IOException { 
    
    Path inputPath = new Path(commandLine.getOptionValue("input"));
    Path outputPath = new Path(commandLine.getOptionValue("output"));
    
    int shardCount = CrawlDBCommon.NUM_SHARDS;
    if (commandLine.hasOption("shards")) { 
      shardCount = Integer.parseInt(commandLine.getOptionValue("shards"));
    }
    
    Configuration conf = new Configuration();
    
    // construct input paths ...  
    ArrayList<Path> inputPaths = new ArrayList<Path>();
    
    inputPaths.add(inputPath);
    
    JobConf jobConf = new JobBuilder("Index Builder", conf)
    .inputs(inputPaths)
    .inputFormat(MultiFileMergeUtils.MultiFileMergeInputFormat.class)
    .mapperKeyValue(IntWritable.class, Text.class)
    .outputKeyValue(TextBytes.class, TextBytes.class)
    .outputFormat(NullOutputFormat.class)
    .reducer(CrawlDBIndexWriterReducer.class,false)
    .partition(MultiFileMergeUtils.MultiFileMergePartitioner.class)
    .numReducers(shardCount)
    .speculativeExecution(true)
    .output(outputPath)
    .compressMapOutput(true)
    .compressor(CompressionType.BLOCK, GzipCodec.class)
    .maxMapAttempts(10)
    .maxReduceAttempts(3)
    .maxReduceTaskFailures(5)
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
  
  public static class CrawlDBIndexWriterReducer implements Reducer<IntWritable, Text ,TextBytes,TextBytes> {

    JobConf _conf;

    @Override
    public void configure(JobConf job) {
      LOG.info("Configuring");
      _conf = job;
    }

    @Override
    public void close() throws IOException {
      // TODO Auto-generated method stub
      
    }

    
    private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
    static {
      NUMBER_FORMAT.setMinimumIntegerDigits(5);
      NUMBER_FORMAT.setGroupingUsed(false);
    }    

    @Override
    public void reduce(IntWritable key, Iterator<Text> values,
        OutputCollector<TextBytes, TextBytes> output, Reporter reporter)
        throws IOException {
      
      LOG.info("Shard:" + key.get());
      // collect all incoming paths first
      Vector<Path> incomingPaths = new Vector<Path>();
      
      Set<String> fsType = new HashSet<String>();
      
      while(values.hasNext()){ 
        String path = values.next().toString();
        LOG.info("Found Incoming Path:" + path);
        incomingPaths.add(new Path(path));
        // convert to uri ... 
        URI uri = new Path(path).toUri();
        // get scheme if present ... 
        String scheme = uri.getScheme();
        if (scheme == null || scheme.length() == 0) { 
          fsType.add("default");
        }
        else { 
          fsType.add(scheme);
        }
      }
      
      if (fsType.size() != 1) { 
        throw new IOException("Only One Input Scheme at a time supported!");
      }
      
      // pick filesystem based on path ... 
      FileSystem fs = FileSystem.get(incomingPaths.get(0).toUri(),_conf);

      LOG.info("FileSystem is:" + fs.toString());
      // create output path... 
      Path indexFilePath = new Path(FileOutputFormat.getWorkOutputPath(_conf),"part-" + NUMBER_FORMAT.format(key.get()));
      
      // create crawl db writer, which is the actual reducer we want to use ...  
      KeyBasedSequenceFileIndex.IndexWriter<TextBytes,TextBytes> indexWriter = new KeyBasedSequenceFileIndex.IndexWriter<TextBytes,TextBytes>(_conf, indexFilePath);

      try { 
        Pair<KeyAndValueData<TextBytes>,Iterable<RawRecordValue>> nextItem = null;
        
        // read the single sharded file ... 
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, incomingPaths.get(0), _conf);
        
        try { 
          DataOutputBuffer keyBuffer = new DataOutputBuffer();
          // walk tuples and feed them to the actual reducer ...  
          long preReadPos = reader.getPosition();
          while (reader.nextRawKey(keyBuffer) != -1) {
            long postReadPos = reader.getPosition();
            if (postReadPos != preReadPos) { 
              indexWriter.indexItem(keyBuffer.getData(),0,keyBuffer.getLength(),null,0,0,preReadPos);
            }
            preReadPos = postReadPos;
            
            reporter.progress();
            keyBuffer.reset();
          }
          
        }
        finally { 
          reader.close();
        }
      }
      finally { 
        indexWriter.close();
      }
    }
    
  }

  
}
