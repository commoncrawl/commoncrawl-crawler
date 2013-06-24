package org.commoncrawl.mapred.ec2.postprocess.crawldb;

import java.io.IOException;
import java.net.URI;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import javax.annotation.Nullable;

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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.commoncrawl.hadoop.mergeutils.MergeSortSpillWriter;
import org.commoncrawl.hadoop.mergeutils.SequenceFileSpillWriter;
import org.commoncrawl.hadoop.util.TextDatumInputSplit;
import org.commoncrawl.mapred.ec2.postprocess.crawldb.CrawlDBMergeSortReducer.RawValueIterator;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.JobBuilder;
import org.commoncrawl.util.KeyBasedSequenceFileIndex;
import org.commoncrawl.util.MultiFileMergeUtils;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader.KeyAndValueData;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader.RawRecordValue;
import org.commoncrawl.util.S3NFileSystem;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.Tuples.Pair;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

/**
 * Although a 10000 shard index (the default when generating the crawldb)
 * is good for merge parallelism, it is unwieldly when trying to run 
 * queries against it. This job shrinks the number of shards down to a more manageabale    
 * level and also builds an index against the resulting database.
 *  
 * @author rana
 *
 */
@SuppressWarnings("static-access")
public class CrawlDBCompactor {

 static final Log LOG = LogFactory.getLog(CrawlDBCompactor.class);
  
  static Options options = new Options();
  
  static { 
    options.addOption(OptionBuilder.withArgName("input").hasArg(true).isRequired().withDescription("Input Path").create("input"));
    options.addOption(OptionBuilder.withArgName("output").hasArg(true).isRequired().withDescription("Output Path").create("output"));
    options.addOption(OptionBuilder.withArgName("shards").hasArg(true).isRequired().withDescription("Desired Output Shard Count").create("shards"));
    options.addOption(OptionBuilder.withArgName("sample").hasArg(true).withDescription("Optional Sample Size").create("sample"));
  }
  
  public static void main(String[] args)throws Exception {
    CommandLineParser parser = new GnuParser();
    
    try { 
      // parse the command line arguments
      CommandLine cmdLine = parser.parse( options, args );
      
      // build the index... 
      compactDB(cmdLine);
    }
    catch (Exception e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp( "CrawlDBCompactor", options );
      throw e;
    }
 
  }
  
  
  
  static void compactDB(CommandLine commandLine)throws IOException { 
    Configuration conf = new Configuration();
    
    Path inputPath = new Path(commandLine.getOptionValue("input"));
    Path outputPath = new Path(commandLine.getOptionValue("output"));
    int  targetShardCount = Integer.parseInt(commandLine.getOptionValue("shards"));
    int  sampleSize = (commandLine.hasOption("sample") ? Integer.parseInt(commandLine.getOptionValue("sample")) : -1);
    
    FileSystem inputFS = FileSystem.get(inputPath.toUri(),conf);
    // collect shards from input 
    FileStatus shards[] = inputFS.globStatus(new Path(inputPath,"part-*"));

    // restrict shard count to sample size if so desired 
    if (sampleSize != -1) { 
      shards = Arrays.copyOfRange(shards, 0, sampleSize);
    }
    
    if (shards.length % targetShardCount != 0) { 
      throw new IOException("input shard count:" + shards.length + " not evenly divisible by target shard count:" + targetShardCount);
    }
    
    // transform to paths 
    Iterator<Path> pathItertor = Iterators.transform(Iterators.forArray(shards), new Function<FileStatus, Path>() {

      @Override
      @Nullable
      public Path apply(@Nullable FileStatus arg0) {
        return arg0.getPath();
      }
    });
    
    // partition ... 
    final List<List<Path>> partitions = Lists.partition(Lists.newArrayList(pathItertor),shards.length / targetShardCount);
    
    // set the partition info into the conf 
    CustomInputFormat.writePartitions(partitions, conf);

    
    // setup job conf 
    JobConf jobConf = new JobBuilder("Index Builder", conf)
    .inputFormat(CustomInputFormat.class)
    .mapperKeyValue(IntWritable.class, Text.class)
    .outputKeyValue(TextBytes.class, TextBytes.class)
    .outputFormat(NullOutputFormat.class)
    .reducer(CrawlDBCompactingReducer.class,false)
    .partition(MultiFileMergeUtils.MultiFileMergePartitioner.class)
    .numReducers(targetShardCount)
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
  
  /** 
   * Merge a bunch of shards into a single output shard
   * Also, create an index of the resulting shard ... 
   * 
   * @author rana
   *
   */
  public static class CrawlDBCompactingReducer implements Reducer<IntWritable,Text,TextBytes,TextBytes> {

    private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
    static {
      NUMBER_FORMAT.setMinimumIntegerDigits(5);
      NUMBER_FORMAT.setGroupingUsed(false);
    }    

    JobConf _conf;

    @Override
    public void configure(JobConf job) {
      LOG.info("Configuring");
      _conf = job;
    }

    @Override
    public void close() throws IOException {
      
    }

    @Override
    public void reduce(IntWritable key, Iterator<Text> values,
        OutputCollector<TextBytes, TextBytes> output, Reporter reporter)
        throws IOException {
      // collect all incoming paths first
      List<Path> incomingPaths = Lists.newArrayList();
      
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
      
      // figure out output path ... 
      Path outputPath = new Path(FileOutputFormat.getWorkOutputPath(_conf),"part-" + NUMBER_FORMAT.format(key.get()));
      Path indexOutputPath = new Path(FileOutputFormat.getWorkOutputPath(_conf),"index-" + NUMBER_FORMAT.format(key.get()));


      // set up merge attributes
      Configuration localMergeConfig = new Configuration(_conf);
      // we don't want to use a grouping comparator because the we are using the reducer code from the intermediate 
      // merge 
      localMergeConfig.setClass(MultiFileInputReader.MULTIFILE_COMPARATOR_CLASS,CrawlDBKey.LinkKeyComparator.class, RawComparator.class);
      localMergeConfig.setClass(MultiFileInputReader.MULTIFILE_KEY_CLASS,TextBytes.class, WritableComparable.class);

      // setup big buffer sizes for merge sort  
      localMergeConfig.setInt(MergeSortSpillWriter.SPILL_INDEX_BUFFER_SIZE_PARAM, 250000000);
      localMergeConfig.setInt(SequenceFileSpillWriter.SPILL_WRITER_BUFFER_SIZE_PARAM,250000000);
      // set small queue size so as to not run out RAM 
      localMergeConfig.setInt(SequenceFileSpillWriter.SPILL_WRITER_BUFFER_QUEUE_SIZE_PARAM,1);
      // set codec ... 
      localMergeConfig.set(SequenceFileSpillWriter.SPILL_WRITER_COMPRESSION_CODEC,GzipCodec.class.getName());
      
      // create index writer ... 
      KeyBasedSequenceFileIndex.IndexWriter<TextBytes,TextBytes> indexWriter = new KeyBasedSequenceFileIndex.IndexWriter<TextBytes,TextBytes>(localMergeConfig, indexOutputPath);

      // splill writer ... 
      SequenceFileSpillWriter<TextBytes, TextBytes> spillWriter 
      = new SequenceFileSpillWriter<TextBytes, TextBytes>(
          FileSystem.get(outputPath.toUri(),localMergeConfig),
          localMergeConfig, 
              outputPath, 
              TextBytes.class, 
              TextBytes.class, 
              indexWriter, true);

      try { 
        // pick filesystem based on path ... 
        FileSystem mergefs = getFileSystemForMergePath(incomingPaths.get(0),localMergeConfig);
  
        // initialize reader ... 
        LOG.info("FileSystem is:" + mergefs.toString());
        MultiFileInputReader<TextBytes> multiFileInputReader = new MultiFileInputReader<TextBytes>(mergefs, incomingPaths, localMergeConfig);
  
        try { 
          RawValueIterator rawValueIterator = new RawValueIterator();
          
          Pair<KeyAndValueData<TextBytes>,Iterable<RawRecordValue>> nextItem = null;
          // walk tuples and feed them to the actual reducer ...  
          while ((nextItem = multiFileInputReader.getNextItemIterator()) != null) {
    
            for (RawRecordValue rawValue : nextItem.e1) { 
              
              spillWriter.spillRawRecord(
                  nextItem.e0._keyData.getData(), 
                  0, 
                  nextItem.e0._keyData.getLength(),
                  rawValue.data.getData(),
                  0,
                  rawValue.data.getLength());
            }
            reporter.progress();
          }
        }
        finally { 
          multiFileInputReader.close();
        }
      }
      finally { 
        spillWriter.close();
      }
    }      

    private static FileSystem getFileSystemForMergePath(Path path,Configuration conf)throws IOException { 
      // override S3N 
      if (path.toUri().getScheme().equalsIgnoreCase("s3n")) { 
        FileSystem fs = new S3NFileSystem();
        fs.initialize(path.toUri(), conf);
        return fs;
      }
      // conf.setClass("fs.s3n.impl", S3NFileSystem.class,FileSystem.class);
      return FileSystem.get(path.toUri(),conf);
    }
    
  }

  /** 
   * An InputFormat that groups paths by shard id   
   * @author rana
   *
   */
  public static class CustomInputFormat implements InputFormat<IntWritable,Text> {

    public static final String PARTITION_COUNT_TEXT = "CustomFF.ParitionCount";
    public static final String PARTITION_ID_PREFIX = "CustomFF.ParitionID";
    public static void writePartitions(List<List<Path>> partitions,Configuration conf) { 
      conf.setInt(PARTITION_COUNT_TEXT,partitions.size());
      for (int partIndex=0;partIndex<partitions.size();++partIndex) { 
        conf.set(PARTITION_ID_PREFIX+partIndex,Joiner.on(',').join(partitions.get(partIndex)).toString());
      }
    }
    
    
    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits)
        throws IOException {
      int numPartitions = job.getInt(PARTITION_COUNT_TEXT, -1);
      InputSplit splits[] = new InputSplit[numPartitions];
      for (int i=0;i<numPartitions;++i) { 
        splits[i] = new TextDatumInputSplit(Integer.toString(i) +"," + job.get(PARTITION_ID_PREFIX+i));
      }
      return splits;
    }

    @Override
    public RecordReader<IntWritable, Text> getRecordReader(final InputSplit split,
        JobConf job, Reporter reporter) throws IOException {
      
      final ArrayList<String> parts = Lists.newArrayList(Splitter.on(',')
      .trimResults()
      .omitEmptyStrings()
      .split(((TextDatumInputSplit)split).getDatum()));
      
      final int partitionId = Integer.parseInt(parts.remove(0));

      return new RecordReader<IntWritable,Text>() {

        int index=0;
        
        @Override
        public boolean next(IntWritable key, Text value) throws IOException {
          if (index < parts.size()) { 
            key.set(partitionId);
            value.set(parts.get(index));
            index++;
            return true;
          }
          return false;
        }

        @Override
        public IntWritable createKey() {
          return new IntWritable();
        }

        @Override
        public Text createValue() {
          return new Text();
        }

        @Override
        public long getPos() throws IOException {
          return 0;
        }

        @Override
        public void close() throws IOException {
          
        }

        @Override
        public float getProgress() throws IOException {
          return 0;
        }
      };
    } 
    
  }
  
 }
