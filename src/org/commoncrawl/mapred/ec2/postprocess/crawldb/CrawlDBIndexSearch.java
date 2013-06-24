package org.commoncrawl.mapred.ec2.postprocess.crawldb;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.ValueBytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.commoncrawl.hadoop.mergeutils.TextFileSpillWriter;
import org.commoncrawl.mapred.ec2.postprocess.crawldb.CrawlDBMergeSortReducer.RawValueIterator;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.KeyBasedSequenceFileIndex;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader.KeyAndValueData;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader.RawRecordValue;
import org.commoncrawl.util.S3NFileSystem;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.Tuples.Pair;
import org.commoncrawl.util.URLUtils;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

@SuppressWarnings("static-access")
public class CrawlDBIndexSearch {
  
  static Options options = new Options();
  
  static final Log LOG = LogFactory.getLog(CrawlDBIndexSearch.class);

  static { 
    options.addOption(OptionBuilder.withArgName("indexpath").hasArg(true).isRequired().withDescription("Index Path").create("indexpath"));
    options.addOption(OptionBuilder.withArgName("dbpath").hasArg(true).withDescription("Database Path").create("dbpath"));
    options.addOption(OptionBuilder.withArgName("dbpaths").hasArg(true).withDescription("Database Paths").create("dbpaths"));
    options.addOption(OptionBuilder.withArgName("domain").hasArg(true).isRequired().withDescription("Domain Name").create("domain"));
    options.addOption(OptionBuilder.withArgName("outputpath").hasArg(true).isRequired().withDescription("Output Path").create("outputpath"));
    options.addOption(OptionBuilder.withArgName("fileprefix").hasArg(true).withDescription("Index File Prefix").create("fileprefix"));
  }
  
  public static void main(String[] args) throws Exception {
    CommandLineParser parser = new GnuParser();
    
    try { 
      // parse the command line arguments
      CommandLine cmdLine = parser.parse( options, args );
      
      runQuery(cmdLine);
    }
    catch (Exception e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp( "CrawlDBBlekkoMerge", options );
      
      throw e;
    }
    finally {
      System.exit(0);
    }
  }
  
  private static FileSystem getFileSystemForPath(Path path,Configuration conf)throws IOException { 
    // override S3N 
    conf.setClass("fs.s3n.impl", S3NFileSystem.class,FileSystem.class);
    return FileSystem.get(path.toUri(),conf);
  }
  
  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }    

  private static Function<TextBytes,String> createStandardTransformer() { 
    final JsonParser parser = new JsonParser();
    return new Function<TextBytes, String>() {

      @Override
      public String apply(final TextBytes arg0) {
        JsonObject jsonObj = parser.parse(arg0.toString()).getAsJsonObject();
        
        ArrayList<String> fields = Lists.newArrayList();
        
        if (jsonObj != null) {
          JsonObject blekkoStatus = jsonObj.getAsJsonObject(CrawlDBCommon.TOPLEVEL_BLEKKO_METADATA_PROPERTY);
          if (blekkoStatus != null) {
            // blekko url 
            fields.add("1");
            // blekko crawl status 
            if (blekkoStatus.has(CrawlDBCommon.BLEKKO_METADATA_STATUS) 
                && blekkoStatus.get(CrawlDBCommon.BLEKKO_METADATA_STATUS).getAsString().equalsIgnoreCase("crawled")) { 
              fields.add("1");
            }
            else { 
              fields.add("0");
            }
          }
          else { 
            // is blekko url 
            fields.add("0");
            // blekko crawled 
            fields.add("0");
          }
          JsonObject crawlStatus = jsonObj.getAsJsonObject(CrawlDBCommon.TOPLEVEL_SUMMARYRECORD_PROPRETY);
          JsonObject linkStatus = jsonObj.getAsJsonObject(CrawlDBCommon.TOPLEVEL_LINKSTATUS_PROPERTY);
          if (crawlStatus != null || linkStatus != null) { 
            // is cc url 
            fields.add("1");
            if (crawlStatus != null
                && crawlStatus.get(CrawlDBCommon.SUMMARYRECORD_ATTEMPT_COUNT_PROPERTY).getAsInt() != 0) { 
                // crawled ... 
                fields.add("1");
            }
            else { 
              // crawled ... 
              fields.add("0");
            }
            if (linkStatus != null 
                && linkStatus.has(CrawlDBCommon.LINKSTATUS_EXTRADOMAIN_SOURCES_COUNT_PROPERTY)
                && linkStatus.get(CrawlDBCommon.LINKSTATUS_EXTRADOMAIN_SOURCES_COUNT_PROPERTY).getAsInt() != 0) { 
              // ext hrefs
              fields.add("1");
            }
            else { 
              // ext hrefs 
              fields.add("0");
            }
          }
          else { 
            // is cc url ... 
            fields.add("0");
            // crawled (not) 
            fields.add("0");
            // ext hrefs 
            fields.add("0");
          }
        }
        else { 
          for (int i=0;i<5;++i)
            fields.add("0");
        }
        return Joiner.on("\t").join(fields).toString();
      }
    };
  }
  
  private static void runQuery(CommandLine commandLine) throws IOException {
    
    final Configuration conf = new Configuration();
    
    URLFPV2 fp = URLUtils.getURLFPV2FromHost(commandLine.getOptionValue("domain"));
    if (fp == null) { 
      throw new IOException("Invalid Domain:" + commandLine.getOptionValue("domain"));
    }

    // construct min/max keys
    final Pair<TextBytes,TextBytes> minMaxKeys 
      = CrawlDBKey.generateMinMaxKeysForDomain(
          fp.getRootDomainHash(),
          (fp.getRootDomainHash() == fp.getDomainHash()) ? -1L:fp.getDomainHash());
    
    Path dbPath = null;
    final Path indexPath = new Path(commandLine.getOptionValue("indexpath"));

    
    Map<Integer,Pair<Path,Path>> shardToPathMap = Maps.newTreeMap();
    
    if (commandLine.hasOption("dbpath")) { 
      dbPath = new Path(commandLine.getOptionValue("dbpath"));
      FileSystem dbFS      = getFileSystemForPath(dbPath,conf);
      FileStatus parts[] = dbFS.globStatus(new Path(indexPath,"part-*"));
      for (FileStatus part : parts) { 
        Path itemPath = part.getPath();
        int shardIndex = Integer.parseInt(itemPath.getName().substring("part-".length()));
        Pair<Path,Path> tuple = new Pair<Path,Path>(itemPath,new Path(indexPath,"index-" + NUMBER_FORMAT.format(shardIndex)));
        shardToPathMap.put(shardIndex, tuple);
      }
    }
    else if (commandLine.hasOption("dbpaths")) {
      
      Path partsFilePath = new Path(commandLine.getOptionValue("dbpaths"));
      LOG.info("Parts File:" + partsFilePath);
      
      FSDataInputStream inputStream = FileSystem.get(partsFilePath.toUri(),conf).open(partsFilePath);
      try { 
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line = null;
        while ((line = reader.readLine()) != null) { 
          Path itemPath = new Path(line);
          int shardIndex = Integer.parseInt(itemPath.getName().substring("part-".length()));
          Pair<Path,Path> tuple = new Pair<Path,Path>(itemPath,new Path(indexPath,"index-" + NUMBER_FORMAT.format(shardIndex)));
          shardToPathMap.put(shardIndex, tuple);
        }
      }
      finally { 
        inputStream.close();
      }
    }
     
    if (shardToPathMap.size() == 0) { 
      throw new IOException("No Valid Shards Specified!");
    }
    
    final Path outputPath = new Path(commandLine.getOptionValue("outputpath"));
    final FileSystem outputFS  = getFileSystemForPath(outputPath,conf);

    // for each part invoke a parallel query 
    List<Callable<String>> callables = Lists.newArrayList();
    for (Map.Entry<Integer,Pair<Path,Path>> entry : shardToPathMap.entrySet()) { 
      final int threadIndex = entry.getKey();

      final FileSystem indexFS   = getFileSystemForPath(entry.getValue().e1,conf);
      final FileSystem dbFS      = getFileSystemForPath(entry.getValue().e0,conf);

      final Path indexFilePath = entry.getValue().e1;
      final Path crawlDBFilePath = entry.getValue().e0;
      final Path outputFilePath = new Path(outputPath,"part-" + NUMBER_FORMAT.format(threadIndex));
      final DataOutputBuffer minKeyOutputBuffer = new DataOutputBuffer();
      final DataOutputBuffer maxKeyOutputBuffer = new DataOutputBuffer();

      minMaxKeys.e0.write(minKeyOutputBuffer);
      minMaxKeys.e1.write(maxKeyOutputBuffer);
      
      callables.add(new Callable<String>() {

        @Override
        public String call() throws Exception {
          try { 
            // load index file 
            LOG.info("Loading Index:" + threadIndex);
            KeyBasedSequenceFileIndex<TextBytes> index = new KeyBasedSequenceFileIndex<TextBytes>(conf,indexFilePath,new CrawlDBKey.LinkKeyComparator());
            KeyBasedSequenceFileIndex.IndexReader<TextBytes> reader = new KeyBasedSequenceFileIndex.IndexReader<TextBytes>(index);
            
            // find best position 
            LOG.info("Searching Index:" + threadIndex);
            long seqFilePos = reader.findBestPositionForKey(minKeyOutputBuffer.getData(),0,minKeyOutputBuffer.getLength());
            LOG.info("Search of Index:" + threadIndex + " Returned Pos:" + seqFilePos);
  
            
            SequenceFile.Writer writer = SequenceFile.createWriter(outputFS, conf,outputFilePath, Text.class, Text.class,CompressionType.BLOCK,new SnappyCodec());
            try { 
              // open a reader 
              LOG.info("Opening CrawlDB File at Path:" + crawlDBFilePath);
              SequenceFile.Reader seqReader = new SequenceFile.Reader(dbFS, crawlDBFilePath, conf);
              RawComparator<TextBytes> comparator = new CrawlDBKey.LinkKeyComparator();
              
              try { 
                LOG.info("Seeking Index File:" + seqFilePos);
                // seqReader.seek(seqFilePos - 16);
                seqReader.sync(seqFilePos - 16);
                LOG.info("Position Now:" + seqReader.getPosition());
                
                boolean enteredRange = false;
                boolean exitedRange = false;
                
                DataOutputBuffer keyOutputBuffer = new DataOutputBuffer();
                DataOutputBuffer valueOutputBuffer = new DataOutputBuffer();
                DataInputBuffer valueInputBuffer = new DataInputBuffer();
                TextBytes        valueText = new TextBytes();
                Text             urlText = new Text();
                ValueBytesWrapper valBytesWrap = new ValueBytesWrapper();
                valBytesWrap.sourceData = valueOutputBuffer; 
                
                DataInputBuffer keyInputBuffer = new DataInputBuffer();
                JsonParser parser = new JsonParser();
                
                ValueBytes valueBytes = seqReader.createValueBytes();
                
                TextBytes keyBytes = new TextBytes();
                long emittedKeyCount=0;
                while (!exitedRange) { 
                  if (seqReader.nextRawKey(keyOutputBuffer)== -1) { 
                    break;
                  }
                  else {
                    keyInputBuffer.reset(keyOutputBuffer.getData(),0,keyOutputBuffer.getLength());
                    keyBytes.setFromRawTextBytes(keyInputBuffer);
                    //LOG.info("SeqFileKey:" + debugStr + " TargetKey:" + minMaxKeys.e0);
                    
                    if (!enteredRange) { 
                      if (comparator.compare(keyOutputBuffer.getData(),0, keyOutputBuffer.getLength(),
                          minKeyOutputBuffer.getData(), 0, minKeyOutputBuffer.getLength()) >= 0) { 
                        LOG.info("Entered Range");
                        enteredRange = true;
                      }
                    }
                    if (enteredRange) { 
                      if (comparator.compare(keyOutputBuffer.getData(),0, keyOutputBuffer.getLength(),
                          maxKeyOutputBuffer.getData(), 0, maxKeyOutputBuffer.getLength()) > 0) {
                        LOG.info("Exited Range - Emitted Keys:" + emittedKeyCount);
                        exitedRange = true;
                      }
                    }
                    if (enteredRange && !exitedRange) {
                      long keyType = CrawlDBKey.getLongComponentFromKey(keyBytes, CrawlDBKey.ComponentId.TYPE_COMPONENT_ID);
                      if (keyType == CrawlDBKey.Type.KEY_TYPE_MERGED_RECORD.ordinal()) { 
                        seqReader.nextRawValue(valueBytes);
                        // valid data found
                        valueBytes.writeUncompressedBytes(valueOutputBuffer);
                        valueInputBuffer.reset(valueOutputBuffer.getData(), valueOutputBuffer.getLength());
                        valueText.setFromRawTextBytes(valueInputBuffer);
                        // inefficient ... but in the interest of getting shit done ...
                        try { 
                          JsonObject object = parser.parse(valueText.toString()).getAsJsonObject();
                          urlText.set(object.get("source_url").getAsString());
                          // write the key out raw ... 
                          keyOutputBuffer.reset();
                          urlText.write(keyOutputBuffer);
                          // write out key/val raw 
                          writer.appendRaw(
                              keyOutputBuffer.getData(), 
                              0, 
                              keyOutputBuffer.getLength(),
                              valBytesWrap);
                              
                              
                          emittedKeyCount++;
                        }
                        catch (Exception e) { 
                          LOG.error(CCStringUtils.stringifyException(e));
                          throw new IOException("Invalid JSON!:" + valueText.toString());
                        }
                      }
                    }
                  }
                  keyOutputBuffer.reset();
                  valueOutputBuffer.reset();
                }
              }
              finally { 
                seqReader.close();
              }
            }
            finally { 
              writer.close();
            }
            return outputFilePath.toString();
          }
          catch (Exception e) { 
            LOG.error(CCStringUtils.stringifyException(e));
            throw e;
          }
        } 
      });
    }
    // create the exector
    ExecutorService executor = Executors.newFixedThreadPool(500);
    LOG.info("Queueing " + callables.size() + " Work Items");
    // for now, just standard transformer ... 
    Function<TextBytes,String> valueTransformer = createStandardTransformer();
    
    // execute queued items 
    try {
      List<Future<String>> futures = executor.invokeAll(callables);
      mergeResults(conf,outputFS,outputPath,valueTransformer);
    } catch (InterruptedException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    // shutdown executor service gracefully  
    executor.shutdown();
    LOG.info("Waiting for shutdown");
    // wait for completion 
    try {
      executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
      LOG.info("Execution Completed");
    } catch (InterruptedException e) {
      LOG.error("Execution Interrupted!");
    }
  }
  
  static void mergeResults(Configuration conf,FileSystem fs,Path outputPath,Function<TextBytes,String> valueTransformer)throws IOException { 
    // collect parts 
    FileStatus parts[] = fs.globStatus(new Path(outputPath,"part-*"));
    List<Path> inputs = ImmutableList.copyOf(
        Iterators.transform(Iterators.forArray(parts),new Function<FileStatus,Path>() {

      @Override
      @Nullable
      public Path apply(@Nullable FileStatus arg0) {
        return arg0.getPath();
      } 
      
    }));
    // feed to merger ... 
    // set up merge attributes
    Configuration localMergeConfig = new Configuration(conf);
    // we don't want to use a grouping comparator because the we are using the reducer code from the intermediate 
    // merge 
    localMergeConfig.setClass(MultiFileInputReader.MULTIFILE_COMPARATOR_CLASS,TextBytes.Comparator.class, RawComparator.class);
    localMergeConfig.setClass(MultiFileInputReader.MULTIFILE_KEY_CLASS,TextBytes.class, WritableComparable.class);
    

    Writer outputWriter = new OutputStreamWriter(fs.create(
        new Path(outputPath,"results.txt"),
        true,
        1000000
        ),Charset.forName("UTF-8"));

    try { 
      // initialize reader ... 
      LOG.info("FileSystem is:" + fs.toString());
      MultiFileInputReader<TextBytes> multiFileInputReader = new MultiFileInputReader<TextBytes>(fs, inputs, localMergeConfig);

      try { 
        RawValueIterator rawValueIterator = new RawValueIterator();
        
        Pair<KeyAndValueData<TextBytes>,Iterable<RawRecordValue>> nextItem = null;
        
        TextBytes valueText = new TextBytes();
        DataInputBuffer inputBuffer = new DataInputBuffer();
        
        // walk tuples and feed them to the actual reducer ...  
        while ((nextItem = multiFileInputReader.getNextItemIterator()) != null) {
          for (RawRecordValue rawValue : nextItem.e1) { 
            
            inputBuffer.reset(rawValue.data.getData(), 0, rawValue.data.getLength());
            valueText.setFromRawTextBytes(inputBuffer);
            outputWriter.write(nextItem.e0._keyObject.toString()+"\t"+valueTransformer.apply(valueText)+"\n");
          }
        }
      }
      finally { 
        multiFileInputReader.close();
      }
    }
    finally {
      try {
        outputWriter.flush();
      }
      finally { 
        IOUtils.closeStream(outputWriter);
      }
    }
  }
  
  /** 
   * We need this to avoid double buffer copies 
   * @author rana
   *
   */
  private static class ValueBytesWrapper implements ValueBytes {
    public DataOutputBuffer sourceData;
    @Override
    public void writeUncompressedBytes(DataOutputStream outStream)
        throws IOException {
      outStream.write(sourceData.getData(),0,sourceData.getLength());
    }

    @Override
    public void writeCompressedBytes(DataOutputStream outStream)
        throws IllegalArgumentException, IOException {
      outStream.write(sourceData.getData(),0,sourceData.getLength());
    }

    @Override
    public int getSize() {
      return sourceData.getLength();
    } 
    
  }

}
