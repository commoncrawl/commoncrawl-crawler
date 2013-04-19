package org.commoncrawl.mapred.ec2.postprocess.crawldb;

import java.io.DataOutputStream;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.ValueBytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.KeyBasedSequenceFileIndex;
import org.commoncrawl.util.S3NFileSystem;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.Tuples.Pair;
import org.commoncrawl.util.URLUtils;

import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

@SuppressWarnings("static-access")
public class CrawlDBIndexSearch {
  
  static Options options = new Options();
  
  static final Log LOG = LogFactory.getLog(CrawlDBIndexSearch.class);

  static { 
    options.addOption(OptionBuilder.withArgName("indexpath").hasArg(true).isRequired().withDescription("Index Path").create("indexpath"));
    options.addOption(OptionBuilder.withArgName("dbpath").hasArg(true).isRequired().withDescription("Database Path").create("dbpath"));
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
          
    final Path indexPath = new Path(commandLine.getOptionValue("indexpath"));
    final Path dbPath = new Path(commandLine.getOptionValue("dbpath"));
    final Path outputPath = new Path(commandLine.getOptionValue("outputpath"));
    final String indexFilePrefix = (commandLine.hasOption("fileprefix")?
        commandLine.getOptionValue("fileprefix") : "index-");
    final FileSystem indexFS   = getFileSystemForPath(indexPath,conf);
    final FileSystem dbFS      = getFileSystemForPath(dbPath,conf);
    final FileSystem outputFS  = getFileSystemForPath(outputPath,conf);

    // find out number for shard indexes... 
    int partCount = indexFS.globStatus(new Path(indexPath,"part-*")).length;

    if (partCount == 0) 
      throw new IOException("Invalid Index Path:" + indexPath);
    
    // for each part invoke a parallel query 
    List<Callable<String>> callables = Lists.newArrayList();
    for (int i=0;i<partCount;++i) {
      final int threadIndex = i;
      
      final Path indexFilePath = new Path(indexPath,indexFilePrefix + NUMBER_FORMAT.format(threadIndex));
      final Path crawlDBFilePath = new Path(dbPath,"part-" + NUMBER_FORMAT.format(threadIndex));
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
    // execute queued items 
    try {
      List<Future<String>> futures = executor.invokeAll(callables);
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
