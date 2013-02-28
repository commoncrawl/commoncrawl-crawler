package org.commoncrawl.tools;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.util.Progressable;
import org.commoncrawl.util.CCStringUtils;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.io.CountingInputStream;

/**
 * Hacked together utility to breakup the bulk Blekko URL list file into smaller SequenceFile
 * chunks and push them up to S3. 
 *   
 * @author rana
 */
public class BlekkoURLListTransfer {

  public static final Log LOG = LogFactory.getLog(BlekkoURLListTransfer.class);
  
  static Options options = new Options();
  static { 
    
    options.addOption(
        OptionBuilder.withArgName("awsKey").hasArg().withDescription("AWS Key").isRequired().create("awsKey"));
    
    options.addOption(
        OptionBuilder.withArgName("awsSecret").hasArg().withDescription("AWS Secret").isRequired().create("awsSecret"));

    options.addOption(
        OptionBuilder.withArgName("s3bucket").hasArg().withDescription("S3 bucket name").isRequired().create("s3bucket"));
    
    options.addOption(
        OptionBuilder.withArgName("s3path").hasArg().withDescription("S3 path prefix").isRequired().create("s3path"));
    
    options.addOption(
        OptionBuilder.withArgName("input").hasArg().withDescription("Input URL List").isRequired().create("input"));

  }
  
  static void printUsage() { 
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( "BlekkoURLListTransfer", options );
  }  

  private static final String IN_MEMORY_FS_URI = "imfs://localhost/";

  private static final int HOLDING_BUFFER_SIZE = 1 << 16;
  private static final int SCAN_BUFFER_SIZE = HOLDING_BUFFER_SIZE/4; // 1 << 20;
  private static final int CANNED_FILE_COMPRESSED_BLOCK_SIZE = 1 << 20; // 1 MB
  private static final int CANNED_FILE_SIZE = CANNED_FILE_COMPRESSED_BLOCK_SIZE * 100;
  private static final int CANNED_FILE_SIZE_PAD = CANNED_FILE_COMPRESSED_BLOCK_SIZE  * 10;
  private static final Path CANNED_FILE_PATH = new Path("/tmp/cannedFile");
  
  private static final String COMPLETION_FILE_SUFFIX = "COMPLETE";
 
  private static class InMemoryFSHack extends FileSystem {

    static class CustomInputStream extends DataInputBuffer implements PositionedReadable, Seekable {
      
      int offset;
      int len;
      
      public CustomInputStream(byte[] data,int offset,int length) { 
        this.offset = offset;
        this.len = length;
        super.reset(data,offset, length);
      }
      
      @Override
      public void readFully(long position, byte[] buffer) throws IOException {
        read(position,buffer,0,buffer.length);
      }
      
      @Override
      public void readFully(long position, byte[] buffer, int offset, int length)
          throws IOException {
        read(position,buffer,offset,length);
      }
      
      @Override
      public int read(long position, byte[] buffer, int offset, int length)
          throws IOException {
        System.arraycopy(this.getData(), (int)position, buffer, 0, buffer.length);
        return length;
      }

      @Override
      public void seek(long pos) throws IOException {
        super.reset(getData(),offset+(int)pos,len-(int)pos);
      }

      @Override
      public long getPos() throws IOException {
        return super.getPosition();
      }

      @Override
      public boolean seekToNewSource(long targetPos) throws IOException {
        return false;
      }
      
    }
    
    InMemoryFSHack(Configuration conf) { 
      setConf(conf);
    }
    
    DataOutputBuffer outputStream = new DataOutputBuffer(CANNED_FILE_SIZE + CANNED_FILE_SIZE_PAD);
    
    @Override
    public URI getUri() {
      try {
        return new URI(IN_MEMORY_FS_URI);
      } catch (URISyntaxException e) {
        return null;
      }
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
      CustomInputStream inputStream = new CustomInputStream(outputStream.getData(),0,outputStream.getLength());
      inputStream.reset(outputStream.getData(),0,outputStream.getLength());
      return new FSDataInputStream(inputStream);
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission,
        boolean overwrite, int bufferSize, short replication, long blockSize,
        Progressable progress) throws IOException {
      outputStream.reset();
      
      return new FSDataOutputStream(outputStream, null);
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize,
        Progressable progress) throws IOException {
      return null;
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    @Deprecated
    public boolean delete(Path f) throws IOException {
      return false;
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
      outputStream.reset();
      return true;
    }

    public DataOutputBuffer swapBuffers() { 
      DataOutputBuffer out = outputStream;
      outputStream = new DataOutputBuffer(CANNED_FILE_SIZE + CANNED_FILE_SIZE_PAD);
      return out;
    }
    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
      return null;
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {      
    }

    @Override
    public Path getWorkingDirectory() {
      return null;
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
      return false;
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      return new FileStatus(outputStream.getLength(),false,1,1,1,CANNED_FILE_PATH);
    } 
  }
  
  private static long readWriteNextLine(CountingInputStream is,ByteBuffer inputBuffer,DataOutputBuffer outputBuffer,SequenceFile.Writer writer)throws IOException { 
  
    outputBuffer.reset();
    
    for (;;) {

      if (inputBuffer.remaining() == 0) {
        int bytesRead = is.read(inputBuffer.array());
        if (bytesRead == -1) { 
          throw new EOFException();
        }
        else { 
          inputBuffer.clear();
          inputBuffer.limit(bytesRead);
        }

      }
      
      int scanStartPos = inputBuffer.position();
      boolean eos=false;
      while (inputBuffer.remaining() != 0) {
        byte nextChar = inputBuffer.get();
        if ((nextChar == '\n') || (nextChar == '\r')) {
          eos=true;
          break;
        }
      }
      // put whatever we read into the output buffer .. .
      outputBuffer.write(inputBuffer.array(),scanStartPos,inputBuffer.position() - scanStartPos);
      
      if (eos) { 
        break;
      }
    }
    String line = new String(outputBuffer.getData(),0,outputBuffer.getLength(),Charset.forName("UTF-8"));
    int spaceDelimiter = line.indexOf(' ');
    if (spaceDelimiter != -1 && spaceDelimiter < line.length() - 1) { 
      String url = line.substring(0, spaceDelimiter);
      String metadata = line.substring(spaceDelimiter+1);
      if (url.length() != 0 && metadata.length() != 0) {
        writer.append(new Text(url), new Text(metadata));
        // System.out.println("URL:" + url + " Metadata:" + metadata);
      }
    }
    return is.getCount() + inputBuffer.position();
  }
  
  
  private static boolean scanForCompletionFile(AmazonS3Client s3Client,String s3Bucket,String s3Path)throws IOException {
    String finalPath = s3Path +  COMPLETION_FILE_SUFFIX;
    
    try { 
      s3Client.getObjectMetadata(s3Bucket,finalPath);
      return true;
    }
    catch (AmazonServiceException e) { 
      if (e.getStatusCode() == 404) { 
        return false;
      }
      else { 
        throw new IOException(e);
      }
    }
  }
  
  private static Pattern seqFilePattern = Pattern.compile(".*/([0-9]*)\\.seq");
  
  private static SequenceFile.Writer flushFile(InMemoryFSHack fs,Configuration conf, Uploader uploader,String s3Bucket,String s3FolderPath,long lastValidReadPos, SequenceFile.Writer writer) throws IOException {
    
    writer.close();
    
    String fullS3Path =s3FolderPath + Long.toString(lastValidReadPos) + ".seq";
    
    // ok detach the buffer 
    DataOutputBuffer bufferOut = fs.swapBuffers();
    
    DataInputBuffer inputStream = new DataInputBuffer();
    inputStream.reset(bufferOut.getData(),0,bufferOut.getLength());
    
    QueueItem queueItem = new QueueItem(s3Bucket,fullS3Path,inputStream);

    LOG.info("Queueing for Upload File:" + fullS3Path + "of size:" + fs.getFileStatus(CANNED_FILE_PATH).getLen() + " to S3");
    try {
      uploader.queue.put(queueItem);
    } catch (InterruptedException e) {
    }
    LOG.info("Queued for Upload File:" + fullS3Path + "of size:" + fs.getFileStatus(CANNED_FILE_PATH).getLen() + " to S3");
        
    return createWriter(fs, conf);
  }
  
  private static long scanForLastValidOffset(AmazonS3Client s3Client,String s3Bucket,String s3Path) throws IOException { 
    ObjectListing listing = s3Client.listObjects(s3Bucket,s3Path);
    boolean done = false;

    long lastValidOffsetOut = 0L;
    do { 
      for (S3ObjectSummary summary : listing.getObjectSummaries()) {
        Matcher seqFileMatcher = seqFilePattern.matcher(summary.getKey());
        if (seqFileMatcher.matches()) { 
          lastValidOffsetOut = Math.max(lastValidOffsetOut,Long.parseLong(seqFileMatcher.group(1)));
        }
      }
      if (listing.isTruncated()) { 
        listing = s3Client.listNextBatchOfObjects(listing);
      }
      else { 
        done = true;
      }
    }
    while (!done);
    
    return lastValidOffsetOut;
  }
  
  private static SequenceFile.Writer createWriter(FileSystem fs,Configuration conf) throws IOException { 
    return SequenceFile.createWriter(fs,conf,CANNED_FILE_PATH,Text.class,Text.class,CompressionType.BLOCK,new SnappyCodec());
  }
  
  public static class QueueItem {
    String bucket;
    String path;
    DataInputBuffer payload;
    
    public QueueItem() { 
      
    }
    
    public QueueItem(String bucket,String path,DataInputBuffer payload) { 
      this.bucket = bucket;
      this.path = path;
      this.payload = payload;
    }
  }
  
  public static class Uploader {

    static final int MAX_BACKLOG_SIZE  = 15;
    static final int UPLOADER_THREAD_COUNT = 10;
    
    LinkedBlockingQueue<QueueItem> queue = new LinkedBlockingQueue<QueueItem>(MAX_BACKLOG_SIZE);
    Thread threads[] = new Thread[UPLOADER_THREAD_COUNT];
    AmazonS3Client s3Client;
    Semaphore runningWaitSemaphore = new Semaphore( - (UPLOADER_THREAD_COUNT -1));
    
    public Uploader(String awsAccessKey,String awsSecret) throws IOException { 
      BasicAWSCredentials credentials 
        = new BasicAWSCredentials(
        awsAccessKey,awsSecret);
      // create the client ... 
      s3Client = new AmazonS3Client(credentials);
      
      for (int i=0;i<UPLOADER_THREAD_COUNT;++i) {
        
        // closure the thread index ... 
        final int threadIndex = i;
        threads[threadIndex] = new Thread(new Runnable() {
  
          @Override
          public void run() {
            try { 
              while (true) { 
                try { 
                  QueueItem item = queue.take();
                  
                  if (item.payload == null) { 
                    LOG.info("UPLOADER_THREAD[" + threadIndex + "]:Received NULL Queue Item. Exiting");
                    break;
                  }
                  else {
                    
                    boolean done = false;
                    int retryCount = 0;
    
                    while (!done) {
                      
                      try { 
                        long flushStartTime = System.currentTimeMillis();
                        ObjectMetadata metadata = new ObjectMetadata();
                        metadata.setContentLength(item.payload.getLength());
                        s3Client.putObject(item.bucket, item.path, item.payload,metadata);
                        long flushEndTime = System.currentTimeMillis();
                        LOG.info("UPLOADER_THREAD[" + threadIndex + "]: Flushing Finished for File:" + item.path + "of size:" + item.payload.getLength() + " Took:" + (flushEndTime-flushStartTime));
                        done = true;
                      }
                      catch (Exception e) { 
                        LOG.error("UPLOADER_THREAD[" + threadIndex + "]: Exception While Flusing File:" + item.path + " of size:" + item.payload.getLength() 
                            + " Exception:" + CCStringUtils.stringifyException(e) + " RetryCount:" + retryCount);
                        
                        ++retryCount;
                      }
                    }
                  }
                }
                catch (InterruptedException e) { 
                  
                }
              }
              
              LOG.info("UPLOADER_THREAD[" + threadIndex + "]: DONE");
            }
            finally { 
            runningWaitSemaphore.release();
            }
          }
        });
        threads[threadIndex].start();
      }
    }
  }
  
  public static void main(String[] args) {
    CommandLineParser parser = new GnuParser();
    
    try { 
      // parse the command line arguments
      CommandLine cmdLine = parser.parse( options, args );
 
      BasicAWSCredentials credentials 
        = new BasicAWSCredentials(
          cmdLine.getOptionValue("awsKey"),
          cmdLine.getOptionValue("awsSecret"));
      
      // create the client ... 
      AmazonS3Client s3Client = new AmazonS3Client(credentials);
      
      // create uploader thread ... 
      Uploader uploader = new Uploader(cmdLine.getOptionValue("awsKey"), cmdLine.getOptionValue("awsSecret"));
      
      // get length of input file ...  
      File inputFile = new File(cmdLine.getOptionValue("input"));
      
      // allocate in memory file system  
      Configuration conf = new Configuration();
      conf.setInt("io.seqfile.compress.blocksize",CANNED_FILE_COMPRESSED_BLOCK_SIZE);
      
      InMemoryFSHack fsHack = new InMemoryFSHack(conf);
      DataOutputBuffer outputBuffer = new DataOutputBuffer(HOLDING_BUFFER_SIZE);
      
      // get bucket and input path parameters 
      String s3bucket = cmdLine.getOptionValue("s3bucket");
      String s3path   = cmdLine.getOptionValue("s3path");
      // scan for completion marker ... 
      if (scanForCompletionFile(s3Client,s3bucket,s3path) == false) { 
        // scan existing files to find last decompressed offset ...
        long lastReadPos = scanForLastValidOffset(s3Client,s3bucket,s3path);
        LOG.info("Last Valid Read Pos:" + lastReadPos);
        // open input stream ... 
        CountingInputStream countingInputStream = new CountingInputStream(new FileInputStream(inputFile));
        // setup inflater ... 
        LOG.info("Initializing GZIP Stream for File at:" + inputFile);
        GZIPInputStream inflater = new GZIPInputStream(countingInputStream,SCAN_BUFFER_SIZE);
        // init counting stream to wrap inflater 
        CountingInputStream countingDecompressedStream = new CountingInputStream(inflater);
        // skip to last scan offset 
        inflater.skip(lastReadPos);
        
        ByteBuffer scanBuffer = ByteBuffer.allocate(SCAN_BUFFER_SIZE);
        boolean eof = false;
        
        //read input file, collecting lines into buffer ...
        long lineCount = 0;
        
        // create sequence file ... 
        SequenceFile.Writer writer = createWriter(fsHack, conf);
        
        while (!eof) { 
          
          try { 
            lastReadPos = readWriteNextLine(countingDecompressedStream, scanBuffer, outputBuffer,writer);
            ++lineCount;
            if (lineCount % 10000 == 0) { 
              LOG.info("Read 10000 lines RAW Pos:" + countingInputStream.getCount() + " lastReadPos:" + lastReadPos + " TotalLines:" + lineCount);
            }
          }
          catch (EOFException e) { 
            LOG.info("HIT EOF AT Raw Pos:" + countingInputStream.getCount() + " lastReadPos:" + lastReadPos);
            eof = true;
          }
          
          // once our buffer flush threshold is hit or if eof .. .
          if (eof || writer.getLength() >= CANNED_FILE_SIZE) { 
            // flush buffer to s3
            writer = flushFile(fsHack,conf,uploader,s3bucket,s3path,lastReadPos,writer);
            // reset output buffer 
            outputBuffer.reset();
          }
        }
        
        LOG.info("Done Processing Data. Queueing Empty Item");
        uploader.queue.put(new QueueItem());
        
        LOG.info("Waiting for Uploader Threads to Die");
        uploader.runningWaitSemaphore.acquireUninterruptibly();
        LOG.info("Uploader Thread Dead. Exiting");
      }
    }
    catch (ParseException e) { 
      System.out.println("Error parsing command line:" + e.getMessage());
    }
    catch( Exception exp ) {
      // oops, something went wrong
      LOG.error(CCStringUtils.stringifyException(exp));
      
      printUsage();
    }    
  }
}
