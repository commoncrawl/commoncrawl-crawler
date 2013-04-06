package org.commoncrawl.util;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.concurrent.Semaphore;
import java.util.zip.Deflater;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.io.NIOBufferList;
import org.commoncrawl.io.NIOHttpConnection;
import org.commoncrawl.protocol.shared.ArcFileHeaderItem;
import org.commoncrawl.protocol.shared.ArcFileItem;

/** 
 * Utility class used to retrieve a specific document from an ArcFile stored on S3
 * 
 * @author rana
 *
 */
@SuppressWarnings("static-access")
public class ArcFileItemFetcher {
  
  private static final Log LOG = LogFactory.getLog(ArcFileItemFetcher.class);
  
  
  public static ArcFileItem retrieveItem(String awsKey,String awsSecret,String awsBucket,String arcFilePath,long arcFileOffset,int arcItemSize) throws IOException {
    EventLoop eventLoop = new EventLoop();
    eventLoop.start();
    ArcFileItem itemOut = null;
    try { 
      itemOut = retrieveItem(eventLoop, awsKey, awsSecret, awsBucket, arcFilePath, arcFileOffset, arcItemSize);
    }
    finally { 
      eventLoop.stop();
    }
    return itemOut;
  }
  
  public static ArcFileItem retrieveItem(EventLoop eventLoop,String awsKey,String awsSecret,String awsBucket,String arcFilePath,long arcFileOffset,int arcItemSize) throws IOException {
        
    S3Downloader downloader = new S3Downloader(awsBucket,awsKey,awsSecret,false);
    
    // now activate the segment log ... 
    final Semaphore downloadCompleteSemaphore = new Semaphore(0);
    final StreamingArcFileReader arcFileReader = new StreamingArcFileReader(false);
    //arcFileReader.setArcFileHasHeaderItemFlag(false);
    
    // create a buffer list we will append incoming content into ... 
    final LinkedList<ByteBuffer> bufferList = new LinkedList<ByteBuffer>();
 
    downloader.initialize(new S3Downloader.Callback() {

      @Override
      public boolean contentAvailable(NIOHttpConnection connection,int itemId, String itemKey,NIOBufferList contentBuffer) {
        LOG.info("ContentQuery contentAvailable called for Item:" + itemKey + " totalBytesAvailable:" + contentBuffer.available());
        
        
        try { 
          while (contentBuffer.available() != 0) { 
            bufferList.add(contentBuffer.read());
          }
          return true;
        }
        catch (IOException e) {
          LOG.error(CCStringUtils.stringifyException(e));
          return false;
        }
      }

      @Override
      public void downloadComplete(int itemId, String itemKey) {
        LOG.info("S3 Download Complete for item:" + itemKey);
        downloadCompleteSemaphore.release();
      }

      @Override
      public void downloadFailed(int itemId, String itemKey, String errorCode) {
        LOG.info("S3 Download Failed for item:" + itemKey);
        downloadCompleteSemaphore.release();
      }

      @Override
      public boolean downloadStarting(int itemId, String itemKey,long contentLength) {
        LOG.info("ContentQuery DownloadStarting for Item:" + itemKey + " contentLength:" + contentLength);
        return true;
      } 
      
    },eventLoop);
    
    
    LOG.info("Starting request for Item:" + arcFilePath);
        
    //TODO: FIX RANGE OFFSET TO BE A LONG IN DOWNLOADER
    downloader.fetchPartialItem(arcFilePath, (int)arcFileOffset, arcItemSize);
    downloadCompleteSemaphore.acquireUninterruptibly();
    
    if (bufferList.size() == 0) { 
      return null;
    }
    
    ByteBuffer firstBuffer = bufferList.getFirst();
    if (firstBuffer != null) { 
      int offsetToGZIPHeader = scanForGZIPHeader(firstBuffer.duplicate());
      if (offsetToGZIPHeader != -1) { 
        firstBuffer.position(offsetToGZIPHeader);
        LOG.info("*** Offset to GZIP Header:" + offsetToGZIPHeader);
      }
      else { 
        LOG.error("*** Failed to find GZIP Header offset");
      }
    }
    
    // now try to decode content if possible
    for (ByteBuffer buffer : bufferList) { 
      LOG.info("Adding Buffer of Size:" + buffer.remaining() + " Position:" + buffer.position() + " Limit:" + buffer.limit());
      arcFileReader.available(buffer);
    }
    
    ArcFileItem item = arcFileReader.getNextItem();

    if (item != null) { 
      LOG.info("Request Returned item:" + item.getUri());
      LOG.info("Uncompressed Size:" + item.getContent().getCount());
    }
    return item;
  }
  
  static int scanForGZIPHeader(ByteBuffer byteBuffer) throws IOException { 
    
    LOG.info("*** SCANNING FOR GZIP MAGIC Bytes:" + Byte.toString((byte)StreamingArcFileReader.GZIP_MAGIC) + " " + Byte.toString((byte)(StreamingArcFileReader.GZIP_MAGIC >> 8)) + " BufferSize is:" + byteBuffer.limit() + " Remaining:" + byteBuffer.remaining());
    int limit = byteBuffer.limit();
    
    while (byteBuffer.position() + 2 < limit) { 
      //LOG.info("Reading Byte At:"+ byteBuffer.position());
      int b = byteBuffer.get();
      //LOG.info("First Byte is:"+ b);
      if (b == (byte)(StreamingArcFileReader.GZIP_MAGIC)) {
        
        byteBuffer.mark();
        
        byte b2 = byteBuffer.get();
        //LOG.info("Second Byte is:"+ b2);
        if (b2 == (byte)(StreamingArcFileReader.GZIP_MAGIC >> 8)) {
          
          byte b3 = byteBuffer.get();
          if (b3 == Deflater.DEFLATED) { 
            LOG.info("Found GZip Magic at:" + (byteBuffer.position() - 3));
            return byteBuffer.position() - 3;
          }
        }
        byteBuffer.reset();
      }
    }
    LOG.error("Failed to Find GZIP Magic!!");
    //LOG.error(Arrays.toString(byteBuffer.array()));
    return -1;
  }
  
  
  static Options options = new Options();
  static { 
    
    options.addOption(
        OptionBuilder.withArgName("awsKey").hasArg().withDescription("AWS Key").isRequired().create("awsKey"));
    
    options.addOption(
        OptionBuilder.withArgName("awsSecret").hasArg().withDescription("AWS Secret").isRequired().create("awsSecret"));

    options.addOption(
        OptionBuilder.withArgName("bucket").hasArg().withDescription("S3 bucket name").isRequired().create("bucket"));
    
    options.addOption(
        OptionBuilder.withArgName("path").hasArg().withDescription("S3 path prefix").isRequired().create("path"));

    options.addOption(
        OptionBuilder.withArgName("offset").hasArg().withDescription("Offset of item in file").isRequired().create("offset"));

    options.addOption(
        OptionBuilder.withArgName("length").hasArg().withDescription("Compressed length of item in file").isRequired().create("length"));
    
  }
  
  static void printUsage() { 
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( "ArcFileItemFetcher", options );
  }
  
  public static void main(String[] args) {
    CommandLineParser parser = new GnuParser();

    try {
      // parse the command line arguments
      CommandLine cmdLine = parser.parse( options, args );
      
      try { 
        ArcFileItem item = retrieveItem(
            cmdLine.getOptionValue("awsKey"),
            cmdLine.getOptionValue("awsSecret"),
            cmdLine.getOptionValue("bucket"),
            cmdLine.getOptionValue("path"),
            Integer.parseInt(cmdLine.getOptionValue("offset")),
            Integer.parseInt(cmdLine.getOptionValue("length")));
        
        if (item != null) { 
          OutputStreamWriter writer = new OutputStreamWriter(System.out, Charset.forName("UTF-8"));
          try { 
            writer.write(item.getUri());
            writer.write('\n');
            for (ArcFileHeaderItem header : item.getHeaderItems()) { 
              writer.write(header.getItemKey()+":"+header.getItemValue());
              writer.write("\r\n");
            }
            writer.write("\r\n");
            writer.flush();
            System.out.write(item.getContent().getReadOnlyBytes(),item.getContent().getOffset(),item.getContent().getCount());
            System.out.flush();
            System.exit(0);
          }
          finally { 
            writer.close();
          }
        }
        else { 
          System.err.println("Unable to retrieve/decode item!");
          System.exit(1);
        }
      }
      catch (IOException e) { 
        LOG.error("Failed to retrieve item with Error:" + CCStringUtils.stringifyException(e));
      }
    }
    catch (Exception e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      
      printUsage();
    }
  }
}
