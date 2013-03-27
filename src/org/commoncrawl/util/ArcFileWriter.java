/**
 * Copyright 2008 - CommonCrawl Foundation
 * 
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 **/

package org.commoncrawl.util;

import java.io.ByteArrayOutputStream;
import java.io.CharArrayWriter;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Date;
import java.util.LinkedList;
import java.util.Map;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableName;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.crawl.common.shared.Constants;
import org.commoncrawl.io.NIOBufferList;
import org.commoncrawl.io.NIOBufferListOutputStream;
import org.commoncrawl.io.NIODataSink;
import org.commoncrawl.io.NIOHttpHeaders;
import org.commoncrawl.protocol.ArcFileWriterStats;
import org.commoncrawl.protocol.CrawlURL;
import org.commoncrawl.protocol.MimeTypeCount;
import org.commoncrawl.protocol.shared.ArcFileItem;
import org.commoncrawl.util.GZIPUtils.UnzipResult;
import org.junit.Test;

import com.google.common.collect.TreeMultimap;

/**
 * 
 * @author rana
 * 
 */
public class ArcFileWriter {

  /** logging **/
  private static final Log              LOG                      = LogFactory
                                                                     .getLog(ArcFileWriter.class);

  private static SimpleDateFormat       TIMESTAMP14              = new SimpleDateFormat(
                                                                     "yyyyMMddHHmmss");
  private static SimpleDateFormat       FILENAME_TIMESTAMP       = new SimpleDateFormat(
                                                                     "yyyy/MM/dd/");

  public  static final int              MAX_SIZE_DEFAULT         = 100000000;
  private static final int              MAX_WRITERS_DEFAULT      = 10;
  private static final String           DEFAULT_ENCODING         = "ISO-8859-1";
  private static final String           ARC_MAGIC_NUMBER         = "filedesc://";
  public static final char              LINE_SEPARATOR           = '\n';
  private static final byte[]           ARC_GZIP_EXTRA_FIELD     = { 8, 0, 'L',
      'X', 4, 0, 0, 0, 0, 0                                     };
  private final static Pattern          TRUNCATION_REGEX         = Pattern
                                                                     .compile("^([^\\s;,]+).*");
  private static final String           NO_TYPE_MIMETYPE         = "no-type";
  private static final int              MAX_METADATA_LINE_LENGTH = (8 * 1024);
  private static final Pattern          METADATA_LINE_PATTERN    = Pattern
                                                                     .compile("^\\S+ \\S+ \\S+ \\S+ \\S+("
                                                                         + LINE_SEPARATOR
                                                                         + "?)$");
  private static final char             HEADER_FIELD_SEPARATOR   = ' ';
  private static final String           UTF8                     = "UTF-8";

  private FileSystem                    _fileSystem;
  private Path                          _outputPath;
  private int                           _id;
  private int                           _maxSize                 = MAX_SIZE_DEFAULT;
  private int                           _maxWriters              = MAX_WRITERS_DEFAULT;
  private Semaphore                     _maxWritersSemaphore     = null;
  private Vector<ArcFile>               _arcFiles                = new Vector<ArcFile>();
  private String                        _activeFileName          = null;
  private int                           _lastItemPos             = -1;
  private int                           _lastItemCompressedSize  = -1;
  private TreeMultimap<String, Integer> _mimeTypeCounts          = TreeMultimap
                                                                     .create();
  
  public static final String ARC_FILE_SUFFIX = ".arc.gz";

  private OutputStream                  _out                     = null;
  private static BitSet                 dontNeedEncoding;
  static final int                      caseDiff                 = ('a' - 'A');

  static {

    dontNeedEncoding = new BitSet(256);
    // alpha characters
    for (int i = 'a'; i <= 'z'; i++) {
      dontNeedEncoding.set(i);
    }
    for (int i = 'A'; i <= 'Z'; i++) {
      dontNeedEncoding.set(i);
    }
    // numeric characters
    for (int i = '0'; i <= '9'; i++) {
      dontNeedEncoding.set(i);
    }
    // special chars
    dontNeedEncoding.set('-');
    dontNeedEncoding.set('~');
    dontNeedEncoding.set('_');
    dontNeedEncoding.set('.');
    dontNeedEncoding.set('*');
    dontNeedEncoding.set('/');
    dontNeedEncoding.set('=');
    dontNeedEncoding.set('&');
    dontNeedEncoding.set('+');
    dontNeedEncoding.set(',');
    dontNeedEncoding.set(':');
    dontNeedEncoding.set(';');
    dontNeedEncoding.set('@');
    dontNeedEncoding.set('$');
    dontNeedEncoding.set('!');
    dontNeedEncoding.set(')');
    dontNeedEncoding.set('(');
    // experiments indicate: Firefox (1.0.6) never escapes '%'
    dontNeedEncoding.set('%');
    // experiments indicate: Firefox (1.0.6) does not escape '|' or '''
    dontNeedEncoding.set('|');
    dontNeedEncoding.set('\'');
  }

  private static class BufferItem {

    public BufferItem(ByteBuffer bufferItem) {
      _buffer = bufferItem;
    }

    public ByteBuffer _buffer;
  }

  private static final class ThreadSync extends AbstractQueuedSynchronizer {

    /**
     * 
     */
    private static final long serialVersionUID = 8771504638721679952L;

    ThreadSync() {
      setState(0);
    }

    int getCount() {
      return getState();
    }

    public int tryAcquireShared(int acquires) {
      return getState() == 0 ? 1 : -1;
    }

    public boolean tryReleaseShared(int releases) {
      // Decrement count; signal when transition to zero
      for (;;) {
        int c = getState();
        if (c == 0)
          return false;
        int nextc = c - 1;
        if (compareAndSetState(c, nextc))
          return nextc == 0;
      }
    }

    public void incrementCount() {

      // loop until we can atomically increment ...
      for (;;) {
        int c = getState();
        int nextc = c + 1;
        if (compareAndSetState(c, nextc))
          break;
      }
    }

  }

  private ThreadSync _activeWriterCount = new ThreadSync();

  private final class ArcFile implements NIODataSink {

    private Path                            _hdfsPath;
    private NIOBufferList                   _buffer                   = new NIOBufferList();
    private NIOBufferListOutputStream       _nioStream                = new NIOBufferListOutputStream(
                                                                          _buffer);
    private int                             _streamPos                = 0;
    public int                              _totalHeaderBytesWritten  = 0;
    public int                              _totalContentBytesWritten = 0;
    public int                              _itemsWritten             = 0;
    public int                              _compressedBytesWritten   = 0;
    private final ReentrantLock             queueLock                 = new ReentrantLock();

    private OutputStream                    _out                      = new FilterOutputStream(
                                                                          _nioStream) {

                                                                        @Override
                                                                        public void write(
                                                                            int b)
                                                                            throws IOException {
                                                                          ++_streamPos;
                                                                          _nioStream
                                                                              .write(b);
                                                                        }

                                                                        @Override
                                                                        public void write(
                                                                            byte[] b,
                                                                            int off,
                                                                            int len)
                                                                            throws IOException {
                                                                          _streamPos += len;
                                                                          _nioStream
                                                                              .write(
                                                                                  b,
                                                                                  off,
                                                                                  len);
                                                                        }
    };

    private LinkedBlockingQueue<BufferItem> _consumerQueue            = new LinkedBlockingQueue<BufferItem>();
    private LinkedList<BufferItem>          _rewindQueue              = new LinkedList<BufferItem>();
    private FSDataOutputStream              _hdfsStream               = null;
    private FileSystem                      _hdfs                     = null;
    private Thread                          _hdfsWriterThread         = null;
    private long                            _timestamp;
    // bytes consumed via Blocking Consumer interface ...
    int                                     _bytesConsumed            = 0;
    private boolean                         _abort                    = false;

    // failure exception ... if any ...
    private IOException                     _failureException         = null;

    private void restartWrite() throws IOException {
      LOG.info("Restarting Write of File:" + _hdfsPath);
      if (_hdfsStream != null) {
        LOG
            .warn("HDFSStream != NULL for File:" + _hdfsPath
                + " during restart");
        _hdfsStream.close();
        _hdfsStream = null;
      }

      LOG.info("REWIND - Deleting File :" + _hdfsPath);
      // delete existing ...
      _hdfs.delete(_hdfsPath,false);
      LOG.info("REWIND - ReCreating File :" + _hdfsPath);
      // create new file stream ...
      _hdfsStream = _hdfs.create(_hdfsPath);
      // lock queue
      try {
        queueLock.lock();

        ArrayList<BufferItem> itemList = new ArrayList<BufferItem>();
        LOG.info("REWIND - There are:" + _rewindQueue.size()
            + " Items in the Rewind Queue for File :" + _hdfsPath);
        itemList.addAll(_rewindQueue);
        LOG.info("REWIND - There are:" + _consumerQueue.size()
            + " Items in the Consumer Queue for File :" + _hdfsPath);
        _consumerQueue.drainTo(_rewindQueue);
        _consumerQueue.clear();

        int itemCount = 0;
        for (BufferItem bufferItem : itemList) {
          _consumerQueue.offer(bufferItem);
          itemCount++;
        }
        LOG.info("REWIND - There should be:" + itemCount
            + " Items in the Consumer Queue for File :" + _hdfsPath);
        _rewindQueue.clear();
      } finally {
        queueLock.unlock();
      }
    }

    public ArcFile(FileSystem fileSystem, Path arcFilePath, long timestamp)
        throws IOException {
      // first things first ... we need to acquire the writer semaphore ...
      _maxWritersSemaphore.acquireUninterruptibly();
      // increment thread count in parent class ...
      _activeWriterCount.incrementCount();
      // store hdfs filesystem reference ...
      _hdfs = fileSystem;
      // and the path to our arc file ...
      _hdfsPath = arcFilePath;
      // delete existing ...
      _hdfs.delete(_hdfsPath,false);
      // create new file stream ...
      _hdfsStream = _hdfs.create(_hdfsPath);
      // and setup the consumer queue relationship
      _buffer.setSink(this);
      // store timestamp that was used to create unique filename
      _timestamp = timestamp;

      // and finally start the blocking writer thread ...
      _hdfsWriterThread = new Thread(new Runnable() {

        public void run() {

          LOG.info("Writing File:" + _hdfsPath.toString());
          test: for (;;) {
            try {
              BufferItem item = _consumerQueue.take();

              // add item to rewind queue
              _rewindQueue.addLast(item);

              // if buffer item is null... this is considered an eof condition
              // ... break out ...
              if (item._buffer == null) {
                // LOG.info("Received Null BufferItem ... Shutting down File:" +
                // _hdfsPath.toString());
                // time to shutdown stream ...
                try {
                  _hdfsStream.flush();
                  _hdfsStream.close();
                  _hdfsStream = null;
                  break;
                } catch (IOException e) {
                  if (!_abort) {
                    LOG.error("Exception During Flush of File:" + _hdfsPath
                        + "(Restarting)  Exception:"
                        + CCStringUtils.stringifyException(e));
                    try {
                      _hdfsStream = null;
                      restartWrite();
                      continue test;
                    } catch (IOException e2) {
                      LOG.error("Restart of Stream:" + _hdfsPath.toString()
                          + " Failed with Exception:"
                          + CCStringUtils.stringifyException(e2));
                      _failureException = e2;
                      // break out of outer loop
                      break;
                    }
                  } else {
                    LOG.error("Aborting Operation for File:" + _hdfsPath);
                    break;
                  }
                }
              }
              // otherwise ... write the
              else {

                try {

                  int arrayOffset = item._buffer.arrayOffset();
                  arrayOffset += item._buffer.position();
                  int end = item._buffer.limit();
                  byte[] byteBuffer = item._buffer.array();

                  // LOG.info("Wrote:" + (end-arrayOffset) + "bytes for File:" +
                  // _hdfsPath.toString());
                  // write the buffer to disk ...
                  _hdfsStream.write(byteBuffer, arrayOffset, end - arrayOffset);

                } catch (IOException e) {
                  try {
                    _hdfsStream.close();
                  } catch (IOException e2) {
                    LOG.error("Ignoring Exception During Close:"
                        + CCStringUtils.stringifyException(e2));
                  } finally {
                    _hdfsStream = null;
                  }

                  if (!_abort) {
                    LOG.error("Exception During Write of File:" + _hdfsPath
                        + "(Restarting)  Exception:"
                        + CCStringUtils.stringifyException(e));
                    try {
                      restartWrite();
                      continue test;
                    } catch (IOException e2) {
                      LOG.error("Restart of Stream:" + _hdfsPath.toString()
                          + " Failed with Exception:"
                          + CCStringUtils.stringifyException(e2));
                      _failureException = e2;
                      // break out of outer loop
                      break;
                    }
                  } else {
                    LOG.error("Aborting Operation for File:" + _hdfsPath);

                    break;
                  }
                }
              }
            } catch (InterruptedException e) {

            }
          }

          LOG.info("Finished Writing File:" + _hdfsPath.toString()
              + ". Clearing Rewind Queue");
          _rewindQueue.clear();
          // release our reference to ourselves ...
          _hdfsWriterThread = null;
          // and release the semaphore ...
          _maxWritersSemaphore.release();
          // decrement the active thread count ...
          _activeWriterCount.releaseShared(1);
        }
      });
      // launch the writer thread ...
      _hdfsWriterThread.start();
    }

    public void available(ByteBuffer availableReadBuffer) {
      try {
        queueLock.lock();
        _consumerQueue.offer(new BufferItem(availableReadBuffer));
        _bytesConsumed += availableReadBuffer.remaining();
      } finally {
        queueLock.unlock();
      }
    }

    public void finished() {
      // NOOP
    }

    public void freeze() {
      // add empty buffer to consumer queue ... which will trigger writer thread
      // to flush and terminate ...
      _consumerQueue.offer(new BufferItem(null));
    }

    public OutputStream getOutputStream() {
      return _out;
    }

    public IOException getFailureException() {
      return _failureException;
    }

    public long getTimestamp() {
      return _timestamp;
    }

    /**
     * get the stream position (the number of bytes written to the output stream
     * (or file) )
     */
    public int getStreamPos() {
      return _streamPos;
    }

    /** get the estimated output file size **/
    public int getFileSize() {
      int fileSizeOut = 0;

      // pickup anything pending (uflushed) data ...
      ByteBuffer writeBuffer = _buffer.peekAtWriteBuffer();

      if (writeBuffer != null) {
        fileSizeOut += writeBuffer.capacity() - writeBuffer.remaining();
      }
      fileSizeOut += _bytesConsumed;

      return fileSizeOut;
    }

    public void flush() {
      _buffer.flush();
    }

    public void close() {
      if (_hdfsWriterThread != null) {
        throw new RuntimeException(
            "Arc File close called w/ writer thread still running ...!");
      }
      // ok ... either is called in a clean state or NOT in a clean state ...
      // if stream is open ... non-clean state ... close it ...
      if (_hdfsStream != null) {
        _abort = true;
        try {
          _hdfsStream.close();
          _hdfsStream = null;
        } catch (IOException e) {
          LOG.error(CCStringUtils.stringifyException(e));
        }
        // time to delete the underlying file since it is corrupt ...
        try {
          _hdfs.delete(_hdfsPath,false);
        } catch (IOException e) {
          LOG.error(CCStringUtils.stringifyException(e));
        }
        // and set error condition (if not already set)
        if (_failureException == null) {
          _failureException = new IOException(
              "ArcFile close called on file in improper state");
        }
      }
    }

    public void delete() {
      try {
        _hdfs.delete(_hdfsPath,false);
      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
      }
    }

  }

  /** Unit Test Constructor ***/
  public ArcFileWriter() throws IOException {

    if (CrawlEnvironment.getHadoopConfig() == null) {
      Configuration conf = new Configuration();

      conf.addResource("commoncrawl-default.xml");
      conf.addResource("commoncrawl-site.xml");

      CrawlEnvironment.setHadoopConfig(conf);
    }

    _fileSystem = CrawlEnvironment.getDefaultFileSystem();
    _outputPath = new Path("crawl/test");
    _id = 1;
    _maxWritersSemaphore = new Semaphore(_maxWriters);
    rotateFile();
  }

  /**
   * constructor for arc file writer *
   * 
   * @throws IOException
   */
  public ArcFileWriter(FileSystem fileSystem, Path outputPath, int writerId,
      int maxSimultaneousWriters) throws IOException {

    _fileSystem = fileSystem;
    _outputPath = outputPath;
    _id = writerId;
    _maxWriters = maxSimultaneousWriters;
    _maxWritersSemaphore = new Semaphore(_maxWriters);

    // set up the initial arc file .
    rotateFile();
  }

  @Test
  public void testArcFileWriter() throws Exception {

    Path crawlFilePath = new Path(
        "crawl/checkpoint_data/CrawlLog_cc08_1210918849380");

    WritableName.setName(CrawlURL.class, "org.crawlcommons.protocol.CrawlURL");

    SequenceFile.Reader reader = new SequenceFile.Reader(_fileSystem,
        crawlFilePath, CrawlEnvironment.getHadoopConfig());

    Text url = new Text();
    CrawlURL urlData = new CrawlURL();

    while (reader.next(url, urlData)) {

      NIOHttpHeaders headers = CrawlURLHelper.getHeadersFromCrawlURL(urlData);
      write(url.toString(), 1, 1, urlData, headers, "text/html", "test");
    }

    reader.close();
    this.close(false);
  }

  public ArcFileWriterStats close(boolean purgeOutput) throws IOException {

    ArcFileWriterStats statsOut = new ArcFileWriterStats();

    if (getActiveFile() != null) {
      LOG.info("Closing ArcFileWriter ... flushing active file");
      // flush any partial writes ...
      getActiveFile().flush();
      getActiveFile().freeze();
    }

    LOG.info("Generating Stats");
    // flush mime type stats
    for (Map.Entry<String, Integer> mimeTypeEntry : _mimeTypeCounts.entries()) {
      MimeTypeCount mimeTypeCount = new MimeTypeCount();
      mimeTypeCount.setMimeType(mimeTypeEntry.getKey());
      mimeTypeCount.setCount(mimeTypeEntry.getValue());
      statsOut.getMimeTypeCounts().add(mimeTypeCount);
    }
    _mimeTypeCounts.clear();

    SmoothedAverage itemsPerArcFileAvg = new SmoothedAverage(.25);
    for (ArcFile arcFile : _arcFiles) {
      statsOut.setArcFilesWritten(statsOut.getArcFilesWritten() + 1);
      statsOut.setTotalItemsWritten(statsOut.getTotalItemsWritten()
          + arcFile._itemsWritten);
      itemsPerArcFileAvg.addSample(arcFile._itemsWritten);
      statsOut.setHeaderBytesWritten(statsOut.getHeaderBytesWritten()
          + arcFile._totalHeaderBytesWritten);
      statsOut.setContentBytesWritten(statsOut.getContentBytesWritten()
          + arcFile._totalContentBytesWritten);
      statsOut.setCompressedBytesWritten(statsOut.getCompressedBytesWritten()
          + arcFile._compressedBytesWritten);
    }
    statsOut.setAverageItemsPerFile((float) itemsPerArcFileAvg.getAverage());

    LOG.info("Closing ArcFileWriter ... waiting for all writers to complete");
    // now wait for all arc files writes to finish ...
    _activeWriterCount.acquireShared(1);
    LOG.info("Closing ArcFileWriter ... all writers completed. closing files");

    IOException exceptionOut = null;

    // now walk arc files collecting any exceptions ...
    for (ArcFile arcFile : _arcFiles) {
      if (arcFile.getFailureException() != null) {
        exceptionOut = arcFile.getFailureException();
      }
      arcFile.close();
    }

    LOG.info("Closing ArcFileWriter ... close complete");

    if (purgeOutput) {
      LOG.info("Purging ArcFiles Due to Possible Error");
      for (ArcFile arcFile : _arcFiles) {
        arcFile.delete();
      }
    }
    _arcFiles.clear();

    if (exceptionOut != null)
      throw exceptionOut;

    return statsOut;
  }

  private String escapeURI(String uri, String charsetEncoding)
      throws IOException {

    boolean needToChange = false;

    StringBuffer out = new StringBuffer(uri.length());

    Charset charset;

    CharArrayWriter charArrayWriter = new CharArrayWriter();

    if (charsetEncoding == null)
      throw new NullPointerException("charsetName");

    try {
      charset = Charset.forName(charsetEncoding);
    } catch (IllegalCharsetNameException e) {
      throw new UnsupportedEncodingException(charsetEncoding);
    } catch (UnsupportedCharsetException e) {
      throw new UnsupportedEncodingException(charsetEncoding);
    }

    for (int i = 0; i < uri.length();) {
      int c = (int) uri.charAt(i);
      // System.out.println("Examining character: " + c);
      if (dontNeedEncoding.get(c)) {
        out.append((char) c);
        i++;
      } else {
        // convert to external encoding before hex conversion
        do {
          charArrayWriter.write(c);
          /*
           * If this character represents the start of a Unicode surrogate pair,
           * then pass in two characters. It's not clear what should be done if
           * a bytes reserved in the surrogate pairs range occurs outside of a
           * legal surrogate pair. For now, just treat it as if it were any
           * other character.
           */
          if (c >= 0xD800 && c <= 0xDBFF) {
            /*
             * System.out.println(Integer.toHexString(c) +
             * " is high surrogate");
             */
            if ((i + 1) < uri.length()) {
              int d = (int) uri.charAt(i + 1);
              /*
               * System.out.println("\tExamining " + Integer.toHexString(d));
               */
              if (d >= 0xDC00 && d <= 0xDFFF) {
                /*
                 * System.out.println("\t" + Integer.toHexString(d) +
                 * " is low surrogate");
                 */
                charArrayWriter.write(d);
                i++;
              }
            }
          }
          i++;
        } while (i < uri.length()
            && !dontNeedEncoding.get((c = (int) uri.charAt(i))));

        charArrayWriter.flush();
        String str = new String(charArrayWriter.toCharArray());
        byte[] ba = str.getBytes(charsetEncoding);
        for (int j = 0; j < ba.length; j++) {
          out.append('%');
          char ch = Character.forDigit((ba[j] >> 4) & 0xF, 16);
          // converting to use uppercase letter as part of
          // the hex value if ch is a letter.
          if (Character.isLetter(ch)) {
            ch -= caseDiff;
          }
          out.append(ch);
          ch = Character.forDigit(ba[j] & 0xF, 16);
          if (Character.isLetter(ch)) {
            ch -= caseDiff;
          }
          out.append(ch);
        }
        charArrayWriter.reset();
        needToChange = true;
      }
    }

    return (needToChange ? out.toString() : uri);
  }

  /**
   * write a url entry via the arc file writer NOTE: BY DESIGN this call could
   * BLOCK if the number of active writers exceeds the value specified by
   * maxSimultaneousWriters (in the constructor)
   * **/
  public boolean write(String normalizedURL, int segmentid, int crawlNumber,
      CrawlURL urlItem, NIOHttpHeaders headers, String contentType,
      String signature) throws IOException {

    boolean generatedARCFileContent = false;

    // String encodedURI = escapeURI(normalizedURL,UTF8);
    String encodedURI = normalizedURL;
    GoogleURL url = new GoogleURL(normalizedURL);
    if (url.isValid()) {
      encodedURI = url.getCanonicalURL();
    }

    int hostIP = urlItem.getServerIP();
    String hostIPStr = IPAddressUtils.IntegerToIPAddressString(hostIP);
    long fetchBeginTimestamp = urlItem.getLastAttemptTime();
    String encoding = headers.findValue("Content-Encoding");
    String truncationFlags = "";
    if ((urlItem.getFlags() & CrawlURL.Flags.TruncatedDuringDownload) != 0) {
      truncationFlags += ArcFileItem.Flags
          .toString(ArcFileItem.Flags.TruncatedInDownload);
    }

    byte[] crawlData = urlItem.getContentRaw().getReadOnlyBytes();
    int crawlDataLen = (crawlData != null) ? crawlData.length : 0;

    // validate content type ...
    if (contentType == null) {
      LOG.error("URL:" + normalizedURL + " Rejected - Invalid Content Type:"
          + contentType);
    } else {

      if (crawlData != null && encoding != null
          && encoding.equalsIgnoreCase("gzip")) {
        int compressedSize = crawlData.length;
        try {
          UnzipResult result = GZIPUtils.unzipBestEffort(crawlData,
              CrawlEnvironment.CONTENT_SIZE_LIMIT);

          crawlData = result.data.get();
          crawlDataLen = result.data.getCount();

          if (result.wasTruncated) {
            if (truncationFlags.length() != 0)
              truncationFlags += ",";
            truncationFlags += ArcFileItem.Flags
                .toString(ArcFileItem.Flags.TruncatedInInflate);
          }
        } catch (Exception e) {
          LOG.error("URL:" + normalizedURL
              + " Rejected - GZIP Decompression Failed");
          crawlData = null;
        }
      }

      // content must not be null
      if (crawlData == null) {
        LOG.error("URL:" + normalizedURL + " Rejected - Content is NULL");
      } else {

        // add in our custom headers ...
        headers.add(Constants.ARCFileHeader_ParseSegmentId,
            ((Integer) segmentid).toString());
        headers.add(Constants.ARCFileHeader_OriginalURL, normalizedURL);

        headers.add(Constants.ARCFileHeader_URLFP, Long.toString(urlItem
            .getFingerprint()));
        headers.add(Constants.ARCFileHeader_HostFP, Long.toString(urlItem
            .getHostFP()));
        headers.add(Constants.ARCFileHeader_Signature, signature);
        headers.add(Constants.ARCFileHeader_CrawlNumber, Integer
            .toString(crawlNumber));
        headers.add(Constants.ARCFileHeader_FetchTimeStamp, Long
            .toString(urlItem.getLastAttemptTime()));
        // headers.add(Environment.ARCFileHeader_CrawlerId,
        // Integer.toString((int)urlItem.get));

        if (truncationFlags.length() != 0) {
          headers
              .add(Constants.ARCFileHeader_ContentTruncated, truncationFlags);
        }

        String headerString = headers.toString() + "\r\n";

        byte[] headerBytes = headerString.getBytes("UTF-8");

        // content is truncated further upstream, so this redundant check /
        // truncation is problematic
        // int contentLength = Math.min(crawlData.length,CONTENT_SIZE_LIMIT);

        // extract metadata line upfront, since if the url exceeds a certain
        // size limit , we are going to reject the entry...
        byte metaDataLine[];

        try {
          metaDataLine = getMetaLine(encodedURI, contentType, hostIPStr,
              fetchBeginTimestamp, crawlDataLen + headerBytes.length).getBytes(
              UTF8);
        } catch (IOException e) {
          LOG.error("Metadata Line Validation FAILED with Exception:"
              + CCStringUtils.stringifyException(e));
          // bail here ...
          return false;
        }

        // get ready to write out a new gziped entry ...
        preWriteRecordTasks(headerBytes.length, crawlDataLen, contentType);
        try {
          // read to write an entry ...
          write(metaDataLine);

          // write out the headers ...
          write(headerBytes, 0, headerBytes.length);
          // write out the content
          write(crawlData, 0, crawlDataLen);
          // line separator ...
          write(LINE_SEPARATOR);

          // indicate success ...
          generatedARCFileContent = true;

        } finally {
          // flush the gzip stream...
          postWriteRecordTasks();
        }
      }
    }

    return generatedARCFileContent;
  }

  /**
   * 
   * @return timestamp of the current arc file
   */
  public long getActiveFileTimestamp() {
    return getActiveFile().getTimestamp();
  }

  /**
   * 
   * @return the position in the arc file of the last written item
   */
  public int getLastItemPos() {
    return _lastItemPos;
  }

  /**
   * 
   * @return the compressed size (within the arc file) of the last written item
   */
  public int getLastItemCompressedSize() {
    return _lastItemCompressedSize;
  }

  private ArcFile getActiveFile() {
    if (_arcFiles.size() != 0) {
      return _arcFiles.lastElement();
    }
    return null;
  }

  private static NIOHttpHeaders getHeadersFromString(String headers) {

    NIOHttpHeaders headersOut = new NIOHttpHeaders();

    StringTokenizer tokenizer = new StringTokenizer(headers, "\r\n");

    while (tokenizer.hasMoreElements()) {
      String token = tokenizer.nextToken();

      if (token != null && token.length() != 0) {
        int colonPos = token.indexOf(':');

        if (colonPos != -1 && colonPos != token.length() - 1) {

          String key = token.substring(0, colonPos);
          String value = token.substring(colonPos + 1);

          if (key.length() != 0 && value.length() != 0) {
            headersOut.add(key, value);
          }
        } else {
          headersOut.add(null, token);
        }

      }
    }
    return headersOut;
  }

  public static String getMetaLine(String uri, String contentType,
      String hostIP, long fetchBeginTimeStamp, long recordLength)
      throws IOException {

    if (fetchBeginTimeStamp <= 0) {
      throw new IOException("Bogus fetchBeginTimestamp: "
          + Long.toString(fetchBeginTimeStamp));
    }

    return createMetaline(uri, hostIP, TIMESTAMP14.format(new Date(
        fetchBeginTimeStamp)), contentType, Long.toString(recordLength));
  }

  public static String createMetaline(String uri, String hostIP,
      String timeStamp, String mimetype, String recordLength) {
    return uri + HEADER_FIELD_SEPARATOR + hostIP + HEADER_FIELD_SEPARATOR
        + timeStamp + HEADER_FIELD_SEPARATOR + mimetype
        + HEADER_FIELD_SEPARATOR + recordLength + LINE_SEPARATOR;
  }

  protected void rotateFile() throws IOException {

    if (getActiveFile() != null) {

      ArcFile activeFile = getActiveFile();

      // flush any partial writes ...
      activeFile.flush();
      // close it ...
      activeFile.freeze();

    }

    // generate a timestamp value ...
    long timestamp = System.currentTimeMillis();

    // create a new arc file based on path and timestamp
    _activeFileName = generateNewARCFilename(timestamp);

    // create arc file path ...
    Path arcFilePath = new Path(_outputPath, _activeFileName);
    // and create a new ArcFile object ...
    ArcFile newArcFile = new ArcFile(_fileSystem, arcFilePath, timestamp);
    // and make it the active arc file ...
    _arcFiles.add(newArcFile);
    // and set up output stream ...
    _out = newArcFile.getOutputStream();
    // and write out firt record ...
    writeFirstRecord(TIMESTAMP14.format(new Date(System.currentTimeMillis())));
  }

  private String generateNewARCFilename(long timestamp) {
    return timestamp + "_" + _id + ARC_FILE_SUFFIX;
    /*
     * Date date = new Date(timestamp); String arcFileName =
     * FILENAME_TIMESTAMP.format(date) + timestamp + "-" + _id + "arc.gz";
     * return arcFileName;
     */
  }

  private String getARCFilename() {
    return _activeFileName;
  }

  /**
   * Call this method just before/after any significant write.
   * 
   * Call at the end of the writing of a record or just before we start writing
   * a new record. Will close current file and open a new file if file size has
   * passed out maxSize.
   * 
   * <p>
   * Creates and opens a file if none already open. One use of this method then
   * is after construction, call this method to add the metadata, then call
   * {@link #getPosition()} to find offset of first record.
   * 
   * @exception IOException
   */
  private void checkSize(int headerBytesLength, int contentBytesLength)
      throws IOException {
    if (getActiveFile() == null
        || (_maxSize != -1 && (getActiveFile().getFileSize() > _maxSize))) {
      rotateFile();
    }
  }

  /**
   * append a pre-generated arcfile entry directly into the arc file writer
   * 
   * @param arcFileData
   *          - the compressed arc file entry
   * @param dataBufferLength
   *          - the entry length
   * @throws IOException
   */
  public void writeRawArcFileItem(String contentType, byte[] arcFileData,
      int dataBufferLength) throws IOException {
    // check to see if we need to start a new underlying file
    checkSize(0, dataBufferLength);
    // update stats
    getActiveFile()._totalContentBytesWritten += dataBufferLength;
    getActiveFile()._itemsWritten++;
    SortedSet<Integer> counts = _mimeTypeCounts.get(contentType);
    if (counts.size() == 0) {
      counts.add(1);
    } else {
      int count = counts.first() + 1;
      counts.clear();
      counts.add(count);
    }
    // record start position of this item
    _lastItemPos = getActiveFile().getFileSize();
    // write out data
    _out.write(arcFileData, 0, dataBufferLength);
    // record size of last item
    _lastItemCompressedSize = (getActiveFile().getFileSize() - _lastItemPos);
    // update stats
    getActiveFile()._compressedBytesWritten += _lastItemCompressedSize;
  }

  private void preWriteRecordTasks(int headerBytesLength,
      int contentBytesLength, String contentType) throws IOException {

    checkSize(headerBytesLength, contentBytesLength);

    // update stats
    getActiveFile()._totalHeaderBytesWritten += headerBytesLength;
    getActiveFile()._totalContentBytesWritten += contentBytesLength;
    getActiveFile()._itemsWritten++;
    SortedSet<Integer> counts = _mimeTypeCounts.get(contentType);
    if (counts.size() == 0) {
      counts.add(1);
    } else {
      int count = counts.first() + 1;
      counts.clear();
      counts.add(count);
    }

    // record start position of this item
    _lastItemPos = getActiveFile().getFileSize();

    // Wrap stream in GZIP Writer.
    // The below construction immediately writes the GZIP 'default'
    // header out on the underlying stream.
    _out = new CompressedStream(_out);
  }

  private void postWriteRecordTasks() throws IOException {
    CompressedStream o = (CompressedStream) _out;
    o.finish();
    o.flush();
    o.end();
    _out = o.getWrappedStream();
    // record size of last item
    _lastItemCompressedSize = (getActiveFile().getFileSize() - _lastItemPos);
    // update stats
    getActiveFile()._compressedBytesWritten += _lastItemCompressedSize;
  }

  private void write(final byte[] b, int offset, int size) throws IOException {
    _out.write(b, offset, size);
  }

  private void write(final byte[] b) throws IOException {
    _out.write(b);
  }

  private void write(int b) throws IOException {
    _out.write(b);
  }

  private void writeFirstRecord(final String ts) throws IOException {
    write(generateARCFileMetaData(ts));
  }

  /**
   * An override so we get access to underlying output stream and offer an end()
   * that does not accompany closing underlying stream.
   * 
   * @author stack
   */
  public static class CompressedStream extends GZIPOutputStream {
    public CompressedStream(OutputStream out) throws IOException {
      super(out);
    }

    /**
     * @return Reference to stream being compressed.
     */
    OutputStream getWrappedStream() {
      return this.out;
    }

    /**
     * Release the deflater's native process resources, which otherwise would
     * not occur until either finalization or DeflaterOutputStream.close()
     * (which would also close underlying stream).
     */
    public void end() {
      def.end();
    }
  }

  /**
   * Gzip passed bytes. Use only when bytes is small.
   * 
   * @param bytes
   *          What to gzip.
   * @return A gzip member of bytes.
   * @throws IOException
   */
  private static byte[] gzip(byte[] bytes) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    GZIPOutputStream gzipOS = new GZIPOutputStream(baos);
    gzipOS.write(bytes, 0, bytes.length);
    gzipOS.close();
    return baos.toByteArray();
  }

  private byte[] generateARCFileMetaData(String date) throws IOException {

    String metadataHeaderLinesTwoAndThree = getMetadataHeaderLinesTwoAndThree("1 "
        + "0");
    int recordLength = metadataHeaderLinesTwoAndThree
        .getBytes(DEFAULT_ENCODING).length;
    String metadataHeaderStr = ARC_MAGIC_NUMBER + getARCFilename()
        + " 0.0.0.0 " + date + " text/plain " + recordLength
        + metadataHeaderLinesTwoAndThree;

    ByteArrayOutputStream metabaos = new ByteArrayOutputStream(recordLength);

    // Write the metadata header.
    metabaos.write(metadataHeaderStr.getBytes(DEFAULT_ENCODING));
    // Write out a LINE_SEPARATORs to end this record.
    metabaos.write(LINE_SEPARATOR);

    // Now get bytes of all just written and compress if flag set.
    byte[] bytes = metabaos.toByteArray();

    // GZIP the header but catch the gzipping into a byte array so we
    // can add the special IA GZIP header to the product. After
    // manipulations, write to the output stream (The JAVA GZIP
    // implementation does not give access to GZIP header. It
    // produces a 'default' header only). We can get away w/ these
    // maniupulations because the GZIP 'default' header doesn't
    // do the 'optional' CRC'ing of the header.

    byte[] gzippedMetaData = gzip(bytes);

    if (gzippedMetaData[3] != 0) {
      throw new IOException("The GZIP FLG header is unexpectedly "
          + " non-zero.  Need to add smarter code that can deal "
          + " when already extant extra GZIP header fields.");
    }

    // Set the GZIP FLG header to '4' which says that the GZIP header
    // has extra fields. Then insert the alex {'L', 'X', '0', '0', '0,
    // '0'} 'extra' field. The IA GZIP header will also set byte
    // 9 (zero-based), the OS byte, to 3 (Unix). We'll do the same.
    gzippedMetaData[3] = 4;
    gzippedMetaData[9] = 3;

    byte[] assemblyBuffer = new byte[gzippedMetaData.length
        + ARC_GZIP_EXTRA_FIELD.length];
    // '10' in the below is a pointer past the following bytes of the
    // GZIP header: ID1 ID2 CM FLG + MTIME(4-bytes) XFL OS. See
    // RFC1952 for explaination of the abbreviations just used.
    System.arraycopy(gzippedMetaData, 0, assemblyBuffer, 0, 10);
    System.arraycopy(ARC_GZIP_EXTRA_FIELD, 0, assemblyBuffer, 10,
        ARC_GZIP_EXTRA_FIELD.length);
    System.arraycopy(gzippedMetaData, 10, assemblyBuffer,
        10 + ARC_GZIP_EXTRA_FIELD.length, gzippedMetaData.length - 10);
    bytes = assemblyBuffer;

    return bytes;
  }

  private String getMetadataHeaderLinesTwoAndThree(String version) {
    StringBuffer buffer = new StringBuffer();
    buffer.append(LINE_SEPARATOR);
    buffer.append(version);
    buffer.append(" CommonCrawl");
    buffer.append(LINE_SEPARATOR);
    buffer.append("URL IP-address Archive-date Content-type Archive-length");
    buffer.append(LINE_SEPARATOR);
    return buffer.toString();
  }

  private static String truncateMimeType(String contentType) {
    if (contentType == null) {
      contentType = NO_TYPE_MIMETYPE;
    } else {
      Matcher matcher = TRUNCATION_REGEX.matcher(contentType);
      if (matcher.matches()) {
        contentType = matcher.group(1);
      } else {
        contentType = NO_TYPE_MIMETYPE;
      }
    }

    return contentType;
  }

  /**
   * Test that the metadata line is valid before writing.
   * 
   * @param metaLineStr
   * @throws IOException
   * @return The passed in metaline.
   */
  protected String validateMetaLine(String metaLineStr) throws IOException {
    if (metaLineStr.length() > MAX_METADATA_LINE_LENGTH) {
      throw new IOException("Metadata line length is " + metaLineStr.length()
          + " which is > than maximum " + MAX_METADATA_LINE_LENGTH);
    }
    Matcher m = METADATA_LINE_PATTERN.matcher(metaLineStr);
    if (!m.matches()) {
      throw new IOException("Metadata line doesn't match expected"
          + " pattern: " + metaLineStr);
    }
    return metaLineStr;
  }

}
