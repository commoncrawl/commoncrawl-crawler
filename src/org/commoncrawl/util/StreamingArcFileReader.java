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

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.SequenceInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;
import java.util.zip.ZipException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.record.Buffer;
import org.apache.hadoop.util.StringUtils;
import org.commoncrawl.crawl.common.shared.Constants;
import org.commoncrawl.io.NIODataSink;
import org.commoncrawl.protocol.shared.ArcFileHeaderItem;
import org.commoncrawl.protocol.shared.ArcFileItem;



/**
 * 
 * StreamingArcFileDecoder - Decoder capable of extracting ArcFileItem(s) from an ARC file in a non-blocking, streaming 
 * manner.
 * 
 * @author rana
 *
 */
public final class StreamingArcFileReader implements NIODataSink  {
 
  //////////////////////////////////////////////////////////////////////////////////
  // data members 
  //////////////////////////////////////////////////////////////////////////////////

  /** logging **/
  private static final Log LOG = LogFactory.getLog(StreamingArcFileReader.class);
  
  /** max expected arc header size **/
  private static final int MAX_ARCHEADER_SIZE = 4096;
  
  /** block size used for various operations **/
  private static final int BLOCK_SIZE = 32 * 1024;
  
  /** internal ByteBuffer wrapper for queuing byte buffers **/ 
  private static final class BufferItem { 
    
    public BufferItem(ByteBuffer bufferItem) { 
      _buffer = bufferItem;
    }
    public ByteBuffer _buffer;
  };
  


  /** blocking consumer queue **/
  private LinkedBlockingQueue<BufferItem> _consumerQueue = new LinkedBlockingQueue<BufferItem>();
  /** current data available */
  private int _bytesAvailable = 0;
  
  /** 32 bit crc  **/
  private CRC32 _crc = new CRC32();
  /** End Of Stream Indicator **/
  private boolean _eosReached = false;
  /** arc file header accumulator **/
  private byte[] _arcFileHeader = new byte[MAX_ARCHEADER_SIZE];
  /** arc file header size **/
  private int     _arcFileHeaderSize = 0;
  /** input streams **/
  private InputStream _rawInput = null;
  private CheckedInputStream _checkedInput = null;
  /** content bytes read counter **/
  private int _contentBytesRead = 0;
  /** inflater object **/
  private Inflater _inflater = new Inflater(true);
  /** the active input buffer **/
  private ByteBuffer _activeInputBuffer = null;
  /** flag indicating that this arc file has a header item **/
  private boolean _hasHeaderItem = true;
  
  long _streamPos = 0;
  long _arcFileStartOffset;

  
  private final static int FixedHeaderBytes = 2 + 1 + 1 + 6;
  
  
  
  enum ReadState { 
    ReadingArcHeader,
    ReadingArcHeaderData,
    ReadingArcHeaderTrailer,
    ReadingEntryHeader,
    ReadingEntryData,
    ReadingEntryTrailer,
    Done
    
  }

  enum HeaderReadState { 
    ReadingFixedHeader,
    ReadingFlagValues
  }
  
  ReadState _readState = ReadState.ReadingArcHeader;
  ArcFileBuilder _builder = null;
  HeaderReadState _headerReadState = HeaderReadState.ReadingFixedHeader;
  int _headerFlags = 0;
  int _headerExtraBytes = -1;
  
  
  //////////////////////////////////////////////////////////////////////////////////
  // public API
  //////////////////////////////////////////////////////////////////////////////////

  
  /** 
   * Costructs a new StreamingArcFileReader object
   * 
   */
  
  
  public StreamingArcFileReader(boolean hasArcFileHeader) { 
    
    // setup the proper stream...
    _rawInput = new InputStream() {

      byte oneByteArray[] = new byte[1];
      
      @Override 
      public synchronized int available() throws IOException {
        return _bytesAvailable;
      }
      
      
      @Override
      public int read() throws IOException {
        if (read(oneByteArray,0,1) != -1) {
        	_streamPos++;
          return oneByteArray[0] & 0xff;
        }
        return -1;
      } 
      
      @Override
      public int read(byte b[], int off, int len) throws IOException {
        if (_activeInputBuffer == null || _activeInputBuffer.remaining() == 0) {
          
          _activeInputBuffer = null;
          
          BufferItem nextItem = null;
          
          try {
            if (_consumerQueue.size() != 0) { 
              nextItem = _consumerQueue.take();
            }
          } catch (InterruptedException e) {
          }
          
          if (nextItem != null) { 
            if (nextItem._buffer == null) { 
              return -1;
            }
            else { 
              _activeInputBuffer = nextItem._buffer;
            }
          }
        }
        
        if (_activeInputBuffer != null || _activeInputBuffer.remaining() != 0) { 
          
          final int sizeAvailable = _activeInputBuffer.remaining();
          final int sizeToRead    = Math.min(sizeAvailable,len);
          
          _activeInputBuffer.get(b, off, sizeToRead);
          
          _streamPos += sizeToRead;
          
          synchronized(this) { 
            _bytesAvailable -= sizeToRead;
          }
       
          return sizeToRead;
        }
        else { 
          return 0;
        }
      }
    };  
    
    _checkedInput = new CheckedInputStream(_rawInput,_crc);
    if (!hasArcFileHeader) { 
    	_readState = ReadState.ReadingEntryHeader;
    }
  }

  /**
   * Reset all interal variables and get the Reader ready to process a new ArcFile
   */
  public void resetState() {
    _readState = ReadState.ReadingArcHeader;
    _builder = null;
    _headerReadState = HeaderReadState.ReadingFixedHeader;
    _headerFlags = 0;
    _headerExtraBytes = -1;
    _activeInputBuffer = null;
    _consumerQueue.clear();
    _crc.reset();
    _eosReached = false;
    _bytesAvailable = 0;
    resetInflater();
  }
  
  /** indicate whether this arc file has a header item **/
  public void setArcFileHasHeaderItemFlag(boolean value) { 
    _hasHeaderItem = value;
  }

  
  enum TriStateResult { 
    NeedsMoreData,
    MoreItems,
    NoMoreItems,
  }
  
  /**
   * Checks to see if additional ArcFileItems can be extracted from the current ARC File Stream
   * NON-BLOCKING version. 
   * @return TriStateResult.MoreItems if more items can be decoded from the stream,
   *              TriStateResult.NoMoreItems if we have reached the end of this stream,
   *              TriStateResult.NeedsMoreData if the decoder needs more data to determine next valid state 
   * @throws IOException if an error occurs processing ARC file data
   */
  public synchronized TriStateResult hasMoreItems() throws IOException {
    synchronized (this) { 
      // if data is still queued in the buffer ... 
      if (_bytesAvailable != 0) { 
        // then this means we have more items to process ... 
        return TriStateResult.MoreItems;
      }
      else {
        // otherwise if eos stream indicator is set ... 
        if (_eosReached) { 
          // set appropriate state   
          _readState = ReadState.Done;
          // and return nomore items
          return TriStateResult.NoMoreItems;
        }
        else { 
          return TriStateResult.NeedsMoreData;
        }
      }
    }
  }

  
  /**
   * Attempts to deflate and read the next ArcFileItem from bytes available - NON-BLOCKING version
   * 
   * @return Fully constructed ArcFileItem or NULL if not enough data is available to service the request
   * @throws EOFException if end of stream is reached decoding item, or generic IOException if a corrupt stream is detected
   */
  public ArcFileItem getNextItem() throws IOException { 
    
    // check state ... 
    if (_readState.ordinal()  <= ReadState.ReadingArcHeaderTrailer.ordinal()) {
      if (_hasHeaderItem) { 
        if (readARCHeader()) { 
          _crc.reset();
          _readState = ReadState.ReadingEntryHeader;
        }
      }
      else {
        // skip arc header
        _readState = ReadState.ReadingEntryHeader;
      }
    }
    
    // if reading header for entry 
    if (_readState == ReadState.ReadingEntryHeader) { 
      if (readHeader()) { 
        _readState = ReadState.ReadingEntryData;
        // reset crc accumulator 
        _crc.reset();
        // and allocate a fresh builder object ..
        _builder = new ArcFileBuilder();
      }
    }
    
    // if reading data for entry ... 
    if (_readState == ReadState.ReadingEntryData) { 

      // read header line buffer
      for(;;) { 
  
        byte scanBuffer[] = new byte[BLOCK_SIZE];
        ByteBuffer byteBuffer = ByteBuffer.wrap(scanBuffer);
        
        // read up to scan buffer size of data ... 
        int readAmount = readInflatedBytes(scanBuffer,0,scanBuffer.length);
        
        // if we did not read any bytes ... return immediately ... 
        if (readAmount == 0) { 
          return null;
        }
        else if (readAmount != -1) { 
          // update crc value ... 
          _crc.update(scanBuffer,0,readAmount);
          // update content bytes read 
          _contentBytesRead += readAmount;
          // and setup buffer pointers ... 
          byteBuffer.position(0);
          byteBuffer.limit(readAmount);
          // and input data into builder ... 
          _builder.inputData(byteBuffer);
        }
        // -1 indicates eos 
        else {
          // reset inflater ... 
          resetInflater();
          // and transition to reading trailing bytes 
          _readState = ReadState.ReadingEntryTrailer;
          break;
        }
      }
    }
    
    if (_readState == ReadState.ReadingEntryTrailer) { 
      // validate crc and header length ... 
      if (readTrailer()) { 
        
        // transition to assumed state ... 
        _readState = ReadState.ReadingEntryHeader;
        
        // get the arc file item 
        ArcFileItem itemOut = _builder.finish();
        
        itemOut.setArcFilePos((int)_arcFileStartOffset);
        
        // reset builder 
        _builder = null;
        //reset crc
        _crc.reset();
        
        // if no more data coming down the pipe...
        if (_rawInput.available() == 0 && _eosReached) { 
          // transition to done state ...
          _readState = ReadState.Done;
        }
        return itemOut;
      }
    }
    return null;
  }
    
  
  /**
   * NIODataSink method - called by implementor when all ARC File data has been exhauseted
   * 
   */
  public void finished() {
    _consumerQueue.add(new BufferItem(null));
    _eosReached = true;
  }

  /**
   * NIODataSink method - called by the implementor to queue up compressed ARC File data for processing 
   */
  public void available(ByteBuffer availableReadBuffer) {
    _consumerQueue.add(new BufferItem(availableReadBuffer));
    
    synchronized(this) { 
      _bytesAvailable += availableReadBuffer.remaining();      
    }
  }

  //////////////////////////////////////////////////////////////////////////////////
  // internal helpers 
  //////////////////////////////////////////////////////////////////////////////////
  
  
  
  private  void resetInflater() {
    _inflater.reset();
  }

  private int readInflatedBytes(byte[] b,int off,int len)throws IOException { 
    if (b == null) {
        throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > b.length - off) {
        throw new IndexOutOfBoundsException();
    } else if (len == 0) {
        return 0;
    }
    try {
        //try to output some bytes from the inflater 
        int n;
        while ((n = _inflater.inflate(b, off, len)) == 0) {
          if (_inflater.finished() || _inflater.needsDictionary()) {
            // these are EOS conditions 
            
            //first reclaim any remaining data from the inflater ... 
            if (_inflater.getRemaining() != 0) { 
              if (_activeInputBuffer == null) { 
                throw new RuntimeException("Bad State");
              }
              else { 
                // increment bytes available ... 
                synchronized(this) {
                  _bytesAvailable += _inflater.getRemaining();
                  _streamPos -= _inflater.getRemaining();
                }
                // and reposition cursor ...
                _activeInputBuffer.position(_activeInputBuffer.position() - _inflater.getRemaining());
              }
            }
            // b
            return -1;
          }
          // we produced no output .. check to see if have more input to add 
          if (_inflater.needsInput()) {
            if (_activeInputBuffer == null || _activeInputBuffer.remaining() == 0) {
              
              _activeInputBuffer = null;
              
              if (_consumerQueue.size() != 0) { 
                BufferItem nextItem = null;
                try {
                  nextItem = _consumerQueue.take();
                } catch (InterruptedException e) {
                  LOG.error(StringUtils.stringifyException(e));
                }
                if (nextItem._buffer == null) { 
                  throw new EOFException();
                }
                else { 
                  _activeInputBuffer = nextItem._buffer;
                }
              }
            }
            if (_activeInputBuffer == null) { 
              return 0;
            }
            else {
              // feed the buffer to the inflater ...
              _inflater.setInput(_activeInputBuffer.array(), _activeInputBuffer.position(), _activeInputBuffer.remaining());
              // decrement bytes available ... 
              synchronized(this) {
                _bytesAvailable -= _activeInputBuffer.remaining();
                _streamPos += _activeInputBuffer.remaining();
              }
              // and advance its position so 
              _activeInputBuffer.position(_activeInputBuffer.position() + _activeInputBuffer.remaining());
            }
          }
        }
        
        return n;
    } catch (DataFormatException e) {
        String s = e.getMessage();
        throw new ZipException(s != null ? s : "Invalid ZLIB data format");
    }
  }
  
  
  private boolean readARCHeader() throws IOException { 
    
    if (_readState == ReadState.ReadingArcHeader) { 
      
      if (readHeader()) { 
      		LOG.info("*** Found Fixed Header. Reading Metadata");
          // reset crc here... 
          _crc.reset();
          // and transition to reading data state ..
          _readState = ReadState.ReadingArcHeaderData;
      }
    }
    
    if (_readState == ReadState.ReadingArcHeaderData) { 

      int readAmount = 0;

      while ((readAmount = readInflatedBytes(_arcFileHeader,_arcFileHeaderSize,_arcFileHeader.length - _arcFileHeaderSize)) > 0) {
      	LOG.info("*** Read:" + readAmount + " Metadata Bytes");
        // update crc ... 
        _crc.update(_arcFileHeader,_arcFileHeaderSize,_arcFileHeaderSize + readAmount);
        // increment content bytes read ... 
        _contentBytesRead += readAmount;
        // and update length ...
        _arcFileHeaderSize += readAmount;
      }
      
      if (_arcFileHeaderSize == MAX_ARCHEADER_SIZE) { 
        throw new IOException("Invalid ARC File Header. Exceeded Arc File Header Size:" + _arcFileHeaderSize);
      }
      else if (readAmount == -1) {
      	
      	LOG.info("*** ARC File Header Size is:" + _arcFileHeaderSize);
        // reached eos ...
        // reset inflater
        resetInflater();
        // go to next state
        _readState = ReadState.ReadingArcHeaderTrailer;
      }
    }
    
    if (_readState == ReadState.ReadingArcHeaderTrailer) { 
      // read trailing bytes in gzip stream ...   
      if (readTrailer()) { 
        return true;
      }
    }
    return false;
  }
 
  /**
   * GZIP Code derived from GZIPInputStream code
   */
  
  // GZIP header magic number.
  public final static int GZIP_MAGIC = 0x8b1f;

  /*
   * File header flags.
   */
  private final static int FHCRC  = 2;  // Header CRC
  private final static int FEXTRA = 4;  // Extra field
  private final static int FNAME  = 8;  // File name
  private final static int FCOMMENT = 16; // File comment


    
  /*
   * Reads GZIP member header.
   */
  private boolean readHeader() throws IOException {
    
    if (_rawInput.available() == 0 && _eosReached) { 
      throw new EOFException();
    }
    
    
   
    switch (_headerReadState) { 
    
      case ReadingFixedHeader: { 
        
        if (_rawInput.available() >= FixedHeaderBytes ) {
          
        	_arcFileStartOffset = _streamPos;
        	
          // reset crc accumulator first ...
          _crc.reset();
          // reset content bytes read counter ..
          _contentBytesRead = 0;
          
          // Check header magic
          if (readUShort(_checkedInput) != GZIP_MAGIC) {
              throw new IOException("Not in GZIP format");
          }
          // Check compression method
          if (readUByte(_checkedInput) != 8) {
              throw new IOException("Unsupported compression method");
          }
          // Read flags
          _headerFlags = readUByte(_checkedInput);
          // Skip MTIME, XFL, and OS fields
          skipBytes(_checkedInput, 6);
          
          _headerReadState = HeaderReadState.ReadingFlagValues;
        }
        else { 
          break;
        }
      }
      
      case ReadingFlagValues: { 
        
        boolean advanceToNext = true;
        
        // Skip optional extra field
        if ((_headerFlags & FEXTRA) == FEXTRA) {
          advanceToNext = false;
          if (_headerExtraBytes == -1) {   
            if (_checkedInput.available() >= 2) { 
              _headerExtraBytes = readUShort(_checkedInput);
            }
          }
          if (_headerExtraBytes != -1) { 
            if (_checkedInput.available() >= _headerExtraBytes) { 
              // skip the requisite bytes 
              skipBytes(_checkedInput, _headerExtraBytes);
              // mask out current flag value ... 
              _headerFlags &= ~FEXTRA;
              // set advanceToNext flag
              advanceToNext = true;
            }
          }
        }
        
        while (advanceToNext && (_headerFlags & (FNAME|FCOMMENT)) != 0) { 
          
          int activeFlag = FCOMMENT;
          
          if ((_headerFlags & FNAME) == FNAME)
            activeFlag = FNAME;
            
          advanceToNext = false;
          
          while (_checkedInput.available() != 0) {
            // keep scanning for null terminator 
            if (readUByte(_checkedInput) == 0) { 
              _headerFlags &= ~activeFlag;
              advanceToNext = true;
            }
          }
        }
        
        if (advanceToNext && (_headerFlags & FHCRC) == FHCRC) { 
          if (_checkedInput.available() >= 2) { 
            int v = (int)_crc.getValue() & 0xffff;
            if (readUShort(_checkedInput) != v) {
              throw new IOException("Corrupt GZIP header");
            }
            _headerFlags &= ~FHCRC;
          }
        }
        
        if (_headerFlags == 0 && _headerReadState == HeaderReadState.ReadingFlagValues) {
          //reset header state variables... 
          _headerReadState = HeaderReadState.ReadingFixedHeader;
          _headerFlags = 0;
          _headerExtraBytes = -1;
          return true;
        }
      }
      break;
    }
    return false;
  }

  
  private static int ZIPTraierBytes = 8;
  /*
   * Reads GZIP member trailer.
   */
  private boolean readTrailer() throws IOException {
    
    if (_rawInput.available() >= ZIPTraierBytes) { 
      // Uses left-to-right evaluation order
      if ((readUInt(_rawInput) != _crc.getValue()) ||
      // rfc1952; ISIZE is the input size modulo 2^32
      (readUInt(_rawInput) != _contentBytesRead))
        throw new IOException("Corrupt GZIP trailer");
      
      return true;

    }
    
    return false;
  }
  
  
  /*
   * Reads unsigned integer in Intel byte order.
   */
  private static long readUInt(InputStream in) throws IOException {
    long s = readUShort(in);
    return ((long)readUShort(in) << 16) | s;
  }  
  
  /*
   * Reads unsigned short in Intel byte order.
   */
  private static int readUShort(InputStream in) throws IOException {
    int b = readUByte(in);
    return ((int)readUByte(in) << 8) | b;
  }

  /*
   * Reads unsigned byte.
   */
  private static int readUByte(InputStream in) throws IOException {
    int b = in.read();
    if (b == -1) {
      throw new EOFException();
    }
    if (b < -1 || b > 255) {
      // Report on this.in, not argument in; see read{Header, Trailer}.
      throw new IOException("read() returned value out of range -1..255: " + b);
      }
    return b;
  }
  
  private byte[] tmpbuf = new byte[128];

  
  /*
   * Skips bytes of input data blocking until all bytes are skipped.
   * Does not assume that the input stream is capable of seeking.
   */
  private void skipBytes(InputStream in, int n) throws IOException {
    while (n > 0) {
      int len = in.read(tmpbuf, 0, n < tmpbuf.length ? n : tmpbuf.length);
      if (len == -1) {
        throw new EOFException();
      }
      n -= len;
    }
  }  
  
  
  /**
   * 
   * ArcFileBuilder helper class - used to construct ArcFileItem objects from an ARC File Entry in a stateful manner
   *
   */
  private static class ArcFileBuilder {
    
    //various states of processing an ARC FILE
    private enum State { 
      LookingForMetadata,
      LookingForHeaderTerminator,
      ReadingContent
    }
    
    // ARC FILE HEADER TIMESTAMP FORMAT 
    SimpleDateFormat TIMESTAMP14 = new SimpleDateFormat("yyyyMMddHHmmss");
    // ArcFileItem this builder returns
    ArcFileItem _item = new ArcFileItem();
    // underlying content buffer 
    Buffer  _content = new Buffer();
    // Builder State
    State _state = State.LookingForMetadata;
    // Queued Input State
    LinkedList<ByteBuffer> _buffers = new LinkedList<ByteBuffer>();
    // Active Input Buffer 
    ByteBuffer _activeBuffer = null;
    // Pattern Buffer - for capturing termination patterns
    byte patternBuffer[] = new byte[4];
    // Captured Pattern Length
    int patternSize = 0;
    // End Of Stream Indicator
    boolean eos = false;
    // Charsets used during decoding process
    static Charset UTF8_Charset = Charset.forName("UTF8");
    static Charset ASCII_Charset = Charset.forName("ASCII");
    
    
    /** check for terminator pattern **/
    private boolean checkForTerminator() {
      
      boolean terminatorFound = false;
      
      switch (_state) { 
        // metadata line is terminated by a single line-feed
        case LookingForMetadata: { 
          if (patternBuffer[0] == '\n') { 
            terminatorFound = true;
          }
        }
        break;
        
        // http headers are terminated by the standard crlf-crlf pattern
        case LookingForHeaderTerminator: { 
          if (patternSize == 4 
              && patternBuffer[0] == '\r'
                && patternBuffer[1] == '\n'
                  && patternBuffer[2] == '\r'
                    && patternBuffer[3] == '\n') { 
            terminatorFound = true;
          }
        }
        break;
      }
      
      if (terminatorFound) { 
        
        // if active buffer contains no more characters... 
        if (_activeBuffer.remaining() == 0) { 
          // add entire active buffer to input state 
          _activeBuffer.rewind();
          _buffers.addLast(_activeBuffer);
          _activeBuffer = null;
        }
        else { 
          // otherwise, slice buffer at current position, and
          // add one buffer to input state, and make the other current
          ByteBuffer oldBuffer = _activeBuffer;
          _activeBuffer = _activeBuffer.slice();
          oldBuffer.limit(oldBuffer.position());
          oldBuffer.rewind();
          _buffers.addLast(oldBuffer);
        }
      }
      return terminatorFound;
    }
    
    /**newInputStream
     * 
     * @param buf - ByteBuffer to wrap as an InputStream
     * @return InputStream - wrapped InputStream object 
     */
    private static InputStream newInputStream(final ByteBuffer buf) {
        return new InputStream() {
            public synchronized int read() throws IOException {
                if (!buf.hasRemaining()) {
                    return -1;
                }
                return buf.get();
            }
    
            public synchronized int read(byte[] bytes, int off, int len) throws IOException {
                // Read only what's left
                len = Math.min(len, buf.remaining());
                buf.get(bytes, off, len);
                return len;
            }
        };
    }
    
    /** construct a reader given a list of ByteBuffers **/
    private static InputStreamReader readerFromScanBufferList(LinkedList<ByteBuffer> buffers, Charset charset) throws IOException { 
      Vector<InputStream> inputStreams = new Vector<InputStream>();
      
      for (ByteBuffer buffer : buffers) { 
        inputStreams.add(newInputStream(buffer));
      }
      buffers.clear();
      
      SequenceInputStream seqInputStream = new SequenceInputStream(inputStreams.elements());;
     
      return new InputStreamReader(seqInputStream,charset);
    }
    
    /** construct a single line from the current input state **/
    private String readLine(Charset charset) throws IOException {
      
      BufferedReader reader = new BufferedReader(readerFromScanBufferList(_buffers, charset));
      
      return reader.readLine();
    }
    
    
    /** process the metadata line of an ARC File Entry **/
    private void processMetadataLine(String metadata)throws IOException {
      
      //LOG.info("Metadata line is:" + metadata);
      StringTokenizer tokenizer = new StringTokenizer(metadata," ");
      int tokenCount = 0;
      while (tokenizer.hasMoreElements() && tokenCount <=5) { 
        switch (++tokenCount) { 
          
          // URI
          case 1: { 
            _item.setUri(tokenizer.nextToken());
          }
          break;
          
          // Host IP Address
          case 2: { 
            _item.setHostIP(tokenizer.nextToken());
          }
          break;
          
          // Timestamp
          case 3: { 
            String timestamp = tokenizer.nextToken();
            try {
              _item.setTimestamp(TIMESTAMP14.parse(timestamp).getTime());
            } catch (ParseException e) {
              LOG.error(StringUtils.stringifyException(e));
              throw new IOException("Invalid Timestamp in Metadata");
            }
            catch (NumberFormatException e) { 
              LOG.error("Number Format Exception Parsing Metadata Line:" + metadata + " TimeStamp:" + timestamp);
              throw e;
            }
          }
          break;
          
          // MimeType
          case 4: { 
            _item.setMimeType(tokenizer.nextToken());
          }
          break;
          
          // and Record Length
          case 5: { 
            _item.setRecordLength(Integer.parseInt(tokenizer.nextToken()));
          }
          break;
        }
      }
    }
    
    /** extract http headers from the current input state **/
    private void processHeaders() throws IOException {

      BufferedReader reader = new BufferedReader(readerFromScanBufferList(_buffers,ArcFileBuilder.UTF8_Charset));
      
      String line = null;
      
      _item.setFieldDirty(ArcFileItem.Field_HEADERITEMS);
      
      while ((line = reader.readLine()) != null) { 
        if (line.length() != 0) { 
          int colonPos = line.indexOf(':');

          ArcFileHeaderItem item = new ArcFileHeaderItem();

          if (colonPos != -1 && colonPos != line.length() - 1) { 

            item.setItemKey(line.substring(0,colonPos));
            item.setItemValue(line.substring(colonPos + 1));
            
            // if this is our special truncation flag ... 
            if (item.getItemKey().equals(Constants.ARCFileHeader_ContentTruncated)) { 
              String parts[] = item.getItemValue().split(",");
              for (String part : parts) { 
                if (part.equals(ArcFileItem.Flags.toString(ArcFileItem.Flags.TruncatedInInflate))) { 
                  _item.setFlags(_item.getFlags() | ArcFileItem.Flags.TruncatedInDownload);
                }
                else if (part.equals(ArcFileItem.Flags.toString(ArcFileItem.Flags.TruncatedInInflate))) {
                  _item.setFlags(_item.getFlags() | ArcFileItem.Flags.TruncatedInInflate);
                }
              }
            }            
          }
          else {
            item.setItemValue(line);
           }
          _item.getHeaderItems().add(item);
        }
      }
    }
    
    /** transition from the current input state to the next  input state **/
    private void transitionState()throws IOException {
      
      switch (_state) {
      
        case LookingForMetadata: { 
          String metadataline = readLine(ASCII_Charset);
          try { 
            // decode the string as a utf-8 string  
            processMetadataLine(metadataline);
          }
          catch (NumberFormatException e) { 
            LOG.error("Error Parsing Metadata Line:" + metadataline + " Length:" + metadataline.length());
            throw e;
          }
          // and advance to next state ... 
          _state = ArcFileBuilder.State.LookingForHeaderTerminator;
        }
        break;
        case LookingForHeaderTerminator: { 
          // found header terminator
          processHeaders();
          // and advance to next state ...
          _state = ArcFileBuilder.State.ReadingContent;
          // and set up arc file item for read ... 
          _content.setCapacity(BLOCK_SIZE);
        }
        break;
      }    
    }
    
    /**  inform builder that input for the current item has been exhauseted
     * 
     * @return ArcFileItem - the fully constructed ArcFileItem object if construction was successful
     * @throws IOException - if building fails 
     */
    public final ArcFileItem finish() throws IOException { 
      if (_state == State.ReadingContent && _content.getCount() != 0) {
        _item.setContent(_content);
        _content = new Buffer();
        return _item;
      }
      else { 
        throw new IOException("Incomplete ARC File Data Stream");
      }
    }
    
    /**
     * Input Data into the builder 
     * 
     * @param buffer - a piece of uncompressed content 
     * @throws IOException - throws exception if building fails 
     */
    public final void inputData(ByteBuffer buffer) throws IOException { 
      
      // set the buffer as the active buffer ... 
      _activeBuffer = buffer;
      
      // scan looking for terminator 
      while(_activeBuffer != null && _activeBuffer.remaining() != 0) {

        // if not reading content then 
        if (_state  != ArcFileBuilder.State.ReadingContent) { 
        
          // read a byte at a time ...
          byte b = _activeBuffer.get();
          
          // and if the byte is a delimiter ... 
          if (b == '\r' || b == '\n') { 
            
            // add it to our pattern buffer 
            patternBuffer[patternSize++] = b;
            
            // and check for pattern match (terminator match)
            if (checkForTerminator()) { 
              transitionState();
            }
          }
          // otherwise reset pattern buffer 
          else {  
            patternSize = 0;          
          }
        }
        else { 
          // calculate available storage in buffer ... 
          int available  = _content.getCapacity() - _content.getCount();
          // if we need more room ... 
          if (available < _activeBuffer.remaining()) { 
            // figure out how much to grow buffer by ... 
            int growByAmount = Math.max(_activeBuffer.remaining() - available, BLOCK_SIZE * 2);
            // and grow the buffer ... 
            _content.setCapacity(_content.getCapacity() + growByAmount);
          }
          // copy the buffer data in one go ...
          _content.append(_activeBuffer.array(),_activeBuffer.position() + _activeBuffer.arrayOffset(),_activeBuffer.remaining());
          _activeBuffer = null;
        }
      }
      // now if we reached the end of the buffer while scanning for a token ... 
      if (_activeBuffer != null) { 
        // add entire buffer to buffer list ... 
        _activeBuffer.rewind();
        _buffers.add(_activeBuffer);
        _activeBuffer = null;
      }
    }
  }

  //////////////////////////////////////////////////////////////////////////////////
  // test routines
  //////////////////////////////////////////////////////////////////////////////////

  
  public void testReader(File arcFileItem) throws Exception {
    
    resetState();
    
    Thread thread = new Thread(new Runnable() {

      public void run() {
        try { 
          
          TriStateResult result;
          
          while ((result = hasMoreItems()) != TriStateResult.NoMoreItems) { 
            
            if (result == TriStateResult.MoreItems) { 
              
              ArcFileItem item = null;
              
              while ((item = getNextItem()) == null) { 
                LOG.info("Waiting to Read Next Item...");
                try {
                  Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
              }
              
              LOG.info("GOT Item URL:" + item.getUri() + " OFFSET:" + item.getArcFilePos() + " ContentSize:" + item.getContent().getCount());
              for (ArcFileHeaderItem headerItem : item.getHeaderItems()) { 
                if (headerItem.isFieldDirty(ArcFileHeaderItem.Field_ITEMKEY)) { 
                  //LOG.info("Header Item:" + headerItem.getItemKey() + " :" + headerItem.getItemValue());
                }
                else { 
                  //LOG.info("Header Item:" + headerItem.getItemValue());
                }
              }
              //LOG.info("Content Length:" + item.getContent().getCount());
            }
            else { 
              // LOG.info("Has More Items Returned Need More Data. Sleeping");
              try {
                Thread.sleep(1000);
              } catch (InterruptedException e) {
              }
            }
          }
          LOG.info("NO MORE ITEMS... BYE");
        }
        catch (IOException e) { 
          LOG.error(StringUtils.stringifyException(e));
        }
      } 
      
    });
    
    // run the thread ... 
    thread.start();
    
    ReadableByteChannel channel = Channels.newChannel(new FileInputStream(arcFileItem));
    
    try { 
      
    
      for(;;){ 
        
        ByteBuffer buffer = ByteBuffer.allocate(BLOCK_SIZE);
        
        int bytesRead = channel.read(buffer);
        LOG.info("Read "+bytesRead + " From File");
        
        if (bytesRead == -1) { 
          finished();
          break;
        }
        else {
          buffer.flip();
          available(buffer);
        }
      }
    }
    finally { 
      channel.close();
    }
    
    // now wait for thread to die ...
    LOG.info("Done Reading File.... Waiting for ArcFileThread to DIE");
    thread.join();
    LOG.info("Done Reading File.... ArcFileThread to DIED");
  }
  
  public static void main(String[] args) {
  	File file = new File(args[0]);
  	
  	StreamingArcFileReader reader = new StreamingArcFileReader(true);
  	try {
	    reader.testReader(file);
    } catch (Exception e) {
	    e.printStackTrace();
    }
  }
}
