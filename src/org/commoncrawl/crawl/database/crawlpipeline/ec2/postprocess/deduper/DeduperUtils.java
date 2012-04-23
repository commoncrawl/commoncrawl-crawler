package org.commoncrawl.crawl.database.crawlpipeline.ec2.postprocess.deduper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.internal.GoogleURL;
import org.commoncrawl.util.internal.URLFPBloomFilter;
import org.commoncrawl.util.internal.URLUtils;
import org.commoncrawl.util.shared.CCStringUtils;
import org.commoncrawl.util.shared.IPAddressUtils;
import org.commoncrawl.util.shared.SimHash;
import org.commoncrawl.util.shared.TextBytes;
import org.junit.Assert;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonWriter;

/** 
 * Various utilities and classes to support dedupe rewrite
 * @author rana
 *
 */
public class DeduperUtils {
  
  static final Log LOG = LogFactory.getLog(DeduperUtils.class);
  
      
  /**
   * key consisting of the pattern index and 
   * the key bits
   * 
   * @author rana
   *
   */
  public static class DeduperKey extends LongWritable { 

    static final long KEY_COMPONENT_MASK = 0xFFFFFFFFFFFFL;
    static final long PATTERN_COMPONENT_MASK = 0xFFFF000000000000L;
    static final int  PATTERN_BITS = 16;

    
    public static void setKey(LongWritable writableTarget,int patternIndex,long key) { 
      writableTarget.set(keyToLong(patternIndex,key));
    }
    
    public static long keyToLong(int patternIndex,long keyValue) { 
      return ( ((long)patternIndex) << (64-PATTERN_BITS)) | (keyValue >> (64-patternKeyMSBits[patternIndex]) & KEY_COMPONENT_MASK); 
    }
    
    public static long keyFromLong(long longValue) { 
      return (longValue & KEY_COMPONENT_MASK);
    }
    
    public static int patternIndexFromLong(long longValue) { 
      return (int) (longValue >>> (64-PATTERN_BITS));
    }
  }
  
  /**
   * DeduperValue
   * 
   * @author rana
   *
   */
  public static class DeduperValue implements Writable { 
    public long        _simHashValue;
    public long        _rootHash;
    public long        _urlHash;
    public int         _srcIP;
    public TextBytes   _urlText = new TextBytes();
    
    public DeduperValue() { 
      
    }
    
    public DeduperValue(long simhashValue,long rootHash,long urlHashValue,int srcIP, TextBytes urlText) { 
      setValue(simhashValue, rootHash, urlHashValue,srcIP,urlText);
    }
    
    public void setValue(long simHashValue,long rootHash,long urlHashValue,int srcIP,TextBytes urlText) { 
      _simHashValue = simHashValue;
      _rootHash = rootHash;
      _urlHash = urlHashValue;
      _urlText.set(urlText);
      _srcIP = srcIP;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
      _simHashValue = in.readLong();
      _rootHash  = in.readLong();
      _urlHash = in.readLong();
      _srcIP = in.readInt();
      _urlText.readFields(in);
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
      out.writeLong(_simHashValue);
      out.writeLong(_rootHash);
      out.writeLong(_urlHash);
      out.writeInt(_srcIP);
      _urlText.write(out);
    }
  }
  
  /**
   * 
   * @author rana
   *
   */
  public static class DeduperSetTuple implements Writable { 
    public long _rootHashA;
    public long _urlHashA;
    public long _rootHashB;
    public long _urlHashB;
    public TextBytes _textURLA = new TextBytes();
    public TextBytes _textURLB = new TextBytes();
    
    public DeduperSetTuple() { 
      
    }
    
    public void setIntegralValues(long rootHashA,long urlHashA, long rootHashB,long urlHashB) { 
      _rootHashA = rootHashA;
      _urlHashA = urlHashA;
      _rootHashB = rootHashB;
      _urlHashB = urlHashB;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      _rootHashA = in.readLong();
      _urlHashA =  in.readLong();
      _rootHashB = in.readLong();
      _urlHashB = in.readLong();
      _textURLA.readFields(in);
      _textURLB.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeLong(_rootHashA);
      out.writeLong(_urlHashA);
      out.writeLong(_rootHashB);
      out.writeLong(_urlHashB);
      _textURLA.write(out);
      _textURLB.write(out);      
    }
  }
  
  static final int TOTAL_CHUNKS = 6;
  static final int K = 3;
  static final int BINOMIAL_COFF = 20; //PRECOMPUTED - BASED ON (n=6,k=3) 

  static final int CHUNK_LENGTHS[] = { 
    11,
    11,
    11,
    11,
    10,
    10
  };
  
  // based on n == 6 and k == 3
  static final int patternArray[];
  static final int patternKeyMSBits[];
  static { 
    patternArray = new int[BINOMIAL_COFF];
    patternKeyMSBits = new int[BINOMIAL_COFF];
    
    // run through all 64 combinations looking for 
    // the ones where only three out six bits are ones
    int patternIndex=0;
    for (int i=0;i<=63;++i) { 
      int test = i;
      int oneBitsCount = 0;
      int chunkIndex=TOTAL_CHUNKS-1;
      int keyMSBits = 0;
      while (test != 0) { 
        if ((test & 0x01) == 1) { 
          oneBitsCount++;
          keyMSBits += CHUNK_LENGTHS[chunkIndex];
        }
        test >>= 1;
        chunkIndex--;
      }
      if (oneBitsCount == K) { 
        patternArray[patternIndex] = i;
        patternKeyMSBits[patternIndex] = keyMSBits;
        patternIndex++;
      }
    }
  }
  
  static final long ELEVEN_BITS_MASK = 0x7FF;
  static final long TEN_BITS_MASK = 0x3FF;

  static final int CHUNK_POS[] = { 
    0,
    11,
    22,
    33,
    44,
    54
  };

  static final long CHUNK_MASKS[] = { 
    ELEVEN_BITS_MASK,
    ELEVEN_BITS_MASK,
    ELEVEN_BITS_MASK,
    ELEVEN_BITS_MASK,
    TEN_BITS_MASK,
    TEN_BITS_MASK
  };
  
  /**
   * Divide incoming key into chunks and then produce a resulting key based on the defined bit pattern
   * 
   * @param pattern
   * @param originalValue
   * @return
   */
  public static long buildKeyForPatternIndex(int patternIdx,long originalValue) {
    
    // get the bit pattern specifying key/non-key chunk for the given 
    // pattern index 
    int pattern = patternArray[patternIdx];
    
    long keyOut = 0;
    
    int  onChunkPos = 0;
    int  offChunkPos = 0;
    
    //TODO: GOING WITH THE LESS EFFICIENT ROUTE FOR EXPEDIENCY'S SAKE
    //TODO: WE ONLY GENERATE THE KEY COMPONENT, AND SKIP THE VALUE BITS ALTOGETHER
    for (int pass=0;pass<1;++pass) { 
      for (int chunkNumber=0;chunkNumber<TOTAL_CHUNKS;++chunkNumber) { 
        // figure out on or off ... 
        boolean onChunk = ((pattern & (1 << (TOTAL_CHUNKS - (chunkNumber + 1)))) != 0);
        if (pass == 0 && onChunk) {
          // get chunk bits ... 
          //System.out.println("Chunk:" + chunkNumber + " is on");
          long chunkBits = ((originalValue >>> (64 - (CHUNK_POS[chunkNumber]+ CHUNK_LENGTHS[chunkNumber]))) & CHUNK_MASKS[chunkNumber]);
          //System.out.println("Chunk Bits are:" + Long.toHexString(chunkBits));
          // shift back in 
          keyOut |= (chunkBits << (64 - (onChunkPos+CHUNK_LENGTHS[chunkNumber])));
          // increment offset ... 
          onChunkPos += CHUNK_LENGTHS[chunkNumber];
        }
        else if (pass == 1 && !onChunk) { 
          //System.out.println("Chunk:" + chunkNumber + " is off");
          // get chunk bits ... 
          long chunkBits = ((originalValue >>> (64 - (CHUNK_POS[chunkNumber]+ CHUNK_LENGTHS[chunkNumber]))) & CHUNK_MASKS[chunkNumber]);
          //System.out.println("Chunk Bits are:" + Long.toHexString(chunkBits));
          // shift back in 
          keyOut |= (chunkBits << (64 - (onChunkPos+offChunkPos+CHUNK_LENGTHS[chunkNumber])));
          // increment offset 
          offChunkPos += CHUNK_LENGTHS[chunkNumber];
        }
      }
    }
    return keyOut;
  }
  
  
  /**
   
   The various chunk combinations and their bit representations ... 
   
   Values: [7, 11, 13, 14, 19, 21, 22, 25, 26, 28, 35, 37, 38, 41, 42, 44, 49, 50, 52, 56]
    
    Bits: 
      000111
      001011
      001101
      001110
      010011
      010101
      010110
      011001
      011010
      011100
      100011
      100101
      100110
      101001
      101010
      101100
      110001
      110010
      110100
      111000
   */

  /**
   * BitBuilder helper class 
   */
  static class BitBuilder { 
    long bits;
    int count;
    
    BitBuilder() { 
      bits = 0;
      count = 0;
    }
    
    BitBuilder on(int amt) { 
      for (int i=0;i<amt;++i) {
        bits  = bits <<  1;
        bits |= 1L;
      }
      return this;
    }

    BitBuilder off(int amt) {
      bits =  bits <<  amt;
      return this;
    }
    
    long bits() { 
      return bits;
    }
  }  
  
  /** 
   * TestCase - pattern generator validator 
   * 
   * @author rana
   *
   */
  static class TestCase {
    int _patternIdx;
    long _key;
    long _expectedResult;
    
    TestCase(int patternIdx,long key,long expectedResult) { 
      _patternIdx = patternIdx;
      _key = key;
      _expectedResult = expectedResult;
    }
    
    void validate() {
      long expectedResult = (_expectedResult & 
          new BitBuilder().on(patternKeyMSBits[_patternIdx]).off(64-patternKeyMSBits[_patternIdx]).bits());
      
      System.out.println("pattern:" + Integer.toHexString(patternArray[_patternIdx]) + " testKey:" + Long.toHexString(_key) + " expectedKey:" + Long.toHexString(expectedResult));
      Assert.assertEquals(expectedResult,buildKeyForPatternIndex(_patternIdx,_key));
    }
  }
  
  static final long FIRST_VALUE = 10;
  static final long SECOND_VALUE = 11;
  static final long THIRD_VALUE = 12;
  
  //TODO: NEED A MORE SANE WAY TO DEFINE TEST CASES ... 
  static ImmutableSet<TestCase> testCases = new ImmutableSet.Builder<TestCase>()
    // 000111
    .add(new TestCase(0,((FIRST_VALUE << (10 + 10)) | (SECOND_VALUE << (10)) | (THIRD_VALUE << 0)),((FIRST_VALUE << (64 - 11)) | (SECOND_VALUE << (64 - (11+10))) | (THIRD_VALUE << (64 - (11+10+10))))))
    // 001011
    .add(new TestCase(1,((FIRST_VALUE << (10 + 10)) | (SECOND_VALUE << (10)) | (THIRD_VALUE << 0)),((SECOND_VALUE << (64 - (11+10))) | (THIRD_VALUE << (64 - (11+10+10))) | FIRST_VALUE)))
    // 001101
    .add(new TestCase(2,((FIRST_VALUE << (10 + 10)) | (SECOND_VALUE << (10)) | (THIRD_VALUE << 0)),((FIRST_VALUE << (64 - (11+11))) | (THIRD_VALUE << (64 - (11+11+10))) | SECOND_VALUE)))
    // 001110
    .add(new TestCase(3,((FIRST_VALUE << (10 + 10)) | (SECOND_VALUE << (10)) | (THIRD_VALUE << 0)),((FIRST_VALUE << (64 - (11 + 11))) | (SECOND_VALUE << (64 - (11+11+10))) | THIRD_VALUE)))
    // 010011
    .add(new TestCase(4,((FIRST_VALUE << (10 + 10)) | (SECOND_VALUE << (10)) | (THIRD_VALUE << 0)),((SECOND_VALUE << (64 - (11+10))) | (THIRD_VALUE << (64 - (11+10+10))) |FIRST_VALUE)))
    // 010101
    .add(new TestCase(5,((FIRST_VALUE << (10 + 10)) | (SECOND_VALUE << (10)) | (THIRD_VALUE << 0)),((FIRST_VALUE << (64 - (11 + 11))) | (THIRD_VALUE << (64 - (11+11+10))) |SECOND_VALUE)))
    //110010
    .add(new TestCase(17,((FIRST_VALUE << (10 + 10)) | (SECOND_VALUE << (10)) | (THIRD_VALUE << 0)),((SECOND_VALUE << (64 - (11 + 11 + 10))) | (FIRST_VALUE << (10) |THIRD_VALUE))))
    //110010 (REPEAT)
    .add(new TestCase(17,((FIRST_VALUE << (64-11)) | (SECOND_VALUE << (64 - (11+11))) | (THIRD_VALUE << 0)),((FIRST_VALUE << (64-11)) | (SECOND_VALUE << (64 - (11+11))) | THIRD_VALUE)))
    //110100
    .add(new TestCase(18,((FIRST_VALUE << (64-11)) | (SECOND_VALUE << (64 - (11+11))) | (THIRD_VALUE << 0)),((FIRST_VALUE << (64-11)) | (SECOND_VALUE << (64 - (11+11))) | THIRD_VALUE)))
    //111000
    .add(new TestCase(19,((FIRST_VALUE << (64-11)) | (SECOND_VALUE << (64 - (11+11))) | (THIRD_VALUE << (11+11+11))),((FIRST_VALUE << (64-11)) | (SECOND_VALUE << (64 - (11+11))) | (THIRD_VALUE << (11+11+11)))))

  .build(); 
    
  
  static void validateGenerator() { 
    for (TestCase testCase : testCases) { 
      testCase.validate();
    }
  }

  public static class JSONSetBuilder { 
    
    public static final int NUM_HASH_FUNCTIONS = 10;
    public static final int NUM_BITS = 11;
    public static final int NUM_ELEMENTS = 1 << 18;  
    
    DataOutputBuffer _outputBuffer = new DataOutputBuffer();
    JsonWriter writer;
    URLFPBloomFilter filter = new URLFPBloomFilter(NUM_ELEMENTS, NUM_HASH_FUNCTIONS, NUM_BITS);
    
    public JSONSetBuilder() throws IOException {
      reset();
    }

    public void reset() throws IOException {
      filter.clear();
      _outputBuffer.reset();
      writer = new JsonWriter(new OutputStreamWriter(_outputBuffer, Charset.forName("UTF-8")));
      writer.beginArray();
    }
    
    URLFPV2 fp = new URLFPV2();
    
    public void add(long rootDomainHash,long urlHash,int ipAddress,TextBytes urlData)throws IOException  { 
      fp.setRootDomainHash(rootDomainHash);
      fp.setDomainHash(rootDomainHash);
      fp.setUrlHash(urlHash);
      if (!filter.isPresent(fp)) { 
        filter.add(fp);
        writer.beginObject();
        writer.name("dh").value(rootDomainHash);
        writer.name("uh").value(urlHash);
        writer.name("url").value(urlData.toString());
        writer.name("ip").value(Integer.toString(ipAddress));
        writer.endObject();
      }
    }
    
    public TextBytes flush() throws IOException { 
      writer.endArray();
      writer.flush();
      
      TextBytes textBytes = new TextBytes();
      textBytes.set(_outputBuffer.getData(), 0, _outputBuffer.getLength());
      
      return textBytes;
    }
    
  }
  
  
  
  /**
   * Build sets by comparing simhash values  
   * 
   * @author rana
   *
   */
  public static class SimhashMatcher {
    
    private DataOutputBuffer _dataBuffer = new DataOutputBuffer();
    private DataOutputBuffer _textDataBuffer = new DataOutputBuffer();
    private int[] id;
    private int   count;
    JSONSetBuilder setBuilder;


    private static final int SIZEOF_DATABUF_ENTRY = 8 * 5;
    
    public static final int SIMHASH_COMPONENT_IDX = 0;
    public static final int ROOTHASH_COMPONENT_IDX = 1;
    public static final int URLHASH_COMPONENT_IDX = 2;
    public static final int IP_COMPONENT_IDX = 3;
    public static final int TEXT_DATA_COMPONENT_IDX = 4;

    
    /** 
     * Constructor - slurp in all values associated with current deduper key... 
     * @param valueIterator
     * @throws IOException
     */
    public SimhashMatcher() throws IOException {
      setBuilder = new JSONSetBuilder();
    }
    
    
    static long readLongComponent(DataOutputBuffer buffer,int index,int componentIndex)throws IOException { 
      byte readBuffer[] = buffer.getData();
      int offset = (index * SIZEOF_DATABUF_ENTRY) + (componentIndex * 8);
      return (((long)readBuffer[offset+0] << 56) +
              ((long)(readBuffer[offset+1] & 255) << 48) +
              ((long)(readBuffer[offset+2] & 255) << 40) +
              ((long)(readBuffer[offset+3] & 255) << 32) +
              ((long)(readBuffer[offset+4] & 255) << 24) +
              ((readBuffer[offset+5] & 255) << 16) +
              ((readBuffer[offset+6] & 255) <<  8) +
              ((readBuffer[offset+7] & 255) <<  0));
    }
    
    TextBytes textFromPackedLongInfo(TextBytes textToPopulate,long packedValue)throws IOException { 
      int offset = (int)((packedValue >> 32) & 0xFFFFFFFFL);
      int length = (int)(packedValue & 0xFFFFFFFFL);
      textToPopulate.set(_textDataBuffer.getData(),offset,length);
      return textToPopulate;
    }
        
    private void collectRoots(Map<Long,TextBytes> rootDomainMap,TextBytes urlSampler,int N,int rootItemIndex)throws IOException  {
      // iterate the set looking for other items that have the same root
      for (int j = 0; j < N; ++j) {
        // ok found a match ... 
        if (id[j] == rootItemIndex && j != rootItemIndex){
          long rootDomainA = readLongComponent(_dataBuffer, rootItemIndex, ROOTHASH_COMPONENT_IDX);
          // OK .. ONE BIG LAST MINUTE HACK :-( - Need to join by root domain text key, not the long value ... :-( 
          // so we need to extract the key here... from the first matching hit url ... 
          if (!rootDomainMap.containsKey(rootDomainA)) { 
            textFromPackedLongInfo(urlSampler,readLongComponent(_dataBuffer, rootItemIndex,TEXT_DATA_COMPONENT_IDX));
            String rootDomainStr = URLUtils.extractRootDomainName(new GoogleURL(urlSampler.toString()).getHost());
            if (rootDomainStr != null) { 
              rootDomainMap.put(rootDomainA, new TextBytes(rootDomainStr));
            }
          }
          // ok now do the same thing for the second component ... 
          long rootDomainB = readLongComponent(_dataBuffer, j, ROOTHASH_COMPONENT_IDX);
          if (rootDomainA != rootDomainB) { 
            if (!rootDomainMap.containsKey(rootDomainB)) { 
              textFromPackedLongInfo(urlSampler,readLongComponent(_dataBuffer, j,TEXT_DATA_COMPONENT_IDX));
              String rootDomainStr = URLUtils.extractRootDomainName(new GoogleURL(urlSampler.toString()).getHost());
              if (rootDomainStr != null) { 
                rootDomainMap.put(rootDomainB, new TextBytes(rootDomainStr));
              }
            }                    
          }
        }
      }
    }
    
    private static final int EXTRA_DOMAIN_MAX_SAMPLE_SIZE = 100;
    private static final int OVERFLOW_THRESHOLD = 1 << 18;
    /**
     * emit any matched sets 
     * 
     * @param collector
     * @throws IOException
     */
    public void emitMatches(int maxHammingDistance,Iterator<DeduperValue> valueIterator,OutputCollector<TextBytes,TextBytes> collector,Reporter reporter) throws IOException {
      
      _dataBuffer.reset();
      _textDataBuffer.reset();
      
      int itemCount = 0;
      // ok slurp in values ... 
      while (valueIterator.hasNext()) {
        if (++itemCount >= OVERFLOW_THRESHOLD) { 
          break;
        }
        DeduperValue value = valueIterator.next();
        _dataBuffer.writeLong(value._simHashValue);
        _dataBuffer.writeLong(value._rootHash);
        _dataBuffer.writeLong(value._urlHash);
        _dataBuffer.writeLong(value._srcIP);
        int originalSize = _textDataBuffer.size();
        // write offset 
        _dataBuffer.writeInt(originalSize);
        _textDataBuffer.write(value._urlText.getBytes(),value._urlText.getOffset(),value._urlText.getLength());
        // write length 
        _dataBuffer.writeInt(_textDataBuffer.size() - originalSize);

      }
      
      if (itemCount < OVERFLOW_THRESHOLD) { 
        // count entries in data buffer 
        int N = count = _dataBuffer.size() / SIZEOF_DATABUF_ENTRY;
        // allocate id array 
        id = new int[N];
        // assume all sets are disjoint upfront ... 
        for (int i = 0; i < N; i++)
          id[i] = i;
        // ok time to start iteration ... 
        for (int i=0;i<N;++i) { 
          // forward scan potential match candidates ... 
          for (int j=i+1;j<N;++j) { 
            // if not already matched ... 
            if (id[i] != id[j]) { 
              if (SimHash.hammingDistance(
                  readLongComponent(_dataBuffer, i, SIMHASH_COMPONENT_IDX),
                  readLongComponent(_dataBuffer, j, SIMHASH_COMPONENT_IDX)) <= maxHammingDistance) { 
                // match ...
                // union it ... 
                union(j,i);
              }
            }
          }
        }
        
        // time to emit sets ... 
        for (int i = 0; i < N; ++i) {
          // see if this is a root item 
          if (id[i] == i) {
            // allocate hash set to contain root Domains
            HashMap<Long,TextBytes> rootDomainMap = new HashMap<Long,TextBytes>();
            // and a text bytes to collect url data 
            TextBytes urlSampler = new TextBytes();
            // collect roots ... 
            collectRoots(rootDomainMap, urlSampler, N, i);
            
            // ok walk roots... 
            for (Map.Entry<Long,TextBytes> rootEntry : rootDomainMap.entrySet()) { 
              // for each root ... walk items 
              // reset set builder ... 
              setBuilder.reset();
              // reset extra domain item count 
              int extraDomainItemCount = 0;
              
              // iterate the set 
              for (int j = 0; j < N; ++j) {
                // if in set ... 
                if (id[j] == i){
                  // get root domain of entry ... 
                  long itemRootDomain = readLongComponent(_dataBuffer, j, ROOTHASH_COMPONENT_IDX);
  
                  // IFF pass 0 .. only process documents from our root domain ...
                  if (itemRootDomain == rootEntry.getKey()) { 
                    // add item no matter what ... 
                    setBuilder.add( 
                        readLongComponent(_dataBuffer, j, ROOTHASH_COMPONENT_IDX),
                        readLongComponent(_dataBuffer, j, URLHASH_COMPONENT_IDX),
                        (int) readLongComponent(_dataBuffer, j, IP_COMPONENT_IDX),
                        textFromPackedLongInfo(urlSampler,readLongComponent(_dataBuffer, j,TEXT_DATA_COMPONENT_IDX)));
                  }
                  else { 
                    if (extraDomainItemCount++ < EXTRA_DOMAIN_MAX_SAMPLE_SIZE) { 
                      setBuilder.add( 
                          readLongComponent(_dataBuffer, j, ROOTHASH_COMPONENT_IDX),
                          readLongComponent(_dataBuffer, j, URLHASH_COMPONENT_IDX),
                          (int) readLongComponent(_dataBuffer, j, IP_COMPONENT_IDX),
                          textFromPackedLongInfo(urlSampler,readLongComponent(_dataBuffer, j,TEXT_DATA_COMPONENT_IDX)));
                    }
                  }
                }
              }
              // emit data ...
              TextBytes setDataOut = setBuilder.flush();
              
              collector.collect(rootEntry.getValue(), setDataOut);
            }
          }
        } 
      }
      else { 
        LOG.error("Hit too many items in set! - skipping");
        reporter.incrCounter("", "skipping-overflow-set", 1);
        
        int N = count = _dataBuffer.size() / SIZEOF_DATABUF_ENTRY;
        
        for (int i=0;i<100;++i) {
          TextBytes urlSampler = new TextBytes();
          textFromPackedLongInfo(urlSampler,readLongComponent(_dataBuffer, i,TEXT_DATA_COMPONENT_IDX));
          LOG.error("Skipped URL Sample:" + urlSampler.toString());
        }
      }
    }

    // Return component identifier for component containing p
    int find(int p) {
      return id[p];
    }

    // are elements p and q in the same component?
    boolean connected(int p, int q) {
      return id[p] == id[q];
    }

    // merge components containing p and q
    void union(int p, int q) {
      if (connected(p, q))
        return;
      int pid = id[p];
      for (int i = 0; i < id.length; i++)
        if (id[i] == pid)
          id[i] = id[q];
      count--;
    }
  }
  
  /** 
   * union incoming sets  
   * 
   * @author rana
   *
   */
  public static class SetUnionFinder  {
    
    public static final int NUM_HASH_FUNCTIONS = 10;
    public static final int NUM_BITS = 11;
    public static final int NUM_ELEMENTS = 1 << 18;  

    private DataOutputBuffer _dataBuffer = new DataOutputBuffer();
    private DataOutputBuffer _textDataBuffer = new DataOutputBuffer();
    private int[] id;
    private int   count;
    private URLFPBloomFilter filter = new URLFPBloomFilter(NUM_ELEMENTS, NUM_HASH_FUNCTIONS, NUM_BITS);
    private JsonParser parser = new JsonParser();
    private URLFPV2 fp = new URLFPV2();
    private int lastUsedId=-1;
    private TextBytes textBytes = new TextBytes();
    private TreeMap<Long,Integer> hashToIdMap = new TreeMap<Long,Integer>();
    private JSONSetBuilder setBuilder;
    
    private static final int SIZEOF_DATABUF_ENTRY = 8 * 4;
    
    public static final int ROOTHASH_COMPONENT_IDX = 0;
    public static final int URLHASH_COMPONENT_IDX = 1;
    public static final int IP_ADDRESS_COMPONENT = 2;
    public static final int TEXT_DATA_COMPONENT_IDX = 3;
    
    
    private void reset() throws IOException { 
      _dataBuffer.reset();
      _textDataBuffer.reset();
      filter.clear();
      lastUsedId = -1;
      hashToIdMap.clear();
      if (setBuilder == null) { 
        setBuilder = new JSONSetBuilder();
      }
      else { 
        setBuilder.reset();
      }
    }
    
    private int insertItemGetId(long domainHash,long urlHash,int ipAddress,String url)throws IOException {
      Integer existingId = hashToIdMap.get(urlHash);

      if (existingId == null) { 
        // make string to utf-8 bytes ... 
        textBytes.set(url);
        // write out id info 
        _dataBuffer.writeLong(domainHash);
        _dataBuffer.writeLong(urlHash);
        _dataBuffer.writeLong(ipAddress);
        // and string 
        int originalSize = _textDataBuffer.size();
        // write offset 
        _dataBuffer.writeInt(originalSize);
        _textDataBuffer.write(textBytes.getBytes(),0,textBytes.getLength());
        // write length 
        _dataBuffer.writeInt(_textDataBuffer.size() - originalSize);
        
        hashToIdMap.put(urlHash, ++lastUsedId);
        
        return lastUsedId;
      }
      return existingId;
    }
    
    private TextBytes textFromPackedLongInfo(TextBytes textToPopulate,long packedValue)throws IOException { 
      int offset = (int)((packedValue >> 32) & 0xFFFFFFFFL);
      int length = (int)(packedValue & 0xFFFFFFFFL);
      textToPopulate.set(_textDataBuffer.getData(),offset,length);
      return textToPopulate;
    }
    
    
    /** 
     * union incoming sets 
     * 
     * @param incomingSets
     * @throws IOException
     */
    public void union(Iterator<TextBytes> incomingSets)throws IOException  {
      
      reset();
      
      ArrayList<ArrayList<Integer>> arrayOfSets = new ArrayList<ArrayList<Integer>>();
      
      while (incomingSets.hasNext()) {

        // allocate a new set array 
        ArrayList<Integer> setIdArray = new ArrayList<Integer>();
        
        TextBytes setJSON = incomingSets.next();
        try {
          //
          JsonArray array = parser.parse(setJSON.toString()).getAsJsonArray();
          for (JsonElement element : array) { 
            JsonObject data = element.getAsJsonObject();
            
            long domainHash = data.get("dh").getAsLong();
            long urlHash    = data.get("uh").getAsLong();
            String url      = data.get("url").getAsString();
            int  ipAddress  = data.get("ip").getAsInt();
            
            // insert the item into meta set, get back an id ...  
            int id = insertItemGetId(domainHash, urlHash,ipAddress, url);
            
            // add id to local set 
            setIdArray.add(id);
          }
          // if not disjoint ... 
          if (setIdArray.size() > 1){ 
            // sort new set first ... 
            Collections.sort(setIdArray);
            // ok add this set to list of sets ... 
            arrayOfSets.add(setIdArray);
          }
        }
        catch (Exception e) { 
          LOG.error("Exceptin in UnionFinder:" + CCStringUtils.stringifyException(e));
          throw new IOException(e);
        }
      }
      // allocate id array 
      id = new int[lastUsedId+1];
      // assume all sets are disjoint upfront ... 
      for (int i = 0; i <= lastUsedId; i++)
        id[i] = i;
      // ok walk individual sets 
      for (ArrayList<Integer> idSet : arrayOfSets) { 
        // get root id 
        int rootId = idSet.get(0);
        // walk remaining members and union to root 
        for (int i=1;i<idSet.size();++i) { 
          union(idSet.get(i),rootId);
        }
      }
    }
    
    static long readLongComponent(DataOutputBuffer buffer,int index,int componentIndex)throws IOException { 
      byte readBuffer[] = buffer.getData();
      int offset = (index * SIZEOF_DATABUF_ENTRY) + (componentIndex * 8);
      return (((long)readBuffer[offset+0] << 56) +
              ((long)(readBuffer[offset+1] & 255) << 48) +
              ((long)(readBuffer[offset+2] & 255) << 40) +
              ((long)(readBuffer[offset+3] & 255) << 32) +
              ((long)(readBuffer[offset+4] & 255) << 24) +
              ((readBuffer[offset+5] & 255) << 16) +
              ((readBuffer[offset+6] & 255) <<  8) +
              ((readBuffer[offset+7] & 255) <<  0));
    }
    
    public void emit(TextBytes rootKey,OutputCollector<TextBytes,TextBytes> collector,Reporter reporter)throws IOException { 
      // and a text bytes to collect url data 
      TextBytes urlSampler = new TextBytes();
      
      // walk all members of the set 
      for (int i = 0; i < id.length; ++i) {
        // see if this is a root item 
        if (id[i] == i) {
          // reset set builder ... 
          setBuilder.reset();
          // iterate the entire set 
          for (int j = 0; j < id.length; ++j) {
            // if current item's root is current root ... 
            if (id[j] == i){
              
              // add item to set builder  
              setBuilder.add( 
                  readLongComponent(_dataBuffer, j, ROOTHASH_COMPONENT_IDX),
                  readLongComponent(_dataBuffer, j, URLHASH_COMPONENT_IDX),
                  (int)readLongComponent(_dataBuffer, j, IP_ADDRESS_COMPONENT),
                  textFromPackedLongInfo(urlSampler,readLongComponent(_dataBuffer, j,TEXT_DATA_COMPONENT_IDX)));
            }
          }
          // emit data ...
          TextBytes setDataOut = setBuilder.flush();
          
          collector.collect(rootKey, setDataOut);          
        }
      }           
    }
    
    // are elements p and q in the same component?
    boolean connected(int p, int q) {
      return id[p] == id[q];
    }

    // merge components containing p and q
    void union(int p, int q) {
      if (connected(p, q))
        return;
      int pid = id[p];
      for (int i = 0; i < id.length; i++)
        if (id[i] == pid)
          id[i] = id[q];
      count--;
    }    
  }
  
  static private void populateTestJSONSetData(Multimap<String,Long> map,TextBytes rootDomain,TextBytes jsonPayload) throws IOException { 
    JsonParser parser = new JsonParser();
    JsonArray array = parser.parse(jsonPayload.toString()).getAsJsonArray();
    for (JsonElement el : array) { 
      JsonObject tuple = el.getAsJsonObject();
      long urlHash = tuple.get("uh").getAsLong();
      map.put(rootDomain.toString(), urlHash);
    }
    
  }
  /** 
   * 
   * @param args
   */
  public static void main(String[] args) throws IOException {
    
    URLFPBloomFilter filter = new URLFPBloomFilter(JSONSetBuilder.NUM_ELEMENTS, JSONSetBuilder.NUM_HASH_FUNCTIONS, JSONSetBuilder.NUM_BITS);
    DescriptiveStatistics filterClearStats = new DescriptiveStatistics();
    for (int i=0;i<1000;++i) { 
      long timeStart = System.nanoTime();
      filter.clear();
      long timeEnd = System.nanoTime();
      filterClearStats.addValue(timeEnd - timeStart);
    }
    System.out.println("Mean Clear Time:" + filterClearStats.getMean());
    
    
    System.out.println("size:" + BINOMIAL_COFF);
    for (int j=0;j<BINOMIAL_COFF;++j) { 
      int value = patternArray[j];
      System.out.print("value:" + value + " "); 
      for (int i=5;i>=0;--i) {
        System.out.print(((value & (1 << i)) != 0)? '1':'0');
      }
      System.out.print("  Key MSBLen:" + Integer.toString(patternKeyMSBits[j]) + "\n");
    }
    validateGenerator();
    
    
    long key1 = new BitBuilder().on(10).off(1).on(53).bits();
    long key2 = new BitBuilder().on(10).off(4).on(50).bits();
    long key3 = new BitBuilder().on(10).off(4).on(47).off(3).bits();
    long key4 = new BitBuilder().off(10).on(4).off(47).on(3).bits();
    long key5 = new BitBuilder().off(10).on(4).off(47).on(1).off(2).bits();

    
    Assert.assertTrue(SimHash.hammingDistance(key1, key2) == 3);
    Assert.assertTrue(SimHash.hammingDistance(key1, key3) != 3);
    Assert.assertTrue(SimHash.hammingDistance(key2, key3) == 3);
    Assert.assertTrue(SimHash.hammingDistance(key1, key4) > 3);
    Assert.assertTrue(SimHash.hammingDistance(key2, key4) > 3);
    Assert.assertTrue(SimHash.hammingDistance(key3, key4) > 3);
    Assert.assertTrue(SimHash.hammingDistance(key4, key5) <= 3);

    
    ImmutableList<DeduperValue> values = new ImmutableList.Builder<DeduperValue>()
      
      .add(new DeduperValue(key1,1000,2000,IPAddressUtils.IPV4AddressStrToInteger("10.0.0.1"),new TextBytes("http://adomain.com/")))
      .add(new DeduperValue(key2,1001,2001,IPAddressUtils.IPV4AddressStrToInteger("10.0.0.2"),new TextBytes("http://bdomain.com/")))
      .add(new DeduperValue(key3,1002,2002,IPAddressUtils.IPV4AddressStrToInteger("10.0.0.3"),new TextBytes("http://cdomain.com/")))
      .add(new DeduperValue(key4,1003,2003,IPAddressUtils.IPV4AddressStrToInteger("10.0.0.4"),new TextBytes("http://ddomain.com/")))
      .add(new DeduperValue(key5,1004,2004,IPAddressUtils.IPV4AddressStrToInteger("10.0.0.5"),new TextBytes("http://edomain.com/")))
      .build();
    
    SimhashMatcher unionFinder = new SimhashMatcher();
    
    final Multimap<String,Long> rootDomainToDupes = TreeMultimap.create();
    // collect all json set representations ... 
    final ArrayList<TextBytes> jsonSets = new ArrayList<TextBytes>(); 
    
    
    unionFinder.emitMatches(3,values.iterator(),new OutputCollector<TextBytes, TextBytes>() {
      
      @Override
      public void collect(TextBytes key, TextBytes value)throws IOException {
        System.out.println("Root:" + key 
            + " JSON: " + value.toString() );
        
        populateTestJSONSetData(rootDomainToDupes,key,value);
        // collect all json sets for later disjoint-set join 
        jsonSets.add(value);
      }
    },null);
    
    ImmutableList<Long> hashSuperSet1 = ImmutableList.of(2000L,2001L,2002L);
    ImmutableList<Long> hashSuperSet2 = ImmutableList.of(2003L,2004L);
    
    Assert.assertTrue(rootDomainToDupes.get("adomain.com").containsAll(hashSuperSet1));
    Assert.assertTrue(rootDomainToDupes.get("bdomain.com").containsAll(hashSuperSet1));
    Assert.assertTrue(rootDomainToDupes.get("cdomain.com").containsAll(hashSuperSet1));

    Assert.assertTrue(rootDomainToDupes.get("ddomain.com").containsAll(hashSuperSet2));
    Assert.assertTrue(rootDomainToDupes.get("edomain.com").containsAll(hashSuperSet2));
    
    ImmutableList<DeduperValue> secondSetValues = new ImmutableList.Builder<DeduperValue>()
    
    .add(new DeduperValue(key1,1000,2000,IPAddressUtils.IPV4AddressStrToInteger("10.0.0.2"),new TextBytes("http://adomain.com/")))
    .add(new DeduperValue(key1,1007,2007,IPAddressUtils.IPV4AddressStrToInteger("10.0.0.2"),new TextBytes("http://z1domain.com/")))
    .add(new DeduperValue(key2,1008,2008,IPAddressUtils.IPV4AddressStrToInteger("10.0.0.2"),new TextBytes("http://z2domain.com/")))
    .add(new DeduperValue(key3,1009,2009,IPAddressUtils.IPV4AddressStrToInteger("10.0.0.2"),new TextBytes("http://z3domain.com/")))
    .build();
    
    unionFinder.emitMatches(3,secondSetValues.iterator(),new OutputCollector<TextBytes, TextBytes>() {
      
      @Override
      public void collect(TextBytes key, TextBytes value)throws IOException {
        System.out.println("Root:" + key 
            + " JSON: " + value.toString() );
        // collect all json sets for later disjoint-set join 
        jsonSets.add(value);
      }
    },null);    
    
    SetUnionFinder unionFinder2 = new SetUnionFinder();
    
    // union all json sets ... 
    unionFinder2.union(jsonSets.iterator());
    
    // ok emit union of sets ... 
    unionFinder2.emit(new TextBytes("test"), new OutputCollector<TextBytes, TextBytes>() {

      @Override
      public void collect(TextBytes key, TextBytes value) throws IOException {
        System.out.println("Root:" + key 
            + " JSON: " + value.toString() );
      }
    },null);
    
  }
}
