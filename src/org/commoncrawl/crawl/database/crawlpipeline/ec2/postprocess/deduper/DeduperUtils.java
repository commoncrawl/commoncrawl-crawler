package org.commoncrawl.crawl.database.crawlpipeline.ec2.postprocess.deduper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.OutputCollector;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.internal.GoogleURL;
import org.commoncrawl.util.internal.URLUtils;
import org.commoncrawl.util.shared.SimHash;
import org.commoncrawl.util.shared.TextBytes;
import org.junit.Assert;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

/** 
 * Various utilities and classes to support dedupe rewrite
 * @author rana
 *
 */
public class DeduperUtils {
  
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
    public TextBytes   _urlText = new TextBytes();
    
    public DeduperValue() { 
      
    }
    
    public DeduperValue(long simhashValue,long rootHash,long urlHashValue,TextBytes urlText) { 
      setValue(simhashValue, rootHash, urlHashValue,urlText);
    }
    
    public void setValue(long simHashValue,long rootHash,long urlHashValue,TextBytes urlText) { 
      _simHashValue = simHashValue;
      _rootHash = rootHash;
      _urlHash = urlHashValue;
      _urlText.set(urlText);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
      _simHashValue = in.readLong();
      _rootHash  = in.readLong();
      _urlHash = in.readLong();
      _urlText.readFields(in);
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
      out.writeLong(_simHashValue);
      out.writeLong(_rootHash);
      out.writeLong(_urlHash);
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

  public static class SimhashMatcher {
    
    private DataOutputBuffer _dataBuffer = new DataOutputBuffer();
    private DataOutputBuffer _textDataBuffer = new DataOutputBuffer();
    private int[] id;
    private int   count;

    private static final int SIZEOF_DATABUF_ENTRY = 8 * 4;
    
    public static final int SIMHASH_COMPONENT_IDX = 0;
    public static final int ROOTHASH_COMPONENT_IDX = 1;
    public static final int URLHASH_COMPONENT_IDX = 2;
    public static final int TEXT_DATA_COMPONENT_IDX = 3;

    
    /** 
     * Constructor - slurp in all values associated with current deduper key... 
     * @param valueIterator
     * @throws IOException
     */
    public SimhashMatcher(Iterator<DeduperValue> valueIterator)throws IOException {
      while (valueIterator.hasNext()) { 
        DeduperValue value = valueIterator.next();
        _dataBuffer.writeLong(value._simHashValue);
        _dataBuffer.writeLong(value._rootHash);
        _dataBuffer.writeLong(value._urlHash);
        int originalSize = _textDataBuffer.size();
        // write offset 
        _dataBuffer.writeInt(originalSize);
        _textDataBuffer.write(value._urlText.getBytes(),value._urlText.getOffset(),value._urlText.getLength());
        // write length 
        _dataBuffer.writeInt(_textDataBuffer.size() - originalSize);
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
    
    void textFromPackedLongInfo(TextBytes textToPopulate,long packedValue)throws IOException { 
      int offset = (int)((packedValue >> 32) & 0xFFFFFFFFL);
      int length = (int)(packedValue & 0xFFFFFFFFL);
      textToPopulate.set(_textDataBuffer.getData(),offset,length);
    }
    
    void buildTupleFromIndexes(int leftIndex,int rightIndex,DeduperSetTuple tuple) throws IOException { 
      tuple.setIntegralValues(
          readLongComponent(_dataBuffer, leftIndex, ROOTHASH_COMPONENT_IDX),
          readLongComponent(_dataBuffer, leftIndex, URLHASH_COMPONENT_IDX),
          readLongComponent(_dataBuffer, rightIndex, ROOTHASH_COMPONENT_IDX),
          readLongComponent(_dataBuffer, rightIndex, URLHASH_COMPONENT_IDX));
      // ok now populate text bytes ... 
      textFromPackedLongInfo(tuple._textURLA,readLongComponent(_dataBuffer, leftIndex,TEXT_DATA_COMPONENT_IDX));
      textFromPackedLongInfo(tuple._textURLB,readLongComponent(_dataBuffer, rightIndex,TEXT_DATA_COMPONENT_IDX));
    }
    
    static final void swapKeys(DeduperSetTuple tuple) { 
      long temp = tuple._rootHashB;
      tuple._rootHashB = tuple._rootHashA;
      tuple._rootHashA = temp;
      temp = tuple._urlHashB;
      tuple._urlHashB = tuple._urlHashA;
      tuple._urlHashA = temp;
      TextBytes textTemp = tuple._textURLB;
      tuple._textURLB = tuple._textURLA;
      tuple._textURLA = textTemp;
    }
    
    /**
     * emit any matched sets 
     * 
     * @param collector
     * @throws IOException
     */
    public void emitMatches(OutputCollector<TextBytes,DeduperSetTuple> collector) throws IOException {
      
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
                readLongComponent(_dataBuffer, j, SIMHASH_COMPONENT_IDX)) <= K) { 
              // match ...
              // union it ... 
              union(j,i);
            }
          }
        }
      }
      
      DeduperSetTuple tuple = new DeduperSetTuple();
      
      // ok time to emit sets ... 
      

      
      for (int i = 0; i < N; ++i) {
        // see if this is a root item 
        if (id[i] == i) {
          // ok two passes 
          // ... first collect root domains ...
          // ... second emit tuples 
          
          // allocate hash set to contain root Domains
          HashMap<Long,TextBytes> rootDomainMap = new HashMap<Long,TextBytes>();
          TextBytes urlSampler = new TextBytes();
          
          for (int pass=0;pass<2;++pass) { 
            // iterate the set looking for other items that have the same root
            for (int j = 0; j < N; ++j) {
              // ok found a match ... 
              if (id[j] == i && j != i){
                if (pass == 0) {
                  long rootDomainA = readLongComponent(_dataBuffer, i, ROOTHASH_COMPONENT_IDX);
                  // OK .. ONE BIG LAST MINUTE HACK :-( - Need to join by root domain text key, not the long value ... :-( 
                  // so we need to extract the key here... from the first matching hit url ... 
                  if (!rootDomainMap.containsKey(rootDomainA)) { 
                    textFromPackedLongInfo(urlSampler,readLongComponent(_dataBuffer, i,TEXT_DATA_COMPONENT_IDX));
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
                else { 
                  // build tuple 
                  buildTupleFromIndexes(i,j,tuple);
                  // walk roots and emit tuple ... 
                  for (Map.Entry<Long,TextBytes> rootEntry : rootDomainMap.entrySet()) { 
                    // check to see if second item in tuple belong to current root domain ..
                    // if so, swap items in tuple to make item B lead item in tuple
                    if (tuple._rootHashA != tuple._rootHashB && tuple._rootHashB == rootEntry.getKey()) {
                      // swap keys in tuple ... 
                      swapKeys(tuple);
                    }
                    // ok output tuple 
                    collector.collect(rootEntry.getValue(), tuple);
                  }
                }
              }
            }
          }
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
   * 
   * @param args
   */
  public static void main(String[] args) throws IOException {
    
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
      
      .add(new DeduperValue(key1,1000,2000,new TextBytes("http://adomain.com/")))
      .add(new DeduperValue(key2,1001,2001,new TextBytes("http://bdomain.com/")))
      .add(new DeduperValue(key3,1002,2002,new TextBytes("http://cdomain.com/")))
      .add(new DeduperValue(key4,1003,2003,new TextBytes("http://ddomain.com/")))
      .add(new DeduperValue(key5,1004,2004,new TextBytes("http://edomain.com/")))
      .build();
    
    SimhashMatcher unionFinder = new SimhashMatcher(values.iterator());
    
    final Multimap<String,Long> rootDomainToDupes = TreeMultimap.create();
    
    unionFinder.emitMatches(new OutputCollector<TextBytes, DeduperSetTuple>() {
      
      @Override
      public void collect(TextBytes key, DeduperSetTuple value)throws IOException {
        System.out.println("Root:" + key 
            + " LD:"+ value._rootHashA +" LH:" + value._urlHashA + " LT:" + value._textURLA.toString() 
            + " RD:"+ value._rootHashB + " RH:" + value._urlHashB + " RT:" + value._textURLB.toString() );
        rootDomainToDupes.put(key.toString(), value._urlHashA);
        rootDomainToDupes.put(key.toString(), value._urlHashB);
      }
    });
    
    ImmutableList<Long> hashSuperSet1 = ImmutableList.of(2000L,2001L,2002L);
    ImmutableList<Long> hashSuperSet2 = ImmutableList.of(2003L,2004L);
    
    Assert.assertTrue(rootDomainToDupes.get("adomain.com").containsAll(hashSuperSet1));
    Assert.assertTrue(rootDomainToDupes.get("bdomain.com").containsAll(hashSuperSet1));
    Assert.assertTrue(rootDomainToDupes.get("cdomain.com").containsAll(hashSuperSet1));

    Assert.assertTrue(rootDomainToDupes.get("ddomain.com").containsAll(hashSuperSet2));
    Assert.assertTrue(rootDomainToDupes.get("edomain.com").containsAll(hashSuperSet2));

  }
}
