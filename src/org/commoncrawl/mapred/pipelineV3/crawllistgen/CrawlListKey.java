package org.commoncrawl.mapred.pipelineV3.crawllistgen;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.junit.Test;

public class CrawlListKey implements WritableComparable<CrawlListKey> {

  public static final int KEY_TYPE_HOMEPAGE_URL = 1;
  public static final int KEY_TYPE_WIKIPEDIA_URL = 2;
  public static final int KEY_TYPE_URL = 10;
  
  public long partitionDomainKey;
  public long comparisonDomainKey;
  public int    type;
  public double rank0;
  public long   rank1;
  
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(partitionDomainKey);
    out.writeLong(comparisonDomainKey);
    out.writeShort(type);
    out.writeDouble(rank0);
    out.writeLong(rank1);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    partitionDomainKey = in.readLong();
    comparisonDomainKey = in.readLong();
    type = in.readShort();
    rank0 = in.readDouble();
    rank1 = in.readLong();
  }

  @Override
  public int compareTo(CrawlListKey arg0) {
    int result = (comparisonDomainKey < arg0.comparisonDomainKey) ? -1 : (comparisonDomainKey > arg0.comparisonDomainKey) ? 1: 0;
    if (result == 0) 
      result = (type < arg0.type) ? -1 : (type > arg0.type) ? 1: 0;
    if (result == 0) 
      result = (rank0 < arg0.rank0) ? 1 : (rank0 > arg0.rank0) ? -1: 0;
    if (result == 0) 
      result = (rank1 < arg0.rank1) ? -1 : (rank1 > arg0.rank1) ? 1: 0;
    
    return result;
  }
  
  @Override
  public String toString() {
    return "PD:" + partitionDomainKey + 
        " DH:" + comparisonDomainKey + 
        " T:" + type + 
        " Rank0:" + rank0;
  }
  
  public static final class CrawListKeyComparator implements RawComparator<CrawlListKey> {

    @Override
    public int compare(CrawlListKey arg0, CrawlListKey arg1) {
      return arg0.compareTo(arg1);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      s1 += 8;
      s2 += 8;
      long lkey = WritableComparator.readLong(b1, s1);
      long rkey = WritableComparator.readLong(b2, s2);
      int result = (lkey < rkey) ? -1 : (lkey > rkey) ? 1: 0;
      if (result == 0) { 
        
        s1 += 8;
        s2 += 8;
        
        lkey = WritableComparator.readUnsignedShort(b1, s1);
        rkey = WritableComparator.readUnsignedShort(b2, s2);
        result = (lkey < rkey) ? -1 : (lkey > rkey) ? 1: 0;
        
        if (result == 0) { 

          s1 += 2;
          s2 += 2;
          
          double ldkey = WritableComparator.readDouble(b1, s1);
          double rdkey = WritableComparator.readDouble(b2, s2);
          
          result = (ldkey < rdkey) ? 1 : (ldkey > rdkey) ? -1: 0;
          
          if (result == 0) { 
          
            s1 += 8;
            s2 += 8;
           
            lkey = WritableComparator.readLong(b1, s1);
            rkey = WritableComparator.readLong(b2, s2);
            
            result = (lkey < rkey) ? -1 : (lkey > rkey) ? 1: 0;
          }
        }
      }
      return result;
    } 
    
  }
  
  public static class CrawlListKeyPartitioner implements Partitioner<CrawlListKey,Writable> {

    @Override
    public void configure(JobConf job) {
    }

    @Override
    public int getPartition(CrawlListKey key, Writable value, int numPartitions) {
      return (((int)key.partitionDomainKey) & Integer.MAX_VALUE) % numPartitions;
    } 
  }

  public static CrawlListKey generateKey(long partitionDomain,long comparisonDomain,int type,double rank0,long rank1) { 
    CrawlListKey keyOut = new CrawlListKey();
    return generateKey(keyOut,partitionDomain, comparisonDomain, type, rank0, rank1);
  }
  
  public static CrawlListKey generateKey(CrawlListKey keyOut,long partitionDomain,long comparisonDomain,int type,double rank0,long rank1) {
    keyOut.partitionDomainKey = partitionDomain;
    keyOut.comparisonDomainKey = comparisonDomain;
    keyOut.type = type;
    keyOut.rank0 = rank0;
    keyOut.rank1 = rank1;
    return keyOut;
  }
  
  private static final DataOutputBuffer writeTestKey(CrawlListKey key)throws IOException { 
    DataOutputBuffer temp = new DataOutputBuffer();
    key.write(temp);
    DataInputBuffer  input = new DataInputBuffer();
    // validate serialization while we are at it ... 
    input.reset(temp.getData(), temp.getLength());
    CrawlListKey tempKey = new CrawlListKey();
    tempKey.readFields(input);
    Assert.assertEquals(0, key.compareTo(tempKey));
    temp.reset();
    return temp;
  }
  private static final int compareRaw(CrawListKeyComparator comparator,CrawlListKey key1,CrawlListKey key2) throws IOException { 
    DataOutputBuffer buffer1 = writeTestKey(key1);
    DataOutputBuffer buffer2= writeTestKey(key2);
        
    return comparator.compare(buffer1.getData(), 0, buffer1.getLength(), buffer2.getData(), 0, buffer2.getLength());
  }
  
  @Test
  public void testComparator() throws Exception { 
    CrawlListKey key1 = generateKey(1L, 1L, 0, 0, 0);
    CrawlListKey key2 = generateKey(1L, 1L, 1, 0, 0);
    CrawlListKey key3 = generateKey(1L, 1L, 1, 1, 0);
    CrawlListKey key4 = generateKey(1L, 1L, 1, 1, 1);
    
    CrawListKeyComparator comparator = new CrawlListKey.CrawListKeyComparator();
    
    Assert.assertEquals(0, compareRaw(comparator,key1,key1));
    Assert.assertEquals(0, comparator.compare(key1,key1));
    Assert.assertEquals(-1, compareRaw(comparator,key1,key2));
    Assert.assertEquals(-1, comparator.compare(key1,key2));
    Assert.assertEquals(-1, compareRaw(comparator,key1,key3));
    Assert.assertEquals(-1, comparator.compare(key1,key3));
    Assert.assertEquals(-1, compareRaw(comparator,key1,key4));
    Assert.assertEquals(-1, comparator.compare(key1,key4));
    Assert.assertEquals(1, compareRaw(comparator,key2,key1));
    Assert.assertEquals(1, comparator.compare(key2,key1));
    Assert.assertEquals(0, compareRaw(comparator,key2,key2));
    Assert.assertEquals(0, comparator.compare(key2,key2));
    Assert.assertEquals(-1, compareRaw(comparator,key2,key3));
    Assert.assertEquals(-1, comparator.compare(key2,key3));
    Assert.assertEquals(-1, compareRaw(comparator,key2,key4));
    Assert.assertEquals(-1, comparator.compare(key2,key4));
    Assert.assertEquals(1, compareRaw(comparator,key3,key1));
    Assert.assertEquals(1, comparator.compare(key3,key1));
    Assert.assertEquals(1, compareRaw(comparator,key3,key2));
    Assert.assertEquals(1, comparator.compare(key3,key2));
    Assert.assertEquals(0, compareRaw(comparator,key3,key3));
    Assert.assertEquals(0, comparator.compare(key3,key3));
    Assert.assertEquals(-1, compareRaw(comparator,key3,key4));
    Assert.assertEquals(-1, comparator.compare(key3,key4));
    Assert.assertEquals(1, compareRaw(comparator,key4,key1));
    Assert.assertEquals(1, comparator.compare(key4,key1));
    Assert.assertEquals(1, compareRaw(comparator,key4,key2));
    Assert.assertEquals(1, comparator.compare(key4,key2));
    Assert.assertEquals(1, compareRaw(comparator,key4,key3));
    Assert.assertEquals(1, comparator.compare(key4,key3));
    Assert.assertEquals(0, compareRaw(comparator,key4,key4));
    Assert.assertEquals(0, comparator.compare(key4,key4));
    
    
    
  }
}
