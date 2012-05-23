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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.TreeSet;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.protocol.URLFP;
import org.commoncrawl.protocol.URLFPV2;

/**
 * 
 * @author rana
 *
 */
public class URLFPBloomFilter {
  

  long nbits  = 0;
  int  numElements = 0;
  int  bucketsPerElement = 0;
  int  hashCount = 0;
  OpenBitSet bits = null;
  long hashResults[] = null;
  
  static final int BUCKETS_PER_WORD = 16;

  
  public URLFPBloomFilter(int numElements,int hashCount, int bucketsPerElement){
    this.numElements = numElements;
    this.bucketsPerElement = bucketsPerElement;
    this.hashCount = hashCount; 
    this.nbits = (long)numElements * (long)bucketsPerElement + 20;
    System.out.println("Number of Bits is:" + nbits);
    System.out.println("Number of Bytes is:" + nbits / 8);
    this.bits  = new OpenBitSet(nbits,true);
    this.hashResults = new long[hashCount];
  }
  
  public final synchronized boolean isPresent(URLFP key) {
    if (key == null) 
      return false;
    for (long bucketIndex : getHashBuckets(key,hashCount,nbits)){
      if (!bits.fastGet(bucketIndex))
        return false;
    }
    return true;
  }

  public final synchronized boolean isPresent(URLFPV2 key) {
    if (key == null) 
      return false;
    for (long bucketIndex : getHashBuckets(key,hashCount,nbits)){
      if (!bits.fastGet(bucketIndex))
        return false;
    }
    return true;
  }

  public final synchronized void add(URLFP key) {
  		if (key != null) { 
	      for (long bucketIndex : getHashBuckets(key,hashCount,nbits)) {
	        bits.fastSet(bucketIndex);
	      }
  		}
  }  

  public final synchronized void add(URLFPV2 key) {
    if (key != null) { 
      for (long bucketIndex : getHashBuckets(key,hashCount,nbits)) {
        bits.fastSet(bucketIndex);
      }
    }
  }
  
  public final synchronized void clear() {
    if (bits != null) {
      bits.clear();
    }
  }

  public synchronized void copyBitsTo(URLFPBloomFilter destination)throws IOException { 
    if (this.nbits != destination.nbits || bits.getNumWords() != destination.bits.getNumWords()) { 
      throw new IOException("Source and Destination BloomFilters are sized differently!");
    }

    int pageCount = bits.getPageCount();
    
    for (int p = 0; p < pageCount; p++){
      long[] srcData = bits.getPage(p);
      long[] destData = destination.bits.getPage(p);
      System.arraycopy(srcData,0,destData,0,srcData.length);
    }
  }
  
  
  private void serializeBits(DataOutput dos) throws IOException
  {
      int bitLengthInWords = bits.getNumWords();
      int pageSize = bits.getPageSize();
      int pageCount = bits.getPageCount();

      dos.writeLong(nbits);

      for (int p = 0; p < pageCount; p++){
          long[] data = bits.getPage(p);
          for (int i = 0; i < pageSize && bitLengthInWords-- > 0; i++)
              dos.writeLong(data[i]);
      }
  }

  private void deserializeBits(DataInput dis) throws IOException {
    long nbitsOut = dis.readLong();
    if (nbitsOut != nbits) { 
      throw new IOException("Serialized bitCount:"+ nbitsOut + " Expected bitCount:" + nbits);
    }
    
    int bitLengthInWords   = bits.getNumWords();
    int pageSize    = bits.getPageSize();
    int pageCount   = bits.getPageCount();

    for (int p = 0; p < pageCount; p++) {
        long[] data = bits.getPage(p);
        for (int i = 0; i < pageSize && bitLengthInWords-- > 0; i++)
          data[i] = dis.readLong();
    }
  }
  
  public final synchronized void serialize(OutputStream outputStream) throws IOException { 

    DataOutputStream dataOut = new DataOutputStream(outputStream);
    dataOut.writeInt(0); // version placeholder
    dataOut.writeInt(numElements);
    dataOut.writeInt(hashCount);
    dataOut.writeInt(bucketsPerElement);
    serializeBits(dataOut);
    outputStream.flush();
  }

  public static URLFPBloomFilter load(InputStream inputStream) throws IOException { 
    DataInputStream dataIn = new DataInputStream(inputStream);
    // skip version bytes ... 
    dataIn.readInt();
    // initialize filter ... 
    URLFPBloomFilter filter = new URLFPBloomFilter(dataIn.readInt(),dataIn.readInt(),dataIn.readInt());
    // read bits
    if (inputStream instanceof FSDataInputStream) {
      filter.deserializeBits((FSDataInputStream)inputStream);
    }
    else { 
      // load bits ... 
      filter.deserializeBits(new DataInputStream(inputStream));
    }
    return filter;
  }  
  
  final long[] getHashBuckets(URLFP key, int hashCount, long max) {
      byte[] b = new byte[12];
      b[0] = (byte)((key.getDomainHash() >>> 24) & 0xFF);
      b[1] = (byte)((key.getDomainHash() >>> 16) & 0xFF);
      b[2] = (byte)((key.getDomainHash() >>> 8) & 0xFF);
      b[3] = (byte)((key.getDomainHash()) & 0xFF);
      
      b[4] = (byte)((key.getUrlHash() >>> 56) & 0xFF);
      b[5] = (byte)((key.getUrlHash()>>> 48) & 0xFF);
      b[6] = (byte)((key.getUrlHash() >>> 40) & 0xFF);
      b[7] = (byte)((key.getUrlHash() >>> 32) & 0xFF);
      b[8] = (byte)((key.getUrlHash() >>> 24) & 0xFF);
      b[9] = (byte)((key.getUrlHash() >>> 16) & 0xFF);
      b[10] = (byte)((key.getUrlHash() >>> 8) & 0xFF);
      b[11] = (byte)((key.getUrlHash()) & 0xFF);
      
      
      int  hash1 = MurmurHash.hash(b, b.length, 0);
      int hash2 = MurmurHash.hash(b, b.length, hash1);
      for (int i = 0; i < hashCount; i++) {
        hashResults[i] = Math.abs(((long)hash1 + i * (long)hash2) % max);
      }
      return hashResults;
  }

  final long[] getHashBuckets(URLFPV2 key, int hashCount, long max) {
    byte[] b = new byte[16];
    long domainHash = key.getDomainHash();
    long urlHash = key.getUrlHash();
    
    b[0] = (byte)((domainHash >>> 56) & 0xFF);
    b[1] = (byte)((domainHash>>> 48) & 0xFF);
    b[2] = (byte)((domainHash >>> 40) & 0xFF);
    b[3] = (byte)((domainHash >>> 32) & 0xFF);
    b[4] = (byte)((domainHash >>> 24) & 0xFF);
    b[5] = (byte)((domainHash >>> 16) & 0xFF);
    b[6] = (byte)((domainHash >>> 8) & 0xFF);
    b[7] = (byte)((domainHash) & 0xFF);
    
    b[8] = (byte)((urlHash >>> 56) & 0xFF);
    b[9] = (byte)((urlHash>>> 48) & 0xFF);
    b[10] = (byte)((urlHash >>> 40) & 0xFF);
    b[11] = (byte)((urlHash >>> 32) & 0xFF);
    b[12] = (byte)((urlHash >>> 24) & 0xFF);
    b[13] = (byte)((urlHash >>> 16) & 0xFF);
    b[14] = (byte)((urlHash >>> 8) & 0xFF);
    b[15] = (byte)((urlHash) & 0xFF);
    
    
    int  hash1 = MurmurHash.hash(b, b.length, 0);
    int hash2 = MurmurHash.hash(b, b.length, hash1);
    for (int i = 0; i < hashCount; i++) {
      hashResults[i] = Math.abs(((long)hash1 + i * (long)hash2) % max);
    }
    return hashResults;
  }

  public static void main(String[] args) {

    Configuration conf = new Configuration();
    
    conf.addResource("nutch-default.xml");
    conf.addResource("nutch-site.xml");
    conf.addResource("hadoop-default.xml");
    conf.addResource("hadoop-site.xml");
    conf.addResource("commoncrawl-default.xml");
    conf.addResource("commoncrawl-site.xml");
    
    CrawlEnvironment.setHadoopConfig(conf);
    CrawlEnvironment.setDefaultHadoopFSURI("hdfs://ccn01:9000/");
    
    simpleTest(args[0]);
  }
  
  public static void simpleTest(String outputFileName) { 
    URLFPBloomFilter bigBloomFilter = new URLFPBloomFilter(750000000,10,11);
    TreeSet<URLFP> addedSet = new TreeSet<URLFP>();
    TreeSet<URLFP> notAddedSet = new TreeSet<URLFP>();
    for (int i=0;i<100000;++i) { 
      URLFP fingerprint = URLUtils.getURLFPFromURL("http://foo.bar.com/" + i,false);
      URLFP notfingerprint = URLUtils.getURLFPFromURL("http://someother.bar.com/" + i,false);
      addedSet.add(fingerprint);
      notAddedSet.add(notfingerprint);
    }
    
    System.out.println("Adding " + addedSet.size() + " elements to bloom filter");
    long timeStart = System.currentTimeMillis();
    for (URLFP testFingerprint : addedSet) { 
      bigBloomFilter.add(testFingerprint); 
    }
    long timeEnd = System.currentTimeMillis();
    System.out.println("Add Took:" + (timeEnd - timeStart) + " MS");
    
    timeStart = System.currentTimeMillis();    
    for (URLFP testFingerprint : addedSet) { 
      if (!bigBloomFilter.isPresent(testFingerprint)) { 
        Assert.assertFalse(true);
      }
    }
    timeEnd = System.currentTimeMillis();
    
    System.out.println("Lookup of " + addedSet.size() + " items in set took:" + (timeEnd - timeStart) + " MS");

    timeStart = System.currentTimeMillis();
    for (URLFP testFingerprint : notAddedSet) { 
      if (bigBloomFilter.isPresent(testFingerprint)) {
        Assert.assertTrue(addedSet.contains(testFingerprint));
      }
    }
    timeEnd = System.currentTimeMillis();
    System.out.println("Lookup of  " + addedSet.size() + " items not in set took:" + (timeEnd - timeStart) + " MS");
    
    System.out.println("Cloning");
    URLFPBloomFilter clone = null; 
    
    timeStart = System.currentTimeMillis();
    try {
      clone = (URLFPBloomFilter) bigBloomFilter.clone();
    } catch (CloneNotSupportedException e1) {
      e1.printStackTrace();
    }
    timeEnd = System.currentTimeMillis();
    
    System.out.println("Clone took:" + (timeEnd - timeStart) + " MS");
    
    
    Path outputLocation = new Path(outputFileName);
    // serialize 
    System.out.println("Serializing to:" + outputLocation);
    
    try {
      timeStart = System.currentTimeMillis();
      FileSystem fs = CrawlEnvironment.getDefaultFileSystem();
      FSDataOutputStream outputStream = fs.create(outputLocation,true,10240000);      
      
      clone.serialize(outputStream);
      
      outputStream.flush();
      outputStream.close();
      
      timeEnd = System.currentTimeMillis();
      System.out.println("Seialization took:" + (timeEnd - timeStart) + " MS");
      
      clone = null;
      bigBloomFilter = null;
      
      System.out.println("Reloading");

      timeStart = System.currentTimeMillis();
      FSDataInputStream inputStream = fs.open(outputLocation);
      bigBloomFilter = URLFPBloomFilter.load(inputStream);
      inputStream.close();
      timeEnd = System.currentTimeMillis();

      System.out.println("Reload took:" + (timeEnd - timeStart) + " MS");
      
      timeStart = System.currentTimeMillis();    
      for (URLFP testFingerprint : addedSet) { 
        if (!bigBloomFilter.isPresent(testFingerprint)) { 
          Assert.assertFalse(true);
        }
      }
      timeEnd = System.currentTimeMillis();
      
      System.out.println("Lookup of " + addedSet.size() + " items in set took:" + (timeEnd - timeStart) + " MS");

      timeStart = System.currentTimeMillis();
      for (URLFP testFingerprint : notAddedSet) { 
        if (bigBloomFilter.isPresent(testFingerprint)) {
          Assert.assertTrue(addedSet.contains(testFingerprint));
        }
      }
      timeEnd = System.currentTimeMillis();
      System.out.println("Lookup of  " + addedSet.size() + " items not in set took:" + (timeEnd - timeStart) + " MS");      
      
      
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public int getNumElements() {
    return numElements;
  }

  public int getBucketsPerElement() {
    return bucketsPerElement;
  }

  public int getHashCount() {
    return hashCount;
  }
}
