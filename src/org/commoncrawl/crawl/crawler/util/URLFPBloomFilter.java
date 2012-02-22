/**
 * Copyright 2008 - CommonCrawl Foundation
 * 
 * CommonCrawl licenses this file to you under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.commoncrawl.crawl.crawler.util;

import java.io.DataInputStream;
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
import org.commoncrawl.util.internal.URLUtils;
import org.commoncrawl.util.shared.MurmurHash;

/** 
 * The OLD BloomFilter implementation used by the crawler redirect bloomfilter code.
 *  
 * @author rana
 *
 */
public class URLFPBloomFilter implements Cloneable {
  

  long nbits  = 0;
  int  numElements = 0;
  int  bucketsPerElement = 0;
  int  hashCount = 0;
  byte bits[] = null;
  long hashResults[] = null;
  

  
  public URLFPBloomFilter(int numElements,int hashCount, int bucketsPerElement){
    this.numElements = numElements;
    this.bucketsPerElement = bucketsPerElement;
    this.hashCount = hashCount; 
    this.nbits = (long)numElements * (long)bucketsPerElement + 20;
    System.out.println("Number of Bits is:" + nbits);
    System.out.println("Number of Bytes is:" + nbits / 8);
    this.bits  = new byte[(int)(((nbits + 7) / 8))];
    this.hashResults = new long[hashCount];
  }
  
  public final synchronized boolean isPresent(URLFP key) {
    if (key == null) 
      return false;
    for (long bucketIndex : getHashBuckets(key,hashCount,nbits)){
      
      if (((bits[(int)(bucketIndex >> 3)] >> (bucketIndex & 0x7)) & 0x1) != 1) {
      //if (((bits.get((int)(bucketIndex >> 3)) >> (bucketIndex & 0x7))& 0x1) != 1) { 
        return false;
      }
      
      /*
      if (!bitset.get(bucketIndex)) { 
        return false;
      }
      */
    }
    return true;
  }
  
  public final synchronized void add(URLFP key) {
     
        if (key != null) { 
          for (long bucketIndex : getHashBuckets(key,hashCount,nbits)) {
            bits[(int)(bucketIndex >> 3)] |= (1 << (bucketIndex & 0x7));
            //bitset.set(bucketIndex);
            //bits.put((int)(bucketIndex >> 3), (byte) (bits.get((int)(bucketIndex >> 3)) | (1 << (bucketIndex & 0x7)))); 
          }
        }
  }  
  
  public final synchronized void serialize(OutputStream outputStream) throws IOException { 

    DataOutputStream dataOut = new DataOutputStream(outputStream);
    dataOut.writeInt(0); // version placeholder
    dataOut.writeInt(numElements);
    dataOut.writeInt(hashCount);
    dataOut.writeInt(bucketsPerElement);
    outputStream.write(bits);
    outputStream.flush();
  }

  public static URLFPBloomFilter load(InputStream inputStream) throws IOException { 
    DataInputStream dataIn = new DataInputStream(inputStream);
    // skip version bytes ... 
    dataIn.readInt();
    // initialize filter ... 
    URLFPBloomFilter filter = new URLFPBloomFilter(dataIn.readInt(),dataIn.readInt(),dataIn.readInt());
    
    if (inputStream instanceof FSDataInputStream) {
      ((FSDataInputStream)inputStream).readFully(filter.bits);
    }
    else { 
      // load bits ... 
      inputStream.read(filter.bits);
    }
    return filter;
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    URLFPBloomFilter cloned = new URLFPBloomFilter(this.numElements,this.hashCount,this.bucketsPerElement);    
    System.arraycopy(this.bits, 0, cloned.bits, 0, this.bits.length);
    return cloned;
  }
  
  
  
  final long[] getHashBuckets(URLFP key, int hashCount, long max) {
      byte[] b = new byte[16];
      b[0] = (byte)((key.getDomainHash() >>> 24) & 0xFF);
      b[1] = (byte)((key.getDomainHash() >>> 16) & 0xFF);
      b[2] = (byte)((key.getDomainHash() >>> 8) & 0xFF);
      b[3] = (byte)((key.getDomainHash() >>> 32) & 0xFF);
      b[4] = (byte)((key.getDomainHash()) & 0xFF);
      
      b[5] = (byte)((key.getUrlHash() >>> 56) & 0xFF);
      b[6] = (byte)((key.getUrlHash()>>> 48) & 0xFF);
      b[7] = (byte)((key.getUrlHash() >>> 40) & 0xFF);
      b[8] = (byte)((key.getUrlHash() >>> 32) & 0xFF);
      b[9] = (byte)((key.getUrlHash() >>> 24) & 0xFF);
      b[10] = (byte)((key.getUrlHash() >>> 16) & 0xFF);
      b[11] = (byte)((key.getUrlHash() >>> 8) & 0xFF);
      b[12] = (byte)((key.getUrlHash()) & 0xFF);
      
      
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
