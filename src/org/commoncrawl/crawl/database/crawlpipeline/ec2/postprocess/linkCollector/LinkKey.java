/**
 * Copyright 2012 - CommonCrawl Foundation
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

package org.commoncrawl.crawl.database.crawlpipeline.ec2.postprocess.linkCollector;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.internal.URLUtils;
import org.commoncrawl.util.internal.Tuples.Pair;
import org.commoncrawl.util.shared.ByteArrayUtils;
import org.commoncrawl.util.shared.FlexBuffer;
import org.commoncrawl.util.shared.MurmurHash;
import org.commoncrawl.util.shared.TextBytes;
import org.junit.Assert;

/**
 * 
 * @author rana
 *
 */
public class LinkKey { 
  
  private static final Log LOG = LogFactory.getLog(LinkKey.class);
  
  public enum ComponentId { 
    ROOT_DOMAIN_HASH_COMPONENT_ID,
    DOMAIN_HASH_COMPONENT_ID,
    URL_HASH_COMPONENT_ID,
    TYPE_COMPONENT_ID,
    EXTRA_DATA_COMPONENT_ID
  }
  
  public enum Type { 
    KEY_TYPE_CRAWL_STATUS, 
    KEY_TYPE_HTML_LINK,
    KEY_TYPE_ATOM_LINK,
    KEY_TYPE_RSS_LINK
  }
  
  public static FlexBuffer[] allocateScanArray() { 
    FlexBuffer[] array = new FlexBuffer[ComponentId.values().length];
    for (int i=0;i<array.length;++i) { 
      array[i] = new FlexBuffer();
    }
    return array;
  }
  
  public static TextBytes generateLinkKey(TextBytes url,LinkKey.Type recordType,String md5Bytes) throws IOException { 
    URLFPV2 fp = URLUtils.getURLFPV2FromURL(url.toString());
    if (fp != null) { 
      String key = 
        fp.getRootDomainHash()
        +":"+fp.getDomainHash()
        +":"+fp.getUrlHash()
        +":"+recordType.ordinal() 
        + ":" + ((md5Bytes != null) ? md5Bytes : "");
        
      return new TextBytes(key);
    }
    return null;
  }
  
  public static TextBytes generateLinkKey(URLFPV2 fp,LinkKey.Type recordType,String md5Bytes) throws IOException { 
    if (fp != null) { 
      String key = 
        fp.getRootDomainHash()
        +":"+fp.getDomainHash()
        +":"+fp.getUrlHash()
        +":"+recordType.ordinal()
        +":" + ((md5Bytes != null) ? md5Bytes : "");
        
      return new TextBytes(key);
    }
    return null;
  }
  
  public static TextBytes generateCrawlStatusKey(TextBytes url,long timestamp) throws IOException { 
    URLFPV2 fp = URLUtils.getURLFPV2FromURL(url.toString());
    if (fp != null) { 
      String key = 
        fp.getRootDomainHash()
        +":"+fp.getDomainHash()
        +":"+fp.getUrlHash()
        +":"+Type.KEY_TYPE_CRAWL_STATUS.ordinal() 
        + ":" + timestamp;
        
      return new TextBytes(key);
    }
    return null;
  }

  public static TextBytes generateCrawlStatusKey(URLFPV2 fp,long timestamp) throws IOException { 
    if (fp != null) { 
      String key = 
        fp.getRootDomainHash()
        +":"+fp.getDomainHash()
        +":"+fp.getUrlHash()
        +":"+Type.KEY_TYPE_CRAWL_STATUS.ordinal() 
        + ":" + timestamp;
        
      return new TextBytes(key);
    }
    return null;
  }
  
  public static int scanForComponents(TextBytes key,int terminator,FlexBuffer[] parts) {
    
    int scanPos = key.getOffset();
    int endPos  = key.getOffset() + key.getLength() - 1;
    
    //Pair<Integer,Integer> tupleOut = new Pair<Integer, Integer>(scanPos,0);
    int partCount = 0;
    int tokenStart = key.getOffset();
    byte[] data = key.getBytes();
    do { 
      if (scanPos == endPos || data[scanPos] == terminator) {
        if(data[scanPos] == terminator) 
          parts[partCount++].set(data,tokenStart,scanPos-tokenStart);
        else 
          parts[partCount++].set(data,tokenStart,scanPos-tokenStart + 1);
        tokenStart = scanPos + 1;
      }
      scanPos++;
    }while (scanPos <= endPos && partCount < parts.length);
    
    if (scanPos <= endPos) { 
      LOG.error("scanPos:" + scanPos + "<= endPos:" + endPos + " partCount:"+ partCount + " for key:" + key.toString()); 
    }
    return partCount;
  }
  
  public static Pair<Integer,Integer> scanAndTerminateOn(byte[] data,int offset,int length,int terminator,int targetHitCount) {
    int scanPos = offset;
    int endPos  = offset + length;
    int hitCount = 0;
    
    Pair<Integer,Integer> tupleOut = new Pair<Integer, Integer>(scanPos,0);
    
    while (scanPos != endPos) {
      if (data[scanPos] == terminator) { 
        if (++hitCount == targetHitCount)
          break;
        else {
          tupleOut.e0 = scanPos + 1;
          scanPos++;
        }
      }
      else {  
        scanPos++;
      }
    }
    tupleOut.e1 = scanPos - 1;
    

    return tupleOut;
  }
  
  public static class LinkKeyPartitioner implements Partitioner<TextBytes, TextBytes> {

    static int hashCodeFromKey(TextBytes key) { 
      int result = 1;
      result = MurmurHash.hashLong(getLongComponentFromKey(key, ComponentId.DOMAIN_HASH_COMPONENT_ID),result);
      result = MurmurHash.hashLong(getLongComponentFromKey(key, ComponentId.URL_HASH_COMPONENT_ID),result); 
      
      return result;
    }
    
    @Override
    public int getPartition(TextBytes key, TextBytes value, int numPartitions) {
      return (hashCodeFromKey(key) & Integer.MAX_VALUE) % numPartitions;
    }

    @Override
    public void configure(JobConf job) {
      
    } 
    
  }
  
  public static class LinkKeyGroupingComparator implements RawComparator<TextBytes> {

    TextBytes key1 = new TextBytes();
    TextBytes key2 = new TextBytes();
    
    FlexBuffer scanArray1[] = allocateScanArray();
    FlexBuffer scanArray2[] = allocateScanArray();
    
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      key1.set(b1, s1, l1);
      key2.set(b2, s2, l2);
      
      return compare(key1,key2);
    }

    @Override
    public int compare(TextBytes o1, TextBytes o2) {

      scanForComponents(o1, ':',scanArray1);
      scanForComponents(o2, ':',scanArray2);
      
      //long domain1Key = getLongComponentFromKey(o1,LinkKey.ComponentId.DOMAIN_HASH_COMPONENT_ID);
      //long domain2Key = getLongComponentFromKey(o2,LinkKey.ComponentId.DOMAIN_HASH_COMPONENT_ID);
      long domain1Key = getLongComponentFromComponentArray(scanArray1,ComponentId.DOMAIN_HASH_COMPONENT_ID);
      long domain2Key = getLongComponentFromComponentArray(scanArray2,ComponentId.DOMAIN_HASH_COMPONENT_ID);
      
      int result = (domain1Key < domain2Key) ? -1 : (domain1Key > domain2Key) ? 1 : 0;
      
      if (result == 0) { 
        
        //long hash1Key = getLongComponentFromKey(o1,LinkKey.ComponentId.URL_HASH_COMPONENT_ID);
        //long hash2Key = getLongComponentFromKey(o2,LinkKey.ComponentId.URL_HASH_COMPONENT_ID);
        
        long hash1Key = getLongComponentFromComponentArray(scanArray1,ComponentId.URL_HASH_COMPONENT_ID);
        long hash2Key = getLongComponentFromComponentArray(scanArray2,ComponentId.URL_HASH_COMPONENT_ID);
        
        
        result = (hash1Key < hash2Key) ? -1 : (hash1Key > hash2Key) ? 1 : 0;
      }
      
      return result;
    }
  }
  
  public static class LinkKeyComparator implements RawComparator<TextBytes> {

    TextBytes key1 = new TextBytes();
    TextBytes key2 = new TextBytes();
    
    FlexBuffer scanArray1[] = allocateScanArray();
    FlexBuffer scanArray2[] = allocateScanArray();
    
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      key1.set(b1, s1, l1);
      key2.set(b2, s2, l2);
      
      return compare(key1,key2);
    }

    @Override
    public int compare(TextBytes o1, TextBytes o2) {

      scanForComponents(o1, ':',scanArray1);
      scanForComponents(o2, ':',scanArray2);
      
      //long domain1Key = getLongComponentFromKey(o1,LinkKey.ComponentId.DOMAIN_HASH_COMPONENT_ID);
      //long domain2Key = getLongComponentFromKey(o2,LinkKey.ComponentId.DOMAIN_HASH_COMPONENT_ID);
      long domain1Key = getLongComponentFromComponentArray(scanArray1,ComponentId.DOMAIN_HASH_COMPONENT_ID);
      long domain2Key = getLongComponentFromComponentArray(scanArray2,ComponentId.DOMAIN_HASH_COMPONENT_ID);
      
      int result = (domain1Key < domain2Key) ? -1 : (domain1Key > domain2Key) ? 1 : 0;
      
      if (result == 0) { 
        
        //long hash1Key = getLongComponentFromKey(o1,LinkKey.ComponentId.URL_HASH_COMPONENT_ID);
        //long hash2Key = getLongComponentFromKey(o2,LinkKey.ComponentId.URL_HASH_COMPONENT_ID);
        
        long hash1Key = getLongComponentFromComponentArray(scanArray1,ComponentId.URL_HASH_COMPONENT_ID);
        long hash2Key = getLongComponentFromComponentArray(scanArray2,ComponentId.URL_HASH_COMPONENT_ID);
        
        
        result = (hash1Key < hash2Key) ? -1 : (hash1Key > hash2Key) ? 1 : 0;
      }
      
      if (result == 0) { 
        
        //long type1 = getLongComponentFromKey(o1,LinkKey.ComponentId.TYPE_COMPONENT_ID);
        //long type2 = getLongComponentFromKey(o2,LinkKey.ComponentId.TYPE_COMPONENT_ID);
        
        long type1 = getLongComponentFromComponentArray(scanArray1,ComponentId.TYPE_COMPONENT_ID);
        long type2 = getLongComponentFromComponentArray(scanArray2,ComponentId.TYPE_COMPONENT_ID);
        
        result = (type1 < type2) ? -1 : (type1 > type2) ? 1 : 0;
      
        if (result == 0) { 
          if (type1 == LinkKey.Type.KEY_TYPE_CRAWL_STATUS.ordinal()) { 
            //long timestamp1 = getLongComponentFromKey(o1,LinkKey.ComponentId.EXTRA_DATA_COMPONENT_ID);
            //long timestamp2 = getLongComponentFromKey(o2,LinkKey.ComponentId.EXTRA_DATA_COMPONENT_ID);

            long timestamp1 = getLongComponentFromComponentArray(scanArray1,ComponentId.EXTRA_DATA_COMPONENT_ID);
            long timestamp2 = getLongComponentFromComponentArray(scanArray2,ComponentId.EXTRA_DATA_COMPONENT_ID);
            
            result = (timestamp1 < timestamp2) ? -1 : (timestamp1 > timestamp2) ? 1 : 0;
            
          }
          else { 
            //FlexBuffer bytes1 = getByteArrayComponentFromKey(o1, LinkKey.ComponentId.EXTRA_DATA_COMPONENT_ID);
            //FlexBuffer bytes2 = getByteArrayComponentFromKey(o2, LinkKey.ComponentId.EXTRA_DATA_COMPONENT_ID);
            
            FlexBuffer bytes1 = getByteArrayFromComponentArray(scanArray1, LinkKey.ComponentId.EXTRA_DATA_COMPONENT_ID);
            FlexBuffer bytes2 = getByteArrayFromComponentArray(scanArray2, LinkKey.ComponentId.EXTRA_DATA_COMPONENT_ID);
            
            result = bytes1.compareTo(bytes2);
          }
        }
      }
      return result;
    } 
  }
  
  public static long getLongComponentFromComponentArray(FlexBuffer[] array,ComponentId componentId) { 
    int index = componentId.ordinal();
    return ByteArrayUtils.parseLong(array[index].get(),array[index].getOffset(), array[index].getCount(), 10);
  }
  
  public static long getLongComponentFromKey(TextBytes key,ComponentId componentId) { 
    byte[] data = key.getBytes();
    int offset  = key.getOffset();
    int length  = key.getLength();
    
    long startTime = System.nanoTime();
    Pair<Integer,Integer> scanResult = scanAndTerminateOn(data, offset, length, ':', componentId.ordinal() + 1);
    
    long result = ByteArrayUtils.parseLong(data, scanResult.e0, scanResult.e1 - scanResult.e0 + 1, 10);
    long endTime = System.nanoTime();
    
    return result;
  }

  public static FlexBuffer getByteArrayFromComponentArray(FlexBuffer[] array,ComponentId componentId) { 
    return array[componentId.ordinal()];
  }
  
  public static FlexBuffer getByteArrayComponentFromKey(TextBytes key,ComponentId componentId) { 
    byte[] data = key.getBytes();
    int offset  = key.getOffset();
    int length  = key.getLength();
    
    Pair<Integer,Integer> scanResult = scanAndTerminateOn(data, offset, length, ':', componentId.ordinal() + 1);
    
    return new FlexBuffer(data, scanResult.e0, scanResult.e1 - scanResult.e0 + 1);
  }
  
  
  public static void main(String[] args) throws IOException {
    // allocate scan arrays 
    FlexBuffer[] scanArray = allocateScanArray();
    
    URLFPV2 fp = URLUtils.getURLFPV2FromURL("http://www.google.com/");
    if (fp != null) { 
        TextBytes key = generateLinkKey(fp,LinkKey.Type.KEY_TYPE_HTML_LINK,"FOOBAR");
        // get it the hard way
        scanForComponents(key,':',scanArray);
        
        System.out.println("Key is:" + key.toString());
        System.out.println("Check Root Domain Key");
        Assert.assertTrue(fp.getRootDomainHash() == getLongComponentFromKey(key, LinkKey.ComponentId.ROOT_DOMAIN_HASH_COMPONENT_ID));
        Assert.assertTrue(fp.getRootDomainHash() == getLongComponentFromComponentArray(scanArray,LinkKey.ComponentId.ROOT_DOMAIN_HASH_COMPONENT_ID));
        System.out.println("Check Domain Key");
        Assert.assertTrue(fp.getDomainHash() == getLongComponentFromKey(key, LinkKey.ComponentId.DOMAIN_HASH_COMPONENT_ID));
        Assert.assertTrue(fp.getDomainHash() == getLongComponentFromComponentArray(scanArray,LinkKey.ComponentId.DOMAIN_HASH_COMPONENT_ID));
        System.out.println("Check URL Hash Key");
        Assert.assertTrue(fp.getUrlHash() == getLongComponentFromKey(key, LinkKey.ComponentId.URL_HASH_COMPONENT_ID));
        Assert.assertTrue(fp.getUrlHash() == getLongComponentFromComponentArray(scanArray,LinkKey.ComponentId.URL_HASH_COMPONENT_ID));
        System.out.println("Check Type");
        Assert.assertTrue(LinkKey.Type.KEY_TYPE_HTML_LINK.ordinal() == getLongComponentFromKey(key, LinkKey.ComponentId.TYPE_COMPONENT_ID));
        Assert.assertTrue(LinkKey.Type.KEY_TYPE_HTML_LINK.ordinal() == getLongComponentFromComponentArray(scanArray,LinkKey.ComponentId.TYPE_COMPONENT_ID));
        System.out.println("Check ExtraData");
        Assert.assertTrue(new FlexBuffer("FOOBAR".getBytes()).compareTo(getByteArrayComponentFromKey(key, LinkKey.ComponentId.EXTRA_DATA_COMPONENT_ID)) == 0);
        Assert.assertTrue(new FlexBuffer("FOOBAR".getBytes()).compareTo(getByteArrayFromComponentArray(scanArray, LinkKey.ComponentId.EXTRA_DATA_COMPONENT_ID)) == 0);
        
        TextBytes statusKey1 = generateCrawlStatusKey(new TextBytes("http://www.google.com/"),12345L);
        TextBytes statusKey2 = generateCrawlStatusKey(URLUtils.getURLFPV2FromURL("http://www.google.com/"),12345L);
        TextBytes statusKey3 = generateCrawlStatusKey(URLUtils.getURLFPV2FromURL("http://www.google.com/"),12346L);
        TextBytes linkKey1   = generateLinkKey(URLUtils.getURLFPV2FromURL("http://www.google.com/"),LinkKey.Type.KEY_TYPE_HTML_LINK,MD5Hash.digest("123").toString());
        TextBytes linkKey2   = generateLinkKey(URLUtils.getURLFPV2FromURL("http://www.google.com/"),LinkKey.Type.KEY_TYPE_HTML_LINK,MD5Hash.digest("1234").toString());
        URLFPV2 fpLink3 = URLUtils.getURLFPV2FromURL("http://www.google.com/");
        fpLink3.setUrlHash(fpLink3.getUrlHash() + 1);
        TextBytes linkKey3   = generateLinkKey(fpLink3,LinkKey.Type.KEY_TYPE_HTML_LINK,"12345");
        TextBytes linkKey4   = generateLinkKey(URLUtils.getURLFPV2FromURL("http://www.google.com/"),LinkKey.Type.KEY_TYPE_ATOM_LINK,"1234");
        TextBytes linkKey5   = generateLinkKey(fpLink3,LinkKey.Type.KEY_TYPE_ATOM_LINK,"12345");
        
        LinkKeyComparator comparator = new LinkKeyComparator();
        LinkKeyGroupingComparator gcomparator = new LinkKeyGroupingComparator();
        
        System.out.println("Comparing Similar status Keys");
        Assert.assertTrue(comparator.compare(statusKey1,statusKey2) == 0);
        Assert.assertTrue(comparator.compare(statusKey2,statusKey1) == 0);
        System.out.println("Comparing Similar status Keys w/Grouping C");
        Assert.assertTrue(gcomparator.compare(statusKey1,statusKey2) == 0);
        Assert.assertTrue(gcomparator.compare(statusKey2,statusKey1) == 0);
        System.out.println("Comparing Similar status Keys with different timestamps");
        Assert.assertTrue(comparator.compare(statusKey2,statusKey3) == -1);
        Assert.assertTrue(comparator.compare(statusKey3,statusKey2) == 1);
        System.out.println("Comparing Similar status Keys with different timestamps w/Grouping C");
        Assert.assertTrue(gcomparator.compare(statusKey2,statusKey3) == 0);
        Assert.assertTrue(gcomparator.compare(statusKey3,statusKey2) == 0);
        System.out.println("Comparing Status Key to Link Key");
        Assert.assertTrue(comparator.compare(statusKey1,linkKey1) == -1);
        Assert.assertTrue(comparator.compare(linkKey1,statusKey1) == 1);
        System.out.println("Comparing Status Key to Link Key Grouping C");
        Assert.assertTrue(gcomparator.compare(statusKey1,linkKey1) == 0);
        Assert.assertTrue(gcomparator.compare(linkKey1,statusKey1) == 0);
        System.out.println("Comparing TWO Link Keys with same hash value");
        Assert.assertTrue(comparator.compare(linkKey1,linkKey1) == 0);
        Assert.assertTrue(comparator.compare(linkKey1,linkKey1) == 0);
        System.out.println("Comparing TWO Link Keys with same type but different hash values");
        Assert.assertTrue(comparator.compare(linkKey2,linkKey3) == -1);
        Assert.assertTrue(comparator.compare(linkKey3,linkKey2) == 1);
        System.out.println("Comparing TWO Link Keys with same type but different hash values - Grouping  C");
        Assert.assertTrue(gcomparator.compare(linkKey2,linkKey3) == -1);
        Assert.assertTrue(gcomparator.compare(linkKey3,linkKey2) == 1);
        System.out.println("Comparing TWO Link Keys with different types but same hash values");
        Assert.assertTrue(comparator.compare(linkKey2,linkKey4) == -1);
        Assert.assertTrue(comparator.compare(linkKey4,linkKey2) == 1);
        System.out.println("Comparing TWO Link Keys with different types but same hash values - Grouping C ");
        Assert.assertTrue(gcomparator.compare(linkKey2,linkKey4) == 0);
        Assert.assertTrue(gcomparator.compare(linkKey4,linkKey2) == 0);
        System.out.println("Comparing TWO Link Keys with similar types but different hash values");
        Assert.assertTrue(comparator.compare(linkKey4,linkKey5) == -1);
        Assert.assertTrue(comparator.compare(linkKey5,linkKey4) == 1);
        System.out.println("Comparing TWO Link Keys with similar types but different hash values - Grouping C");
        Assert.assertTrue(gcomparator.compare(linkKey4,linkKey5) == -1);
        Assert.assertTrue(gcomparator.compare(linkKey5,linkKey4) == 1);
        
        
        TextBytes linkKeyTest   = generateLinkKey(URLUtils.getURLFPV2FromURL("http://www.google.com/"),LinkKey.Type.KEY_TYPE_HTML_LINK,"");
        Assert.assertTrue(scanForComponents(linkKeyTest, ':',scanArray) == scanArray.length -1);
        for (FlexBuffer buffer : scanArray)
          LOG.info("Scan Item:" + buffer.toString());
        TextBytes linkKeyTest2   = generateLinkKey(URLUtils.getURLFPV2FromURL("http://www.google.com/"),LinkKey.Type.KEY_TYPE_HTML_LINK,MD5Hash.digest("REALLY LONG SOMETHING OR ANOTHER").toString());
        Assert.assertTrue(scanForComponents(linkKeyTest2, ':',scanArray) == scanArray.length);
        for (FlexBuffer buffer : scanArray)
          LOG.info("Scan Item:" + buffer.toString());
        
    }
    else { 
      System.out.println("Invalid URL:" + args[0]);
    }
  }
}
