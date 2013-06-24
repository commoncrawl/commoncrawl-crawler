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

import java.io.IOException;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;

public class JoinByTextSortByTagMapper extends JoinMapper {

  private static final Log LOG = LogFactory.getLog(JoinByTextSortByTagMapper.class);

  public static final char tagDelimiter = '|';
  public JoinByTextSortByTagMapper() { 
  }
  
  DataOutputBuffer compositeBuffer = new DataOutputBuffer();
  TextBytes keyOut = new TextBytes();
  
  @Override
  public WritableComparable mapKey(WritableComparable key,JoinValue value)throws IOException { 
    
    makeCompositeKey(compositeBuffer, (TextBytes)key, value._tag, keyOut);
    
    return keyOut;
  }
  
  public static void makeCompositeKey(DataOutputBuffer compositeBuffer,TextBytes textKey,TextBytes tagValue,TextBytes textOut)throws IOException { 
    compositeBuffer.reset();
    compositeBuffer.write(textKey.getBytes(),0,textKey.getLength());
    compositeBuffer.write(tagDelimiter);
    compositeBuffer.write(tagValue.getBytes(),0,tagValue.getLength());
    
    textOut.set(compositeBuffer.getData(), 0, compositeBuffer.getLength());
    
  }
  public static void getKeyFromCompositeKey(TextBytes compositeKey,TextBytes textObject)throws IOException { 
    byte searchTerm[] = {  tagDelimiter };
    int indexPos = ByteArrayUtils.indexOf(compositeKey.getBytes(), compositeKey.getOffset(), compositeKey.getLength(), searchTerm);
    if (indexPos == -1) {
      throw new IOException("Search Term Not Found in Key:" + compositeKey.toString());
    }
    textObject.set(compositeKey.getBytes(), compositeKey.getOffset(), indexPos - compositeKey.getOffset());
  }

  public static void getTagFromCompositeKey(TextBytes compositeKey,TextBytes textObject)throws IOException { 
    byte searchTerm[] = {  tagDelimiter };
    int indexPos = ByteArrayUtils.indexOf(compositeKey.getBytes(), compositeKey.getOffset(), compositeKey.getLength(), searchTerm);
    if (indexPos == -1) {
      throw new IOException("Tag Term Not Found in Key:" + compositeKey.toString());
    }
    textObject.set(compositeKey.getBytes(), indexPos + 1,(compositeKey.getLength() + compositeKey.getOffset()) - (indexPos+1));
  }
 
  
  public static class Partitioner implements org.apache.hadoop.mapred.Partitioner<TextBytes,JoinValue> {

    TextBytes keyBuffer = new TextBytes();
    
    @Override
    public int getPartition(TextBytes key, JoinValue value, int numPartitions) {
      try {
        getKeyFromCompositeKey(key, keyBuffer);
      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
        throw new RuntimeException(e);
      }
      return (keyBuffer.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }

    @Override
    public void configure(JobConf job) {
      
    } 
    
  }
  public static void main(String[] args)throws IOException {
    DataOutputBuffer outputBuffer = new DataOutputBuffer();
    TextBytes textKey = new TextBytes("test");
    TextBytes tagValue = new TextBytes("tag");
    TextBytes textOut = new TextBytes();
    
    makeCompositeKey(outputBuffer, textKey, tagValue, textOut);
    System.out.println("CompositeKey:" +textOut.toString());
    
    TextBytes textKeyOut = new TextBytes();
    TextBytes tagValueOut = new TextBytes();
    
    getKeyFromCompositeKey(textOut, textKeyOut);
    getTagFromCompositeKey(textOut, tagValueOut);
    
    Assert.assertTrue(textKey.compareTo(textKeyOut) == 0);
    Assert.assertTrue(tagValue.compareTo(tagValueOut) == 0);
  }
}
