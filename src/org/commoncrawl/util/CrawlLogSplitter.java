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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.ValueBytes;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.commoncrawl.protocol.CrawlURL;

/**
 * 
 * @author rana
 *
 */
public class CrawlLogSplitter {

  public static final Log LOG = LogFactory.getLog(CrawlLogSplitter.class);
    
  final static Pattern crawlLogRegExp = Pattern.compile("CrawlLog_ccc[0-9]{2}-[0-9]{2}_([0-9]*)");
  final static TreeSet<Path> candidateList = new TreeSet<Path>(new Comparator<Path>() {

   @Override
   public int compare(Path p1, Path p2) {
     String n1 = p1.getName();
     String n2 = p2.getName();
     Matcher m1 = crawlLogRegExp.matcher(n1);
     Matcher m2 = crawlLogRegExp.matcher(n2);
     m1.matches();
     m2.matches();
     Long   v1 = Long.parseLong(m1.group(1));
     Long   v2 = Long.parseLong(m2.group(1));

     return v1.compareTo(v2); 
   }
  
  });
  
  static Pattern crawlLogRegExp2 = Pattern.compile("CrawlLog_ccc([0-9]{2})-([0-9]{2})_([0-9]*)");

  static Path buildIncrementalPathGivenPathAndIndex(Path tempDir,String baseName,int index)throws IOException { 
    Matcher m = crawlLogRegExp2.matcher(baseName);
    if (m.matches()) {
      return new Path(tempDir,"CrawlLog_ccc"+ m.group(1)+"-"+m.group(2)+"_"+(Long.parseLong(m.group(3)) + (index + 1)));
    }
    throw new IOException("Invalid Base Name:" + baseName);
  }
  
  
  static final long SPLIT_SIZE = 5368709120L;
  static final long IDEAL_SIZE = SPLIT_SIZE / 2;
  
  
  public static void main(String[] args)throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    FileStatus arcFiles[] = fs.globStatus(new Path("crawl/checkpoint_data/CrawlLog_*"));
    for (FileStatus candidate : arcFiles) { 
      if (candidate.getLen() > SPLIT_SIZE) { 
        candidateList.add(candidate.getPath());
      }
    }
    
    LOG.info("Found:" + candidateList.size() + " oversized candidates");
    
    Path tempOutputDir = new Path(conf.get("mapred.temp.dir", "."));
    
    while (candidateList.size() != 0) { 
      Path candidateName = candidateList.first();
      candidateList.remove(candidateName);
      
      LOG.info("Processing Candidate:" + candidateName);
      long fileSize = fs.getFileStatus(candidateName).getLen();
      //get crawl log filename components
      
      ArrayList<Path> splitItems = new ArrayList<Path>();

      int index = 0;
      
      Path outputPart = buildIncrementalPathGivenPathAndIndex(tempOutputDir,candidateName.getName(),index);
      
      LOG.info("Initial Output Path is:"+ outputPart);
      
      fs.delete(outputPart,false);
      
      // create reader 
      SequenceFile.Reader reader = new SequenceFile.Reader(fs, candidateName, conf);
      ValueBytes sourceVB = reader.createValueBytes();
      DataOutputBuffer sourceKeyData = new DataOutputBuffer();
      
      try { 
        // ok create temp file 
        SequenceFile.Writer activeWriter = SequenceFile.createWriter(fs, conf,outputPart, Text.class, CrawlURL.class,CompressionType.BLOCK,new SnappyCodec());
                 
        // add to split items array 
        splitItems.add(outputPart);
        
        try { 
          long recordsWritten = 0;
          while (reader.nextRawKey(sourceKeyData) != -1) { 
            reader.nextRawValue(sourceVB);
            long lengthPreWrite = activeWriter.getLength();
            activeWriter.appendRaw(sourceKeyData.getData(), 0, sourceKeyData.getLength(), sourceVB);
            if (++recordsWritten % 10000 == 0) { 
              LOG.info("Write 10000 records");
            }
            long lengthPostWrite = activeWriter.getLength();
            if (lengthPostWrite != lengthPreWrite) { 
              if (lengthPostWrite >= IDEAL_SIZE) { 
                LOG.info("Hit Split Point. Flushing File:" + outputPart);
                activeWriter.close();
                outputPart = buildIncrementalPathGivenPathAndIndex(tempOutputDir,candidateName.getName(),++index);
                LOG.info("Creating New File:" + outputPart);
                activeWriter = SequenceFile.createWriter(fs, conf,outputPart, Text.class, CrawlURL.class,CompressionType.BLOCK,new SnappyCodec());
                splitItems.add(outputPart);
              }
            }
            sourceKeyData.reset();
          }
        }
        finally { 
          activeWriter.close();
        }
      }
      finally { 
       reader.close(); 
      }
      LOG.info("Rewrote Source:" + candidateName + " into:" + splitItems.size() + " split files");
      for (Path splitItem : splitItems) {
        Path destPath = new Path("crawl/checkpoint_data",splitItem.getName());
        LOG.info("Moving:" + splitItem + " to:" + destPath);
        fs.rename(splitItem,destPath);
      }
      Path sourceMoveLocation = new Path("crawl/checkpoint_data_split",candidateName.getName());
      LOG.info("Moving SOURCE:" + candidateName + " to:"+ sourceMoveLocation);
      fs.rename(candidateName,sourceMoveLocation);
    }
  }
}
