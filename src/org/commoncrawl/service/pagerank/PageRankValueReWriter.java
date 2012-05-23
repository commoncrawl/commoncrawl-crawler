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

package org.commoncrawl.service.pagerank;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.text.NumberFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.WritableUtils;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.protocol.URLFP;
import org.commoncrawl.util.CCStringUtils;

public class PageRankValueReWriter {

  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }    
  
  
  public static final Log LOG = LogFactory.getLog(PageRankValueReWriter.class);

  public static void main(String[] args) {

    
    int    nodeIndex       = Integer.parseInt(args[0]);
    LOG.info("Node Index:" + args[0]);
    int    nodeCount       = Integer.parseInt(args[1]);
    LOG.info("Node Count:" + args[1]);
    String idsDirectory = args[2];
    LOG.info("ID Directory is:" + args[2]);
    String valuesDirectory = args[3];
    LOG.info("Values Directory is:" + args[3]);
    int    iterationNumber = Integer.parseInt(args[4]);
    LOG.info("Iteration Number is:" + args[4]);
    int    runDate = Integer.parseInt(args[5]);
    LOG.info("runDate is:" + args[5]);
    
    Configuration conf = new Configuration();
    
    conf.addResource("nutch-default.xml");
    conf.addResource("nutch-site.xml");
    conf.addResource("hadoop-default.xml");
    conf.addResource("hadoop-site.xml");
    conf.addResource("commoncrawl-default.xml");
    conf.addResource("commoncrawl-site.xml");
    
    CrawlEnvironment.setHadoopConfig(conf);
    CrawlEnvironment.setDefaultHadoopFSURI("hdfs://ccn01:9000/");


    try { 
      FileSystem fileSystem = CrawlEnvironment.getDefaultFileSystem();

      
      Path outputPath = new Path("crawl/pageRank/out",Integer.toString(runDate));
      LOG.info("Output Directory is:"+ outputPath);
      
      fileSystem.mkdirs(outputPath);
      
      
      //iterate values based on node id 
      for (int i=nodeIndex;i<nodeIndex+1;++i) { 
        
        LOG.info("Processing output for Node:" + i);
        Path valuePath = new Path(valuesDirectory,"value_"+ NUMBER_FORMAT.format(iterationNumber) + "-" + NUMBER_FORMAT.format(i));
        LOG.info("Value File Path is:" + valuePath);
        Path idsPath   = new Path(idsDirectory,"ids_"+ NUMBER_FORMAT.format(i));
        LOG.info("IDs File Path is:" + idsPath);
        Path outputFile = new Path(outputPath,"part-" + NUMBER_FORMAT.format(i));
        LOG.info("Output File Path is:" + outputFile);
        byte[] valueData = null;
        { 
          FileStatus valueFileStatus = fileSystem.getFileStatus(valuePath);
          FSDataInputStream valueInputStream = fileSystem.open(valuePath);
          LOG.info("Allocating Value Array of Size:" + valueFileStatus.getLen());
          valueData = new byte[(int) valueFileStatus.getLen()];
          LOG.info("Reading Value Data Size:" + valueFileStatus.getLen());
          for (int offset=0,totalRead=0;offset<valueFileStatus.getLen();) { 
            int bytesToRead = Math.min(16384,(int)valueFileStatus.getLen() - totalRead);
            valueInputStream.read(valueData,offset,bytesToRead);
            offset+= bytesToRead;
            totalRead += bytesToRead;
          }
          valueInputStream.close();
          LOG.info("Finished Reading Value Data Size:" + valueFileStatus.getLen());
        }
        
        byte[] idData = null;
        { 
          FileStatus idFileStatus = fileSystem.getFileStatus(idsPath);
          FSDataInputStream idInputStream = fileSystem.open(idsPath);
          LOG.info("Allocating ID Array of Size:" + idFileStatus.getLen());
          idData = new byte[(int) idFileStatus.getLen()];
          LOG.info("Reading ID Array  Data Size:" + idFileStatus.getLen());
          for (int offset=0,totalRead=0;offset<idFileStatus.getLen();) { 
            int bytesToRead = Math.min(16384,(int)idFileStatus.getLen() - totalRead);
            idInputStream.read(idData,offset,bytesToRead);
            offset+= bytesToRead;
            totalRead += bytesToRead;
          }
          idInputStream.close();
          LOG.info("Finished Reading ID Array Data Size:" + idFileStatus.getLen());

        }
        
        DataInputStream idInputStream = new DataInputStream(new ByteArrayInputStream(idData));
        DataInputStream valueInputStream = new DataInputStream(new ByteArrayInputStream(valueData));
        
        SequenceFile.Writer output = SequenceFile.createWriter(fileSystem,conf,outputFile,URLFP.class,VIntWritable.class);
        LOG.info("Opened Output Stream");
        
        URLFP currentFP = new URLFP();
        boolean eof = false;
        int itemCount = 0;
        while (!eof) { 
          
          try {
            long timeStart = System.currentTimeMillis();
           currentFP.readFields(idInputStream);
            long timeEnd = System.currentTimeMillis();
            // LOG.info("ReadFields Took:" + (timeEnd - timeStart));
           ++itemCount;
          }
          catch (EOFException e) { 
            LOG.info("EOF reached. Total Item Count:" + itemCount);
            eof = true;
          }
          
          if (!eof) { 
            long valueFingerprint = WritableUtils.readVLong(valueInputStream);
            
            int prValue = valueInputStream.readInt();
            
            if (valueFingerprint != currentFP.getUrlHash()) { 
              throw new IOException("Fingerprint Mismatch! Expected:" + currentFP.getUrlHash() + " Found:" + valueFingerprint + " ItemCount:" + itemCount);
            }
            
            output.append(currentFP, new VIntWritable(prValue));
            
            if (itemCount % 10000 == 0) { 
              LOG.info("Processed " + itemCount + " Values. Last Sampled FP:" + valueFingerprint + " With PR:" + prValue);
            }
            currentFP = new URLFP();
          }
        }
        LOG.info("Done outputing pagerank for Node:" + i + " ItemCount:" + itemCount);
        
        valueInputStream.close();
        idInputStream.close();
        output.close();
      }
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
    }
  }
  
}
