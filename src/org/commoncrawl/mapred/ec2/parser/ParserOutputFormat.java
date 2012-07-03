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


package org.commoncrawl.mapred.ec2.parser;

import java.io.IOException;
import java.text.NumberFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.record.Buffer;
import org.apache.hadoop.util.Progressable;
import org.commoncrawl.io.NIOHttpHeaders;
import org.commoncrawl.mapred.ArcFileContentItem;
import org.commoncrawl.protocol.ArchiveInfo;
import org.commoncrawl.protocol.ParseOutput;
import org.commoncrawl.protocol.shared.ArcFileHeaderItem;
import org.commoncrawl.protocol.shared.ArcFileItem;
import org.commoncrawl.util.ArcFileWriter;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.FlexBuffer;
import org.commoncrawl.util.ArcFileWriter.CompressedStream;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/** 
 * OutputFormat that splits the output from the ParseMapper into a bunch of 
 * distinct files, including: 
 * 
 * (1) A metadata file, that contains the crawl status, the http headers, the
 * meta tags,title,links (if HTML), and Feed related data (if RSS/ATOM) in a 
 * convenient JSON format. Key is URL.
 * (2) A bunch of ARC files generated in the same manner as the old code used 
 * to generate content for the old crawl.
 * (3) ARCMetadata - A file that contains ArcMetadata structures,keyed off of 
 * URL that help locate content within the set of ARC Files.
 * (4) TextContent - A set of files representing the full-text (HTML stripped)
 * content for all HTML / Feed content encountered during the crawl. Note: We 
 * currently don't emit text content for other mime types. This will come later.
 * (5) RawContent - As an experiment, a copy of the RAW content emitted via the 
 * ARC files, but in convenient SequenceFileFormat. The hope is that the 
 * community <em>might</em> decide to migrate to SEQ files once they are 
 * available (vs. the less standard ARC files).
 * 
 * These files are emitted into "segments", which basically group a bucnh of 
 * CrawlLog files into a unit for the purposes of EMR job management and error
 * recovery.
 * 
 * @author rana
 *
 */
public class ParserOutputFormat extends FileOutputFormat<Text,ParseOutput> {

  public static final Log LOG = LogFactory.getLog(ParserOutputFormat.class);
  
  public static final String METADATA_FILE_PREFIX   = "metadata-";
  public static final String TEXTDATA_FILE_PREFIX   = "textData-";
  public static final String ARC_FILE_PREFIX        = "arcContent-";
  
  private  static NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  static { 
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }
  
  @Override
  public RecordWriter<Text, ParseOutput> getRecordWriter(FileSystem fs,JobConf job, String name, Progressable progress) throws IOException {
    return new MultiFileRecordWriter(fs,job,name,progress);
  }  

  public static class MultiFileRecordWriter implements RecordWriter<Text, ParseOutput> {

    int _partition;
    SequenceFile.Writer _metadataWriter = null;
    SequenceFile.Writer _textWriter = null;
    ArcFileWriter       _arcWriter = null;
    
    public MultiFileRecordWriter(FileSystem fs,JobConf conf,String name,Progressable progress)throws IOException {
      // get partition number ...
      _partition = conf.getInt("mapred.task.partition",-1);
      if (_partition == -1) { 
        throw new IOException("Invalid Partition ID in Job Config!");
      }
      // get work output path ... 
      Path outputPath = FileOutputFormat.getWorkOutputPath(conf);
      // create output paths ... 
      
      final Path metadataPath = new Path(outputPath,METADATA_FILE_PREFIX + NUMBER_FORMAT.format(_partition));
      final Path textContentPath = new Path(outputPath,TEXTDATA_FILE_PREFIX + NUMBER_FORMAT.format(_partition));
      
      _metadataWriter = SequenceFile.createWriter(
          fs, 
          conf, 
          metadataPath, 
          Text.class, 
          Text.class, 
          65536*10,
          fs.getDefaultReplication(), 
          fs.getDefaultBlockSize(),
          CompressionType.BLOCK, 
          new GzipCodec(),          
          progress,
          new Metadata());
      
      
     _textWriter = SequenceFile.createWriter(
          fs, 
          conf, 
          textContentPath, 
          Text.class, 
          Text.class, 
          65536*10,
          fs.getDefaultReplication(), 
          fs.getDefaultBlockSize(),
          CompressionType.BLOCK, 
          new SnappyCodec(),          
          progress,
          new Metadata());
            
      _arcWriter = new ArcFileWriter(fs,outputPath,_partition,1);
    }
    
    @Override
    public void close(Reporter reporter) throws IOException {
      if (_metadataWriter != null) {
        try { 
          _metadataWriter.close();
        }
        catch (Exception e) { 
          LOG.error(CCStringUtils.stringifyException(e));
        }
      }
      
      if (_textWriter != null) { 
        try { 
          _textWriter.close();
        }
        catch (Exception e) { 
          LOG.error(CCStringUtils.stringifyException(e));
        }
      }      
      if (_arcWriter != null) { 
        try { 
          _arcWriter.close(false);
        }
        catch (Exception e) { 
          LOG.error(CCStringUtils.stringifyException(e));
        }
      }      
      
    }

    DataOutputBuffer contentBufferOut = new DataOutputBuffer();
    
    ArcFileItem buildArcFileItemFromParseOutput(ParseOutput parseOutput)throws IOException { 
      ArcFileItem item = new ArcFileItem();
      
      NIOHttpHeaders headers = NIOHttpHeaders.parseHttpHeaders(parseOutput.getHeaders());
      
      for (int i=0;i<headers.getKeyCount();++i) { 
        ArcFileHeaderItem headerItem = new ArcFileHeaderItem();
        String key = headers.getKey(i);
        String value = headers.getValue(i);
        if (key != null) { 
          headerItem.setItemKey(key);
        }
        if (value != null) { 
          headerItem.setItemValue(value);
        }
        item.getHeaderItems().add(headerItem);
      }
      
      item.setContent(new FlexBuffer(parseOutput.getRawContent().getReadOnlyBytes(),0,parseOutput.getRawContent().getCount()));
      item.setHostIP(parseOutput.getHostIPAddress());
      item.setMimeType(parseOutput.getNormalizedMimeType());
      item.setTimestamp(parseOutput.getFetchTime());
      
      return item;
    }
    
    
    static final byte[] CRLF = "\r\n".getBytes();
    
    /**
     * Build an ArcFileContentItem structure and popuate it's buffer with a 
     * valid ArcFile entry given the ParseOutput data 
     *  
     * @param key
     * @param parseOutput
     * @return
     * @throws IOException
     */
    ArcFileContentItem buildArcFileItemFromParseOutput(String key,ParseOutput parseOutput)throws IOException {
      
      
      // extract metadata line upfront, since if the url exceeds a certain size limit , we are going to reject the entry... 
      byte metaDataLine[];

      try { 
        metaDataLine 
          = ArcFileWriter.getMetaLine(
                key,
                parseOutput.getNormalizedMimeType(), 
                parseOutput.getHostIPAddress(), 
                parseOutput.getFetchTime(),
                parseOutput.getRawContent().getCount() + parseOutput.getHeadersAsTextBytes().getLength()).getBytes("UTF-8");
      }
      catch (IOException e) {
        LOG.error("Metadata Line Validation FAILED with Exception:" + CCStringUtils.stringifyException(e));
        return null;
      }      
      
      contentBufferOut.reset();
      try {
        CompressedStream stream = new CompressedStream(contentBufferOut);

        //read to write an entry ... 
        stream.write(metaDataLine);

        // write out the headers ... 
        stream.write(parseOutput.getHeadersAsTextBytes().getBytes(),0,parseOutput.getHeadersAsTextBytes().getLength());
        // write trailing crlf to signify transition to content 
        stream.write(CRLF);
        // write out the content 
        stream.write(parseOutput.getRawContent().getReadOnlyBytes(),0,parseOutput.getRawContent().getCount());
        // line separator ... 
        stream.write(ArcFileWriter.LINE_SEPARATOR);

        stream.finish();
        stream.flush();
        stream.end();
        stream.close();
        
        ArcFileContentItem itemOut = new ArcFileContentItem();

        itemOut.setUrl(key);
        itemOut.setContent(new Buffer(contentBufferOut.getData(),0,contentBufferOut.getLength()));
        itemOut.setContentType(parseOutput.getNormalizedMimeType());
        itemOut.setParseSegmentId(-1);
        itemOut.setMd5Hash(new FlexBuffer(parseOutput.getMd5Hash().getReadOnlyBytes(),0,parseOutput.getMd5Hash().getCount()));
        if (parseOutput.isFieldDirty(ParseOutput.Field_SIMHASH)) { 
          itemOut.setSimhash(parseOutput.getSimHash());
        }
        return itemOut;

      } catch (Exception e){
        LOG.error(CCStringUtils.stringifyException(e));
        return null;
      }
    }
    
    JsonParser _parser = new JsonParser();
    
    @Override
    public void write(Text key, ParseOutput value) throws IOException {
      String url = key.toString();
      
      // first write out the arc contents 
      // and store arc offset info in ArchiveInfo struct
      ArchiveInfo archiveInfo = null;
      
      if (value.getRawContent().getCount() != 0) {
        
        try {
          
          ArcFileContentItem itemOut = buildArcFileItemFromParseOutput(url,value);
          if (itemOut != null) {
            _arcWriter.writeRawArcFileItem(itemOut.getContentType(), itemOut.getContent().getReadOnlyBytes(), itemOut.getContent().getCount());
            
            // construct a record of the write ... 
            archiveInfo = new ArchiveInfo();
  
            archiveInfo.setArcfileDate(_arcWriter.getActiveFileTimestamp());
            archiveInfo.setArcfileIndex(_partition);
            archiveInfo.setArcfileOffset(_arcWriter.getLastItemPos());
            archiveInfo.setCompressedSize(itemOut.getContent().getCount());
            archiveInfo.setParseSegmentId(-1);
          }
        }
        catch (IOException e) { 
          LOG.error(CCStringUtils.stringifyException(e));
          throw e;
        }
      }
      
      Text metadataOutput = null;
      
      // if archive info is not null, stir in information into JSON structure ... 
      if (archiveInfo != null) { 
        // ok super inefficient, but for the sake of expediency ... 
        // bump output json back to ucs2, parse it, stir in archive info, and then 
        // write it back out as utf-8
        try { 
          JsonObject archiveJson = new JsonObject();
          
          archiveJson.addProperty("arcFileDate",archiveInfo.getArcfileDate());
          archiveJson.addProperty("arcFileParition",archiveInfo.getArcfileIndex());
          archiveJson.addProperty("arcFileOffset", archiveInfo.getArcfileOffset());
          archiveJson.addProperty("compressedSize", archiveInfo.getCompressedSize());
          
          JsonObject metadataJSON= _parser.parse(value.getMetadata()).getAsJsonObject();
          // stir in archive info 
          metadataJSON.add("archiveInfo",archiveJson);
          // dump it back out 
          metadataOutput = new Text(metadataJSON.toString());
        }
        catch (Exception e) { 
          LOG.error(CCStringUtils.stringifyException(e));
          throw new IOException(e);
        }
      }
      else { 
        metadataOutput = new Text();
        // copy in original utf-8 bytes ... 
        metadataOutput.set(
            value.getMetadataAsTextBytes().getBytes(),
            value.getMetadataAsTextBytes().getOffset(),
            value.getMetadataAsTextBytes().getLength());
      }
      
      // spill metadata output 
      _metadataWriter.append(key,metadataOutput);
      
      // and text output
      if (value.getTextContentAsTextBytes().getLength() != 0) {
        Text textBytes = new Text();
        
        textBytes.set(
            value.getTextContentAsTextBytes().getBytes(),
            value.getTextContentAsTextBytes().getOffset(),
            value.getTextContentAsTextBytes().getLength());
        
        _textWriter.append(key,textBytes);
      }
    }
  }
}
