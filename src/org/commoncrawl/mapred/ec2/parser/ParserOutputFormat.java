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
import org.commoncrawl.crawl.database.ArcFileContentItem;
import org.commoncrawl.io.shared.NIOHttpHeaders;
import org.commoncrawl.protocol.ArchiveInfo;
import org.commoncrawl.protocol.ParseOutput;
import org.commoncrawl.protocol.shared.ArcFileHeaderItem;
import org.commoncrawl.protocol.shared.ArcFileItem;
import org.commoncrawl.protocol.shared.CrawlMetadata;
import org.commoncrawl.util.ArcFileWriter;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.FlexBuffer;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.ArcFileWriter.CompressedStream;

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
  public static final String ARCMETA_FILE_PREFIX    = "arcMetadata-";
  public static final String RAW_FILE_PREFIX    = "rawContent-";
  
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
    SequenceFile.Writer _arcMetaWriter = null;
    SequenceFile.Writer _contentWriter = null;
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
      final Path arcMetadataPath = new Path(outputPath,ARCMETA_FILE_PREFIX + NUMBER_FORMAT.format(_partition));
      final Path rawContentPath = new Path(outputPath,RAW_FILE_PREFIX + NUMBER_FORMAT.format(_partition));
      
      _metadataWriter = SequenceFile.createWriter(
          fs, 
          conf, 
          metadataPath, 
          TextBytes.class, 
          TextBytes.class, 
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
          TextBytes.class, 
          TextBytes.class, 
          65536*10,
          fs.getDefaultReplication(), 
          fs.getDefaultBlockSize(),
          CompressionType.BLOCK, 
          new SnappyCodec(),          
          progress,
          new Metadata());
      
      _arcMetaWriter = SequenceFile.createWriter(
          fs, 
          conf, 
          arcMetadataPath, 
          TextBytes.class, 
          ArchiveInfo.class, 
          fs.getConf().getInt("io.file.buffer.size", 4096),
          fs.getDefaultReplication(), 
          fs.getDefaultBlockSize(),
          CompressionType.BLOCK, 
          new SnappyCodec(),          
          progress,
          new Metadata());
      
      
      _contentWriter = SequenceFile.createWriter(
          fs, 
          conf, 
          rawContentPath, 
          TextBytes.class, 
          ParseOutput.class, 
          65536*10,
          fs.getDefaultReplication(), 
          fs.getDefaultBlockSize(),
          CompressionType.BLOCK, 
          new GzipCodec(),          
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
      if (_contentWriter != null) { 
        try { 
          _contentWriter.close();
        }
        catch (Exception e) { 
          LOG.error(CCStringUtils.stringifyException(e));
        }
      }
      if (_arcMetaWriter != null) { 
        try { 
          _arcMetaWriter.close();
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
    
    @Override
    public void write(Text key, ParseOutput value) throws IOException {
      String url = key.toString();

      _metadataWriter.append(new TextBytes(key),value.getMetadataAsTextBytes());

      if (value.getTextContentAsTextBytes().getLength() != 0) { 
        _textWriter.append(new TextBytes(key),value.getTextContentAsTextBytes()); 
      }
      
      if (value.getRawContent().getCount() != 0) {
        
        try {
          
          value.setFieldClean(ParseOutput.Field_METADATA);
          value.setFieldClean(ParseOutput.Field_TEXTCONTENT);
          value.setFieldClean(ParseOutput.Field_MD5HASH);
          value.setFieldClean(ParseOutput.Field_SIMHASH);
          value.setCrawlMetadata(new CrawlMetadata());
          
          _contentWriter.append(new TextBytes(key),value);
          
          
          ArcFileContentItem itemOut = buildArcFileItemFromParseOutput(url,value);
          if (itemOut != null) {
            _arcWriter.writeRawArcFileItem(itemOut.getContentType(), itemOut.getContent().getReadOnlyBytes(), itemOut.getContent().getCount());
            
            // construct a record of the write ... 
            ArchiveInfo info = new ArchiveInfo();
  
            info.setArcfileDate(_arcWriter.getActiveFileTimestamp());
            info.setArcfileIndex(_partition);
            info.setArcfileOffset(_arcWriter.getLastItemPos());
            info.setCompressedSize(itemOut.getContent().getCount());
            info.setParseSegmentId(-1);
  
            if (itemOut.isFieldDirty(ArcFileContentItem.Field_MD5HASH)) { 
              info.setSignature(new Buffer(itemOut.getMd5Hash().getReadOnlyBytes()));
            }
            if (itemOut.isFieldDirty(ArcFileContentItem.Field_SIMHASH)) { 
              info.setSimhash(itemOut.getSimhash());
            }
            _arcMetaWriter.append(new TextBytes(key), info);
          }
          
        }
        catch (IOException e) { 
          LOG.error(CCStringUtils.stringifyException(e));
          throw e;
        }
        
      }
    }
    
    
  }
  

}
