package org.commoncrawl.hadoop.mergeutils;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class TextFileSpillWriter implements SpillWriter<WritableComparable,Writable> {

  Writer writer;
  
  // default buffer size
  public static final int DEFAULT_SPILL_BUFFER_SIZE = 1000000;
  
  // the size of our spill buffer 
  public static final String SPILL_WRITER_BUFFER_SIZE_PARAM = "commoncrawl.spillwriter.buffer.size";

  public TextFileSpillWriter(FileSystem fileSystem, Configuration conf, Path outputFilePath)throws IOException { 
    writer 
    = new OutputStreamWriter(fileSystem.create(
        outputFilePath,
        true,
        conf.getInt(SPILL_WRITER_BUFFER_SIZE_PARAM, DEFAULT_SPILL_BUFFER_SIZE)
        ),Charset.forName("UTF-8"));
  }
  
  @Override
  public void close() throws IOException {
    try { 
      writer.flush();
    }
    finally { 
      IOUtils.closeStream(writer);
    }
  }

  @Override
  public void spillRecord(WritableComparable key, Writable value) throws IOException {
    if (!(key instanceof NullWritable)) { 
      writer.write(key.toString());
      if (!(value instanceof NullWritable)) { 
        writer.write('\t');
      }
    }
    if (!(value instanceof NullWritable)) {
      writer.write(value.toString());
    }
    writer.write('\n');
  }


}
