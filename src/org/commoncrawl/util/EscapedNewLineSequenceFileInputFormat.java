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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileRecordReader;

/**
 * A Text/Text sequence file inputformat that removes carriage returns and escapes tabs and newlines
 * in the record value
 */
public class EscapedNewLineSequenceFileInputFormat extends SequenceFileInputFormat<Text, Text>{
  
  @Override
  public RecordReader<Text, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException { 
    return new EscapeNewLineRecordReader(job, (FileSplit)split);
  }

  public final class EscapeNewLineRecordReader extends SequenceFileRecordReader<Text, Text> {
        
    public EscapeNewLineRecordReader(Configuration conf, FileSplit split) throws IOException {
      super(conf, split);
    }

    @Override
    protected synchronized void getCurrentValue(Text value) throws IOException {
      // pull in next text 
      super.getCurrentValue(value);
      
      // clean it ready for streaming
      String cleanedup = value.toString().
          replaceAll("\\r", " ").      // remove rogue CRs, streaming gets confused about these
          trim().                      // remove any obvious leading/trialing stuff (including from those CRs)
          replaceAll("\\n", "\\\\n").  // escape all \n s
          replaceAll("\\t", "\\\\t");  // and since streaming uses \t as k/v sep lets escape them too

      // replace value
      value.set(cleanedup);
    }
    
  }
  
}
