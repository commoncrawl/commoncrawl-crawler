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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.ImmutableList;

/**
 * 
 * @author rana
 *
 */
public class HDFSUtils {

  private static final Log LOG = LogFactory.getLog(HDFSUtils.class);
  
  public static List<String> textFileToList(FileSystem fs,Path path)throws IOException { 
    
    ImmutableList.Builder<String> builder = new ImmutableList.Builder<String>(); 
    
    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path),Charset.forName("UTF-8")));
    try { 
      String line;
      while ((line = reader.readLine()) != null) { 
        if (line.length() != 0 && !line.startsWith("#")) 
          builder.add(line);
      }
    }
    finally { 
      reader.close();
    }
    return builder.build();
  }
  
  public static void listToTextFile(List<String> lines,FileSystem fs,Path path)throws IOException { 
    Writer writer = new OutputStreamWriter(fs.create(path), Charset.forName("UTF-8"));
    try { 
      for (String line : lines) { 
        writer.write(line);
        writer.append("\n");
      }
      writer.flush();
    }
    finally { 
      writer.close();
    }
  }
  
  public static long findLatestDatabaseTimestamp(FileSystem fs,Path rootPath) throws IOException {

    FileStatus candidates[] = fs.globStatus(new Path(rootPath, "*"));

    long candidateTimestamp = -1L;

    for (FileStatus candidate : candidates) {
      LOG.info("Found Seed Candidate:" + candidate.getPath());
      try { 
        long timestamp = Long.parseLong(candidate.getPath().getName());
        if (candidateTimestamp == -1 || candidateTimestamp < timestamp) {
          candidateTimestamp = timestamp;
      
        }
      }
      catch (Exception e) {
        LOG.error("Invalid Path:"+ candidate.getPath());
      }
    }
    LOG.info("Selected Candidate is:" + candidateTimestamp);
    return candidateTimestamp;
  }
}
