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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

/**
 * 
 * @author rana
 *
 */
public class SequenceFileUtils {
  
  @SuppressWarnings({ "unchecked", "deprecation" })
  public static Class sniffValueTypeFromSequenceFile(FileSystem fs,Configuration conf,Path path)throws IOException { 
    if (fs.isDirectory(path)) { 
      path = new Path(path,"part-00000");
    }

    SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
    try {
      return reader.getValueClass();
    }
    finally { 
      reader.close();
    }
  }
  
  public static void printContents(FileSystem fs,Configuration conf,Path path)throws IOException { 
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
    
    JsonParser parser = new JsonParser();
    
    try { 
      Writable key = (Writable) reader.getKeyClass().newInstance();
      Writable value = (Writable)  reader.getValueClass().newInstance();
      
      boolean more = true;
      boolean checkedIsJSON = false;
      boolean isJSON = false;
      do { 
        more = reader.next(key,value);
        if (more) {
          System.out.println("Key:" + key.toString());
          
          if (!checkedIsJSON) {
            checkedIsJSON = true;
            try {
              parser.parse(value.toString());
              isJSON = true;
            }
            catch (Exception e) { 
              
            }
          }
          if (!isJSON) { 
            System.out.print(" Value:" + value.toString());
          }
          else { 
            System.out.print("\n");
            JsonElement e = parser.parse(value.toString());
            JSONUtils.prettyPrintJSON(e);
          }
        }
      }
      while (more);
    }
    catch (Exception e) { 
      e.printStackTrace();
    }
    finally { 
      reader.close();
    }
  }
  
  public static void main(String[] args)throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    printContents(fs, conf, new Path(args[0]));
  }
  
}
