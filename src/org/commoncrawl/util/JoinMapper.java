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
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * 
 * @author rana
 *
 */
public class JoinMapper implements Mapper<WritableComparable,Writable,WritableComparable,JoinValue> {
  
  private static final Log LOG = LogFactory.getLog(JoinMapper.class);

  public static final String PATH_TO_TAG_MAPPING = "path_to_tag";
  
  static String getParentDirFromPath(Path path) { 
    Path parent = path.getParent();
    if (Character.isDigit(parent.getName().charAt(0))) { 
      return parent.getParent().getName();
    }
    return parent.getName();
  }
  
  @Override
  public void configure(JobConf job) {
    FileSystem fs;
    try {
      fs = FileSystem.get(job);
      readTagMappings(job);
    } catch (IOException e) {
      LOG.error(CCStringUtils.stringifyException(e));
      throw new RuntimeException(e);
    }
    Path inputSplit = new Path(job.get("map.input.file"));
    String tagName = getParentDirFromPath(inputSplit);
    
    if (_mappings != null) {
      
      String tagId = _mappings.get(inputSplit.getParent().makeQualified(fs));
      LOG.info("Mapped Path:" + inputSplit.getParent() + " to:" + tagId);
      
      _tagType.set(tagId);
    }
    else { 
      _tagType.set(tagName);
    }
  }

  @Override
  public void close() throws IOException {
    
  }
  
  public static void setPathToTagMapping(Map<Path,String> mappings,JobConf conf)throws IOException { 
    JsonArray jsonArray = new JsonArray();
    for (Map.Entry<Path,String> entry : mappings.entrySet()) { 
      JsonObject jsonEntry = new JsonObject();
      
      jsonEntry.addProperty("key", entry.getKey().toString());
      jsonEntry.addProperty("value", entry.getValue());
    
      jsonArray.add(jsonEntry);
    }
    conf.set(PATH_TO_TAG_MAPPING, jsonArray.toString());
  }
  
  Map<Path,String> _mappings = null;
  
  void readTagMappings(JobConf jobConf)throws IOException {
    
    FileSystem fs = FileSystem.get(jobConf);
    
    try { 
      JsonParser parser = new JsonParser();
      
      String mappings = jobConf.get(PATH_TO_TAG_MAPPING);
      
      LOG.info("Got Mappings:" + mappings);
      
      if (mappings != null) { 
        JsonArray jsonArray = parser.parse(mappings).getAsJsonArray();
        
        if (jsonArray != null && jsonArray.size() != 0) { 
          _mappings = new HashMap<Path,String>();
          for (int i=0;i<jsonArray.size();++i) { 
            JsonObject jsonObject = jsonArray.get(i).getAsJsonObject();
            _mappings.put(new Path(jsonObject.get("key").getAsString()).makeQualified(fs),jsonObject.get("value").getAsString());
            LOG.info("Got Key/Value Mapping:"+ jsonObject.get("key").getAsString() + " " + jsonObject.get("value").getAsString());
          }
        }
      }
    }
    catch (Exception e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      throw new IOException("Failed to Parse Path to Tag Mappings!");
    }
  }

  TextBytes _tagType = new TextBytes();
  
  public WritableComparable mapKey(WritableComparable key,JoinValue value)throws IOException { 
    return key;
  }
  
  @Override
  public void map(WritableComparable key, Writable value,
      OutputCollector<WritableComparable, JoinValue> output, Reporter reporter)
      throws IOException {
    
    JoinValue joinValue = JoinValue.getJoinValue(_tagType, value);
    WritableComparable outputKey = mapKey(key,joinValue);
    
    output.collect(outputKey, joinValue);
  }
  
}
