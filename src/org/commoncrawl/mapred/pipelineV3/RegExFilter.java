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
package org.commoncrawl.mapred.pipelineV3;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapRunner;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.util.TextBytes;

/**
 * 
 * @author rana
 *
 */
public class RegExFilter extends
    MapRunner<TextBytes, TextBytes, TextBytes, TextBytes> {

  public static final String REGEX_KEY    = "REG-EX-EXP";

  Pattern                    pattern      = null;
  String                     inputFile    = null;
  long                       inputFilePos = 0;

  @Override
  public void configure(JobConf job) {

    super.configure(job);

    String regEx = job.get(REGEX_KEY);
    pattern = Pattern.compile(regEx);
    inputFile = job.get("map.input.file");
    inputFilePos = job.getLong("map.input.start", -1L);
  }

  @Override
  public void run(RecordReader<TextBytes, TextBytes> input,
      OutputCollector<TextBytes, TextBytes> output, Reporter reporter)
      throws IOException {
    try {
      // allocate key & value instances that are re-used for all entries
      TextBytes key = input.createKey();
      TextBytes value = input.createValue();

      while (input.next(key, value)) {

        String url = key.toString();
        if (pattern.matcher(url).matches()) {
          output.collect(new TextBytes("Key:" + key), value);
        }

      }
    } finally {

    }
  }
}
