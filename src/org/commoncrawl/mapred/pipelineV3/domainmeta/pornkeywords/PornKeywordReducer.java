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
package org.commoncrawl.mapred.pipelineV3.domainmeta.pornkeywords;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.util.TextBytes;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

/**
 * 
 * @author rana
 *
 */
public class PornKeywordReducer implements Reducer<TextBytes, TextBytes, TextBytes, TextBytes> {

  JsonParser parser = new JsonParser();

  double HOST_WEIGHT = 1.0;
  double TITLE_WEIGHT = 1.75;
  double PATH_WEIGHT = 1.75;

  static final int MAX_SAMPLES = 5;
  String samples[] = new String[MAX_SAMPLES];

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void configure(JobConf job) {
  }

  @Override
  public void reduce(TextBytes key, Iterator<TextBytes> values, OutputCollector<TextBytes, TextBytes> output,
      Reporter reporter) throws IOException {

    DescriptiveStatistics titleMetaScoreStats = new DescriptiveStatistics();
    DescriptiveStatistics hostScoreStats = new DescriptiveStatistics();
    DescriptiveStatistics pathScoreStats = new DescriptiveStatistics();

    int wNoScores = 0;
    int wScores = 0;
    int sampleCount = 0;

    while (values.hasNext()) {
      JsonObject object = parser.parse(values.next().toString()).getAsJsonObject();

      if (object.has("no-score")) {
        wNoScores++;
      } else {
        titleMetaScoreStats.addValue(object.get("titlemeta-score").getAsDouble());
        hostScoreStats.addValue(object.get("host-score").getAsDouble());
        pathScoreStats.addValue(object.get("path-score").getAsDouble());
        if (sampleCount < MAX_SAMPLES) {
          samples[sampleCount++] = object.get("source").getAsString();
        }
        wScores++;
      }
    }

    if (wScores != 0) {

      JsonObject objectOut = new JsonObject();

      JsonArray samplesArray = new JsonArray();
      for (int i = 0; i < sampleCount; ++i) {
        samplesArray.add(new JsonPrimitive(samples[i]));
      }

      objectOut.addProperty("titlemeta-score", titleMetaScoreStats.getMax());
      objectOut.addProperty("host-score", hostScoreStats.getMax());
      objectOut.addProperty("path-score", pathScoreStats.getMax());
      objectOut.addProperty("noScores", wNoScores);
      objectOut.addProperty("wScores", wScores);
      objectOut.add("Samples", samplesArray);

      double finalScore = Math.sqrt(Math.pow(TITLE_WEIGHT * titleMetaScoreStats.getMax(), 2)
          + Math.pow(HOST_WEIGHT * hostScoreStats.getMax(), 2) + Math.pow(PATH_WEIGHT * pathScoreStats.getMax(), 2))
          / Math.sqrt(Math.pow(HOST_WEIGHT, 2) + Math.pow(TITLE_WEIGHT, 2) + Math.pow(PATH_WEIGHT, 2));

      objectOut.addProperty("score", finalScore);

      output.collect(key, new TextBytes(objectOut.toString()));
    }

  }

}
