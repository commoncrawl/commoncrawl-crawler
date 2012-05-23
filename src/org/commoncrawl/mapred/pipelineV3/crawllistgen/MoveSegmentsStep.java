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

package org.commoncrawl.mapred.pipelineV3.crawllistgen;

import java.io.IOException;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.mapred.segmenter.SegmentMover.CrawlSegmentFile;

/**
 * 
 * @author rana
 *
 */
public class MoveSegmentsStep extends CrawlPipelineStep {

  public static final String OUTPUT_DIR_NAME = "moverStub";

  private static final Log LOG = LogFactory.getLog(MoveSegmentsStep.class);

  public MoveSegmentsStep(CrawlPipelineTask task) {
    super(task, "Segment Mover", OUTPUT_DIR_NAME);
  }

  public int findNextSegmentId() throws IOException {
    int maxSegmentId = 100;
    FileStatus candidates[] = getFileSystem().globStatus(new Path("crawl/crawl_segments/[0-9]*"));
    for (FileStatus candidate : candidates) {
      try {
        int segmentId = Integer.parseInt(candidate.getPath().getName());
        maxSegmentId = Math.max(maxSegmentId, segmentId);
      } catch (Exception e) {

      }
    }
    return maxSegmentId + 1;
  }

  @Override
  public Log getLogger() {
    return LOG;
  }

  @Override
  public void runStep(Path outputPathLocation) throws IOException {

    int crawlNumber = findNextSegmentId();

    Path latestPath = getOutputDirForStep(GenSegmentsStep.class);

    // collect crawlers
    // ArrayList<String> crawlers = new ArrayList<String>();
    FileStatus crawlers[] = getFileSystem().globStatus(new Path(latestPath, "ccc*"));
    for (FileStatus crawlerCandidate : crawlers) {

      String crawlerName = crawlerCandidate.getPath().getName();
      LOG.info("Found Crawler:" + crawlerName);

      TreeSet<CrawlSegmentFile> fileSet = new TreeSet<CrawlSegmentFile>();

      FileStatus partitions[] = getFileSystem().globStatus(new Path(crawlerCandidate.getPath(), "*"));

      LOG.info("Doing Segment Scan");
      for (FileStatus partition : partitions) {
        FileStatus segments[] = getFileSystem().globStatus(new Path(partition.getPath(), "*"));

        for (FileStatus segment : segments) {
          CrawlSegmentFile fileCandidate = new CrawlSegmentFile(segment.getPath(), Long.parseLong(partition.getPath()
              .getName()), Long.parseLong(segment.getPath().getName()));
          fileSet.add(fileCandidate);
        }
      }

      int segmentId = 0;
      for (CrawlSegmentFile file : fileSet) {
        Path sourcePath = file.location;
        Path destPath = new Path("crawl/crawl_segments/" + crawlNumber + "/" + (segmentId++) + "/" + crawlerName);
        LOG.info("Moving:" + sourcePath + " to:" + destPath);
        getFileSystem().mkdirs(destPath.getParent());
        getFileSystem().rename(sourcePath, destPath);
      }
    }
    // fake out the output path
    getFileSystem().mkdirs(outputPathLocation);
  }
}
