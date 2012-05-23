package org.commoncrawl.mapred.segmenter;

import java.io.IOException;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;

public class SegmentMover {

  private static final Log LOG = LogFactory.getLog(SegmentMover.class);
  
  private static long findLatestDatabaseTimestamp(Path rootPath) throws IOException {
    FileSystem fs = CrawlEnvironment.getDefaultFileSystem();

    FileStatus candidates[] = fs.globStatus(new Path(rootPath, "*"));

    long candidateTimestamp = -1L;

    for (FileStatus candidate : candidates) {
      LOG.info("Found Seed Candidate:" + candidate.getPath());
      long timestamp = Long.parseLong(candidate.getPath().getName());
      if (candidateTimestamp == -1 || candidateTimestamp < timestamp) {
        candidateTimestamp = timestamp;
      }
    }
    LOG.info("Selected Candidate is:" + candidateTimestamp);
    return candidateTimestamp;
  }  
  
  public static class CrawlSegmentFile implements Comparable<CrawlSegmentFile> {
    
    public CrawlSegmentFile(Path location, long partitionId,long segmentId) { 
      this.partitionId = partitionId;
      this.segmentId = segmentId;
      this.location = location;
    }
    public Path location = null;
    public long partitionId = -1;
    public long segmentId = -1;
    @Override
    public int compareTo(CrawlSegmentFile o) {
      int result = ((Long)segmentId).compareTo(o.segmentId);
      if (result == 0) {
        result = ((Long)partitionId).compareTo(o.partitionId);
      }
      return result;
    }
  }
  
  public static void main(String[] args)throws IOException {
    Configuration conf = new Configuration();
    
    int crawlNumber = Integer.parseInt(args[0]);
    
    CrawlEnvironment.setHadoopConfig(conf);
    FileSystem fs = FileSystem.get(conf);
    Path basePath = new Path("crawl/reports/crawllistgen/segments/");
    
    long databaseId = findLatestDatabaseTimestamp(basePath);
    if (databaseId != -1) { 
      LOG.info("Found Database Id:" + databaseId);
      
      Path latestPath = new Path(basePath,Long.toString(databaseId));
      // collect crawlers 
      // ArrayList<String> crawlers = new ArrayList<String>();
      FileStatus crawlers[] = fs.globStatus(new Path(latestPath,"ccc*"));
      for (FileStatus crawlerCandidate : crawlers) {

        String crawlerName = crawlerCandidate.getPath().getName();
        LOG.info("Found Crawler:" + crawlerName);
        
        TreeSet<CrawlSegmentFile> fileSet = new TreeSet<CrawlSegmentFile>();
        
        FileStatus partitions[] = fs.globStatus(new Path(crawlerCandidate.getPath(),"*"));
        
        LOG.info("Doing Segment Scan");
        for (FileStatus partition : partitions) { 
          FileStatus segments[] = fs.globStatus(new Path(partition.getPath(),"*"));
          
          for (FileStatus segment :segments) { 
            CrawlSegmentFile fileCandidate 
              = new CrawlSegmentFile(
                  segment.getPath(), 
                  Long.parseLong(partition.getPath().getName()), 
                  Long.parseLong(segment.getPath().getName()));
            fileSet.add(fileCandidate);
          }
        }
        
        int segmentId = 0;
        for (CrawlSegmentFile file : fileSet) { 
          Path sourcePath = file.location;
          Path destPath  
            = new Path(
                "crawl/crawl_segments/" 
                + crawlNumber 
                + "/" 
                + (segmentId++) 
                + "/"
                + crawlerName);
          LOG.info("Moving:" + sourcePath + " to:" + destPath);
          fs.mkdirs(destPath.getParent());
          fs.rename(sourcePath, destPath);
        }
      }
    }
    
  }
}
