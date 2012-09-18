package org.commoncrawl.mapred.ec2.parser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;

public class EC2CheckpointTask implements Constants {
  
  public static void main(String[] args)throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs;
    try {
      fs = FileSystem.get(new URI("s3n://aws-publicdatasets"),conf);
    } catch (URISyntaxException e) {
      throw new IOException(e.toString());
    }
    System.out.println("FileSystem is:" + fs.getUri() +" Scanning for valid checkpoint id");
    long latestCheckpointId = findLastValidCheckpointId(fs,conf);
    System.out.println("Latest Checkpoint Id is:"+ latestCheckpointId);
    System.out.println("Iterating Segments");
    iterateAvailableSegments(fs, latestCheckpointId);    
    
  }
  
  static long findLastValidCheckpointId(FileSystem fs, Configuration conf)throws IOException {
    long lastCheckpointId = -1L;
    for (FileStatus dirStats : fs.globStatus(new Path(CHECKPOINTS_PATH,"[0-9]*"))) {  
      lastCheckpointId = Math.max(lastCheckpointId,Long.parseLong(dirStats.getPath().getName()));
    }
    return lastCheckpointId;
  }
  
  static Pattern arcFileNamePattern = Pattern.compile("^([0-9]*)_([0-9]*).arc.gz$");
  
  static Multimap<Integer,Long> getArcFilesSizesSegment(FileSystem fs,long segmentId) throws IOException  {
    
    Multimap<Integer,Long> splitToSizeMap = TreeMultimap.create();
    
    for (FileStatus arcCandidate : fs.globStatus(new Path(SEGMENTS_PATH + segmentId,"*.arc.gz"))) { 
      Matcher m = arcFileNamePattern.matcher(arcCandidate.getPath().getName());
      if (m.matches() && m.groupCount() == 2) { 
        int splitId = Integer.parseInt(m.group(2));
        splitToSizeMap.put(splitId,arcCandidate.getLen());
      }
    }
    return splitToSizeMap;
  }
  
  static void iterateAvailableSegments(FileSystem fs,long lastCheckpointId)throws IOException { 
    for (long segmentId : buildValidSegmentListGivenCheckpointId(fs, lastCheckpointId)) {
      System.out.println("Found Segment:" + segmentId);
      
      // get arc sizes by split upfront (because S3n wildcard operations are slow) 
      Multimap<Integer, Long> splitSizes= getArcFilesSizesSegment(fs,segmentId);
      
      System.out.println("Found ArcFiles for:" + splitSizes.keySet().size() + " Splits");
      
      // get failed and partial splits for segment 
      SortedSet<SegmentSplitDetail> failedSplits = getFailedSplits(fs, segmentId);
      SortedSet<SegmentSplitDetail> partialSplits = getPartialSplits(fs, segmentId);
      
      System.out.println("There are " + partialSplits.size() + " Partial splits");
      // exclude partial from failed to see how many actually failed ... 
      Sets.SetView<SegmentSplitDetail> reallyFailedSet = Sets.difference(failedSplits,partialSplits);
      System.out.println("There are " + reallyFailedSet.size() + " Partial splits");
      // walk each validating actual failure condidition
      for (SegmentSplitDetail split : reallyFailedSet) { 
        if (splitSizes.containsKey(split.splitIndex)) { 
          System.out.println("Failed Split: " + split.splitIndex + " has arc data:" + splitSizes.get(split.splitIndex));
        }
      }
    }
  }
  
  static Set<Long> buildValidSegmentListGivenCheckpointId(FileSystem fs,long lastCheckpointId)throws IOException { 
    TreeSet<Long> validsegments = new TreeSet<Long>();
    for (FileStatus segmentStatus: fs.globStatus(new Path(VALID_SEGMENTS_PATH,"[0-9]*"))) {
      long segmentId = Long.parseLong(segmentStatus.getPath().getName());
      if (segmentId > lastCheckpointId) { 
        validsegments.add(segmentId);
      }
    }
    return validsegments;
  }
  
  static SortedSet<SegmentSplitDetail> getFailedSplits(FileSystem fs,long segmentId)throws IOException { 
    return getSplitDetailsFromFile(fs,new Path(VALID_SEGMENTS_PATH+Long.toString(segmentId)+"/"+FAILED_SPLITS_MANIFEST_FILE),false); 
  }
  
  static SortedSet<SegmentSplitDetail> getPartialSplits(FileSystem fs,long segmentId)throws IOException { 
    return getSplitDetailsFromFile(fs,new Path(VALID_SEGMENTS_PATH+Long.toString(segmentId)+"/"+TRAILING_SPLITS_MANIFEST_FILE),true);
  }
  
  static SortedSet<SegmentSplitDetail> getSplitDetailsFromFile(FileSystem fs,Path path,boolean isPartialSplitInfo)throws IOException { 
        
    TreeSet<SegmentSplitDetail> splits = new TreeSet<EC2CheckpointTask.SegmentSplitDetail>();
    
    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path),Charset.forName("UTF-8")));
    try { 
      String line;
      while ((line = reader.readLine()) != null) { 
        if (line.length() != 0 && !line.startsWith("#")) 
          splits.add(splitDetailFromLogLine(line, isPartialSplitInfo));
      }
    }
    finally { 
      reader.close();
    }
    return splits;
  }
  
  static Pattern partialSplitLogPattern = Pattern.compile("^([0-9]*),([^,]*),([^,]*)$");
  static Pattern failedSplitLogPattern = Pattern.compile("^([0-9]*),(.*)$");
  
  static SegmentSplitDetail splitDetailFromLogLine(String logLine,boolean isPartialSplit) throws IOException { 
    if (isPartialSplit) { 
      Matcher m = partialSplitLogPattern.matcher(logLine);
      if (m.matches() && m.groupCount() == 3) { 
        
        SegmentSplitDetail detail = new SegmentSplitDetail();
        
        detail.splitIndex = Integer.parseInt(m.group(1));
        detail.partialSplit = new SplitInfo(m.group(2));
        detail.originalSplit = new SplitInfo(m.group(3));
        
        return detail;

      }
      else { 
        throw new IOException("Invalid Split Info:" + logLine);
      }
    }
    else { 
      Matcher m = failedSplitLogPattern.matcher(logLine);

      if (m.matches() && m.groupCount() == 2) { 
        
        SegmentSplitDetail detail = new SegmentSplitDetail();
        
        detail.splitIndex = Integer.parseInt(m.group(1));
        detail.originalSplit = new SplitInfo(m.group(2));
        
        return detail;
      }
      else { 
        throw new IOException("Invalid Split Info:" + logLine);
      }
    }
  }
  

  static class SplitInfo { 
    
    String  sourceFilePath;
    long    offset;
    long    length;
    
    static Pattern pattern = Pattern.compile("^([^:]*)://([^:]*):([^+]*)\\+(.*)$");
    
    SplitInfo(String splitText)throws IOException { 
      Matcher m = pattern.matcher(splitText);
      if (m.matches() && m.groupCount() == 4) { 
        sourceFilePath = m.group(1)+"://"+m.group(2);
        offset = Long.parseLong(m.group(3));
        length = Long.parseLong(m.group(4));
      }
      else { 
        throw new IOException("Invalid Split:"+ splitText);
      }
    }
  }
  
  static class SegmentSplitDetail implements Comparable<SegmentSplitDetail>{

    int     splitIndex;
    SplitInfo originalSplit;
    SplitInfo partialSplit;    
    
    @Override
    public int compareTo(SegmentSplitDetail o) {
      return (splitIndex < o.splitIndex) ? -1: (splitIndex > o.splitIndex) ? 1: 0;
    }
    
  }
  
}
