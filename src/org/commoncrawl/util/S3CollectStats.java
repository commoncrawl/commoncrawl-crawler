package org.commoncrawl.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Lists;

/**
 * Collect stats about the public dataset bucket  
 * @author rana
 *
 */
public class S3CollectStats {
  
  public static final Log LOG = LogFactory.getLog(S3FixCCACL.class); 
  
  static Options options = new Options();
  static { 
    
    options.addOption(
        OptionBuilder.withArgName("awsKey").hasArg().withDescription("AWS Key").isRequired().create("awsKey"));
    
    options.addOption(
        OptionBuilder.withArgName("awsSecret").hasArg().withDescription("AWS Secret").isRequired().create("awsSecret"));

  }
  
  private static final String DATASET_BUCKET = "aws-publicdatasets";

  
  static void printUsage() { 
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( "S3CollectStats", options );
  }
  
  private static List<Long> getValidSegmentIds(AmazonS3Client s3Client)throws IOException { 
    S3Object validSegmentsObject = s3Client.getObject(DATASET_BUCKET, "/common-crawl/parse-output/valid_segments.txt");
    
    BufferedReader reader =  new BufferedReader(new InputStreamReader(validSegmentsObject.getObjectContent()));
    
    ArrayList<Long> segmentIds = Lists.newArrayList();
    
    try { 
      String line = null;
      while ((line = reader.readLine()) != null) { 
        if (line.length() != 0) { 
          try { 
            segmentIds.add(Long.parseLong(line.trim()));
          }
          catch (Exception e) { 
            throw new IOException("Invalid Segment Id Encountered:" + line);
          }
        }
      }
    }
    finally { 
      reader.close();
    }
    
    return segmentIds;
  }
  
  private static class SegmentStats {
    
    public SegmentStats(long segmentId) { 
      this.segmentId = segmentId;
    }
    
    long segmentId;
    int  arcFileCount;
    long arcFileSizeTotal;
    int  metadataCount;
    long metadataSizeTotal;
    long textSizeTotal;
  }
  
  public static void main(String[] args) {
    
    CommandLineParser parser = new GnuParser();

    try {
      // parse the command line arguments
      CommandLine cmdLine = parser.parse( options, args );
      
      BasicAWSCredentials credentials 
        = new BasicAWSCredentials(
            cmdLine.getOptionValue("awsKey"),
            cmdLine.getOptionValue("awsSecret"));
      
      AmazonS3Client s3Client = new AmazonS3Client(credentials);
      
      // read valid segments file 
      List<Long> segmentIds = getValidSegmentIds(s3Client);
            
      for (long segmentId : segmentIds) {
        
        SegmentStats stats = new SegmentStats(segmentId);
        String segmentPath = "/common-crawl/parse-output/segment/" + segmentId + "/*";
        
        ObjectListing listing = s3Client.listObjects(DATASET_BUCKET,segmentPath);
        
        boolean done = false;

        do { 
          for (S3ObjectSummary summary : listing.getObjectSummaries()) {
            System.out.println("Updating Stats For:" + summary.getKey());
            
            if (summary.getKey().endsWith("arc.gz")) { 
              stats.arcFileCount++;
              stats.arcFileSizeTotal += summary.getSize();
            }
            else if (summary.getKey().startsWith("metadata-")) { 
              stats.metadataCount++;
              stats.metadataSizeTotal += summary.getSize();
            }
            else if (summary.getKey().startsWith("textData-")) { 
              stats.textSizeTotal += summary.getSize();
            }
          }
          if (listing.isTruncated()) { 
            listing = s3Client.listNextBatchOfObjects(listing);
          }
          else { 
            done = true;
          }
        }
        while (!done);
        
        System.out.println(
            "@@@Stats\t" + 
            segmentId + "\t" + 
            stats.arcFileCount + "\t" +
            stats.arcFileSizeTotal + "\t" +
            stats.metadataCount + "\t" +
            stats.metadataSizeTotal + "\t" +
            stats.textSizeTotal);
      }
    }
    catch( Exception exp ) {
        // oops, something went wrong
        System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
        
        printUsage();
    }
  }

}
