package org.commoncrawl.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * Fix the ACL in the public data set bucket 
 * @author rana
 *
 */

@SuppressWarnings("static-access")
public class S3FixCCACL {

  static Options options = new Options();
  static { 
    
    options.addOption(
        OptionBuilder.withArgName("awsKey").hasArg().withDescription("AWS Key").isRequired().create("awsKey"));
    
    options.addOption(
        OptionBuilder.withArgName("awsSecret").hasArg().withDescription("AWS Secret").isRequired().create("awsSecret"));

    options.addOption(
        OptionBuilder.withArgName("bucket").hasArg().withDescription("S3 bucket name").isRequired().create("bucket"));
    
    options.addOption(
        OptionBuilder.withArgName("path").hasArg().withDescription("S3 path prefix").isRequired().create("path"));

    options.addOption(
        OptionBuilder.withArgName("print").withDescription("Print ACLS").create("print"));
  }
  
  static void printUsage() { 
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( "S3FixCCACL", options );
  }
    
  /**
   * 
   * @param args
   */
  public static void main(String[] args) {
    
    CommandLineParser parser = new GnuParser();

    try {
      // parse the command line arguments
      CommandLine cmdLine = parser.parse( options, args );
      
      try { 
        BasicAWSCredentials credentials 
          = new BasicAWSCredentials(
              cmdLine.getOptionValue("awsKey"),
              cmdLine.getOptionValue("awsSecret"));
        
        AmazonS3Client s3Client = new AmazonS3Client(credentials);
        
        ObjectListing listing = s3Client.listObjects(cmdLine.getOptionValue("bucket"), cmdLine.getOptionValue("path"));
        
        boolean done = false;
        do { 
          for (S3ObjectSummary summary : listing.getObjectSummaries()) {
            System.out.println("Updating ACL For:" + summary.getKey());
            boolean success = false;
            while (!success) {
              try { 
                if (cmdLine.hasOption("print")) { 
                  AccessControlList acl = s3Client.getObjectAcl(cmdLine.getOptionValue("bucket"),summary.getKey());
                  System.out.println("path: s3://" + cmdLine.getOptionValue("bucket")+"/"+summary.getKey());
                  System.out.println("acl: " + acl.toString()); 
                }
                else { 
                  s3Client.setObjectAcl(cmdLine.getOptionValue("bucket"),summary.getKey(),CannedAccessControlList.PublicRead);
                }
                success = true;
              }
              catch (Exception e) { 
                System.out.println("Error Applying ACL:" + e.getMessage());
                Thread.sleep(1000);
              }
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
      }
      finally { 
      }
    }
    catch( Exception exp ) {
        // oops, something went wrong
        System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
        
        printUsage();
    }
  }
}
