package org.commoncrawl.crawl.database.crawlpipeline.ec2.postprocess.linkCollector;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;


public class EC2Launcer {
  
  static class InputStreamHandler extends Thread {
    /**
     * Stream being read
     */

    private InputStream  m_stream;
    StringBuffer inBuffer = new StringBuffer();

    /**
     * Constructor.
     * 
     * @param
     */

    InputStreamHandler(InputStream stream) {
      m_stream = stream;
      start();
    }

    /**
     * Stream the data.
     */

    public void run() {
      try {
        int nextChar;
        while ((nextChar = m_stream.read()) != -1) {
          inBuffer.append((char) nextChar);
          if (nextChar == '\n' || inBuffer.length() > 2048) {
            System.out.print(inBuffer.toString());
            inBuffer = new StringBuffer();
          }
        }
      } catch (IOException ioe) {
      }
    }
  }

  public static void main(String[] args) {
    System.out.println("Sleeping for 2 mins");
    try {
      Thread.sleep(10000);
    } catch (InterruptedException e1) {
    }
    System.out.println("Done Sleeping");
    
    ProcessBuilder pb = new ProcessBuilder(
        "./bin/ccAppRun.sh",
        "--consoleMode",
        "--noDefaultArgs",
        "--p_arg0",
        "--runOnEC2",
        "--heapSize",
        "4096",
        "--logdir",
        "/mnt/var/EC2TaskLogs",
        "org.commoncrawl.crawl.database.crawlpipeline.ec2.postprocess.linkCollector.LinkCollectorJob",
    "start");
    pb.directory(new File("/home/hadoop/ccprod"));

    try {
      System.out.println("Starting Job");
      Process p = pb.start();
      new InputStreamHandler (p.getErrorStream());
      new InputStreamHandler (p.getInputStream());
      
      System.out.println("Waiting for Job to Finish");
      p.waitFor();
      System.out.println("Job Finished");
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

}
