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

package org.commoncrawl.mapred.ec2.crawlStats;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;


/**
 * EC2Launcher Task (spawned by EMR)
 * 
 * @author rana
 *
 */
public class EC2Launcher {
  
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
        "--heapSize",
        "4096",
        "--logdir",
        "/mnt/var/EC2TaskLogs",
        "org.commoncrawl.mapred.ec2.crawlStats.EC2StatsCollectorJob",
        "start",
        "--runOnEC2");
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
