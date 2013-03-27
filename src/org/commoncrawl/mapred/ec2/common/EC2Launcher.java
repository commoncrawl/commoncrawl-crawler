package org.commoncrawl.mapred.ec2.common;

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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.util.CCStringUtils;


/**
 * EC2Launcher Task (spawned by EMR)
 * 
 * @author rana
 *
 */
@SuppressWarnings("static-access")
public class EC2Launcher {
  
  public static final Log LOG = LogFactory.getLog(EC2Launcher.class);


  
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
   
    try {      
      System.out.println("Sleeping for 2 mins");
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e1) {
      }
      System.out.println("Done Sleeping");
      
      StringBuilder sb = new StringBuilder();
      
      sb.append("sudo -u hadoop bash -c \'export HADOOP_HOME=/home/hadoop;export JAVA_HOME=/usr/lib/jvm/java-6-sun;cd /home/hadoop;./bin/hadoop jar" +   
      " ./ccprod/lib/commoncrawl-0.1.jar " +
      args[0]);
      
      for (int i=1;i<args.length;++i)
        sb.append(" " + args[i]);
      
      
      sb.append("\'");
      
      
      ProcessBuilder pb = new ProcessBuilder(
          "sudo", 
          "/bin/bash",
          "-c",
          sb.toString()
          );
      
      
      
      pb.directory(new File("/home/hadoop/"));
      
      System.out.println("Command Line:" + pb.command());
  
      try {
        System.out.println("Starting Job");
        Process p = pb.start();
        new InputStreamHandler (p.getErrorStream());
        new InputStreamHandler (p.getInputStream());
        
        System.out.println("Waiting for Job to Finish");
        p.waitFor();
        System.out.println("Job Finished");
        System.exit(0);
      } catch (IOException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    catch (Exception e) { 
      LOG.error(CCStringUtils.stringifyException(e));
    }
    System.exit(1);
  }

}
