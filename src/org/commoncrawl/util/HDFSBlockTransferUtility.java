/**
 * Copyright 2008 - CommonCrawl Foundation
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

package org.commoncrawl.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

import org.apache.commons.io.IOUtils;

/**
 * 
 * @author rana
 *
 */
public class HDFSBlockTransferUtility {

  
  public static void main(String[] args) {
    final String transferFromDisk = args[0];
    final String transferToDisks[] = args[1].split(",");
    final LinkedBlockingQueue<String> queues[] = new LinkedBlockingQueue[transferToDisks.length];
    final Semaphore waitSemaphore = new Semaphore(- (transferToDisks.length - 1));
    for (int i=0;i<transferToDisks.length;++i) {
      queues[i] = new LinkedBlockingQueue<String>();
    }
    
    File transferSource = new File(transferFromDisk);
    for (File transferFile : transferSource.listFiles()) { 
      if (transferFile.isDirectory()) { 
        int partition = Math.abs(transferFile.getName().hashCode() % transferToDisks.length);
        try {
          queues[partition].put(transferFile.getAbsolutePath());
        } catch (InterruptedException e) {
        }
      }
      else { 
        try {
          doCopyFile(transferFile, new File(transferToDisks[0],transferFile.getName()), true);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    
    
    Thread threads[] = new Thread[transferToDisks.length];
    for (int i=0;i<transferToDisks.length;++i) { 
      
      final int threadIdx = i;
      
      try {
        queues[threadIdx].put("");
      } catch (InterruptedException e1) {
      }
      
      threads[i] = new Thread(new Runnable() {

        @Override
        public void run() {
          
          try { 
            File transferToDisk = new File(transferToDisks[threadIdx]);
            
            LinkedBlockingQueue<String> queue = queues[threadIdx];
            
            while (true) { 
              try {
                String nextDir = queue.take();
                if (nextDir.length() == 0) { 
                  break;
                }
                else { 
                  File sourceDir       = new File(nextDir);
                  File targetDir       = new File(transferToDisk,sourceDir.getName());
                  
                  try {
                    copyFiles(sourceDir,targetDir,true);
                  } catch (IOException e) {
                    e.printStackTrace();
                  }
                  
                }
              } catch (InterruptedException e) {
              }
            }
          }
          finally { 
            waitSemaphore.release();            
          }
        } 
        
      });
      threads[i].start();
    }
    
    System.out.println("Waiting for Worker Threads");
    try {
      waitSemaphore.acquire();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    System.out.println("Worker Threads Dead");
  }

  private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
  
  
  private static void doCopyFile(File srcFile, File destFile, boolean preserveFileDate) throws IOException {
    if (destFile.exists() && destFile.isDirectory()) {
        throw new IOException("Destination '" + destFile + "' exists but is a directory");
    }

    FileInputStream input = new FileInputStream(srcFile);
    try {
        FileOutputStream output = new FileOutputStream(destFile);
        try {
          copyLarge(input, output);
        } finally {
            IOUtils.closeQuietly(output);
        }
    } finally {
        IOUtils.closeQuietly(input);
    }

    if (srcFile.length() != destFile.length()) {
        throw new IOException("Failed to copy full contents from '" +
                srcFile + "' to '" + destFile + "'");
    }
    if (preserveFileDate) {
        destFile.setLastModified(srcFile.lastModified());
    }
  }
  
  static long copyLarge(InputStream input, OutputStream output)throws IOException {
    byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
    long count = 0;
    int n = 0;
    while (-1 != (n = input.read(buffer))) {
      output.write(buffer, 0, n);
      count += n;
    }
    return count;
  }
  
  
  static void copyFiles(File sourceDir,File targetDir,boolean recurse)throws IOException {
    System.out.println("making targetDir:" + targetDir.getAbsolutePath());
    
    targetDir.mkdirs();
    
    File files[] = sourceDir.listFiles();
    for (File sourceFile : files) { 
      if (sourceFile.isDirectory() && recurse) { 
        File newTargetDir = new File(targetDir,sourceFile.getName());
        copyFiles(sourceFile,newTargetDir,true);
      }
      else { 
        String fileName = sourceFile.getName();
        File source = new File(sourceDir,fileName);
        File target = new File(targetDir,fileName);
        
        if (!target.exists() || target.length() != source.length()) {
          System.out.println("Copying from:" + source + " to:" + target);
          doCopyFile(source, target, true);
        }

      }
    }
  }
}
