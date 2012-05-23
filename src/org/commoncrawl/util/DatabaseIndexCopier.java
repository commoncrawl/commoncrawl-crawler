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
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.BasicConfigurator;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.crawl.database.crawlpipeline.MetadataIndexBuilder;

/**
 * 
 * @author rana
 * 
 */
public class DatabaseIndexCopier {

  public static final Log LOG = LogFactory.getLog(DatabaseIndexCopier.class);

  public static class DistributionItem {
    public int  _driveId;
    public Path _sourcePath;
    public Path _destinatinPath;
  }

  public static void main(String[] args) {

    if (args.length != 3) {
      System.out
          .println("Usage: Copier <seedTimestamp> <dataHomeDir> <driveCount>");
      return;
    }

    // initialize ...
    final Configuration conf = new Configuration();

    conf.addResource("nutch-default.xml");
    conf.addResource("nutch-site.xml");
    conf.addResource("core-site.xml");
    conf.addResource("hdfs-site.xml");
    conf.addResource("mapred-site.xml");

    BasicConfigurator.configure();
    CrawlEnvironment.setHadoopConfig(conf);

    try {
      final FileSystem fs = FileSystem.get(conf);

      final long seedTimestamp = Long.parseLong(args[0]);
      final String dataHome = args[1];
      final int driveCount = Integer.parseInt(args[2]);

      LOG.info("seedTimestamp is:" + seedTimestamp);
      LOG.info("dataHome is:" + dataHome);
      LOG.info("driveCount is:" + driveCount);

      // construct base paths ...
      final Path metadataDBIndexPath = new Path("crawl/metadatadb/"
          + seedTimestamp + "/urlMetadata/index");
      final Path subDomainIdToMetadataDBIndexPath = new Path(
          "crawl/metadatadb/" + seedTimestamp + "/subDomainMetadata/"
              + MetadataIndexBuilder.SUBDOMAIN_INDEX_ID_TO_METADATA);
      final Path subDomainStringsDBIndexPath = new Path("crawl/metadatadb/"
          + seedTimestamp + "/subDomainMetadata/"
          + MetadataIndexBuilder.SUBDOMAIN_INDEX_ID_TO_NAME);
      final Path subDomainStringToMetadataDBIndexPath = new Path(
          "crawl/metadatadb/" + seedTimestamp + "/subDomainMetadata/"
              + MetadataIndexBuilder.SUBDOMAIN_INDEX_NAME_TO_METADATA);
      // create distribution item queues
      final LinkedBlockingQueue<DistributionItem> distributionQueues[] = new LinkedBlockingQueue[driveCount];
      for (int i = 0; i < driveCount; ++i) {
        distributionQueues[i] = new LinkedBlockingQueue<DistributionItem>();
      }
      // start distribution
      // addItemsToQueues("urldb",seedTimestamp,fs.globStatus(new
      // Path(urlDBIndexPath,"part-*.data")),distributionQueues,driveCount,dataHome);
      // addItemsToQueues("urldb",seedTimestamp,fs.globStatus(new
      // Path(urlDBIndexPath,"part-*.index")),distributionQueues,driveCount,dataHome);
      // FOO
      addItemsToQueues("metadata", seedTimestamp, fs.globStatus(new Path(
          metadataDBIndexPath, "part-*.data")), distributionQueues, driveCount,
          dataHome);
      addItemsToQueues("metadata", seedTimestamp, fs.globStatus(new Path(
          metadataDBIndexPath, "part-*.index")), distributionQueues,
          driveCount, dataHome);
      addItemsToQueues("subDomain_"
          + MetadataIndexBuilder.SUBDOMAIN_INDEX_ID_TO_METADATA, seedTimestamp,
          fs.globStatus(new Path(subDomainIdToMetadataDBIndexPath, "part-*")),
          distributionQueues, driveCount, dataHome);
      addItemsToQueues("subDomain_"
          + MetadataIndexBuilder.SUBDOMAIN_INDEX_NAME_TO_METADATA,
          seedTimestamp, fs.globStatus(new Path(
              subDomainStringToMetadataDBIndexPath, "part-*")),
          distributionQueues, driveCount, dataHome);
      addNullItemsToQueues(distributionQueues);

      Thread copyThreads[] = new Thread[driveCount];

      final Semaphore copyThreadWaitSempahore = new Semaphore(-(driveCount - 1));
      // ok start copy threads ...
      for (int i = 0; i < driveCount; ++i) {

        final int threadIndex = i;
        copyThreads[i] = new Thread(new Runnable() {

          @Override
          public void run() {

            LOG.info("Copy Thread at Index: " + threadIndex + " running...");
            try {
              // create thread local file system object ...
              FileSystem threadLocalFS = FileSystem.get(conf);

              while (true) {
                try {
                  DistributionItem item = distributionQueues[threadIndex]
                      .take();

                  if (item._sourcePath == null) {
                    LOG.info("Thread at Index:" + threadIndex
                        + " received null item. Exiting");
                    break;
                  } else {
                    LOG.info("Thread at Index:" + threadIndex
                        + " received item:" + item._sourcePath + " Copying to:"
                        + item._destinatinPath);
                    threadLocalFS.copyToLocalFile(item._sourcePath,
                        item._destinatinPath);
                    LOG.info("Thread at Index:" + threadIndex
                        + " finished copying item:" + item._sourcePath);
                  }

                } catch (InterruptedException e) {
                  // TODO:SHOULD NOT HAPPEN
                  break;
                }

              }
            } catch (IOException e) {
              LOG.error("Thread at Index:" + threadIndex
                  + " Died with Exception:"
                  + CCStringUtils.stringifyException(e));
              return;
            } finally {
              // release wait semaphore
              LOG.info("Thread at Index:" + threadIndex
                  + " releasing lock on copyThreadWaitSemaphore");
              copyThreadWaitSempahore.release();
            }
          }
        });
        copyThreads[i].start();
      }

      LOG.info("Main Thread waiting on copyThreadWaitSemaphore");
      copyThreadWaitSempahore.acquireUninterruptibly();
      LOG.info("Main Thread acquired copyThreadWaitSemaphore. Exiting");

    } catch (IOException e) {
      LOG.error(CCStringUtils.stringifyException(e));
    }
  }

  static void addItemsToQueues(String prefix, long seedTimestamp,
      final FileStatus[] items,
      final LinkedBlockingQueue<DistributionItem>[] distributionQueues,
      int driveCount, String dataHome) {
    if (items.length == 0) {
      LOG.error("No Items Found for Prefix:" + prefix);
    }
    for (int i = 0; i < items.length; ++i) {

      int driveIndex = i % driveCount;
      DistributionItem item = new DistributionItem();

      Path drivePath = new Path(dataHome, Integer.toString(driveIndex));

      item._driveId = driveIndex;
      item._sourcePath = items[i].getPath();
      item._destinatinPath = new Path(drivePath + "/" + prefix + "/"
          + seedTimestamp, items[i].getPath().getName());

      LOG.info("Source File:" + item._sourcePath + " Destination:"
          + item._destinatinPath + " Drive:" + item._driveId);

      File localFile = new File(item._destinatinPath.toString());

      localFile.getParentFile().mkdirs();

      boolean skip = false;
      if (localFile.exists()) {
        if (localFile.length() == items[i].getLen()) {
          LOG.info("File:" + localFile
              + " exists and is the right length. skipping");
          skip = true;
        } else {
          LOG.info("File:" + localFile
              + " exists but is the wrong length.deleting");
          localFile.delete();
        }
      }
      if (!skip) {
        try {
          distributionQueues[item._driveId].put(item);
        } catch (InterruptedException e) {
        }
      }
    }
  }

  static void addNullItemsToQueues(
      final LinkedBlockingQueue<DistributionItem>[] distributionQueues) {
    DistributionItem nullItem = new DistributionItem();

    for (int i = 0; i < distributionQueues.length; ++i) {
      distributionQueues[i].add(nullItem);
    }
  }

}
