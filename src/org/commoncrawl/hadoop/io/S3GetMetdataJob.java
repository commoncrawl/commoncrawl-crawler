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

package org.commoncrawl.hadoop.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapRunnable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.StringUtils;
import org.commoncrawl.protocol.CrawlURLMetadata;
import org.commoncrawl.protocol.shared.ArcFileHeaderItem;
import org.commoncrawl.protocol.shared.ArcFileItem;
import org.commoncrawl.util.ArcFileReader;
import org.commoncrawl.util.IPAddressUtils;

/**
 * 
 * @author rana
 * 
 */
public class S3GetMetdataJob implements MapRunnable<Text, ArcFileItem, Text, CrawlURLMetadata> {

  /** logging **/
  private static final Log LOG = LogFactory.getLog(S3GetMetdataJob.class);

  /** the task's attempt id **/
  private TaskAttemptID _attemptID = null;
  private int _maxAttemptsPerTask = -1;
  private String _splitDetails = null;

  public static final String ARCFileHeader_ParseSegmentId = "x_commoncrawl_ParseSegmentId";
  public static final String ARCFileHeader_OriginalURL = "x_commoncrawl_OriginalURL";
  public static final String ARCFileHeader_URLFP = "x_commoncrawl_URLFP";
  public static final String ARCFileHeader_HostFP = "x_commoncrawl_HostFP";
  public static final String ARCFileHeader_Signature = "x_commoncrawl_Signature";
  public static final String ARCFileHeader_CrawlNumber = "x_commoncrawl_CrawlNo";
  public static final String ARCFileHeader_CrawlerId = "x_commoncrawl_CrawlerId";
  public static final String ARCFileHeader_FetchTimeStamp = "x_commoncrawl_FetchTimestamp";

  public static void main(String[] args) {

    String accessKey = args[0];
    String secretKey = args[1];

    String paths[] = {
    // "2008/06",
    // "2008/07",
    // "2008/08",
    // "2008/09",
    // "2008/10",
    // "2008/11",
    "2009" };

    for (int pathIndex = 0; pathIndex < paths.length; ++pathIndex) {

      LOG.info("Processing Path:" + paths[pathIndex]);

      JobConf job = new JobConf(S3GetMetdataJob.class);

      Path tempDir = new Path(job.get("mapred.temp.dir", ".") + "/generate-temp-" + System.currentTimeMillis());

      LOG.info("Output for Path:" + paths[pathIndex] + " is:" + tempDir);
      System.out.println("Output Path is:" + tempDir);

      job.setJobName("S3 To CrawlURLMetadata Job for Path:" + paths[pathIndex]);

      // setup s3 properties
      JetS3tARCSource.setMaxRetries(job, 1);
      // set up S3 credentials ...
      JetS3tARCSource.setAWSAccessKeyID(job, accessKey);
      JetS3tARCSource.setAWSSecretAccessKey(job, secretKey);
      ARCSplitCalculator.setFilesPerSplit(job, 25);
      // set up arc reader properties
      ArcFileReader.setIOTimeoutValue(30000);
      // set input prefixes ...
      JetS3tARCSource.setInputPrefixes(job, paths[pathIndex]);
      // and S3 bucket name ...
      JetS3tARCSource.setBucketName(job, "commoncrawl");
      // and setup arc source for ArcInputFormat
      ARCInputFormat.setARCSourceClass(job, JetS3tARCSource.class);
      // and set up input format ...
      job.setInputFormat(ARCInputFormat.class);
      // set mapper ...
      job.setMapRunnerClass(S3GetMetdataJob.class);
      // setup reducer (identity in this case ... )
      job.setReducerClass(IdentityReducer.class);
      // standard output format ...
      job.setOutputFormat(SequenceFileOutputFormat.class);
      // set output path
      job.setOutputPath(tempDir);
      // map output types
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(CrawlURLMetadata.class);
      // reduce output types
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(CrawlURLMetadata.class);
      // double the number of reducers ...
      // job.setNumReduceTasks(job.getNumReduceTasks() * 2);

      // run the job ...
      try {
        LOG.info("Starting Job:" + job.getJobName());
        JobClient.runJob(job);
        LOG.info("Finished Job:" + job.getJobName());

        Path finalPath = new Path("jobout/" + paths[pathIndex] + "/result");
        LOG.info("Copying Job Output to:" + finalPath);
        FileSystem fs = FileSystem.get(job);

        try {
          fs.mkdirs(finalPath.getParent());
          fs.rename(tempDir, finalPath);
          LOG.info("Copied Job Output to:" + finalPath);
        } finally {
          // fs.close();
        }

      } catch (IOException e) {
        LOG.error(StringUtils.stringifyException(e));
        e.printStackTrace();
      }
    }
  }

  public void close() throws IOException {

  }

  public void configure(JobConf job) {

    _attemptID = TaskAttemptID.forName(job.get("mapred.task.id"));
    _maxAttemptsPerTask = job.getInt("mapred.max.tracker.failures", 4);
    _splitDetails = job.get(ARCSplitReader.SPLIT_DETAILS, "Spit Details Unknown");
  }

  public void map(Text key, ArcFileItem value, OutputCollector<Text, CrawlURLMetadata> output, Reporter reporter)
      throws IOException {

    try {
      // create a url metadata
      CrawlURLMetadata urlMetadataOut = new CrawlURLMetadata();

      // set direct fields ...

      // set arc file metadata fields ...
      urlMetadataOut.setArcFileName(value.getArcFileName());
      urlMetadataOut.setArcFileOffset(value.getArcFilePos());
      // set ip field ..
      InetAddress address = InetAddress.getByName(value.getHostIP());
      urlMetadataOut.setServerIP(IPAddressUtils.IPV4AddressToInteger(address.getAddress()));
      // set fetch length
      urlMetadataOut.setLastFetchSize(value.getContent().getCount());

      // walk headers ...
      for (ArcFileHeaderItem headerItem : value.getHeaderItems()) {
        if (headerItem.getItemKey().equalsIgnoreCase(ARCFileHeader_ParseSegmentId)) {
          urlMetadataOut.setParseDataSegNo(Integer.parseInt(headerItem.getItemValue()));
        } else if (headerItem.getItemKey().equalsIgnoreCase("Content-Type")) {
          urlMetadataOut.setContentType(headerItem.getItemValue());
        } else if (headerItem.getItemKey().equalsIgnoreCase("Content-Length")) {
          urlMetadataOut.setContentLength(Integer.parseInt((headerItem.getItemValue())));
        } else if (headerItem.getItemKey().equalsIgnoreCase(ARCFileHeader_URLFP)) {
          urlMetadataOut.setUrlFP(Long.parseLong(headerItem.getItemValue()));
        } else if (headerItem.getItemKey().equalsIgnoreCase(ARCFileHeader_HostFP)) {
          urlMetadataOut.setHostFP(Long.parseLong(headerItem.getItemValue()));
        } else if (headerItem.getItemKey().equalsIgnoreCase(ARCFileHeader_Signature)) {
          urlMetadataOut.setSignature(headerItem.getItemValue());
        } else if (headerItem.getItemKey().equalsIgnoreCase(ARCFileHeader_CrawlNumber)) {
          urlMetadataOut.setCrawlNumber(Integer.parseInt(headerItem.getItemValue()));
        } else if (headerItem.getItemKey().equalsIgnoreCase(ARCFileHeader_FetchTimeStamp)) {
          urlMetadataOut.setLastFetchTimestamp(Long.parseLong(headerItem.getItemValue()));
        }
      }

      if (output != null) {
        output.collect(key, urlMetadataOut);
      }
    }
    // catch any type of exception and log it ONLY for now
    catch (Exception e) {

    }
  }

  public void run(RecordReader<Text, ArcFileItem> input, OutputCollector<Text, CrawlURLMetadata> output,
      Reporter reporter) throws IOException {

    int lastValidPos = 0;
    try {
      // allocate key & value instances that are re-used for all entries
      Text key = input.createKey();
      ArcFileItem value = input.createValue();

      while (input.next(key, value)) {

        lastValidPos = value.getArcFilePos();

        // map pair to output
        map(key, value, output, reporter);
      }
    } catch (IOException e) {

      String errorMessage = "Exception processing Split:" + _splitDetails + " Exception:"
          + StringUtils.stringifyException(e);
      LOG.error(errorMessage);

      if (_attemptID.getId() == 0 || (lastValidPos == 0 && _attemptID.getId() != _maxAttemptsPerTask - 1)) {
        throw new IOException(errorMessage);
      }

      // and just ignore the message
    } catch (Throwable e) {
      String errorMessage = "Unknown Exception processing Split:" + _splitDetails + " Exception:"
          + StringUtils.stringifyException(e);
      LOG.error(errorMessage);
      // if attempt number is not max attempt number configured...
      if (_attemptID.getId() != _maxAttemptsPerTask - 1) {
        // then bubble up exception
        throw new IOException(errorMessage);
      }

    } finally {
      close();
    }

  }

  @org.junit.Test
  public void testMapper() throws Exception {

    final ArcFileReader reader = new ArcFileReader();

    Thread thread = new Thread(new Runnable() {

      public void run() {
        try {

          while (reader.hasMoreItems()) {
            ArcFileItem item = new ArcFileItem();

            reader.getNextItem(item);

            map(new Text(item.getUri()), item, null, null);

          }
          LOG.info("NO MORE ITEMS... BYE");
        } catch (IOException e) {
          LOG.error(StringUtils.stringifyException(e));
        }
      }

    });

    // run the thread ...
    thread.start();

    File file = new File("/Users/rana/Downloads/1213886083018_0.arc.gz");
    ReadableByteChannel channel = Channels.newChannel(new FileInputStream(file));

    try {

      int totalBytesRead = 0;
      for (;;) {

        ByteBuffer buffer = ByteBuffer.allocate(ArcFileReader.DEFAULT_BLOCK_SIZE);

        int bytesRead = channel.read(buffer);
        LOG.info("Read " + bytesRead + " From File");

        if (bytesRead == -1) {
          reader.finished();
          break;
        } else {
          buffer.flip();
          totalBytesRead += buffer.remaining();
          reader.available(buffer);
        }
      }
    } finally {
      channel.close();
    }

    // now wait for thread to die ...
    LOG.info("Done Reading File.... Waiting for ArcFileThread to DIE");
    thread.join();
    LOG.info("Done Reading File.... ArcFileThread to DIED");

  }
}
