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

package org.commoncrawl.hadoop.template;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapRunner;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.StringUtils;
import org.commoncrawl.hadoop.io.ARCInputFormat;
import org.commoncrawl.hadoop.io.ARCSplitCalculator;
import org.commoncrawl.hadoop.io.ARCSplitReader;
import org.commoncrawl.hadoop.io.JetS3tARCSource;
import org.commoncrawl.protocol.shared.ArcFileItem;
import org.commoncrawl.util.ArcFileReader;

/**
 * 
 * @author rana
 * 
 */
public class SampleHadoopJob extends MapRunner<Text, ArcFileItem, Text, Text> {

  /** logging **/
  private static final Log LOG = LogFactory.getLog(SampleHadoopJob.class);

  /**
   * main routine
   * 
   * @param args
   */
  public static void main(String[] args) {

    // amazon access key - passed on command line
    String accessKey = args[0];
    // amazon secret key - passed on command line
    String secretKey = args[1];
    // regular expression to match against - passed in command line
    String regEx = args[2];
    // group number to extract
    int groupNumber = Integer.parseInt(args[3]);

    /** arc files names start with year then month **/
    // we want to process all files uploaded in 2009
    // so, we will use the prefix string "2009",
    // buy you could, for example pass in a more restrictive
    // pattern such as "2008/06".

    String inputPrefix = "2009";

    LOG.info("Processing Path:" + inputPrefix);

    // allocate job config
    JobConf job = new JobConf(SampleHadoopJob.class);
    // set job name
    job.setJobName("Sample RegEx Job against path:" + inputPrefix);
    // set regular expression attributes
    job.set("mapred.mapper.regex", regEx);
    job.setInt("mapred.mapper.regex.group", groupNumber);

    // create temp file pth
    Path tempDir = new Path(job.get("mapred.temp.dir", ".") + "/temp-" + System.currentTimeMillis());

    LOG.info("Output for job " + job.getJobName() + " is:" + tempDir);

    // we are going to be using the JetS3ARCSource as an input source to
    // the ArcInputFormat. This input source uses the multi-threaded jets3
    // library to request data from S3.

    /** setup s3 properties **/

    // set the number of retries per ARC file.
    // we are setting this number to one, so if an IOException
    // occurs when processing an ARCFile, we are going to silently skip it
    // and continue processing the next ARC file. You should set this to be
    // a number LESS than mapred.max.tracker.failures (as defined in your
    // job config or hadoop-site.xml). Otherwise, your entire job could
    // fail if it encounteres a bad ARC file in the bucket, or if the S3 serivce
    // exhibits a failure condition specific to a single key or set of keys.
    JetS3tARCSource.setMaxRetries(job, 1);

    // set up S3 credentials ...
    JetS3tARCSource.setAWSAccessKeyID(job, accessKey);
    JetS3tARCSource.setAWSSecretAccessKey(job, secretKey);

    // set the number of files per split
    // set this number higher if the bucket contains lots of files, to reduce
    // the burden on the map-reduce system from tracking too many file splits.
    ARCSplitCalculator.setFilesPerSplit(job, 25);

    /** set up arc reader properties **/

    // again, set the timeout to something reasonable, so that your entire job
    // will not hang if a single GET request fails to complete in a reasonable
    // amount of time
    ArcFileReader.setIOTimeoutValue(30000);
    // set input prefixes ...
    JetS3tARCSource.setInputPrefixes(job, inputPrefix);
    // and S3 bucket name ...
    JetS3tARCSource.setBucketName(job, "commoncrawl");
    // and setup arc source for ArcInputFormat
    ARCInputFormat.setARCSourceClass(job, JetS3tARCSource.class);

    // now inform the job that it needs to use the ARCInputFormat
    job.setInputFormat(ARCInputFormat.class);

    // set up our map runner class
    // we use a map runner instead of a mapper here to give us an extra level of
    // control over how we handle errors. When running a large job against
    // the crawl corpus which may contain hunders of thousands of ARC files, it
    // is extremely important to reduce the risks of abnormal job termination.
    job.setMapRunnerClass(SampleHadoopJob.class);

    // setup reducer (identity in this case ... )
    job.setReducerClass(IdentityReducer.class);
    // standard output format ...
    job.setOutputFormat(SequenceFileOutputFormat.class);
    // set output path
    job.setOutputPath(tempDir);
    // map output types
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);

    // run the job ...
    try {
      LOG.info("Starting Job:" + job.getJobName());
      JobClient.runJob(job);
      LOG.info("Finished Job:" + job.getJobName());
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      e.printStackTrace();
    }
  }

  /** track the task's attempt id **/
  private TaskAttemptID attemptID = null;
  /** and how many task failures we can tolerate **/
  private int maxAttemptTaskId = -1;
  /** track split details for debubbing purposes **/
  private String splitDetails = null;
  /** regular expression pattern - initialized from job config (per mapper) **/
  private Pattern pattern = null;

  private int group = 0;

  /** overloaded to initialize class variables from job config **/
  @Override
  public void configure(JobConf job) {

    attemptID = TaskAttemptID.forName(job.get("mapred.task.id"));
    maxAttemptTaskId = job.getInt("mapred.max.tracker.failures", 4) - 1;
    splitDetails = job.get(ARCSplitReader.SPLIT_DETAILS, "Spit Details Unknown");
    pattern = Pattern.compile(job.get("mapred.mapper.regex"));
    group = job.getInt("mapred.mapper.regex.group", 0);

  }

  /** internal map routine -called by our map runner overload **/
  void map(Text key, ArcFileItem value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

    try {
      // TODO: This is a very simplitic char conversion
      Charset asciiCharset = Charset.forName("ASCII");

      // if the arc file has content ...
      if (value.getContent().getCount() != 0) {
        // attempt to convert it to ascii
        String asciiString = asciiCharset.decode(
            ByteBuffer.wrap(value.getContent().getReadOnlyBytes(), 0, value.getContent().getCount())).toString();

        // ok walk the string looking for the pattern
        Matcher matcher = pattern.matcher(asciiString);
        while (matcher.find()) {
          // found a match, output match, key is url, value is matched group
          output.collect(key, new Text(matcher.group(group)));
        }
      }
    }
    // catch any type of exception and log it ONLY for now
    catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
    }
  }

  /** we extend map runner to extert greater control over failures **/
  @Override
  public void run(RecordReader<Text, ArcFileItem> input, OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException {

    int lastValidPos = 0;
    try {

      // allocate key & value instances that are re-used for all entries
      Text key = input.createKey();
      ArcFileItem value = input.createValue();

      // read next input
      while (input.next(key, value)) {
        // call map function
        map(key, value, output, reporter);
      }
    } catch (IOException e) {

      String errorMessage = "Exception processing Split:" + splitDetails + " Exception:"
          + StringUtils.stringifyException(e);

      LOG.error(errorMessage);

      if (attemptID.getId() != maxAttemptTaskId) {
        throw new IOException(errorMessage);
      }
      // and just ignore the message
    } catch (Throwable e) {
      String errorMessage = "Unknown Exception processing Split:" + splitDetails + " Exception:"
          + StringUtils.stringifyException(e);

      LOG.error(errorMessage);

      // if attempt number is not max attempt number configured...
      if (attemptID.getId() != maxAttemptTaskId) {
        // then bubble up exception
        throw new IOException(errorMessage);
      }
    } finally {
      input.close();
    }
  }
}
