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

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.commoncrawl.util.EscapeUtils;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

/**
 * An {@link ARCSource} for gzipped ARC files stored on Amazon S3 that uses <a
 * href="http://jets3t.s3.amazonaws.com/index.html">JetS3t</a> to interact with
 * S3.
 * 
 * @author Albert Chern
 */
public class JetS3tARCSource extends ARCSplitCalculator implements ARCSource, JobConfigurable {

  /**
   * <tt>jets3t.arc.source.input.prefixes.csv</tt> - the property where the
   * prefixes to match for input files are stored (in the future, we may want to
   * extend this to do some sort of globbing or regular expression matching).
   * 
   * @see #setInputPrefixes
   */
  public static final String P_INPUT_PREFIXES = "jets3t.arc.source.input.prefixes.csv";

  /**
   * <tt>jets3t.arc.source.aws.access.key.id</tt> - the property where the AWS
   * Access Key ID to access the S3 account is stored.
   * 
   * @see #setAWSAccessKeyID
   */
  public static final String P_AWS_ACCESS_KEY_ID = "jets3t.arc.source.aws.access.key.id";

  /**
   * <tt>jets3t.arc.source.aws.secret.access.key</tt> - the property where the
   * AWS Secret Access Key to access the S3 account is stored.
   * 
   * @see #setAWSSecretAccessKey
   */
  public static final String P_AWS_SECRET_ACCESS_KEY = "jets3t.arc.source.aws.secret.access.key";

  /**
   * <tt>jets3t.arc.source.bucket.name</tt> - the property where the name of the
   * S3 bucket to access is stored.
   * 
   * @see #setBucketName
   */
  public static final String P_BUCKET_NAME = "jets3t.arc.source.bucket.name";

  /**
   * <tt>jets3t.arc.source.max.tries</tt> - the property where the maximum
   * number of times to try reading each file is stored.
   * 
   * @see #setMaxRetries
   */
  public static final String P_MAX_TRIES = "jets3t.arc.source.max.tries";

  private static final Log LOG = LogFactory.getLog(JetS3tARCSource.class);

  /**
   * Returns the set of input prefixes for a given job as set by
   * {@link #setInputPrefixes}.
   * 
   * @param job
   *          the job to get the input prefixes from
   * 
   * @see #P_INPUT_PREFIXES
   * 
   * @return a <tt>String[]</tt> with the input prefixes, or <tt>null</tt> if
   *         they have not been set
   */
  public static String[] getInputPrefixes(JobConf job) {
    String inputPrefixes = job.get(P_INPUT_PREFIXES);
    return inputPrefixes == null ? null : EscapeUtils.split(',', inputPrefixes);
  }

  /**
   * Sets the AWS access key ID of the S3 account to use.
   * 
   * @param job
   *          the job to set the AWS access key ID for
   * @param awsAccessKeyId
   *          the AWS access key ID
   * 
   * @see #P_AWS_ACCESS_KEY_ID
   */
  public static final void setAWSAccessKeyID(JobConf job, String awsAccessKeyId) {
    job.set(P_AWS_ACCESS_KEY_ID, awsAccessKeyId);
  }

  /**
   * Sets the AWS access key ID of the S3 account to use.
   * 
   * @param job
   *          the job to set the AWS secret access key for
   * @param awsSecretAccessKey
   *          the AWS secret access key
   * 
   * @see #P_AWS_SECRET_ACCESS_KEY
   */
  public static final void setAWSSecretAccessKey(JobConf job, String awsSecretAccessKey) {
    job.set(P_AWS_SECRET_ACCESS_KEY, awsSecretAccessKey);
  }

  /**
   * Sets the name of the S3 bucket to read from.
   * 
   * @param job
   *          the job to set the bucket name for
   * @param bucketName
   *          the bucket name
   * 
   * @see #P_BUCKET_NAME
   */
  public static final void setBucketName(JobConf job, String bucketName) {
    job.set(P_BUCKET_NAME, bucketName);
  }

  /**
   * Sets the key prefixes used to decide which S3 objects are included for
   * processing.
   * 
   * <p>
   * Call this method to set up the input parameters for a job. The matching
   * will be a pure prefix query with no delimiters.
   * 
   * @param job
   *          the job to set the prefix for
   * @param prefixes
   *          the key prefixes to match S3 objects with
   * 
   * @see #P_INPUT_PREFIXES
   */
  public static void setInputPrefixes(JobConf job, String... prefixes) {
    job.set(P_INPUT_PREFIXES, EscapeUtils.concatenate(',', prefixes));
  }

  /**
   * Sets the maximum number of times to try reading a file.
   * 
   * <p>
   * Default is 4.
   * 
   * @param job
   *          the job to set the maximum number of retries for
   * @param maxTries
   *          the maximum number of attempts per file
   */
  public static final void setMaxRetries(JobConf job, int maxtries) {
    job.setInt(P_MAX_TRIES, maxtries);
  }

  private S3Service service;
  private S3Bucket bucket;
  private int maxTries;

  /**
   * @inheritDoc
   */
  @Override
  protected void configureImpl(JobConf job) {
    try {

      // Pull credentials from the configuration
      String awsAccessKeyId = getProperty(job, P_AWS_ACCESS_KEY_ID);
      String awsSecretAccessKey = getProperty(job, P_AWS_SECRET_ACCESS_KEY);
      String bucketName = getProperty(job, P_BUCKET_NAME);

      // Instantiate JetS3t classes
      AWSCredentials awsCredentials = new AWSCredentials(awsAccessKeyId, awsSecretAccessKey);
      service = new RestS3Service(awsCredentials);
      bucket = new S3Bucket(bucketName);

      maxTries = job.getInt(P_MAX_TRIES, 4);

    } catch (S3ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @inheritDoc
   */
  @Override
  protected Collection<ARCResource> getARCResources(JobConf job) throws IOException {

    try {
      String[] inputPrefixes = getInputPrefixes(job);
      if (inputPrefixes == null) {
        throw new IllegalArgumentException("No inputs prefixes set");
      }

      Map<String, ARCResource> resources = new HashMap<String, ARCResource>();
      for (String prefix : getInputPrefixes(job)) {
        for (S3Object object : service.listObjects(bucket, prefix, null)) {
          String key = object.getKey();
          resources.put(key, new ARCResource(key, object.getContentLength()));
        }
      }
      return resources.values();
    } catch (S3ServiceException e) {
      throw new IOException(e.toString());
    }
  }

  /**
   * Gets a property from a job and throws an exception if it is not set.
   */
  private String getProperty(JobConf job, String property) {
    String result = job.get(property);
    if (result == null) {
      throw new RuntimeException(property + " is not set");
    }
    return result;
  }

  /**
   * @inheritDoc
   */
  public InputStream getStream(String resource, long streamPosition, Throwable lastError, int previousFailures)
      throws Throwable {

    if (lastError == null || previousFailures < maxTries) {

      LOG.info("Opening " + resource + " at byte position " + streamPosition + ", attempt " + (previousFailures + 1)
          + " out of " + maxTries);
      S3Object object = service.getObject(bucket, resource, null, null, null, null, streamPosition, null);
      return object.getDataInputStream();

    } else {

      LOG.info("Too many failures for " + resource + ", aborting");
      return null;
    }
  }
}