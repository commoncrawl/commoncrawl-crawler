/**
 * Copyright 2008 - CommonCrawl Foundation
 * 
 * CommonCrawl licenses this file to you under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.commoncrawl.crawl.database.crawlpipeline.merger.reducers;

import java.io.IOException;
import java.io.OutputStream;
import java.text.NumberFormat;
import java.util.Iterator;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.nutch.crawl.CrawlDatum;
import org.commoncrawl.protocol.CrawlDatumAndMetadata;
import org.commoncrawl.protocol.CrawlURLMetadata;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.util.internal.URLUtils;
import org.commoncrawl.util.internal.MultiFileMergeUtils.MultiFileInputReader;
import org.commoncrawl.util.internal.MultiFileMergeUtils.MultiFileInputReader.KeyAndValueData;
import org.commoncrawl.util.internal.URLUtils.URLFPV2RawComparator;
import org.commoncrawl.util.shared.CCStringUtils;
import org.commoncrawl.util.shared.TextBytes;

/** 
 * The merging reducer that produces a new crawl database 
 * 
 * 
 * @author rana
 *
 */
public class MergeReducer implements
		Reducer<IntWritable, Text, URLFPV2, CrawlDatumAndMetadata> {

	static final Log LOG = LogFactory.getLog(MergeReducer.class);

	FileSystem _fs;
	Configuration _conf;
	private int _partNumber;
	OutputStream _urlOutputStream;

	private static final NumberFormat NUMBER_FORMAT = NumberFormat
			.getInstance();
	static {
		NUMBER_FORMAT.setMinimumIntegerDigits(5);
		NUMBER_FORMAT.setGroupingUsed(false);
	}

	enum Counters {
		GOT_CRAWLDB_RECORD,
		GOT_LINK_RECORD, 
		SKIPPING_DUP_LINK_RECORD, 
		GOT_FETCH_RECORD, 
		REPLACED_DUPE_FETCH_RECORD, 
		GOT_LINKDB_RECORD, 
		REPLACED_DUPE_LINKDB_RECORD, 
		GOT_INV_LINKDB_RECORD, 
		REPLACED_DUPE_INV_LINKDB_RECORD, 
		GOT_PAGERANK_RECORD, 
		GOT_ARCFILE_RECORD, 
		GOT_DUPE_ARCFILE_RECORD, 
		NO_CRAWLDB_RECORD_USING_FETCH_RECORD, 
		NO_CRAWLDB_RECORD_USING_LINKED_RECORD, 
		MERGED_FETCHED_INTO_CRAWL_DB_RECORD, 
		ARC_INFO_SIZE_INCREASED_VIA_FETCH_RECORD, 
		MERGED_LINKDB_RECORD_INTO_CRAWLDB_RECORD, 
		MERGED_INV_LINKDB_RECORD_INTO_CRAWLDB_RECORD, 
		MERGED_PR_RECORD_INTO_CRAWLDB_RECORD, 
		MERGED_ARCFILE_RECORD_INTO_CRAWLDB_RECORD, 
		ARC_INFO_SIZE_DECREASED_VIA_FETCH_RECORD, 
		GOT_FETCH_NOTMODIFIED_STATUS, 
		GOT_FETCH_SUCCESS_STATUS, 
		GOT_FETCH_REDIR_TEMP_STATUS, 
		GOT_FETCH_REDIR_PERM_STATUS, 
		GOT_FETCH_GONE_STATUS, 
		GOT_FETCH_RETRY_STATUS, 
		URL_NULL_AFTER_CANONICALIZATION, 
		TARGET_FP_CRAWL_DB_PTR_NULL, 
		TARGET_FP_USING_FETCH_RECORD, 
		TARGET_FP_USING_LINK_RECORD, 
		TARGET_FP_MERGING_FETCH_STATE_INTO_CRAWLDB_STATE, 
		HIT_VALID_CANDIDATE, 
		SKIPPED, 
		HAD_VALID_SIZE
	}

	@Override
	public void reduce(IntWritable key, Iterator<Text> values,
			OutputCollector<URLFPV2, CrawlDatumAndMetadata> output,
			Reporter reporter) throws IOException {

		// collect all incoming paths first
		Vector<Path> incomingPaths = new Vector<Path>();

		while (values.hasNext()) {

			String path = values.next().toString();
			LOG.info("Found Incoming Path:" + path);
			incomingPaths.add(new Path(path));
		}

		// set up merge attributes
		JobConf localMergeConfig = new JobConf(_conf);

		localMergeConfig.setClass(
				MultiFileInputReader.MULTIFILE_COMPARATOR_CLASS,
				URLFPV2RawComparator.class, RawComparator.class);
		localMergeConfig.setClass(MultiFileInputReader.MULTIFILE_KEY_CLASS,
				URLFPV2.class, WritableComparable.class);

		// ok now spawn merger
		MultiFileInputReader<URLFPV2> multiFileInputReader = new MultiFileInputReader<URLFPV2>(
				_fs, incomingPaths, localMergeConfig);

		// now read one set of values at a time and output result
		KeyAndValueData<URLFPV2> keyValueData = null;
		// temporary data input buffer
		DataInputBuffer dataInputStream = new DataInputBuffer();

		FloatWritable pageRankValue = new FloatWritable();
		boolean pageRankValid;

		while ((keyValueData = multiFileInputReader.readNextItem()) != null) {

			reporter.progress();

			CrawlDatumAndMetadata fetched_status_ptr = null;
			CrawlDatumAndMetadata linked_status_ptr = null;
			CrawlDatumAndMetadata arcfile_ptr = null;
			CrawlDatumAndMetadata crawldb_ptr = null;
			CrawlURLMetadata linkdb_ptr = null;
			CrawlURLMetadata invlinkdb_ptr = null;
			pageRankValid = false;

			// iterate values ...
			for (int i = 0; i < keyValueData._values.size(); ++i) {

				// extract path
				String currentItemPath = keyValueData._values.get(i).source
						.toString();
				// deserialize it
				dataInputStream.reset(
						keyValueData._values.get(i).data.getData(),
						keyValueData._values.get(i).data.getLength());

				// figure out the source

				if (currentItemPath.contains("/crawl/crawldb_new")) {
					// ok original datum record
					crawldb_ptr = new CrawlDatumAndMetadata();
					crawldb_ptr.readFields(dataInputStream);

					reporter.incrCounter(Counters.GOT_CRAWLDB_RECORD, 1);
				} else if (currentItemPath.contains("/crawl/merge/inputs")) {
					// ok read the input ...
					CrawlDatumAndMetadata metadata = new CrawlDatumAndMetadata();
					metadata.readFields(dataInputStream);

					if (metadata.getStatus() == CrawlDatum.STATUS_LINKED) {
						reporter.incrCounter(Counters.GOT_LINK_RECORD, 1);
						if (linkdb_ptr == null) {
							linked_status_ptr = metadata;
						} else {
							reporter.incrCounter(
									Counters.SKIPPING_DUP_LINK_RECORD, 1);
						}
					} else {
						reporter.incrCounter(Counters.GOT_FETCH_RECORD, 1);

						if (fetched_status_ptr != null) {
							reporter.incrCounter(
									Counters.REPLACED_DUPE_FETCH_RECORD, 1);
						}

						if (fetched_status_ptr == null
								|| metadata.getFetchTime() > fetched_status_ptr
										.getFetchTime()
								|| metadata.getStatus() == CrawlDatum.STATUS_FETCH_SUCCESS
								&& fetched_status_ptr.getStatus() != CrawlDatum.STATUS_FETCH_SUCCESS) {
							// if old record is present ...
							if (fetched_status_ptr != null) {
								// new record trumps old record except if old
								// record is
								// success record and new record is not
								if (metadata.getStatus() == CrawlDatum.STATUS_FETCH_SUCCESS
										|| fetched_status_ptr.getStatus() != CrawlDatum.STATUS_FETCH_SUCCESS) {
									fetched_status_ptr = metadata;
								}
							} else {
								fetched_status_ptr = metadata;
							}
						}
					}
				} else if (currentItemPath.contains("crawl/linkdb/merged")) {
					reporter.incrCounter(Counters.GOT_LINKDB_RECORD, 1);
					CrawlURLMetadata metadataTemp = new CrawlURLMetadata();

					metadataTemp.readFields(dataInputStream);
					if (linkdb_ptr == null
							|| metadataTemp.getLinkDBSegmentNo() > linkdb_ptr
									.getLinkDBSegmentNo()) {

						if (linkdb_ptr != null)
							reporter.incrCounter(
									Counters.REPLACED_DUPE_LINKDB_RECORD, 1);
						linkdb_ptr = metadataTemp;
					}
				} else if (currentItemPath
						.contains("crawl/inverse_linkdb/merged")) {
					reporter.incrCounter(Counters.GOT_INV_LINKDB_RECORD, 1);

					if (invlinkdb_ptr != null)
						reporter.incrCounter(
								Counters.REPLACED_DUPE_INV_LINKDB_RECORD, 1);
					invlinkdb_ptr = new CrawlURLMetadata();
					invlinkdb_ptr.readFields(dataInputStream);
				} else if (currentItemPath.contains("crawl/pageRank/db")) {
					reporter.incrCounter(Counters.GOT_PAGERANK_RECORD, 1);
					pageRankValue.readFields(dataInputStream);
					pageRankValid = true;
				} else if (currentItemPath
						.contains("crawl/merge/arcfileMetadata")) {
					reporter.incrCounter(Counters.GOT_ARCFILE_RECORD, 1);
					CrawlDatumAndMetadata metadataTemp = new CrawlDatumAndMetadata();
					metadataTemp.readFields(dataInputStream);

					if (arcfile_ptr != null) {
						reporter.incrCounter(Counters.GOT_DUPE_ARCFILE_RECORD,
								1);
						arcfile_ptr
								.getMetadata()
								.getArchiveInfo()
								.addAll(metadataTemp.getMetadata()
										.getArchiveInfo());
					} else {
						arcfile_ptr = metadataTemp;
					}
				}
			}

			// ok now figure out what to do ...

			int oldCrawlDBStatus = -1;
			int oldCrawlDBProtocolStatus = -1;
			TextBytes oldRedirectLocation = null;

			boolean isTargetFP = keyValueData._keyObject.getDomainHash() == -7358714350918129630L
					&& keyValueData._keyObject.getUrlHash() == -5586402172585772174L;

			// if crawl db ptr is null ...
			if (crawldb_ptr == null) {
				if (isTargetFP) {
					reporter.incrCounter(Counters.TARGET_FP_CRAWL_DB_PTR_NULL,
							1);
				}
				// see if fetched ptr is not null...
				if (fetched_status_ptr != null) {
					// adopt fetched status as final status
					crawldb_ptr = fetched_status_ptr;
					reporter.incrCounter(
							Counters.NO_CRAWLDB_RECORD_USING_FETCH_RECORD, 1);
					if (isTargetFP) {
						reporter.incrCounter(
								Counters.TARGET_FP_USING_FETCH_RECORD, 1);
					}
				} else if (linked_status_ptr != null) {
					// adopt linked status as final status
					crawldb_ptr = linked_status_ptr;
					reporter.incrCounter(
							Counters.NO_CRAWLDB_RECORD_USING_LINKED_RECORD, 1);
					// ok reset status to unfected ...
					crawldb_ptr.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
					// and earliest discovery date ...
					crawldb_ptr.setDiscoveryDateSecs((int) (System
							.currentTimeMillis() / 1000));

					if (isTargetFP) {
						reporter.incrCounter(
								Counters.TARGET_FP_USING_LINK_RECORD, 1);
					}

				}

				if (crawldb_ptr != null) {
					// either way, normalize the url now ...
					String normalizedURL = URLUtils.canonicalizeURL(
							crawldb_ptr.getUrl(), false);

					if (normalizedURL == null) {
						reporter.incrCounter(
								Counters.URL_NULL_AFTER_CANONICALIZATION, 1);
						// drop the url...
						crawldb_ptr = null;
					} else {
						crawldb_ptr.setUrl(normalizedURL);
					}
				}

			}
			// otherwise ... if crawl db record is present ...
			else {

				// retain old crawl db status
				oldCrawlDBStatus = crawldb_ptr.getStatus();
				oldCrawlDBProtocolStatus = crawldb_ptr.getProtocolStatus();
				oldRedirectLocation = crawldb_ptr
						.getRedirectLocationAsTextBytes();

				// if fetched status is valid ...
				if (fetched_status_ptr != null) {

					if (isTargetFP) {
						reporter.incrCounter(
								Counters.TARGET_FP_MERGING_FETCH_STATE_INTO_CRAWLDB_STATE,
								1);
						LOG.info("Fetch URL is:" + fetched_status_ptr.getUrl());
					}
					// merge fetched status into crawl db status
					try {
						// clear url bit in fetched status
						fetched_status_ptr
								.setFieldClean(CrawlDatumAndMetadata.Field_URL);
						int arcInfoSizeOrig = crawldb_ptr.getMetadata()
								.getArchiveInfo().size();
						crawldb_ptr.merge(fetched_status_ptr);
						reporter.incrCounter(
								Counters.MERGED_FETCHED_INTO_CRAWL_DB_RECORD, 1);
						if (crawldb_ptr.getMetadata().getArchiveInfo().size() < arcInfoSizeOrig) {
							// BAD!!!
							reporter.incrCounter(
									Counters.ARC_INFO_SIZE_DECREASED_VIA_FETCH_RECORD,
									1);
						}
					} catch (CloneNotSupportedException e) {
					}
				}
			}

			// ok now merge in link data if present ...
			if (crawldb_ptr != null) {

				// ok flip fetched status as appropriate ...
				switch (crawldb_ptr.getStatus()) {

				case CrawlDatum.STATUS_FETCH_NOTMODIFIED: {
					reporter.incrCounter(Counters.GOT_FETCH_NOTMODIFIED_STATUS,
							1);
					crawldb_ptr.setStatus(CrawlDatum.STATUS_DB_NOTMODIFIED);
				}
					break;

				case CrawlDatum.STATUS_FETCH_SUCCESS: {
					crawldb_ptr.setStatus(CrawlDatum.STATUS_DB_FETCHED);
					reporter.incrCounter(Counters.GOT_FETCH_SUCCESS_STATUS, 1);
				}
					break;
				case CrawlDatum.STATUS_FETCH_REDIR_TEMP: {
					reporter.incrCounter(Counters.GOT_FETCH_REDIR_TEMP_STATUS,
							1);
					crawldb_ptr.setStatus(CrawlDatum.STATUS_DB_REDIR_TEMP);
				}
					break;

				case CrawlDatum.STATUS_FETCH_REDIR_PERM: {
					reporter.incrCounter(Counters.GOT_FETCH_REDIR_PERM_STATUS,
							1);
					crawldb_ptr.setStatus(CrawlDatum.STATUS_DB_REDIR_PERM);
				}
					break;

				case CrawlDatum.STATUS_FETCH_GONE: {
					reporter.incrCounter(Counters.GOT_FETCH_GONE_STATUS, 1);
					crawldb_ptr.setStatus(CrawlDatum.STATUS_DB_GONE);
				}
					break;
				case CrawlDatum.STATUS_FETCH_RETRY: {
					reporter.incrCounter(Counters.GOT_FETCH_RETRY_STATUS, 1);

					if (oldCrawlDBStatus != -1) {
						crawldb_ptr.setStatus((byte) oldCrawlDBStatus);
						// if protocol status is not null
						if (oldCrawlDBProtocolStatus != -1) {
							crawldb_ptr
									.setProtocolStatus((byte) oldCrawlDBProtocolStatus);
						}
						if (oldRedirectLocation != crawldb_ptr
								.getRedirectLocationAsTextBytes()
								&& oldRedirectLocation.getLength() != 0) {
							crawldb_ptr.setRedirectLocation(oldRedirectLocation
									.toString());
						}
					} else {
						// mark document as unfeteched
						crawldb_ptr.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
					}
					// increment failed attempts counter
					crawldb_ptr.setFailedAttemptsCounter(crawldb_ptr
							.getFailedAttemptsCounter() + 1);
				}
					break;
				}

				// zero out certain metadata fields
				crawldb_ptr.getMetadata().setFieldClean(
						CrawlURLMetadata.Field_LINKDBFILENO);
				crawldb_ptr.getMetadata().setFieldClean(
						CrawlURLMetadata.Field_LINKDBOFFSET);
				crawldb_ptr.getMetadata().setFieldClean(
						CrawlURLMetadata.Field_LINKDBSEGMENTNO);
				crawldb_ptr.getMetadata().setFieldClean(
						CrawlURLMetadata.Field_INVERSEDBFILENO);
				crawldb_ptr.getMetadata().setFieldClean(
						CrawlURLMetadata.Field_INVERSEDBOFFSET);
				crawldb_ptr.getMetadata().setFieldClean(
						CrawlURLMetadata.Field_PAGERANK);

				if (linkdb_ptr != null) {
					try {
						reporter.incrCounter(
								Counters.MERGED_LINKDB_RECORD_INTO_CRAWLDB_RECORD,
								1);
						crawldb_ptr.getMetadata().merge(linkdb_ptr);
					} catch (CloneNotSupportedException e) {
					}
				}
				if (invlinkdb_ptr != null) {
					try {
						reporter.incrCounter(
								Counters.MERGED_INV_LINKDB_RECORD_INTO_CRAWLDB_RECORD,
								1);
						crawldb_ptr.getMetadata().merge(invlinkdb_ptr);
					} catch (CloneNotSupportedException e) {
					}
				}
				if (pageRankValid) {
					reporter.incrCounter(
							Counters.MERGED_PR_RECORD_INTO_CRAWLDB_RECORD, 1);
					crawldb_ptr.getMetadata().setPageRank(pageRankValue.get());
				}

				if (arcfile_ptr != null) {
					try {
						reporter.incrCounter(
								Counters.MERGED_ARCFILE_RECORD_INTO_CRAWLDB_RECORD,
								1);
						crawldb_ptr.getMetadata().merge(
								arcfile_ptr.getMetadata());
					} catch (CloneNotSupportedException e) {
					}
				}

				if (isTargetFP) {
					LOG.info("Final URL is:" + crawldb_ptr.getUrl());
				}

				// ok we need to output the url data as well
				_urlOutputStream.write(crawldb_ptr.getUrlAsTextBytes()
						.getBytes(), crawldb_ptr.getUrlAsTextBytes()
						.getOffset(), crawldb_ptr.getUrlAsTextBytes()
						.getLength());

				_urlOutputStream.write((int) '\n');

				output.collect(keyValueData._keyObject, crawldb_ptr);
			}
		}
		multiFileInputReader.close();

	}

	@Override
	public void configure(JobConf job) {

		GzipCodec codec = new GzipCodec();
		codec.setConf(job);

		_conf = job;

		_partNumber = job.getInt("mapred.task.partition", 0);

		try {
			_fs = FileSystem.get(job);
			// path
			Path urlStreamOutputPath = new Path(
					FileOutputFormat.getWorkOutputPath(job), "urlData-"
							+ NUMBER_FORMAT.format(_partNumber));
			// outputstream
			FSDataOutputStream urlOutputStream = _fs
					.create(urlStreamOutputPath);
			// compressed output stream
			_urlOutputStream = codec.createOutputStream(urlOutputStream);

		} catch (IOException e1) {
			LOG.error(CCStringUtils.stringifyException(e1));
		}
	}

	@Override
	public void close() throws IOException {
		if (_urlOutputStream != null) {
			_urlOutputStream.flush();
			_urlOutputStream.close();
		}
	}

}