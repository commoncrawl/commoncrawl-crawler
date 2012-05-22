/**
 * 
 */
package org.commoncrawl.crawl.database.crawlpipeline.generator.segmenter;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.text.NumberFormat;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.crawl.database.SegmentGeneratorBundleKey;
import org.commoncrawl.crawl.database.SegmentGeneratorItem;
import org.commoncrawl.crawl.database.SegmentGeneratorItemBundle;
import org.commoncrawl.protocol.CrawlDatumAndMetadata;
import org.commoncrawl.protocol.CrawlSegmentHost;
import org.commoncrawl.protocol.CrawlSegmentURL;
import org.commoncrawl.util.internal.GoogleURL;
import org.commoncrawl.util.shared.CCStringUtils;
import org.commoncrawl.util.shared.TextBytes;

public class SegmenterReducer implements Reducer<SegmentGeneratorBundleKey,SegmentGeneratorItemBundle,NullWritable,NullWritable> {

  static final Log LOG = LogFactory.getLog(SegmenterReducer.class);
  private  static NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  static { 
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }

  enum Counters {
    BAD_URL_DURING_HOST_EXTRACTION 
    
  }
  
  @Override
  public void reduce(SegmentGeneratorBundleKey key,Iterator<SegmentGeneratorItemBundle> values,
      OutputCollector<NullWritable, NullWritable> output, Reporter reporter)
  throws IOException {

    while (values.hasNext()) { 
      writeBundle(values.next(),reporter);
    }

  }

  Path _workOutputPath = null;
  Path _debugOutputPath = null;
  int  _activeSegmentId = -1;
  SequenceFile.Writer _activeWriter = null;
  int  _activeSegmentURLCount = -1;
  FileSystem _fs = null;
  JobConf _conf;
  String crawlerNames[] = null;
  int taskNumber;
  int crawlerIndex = -1;
  int bucketIndex = -1;
  String crawlerName = null;
  Writer _urlDebugURLWriter;
  FSDataOutputStream _debugURLStream;
  
  @Override
  public void configure(JobConf job) {
    String crawlers         = job.get(CrawlEnvironment.PROPERTY_CRAWLERS);
    // get buckets per crawler ... 
    int bucketsPerCrawler   = job.getInt(CrawlEnvironment.PROPERTY_NUM_BUCKETS_PER_CRAWLER, 8);
    // extract crawler names ... 
    crawlerNames = crawlers.split(",");
    // get the local task index ... 
    taskNumber = job.getInt("mapred.task.partition", 0);
    // compute crawler index based on num crawlers 
    crawlerIndex = taskNumber / bucketsPerCrawler;
    // compute bucket id 
    bucketIndex = taskNumber % bucketsPerCrawler;
    // get the crawler name .. 
    crawlerName = crawlerNames[crawlerIndex];
    // calculate work path ... 
    _workOutputPath = new Path(FileOutputFormat.getWorkOutputPath(job),crawlerName +"/"+NUMBER_FORMAT.format(bucketIndex));
    
    _debugOutputPath  = new Path(FileOutputFormat.getWorkOutputPath(job),"debug/" + crawlerName +"/"+NUMBER_FORMAT.format(bucketIndex));

    try {
      _fs = FileSystem.get(job);
      LOG.info("Making Directory:" + _workOutputPath);
      _fs.mkdirs(_workOutputPath);
      _fs.mkdirs(_debugOutputPath);
      _conf = job;
    } catch (IOException e) {
      LOG.error(CCStringUtils.stringifyException(e));
    }
  }

  @Override
  public void close() throws IOException {
    flushActiveWriter();
  }

  DataInputBuffer datumReaderStream = new DataInputBuffer();
  CrawlDatumAndMetadata datum = new CrawlDatumAndMetadata();
  TextBytes urlBytes = new TextBytes();

  void writeBundle(SegmentGeneratorItemBundle bundle,Reporter reporter)throws IOException { 

    reporter.incrCounter("", "GOT_BUNDLE", 1);

    CrawlSegmentHost host = null;
    int originalPosition = 0;
    for (SegmentGeneratorItem item : bundle.getUrls()) { 

      if (host == null) {
        
        urlBytes.set(item.getUrlAsBuffer().getReadOnlyBytes(),0,item.getUrlAsBuffer().getCount());
        
        GoogleURL urlObject = new GoogleURL(item.getUrl().toString());
        
        String hostName = urlObject.getHost();
        
        if (hostName == null || hostName.length() == 0) {
          reporter.incrCounter(Counters.BAD_URL_DURING_HOST_EXTRACTION, 1);
          continue;
        }
        else {
          reporter.incrCounter("", "GENERATED_SEGMENT_HOST", 1);
          host = new CrawlSegmentHost();
          host.setHostFP(bundle.getHostFP());
          host.setHostName(hostName);
          //LOG.info("Allocated new CrawlSegmentHost:" + hostName);
        }
      }
      // create url item object ...
      CrawlSegmentURL urlObjectOut = new CrawlSegmentURL();

      urlObjectOut.getUrlAsTextBytes().set(item.getUrlAsTextBytes());
      urlObjectOut.setFieldDirty(CrawlSegmentURL.Field_URL);
      urlObjectOut.setUrlFP(item.getUrlFP());
      urlObjectOut.setCrawlSegmentId(_activeSegmentId);
      urlObjectOut.setOriginalPosition(originalPosition++);

      if (item.isFieldDirty(SegmentGeneratorItem.Field_LASTMODIFIEDTIME)) {
        urlObjectOut.setLastModifiedTime(item.getLastModifiedTime());
      }
      if (item.isFieldDirty(SegmentGeneratorItem.Field_ETAG)) { 
        urlObjectOut.setEtag(item.getEtag());
      }
      // debug 
      //urlBytes.set(item.getUrlAsBuffer().getReadOnlyBytes(),0,item.getUrlAsBuffer().getCount());
      //LOG.info("Added URL:" + urlBytes.toString());

      // add to host ... 
      host.getUrlTargets().add(urlObjectOut);
    }

    if (host != null) { 
      // sort the targets in ascending order ... 
      //Collections.sort(host.getUrlTargets());
      // access writer ...
      SequenceFile.Writer writer = ensureWriter(reporter);
      reporter.incrCounter("", "APPENDED_HOST_TO_FILE", 1);
      // and spit the host out into the file ... 
      writer.append(new LongWritable(host.getHostFP()),host);
      
      _urlDebugURLWriter.append("\nHost:" + host.getHostName());
      for (CrawlSegmentURL urlObject : host.getUrlTargets()) { 
        _urlDebugURLWriter.append("   " + urlObject.getUrl());
      }
      
      // ok increment url count for active segment 
      _activeSegmentURLCount += host.getUrlTargets().size();
      //LOG.info("Wrote Host:" + host.getHostName() + " URLCount:" + host.getUrlTargets().size() + " Segment URLCount:"+ _activeSegmentURLCount);
      // now see if need a new segment 
      if (_activeSegmentURLCount >= Segmenter.SEGMENT_SIZE_MAX) {
        //LOG.info("Flushing Active Segment");
        flushActiveWriter();
      }
    }
  }

  void flushActiveWriter() throws IOException { 
    if (_activeWriter != null) {
      // flush 
      _activeWriter.close();
      _activeWriter = null;
      _activeSegmentURLCount =0;				
    }
    if (_debugURLStream != null) { 
      _urlDebugURLWriter.flush();
      _debugURLStream.close();
      _urlDebugURLWriter = null;
      _debugURLStream = null;
    }
  }

  SequenceFile.Writer ensureWriter(Reporter reporter)throws IOException { 
    if (_activeWriter == null) {
      // increment segment id 
      _activeSegmentId++;
      // create path 
      Path outputPath = new Path(_workOutputPath,Integer.toString(_activeSegmentId));
      Path debugPath = new Path(_debugOutputPath,Integer.toString(_activeSegmentId));

      reporter.incrCounter("", "CREATED_WRITER", 1);
      
      LOG.info("Creating Writer at:" + outputPath);
      
      _activeWriter = SequenceFile.createWriter(
          _fs, 
          _conf, 
          outputPath, 
          LongWritable.class,
          CrawlSegmentHost.class,
          SequenceFileOutputFormat.getOutputCompressionType(_conf), 
          reporter);

      _debugURLStream = _fs.create(debugPath);
      _urlDebugURLWriter = new OutputStreamWriter(_debugURLStream,Charset.forName("UTF-8"));
      
      _activeSegmentURLCount = 0;
    }
    return _activeWriter;
  }

}