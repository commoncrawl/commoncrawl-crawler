package org.commoncrawl.mapred.pipelineV1;

import java.io.File;
import java.io.IOException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.file.tfile.TFile;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.hadoop.mergeutils.MergeSortSpillWriter;
import org.commoncrawl.hadoop.mergeutils.RawDataSpillWriter;
import org.commoncrawl.hadoop.mergeutils.RawKeyValueComparator;
import org.commoncrawl.hadoop.mergeutils.SequenceFileSpillWriter;
import org.commoncrawl.hadoop.util.LongWritableComparator;
import org.commoncrawl.protocol.CrawlDatumAndMetadata;
import org.commoncrawl.protocol.CrawlURLMetadata;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.service.crawler.filters.SuperDomainFilter;
import org.commoncrawl.service.crawler.filters.Utils;
import org.commoncrawl.service.crawler.filters.Filter.FilterResult;
import org.commoncrawl.service.queryserver.index.PositionBasedSequenceFileIndex;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.CompressedURLFPListV2;
import org.commoncrawl.util.CrawlDatum;
import org.commoncrawl.util.FileUtils;
import org.commoncrawl.util.FlexBuffer;
import org.commoncrawl.util.NodeAffinityMaskBuilder;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLUtils;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileMergeInputFormat;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileMergePartitioner;
import org.commoncrawl.util.MultiFileMergeUtils.MultiFileInputReader.KeyAndValueData;
import org.commoncrawl.util.URLUtils.URLFPV2RawComparator;
import org.junit.Test;

public class InverseLinksByDomainDBBuilder extends CrawlDBCustomJob  {

	private static final Log LOG = LogFactory.getLog(InverseLinksByDomainDBBuilder.class);
	
	@Override
  public String getJobDescription() {
	  return "Inverse Link DB By Domain Builder";
  }

  enum Counters {
    GOT_CRAWLDB_RECORD, GOT_LINK_RECORD, SKIPPING_DUP_LINK_RECORD, GOT_FETCH_RECORD, REPLACED_DUPE_FETCH_RECORD, GOT_LINKDB_RECORD, REPLACED_DUPE_LINKDB_RECORD, GOT_INV_LINKDB_RECORD, REPLACED_DUPE_INV_LINKDB_RECORD, GOT_PAGERANK_RECORD, GOT_ARCFILE_RECORD, GOT_DUPE_ARCFILE_RECORD, NO_CRAWLDB_RECORD_USING_FETCH_RECORD, NO_CRAWLDB_RECORD_USING_LINKED_RECORD, MERGED_FETCHED_INTO_CRAWL_DB_RECORD, ARC_INFO_SIZE_INCREASED_VIA_FETCH_RECORD, MERGED_LINKDB_RECORD_INTO_CRAWLDB_RECORD, MERGED_INV_LINKDB_RECORD_INTO_CRAWLDB_RECORD, MERGED_PR_RECORD_INTO_CRAWLDB_RECORD, MERGED_ARCFILE_RECORD_INTO_CRAWLDB_RECORD, ARC_INFO_SIZE_DECREASED_VIA_FETCH_RECORD, GOT_FETCH_NOTMODIFIED_STATUS, GOT_FETCH_SUCCESS_STATUS, GOT_FETCH_REDIR_TEMP_STATUS, GOT_FETCH_REDIR_PERM_STATUS, GOT_FETCH_GONE_STATUS, GOT_FETCH_RETRY_STATUS, URL_NULL_AFTER_CANONICALIZATION, TARGET_FP_CRAWL_DB_PTR_NULL 
  	
  , TARGET_FP_USING_FETCH_RECORD, TARGET_FP_USING_LINK_RECORD, TARGET_FP_MERGING_FETCH_STATE_INTO_CRAWLDB_STATE, HIT_VALID_CANDIDATE, SKIPPED, HAD_VALID_SIZE, KEY_SIZE_GT_4096}
	
	

	private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }		
  
  static String PART_PREFIX = "part-";
  
	private long findLatestDatabaseTimestamp(Path rootPath)throws IOException { 
		FileSystem fs = CrawlEnvironment.getDefaultFileSystem();
		
		FileStatus candidates[] = fs.globStatus(new Path(rootPath,"*"));
		
		long candidateTimestamp = -1L;
		
		for (FileStatus candidate : candidates) {
			LOG.info("Found Seed Candidate:" + candidate.getPath());
			try { 
				long timestamp = Long.parseLong(candidate.getPath().getName());
				if (candidateTimestamp == -1 || candidateTimestamp < timestamp) { 
					candidateTimestamp = timestamp;
				}
			}
			catch (NumberFormatException e) { 
				LOG.info("Skipping non-numeric path item:" + candidate.getPath());
			}
		}
		LOG.info("Selected Candidate is:"+ candidateTimestamp);
		return candidateTimestamp;
	}	

	
	@Override
  public void runJob() throws IOException {
		
		FileSystem fs = CrawlEnvironment.getDefaultFileSystem();
		
		// find latest metadata db timestamp
		long linkDBCandidateTimestamp = findLatestDatabaseTimestamp(new Path("crawl/metadatadb"));
		long crawlDBCandidateTimestamp = findLatestDatabaseTimestamp(new Path("crawl/crawldb_new"));
		
		if (linkDBCandidateTimestamp == -1 || crawlDBCandidateTimestamp == -1) { 
			throw new IOException("No MetadataDB or CrawlDB Candidate Found!!!");
		}
		
		// construct various paths ... 
		final Path linkDBPath = new Path("crawl/linkdb/merged" + linkDBCandidateTimestamp + "/linkData");
		final Path crawlDBPath = new Path("crawl/crawldb_new/" + crawlDBCandidateTimestamp);
		final Path inverseLinkDBByDomainRoot = new Path("crawl/inverseLinkDB_ByDomain/" + linkDBCandidateTimestamp);
		
		// make root path 
		fs.mkdirs(inverseLinkDBByDomainRoot);
		// ok make intermediate paths ... 
		final Path phase1DataPath = new Path(inverseLinkDBByDomainRoot,"phase1Data");
		final Path phase1DebugDataPath = new Path(inverseLinkDBByDomainRoot,"phase1DataDebug");
		final Path phase2DataPath = new Path(inverseLinkDBByDomainRoot,"phase2Data");
		final Path phase3DataPath = new Path(inverseLinkDBByDomainRoot,"phase3Data");
		final Path debugData = new Path(inverseLinkDBByDomainRoot,"debugData");
		
		if (!fs.exists(phase3DataPath)) { 
			// ok first see if phase 1 data exists ... 
			if (!fs.exists(phase1DataPath)) { 
				// execute phase 1
				runPhase1(linkDBPath, crawlDBPath, phase1DataPath);
			}
			
			if (!fs.exists(phase2DataPath)) { 
				// execute phase 2.. 
				// run phase 2
				runPhase2(phase1DataPath,phase2DataPath);
			}
			
			if (!fs.exists(phase3DataPath)) { 
				runPhase3(phase2DataPath,phase3DataPath);
			}
		}
		
		/*
		if (fs.exists(phase1DataPath) && !fs.exists(debugData)) { 
			runDebugJob(phase1DataPath,debugData);
		}
		
		if (!fs.exists(phase1DebugDataPath)) { 
			runDebugPhase1(linkDBPath, crawlDBPath, phase1DebugDataPath);
		}
		*/
		
		
		
		// phase1, join source fingerprint metadata with link graph data and output composite key {target fp,page rank} with a value {source fp}
		
		// key { target root domain, target domain, source page rank, source url}  
		// phase2, identity map it the output and partition by target fp, sort by target root domain id, and then pr -> output 
	  
  }
	
	public static class LinkDataInverter implements Reducer<IntWritable,Text,WritableComparable,Writable> {

		DataOutputBuffer keyStream = new DataOutputBuffer();
		FlexBuffer keyBuffer = new FlexBuffer();
		
		public static final String DEBUG_MODE = "debugMode";
		public static final String DEBUG_DOMAIN_HASH = "DH";
		public static final String DEBUG_URL_HASH = "UH";
		boolean debugMode = false;
		long debugTargetDomain = -1;
		long debugTargetHash   = -1;
		
		@Override
    public void reduce(IntWritable key, Iterator<Text> values,OutputCollector<WritableComparable, Writable> output, Reporter reporter)throws IOException {
      // collect all incoming paths first
    	Vector<Path> incomingPaths = new Vector<Path>();
    	
    	while (values.hasNext()) { 
    		
    		String path = values.next().toString();
    		LOG.info("Found Incoming Path:" + path);
    		incomingPaths.add(new Path(path));
    	}

    	// set up merge attributes 
    	JobConf localMergeConfig = new JobConf(_conf);
    	
    	localMergeConfig.setClass(MultiFileInputReader.MULTIFILE_COMPARATOR_CLASS,URLFPV2RawComparator.class,RawComparator.class);
    	localMergeConfig.setClass(MultiFileInputReader.MULTIFILE_KEY_CLASS,URLFPV2.class,WritableComparable.class);
      
    	// ok now spawn merger 
    	MultiFileInputReader<URLFPV2> multiFileInputReader = new MultiFileInputReader<URLFPV2>(_fs,incomingPaths,localMergeConfig);
    	
    	//now read one set of values at a time and output result
    	KeyAndValueData<URLFPV2> keyValueData = null;
    	// temporary data input buffer 
    	DataInputBuffer dataInputStream = new DataInputBuffer();
    	DataInputBuffer linkDataStream  = new DataInputBuffer();

    	boolean hasLinkData;
    	CrawlDatumAndMetadata metadata = null;
    	
    	
    	while ((keyValueData = multiFileInputReader.readNextItem()) != null) {
    		
    		metadata = null;
    		hasLinkData = false;
    		
    		
    		reporter.progress();
    		
    		// iterate values ... 
    		for (int i=0;i<keyValueData._values.size();++i) {
    			// extract path 
    			String currentItemPath = keyValueData._values.get(i).source.toString();
    		
    			dataInputStream.reset(keyValueData._values.get(i).data.getData(),keyValueData._values.get(i).data.getLength());
    			
	    		if (currentItemPath.contains("crawl/crawldb_new")) {
	      		// deserialize it 
	      		
	    			reporter.incrCounter(Counters.GOT_CRAWLDB_RECORD, 1);
	    			metadata = new CrawlDatumAndMetadata();
	    			metadata.readFields(dataInputStream);
	    		}
      		else if (currentItemPath.contains("crawl/linkdb/merged")) {
      			reporter.incrCounter(Counters.GOT_LINKDB_RECORD, 1);
      			linkDataStream.reset(keyValueData._values.get(i).data.getData(),keyValueData._values.get(i).data.getLength());
      			hasLinkData = true;
      		}
    		}
    		
    		// ok in debug mode ... 
    		if (debugMode) { 
    			if (debugTargetDomain == -1 || debugTargetHash == -1) { 
    				throw new IOException("Debug Job Not Initialized!!!");
    				
    			}
    			URLFPV2 sourceFP = keyValueData._keyObject;
    			
    			if (sourceFP.getDomainHash() == debugTargetDomain && sourceFP.getUrlHash() == debugTargetHash) { 
    				// found a match 
    				StringBuffer debugStr = new StringBuffer();

    				debugStr.append("linkStreamSize:" + linkDataStream.getLength() +"\n");
    				debugStr.append("url:" + metadata.getUrl() +"\n");
    				debugStr.append("fetchState:" + CrawlDatum.getStatusName(metadata.getStatus()) +"\n");
    				debugStr.append("linkInfoValid:" + metadata.getMetadata().isFieldDirty(CrawlURLMetadata.Field_LINKDBOFFSET) +"\n");
    				
    				output.collect(new Text(debugStr.toString()), NullWritable.get());
    			}
    			// 
    			
    			continue;
    		}
    		
    		if (metadata != null && hasLinkData && metadata.getMetadata().getPageRank() > .15f) {
    		
    			if (metadata.getUrlAsTextBytes().getLength() >= 4096) {
    				reporter.incrCounter(Counters.KEY_SIZE_GT_4096, 1);
    				LOG.error("Hit Large Key Length:" + metadata.getUrlAsTextBytes().getLength());
    			}
    			else { 
	          // check to see if source root domain is a super domain
	          boolean sourceDomainIsSuperDomain = (superDomainFilter.filterItemByHashIdV2(keyValueData._keyObject.getRootDomainHash()) == FilterResult.Filter_Accept);
	
	    			reporter.incrCounter(Counters.HIT_VALID_CANDIDATE, 1);
	    			
	    			URLFPV2 sourceFP = keyValueData._keyObject;
	    			
	    			
	    			int size = linkDataStream.readInt();
	    			
	    			if (size != 0) { 
	    				
	    				reporter.incrCounter(Counters.HAD_VALID_SIZE, 1);
		          
	    				CompressedURLFPListV2.Reader reader = new CompressedURLFPListV2.Reader(linkDataStream);
		          
	    				TreeSet<Long> visitedDocuments = new TreeSet<Long>();
	    				
		          while (reader.hasNext()) { 
		            
		            URLFPV2 targetFP = reader.next();
		                        
		            boolean isIntraDomainLink = false;
	
		            if (sourceDomainIsSuperDomain) { 
		              if (targetFP.getDomainHash() == targetFP.getDomainHash()) {
		                isIntraDomainLink = true;
		              }
		            }
		            else { 
		              if (targetFP.getRootDomainHash() == sourceFP.getRootDomainHash()) {
		                isIntraDomainLink = true;
		              }            
		            }
		
		            if (!isIntraDomainLink && !visitedDocuments.contains(targetFP.getUrlHash())) {
		            	keyStream.reset();
		            	// ok output the link ... 
		            	
		            	// the root domain this link is pointing to 
		            	keyStream.writeLong(targetFP.getRootDomainHash()); // target root domain
		            	// the page rank of the source document 
		            	keyStream.writeFloat(metadata.getMetadata().getPageRank()); // source rank value  
		            	// the url within the root domain this link points to 
		            	metadata.getUrlAsTextBytes().write(keyStream);
		            	// initialize the flex buffer 
		            	keyBuffer.set(keyStream.getData(),0,keyStream.getLength());
		            	// and write out composite key + source (document) fingerprint 
		            	output.collect(keyBuffer, targetFP);
		            	// updated visited docs so we emit only one outgoing link per document
		            	visitedDocuments.add(targetFP.getUrlHash());
		            }
		            else { 
		            	reporter.incrCounter(Counters.SKIPPED, 1);
		            }
		          }
	    			}
    			}
    		}
    	}
    }

    FileSystem _fs;
    Configuration _conf;
		
    private static SuperDomainFilter superDomainFilter = new SuperDomainFilter(CrawlEnvironment.ROOT_SUPER_DOMAIN_PATH);

    public static void initializeDistributedCache(JobConf job) throws IOException { 

      Utils.initializeCacheSession(job, System.currentTimeMillis());
      LOG.info("Publishing superDomainFilter to Cache");
      superDomainFilter.publishFilter(job);
    }

    
    @Override
    public void configure(JobConf job) {
      
      LOG.info("Loading superDomainFilter to Cache");
      try {
        superDomainFilter.loadFromCache(job);
      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
      }
    	
			_conf = job;
      
			debugMode = job.getBoolean(DEBUG_MODE,false);
			debugTargetDomain = job.getLong(DEBUG_DOMAIN_HASH, -1);
			debugTargetHash = job.getLong(DEBUG_URL_HASH, -1);
			
			
			try {
        _fs = FileSystem.get(job);
      } catch (IOException e1) {
        LOG.error(CCStringUtils.stringifyException(e1));       
      }			
    }

		@Override
    public void close() throws IOException {
	    // TODO Auto-generated method stub
	    
    } 
		
	}
	
	
	public static class ComplexKeyComparator implements RawKeyValueComparator<FlexBuffer,URLFPV2> {

		DataInputBuffer _key1Buffer = new DataInputBuffer();
		DataInputBuffer _key2Buffer = new DataInputBuffer();
		@Override
    public int compareRaw(byte[] key1Data, int key1Offset,
        int key1Length, byte[] key2Data, int key2Offset,
        int key2Length, byte[] value1Data, int value1Offset,
        int value1Length, byte[] value2Data, int value2Offset,
        int value2Length) throws IOException {
      
			_key1Buffer.reset(key1Data,key1Offset,key1Length);
			_key2Buffer.reset(key2Data,key2Offset,key2Length);
			
			// skip flex buffer size variable 
			WritableUtils.readVInt(_key1Buffer);
			WritableUtils.readVInt(_key2Buffer);
			
			// punt to raw stream comparator 
			return _compareRaw();
    }
		
		final int _compareRaw()throws IOException {
			
			long domain1Hash = _key1Buffer.readLong(); // target root domain
			long domain2Hash = _key2Buffer.readLong(); // target root domain
			if (domain1Hash == domain2Hash) { 
				float pageRank1 = _key1Buffer.readFloat();
				float pageRank2 = _key2Buffer.readFloat();
				
				if (pageRank1 == pageRank2) {
					
					// ok compare by url  
					int urlLen1 = WritableUtils.readVInt(_key1Buffer);
					int urlLen2 = WritableUtils.readVInt(_key2Buffer);
					// compare url bytes ... 
					return WritableComparator.compareBytes(_key1Buffer.getData(),_key1Buffer.getPosition(),urlLen1,_key2Buffer.getData(),_key2Buffer.getPosition(),urlLen2);
				}
				else { 
					return (pageRank1 < pageRank2) ? 1 : -1;									
				}
			}
			else { 
				return (domain1Hash < domain2Hash) ? -1 : 1;
			}
		}

		@Override
    public int compare(FlexBuffer key1, URLFPV2 value1,FlexBuffer key2, URLFPV2 value2) {
			_key1Buffer.reset(key1.get(),key1.getOffset(),key1.getCount());
			_key2Buffer.reset(key2.get(),key2.getOffset(),key2.getCount());

			try { 
				return _compareRaw();
			}
			catch (IOException e) { 
				LOG.fatal(CCStringUtils.stringifyException(e));
				throw new RuntimeException(e);
			}
    }
		
		
		static FlexBuffer genTestKey(long targetDomainFP,float pageRank,long sourceDomainFP)throws IOException {
			DataOutputBuffer outputBuffer = new DataOutputBuffer();
			outputBuffer.writeLong(targetDomainFP);
			outputBuffer.writeFloat(pageRank);
			outputBuffer.writeLong(sourceDomainFP);
			
			return new FlexBuffer(outputBuffer.getData(),0,outputBuffer.getLength());
		}
		
		@Test
		public void testComparator()throws IOException { 
			
			FlexBuffer srcArray[] = new FlexBuffer[5];
			
			srcArray[0] = genTestKey(1L, 1.0f, 1L);
			srcArray[1] = genTestKey(1L, 1.0f, 2L);
			srcArray[2] = genTestKey(1L, .5f, 2L);
			srcArray[3] = genTestKey(2L, 1.0f, 1L);
			srcArray[4] = genTestKey(2L, 1.0f, 2L);

			
			
			FlexBuffer destArray[] = new FlexBuffer[srcArray.length];
			for (int i=1;i<=srcArray.length;++i) { 
				destArray[i-1] = srcArray[srcArray.length - i];
			}
			
			DataOutputBuffer srcKeyOutBuffer[] = new DataOutputBuffer[srcArray.length];
			DataOutputBuffer destKeyOutBuffer[] = new DataOutputBuffer[srcArray.length];

			
			for (int i=0;i<srcKeyOutBuffer.length;++i) { 
				srcKeyOutBuffer[i] = new DataOutputBuffer();
				srcArray[i].write(srcKeyOutBuffer[i]);
			}
			for (int i=1;i<=srcKeyOutBuffer.length;++i) { 
				destKeyOutBuffer[i-1] = srcKeyOutBuffer[srcKeyOutBuffer.length - i];
			}
			
			Arrays.sort(destArray, new Comparator<FlexBuffer>() {

				@Override
        public int compare(FlexBuffer o1, FlexBuffer o2) {
	        return ComplexKeyComparator.this.compare(o1, null, o2, null);
	      } 
				
			});
			
			for (int i=0;i<srcArray.length;++i) { 
				Assert.assertTrue(srcArray[i] == destArray[i]);
			}
			
			Arrays.sort(destKeyOutBuffer, new Comparator<DataOutputBuffer>() {

				@Override
        public int compare(DataOutputBuffer o1, DataOutputBuffer o2) {
	        try {
	          return ComplexKeyComparator.this.compareRaw(o1.getData(), 0, o1.getLength(), o2.getData(), 0, o2.getLength(), null, 0, 0, null,0,0);
          } catch (IOException e) {
	          e.printStackTrace();
	          return 0;
          }
	      } 
				
			});
			
			
			for (int i=0;i<srcKeyOutBuffer.length;++i) { 
				Assert.assertTrue(srcKeyOutBuffer[i] == destKeyOutBuffer[i]);
			}
			
			
		}
		
	}

	
	public static class LinkDataResorter implements Reducer<IntWritable,Text,LongWritable,IntWritable> {

		int _partNumber;
		
		@Override
    public void configure(JobConf job) {
			_conf = job;
			_partNumber = job.getInt("mapred.task.partition", 0);

			try {
        _fs = FileSystem.get(job);
      } catch (IOException e1) {
        LOG.error(CCStringUtils.stringifyException(e1));       
      }			
    }

		@Override
    public void close() throws IOException {
	    // TODO Auto-generated method stub
	    
    }

    
		@Override
    public void reduce(IntWritable key, Iterator<Text> values,final OutputCollector<LongWritable,IntWritable> output, Reporter reporter)throws IOException {
			// extract the singe path that we expect 
			Path phase1DataPath = new Path(values.next().toString());
			
			Vector<Path> inputSegments = new Vector<Path>();
			inputSegments.add(phase1DataPath);

			final Path outputPath = new Path(FileOutputFormat.getWorkOutputPath(_conf),"data-" + NUMBER_FORMAT.format(_partNumber));

			LOG.info("Work Output Path is:" + outputPath);
			// create a local directory allocator 
			LocalDirAllocator lDirAlloc = new LocalDirAllocator("mapred.local.dir");
			LOG.info("Creating Local Alloc Directory");
			// create a temp dir name 
			String spillOutDirName = "r-" + NUMBER_FORMAT.format(_partNumber) + "-spillOut-" + System.currentTimeMillis();
			// and create a temp file path 
			Path localSpillOutputPath = lDirAlloc.getLocalPathForWrite(spillOutDirName, _fs.getFileStatus(phase1DataPath).getLen(),_conf);
			LOG.info("Created Local Output Directory:" + localSpillOutputPath);

			try { 
				
				// create a proxy spill writer and wrap the actual spill writer inside of it ... 
				RawDataSpillWriter<FlexBuffer,URLFPV2> proxySpillWriter = new RawDataSpillWriter<FlexBuffer,URLFPV2>() {

					// create the actual spill writer ...  
					SequenceFileSpillWriter<FlexBuffer,URLFPV2> spillwriter 
						= new SequenceFileSpillWriter<FlexBuffer,URLFPV2>(
								_fs,_conf,outputPath,FlexBuffer.class,URLFPV2.class,
								new PositionBasedSequenceFileIndex.PositionBasedIndexWriter(_fs,PositionBasedSequenceFileIndex.getIndexNameFromBaseName(outputPath)),true);
					
					
					DataInputBuffer streamReader = new DataInputBuffer();
					long lastDomainId = 0;
					boolean lastDomainIdIsValid = false;
					int  spilledItemCount = 0;
					
					@Override
          public void spillRawRecord(byte[] keyData, int keyOffset,int keyLength, byte[] valueData, int valueOffset, int valueLength)throws IOException {
						
						//initialize reader ... 
						streamReader.reset(keyData,keyOffset,keyLength);
						// skip flex buffer size variable 
						WritableUtils.readVInt(streamReader);
						// now extract target root domain id ... 
						long rootDomainId = streamReader.readLong();
						// record spill event ... 
						recordSpillAndPotentiallyAddIndexItem(rootDomainId);
						// and spill to delegate 
						spillwriter.spillRawRecord(keyData, keyOffset, keyLength, valueData, valueOffset, valueLength);
          }

					@Override
          public void close() throws IOException {
						// close the encapsulated spill writer 
						spillwriter.close();
					}

					@Override
          public void spillRecord(FlexBuffer key, URLFPV2 value)throws IOException {
						// init reader ... 
						streamReader.reset(key.get(),key.getOffset(),key.getCount());
						// record spill event 
						recordSpillAndPotentiallyAddIndexItem(streamReader.readLong());
						// and spill to delegate ... 
						spillwriter.spillRecord(key, value);
					}
					
					final void recordSpillAndPotentiallyAddIndexItem(long domainId)throws IOException { 
						// only record domain id transitions ... 
						if (!lastDomainIdIsValid || lastDomainId != domainId) { 
							// spill and index point ... 
							output.collect(new LongWritable(domainId), new IntWritable(spilledItemCount));
							// update last domain id ... 
							lastDomainId = domainId;
							lastDomainIdIsValid = true;
						}
						// always increment spill item count .. .
						spilledItemCount++;
					}
					
				};
				
				
				try { 
					
					// 
					MergeSortSpillWriter<FlexBuffer,URLFPV2> merger 
						= new MergeSortSpillWriter<FlexBuffer,URLFPV2>(
								
								_conf,
								proxySpillWriter,
								FileSystem.getLocal(_conf),
								localSpillOutputPath,
								null,
								new ComplexKeyComparator(),
								FlexBuffer.class,
								URLFPV2.class,true,reporter);
					try {
						// read input and spill into merge writer 
						FlexBuffer inputKey = new FlexBuffer();
						URLFPV2  inputValue = new URLFPV2();
						
						LOG.info("Opening Reader on Phase1Output:"+ phase1DataPath);
						SequenceFile.Reader reader = new SequenceFile.Reader(_fs, phase1DataPath,_conf);
						LOG.info("Opened Reader on Phase1Output:"+ phase1DataPath);
						
						LOG.info("Starting Spill");
						int itemCount = 0;
						while (reader.next(inputKey, inputValue)) {
							if (inputKey.getCount() >= 10000) { 
								LOG.error("Hit Too Large URL KEY Length:" + inputKey.getCount());
								throw new IOException("Bad Value Lenght:" + inputKey.getCount());
							}
							merger.spillRecord(inputKey, inputValue);
							if (++itemCount % 100000 == 0) { 
								LOG.info("Spilled:" + itemCount + " Records");
								reporter.progress();
							}
						}
						LOG.info("Done Spilling Records");
					}
					catch (Exception e) { 
						LOG.error(CCStringUtils.stringifyException(e));
						throw new IOException(e);
					}
					finally {
						LOG.info("Flushing MergedOutput");
						merger.close();
						LOG.info("Flushed MergedOutput");
					}
				}
				finally {
					proxySpillWriter.close();
				}
			}
			catch (IOException e) { 
				LOG.error(CCStringUtils.stringifyException(e));
				// delete output file 
				_fs.delete(outputPath);
				// and delete local output dir 
				FileUtils.recursivelyDeleteFile(new File(localSpillOutputPath.toString()));
				throw e;
			}
		}
		
    FileSystem _fs;
    JobConf _conf;

	}
	
	
	void runPhase1(Path linkDBPath,Path crawlDBPath,Path phase1DataPath) throws IOException { 
		
		final FileSystem fs = FileSystem.get(CrawlEnvironment.getHadoopConfig());

		try { 
	 
			LOG.info("Phase1 LinkDBPath:" + linkDBPath);
			LOG.info("Phase1 CrawlDBPath:" + crawlDBPath);
			LOG.info("Phase1 DataOutputPath:" + phase1DataPath);
			
	    JobConf job = new JobConf(CrawlEnvironment.getHadoopConfig());
	    
	    job.setJobName("InverseLinkDB By Domain - Phase 1");
	    
	    // add link db and page rank db to input 
	  	job.addInputPath(linkDBPath);
	  	job.addInputPath(crawlDBPath);
	
	    job.setInputFormat(MultiFileMergeInputFormat.class);
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setMapperClass(IdentityMapper.class);
			job.setReducerClass(LinkDataInverter.class);
	    job.setOutputFormat(SequenceFileOutputFormat.class);
	    job.setOutputKeyClass(FlexBuffer.class);
	    job.setOutputValueClass(URLFPV2.class);
	    job.setPartitionerClass(MultiFileMergePartitioner.class);
	    job.setOutputPath(phase1DataPath);
	    job.setNumReduceTasks(CrawlEnvironment.NUM_DB_SHARDS);
	    job.setNumTasksToExecutePerJvm(1000);
	    
	    LinkDataInverter.initializeDistributedCache(job);
	    
	    String affinityMask = NodeAffinityMaskBuilder.buildNodeAffinityMask(FileSystem.get(job), linkDBPath,null);
	    NodeAffinityMaskBuilder.setNodeAffinityMask(job, affinityMask);
	    
	    LOG.info("Running " + job.getJobName() + " OutputDir:"+ phase1DataPath);
	    JobClient.runJob(job);
		}
		catch (IOException e) { 
			LOG.error(CCStringUtils.stringifyException(e));
			fs.delete(phase1DataPath);
		}
	}
	
	void runPhase2(Path phase1DataPath,Path phase2DataPath)throws IOException { 
		
		final FileSystem fs = FileSystem.get(CrawlEnvironment.getHadoopConfig());

		try { 
	 
			LOG.info("Phase1 InputPath:" + phase1DataPath);
			LOG.info("Phase2 OutputPath:" + phase2DataPath);
			
	    JobConf job = new JobConf(CrawlEnvironment.getHadoopConfig());
	    
	    job.setJobName("InverseLinkDB By Domain - Phase 2");
	    
	    // add link db and page rank db to input 
	  	job.addInputPath(phase1DataPath);
	
	    job.setInputFormat(MultiFileMergeInputFormat.class);
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setMapperClass(IdentityMapper.class);
	    job.setReducerClass(LinkDataResorter.class);
	    job.setOutputFormat(SequenceFileOutputFormat.class);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setPartitionerClass(MultiFileMergePartitioner.class);
	    job.setOutputPath(phase2DataPath);
	    job.setNumReduceTasks(CrawlEnvironment.NUM_DB_SHARDS);
	    job.setNumTasksToExecutePerJvm(1000);
	    job.setInt("mapred.task.timeout", Integer.MAX_VALUE);
	    
	    LinkDataInverter.initializeDistributedCache(job);
	    
	    String affinityMask = NodeAffinityMaskBuilder.buildNodeAffinityMask(FileSystem.get(job), phase1DataPath,null);
	    NodeAffinityMaskBuilder.setNodeAffinityMask(job, affinityMask);
	    
	    LOG.info("Running " + job.getJobName() + " OutputDir:"+ phase1DataPath);
	    JobClient.runJob(job);
		}
		catch (IOException e) { 
			LOG.error(CCStringUtils.stringifyException(e));
			fs.delete(phase2DataPath);
		}
		
	}

	public static class AddShardIndexMapper implements Mapper<LongWritable,IntWritable,LongWritable,FlexBuffer>{

		DataOutputBuffer outputStream = new DataOutputBuffer();
		FlexBuffer outputBuffer = new FlexBuffer();
		
		int _partitionNumber;
		
		@Override
    public void map(LongWritable key, IntWritable value,OutputCollector<LongWritable, FlexBuffer> output, Reporter reporter)throws IOException {
			outputStream.reset();
			// write out partition information 
			outputStream.writeInt(_partitionNumber);
			// write out index offset ... 
			outputStream.writeInt(value.get());
			// write it to the buffer 
			outputBuffer.set(outputStream.getData(), 0, outputStream.getLength());
			// flush it ... 
			output.collect(key, outputBuffer);
    }

		
		@Override
    public void configure(JobConf job) {
			// extract partition number from file name ... 
			Path inputFile = new Path(job.get("map.input.file"));
			try {
	      _partitionNumber = NUMBER_FORMAT.parse(inputFile.getName().substring(PART_PREFIX.length())).intValue();
      } catch (ParseException e) {
      	throw new RuntimeException(e);
      }

    }

		@Override
    public void close() throws IOException {
	    
    } 
		
	}
	
	public static class TFileIndexWriter implements Reducer<LongWritable,FlexBuffer,NullWritable,NullWritable> {

		JobConf _conf;
		FileSystem _fs;
		int partNumber;
		TFile.Writer _tfileIndexWriter = null;
		FSDataOutputStream _tfileOutputStream = null;		
		DataOutputBuffer _tfileKeyStream = new DataOutputBuffer();
		DataOutputBuffer _tfileValueStream = new DataOutputBuffer();
		DataInputBuffer  _inputStreamReader = new DataInputBuffer();
		
		
		@Override
    public void reduce(LongWritable key, Iterator<FlexBuffer> values,OutputCollector<NullWritable, NullWritable> output, Reporter reporter) throws IOException {
			_tfileValueStream.reset();
			_tfileKeyStream.reset();
			_tfileKeyStream.writeLong(key.get());
			
			
			TreeMap<Integer,Integer> partitionToPositionMap = new TreeMap<Integer,Integer>();
			while (values.hasNext()) { 
				FlexBuffer nextBuffer = values.next();
				// initialize reader  
				_inputStreamReader.reset(nextBuffer.get(),0,nextBuffer.getCount());
				// ok write directly to key stream ...
				partitionToPositionMap.put(_inputStreamReader.readInt(), _inputStreamReader.readInt());
			}
			
			// now write it back in proper sorted order ... 
			for (Map.Entry<Integer,Integer> entry : partitionToPositionMap.entrySet()) { 
				_tfileValueStream.writeInt(entry.getKey());
				_tfileValueStream.writeInt(entry.getValue());
			}
			// append 
			_tfileIndexWriter.append(_tfileKeyStream.getData(),0, _tfileKeyStream.getLength(),_tfileValueStream.getData(),0,_tfileValueStream.getLength());
    }

		@Override
    public void configure(JobConf job) {
	    _conf = job;
	    try {
	      _fs   = FileSystem.get(_conf);
	      
	      partNumber = job.getInt("mapred.task.partition", 0);
	      // get the task's temporary file directory ...
	      Path taskOutputPath = FileOutputFormat.getWorkOutputPath(job);
	      // and create the appropriate path ... 
	      Path indexPath = new Path(taskOutputPath,PART_PREFIX + NUMBER_FORMAT.format(partNumber));
	      // and create the writer  ...
	      try {
	      	// create the index data stream ... 
		      _tfileOutputStream = _fs.create(indexPath);
		      _tfileIndexWriter = new TFile.Writer(_tfileOutputStream,64 * 1024,TFile.COMPRESSION_LZO,TFile.COMPARATOR_JCLASS + LongWritableComparator.class.getName(), _conf);
		      
	      } catch (IOException e) {
	        LOG.error(CCStringUtils.stringifyException(e));
	      }
	      
      } catch (IOException e) {
	      LOG.error(CCStringUtils.stringifyException(e));
      }
			
    }

		@Override
    public void close() throws IOException {
			_tfileIndexWriter.close();
			_tfileOutputStream.flush();
			_tfileOutputStream.close();
			
    } 
		
	}
	
	
	void runPhase3(Path phase2DataPath,Path phase3DataPath)throws IOException { 
		
		final FileSystem fs = FileSystem.get(CrawlEnvironment.getHadoopConfig());

		try { 
	 
			LOG.info("Phase2 InputPath:" + phase2DataPath);
			LOG.info("Phase3 OutputPath:" + phase3DataPath);
			
	    JobConf job = new JobConf(CrawlEnvironment.getHadoopConfig());
	    
	    job.setJobName("InverseLinkDB By Domain - Phase 3");
	    
	    ////////////////////////
	    // add only part files from phase2 data path ... 
	    FileStatus parts[] = fs.globStatus(new Path(phase2DataPath,"part-*"));
	    for (FileStatus candidate : parts) { 
	    	job.addInputPath(candidate.getPath());
	    }
	    ////////////////////////
	
	    job.setInputFormat(SequenceFileInputFormat.class);
	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(FlexBuffer.class);
	    job.setMapperClass(AddShardIndexMapper.class);
	    job.setReducerClass(TFileIndexWriter.class);
	    job.setOutputFormat(NullOutputFormat.class);
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(NullWritable.class);
	    job.setOutputPath(phase3DataPath);
	    job.setNumReduceTasks(CrawlEnvironment.NUM_DB_SHARDS);
	    job.setNumTasksToExecutePerJvm(1000);
	    job.setInt("mapred.task.timeout", Integer.MAX_VALUE);
	    	    
	    LOG.info("Running " + job.getJobName() + " OutputDir:"+ phase3DataPath);
	    JobClient.runJob(job);
		}
		catch (IOException e) { 
			LOG.error(CCStringUtils.stringifyException(e));
			fs.delete(phase3DataPath);
		}
		
	}

	void runDebugJob(Path phase1DataPath,Path debugDataPath) throws IOException { 
		final FileSystem fs = FileSystem.get(CrawlEnvironment.getHadoopConfig());

		try { 
	 
			LOG.info("Phase1 InputPath:" + phase1DataPath);
			LOG.info("Debug OutputPath:" + debugDataPath);
			
	    JobConf job = new JobConf(CrawlEnvironment.getHadoopConfig());
	    
	    job.setJobName("Debug Job");
	    
	    ////////////////////////
	    URLFPV2 fp = URLUtils.getURLFPV2FromURL("http://www.factual.com/");
	    
	    job.setLong("TargetFP",fp.getRootDomainHash());
	    job.addInputPath(phase1DataPath);
	    job.setInputFormat(SequenceFileInputFormat.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(NullWritable.class);
	    job.setMapperClass(DebugMapper.class);
	    job.setReducerClass(IdentityReducer.class);
	    job.setOutputFormat(TextOutputFormat.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(NullWritable.class);
	    job.setOutputPath(debugDataPath);
	    job.setNumReduceTasks(1);
	    job.setNumTasksToExecutePerJvm(1000);
	    job.setInt("mapred.task.timeout", Integer.MAX_VALUE);
	    job.setBoolean("mapred.output.compress", false);
	    
	    LOG.info("Running " + job.getJobName() + " OutputDir:"+ debugDataPath);
	    JobClient.runJob(job);
		}
		catch (IOException e) { 
			LOG.error(CCStringUtils.stringifyException(e));
			fs.delete(debugDataPath);
		}		
	}
	
	public static class DebugMapper implements Mapper<FlexBuffer,TextBytes,Text,NullWritable> {

		DataInputBuffer inputStream = new DataInputBuffer();
		long targetFP;
		@Override
    public void map(FlexBuffer key, TextBytes value,OutputCollector<Text, NullWritable> output, Reporter reporter) throws IOException {
			if (targetFP == -1) { 
				throw new IOException("Initialization Failure! No TargetFP!");
			}
			inputStream.reset(key.get(),0,key.getCount());
			if (inputStream.readLong() == targetFP) { 
				output.collect(new Text(value.toString()), NullWritable.get());
			}
    }

		@Override
    public void configure(JobConf job) {
			targetFP = job.getLong("TargetFP",-1);
    }

		@Override
    public void close() throws IOException {
	    // TODO Auto-generated method stub
	    
    } 
	}
	
	void runDebugPhase1(Path linkDBPath,Path crawlDBPath,Path phase1DebugDataPath) throws IOException { 
		
		final FileSystem fs = FileSystem.get(CrawlEnvironment.getHadoopConfig());

		try { 
	 
			LOG.info("Phase1 LinkDBPath:" + linkDBPath);
			LOG.info("Phase1 CrawlDBPath:" + crawlDBPath);
			LOG.info("Phase1 DataOutputPath:" + phase1DebugDataPath);
			
	    JobConf job = new JobConf(CrawlEnvironment.getHadoopConfig());
	    
	    
	    URLFPV2 urlfp = URLUtils.getURLFPV2FromURL("http://gold.rightwhereyouwork.com/index.php");
	    
			job.setBoolean(LinkDataInverter.DEBUG_MODE,true);
			job.setLong(LinkDataInverter.DEBUG_DOMAIN_HASH,urlfp.getDomainHash());
			job.setLong(LinkDataInverter.DEBUG_URL_HASH,urlfp.getUrlHash());
	    
	    
	    job.setJobName("InverseLinkDB By Domain - Phase 1 Debug Job");
	    
	    
	    // add link db and page rank db to input 
	  	job.addInputPath(linkDBPath);
	  	job.addInputPath(crawlDBPath);
	
	    job.setInputFormat(MultiFileMergeInputFormat.class);
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setMapperClass(IdentityMapper.class);
			job.setReducerClass(LinkDataInverter.class);
	    job.setOutputFormat(TextOutputFormat.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(NullWritable.class);
	    job.setPartitionerClass(MultiFileMergePartitioner.class);
	    job.setOutputPath(phase1DebugDataPath);
	    job.setNumReduceTasks(CrawlEnvironment.NUM_DB_SHARDS);
	    job.setNumTasksToExecutePerJvm(1000);
	    job.setBoolean("mapred.output.compress", false);

	    
	    LinkDataInverter.initializeDistributedCache(job);
	    
	    String affinityMask = NodeAffinityMaskBuilder.buildNodeAffinityMask(FileSystem.get(job), linkDBPath,null);
	    NodeAffinityMaskBuilder.setNodeAffinityMask(job, affinityMask);
	    
	    LOG.info("Running " + job.getJobName() + " OutputDir:"+ phase1DebugDataPath);
	    JobClient.runJob(job);
		}
		catch (IOException e) { 
			LOG.error(CCStringUtils.stringifyException(e));
			fs.delete(phase1DebugDataPath);
		}
	}	
}
