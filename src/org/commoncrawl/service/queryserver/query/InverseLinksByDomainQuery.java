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

package org.commoncrawl.service.queryserver.query;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.file.tfile.TFile;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.mapred.pipelineV1.InverseLinksByDomainDBBuilder.ComplexKeyComparator;
import org.commoncrawl.hadoop.mergeutils.MergeSortSpillWriter;
import org.commoncrawl.hadoop.mergeutils.SequenceFileSpillWriter;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.service.queryserver.InlinksByDomainQueryInfo;
import org.commoncrawl.service.queryserver.ShardIndexHostNameTuple;
import org.commoncrawl.service.queryserver.index.PositionBasedSequenceFileIndex;
import org.commoncrawl.service.queryserver.index.DatabaseIndexV2.MasterDatabaseIndex;
import org.commoncrawl.service.queryserver.index.DatabaseIndexV2.SlaveDatabaseIndex;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.FileUtils;
import org.commoncrawl.util.FlexBuffer;
import org.commoncrawl.util.TextBytes;
import org.commoncrawl.util.URLUtils;

/**
 * 
 * @author rana
 *
 */
public class InverseLinksByDomainQuery extends Query<InlinksByDomainQueryInfo,FlexBuffer,URLFPV2>{

	private static final Log LOG = LogFactory.getLog(InverseLinksByDomainQuery.class);
	private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }		
	
  public InverseLinksByDomainQuery() { 
    
  }
  
  public InverseLinksByDomainQuery(InlinksByDomainQueryInfo queryInfo) { 
    setQueryData(queryInfo);
  }
  
  
  static Map<Integer,PositionBasedSequenceFileIndex> _shardToIndexMap = new TreeMap<Integer,PositionBasedSequenceFileIndex>();
	static void collectAllTopLevelDomainRecordsByDomain(FileSystem fs,Configuration conf,long databaseId,long targetRootDomainFP,FileSystem outputFileSystem,Path finalOutputPath) throws IOException {

		File tempFile = new File("/tmp/inverseLinksReport-" + System.currentTimeMillis());
		tempFile.mkdir();

		try { 
			// create the final output spill writer ...  
			SequenceFileSpillWriter<FlexBuffer,URLFPV2> spillwriter 
				= new SequenceFileSpillWriter<FlexBuffer,URLFPV2>(
						outputFileSystem,conf,finalOutputPath,FlexBuffer.class,URLFPV2.class,
						new PositionBasedSequenceFileIndex.PositionBasedIndexWriter(outputFileSystem,PositionBasedSequenceFileIndex.getIndexNameFromBaseName(finalOutputPath))
						,true);
			
			
			try {
				
				MergeSortSpillWriter<FlexBuffer,URLFPV2> finalMerger 
				= new MergeSortSpillWriter<FlexBuffer,URLFPV2>(
						conf,
						spillwriter,
						FileSystem.getLocal(conf),						
						new Path(tempFile.getAbsolutePath()),
						null,
						new ComplexKeyComparator(),
						FlexBuffer.class,
						URLFPV2.class,true,null);
			
				try { 
	
					for (int targetShardId=0;targetShardId<CrawlEnvironment.NUM_DB_SHARDS;++targetShardId) {
						// 0. shard domain id to find index file location ... 
						int indexShardId = (int) ((targetRootDomainFP & Integer.MAX_VALUE) % CrawlEnvironment.NUM_DB_SHARDS);
						// build path to index file 
						Path indexFilePath = new Path("crawl/inverseLinkDB_ByDomain/" + databaseId + "/phase3Data/part-" + NUMBER_FORMAT.format(indexShardId));
						LOG.info("rootDomain is:" + targetRootDomainFP + " ShardId:" + indexShardId + " Index Path:" + indexFilePath);
						// 1. scan domainFP to index file first
						// 2. given index, scan index->pos file to find scan start position
						// 3. given scan start position, scan forward until fp match is found.
						// 4. collect all matching entries and output to a file ? 
					
								FSDataInputStream indexDataInputStream = fs.open(indexFilePath);
								try { 
									TFile.Reader reader = new TFile.Reader(indexDataInputStream,fs.getFileStatus(indexFilePath).getLen(),conf);
									try { 
										TFile.Reader.Scanner scanner = reader.createScanner();
										
										try { 
											// generate key ... 
											DataOutputBuffer keyBuffer = new DataOutputBuffer();
											keyBuffer.writeLong(targetRootDomainFP);
											if (scanner.seekTo(keyBuffer.getData(),0,keyBuffer.getLength())) {
												// setup for value scan 
												DataInputStream valueStream =  scanner.entry().getValueStream();
												int dataOffsetOut = -1;
												while (valueStream.available() >0) { 
													// read entries looking for our specific entry
													int shardIdx = valueStream.readInt();
													int dataOffset = valueStream.readInt();
													if (shardIdx == targetShardId) { 
														dataOffsetOut = dataOffset;
														break;
													}
												}
												LOG.info("Index Search Yielded:"+ dataOffsetOut);
												if (dataOffsetOut != -1) { 
													// ok create a data path 
													Path finalDataPath = new Path("crawl/inverseLinkDB_ByDomain/" + databaseId + "/phase2Data/data-" + NUMBER_FORMAT.format(targetShardId));
													Path finalDataIndexPath = new Path("crawl/inverseLinkDB_ByDomain/" + databaseId + "/phase2Data/data-" + NUMBER_FORMAT.format(targetShardId) + ".index");
													// check to see if index is already loaded ... 
													PositionBasedSequenceFileIndex<FlexBuffer, TextBytes> index = null;
													synchronized(_shardToIndexMap) { 
														index = _shardToIndexMap.get(targetShardId);
													}
													if (index == null) {
														LOG.info("Loading Index from Path:" + finalDataIndexPath);
														// load index
														index = new PositionBasedSequenceFileIndex<FlexBuffer, TextBytes>(fs, finalDataIndexPath, FlexBuffer.class, TextBytes.class);
														// put in cache
														synchronized (_shardToIndexMap) {
							                _shardToIndexMap.put(targetShardId, index);
						                }
													}
													
													LOG.info("Initializing Data Reader at Path:" + finalDataPath);
													// ok time to create a reader 
													SequenceFile.Reader dataReader = new SequenceFile.Reader(fs,finalDataPath,conf);
													
													try { 
														LOG.info("Seeking Reader to Index Position:" + dataOffsetOut);
														index.seekReaderToItemAtIndex(dataReader,dataOffsetOut);
														
														FlexBuffer keyBytes 	= new FlexBuffer();
														URLFPV2  	 sourceFP   = new URLFPV2();
														DataInputBuffer keyReader = new DataInputBuffer();
														TextBytes  urlTxt  = new TextBytes();
														
														// ok read to go ... 
														while (dataReader.next(keyBytes,sourceFP)) { 
															// initialize reader 
															keyReader.reset(keyBytes.get(),keyBytes.getOffset(),keyBytes.getCount());
															
															long targetFP = keyReader.readLong();
															
															
															if (targetRootDomainFP == targetFP) { 
																finalMerger.spillRecord(keyBytes, sourceFP);
															}
															else { 
																LOG.info("FP:"+ targetFP + " > TargetFP:" + targetRootDomainFP + " Exiting Iteration Loop");
																break;
															}
														}
													}
													finally { 
														LOG.info("Closing Reader");
														dataReader.close();
													}
												}
											}
										}
										finally {
											LOG.info("Closing Scanner");
											scanner.close();
										}
											
									}
									finally { 
										LOG.info("Closing TFile Reader");
										reader.close();
									}
								}
								finally { 
									LOG.info("Closing InputStream");
									indexDataInputStream.close();
								}
							}
					}
					finally { 
						finalMerger.close();
					}
			}
			finally { 
				spillwriter.close();
			}
		}
		catch (IOException e) { 
			LOG.error(CCStringUtils.stringifyException(e));
			FileUtils.recursivelyDeleteFile(tempFile);
		}
		
	}
	
	public static void main(String[] args) {
    // initialize ...
    Configuration conf = new Configuration();
    
    conf.addResource("nutch-default.xml");
    conf.addResource("nutch-site.xml");
    conf.addResource("core-site.xml");
    conf.addResource("hdfs-site.xml");
    conf.addResource("mapred-site.xml");
    
    LOG.info("URL:" + args[0] + " ShardId:" + args[1]);

    try { 
	    File tempFile = File.createTempFile("inverseLinksReportTest", "seq");
	    try { 
	    	FileSystem fs = FileSystem.get(conf);
	    	FileSystem localFileSystem = FileSystem.getLocal(conf);
	    	
	    	URLFPV2 fp = URLUtils.getURLFPV2FromURL(args[0]);
	    	if (fp != null) { 
	  			collectAllTopLevelDomainRecordsByDomain(fs,conf,1282844121161L,fp.getRootDomainHash(),localFileSystem,new Path(tempFile.getAbsolutePath()));
	  			
	  			SequenceFile.Reader reader = new SequenceFile.Reader(localFileSystem,new Path(tempFile.getAbsolutePath()) , conf);
	  			try { 
	  				FlexBuffer key = new FlexBuffer();
	  				URLFPV2 	 src = new URLFPV2();
	  				TextBytes  url = new TextBytes();
	  				
	  				DataInputBuffer inputBuffer = new DataInputBuffer();
	  				
	  				while (reader.next(key, src)) { 
	  					inputBuffer.reset(key.get(),key.getOffset(),key.getCount());
	  					long targetFP = inputBuffer.readLong();
	  					float pageRank = inputBuffer.readFloat();
	  					// ok initialize text bytes ... 
	  					int textLen = WritableUtils.readVInt(inputBuffer);
	  					url.set(key.get(), inputBuffer.getPosition(), textLen);
	  					LOG.info("PR:"+ pageRank + " URL:" + url.toString());
	  				}
	  			}
	  			finally { 
	  				reader.close();
	  			}
	    	}
	    }
	    catch (IOException e) { 
	    	LOG.error(CCStringUtils.stringifyException(e));
	    	// tempFile.delete();
	    }
    }
    catch (IOException e) { 
    	LOG.error(CCStringUtils.stringifyException(e));
    }
  }

	@Override
  public boolean cachedResultsAvailable(
      FileSystem fileSystem,
      Configuration conf,
      QueryRequest<InlinksByDomainQueryInfo, FlexBuffer,URLFPV2> theClientRequest)
      throws IOException {

		FileSystem localFileSystem = FileSystem.getLocal(conf);
    Path urlOutputFileName = new Path(getLocalQueryResultsPathPrefix(theClientRequest) + "DATA");
    
    LOG.info("Cached Results Available called for Query:" + theClientRequest.getSourceQuery().getQueryId() + ". Checking Path:" +  urlOutputFileName);
    return localFileSystem.exists(urlOutputFileName);
		
  }

	@Override
  protected long executeRemote(
      FileSystem fileSyste,
      Configuration conf,
      EventLoop eventLoop,
      SlaveDatabaseIndex instanceIndex,
      File tempFirDir,
      QueryProgressCallback<InlinksByDomainQueryInfo, FlexBuffer,URLFPV2> progressCallback)
      throws IOException {
	  // TODO Auto-generated method stub
	  return 0;
  }

	@Override
  public void getCachedResults(
      FileSystem fileSystem,
      Configuration conf,
      EventLoop eventLoop,
      MasterDatabaseIndex masterIndex,
      QueryRequest<InlinksByDomainQueryInfo, FlexBuffer,URLFPV2> theClientRequest,
      QueryCompletionCallback<InlinksByDomainQueryInfo, FlexBuffer,URLFPV2> callback)
      throws IOException {
		
    FileSystem localFileSystem = FileSystem.getLocal(conf);
		
    Path outputFileName = new Path(getLocalQueryResultsPathPrefix(theClientRequest)+"DATA");
    
    //LOG.info("Initializing index reader for outputFile:" + outputFileName);
    Path indexFileName = PositionBasedSequenceFileIndex.getIndexNameFromBaseName(outputFileName);
    //LOG.info("Index FileName is:" + indexFileName);
    
    PositionBasedSequenceFileIndex<FlexBuffer,URLFPV2> index = new PositionBasedSequenceFileIndex<FlexBuffer,URLFPV2>(localFileSystem,indexFileName,FlexBuffer.class,URLFPV2.class);
    
    QueryResult<FlexBuffer,URLFPV2> resultOut = new QueryResult<FlexBuffer,URLFPV2>(); 
    
    LOG.info("getCachedResults called for Query:" + getQueryId() +" Calling ReadPaginationResults");
    index.readPaginatedResults(localFileSystem, conf,
        theClientRequest.getClientQueryInfo().getSortOrder(),
        theClientRequest.getClientQueryInfo().getPaginationOffset(),
        theClientRequest.getClientQueryInfo().getPageSize(),
        resultOut);
    
    LOG.info("getCachedResults called for Query:" + getQueryId() +". Initiating getCachedResults Callback");
    callback.queryComplete(theClientRequest,resultOut);	  
  }
	
  
	@Override
  public String getCanonicalId() {
	  return encodePatternAsFilename("ILBD:" + getQueryData().getDomainName());
  }

	@Override
  public boolean requiresRemoteDispatch(
      FileSystem fileSystem,
      Configuration conf,
      ShardMapper shardMapper,
      QueryRequest<InlinksByDomainQueryInfo, FlexBuffer,URLFPV2> theClientRequest,
      ArrayList<ShardIndexHostNameTuple> shardIdToHostNameMapping)
      throws IOException {
	  return false;
  }
	
	@Override
	protected long executeLocal(
	    FileSystem fileSystem,
	    Configuration conf,
	    MasterDatabaseIndex index,
	    EventLoop eventLoop,
	    File tempFirDir,
	    QueryRequest<InlinksByDomainQueryInfo, FlexBuffer,URLFPV2> requestObject)
	    throws IOException {
		
		
		
  	LocalFileSystem localFS = FileSystem.getLocal(conf);
  	
    Path localURLListPath = new Path(getLocalQueryResultsPathPrefix(requestObject)+"DATA");
    Path localURLListIndexPath = new Path(getLocalQueryResultsPathPrefix(requestObject)+"DATA.index");
    
    LOG.info("executeLocal called. Domain:" + getQueryData().getDomainName() + " cacheFilename:" + localURLListPath);
    
    localFS.delete(localURLListPath,false);
    localFS.delete(localURLListIndexPath,false);		
    
    String queryDomain = getQueryData().getDomainName();
    
    if (queryDomain.length() != 0) { 
    
    	String url = "http://" + queryDomain + "/";
    	
    	URLFPV2 fp = URLUtils.getURLFPV2FromURL(url);
    	
    	if (fp != null) { 
    		return index.collectAllTopLevelDomainRecordsByDomain(fileSystem, conf, fp.getRootDomainHash(), localFS, localURLListPath);
    	}
    }
		throw new IOException("Invalid Domain Name:" + queryDomain);
	}
}
