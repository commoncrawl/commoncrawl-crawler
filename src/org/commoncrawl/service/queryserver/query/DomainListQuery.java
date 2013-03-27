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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.hadoop.mergeutils.MergeSortSpillWriter;
import org.commoncrawl.hadoop.mergeutils.OptimizedKeyGeneratorAndComparator;
import org.commoncrawl.hadoop.mergeutils.RawKeyValueComparator;
import org.commoncrawl.hadoop.mergeutils.SequenceFileMerger;
import org.commoncrawl.hadoop.mergeutils.SequenceFileReader;
import org.commoncrawl.hadoop.mergeutils.SequenceFileSpillWriter;
import org.commoncrawl.protocol.SubDomainMetadata;
import org.commoncrawl.service.queryserver.DomainListQueryInfo;
import org.commoncrawl.service.queryserver.ShardIndexHostNameTuple;
import org.commoncrawl.service.queryserver.index.DatabaseIndexV2;
import org.commoncrawl.service.queryserver.index.PositionBasedSequenceFileIndex;
import org.commoncrawl.service.queryserver.index.DatabaseIndexV2.MasterDatabaseIndex;
import org.commoncrawl.service.queryserver.index.DatabaseIndexV2.SlaveDatabaseIndex;
import org.commoncrawl.util.CCStringUtils;

/**
 * 
 * @author rana
 *
 */
public class DomainListQuery extends Query<DomainListQueryInfo,Text,SubDomainMetadata> {

  private static final Log LOG = LogFactory.getLog(DomainListQuery.class);

  
  public final static String SORT_BY_NAME      = "NAME";
  public final static String SORT_BY_URL_COUNT = "URLCOUNT";
  
  private String getOutputFileNameBasedOnSortByField(String sortByField) throws IOException {
    if(sortByField.equals(SORT_BY_NAME)) { 
      return "DATA_" + SORT_BY_NAME;
    }
    else if (sortByField.equals(SORT_BY_URL_COUNT)) { 
      return "DATA_" + SORT_BY_URL_COUNT;
    }
    throw new IOException(sortByField +" is an INVALID SORT FIELD");
  }
  
  private String getMergedResultsFileName() {
    return "DATA_" + SORT_BY_NAME;
  }
  
  public DomainListQuery() { 
    
  }
  
  public DomainListQuery(DomainListQueryInfo queryInfo) { 
    setQueryData(queryInfo);
  }
  

  @Override
  public String getCanonicalId() {
    return encodePatternAsFilename("DLQ:" + Query.encodePatternAsFilename(getQueryData().getSearchPattern())); 
  }




  @Override
  protected long executeLocal(FileSystem remoteFileSystem, Configuration conf,DatabaseIndexV2.MasterDatabaseIndex index, EventLoop eventLoop,File tempFirDir,QueryRequest<DomainListQueryInfo,Text,SubDomainMetadata> requestObject) throws IOException {
    
    
    Path mergeResultsPath = new Path(getLocalQueryResultsPathPrefix(requestObject)+getMergedResultsFileName());
    
    LOG.info("Execute Local called for Query:" + getQueryId() +" MergeResultsPath is:" + mergeResultsPath);
    
    // get a local file system object
    FileSystem localFileSystem = FileSystem.getLocal(conf);
    
    //LOG.info("Executing LocalQuery - checking if MergedFile:" + mergeResultsPath + " Exists");
    // if source merged results path does not exist ... 
    if (!localFileSystem.exists(mergeResultsPath)) {
      LOG.info("Execute Local for Query:" + getQueryId() +" Source MergeFile:" + mergeResultsPath + " Not Found. Checking for parts files");
      // collect parts ...
      Vector<Path> parts = new Vector<Path>();
      
      FileStatus fileStatusArray[] = remoteFileSystem.globStatus(new Path(getHDFSQueryResultsPath(),"part-*"));
      
      if(fileStatusArray.length == 0) {
        LOG.error("Execute Local for Query:" + getQueryId() +" FAILED. No Parts Files Found!");
        throw new IOException("Remote Component Part Files Not Found");
      }

      for (FileStatus part : fileStatusArray) { 
        //LOG.info("Found Part:"+ part);
        parts.add(part.getPath());
      }
      
      LOG.info("Execute Local for Query:" + getQueryId() +" Initializing Merger");
      SequenceFileSpillWriter<Text,SubDomainMetadata> mergedFileSpillWriter 
      	= new SequenceFileSpillWriter<Text,SubDomainMetadata>(localFileSystem,conf,mergeResultsPath,Text.class,SubDomainMetadata.class,
      			new PositionBasedSequenceFileIndex.PositionBasedIndexWriter(localFileSystem,PositionBasedSequenceFileIndex.getIndexNameFromBaseName(mergeResultsPath))
      			,false);
      
      try { 
	      SequenceFileMerger<Text,SubDomainMetadata> merger 
	        = new SequenceFileMerger<Text,SubDomainMetadata>(
	            remoteFileSystem,
	            conf,
	            parts,
	            mergedFileSpillWriter,
	            Text.class,
	            SubDomainMetadata.class,
	            
	            new RawKeyValueComparator<Text,SubDomainMetadata>() {
	
	            	DataInputBuffer key1Stream = new DataInputBuffer();
	            	DataInputBuffer key2Stream = new DataInputBuffer();
	            	
								@Override
	              public int compareRaw(byte[] key1Data, int key1Offset,
	                  int key1Length, byte[] key2Data, int key2Offset,
	                  int key2Length, byte[] value1Data, int value1Offset,
	                  int value1Length, byte[] value2Data, int value2Offset,
	                  int value2Length) throws IOException {
	
									key1Stream.reset(key1Data, key1Offset, key1Length);
									key2Stream.reset(key2Data, key2Offset, key2Length);
									
									WritableUtils.readVInt(key1Stream);
									WritableUtils.readVInt(key2Stream);
																	
		              return BytesWritable.Comparator.compareBytes(key1Data, key1Stream.getPosition(), key1Length - key1Stream.getPosition(), key2Data, key2Stream.getPosition(), key2Length - key2Stream.getPosition());
	              }
	
								@Override
	              public int compare(Text key1, SubDomainMetadata value1, Text key2,SubDomainMetadata value2) {
		              return key1.compareTo(key2);
	              }
	            	
							}
	        		);
	      
	      try { 
	        LOG.info("Execute Local for Query:" + getQueryId() +" Running Merger");
	        merger.mergeAndSpill(null);
	        LOG.info("Execute Local for Query:" + getQueryId() +" Merge Successful.. Deleting Merge Inputs");
	        for (Path inputPath : parts) { 
	          remoteFileSystem.delete(inputPath,false);
	        }
	      }
	      catch (IOException e){ 
	        LOG.error("Execute Local for Query:" + getQueryId() +" Merge Failed with Exception:" + CCStringUtils.stringifyException(e));
	        throw e;
	      }
	      finally { 
	      	LOG.info("** CLOSING MERGER");
	        merger.close();
	      }
      }
      finally {
      	LOG.info("** FLUSHING SPILLWRITER");
      	mergedFileSpillWriter.close();
      }
    }
    
    // now check for query specific merge file ...
    Path queryResultsPath = new Path(getLocalQueryResultsPathPrefix(requestObject)+getOutputFileNameBasedOnSortByField(requestObject.getClientQueryInfo().getSortByField()));
    
    LOG.info("Execute Local for Query:" + getQueryId() +" Checking for QueryResultsPath:" + queryResultsPath);
    
    if (!localFileSystem.exists(queryResultsPath)) {
    
      LOG.info("Exectue Local for Query:" + getQueryId() +" Results File:" + queryResultsPath + " does not exist. Running sort and merge process");

      LOG.info("Execute Local for Query:" + getQueryId() +" Allocating SpillWriter with output to:" + queryResultsPath);
      // allocate a spill writer ...  
      SequenceFileSpillWriter<Text,SubDomainMetadata> sortedResultsFileSpillWriter = new SequenceFileSpillWriter<Text,SubDomainMetadata>(localFileSystem,conf,queryResultsPath,Text.class,SubDomainMetadata.class,
      		new PositionBasedSequenceFileIndex.PositionBasedIndexWriter(localFileSystem,PositionBasedSequenceFileIndex.getIndexNameFromBaseName(queryResultsPath)),
      		false);
      
      try { 
        
        LOG.info("Execute Local for Query:" + getQueryId() +" Allocating MergeSortSpillWriter");
        // and connect it to the merge spill writer ...
        MergeSortSpillWriter<Text, SubDomainMetadata> mergeSortSpillWriter = new MergeSortSpillWriter<Text, SubDomainMetadata>(
            conf,
            sortedResultsFileSpillWriter,
            localFileSystem,
            new Path(tempFirDir.getAbsolutePath()),
            /*
            new RawKeyValueComparator<Text,SubDomainMetadata>() {
  
              SubDomainMetadata value1 = new SubDomainMetadata();
              SubDomainMetadata value2 = new SubDomainMetadata();
              
  
              @Override
              public int compare(Text key1, SubDomainMetadata value1, Text key2,SubDomainMetadata value2) {
                return value1.getUrlCount() - value2.getUrlCount();
              }

              @Override
              public int compareRaw(byte[] key1Data, int key1Offset,
                  int key1Length, byte[] key2Data, int key2Offset,
                  int key2Length, byte[] value1Data, int value1Offset,
                  int value1Length, byte[] value2Data, int value2Offset,
                  int value2Length) throws IOException {
  
                value1.clear();
                value2.clear();
                
                value1.readFields(new DataInputStream(new ByteArrayInputStream(value1Data,value1Offset,value1Length)));
                value2.readFields(new DataInputStream(new ByteArrayInputStream(value2Data,value2Offset,value2Length)));
                
                return compare(null, value1, null, value2);
              } 
              
            },
            */
            new OptimizedKeyGeneratorAndComparator<Text, SubDomainMetadata>() {


							@Override
              public void generateOptimizedKeyForPair(
                  Text key,
                  SubDomainMetadata value,
                  org.commoncrawl.hadoop.mergeutils.OptimizedKeyGeneratorAndComparator.OptimizedKey optimizedKeyOut)
                  throws IOException {
	              optimizedKeyOut.setLongKeyValue(value.getUrlCount());
              }

							@Override
              public int getGeneratedKeyType() {
	              return OptimizedKey.KEY_TYPE_LONG;
              }
            },
            Text.class,
            SubDomainMetadata.class,false,null);
        
        try { 
          
          // create a vector representing the single input segment 
          Vector<Path> singleInputSegment = new Vector<Path>();
          
          LOG.info("Execute Local for Query:" + getQueryId() +" Adding MergeResultsPath:" + mergeResultsPath + " as input for Merger");
          singleInputSegment.add(mergeResultsPath);
          
          // create a SequenceFileReader
          SequenceFileReader<Text, SubDomainMetadata> mergeSegmentReader = new SequenceFileReader<Text, SubDomainMetadata>(
              localFileSystem,
              conf,
              singleInputSegment,
              mergeSortSpillWriter,
              Text.class,
              SubDomainMetadata.class);
              
          try { 
            LOG.info("Execute Local for Query:" + getQueryId() +" calling readAndSpill");
            mergeSegmentReader.readAndSpill();
            LOG.info("Execute Local for Query:" + getQueryId() +" readAndSpill finished");
          }
          finally { 
            if (mergeSegmentReader != null) { 
              mergeSegmentReader.close();
            }
          }
          
        }
        finally { 
          if (mergeSortSpillWriter != null) { 
            mergeSortSpillWriter.close();
          }
        }
        
      }
      finally { 
        if (sortedResultsFileSpillWriter != null) { 
          sortedResultsFileSpillWriter.close();
        }
      }
    }

    //LOG.info("Allocating SequenceFileIndex object for DomainListQuery Id:" + getQueryId() + " with Path:" + queryResultsPath);
    PositionBasedSequenceFileIndex<Text, SubDomainMetadata> indexFile = new PositionBasedSequenceFileIndex<Text, SubDomainMetadata>(localFileSystem,queryResultsPath,Text.class,SubDomainMetadata.class);
    //LOG.info("SequenceFileIndex object for DomainListQuery Id:" + getQueryId() + " with Path:" + queryResultsPath + " returned record count:" + indexFile.getRecordCount());
    
    return indexFile.getRecordCount();
  }

  
  
  @Override
  public void getCachedResults(
      FileSystem fileSystem,
      Configuration conf,
      EventLoop eventLoop,
      MasterDatabaseIndex masterIndex,
      QueryRequest<DomainListQueryInfo, Text, SubDomainMetadata> theClientRequest,
      QueryCompletionCallback<DomainListQueryInfo, Text, SubDomainMetadata> callback)
      throws IOException {
  	
    LOG.info("getCachedResults called for Query:" + getQueryId());
    /*
    LOG.info("Retrieving Cached Results for Query:" + theClientRequest.getClientQueryInfo().getClientQueryId());
    LOG.info("Sort Field:" + theClientRequest.getClientQueryInfo().getSortByField());
    LOG.info("Sort Order:" + theClientRequest.getClientQueryInfo().getSortOrder());
    LOG.info("Pagination Offset:" + theClientRequest.getClientQueryInfo().getPaginationOffset());
    LOG.info("Page Size:" + theClientRequest.getClientQueryInfo().getPageSize());
    */
    FileSystem localFileSystem = FileSystem.getLocal(conf);
        
    String sortByField = theClientRequest.getClientQueryInfo().getSortByField();
    
    if (sortByField.equalsIgnoreCase(SORT_BY_NAME) || sortByField.equalsIgnoreCase(SORT_BY_URL_COUNT)) {
      
      Path outputFileName = new Path(getLocalQueryResultsPathPrefix(theClientRequest)+getOutputFileNameBasedOnSortByField(theClientRequest.getClientQueryInfo().getSortByField()));
      
      //LOG.info("Initializing index reader for outputFile:" + outputFileName);
      Path indexFileName = PositionBasedSequenceFileIndex.getIndexNameFromBaseName(outputFileName);
      //LOG.info("Index FileName is:" + indexFileName);
      
      PositionBasedSequenceFileIndex<Text, SubDomainMetadata> index = new PositionBasedSequenceFileIndex<Text, SubDomainMetadata>(localFileSystem,indexFileName,Text.class,SubDomainMetadata.class);
      
      QueryResult<Text,SubDomainMetadata> resultOut = new QueryResult<Text,SubDomainMetadata>(); 
      
      LOG.info("getCachedResults called for Query:" + getQueryId() +" Calling ReadPaginationResults");
      index.readPaginatedResults(localFileSystem, conf,
          theClientRequest.getClientQueryInfo().getSortOrder(),
          theClientRequest.getClientQueryInfo().getPaginationOffset(),
          theClientRequest.getClientQueryInfo().getPageSize(),
          resultOut);
      
      LOG.info("getCachedResults called for Query:" + getQueryId() +". Initiating getCachedResults Callback");
      callback.queryComplete(theClientRequest,resultOut);
    }
  }

  @Override
  protected long executeRemote(
      final FileSystem fileSystem,
      final Configuration conf,
      EventLoop eventLoop,
      SlaveDatabaseIndex instanceIndex,
      File tempFirDir,
      QueryProgressCallback<DomainListQueryInfo, Text, SubDomainMetadata> progressCallback)
      throws IOException {

  		int shardsProcessed = 0;
  		
  		// ok create a semaphore for the number of shard we are going to query ...
  		final Semaphore semaphore = new Semaphore(-(getCommonQueryInfo().getRelevantShardIds().size()-1));
  		// and create a record count array 
  		final long recordCounts[] = new long[getCommonQueryInfo().getRelevantShardIds().size()];
  		final IOException exceptions[] = new IOException[getCommonQueryInfo().getRelevantShardIds().size()];

  		int threadIdx = 0;
  		// ok dispatch queries for each shard we are responsible for ... 
  		for (int shardId : getCommonQueryInfo().getRelevantShardIds()) {
  			
  			final int currentShardId = shardId;
  			final int currentThreadIdx = threadIdx++;
  			
  			Thread subQueryThread = new Thread(new Runnable() {

					@Override
          public void run() {
		  			Path shardOutputPath = getHDFSQueryResultsFilePathForShard(currentShardId);
		  			
			      LOG.info("Execute Remote for Query:" + getQueryId() +" for shardId:" + currentShardId+ "  Creating spill file @:" + shardOutputPath);
			      
			      try { 
				      // create SequenceFile Spill Writer ... 
				      SequenceFileSpillWriter<Text, SubDomainMetadata> spillWriter 
				        = new SequenceFileSpillWriter<Text, SubDomainMetadata>(fileSystem,conf,shardOutputPath,Text.class,SubDomainMetadata.class,null,true);
				      try {
				        LOG.info("Execute Remote for Query:" + getQueryId() +" calling executeDomainListQuery on index");
				        // scan index for matching patterns ... spill into writer ...
				        recordCounts[currentThreadIdx] += _slaveDatabaseIndex.queryDomainsGivenPattern(getQueryData().getSearchPattern(), currentShardId, spillWriter);
				        LOG.info("Execute Remote for Query:" + getQueryId() +" executeDomainListQuery returned:" + recordCounts[currentThreadIdx]);
				      }
				      finally { 
				        spillWriter.close();
				        // increment semaphore count 
				        semaphore.release();
				      }
			      }
			      catch (IOException e) {
			        LOG.error("Execute Remote for Query:" + getQueryId() +" executeDomainListQuery failed with error:" + CCStringUtils.stringifyException(e));
			        exceptions[currentThreadIdx] = e;
			      }
          }
  			});
  			subQueryThread.start();
  		}
  		
  		// ok block until all queries are complete
  		LOG.info("Query:" + getQueryId() + " Waiting on Worker Threads");
  		semaphore.acquireUninterruptibly();
  		LOG.info("Query:" + getQueryId() + " All Threads Compelted");
  		
  		for (IOException e : exceptions) { 
  			if (e != null) { 
  				LOG.error("Query:" + getQueryId() + " Failed with Exception:" + CCStringUtils.stringifyException(e));
  				throw e;
  			}
  		}
  		long cumulativeRecordCount = 0L;
  		for (long recordCount : recordCounts)
  			cumulativeRecordCount += recordCount;
      return cumulativeRecordCount;
  }
  
  @Override
  public boolean cachedResultsAvailable(FileSystem fileSystem,Configuration conf, QueryRequest theClientRequest) throws IOException {
    
    FileSystem localFileSystem = FileSystem.getLocal(conf);
    
    Path outputFileName = new Path(getLocalQueryResultsPathPrefix(theClientRequest)+getOutputFileNameBasedOnSortByField(theClientRequest.getClientQueryInfo().getSortByField()));
    
    //LOG.info("Cached Results Available called for Query:" + theClientRequest.getSourceQuery().getQueryId() + ". Checking Path:" + outputFileName);
    //Path indexFileName  = new Path(outputFileName.toString() + ".IDX");
    boolean result = localFileSystem.exists(outputFileName);
    //LOG.info("Cached Results Available called for Query:" + theClientRequest.getSourceQuery().getQueryId() + ". returning:" + result);
   
    return result;
  }

  @Override
  public boolean requiresRemoteDispatch(FileSystem fileSystem,
      Configuration conf, ShardMapper shardMapper,
      QueryRequest<DomainListQueryInfo, Text, SubDomainMetadata> theClientRequest,
      ArrayList<ShardIndexHostNameTuple> shardIdToHostNameMapping)
      throws IOException {
    
  	// get shard mappings for index ... 
  	shardIdToHostNameMapping.addAll(shardMapper.mapShardIdsForIndex(DatabaseIndexV2.MasterDatabaseIndex.INDEX_NAME_DOMAIN_NAME_TO_METADATA));
  	
    // create a set representing the collection of parts required to complete this query ... 
    Set<String> requiredParts = new HashSet<String>();
    
    for (ShardIndexHostNameTuple tuple : shardIdToHostNameMapping) { 
      requiredParts.add(getPartNameForSlave(tuple.getShardId()));
    }
    
    // now iterate parts available on hdfs ... 
    Path remoteQueryPath = getHDFSQueryResultsPath();
    //LOG.info("Results Path is:" + remoteQueryPath);
    
    FileStatus availableParts[] = fileSystem.globStatus(new Path(remoteQueryPath,"part-*"));
    
    for (FileStatus part : availableParts) { 
      //LOG.info("Found Path:" + part.getPath());
      requiredParts.remove(part.getPath().getName());
    }
    
    // now check to see if all parts are available 
    if (requiredParts.size() != 0) { 
      for (String part: requiredParts) { 
        LOG.info("Required remote part:" + part + " NOT available yet.");
      }
      return true;
    }
    else { 
      LOG.info("All parts required for query available.");
      return false;
    }
  }
  
  
}
