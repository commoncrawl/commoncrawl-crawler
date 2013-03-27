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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.protocol.CrawlDatumAndMetadata;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.service.queryserver.ClientQueryInfo;
import org.commoncrawl.service.queryserver.DomainURLListQueryInfo;
import org.commoncrawl.service.queryserver.ShardIndexHostNameTuple;
import org.commoncrawl.service.queryserver.index.DatabaseIndexV2;
import org.commoncrawl.service.queryserver.index.DatabaseIndexV2.SlaveDatabaseIndex;
import org.commoncrawl.service.queryserver.index.DatabaseIndexV2.MasterDatabaseIndex.MetadataOut;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.FlexBuffer;

/**
 * 
 * @author rana
 *
 */
public class DomainURLListQuery extends Query<DomainURLListQueryInfo,URLFPV2,CrawlDatumAndMetadata> {

  private static final Log LOG = LogFactory.getLog(DomainURLListQuery.class);

  
  public final static String SORT_BY_NAME      = "NAME";
  //public final static String SORT_BY_STATUS    = "STATUS";
  //public final static String SORT_BY_TIME      = "TIME";
  public final static String SORT_BY_PR        = "PR";
  
 
  public DomainURLListQuery() { 
    
  }
  
  public DomainURLListQuery(DomainURLListQueryInfo queryInfo) { 
    setQueryData(queryInfo);
  }  
  
  private String getURLOutputFileNameBasedOnSortByField(String sortByField) throws IOException {
    if(sortByField.length() == 0 || sortByField.equals(SORT_BY_NAME)) { 
      return "DATA_" + SORT_BY_NAME;
    }
    else if (sortByField.equals(SORT_BY_PR)) {
    	return "DATA_" + SORT_BY_PR;
    }
    /*
    else if (sortByField.equals(SORT_BY_STATUS)|| sortByField.equals(SORT_BY_TIME)
        || sortByField.equals(SORT_BY_PR)) { 
      return "DATA_" + sortByField;
    }
    */
    throw new IOException(sortByField +" is an INVALID SORT FIELD");
  }
  
  private String getSharedOutputFileNameBasedOnSortByAndShardId(String sortByField,int shardId) throws IOException {
  	if (sortByField == null) { 
  		throw new IOException("Invalid Sort By Field");
  	}
  	return getURLOutputFileNameBasedOnSortByField(sortByField) + "-" + getPartNameForSlave(shardId);  	
  }
  

  @Override
  public boolean cachedResultsAvailable(FileSystem fileSystem,Configuration conf, QueryRequest<DomainURLListQueryInfo,URLFPV2,CrawlDatumAndMetadata> theClientRequest) throws IOException {
    FileSystem localFileSystem = FileSystem.getLocal(conf);

    Path urlOutputFileName = new Path(getLocalQueryResultsPathPrefix(theClientRequest)+getURLOutputFileNameBasedOnSortByField(theClientRequest.getClientQueryInfo().getSortByField()));
   
    // LOG.info("Cached Results Available called for Query:" + theClientRequest.getSourceQuery().getQueryId() + ". Checking Path:" +  urlOutputFileName);
    return localFileSystem.exists(urlOutputFileName);
  }

  
  
  @Override
  protected long executeRemote(
  		FileSystem fileSystem, Configuration conf,
	    EventLoop eventLoop, SlaveDatabaseIndex instanceIndex, File tempFirDir,
	    QueryProgressCallback<DomainURLListQueryInfo,URLFPV2,CrawlDatumAndMetadata> progressCallback		
  ) throws IOException {
    
  	// OK .. WE EXPECT A SINGLE RELEVANT SHARD ID 
  	if (getCommonQueryInfo().getRelevantShardIds().size() != 1) { 
  		throw new IOException("Invalid Shard Id Count in Remote Dispatch");
  	}
  	
    Path remoteURLListPath = getRemoteOutputFilePath(getClientQueryInfo(),getCommonQueryInfo().getRelevantShardIds().get(0)); 
    
    LOG.info("ExecuteRemote called for Query:" + getQueryId() + " Creating spill files:" +remoteURLListPath);
    FSDataOutputStream urlListWriter = fileSystem.create(remoteURLListPath);
 
    try { 
      
      long recordCountOut = 0;

      try {
        LOG.info("Execute Remote for Query:" + getQueryId() +" Calling executeURLListQuery");
        FlexBuffer urlListOut = null;
        if (getClientQueryInfo().getSortByField().compareTo(SORT_BY_NAME) == 0) { 
        	urlListOut = _slaveDatabaseIndex.queryURLListSortedByName(getQueryData().getDomainId());
        }
        else if (getClientQueryInfo().getSortByField().compareTo(SORT_BY_PR) == 0) { 
        	urlListOut = _slaveDatabaseIndex.queryURLListSortedByPR(getQueryData().getDomainId());
        }
        else { 
        	throw new IOException("Invalid Sort Field:" + getClientQueryInfo().getSortByField()); 
        }
        if (urlListOut != null) { 
        	urlListWriter.write(urlListOut.get(),urlListOut.getOffset(),urlListOut.getCount());
        	urlListWriter.flush();
        	recordCountOut = urlListOut.getCount() / 8L;
        }
        LOG.info("Execute Remote for Query:" + getQueryId() +" executeDomainListQuery returned:" + recordCountOut);
        
        return recordCountOut;
      }
      catch (IOException e) {
        LOG.error("Execute Remote for Query:" + getQueryId() +" executeDomainListQuery failed with error:" + CCStringUtils.stringifyException(e));
        throw e;
      }
      
    }
    finally { 
      if (urlListWriter != null) {  
      	urlListWriter.close();
      }

    }
  }

  @Override
  public void remoteDispatchComplete(FileSystem fileSystem,Configuration conf,QueryRequest<DomainURLListQueryInfo,URLFPV2,CrawlDatumAndMetadata> request, long resultCount) throws IOException {
    
  	if (getShardIdToHostMapping().size() != 1) { 
  		throw new IOException("Excepected One ShardIdToHostMapping. Got:" + getShardIdToHostMapping().size());
  	}
  	
  	LOG.info("remoteDispathc Complete Called");
  	Path remoteURLListPath = getRemoteOutputFilePath(getClientQueryInfo(),getShardIdToHostMapping().get(0).getShardId());
    
    if (fileSystem.exists(remoteURLListPath)) {
    	
    	LocalFileSystem localFS = FileSystem.getLocal(conf);
      Path localURLListPath = new Path(getLocalQueryResultsPathPrefix(request)+getURLOutputFileNameBasedOnSortByField(request.getClientQueryInfo().getSortByField()));
      localFS.delete(localURLListPath,false);
      LOG.info("Copying " + remoteURLListPath + " to LocalPath:"  + localURLListPath);
      fileSystem.copyToLocalFile(remoteURLListPath, localURLListPath);
    }
  }
  
  static final int FP_RECORD_SIZE = 8;
  private static void readPaginatedResults(final DatabaseIndexV2.MasterDatabaseIndex masterIndex,long domainId, FSDataInputStream inputStream,long length,String sortByField,int sortOrder,int pageNumber,int pageSize,QueryResult<URLFPV2,CrawlDatumAndMetadata> resultOut) throws IOException { 
	  // if descending sort order ... 
	  // take pageNumber * pageSize as starting point
	  long offset = 0;
	  long startPos = 0;
	  long endPos   = 0;
	  
	  // calculate total record count ... 
	  int totalRecordCount = (int) (length / FP_RECORD_SIZE);
	  
	  resultOut.getResults().clear();
	  resultOut.setPageNumber(pageNumber);
	  resultOut.setTotalRecordCount(totalRecordCount);
	    
	  // flip pr due to bug in how we sort pr 
	  if (sortByField.equals(SORT_BY_PR)) { 
	  	if (sortOrder == ClientQueryInfo.SortOrder.ASCENDING)
	  		sortOrder = ClientQueryInfo.SortOrder.DESCENDING;
	  	else 
	  		sortOrder = ClientQueryInfo.SortOrder.ASCENDING;
	  		
	  }
	  
	  if (sortOrder == ClientQueryInfo.SortOrder.ASCENDING) { 
	    startPos = pageNumber * pageSize;
	    endPos   = Math.min(startPos + pageSize, totalRecordCount);
	    offset	 = pageNumber * pageSize;
	  }
	  else { 
	    startPos = totalRecordCount - ((pageNumber +1) * pageSize);
	    endPos   = startPos + pageSize;
	    startPos = Math.max(0,startPos);
	    offset = totalRecordCount - ((pageNumber +1) * pageSize);
	  }
	  //LOG.info("readPaginatedResults called on Index with sortOrder:" + sortOrder + " pageNumber: " + pageNumber + " pageSize:" + pageSize + " offset is:" + offset);
	  if (startPos < totalRecordCount) { 
	    
	    //LOG.info("Seeking to Offset:" + startPos);
	    inputStream.seek(startPos * FP_RECORD_SIZE);
	    //LOG.info("Reading from:"+ startPos + " to:" + endPos + " (exclusive)");
	    for (long i=startPos;i<endPos;++i) {
	      
	    	URLFPV2 key = new URLFPV2();
	    	
	    	key.setDomainHash(domainId);
	    	key.setUrlHash(inputStream.readLong());
	    	
	    	// ok time to find this item in the master index ... 
	    	CrawlDatumAndMetadata metadataObject = new CrawlDatumAndMetadata();
	    	long timeStart = System.currentTimeMillis();
	    	MetadataOut metadataOut = masterIndex.queryMetadataAndURLGivenFP(key);
	    	long timeEnd = System.currentTimeMillis();
	    	
	    	//LOG.info("Metadata Retrieval for Index:"+ i + " took:" + (timeEnd - timeStart));
	    	
	    	if (metadataOut == null) { 
	    		LOG.error("Failed to Retrieve URL and Metadata for Domain:" + domainId + " FP:" + key.getUrlHash());
	    		metadataObject.setUrl("NULL-DH(" + key.getDomainHash() + ")-FP(" + key.getUrlHash() + ")");
	    	}
	    	else { 
	    		metadataObject.setUrl(metadataOut.url.toString());
	    		metadataObject.setStatus(metadataOut.fetchStatus);
	    		if (metadataOut.lastFetchTime > 0) { 
	    			metadataObject.getMetadata().setLastFetchTimestamp(metadataOut.lastFetchTime);
	    		}
	    		metadataObject.getMetadata().setPageRank(metadataOut.pageRank);
	    	}
	    	
	      if (sortOrder == ClientQueryInfo.SortOrder.DESCENDING) { 
	        resultOut.getResults().add(0,new QueryResultRecord<URLFPV2,CrawlDatumAndMetadata>(key,metadataObject));
	      }
	      else { 
	        resultOut.getResults().add(new QueryResultRecord<URLFPV2,CrawlDatumAndMetadata>(key,metadataObject));
	      }
	    }
	  }
  }
  
  
  @Override
  public void getCachedResults(FileSystem fileSyste, Configuration conf,EventLoop eventLoop, final DatabaseIndexV2.MasterDatabaseIndex masterIndex, QueryRequest<DomainURLListQueryInfo,URLFPV2,CrawlDatumAndMetadata> theClientRequest,QueryCompletionCallback<DomainURLListQueryInfo,URLFPV2,CrawlDatumAndMetadata> callback) throws IOException {
    LOG.info("getCachedResults for Query:" + getQueryId() +" Retrieving Cached Results");
    
    FileSystem localFileSystem = FileSystem.getLocal(conf);
      
    Path outputFileName = new Path(getLocalQueryResultsPathPrefix(theClientRequest)+getURLOutputFileNameBasedOnSortByField(theClientRequest.getClientQueryInfo().getSortByField()));
    
    FSDataInputStream inputStream = localFileSystem.open(outputFileName);
    
    try { 
	    QueryResult<URLFPV2,CrawlDatumAndMetadata> resultOut = new QueryResult<URLFPV2,CrawlDatumAndMetadata>(); 
	    
	    //LOG.info("Calling ReadPaginationResults");
	    readPaginatedResults(
	    		masterIndex, 
	    		getQueryData().getDomainId(), 
	    		inputStream, localFileSystem.getFileStatus(outputFileName).getLen(),
	    		theClientRequest.getClientQueryInfo().getSortByField(),
	        theClientRequest.getClientQueryInfo().getSortOrder(),
	        theClientRequest.getClientQueryInfo().getPaginationOffset(),
	        theClientRequest.getClientQueryInfo().getPageSize(),
	        resultOut);
	
	      
	      //LOG.info("Initiating getCachedResults Callback");
	      callback.queryComplete(theClientRequest,resultOut);
	      
    }
    finally {
    	inputStream.close();
    }
  }

  @Override
  public String getCanonicalId() {
    return encodePatternAsFilename("DURLQ:" + getQueryData().getDomainId()); 
  }

  private Path getRemoteOutputFilePath(ClientQueryInfo queryInfo,int shardId)throws IOException { 
    
  	String sortByField = queryInfo.getSortByField();
    
    Path remoteQueryPath = getHDFSQueryResultsPath();

    // ok construct the final output name based on the shard id 
    return new Path(remoteQueryPath,getSharedOutputFileNameBasedOnSortByAndShardId(sortByField,shardId));  	
  }
  
  private int getShardIdGivenDomainId(long domainId) { 
    //ok, we need to figure out our shard id and map it ... 
    return (((int)getQueryData().getDomainId()) & Integer.MAX_VALUE) %  CrawlEnvironment.NUM_DB_SHARDS;
  }
  
  
  @Override
  public boolean requiresRemoteDispatch(FileSystem fileSystem,
      Configuration conf, ShardMapper shardMapper,
      QueryRequest<DomainURLListQueryInfo,URLFPV2,CrawlDatumAndMetadata> theClientRequest,
      ArrayList<ShardIndexHostNameTuple> shardIdToHostNameMapping) throws IOException { 


  	if (cachedResultsAvailable(fileSystem, conf, theClientRequest)) { 
  		return false;
  	}
  	
    String sortByField = theClientRequest.getClientQueryInfo().getSortByField();
    
    if (sortByField.compareTo(SORT_BY_NAME) != 0 && sortByField.compareTo(SORT_BY_PR) != 0) { 
    	return false;
    }

    int targetShardId = getShardIdGivenDomainId(getQueryData().getDomainId());
    
    // ok construct the final output name based on the shard id 
    Path dataOutputPath = getRemoteOutputFilePath(theClientRequest.getClientQueryInfo(),targetShardId);
    
    // ok does the file exist ... 
    if (fileSystem.exists(dataOutputPath)) {
    	// ok no need for remote dispatch ... 
    	return false;
    }
    
    
    // ok, remote file does not exist
    
    // first map index name based on sort order field 
    String indexName = sortByField.compareTo(SORT_BY_NAME) == 0 ? 
    		DatabaseIndexV2.MasterDatabaseIndex.INDEX_NAME_DOMAIN_ID_TO_URLLIST_SORTED_BY_NAME 
    		: DatabaseIndexV2.MasterDatabaseIndex.INDEX_NAME_DOMAIN_ID_TO_URLLIST_SORTED_BY_PR; 
    
    // now retrieve shard mappings based on index
    ArrayList<ShardIndexHostNameTuple> tuples = shardMapper.mapShardIdsForIndex(indexName);
    
    ShardIndexHostNameTuple targetTuple = null;
    for (ShardIndexHostNameTuple tuple : tuples) { 
    	if (tuple.getShardId() == targetShardId) {
    		targetTuple = tuple;
    		break;
    	}
    }
    
    if (targetTuple == null) { 
    	throw new IOException("Failed to find Mapping for Shard Index:" + targetShardId);
    }
    // add all returned mappings to shard mapping list ... 
    shardIdToHostNameMapping.add(targetTuple);
    
    // return true indicating that we need to execute this query remotely 
    return true;
  }

 
  /*
  @Override
  protected long executeLocal(FileSystem remoteFileSystem, Configuration conf,EventLoop eventLoop, File tempFirDir, QueryRequest requestObject)throws IOException {
    
    Path mergedURLDataPath = new Path(getLocalQueryResultsPath(requestObject),getURLOutputFileNameBasedOnSortByField(SORT_BY_NAME));
    
    //LOG.info("executeLocal called");
    
    // get a local file system object
    FileSystem localFileSystem = FileSystem.getLocal(conf);

    
    if (!localFileSystem.exists(mergedURLDataPath)) { 

      LOG.info("Execute Local for Query:" + getQueryId() +".Starting URL Data Merge");
      
      //LOG.info("Checking for parts files for url data merge");
      FileStatus urlDataStatusArray[] = remoteFileSystem.globStatus(new Path(getHDFSQueryResultsPath(),URL_DATA_PREFIX + "part-*"));
      //LOG.info("Found:" + urlDataStatusArray.length + " url data parts");    
      
      if (urlDataStatusArray.length == 0) { 
        LOG.error("Execute Local for Query:" + getQueryId() +" FAILED.No Parts Files Found!");
        return 0;
      }
      
      Vector<Path> urlDataPaths = new Vector<Path>();
      
      for (FileStatus part : urlDataStatusArray) { 
        //LOG.info("Found Part:"+ part.getPath());
        urlDataPaths.add(part.getPath());
      }
  
      LOG.info("Execute Local for Query:" + getQueryId() +".Initializing Merger");
      SequenceFileSpillWriter<Text,CrawlDatumAndMetadata> mergedFileSpillWriter = new SequenceFileSpillWriter<Text,CrawlDatumAndMetadata>(
          localFileSystem,
          conf,
          mergedURLDataPath,
          Text.class,
          CrawlDatumAndMetadata.class,true,true);
      
      SequenceFileMerger<Text,CrawlDatumAndMetadata> merger 
        = new SequenceFileMerger<Text,CrawlDatumAndMetadata>(
            remoteFileSystem,
            conf,
            urlDataPaths,
            mergedFileSpillWriter,
            Text.class,
            CrawlDatumAndMetadata.class,
            
            new RawValueKeyValueComparator<Text, CrawlDatumAndMetadata>() {
  
              
              @Override
              public int compare(Text key1, CrawlDatumAndMetadata value1, Text key2,CrawlDatumAndMetadata value2) {
                return key1.compareTo(key2);
              }

              @Override
              public int compareRaw(byte[] key1Data, int key1Offset,
                  int key1Length, byte[] key2Data, int key2Offset,
                  int key2Length, byte[] value1Data, int value1Offset,
                  int value1Length, byte[] value2Data, int value2Offset,
                  int value2Length) throws IOException {
                
                return WritableComparator.compareBytes(key1Data,key1Offset,key1Length,key2Data,key2Offset,key2Length);
              }
              
            },null
            ,null);
      
      try { 
        LOG.info("Execute Local for Query:" + getQueryId() +".Running Merger");
        merger.mergeAndSpill();
        LOG.info("Execute Local for Query:" + getQueryId() +".Merge Successful.. Deleting Merge Inputs");
        for (FileStatus urlDataPath : urlDataStatusArray) { 
          remoteFileSystem.delete(urlDataPath.getPath(),false);
        }
        
      }
      catch (IOException e){ 
        LOG.error("Execute Local for Query:" + getQueryId() +" FAILED during Merge with Exception:" + CCStringUtils.stringifyException(e));
        throw e;
      }
      finally { 
        merger.close();
      }
    }
    else { 
      LOG.info("Execute Local for Query:" + getQueryId() +" Merge File NAME URL Data Already Exists.Skipping");
    }
      
    // now check for query specific merge file ...
    Path queryResultsPath = new Path(getLocalQueryResultsPath(requestObject),getURLOutputFileNameBasedOnSortByField(requestObject.getClientQueryInfo().getSortByField()));
    
    LOG.info("Execute Local for Query:" + getQueryId() +" Checking for QueryResultsPath for DomainDetail Path is:" + queryResultsPath);
    
    if (!localFileSystem.exists(queryResultsPath)) {
      
      LOG.info("Execute Local for Query:" + getQueryId() +" Results File:" + queryResultsPath + " does not exist. Running sort and merge process");
      
      String sortByField = requestObject.getClientQueryInfo().getSortByField();
      

      LOG.info("Execute Local for Query:" + getQueryId() +" Allocating SpillWriter with output to:" + queryResultsPath);

      // allocate a spill writer ...  
      SequenceFileSpillWriter<Text,CrawlDatumAndMetadata> sortedResultsFileSpillWriter = new SequenceFileSpillWriter<Text,CrawlDatumAndMetadata>(localFileSystem,conf,queryResultsPath,Text.class,CrawlDatumAndMetadata.class,true,true);
      
      try { 
        
        //LOG.info("Allocating MergeSortSpillWriter");
        // and connect it to the merge spill writer ...
        MergeSortSpillWriter<Text, CrawlDatumAndMetadata> mergeSortSpillWriter = new MergeSortSpillWriter<Text, CrawlDatumAndMetadata>(
            localFileSystem,
            conf,
            sortedResultsFileSpillWriter,
            tempFirDir,
            getComparatorForSortField(sortByField),
            getKeyGeneratorForSortField(sortByField),
            null,
            Text.class,
            CrawlDatumAndMetadata.class,true);
        
        try { 
          
          // create a vector representing the single input segment 
          Vector<Path> singleInputSegment = new Vector<Path>();
          
          //LOG.info("Adding MergeResultsPath:" + mergedURLDataPath + " as input for Merger for DomainDetail URL Query Id:" + getQueryId());
          singleInputSegment.add(mergedURLDataPath);
          
          // create a SequenceFileReader
          SequenceFileReader<Text, CrawlDatumAndMetadata> mergeSegmentReader = new SequenceFileReader<Text, CrawlDatumAndMetadata>(
              localFileSystem,
              conf,
              singleInputSegment,
              mergeSortSpillWriter,
              Text.class,
              CrawlDatumAndMetadata.class);
              
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
      //LOG.info("Allocating SequenceFileIndex object for DomainDetail URL Query Id:" + getQueryId() + " with Path:" + queryResultsPath);
      SequenceFileIndex<Text, SubDomainStats> indexFile = new SequenceFileIndex<Text, SubDomainStats>(new File(queryResultsPath.toString()),Text.class,SubDomainStats.class);
      //LOG.info("SequenceFileIndex object for DomainListQuery Id:" + getQueryId() + " with Path:" + queryResultsPath + " returned record count:" + indexFile.getRecordCount());
      
      return indexFile.getRecordCount();
    }
    return 0;
  }

	*/
  /*
  private static OptimizedKeyGenerator<Text, CrawlDatumAndMetadata> getKeyGeneratorForSortField(String sortByField) throws IOException { 
  
    if (sortByField.equals(SORT_BY_STATUS)) {
      return new OptimizedKeyGenerator<Text, CrawlDatumAndMetadata>() {

        @Override
        public long generateOptimizedKeyForPair(Text keyType,CrawlDatumAndMetadata value) throws IOException {
          return  (long)value.getStatus();
        } 
      };
    }
    else if (sortByField.equals(SORT_BY_TIME)) { 
      return new OptimizedKeyGenerator<Text, CrawlDatumAndMetadata>() {

        @Override
        public long generateOptimizedKeyForPair(Text keyType,CrawlDatumAndMetadata value) throws IOException {
          return (long)value.getMetadata().getLastFetchTimestamp();
        } 
      };
    }
    else if (sortByField.equals(SORT_BY_PR)) {
      return new OptimizedKeyGenerator<Text, CrawlDatumAndMetadata>() {
      
        @Override
        public long generateOptimizedKeyForPair(Text keyType,CrawlDatumAndMetadata value) throws IOException {
          
          long valueOut = (long) value.getMetadata().getPageRank();
          
          valueOut = (valueOut * 1000) +  (long)((value.getMetadata().getPageRank() -(float) valueOut) * 1000.00f);
          
          return valueOut;
        }
      };
    }
    return null;
  }
  */
  /*
  
  private static RawValueKeyValueComparator<Text,CrawlDatumAndMetadata> getComparatorForSortField(String sortByField) throws IOException { 
    RawValueKeyValueComparator<Text,CrawlDatumAndMetadata> comparator = null; 
  
    if (sortByField.equals(SORT_BY_STATUS)) { 
      comparator = new RawValueKeyValueComparator<Text, CrawlDatumAndMetadata>() {

        CrawlDatumAndMetadata value1 = new CrawlDatumAndMetadata();
        CrawlDatumAndMetadata value2 = new CrawlDatumAndMetadata();

  
        @Override
        public int compare(Text key1, CrawlDatumAndMetadata value1, Text key2,CrawlDatumAndMetadata value2) {
          return value1.getStatus() - value2.getStatus();
        }

        @Override
        public int compareRaw(byte[] key1Data, int key1Offset, int key1Length,
            byte[] key2Data, int key2Offset, int key2Length, byte[] value1Data,
            int value1Offset, int value1Length, byte[] value2Data,
            int value2Offset, int value2Length) throws IOException {

          value1.clear();
          value2.clear();
          
          value1.readFields(new DataInputStream(new ByteArrayInputStream(value1Data,value1Offset,value1Length)));
          value2.readFields(new DataInputStream(new ByteArrayInputStream(value2Data,value2Offset,value2Length)));
          
          return compare(null, value1, null, value2);
          
        } 
      };
    }
    else if (sortByField.equals(SORT_BY_TIME)) { 
      comparator = new RawValueKeyValueComparator<Text, CrawlDatumAndMetadata>() {

        CrawlDatumAndMetadata value1 = new CrawlDatumAndMetadata();
        CrawlDatumAndMetadata value2 = new CrawlDatumAndMetadata();
        
        @Override
        public int compare(Text key1, CrawlDatumAndMetadata value1, Text key2,CrawlDatumAndMetadata value2) {
          if (value1.getMetadata().getLastFetchTimestamp() > value2.getMetadata().getLastFetchTimestamp())
            return 1;
          else if (value1.getMetadata().getLastFetchTimestamp() < value2.getMetadata().getLastFetchTimestamp()) 
            return -1;
          return 0;
        }

        @Override
        public int compareRaw(byte[] key1Data, int key1Offset, int key1Length,
            byte[] key2Data, int key2Offset, int key2Length, byte[] value1Data,
            int value1Offset, int value1Length, byte[] value2Data,
            int value2Offset, int value2Length) throws IOException {

          value1.clear();
          value2.clear();
          
          value1.readFields(new DataInputStream(new ByteArrayInputStream(value1Data,value1Offset,value1Length)));
          value2.readFields(new DataInputStream(new ByteArrayInputStream(value2Data,value2Offset,value2Length)));
          
          return compare(null,value1,null,value2);
        } 
      };
    }
    else if (sortByField.equals(SORT_BY_PR)) { 
      comparator = new RawValueKeyValueComparator<Text, CrawlDatumAndMetadata>() {

        CrawlDatumAndMetadata value1 = new CrawlDatumAndMetadata();
        CrawlDatumAndMetadata value2 = new CrawlDatumAndMetadata();
        

        @Override
        public int compare(Text key1, CrawlDatumAndMetadata value1, Text key2,CrawlDatumAndMetadata value2) {
          if (value1.getMetadata().getPageRank() > value2.getMetadata().getPageRank()) 
            return 1;
          else if (value1.getMetadata().getPageRank() < value2.getMetadata().getPageRank())
            return -1;
          return 0;
        }

        @Override
        public int compareRaw(byte[] key1Data, int key1Offset, int key1Length,
            byte[] key2Data, int key2Offset, int key2Length, byte[] value1Data,
            int value1Offset, int value1Length, byte[] value2Data,
            int value2Offset, int value2Length) throws IOException {

          long timeStart = System.currentTimeMillis();
          value1.clear();
          value2.clear();
          
          value1.readFields(new DataInputStream(new ByteArrayInputStream(value1Data,value1Offset,value1Length)));
          value2.readFields(new DataInputStream(new ByteArrayInputStream(value2Data,value2Offset,value2Length)));
          long timeEnd = System.currentTimeMillis();
          
          long timeElapsed =timeEnd - timeStart;

          
          return compare(null,value1,null,value2);
        } 
      };
    }
    
    if (comparator == null) { 
      throw new IOException("Comparator for Field:" + sortByField + " Not Found or Defined!");
    }
    return comparator;
  }
  */  
}
