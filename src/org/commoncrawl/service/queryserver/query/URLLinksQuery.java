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
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.protocol.CrawlDatumAndMetadata;
import org.commoncrawl.protocol.SubDomainMetadata;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.service.queryserver.ClientQueryInfo;
import org.commoncrawl.service.queryserver.InlinkingDomainInfo;
import org.commoncrawl.service.queryserver.ShardIndexHostNameTuple;
import org.commoncrawl.service.queryserver.URLLinkDetailQueryInfo;
import org.commoncrawl.service.queryserver.index.DatabaseIndexV2;
import org.commoncrawl.service.queryserver.index.DatabaseIndexV2.SlaveDatabaseIndex;
import org.commoncrawl.service.queryserver.index.DatabaseIndexV2.MasterDatabaseIndex.MetadataOut;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.CompressedURLFPListV2;
import org.commoncrawl.util.FlexBuffer;

/**
 * 
 * @author rana
 *
 */
public class URLLinksQuery extends Query<URLLinkDetailQueryInfo,Writable,Writable>{

  private static final Log LOG = LogFactory.getLog(URLLinksQuery.class);

  public URLLinksQuery() { 
    
  }
  
  public URLLinksQuery(URLLinkDetailQueryInfo queryInfo) { 
    setQueryData(queryInfo);
  }    
  
  @Override
  public String getCanonicalId() {
  	if (getQueryData().getQueryType() == URLLinkDetailQueryInfo.QueryType.LINKS_QUERY) { 
  		return encodePatternAsFilename("ULQ:" + getQueryData().getTargetURLFP().getDomainHash() + ":" + getQueryData().getTargetURLFP().getUrlHash());
  	}
  	else if (getQueryData().getQueryType() == URLLinkDetailQueryInfo.QueryType.INVERSE_QUERY) { 
  		return encodePatternAsFilename("UILQ:" + getQueryData().getTargetURLFP().getDomainHash() + ":" + getQueryData().getTargetURLFP().getUrlHash());
  	}
  	else if (getQueryData().getQueryType() == URLLinkDetailQueryInfo.QueryType.INVERSE_BY_DOMAIN_QUERY || getQueryData().getQueryType() == URLLinkDetailQueryInfo.QueryType.INVERSE_BY_DOMAIN_DETAIL_QUERY) { 
  		return encodePatternAsFilename("UILBDQ:" + getQueryData().getTargetURLFP().getDomainHash() + ":" + getQueryData().getTargetURLFP().getUrlHash());
  	}
  	else { 
  		return "";
  		//throw new IOException("Unspecified Link Query Type!");
  	}
  }

  
  protected static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }    
  
  @Override
  public boolean cachedResultsAvailable(FileSystem fileSystem,Configuration conf, QueryRequest theClientRequest) throws IOException {

    FileSystem localFileSystem = FileSystem.getLocal(conf);
    Path urlOutputFileName = new Path(getLocalQueryResultsPathPrefix(theClientRequest)+"DATA");

    // LOG.info("Cached Results Available called for Query:" + theClientRequest.getSourceQuery().getQueryId() + ". Checking Path:" +  urlOutputFileName);
    if (localFileSystem.exists(urlOutputFileName)) { 
      return true;
    }
    return false;
  }
  

  
  @SuppressWarnings("unchecked")
  @Override
  protected long executeLocal(FileSystem remoteFileSystem, Configuration conf,DatabaseIndexV2.MasterDatabaseIndex index,EventLoop eventLoop, File tempFileDir, QueryRequest<URLLinkDetailQueryInfo,Writable,Writable> requestObject) throws IOException {
    
    // either we need to fetch the link data information or we are going to need consolidate the remote results ...
    FileSystem localFileSystem = FileSystem.getLocal(conf);
  	
  	int targetShardId = getQueryData().getLinkDBFileNo();
  	
  	LOG.info("remoteDispathc Complete Called");
  	Path remoteFilePath = new Path(getHDFSQueryResultsPath(),getPartNameForSlave(targetShardId));
    
  	long recordCount = 0L;
  	
    if (remoteFileSystem.exists(remoteFilePath)) {

    	LocalFileSystem localFS = FileSystem.getLocal(conf);
    	
      Path localURLListPath = new Path(getLocalQueryResultsPathPrefix(requestObject)+"DATA");
      Path localURLListIndexPath = new Path(getLocalQueryResultsPathPrefix(requestObject)+"DATA.index");
      
      localFS.delete(localURLListPath,false);
      localFS.delete(localURLListIndexPath,false);

      if (getQueryData().getQueryType() == URLLinkDetailQueryInfo.QueryType.LINKS_QUERY || getQueryData().getQueryType() == URLLinkDetailQueryInfo.QueryType.INVERSE_QUERY) { 
      	recordCount = runOutlinkLocalQuery(remoteFileSystem,remoteFilePath,localFS,localURLListPath);
      }
      else if (getQueryData().getQueryType() == URLLinkDetailQueryInfo.QueryType.INVERSE_BY_DOMAIN_QUERY || getQueryData().getQueryType() == URLLinkDetailQueryInfo.QueryType.INVERSE_BY_DOMAIN_DETAIL_QUERY) { 
      	recordCount = runInlinksLocalQuery(index,remoteFileSystem,remoteFilePath,localFileSystem,localURLListIndexPath,localURLListPath);
      }
    }
    return recordCount;
  }
  
  private long runOutlinkLocalQuery(FileSystem inputFileSystem,Path outlinksInputPath,FileSystem outputFileSystem,Path outlinksOutputPath) throws IOException { 
    
  	long recordCount = 0L;
  	
    outputFileSystem.delete(outlinksOutputPath,false);
    
    FSDataInputStream remoteInputStream = inputFileSystem.open(outlinksInputPath);
    
    try { 
    	
    	FSDataOutputStream outputStream = outputFileSystem.create(outlinksOutputPath);
    	
    	try { 
    		CompressedURLFPListV2.Reader reader = new CompressedURLFPListV2.Reader(remoteInputStream);
    		
    		while (reader.hasNext()) { 
    			
    			URLFPV2 fingerprint = reader.next();
    			
    			outputStream.writeLong(fingerprint.getDomainHash());
    			outputStream.writeLong(fingerprint.getUrlHash());
    			
    			recordCount++;
    		}
    	}
    	finally { 
    		outputStream.close();
    	}
    }
    finally { 
    	remoteInputStream.close();
    }
    return recordCount;
  }

  private static class DomainInfo {
  	
  	DomainInfo(long domainId) { 
  		this.domainId = domainId; 
  	}
  	
  	public long domainId = -1;
  	public int  urlCount = 0;
  	public String domainName = null;
  	public long  dataPos = -1;
	}
  
  private long runInlinksLocalQuery(DatabaseIndexV2.MasterDatabaseIndex index,FileSystem inputFileSystem,Path inlinksInputPath,FileSystem outputFileSystem,Path inlinksDomainIndexPath,Path inlinksDetailOutputPath) throws IOException { 
    
  	long recordCount = 0L;
  	
    outputFileSystem.delete(inlinksDomainIndexPath,false);
    outputFileSystem.delete(inlinksDetailOutputPath,false);
    
    FSDataInputStream remoteInputStream = inputFileSystem.open(inlinksInputPath);
    
    try { 
    	
    	FSDataOutputStream indexOutputStream = outputFileSystem.create(inlinksDomainIndexPath);
    	FSDataOutputStream detailOutputStream = outputFileSystem.create(inlinksDetailOutputPath);
    	
    	ArrayList<InlinkingDomainInfo> domainList = new ArrayList<InlinkingDomainInfo>();
    	
    	try { 

    		LOG.info("Writing Detail Stream to:" + inlinksDetailOutputPath);
    		CompressedURLFPListV2.Reader reader = new CompressedURLFPListV2.Reader(remoteInputStream);
    		
    		InlinkingDomainInfo lastDomain = null;
    		
    		while (reader.hasNext()) { 
    			
    			// read the nex fingerprint 
    			URLFPV2 fingerprint = reader.next();
    			// and first see if we have a domain transition 
    			if (lastDomain == null || lastDomain.getDomainId() != fingerprint.getDomainHash()) {
    				// remember the domain 
    				lastDomain = new InlinkingDomainInfo();
    				lastDomain.setDomainId(fingerprint.getDomainHash());
    				// add it to the list 
    				domainList.add(lastDomain);
    				// update date position 
    				lastDomain.setUrlDataPos(detailOutputStream.getPos());
    			}
    			// increment url count for the domain
    			lastDomain.setUrlCount(lastDomain.getUrlCount() + 1);
    			
    			detailOutputStream.writeLong(fingerprint.getDomainHash());
    			detailOutputStream.writeLong(fingerprint.getUrlHash());
    			
    			recordCount++;
    		}
    		
    		LOG.info("Retrieving Domain Metadata for :" + domainList.size() + " Domain Records");
    		// ok, now resolve domain names
    		for (InlinkingDomainInfo domain : domainList) { 
    			SubDomainMetadata metadata = index.queryDomainMetadataGivenDomainId(domain.getDomainId());
    			if (metadata == null) { 
    				LOG.error("*** Failed to Resolve DomainId:" + domain.getDomainId());
    			}
    			else {
    				if (metadata.getDomainText().length() == 0) { 
    					LOG.error("*** Metadata for Domain Id:" + domain.getDomainId() + " contained NULL Name Value.");
    					domain.setDomainName("_ERROR:BAD RECORD");
    				}
    				else { 
    					domain.setDomainName(metadata.getDomainText());
    				}
    				//LOG.info("***Found Domain:" + domain.getDomainName() + " urlCount:" + domain.getUrlCount());
    			}
    		}
    		
    		LOG.info("Sorting Domain List of Size:" + domainList.size());
    		// ok sort by domain name 
    		Collections.sort(domainList);
    		
    		
    		LOG.info("Building In Memory Index");
    		
    		// ok write out domain info
    		DataOutputBuffer indexHeaderBuffer = new DataOutputBuffer();
    		DataOutputBuffer indexDataBuffer = new DataOutputBuffer();
    		
    		LOG.info("***Writing Domain List Size:" + domainList.size());
    		indexHeaderBuffer.writeInt(domainList.size());
    		
    		// ok iterate and write to both buffers  
    		for (InlinkingDomainInfo domain : domainList) { 
    			indexHeaderBuffer.writeInt(indexDataBuffer.getLength());
    			domain.write(indexDataBuffer);
    		}
    		
    		LOG.info("Writing Index to:" + inlinksDomainIndexPath + " IndexHeaderLength:" + indexHeaderBuffer.getLength() + " IndexDataLength:" + indexDataBuffer.getLength());
    		// ok now flush both buffers to disk
    		indexOutputStream.write(indexHeaderBuffer.getData(),0,indexHeaderBuffer.getLength());
    		indexOutputStream.write(indexDataBuffer.getData(),0,indexDataBuffer.getLength());
    	}
    	finally { 					
    		indexOutputStream.flush();
    		indexOutputStream.close();
    		detailOutputStream.flush();
    		detailOutputStream.close();
    	}
    }
    finally { 
    	remoteInputStream.close();
    }
    return recordCount;
  }

  
  private static void readPaginatedInlinkingDomainInfo(final DatabaseIndexV2.MasterDatabaseIndex masterIndex,FileSystem indexFileSystem,Path indexPath,Path detailPath,int sortOrder,int pageNumber,int pageSize,QueryResult<Writable,Writable> resultOut) throws IOException { 
	  // if descending sort order ... 
	  // take pageNumber * pageSize as starting point
	  long offset = 0;
	  long startPos = 0;
	  long endPos   = 0;
	  
	  FSDataInputStream indexStream = indexFileSystem.open(indexPath);
	  
	  try { 
	  
		  // read in the total record count ... 
		  int totalRecordCount = indexStream.readInt();
		  
		  LOG.info("***RecordCount:" + totalRecordCount + " Allocating Buffer Of:" + (totalRecordCount * 4) + " bytes. FileLength:" + indexFileSystem.getFileStatus(indexPath).getLen());
		  // read in index header data upfront 
		  byte indexHeaderData[] = new byte[totalRecordCount * 4];
		  // read it 
		  indexStream.readFully(indexHeaderData);
		  // mark string start pos 
		  long detailStartPos = indexStream.getPos();
		  // initialize index header reader stream 
		  DataInputBuffer indexHeaderStream = new DataInputBuffer();
		  indexHeaderStream.reset(indexHeaderData,0,indexHeaderData.length); 
		  	  
		  
		  resultOut.getResults().clear();
		  resultOut.setPageNumber(pageNumber);
		  resultOut.setTotalRecordCount(totalRecordCount);
		    
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
		  	indexHeaderStream.skip(startPos * 4);
		    //LOG.info("Reading from:"+ startPos + " to:" + endPos + " (exclusive)");
		    for (long i=startPos;i<endPos;++i) {
		      
		    	// read data offset ... 
		    	int domainDataPos = indexHeaderStream.readInt();
		    	// seek to it 
		    	indexStream.seek(detailStartPos + domainDataPos);
		    	// read the detail data  
		    	InlinkingDomainInfo domainInfo = new InlinkingDomainInfo();
		    	domainInfo.readFields(indexStream);
		    	// ok extract name 
		    	String domainName = domainInfo.getDomainName();
		    	if (domainName.length() == 0) {
		    		//TODO: NEED TO TRACK THIS DOWN 
		    		domainName = "<<OOPS-NULL>>";
		    	}
		    	Text key = new Text(domainName);
		    	domainInfo.setFieldClean(InlinkingDomainInfo.Field_DOMAINNAME);
		    	
		      if (sortOrder == ClientQueryInfo.SortOrder.DESCENDING) { 
		        resultOut.getResults().add(0,new QueryResultRecord<Writable,Writable>(key,domainInfo));
		      }
		      else { 
		        resultOut.getResults().add(new QueryResultRecord<Writable,Writable>(key,domainInfo));
		      }
		    }
		  }
	  }
	  finally { 
	  	indexStream.close();
	  }
  }  
  
  
  private static void readPaginatedInlinkingDomainDetail(final DatabaseIndexV2.MasterDatabaseIndex masterIndex,FileSystem indexFileSystem,Path detailDataPath,InlinkingDomainInfo srcDomainInfo,int sortOrder,int pageNumber,int pageSize,QueryResult<Writable,Writable> resultOut) throws IOException { 
	  // if descending sort order ... 
	  // take pageNumber * pageSize as starting point
	  long offset = 0;
	  long startPos = 0;
	  long endPos   = 0;
	  
	  FSDataInputStream dataStream = indexFileSystem.open(detailDataPath);
	  
	  try { 
	  
		  // read in the total record count ... 
		  int totalRecordCount = srcDomainInfo.getUrlCount();
		  	  
		  
		  resultOut.getResults().clear();
		  resultOut.setPageNumber(pageNumber);
		  resultOut.setTotalRecordCount(totalRecordCount);
		    
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
		  	dataStream.skip(srcDomainInfo.getUrlDataPos() + (startPos * FP_RECORD_SIZE));
		    //LOG.info("Reading from:"+ startPos + " to:" + endPos + " (exclusive)");
		    for (long i=startPos;i<endPos;++i) {

		    	URLFPV2 key = new URLFPV2();
		    	
		    	key.setDomainHash(dataStream.readLong());
		    	key.setUrlHash(dataStream.readLong());
		    	
		    	// ok time to find this item in the master index ... 
		    	CrawlDatumAndMetadata metadataObject = new CrawlDatumAndMetadata();
		    	MetadataOut metadataOut = masterIndex.queryMetadataAndURLGivenFP(key);
		    	
		    	if (metadataOut == null) { 
		    		LOG.error("Failed to Retrieve URL and Metadata for Domain:" + key.getDomainHash() + " FP:" + key.getUrlHash());
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
		        resultOut.getResults().add(0,new QueryResultRecord<Writable,Writable>(key,metadataObject));
		      }
		      else { 
		        resultOut.getResults().add(new QueryResultRecord<Writable,Writable>(key,metadataObject));
		      }		    	
		    	
		    }
		  }
	  }
	  finally { 
	  	dataStream.close();
	  }
  }
  
  @Override
  protected long executeRemote(
      FileSystem fileSystem,
      Configuration conf,
      EventLoop eventLoop,
      SlaveDatabaseIndex instanceIndex,
      File tempFirDir,
      QueryProgressCallback<URLLinkDetailQueryInfo, Writable,Writable> progressCallback)
      throws IOException {
  	
  	// OK .. WE EXPECT A SINGLE RELEVANT SHARD ID 
  	if (getCommonQueryInfo().getRelevantShardIds().size() != 1) { 
  		throw new IOException("Invalid Shard Id Count in Remote Dispatch");
  	}
  	
  	int myShardIndex = getCommonQueryInfo().getRelevantShardIds().get(0);
  	
  	// calculate the output path 
    FlexBuffer linkDataOut = null; 
    if (getQueryData().getQueryType() == URLLinkDetailQueryInfo.QueryType.LINKS_QUERY) { 
    	linkDataOut = instanceIndex.queryOutlinksByFP(getQueryData().getTargetURLFP(),myShardIndex, getQueryData().getLinkDBOffset());
    }
    else { 
    	linkDataOut = instanceIndex.queryInlinksByFP(getQueryData().getTargetURLFP(),myShardIndex, getQueryData().getLinkDBOffset());
    }
    
    // ok, if the stream is valid ... 
    long recordCount = 0;
    
  	// ok create output stream 
  	Path remoteFilePath = new Path(getHDFSQueryResultsPath(),getPartNameForSlave(myShardIndex));
  	
  	FSDataOutputStream outputStream = fileSystem.create(remoteFilePath);

  	try {
  		// write inlinks to output stream 
  		
	    if (linkDataOut != null && linkDataOut.getCount() != 0) {
	    	recordCount = 1;
	    	outputStream.write(linkDataOut.get(),linkDataOut.getOffset(),linkDataOut.getCount());
	    	outputStream.flush();
	    }
  	}
  	catch (IOException e) { 
  		//in case of error delete output file 
  		LOG.error(CCStringUtils.stringifyException(e));
  		outputStream.close();
  		outputStream = null;
  		fileSystem.delete(remoteFilePath,false);
  	}
  	finally { 
  		if (outputStream != null) { 
  			outputStream.close();
  		}
  	}
    return recordCount;
  }
  
  @Override
  public boolean requiresRemoteDispatch(
      FileSystem fileSystem,
      Configuration conf,
      ShardMapper shardMapper,
      QueryRequest<URLLinkDetailQueryInfo, Writable,Writable> theClientRequest,
      ArrayList<ShardIndexHostNameTuple> shardIdToHostNameMapping)
      throws IOException {
    
  	if (!getQueryData().isFieldDirty(URLLinkDetailQueryInfo.Field_LINKDBFILENO)) { 
  		throw new IOException("No Shard Id Specified in Query!");
  	}
  	int targetShardId = getQueryData().getLinkDBFileNo();
  	
  	// ok, otherwsie calculate remote output path based on shard id  
  	Path remoteFilePath = new Path(getHDFSQueryResultsPath(),getPartNameForSlave(targetShardId));
  	// ok figure out of the path exists ... 
  	if (fileSystem.exists(remoteFilePath)) { 
  		return false;
  	}
  	// otherwise, yes we need to initiate a remote query ... 
  	// get shard mapping based on index 
  	ArrayList<ShardIndexHostNameTuple> tuples = null;
  	
  	if (getQueryData().getQueryType() == URLLinkDetailQueryInfo.QueryType.LINKS_QUERY) { 
  		tuples = shardMapper.mapShardIdsForIndex(DatabaseIndexV2.MasterDatabaseIndex.INDEX_NAME_OUTLINK_DATA);
  	}
  	else { 
  		tuples = shardMapper.mapShardIdsForIndex(DatabaseIndexV2.MasterDatabaseIndex.INDEX_NAME_INLINK_DATA);
  	}
  	// locate our specific shard tuple 
  	ShardIndexHostNameTuple tupleOut = null;
  	for (ShardIndexHostNameTuple tuple : tuples) { 
  		if (tuple.getShardId() == targetShardId) { 
  			tupleOut = tuple;
  			break;
  		}
  	}
  	
  	// we need that mapping  
  	if (tupleOut == null) { 
  		throw new IOException("Could Not Find Mapping for Shard:" + targetShardId + " in Index:" + DatabaseIndexV2.MasterDatabaseIndex.INDEX_NAME_INLINK_DATA);
  	}
  	else { 
  		// add the mapping to output tuple list
  		shardIdToHostNameMapping.add(tupleOut);
  		// yes.... we require remote dispatch 
  		return true;
  	}
  }  
  
  static final int FP_RECORD_SIZE = 16;
  private static void readPaginatedResults(final DatabaseIndexV2.MasterDatabaseIndex masterIndex,FSDataInputStream inputStream,long length,int sortOrder,int pageNumber,int pageSize,QueryResult<Writable,Writable> resultOut) throws IOException { 
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
	    	
	    	key.setDomainHash(inputStream.readLong());
	    	key.setUrlHash(inputStream.readLong());
	    	
	    	// ok time to find this item in the master index ... 
	    	CrawlDatumAndMetadata metadataObject = new CrawlDatumAndMetadata();
	    	MetadataOut metadataOut = masterIndex.queryMetadataAndURLGivenFP(key);
	    	
	    	if (metadataOut == null) { 
	    		LOG.error("Failed to Retrieve URL and Metadata for Domain:" + key.getDomainHash() + " FP:" + key.getUrlHash());
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
	        resultOut.getResults().add(0,new QueryResultRecord<Writable,Writable>(key,metadataObject));
	      }
	      else { 
	        resultOut.getResults().add(new QueryResultRecord<Writable,Writable>(key,metadataObject));
	      }
	    }
	  }
  }
  
  
  @Override
  public void getCachedResults(FileSystem fileSyste, Configuration conf,EventLoop eventLoop, final DatabaseIndexV2.MasterDatabaseIndex masterIndex, QueryRequest<URLLinkDetailQueryInfo,Writable,Writable> theClientRequest,QueryCompletionCallback<URLLinkDetailQueryInfo,Writable,Writable> callback) throws IOException {
    LOG.info("getCachedResults for Query:" + getQueryId() +" Retrieving Cached Results");
    
    FileSystem localFileSystem = FileSystem.getLocal(conf);
      
    Path cacheDataFileName = new Path(getLocalQueryResultsPathPrefix(theClientRequest)+"DATA");
    Path cacheDataIndexFileName = new Path(getLocalQueryResultsPathPrefix(theClientRequest)+"DATA.index");
    
    QueryResult<Writable,Writable> resultOut = new QueryResult<Writable,Writable>(); 
	    
    if (getQueryData().getQueryType() == URLLinkDetailQueryInfo.QueryType.LINKS_QUERY || getQueryData().getQueryType() == URLLinkDetailQueryInfo.QueryType.INVERSE_QUERY) { 
    	
    	FSDataInputStream inputStream = localFileSystem.open(cacheDataFileName);

    	try {  
		    //LOG.info("Calling ReadPaginationResults");
		    readPaginatedResults(
		    		masterIndex, 
		    		inputStream, localFileSystem.getFileStatus(cacheDataFileName).getLen(),
		        theClientRequest.getClientQueryInfo().getSortOrder(),
		        theClientRequest.getClientQueryInfo().getPaginationOffset(),
		        theClientRequest.getClientQueryInfo().getPageSize(),
		        resultOut);
      }
      finally {
      	inputStream.close();
      }
    }
    else if (getQueryData().getQueryType() == URLLinkDetailQueryInfo.QueryType.INVERSE_BY_DOMAIN_QUERY) {
    	readPaginatedInlinkingDomainInfo(masterIndex, localFileSystem, cacheDataIndexFileName, cacheDataFileName,
	        theClientRequest.getClientQueryInfo().getSortOrder(),
	        theClientRequest.getClientQueryInfo().getPaginationOffset(),
	        theClientRequest.getClientQueryInfo().getPageSize(),
	        resultOut);
    }
    else if (getQueryData().getQueryType() == URLLinkDetailQueryInfo.QueryType.INVERSE_BY_DOMAIN_DETAIL_QUERY) { 

    	InlinkingDomainInfo domainInfo = new InlinkingDomainInfo();
    	
    	domainInfo.setUrlCount(getQueryData().getInlinkDomainURLCount());
    	domainInfo.setUrlDataPos(getQueryData().getUrlDataOffset());
    	
    	readPaginatedInlinkingDomainDetail(masterIndex, localFileSystem, cacheDataFileName,domainInfo,
	        theClientRequest.getClientQueryInfo().getSortOrder(),
	        theClientRequest.getClientQueryInfo().getPaginationOffset(),
	        theClientRequest.getClientQueryInfo().getPageSize(),
	        resultOut);

    }
	      
    //LOG.info("Initiating getCachedResults Callback");
    callback.queryComplete(theClientRequest,resultOut);
	      
  }
}
