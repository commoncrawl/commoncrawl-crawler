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
package org.commoncrawl.service.pagerank.slave;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.commoncrawl.async.CallbackWithResult;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.service.crawler.filters.SuperDomainFilter;
import org.commoncrawl.service.pagerank.Constants;
import org.commoncrawl.service.pagerank.IterationInfo;
import org.commoncrawl.service.pagerank.PRMasterState;
import org.commoncrawl.service.pagerank.PageRankJobConfig;
import org.commoncrawl.service.pagerank.slave.PageRankUtils.PRValueMap;
import org.commoncrawl.util.CCStringUtils;

public class BeginPageRankTask extends PageRankTask<BeginPageRankTask.BeginPageRankTaskResult> {

  private static final Log LOG = LogFactory.getLog(BeginPageRankTask.class);
  
  private PageRankJobConfig _config;
  private int _prMasterStatus;
  private boolean _isCancelled = false;
  
  public BeginPageRankTask(PageRankJobConfig jobConfig,int serverStatus, PageRankSlaveServer server,CallbackWithResult<BeginPageRankTaskResult> completionCallback) {
    super(server,BeginPageRankTask.BeginPageRankTaskResult.class, completionCallback);
    
    _config = jobConfig;
    _prMasterStatus = serverStatus;
  }

  public static class BeginPageRankTaskResult extends PageRankTaskResult { 
    public PRValueMap _valueMap = null;
  }

  @Override
  protected void cancelTask() {
  	_isCancelled = true;
  }

  @Override
  protected BeginPageRankTaskResult runTask() throws IOException {
    
    BeginPageRankTaskResult result = new BeginPageRankTaskResult();

    try { 
    
      // create job local directory if necessary 
      _server.getActiveJobLocalPath().mkdirs();
      
      FileSystem fileSystem = _server.getFileSystem();
      
      // figure out if we are going to load values from base location or job config (based on iteration number)
      
      Path rangeRemotePath = new Path(_config.getInputValuesPath(),PageRankUtils.makeUniqueFileName(Constants.PR_RANGE_FILE_PREFIX,0,_server.getNodeIndex()));
      Path rangeLocalPath  = PageRankUtils.makeRangeFilePath(_server.getActiveJobLocalPath(), _server.getNodeIndex());
      
      Path idsRemotePath = new Path(_config.getInputValuesPath(),PageRankUtils.makeUniqueFileName(Constants.PR_IDS_FILE_PREFIX,0,_server.getNodeIndex()));
      Path idsLocalPath = new Path(PageRankUtils.makeIdsFilePath(_server.getActiveJobLocalPath(), _server.getNodeIndex()).getAbsolutePath());
      
      Path outlinksFileRemotePath = new Path(_config.getOutlinksDataPath(),PageRankUtils.makeUniqueFileName(Constants.PR_OUTLINKS_FILE_PREFIX,0,_server.getNodeIndex()));
      Path outlinksFileLocalPath  = new Path(new File(_server.getActiveJobLocalPath(),PageRankUtils.makeUniqueFileName(Constants.PR_OUTLINKS_FILE_PREFIX,0,_server.getNodeIndex())).getAbsolutePath());
      
      Path valuesRemotePath = null;
      
      if (_config.getIterationNumber() == 0) {
        // fetch values from base values path 
        valuesRemotePath = new Path(_config.getInputValuesPath(),PageRankUtils.makeUniqueFileName(Constants.PR_VALUE_FILE_PREFIX,0,_server.getNodeIndex()));
        LOG.info("Iteration Number is 0. Using Values File:" + valuesRemotePath);
      }
      else { 
        // fetch latest values from job path (hdfs) based on last iteration number ...
        valuesRemotePath = new Path(_config.getJobWorkPath(),PageRankUtils.makeUniqueFileName(Constants.PR_VALUE_FILE_PREFIX,_config.getIterationNumber() - 1,_server.getNodeIndex()));
        LOG.info("Iteration Number is:" + _config.getIterationNumber() + ". Using Values File:" + valuesRemotePath);
      }
      
      /*
      Path localValuesFilePath = new Path(new File(_server.getActiveJobLocalPath(),PageRankUtils.makeUniqueFileName(Constants.PR_VALUE_FILE_PREFIX,_config.getIterationNumber(),_server.getNodeIndex())).getAbsolutePath());
      
      
      // copy the files to the local directory ...
      FileStatus rangeFileStatus = fileSystem.getFileStatus(rangeRemotePath);
      File       rangeLocalFile  = new File(rangeLocalPath.toString());
      
      if (rangeLocalFile.exists() == false || rangeLocalFile.length() != rangeFileStatus.getLen()) { 
        rangeLocalFile.delete();
        LOG.info("Copying Range File:" + rangeRemotePath + " to " + rangeLocalPath);
        fileSystem.copyToLocalFile(rangeRemotePath, rangeLocalPath);
      }
      else { 
        LOG.info("Skipping Copy of Range File:" + rangeRemotePath + " to " + rangeLocalPath);
      }
      
      FileStatus idFileStatus = fileSystem.getFileStatus(idsRemotePath);
      File       idLocalFile  = new File(idsLocalPath.toString());

      if (idLocalFile.exists() == false || idLocalFile.length() != idFileStatus.getLen()) { 
        LOG.info("Copying Ids File:" + idsRemotePath + " to " + idsLocalPath);
        fileSystem.copyToLocalFile(idsRemotePath, idsLocalPath);
      }
      else { 
        LOG.info("Skipping Copying Ids File:" + idsRemotePath + " to " + idsLocalPath);
      }
			*/
      
      FileStatus outlinksFileStatus = fileSystem.getFileStatus(outlinksFileRemotePath);
      File       outlinksLocalFile  = new File(outlinksFileLocalPath.toString());
      
      if (outlinksLocalFile.exists() == false || outlinksLocalFile.length() != outlinksFileStatus.getLen()) { 
        LOG.info("Copying outlinks File:" + outlinksFileRemotePath + " to " + outlinksLocalFile);
        fileSystem.copyToLocalFile(outlinksFileRemotePath,outlinksFileLocalPath);
      }
      else { 
        LOG.info("Skipping Copying outlinks File:" + outlinksFileRemotePath + " to " + outlinksLocalFile);
      }

      /*
      FileStatus valuesFileStatus = fileSystem.getFileStatus(valuesRemotePath);
      File       valuesLocalFile  = new File(localValuesFilePath.toString());

      if (valuesLocalFile.exists() == false || valuesLocalFile.length() != valuesFileStatus.getLen()) { 
        LOG.info("Copying values File:" + valuesRemotePath + " to " + valuesLocalFile);
        fileSystem.copyToLocalFile(valuesRemotePath,localValuesFilePath);
      }
      else { 
        LOG.info("Skipping Copying values File:" + valuesRemotePath + " to " + valuesLocalFile);
      }
      */
      // now load the values map ...
      result._valueMap = new PageRankUtils.PRValueMap();
      //result._valueMap.open(fileSystem,valuesRemotePath, PageRankUtils.makeRangeFilePath(_server.getActiveJobLocalPath(), _server.getNodeIndex()));
      
      boolean valuesFileMissing = false;
      if (_server.getActiveJobConfig().getIterationNumber() != 0 && !_server.getFileSystem().exists(valuesRemotePath)) { 
      	LOG.error("Values File Missing for Iteration:" + _server.getActiveJobConfig().getIterationNumber());
 
      	valuesFileMissing = true;
      	// revert to iteration zero values file ... 
        valuesRemotePath = new Path(_config.getInputValuesPath(),PageRankUtils.makeUniqueFileName(Constants.PR_VALUE_FILE_PREFIX,0,_server.getNodeIndex()));
      }
      result._valueMap.open(fileSystem,valuesRemotePath, rangeRemotePath);
      
      // ok now if iteration number is non-zero,
      // recalculate rank from previous iteration's data ...
      
      if (_config.getIterationNumber() != 0 && valuesFileMissing) { 

      	// load data from previous iteration ... 
      	int iterationNumberToLoadFrom = _config.getIterationNumber() - 1;
      	// ok figure out what state master is in 
      	if (_prMasterStatus == PRMasterState.ServerStatus.ITERATING_CALCULATING) { 
      		// use current iteration number data 
      		iterationNumberToLoadFrom = 0;
      		LOG.info("Master is in CALCULATION PHASE. SKIP LOAD OF VALUEMAP");
      	}
      	// in the distribution case ... check to see if checkpoint file is present ... 
      	else if (_prMasterStatus == PRMasterState.ServerStatus.ITERATING_DISTRIBUTING) {
      		
        	Path checkpointFilePath = PageRankUtils.getCheckpointFilePath(new Path(_server.getActiveJobConfig().getJobWorkPath()),
        			IterationInfo.Phase.DISTRIBUTE, 
        			_server.getActiveJobConfig().getIterationNumber(), 
        			_server.getNodeIndex());
        	
        	// ok checkpoint file exists, use current iteration number to load data 
        	if (_server.getFileSystem().exists(checkpointFilePath)) {
        		LOG.info("Checkpoint file exists. SKIP LOAD OF VALUEMAP");
        		iterationNumberToLoadFrom = 0;
        	}
      	}
      	
      	if (iterationNumberToLoadFrom != 0) { 
	      	// load super domain filter
		    	LOG.info("Initializing SuperDomain Filter");
		    	SuperDomainFilter superDomainFilter = new SuperDomainFilter();
		    	superDomainFilter.loadFromPath(_server.getDirectoryServiceAddress(), CrawlEnvironment.ROOT_SUPER_DOMAIN_PATH, false);
		    	
		      LOG.info("Starting Calculate Task to load value map - Using Iteration Number:" + iterationNumberToLoadFrom);
		      
		      // first zero value map values ... 
		      result._valueMap.zeroValues();
		      
		        PageRankUtils.calculateRank(
		               _server.getConfig(),
		               _server.getFileSystem(),
		        		result._valueMap,
		        		_server.getActiveJobLocalPath(),
		            _server.getActiveJobConfig().getJobWorkPath(),
		            _server.getNodeIndex(),
		            _server.getBaseConfig().getSlaveCount(),
		            iterationNumberToLoadFrom,
		            superDomainFilter,
		            new PageRankUtils.ProgressAndCancelCheckCallback() {
									
		        			
									@Override
									public boolean updateProgress(final float percentComplete) {
										_percentComplete = percentComplete;
										return BeginPageRankTask.this.isCancelled();
									}
								});
      	}
      }
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      throw e;
    }
    return result;
  }
  
  @Override
  public String getDescription() {
    return "Begin PageRank Task";
  }
  
  @Override
  public synchronized boolean isCancelled() {
  	return _isCancelled;
  }

  
}
