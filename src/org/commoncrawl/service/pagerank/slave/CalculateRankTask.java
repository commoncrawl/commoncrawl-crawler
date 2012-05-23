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

import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.commoncrawl.async.CallbackWithResult;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.service.crawler.filters.SuperDomainFilter;
import org.commoncrawl.service.pagerank.Constants;
import org.commoncrawl.service.pagerank.IterationInfo;
import org.commoncrawl.util.CCStringUtils;

/**
 * 
 * @author rana
 *
 */
public class CalculateRankTask extends PageRankTask<CalculateRankTask.CalculateRankTaskResult>{

  private static final Log LOG = LogFactory.getLog(CalculateRankTask.class);

  public CalculateRankTask(PageRankSlaveServer server,CallbackWithResult<CalculateRankTaskResult> completionCallback) {
    super(server,CalculateRankTask.CalculateRankTaskResult.class, completionCallback);
    
  }

  public static class CalculateRankTaskResult extends PageRankTaskResult { 
    public boolean done = false;
    
    public CalculateRankTaskResult() { 
      
    }
    
    public CalculateRankTaskResult(boolean done) { 
      this.done = done;
    }
    
    public boolean isDone() { return done; }

  }
  

  @Override
  protected void cancelTask() {
    
  }

  @Override
  public String getDescription() {
    return "Calculate Rank Task";
  }

  @Override
  protected CalculateRankTaskResult runTask() throws IOException {
    
  	// construct checkpoint filename 
  	Path checkpointFilePath = PageRankUtils.getCheckpointFilePath(new Path(_server.getActiveJobConfig().getJobWorkPath()),
  			IterationInfo.Phase.CALCULATE, 
  			_server.getActiveJobConfig().getIterationNumber(), 
  			_server.getNodeIndex());
  	
  	// check to see if checkpoint file exists ... 
  	if (_server.getFileSystem().exists(checkpointFilePath)) { 
  		LOG.info("Checkpoint File Found.Skipping Task.");
      return new CalculateRankTaskResult(true);
  	}
  	else { 
	  	// load super domain filter
	  	LOG.info("Initializing SuperDomain Filter");
	  	SuperDomainFilter superDomainFilter = new SuperDomainFilter();
	  	superDomainFilter.loadFromPath(_server.getDirectoryServiceAddress(), CrawlEnvironment.ROOT_SUPER_DOMAIN_PATH, false);
	  	
	    LOG.info("Starting Calculate Task - Iteration Number:" + _server.getActiveJobConfig().getIterationNumber());
	    // take value map and distribute it
	    if (_server.getValueMap() == null) {
	      throw new IOException("Value Map NULL! Operation Failed");
	    }
	    
	    // first zero value map values ... 
	    _server.getValueMap().zeroValues();
	    
	    try { 
	      PageRankUtils.calculateRank(
	          _server.getConfig(),
	          _server.getFileSystem(),
	          _server.getValueMap(),_server.getActiveJobLocalPath(),
	          _server.getActiveJobConfig().getJobWorkPath(),
	          _server.getNodeIndex(),
	          _server.getBaseConfig().getSlaveCount(),
	          _server.getActiveJobConfig().getIterationNumber(),
	          superDomainFilter,
	          new PageRankUtils.ProgressAndCancelCheckCallback() {
							
							@Override
							public boolean updateProgress(float percentComplete) {
								synchronized (CalculateRankTask.this) { 
									_percentComplete = percentComplete;
								}
								return false;
							}
						});
	      
	      // and write out value map back to disk ...
	      Path valuesPath = new Path(_server.getActiveJobConfig().getJobWorkPath(),PageRankUtils.makeUniqueFileName(Constants.PR_VALUE_FILE_PREFIX,
	      		_server.getActiveJobConfig().getIterationNumber(),_server.getNodeIndex()));
	      
	      LOG.info("Serializing Values to Path:" + valuesPath + " for Iteration:" + _server.getActiveJobConfig().getIterationNumber());
	      
	      _server.getFileSystem().delete(valuesPath);
	      
	      OutputStream valueStream = null;
	      // create new stream .. 
	    	valueStream = _server.getFileSystem().create(valuesPath);
	      
	      try { 
	      	_server.getValueMap().flush(valueStream);
	      }
	      catch (IOException e) { 
	        LOG.error("Failed to Flush Value Map to OutputStream:" + valuesPath);
	        _server.getFileSystem().delete(valuesPath,false);
	        throw e;
	      }
	      finally { 
	      	if (valueStream != null)
	      		valueStream.close();
	      }
	      
	    	// construct checkpoint filename 	    	
	  		LOG.info("Creating Checkpoint File:" + checkpointFilePath);
	      return new CalculateRankTaskResult(_server.getFileSystem().createNewFile(checkpointFilePath));
	      
	    }
	    catch (IOException e) { 
	      // calculate failed
	      LOG.error("Calculate Rank Failed with Error:" + CCStringUtils.stringifyException(e));
	      throw e;
	    }
  	}
  }
}
