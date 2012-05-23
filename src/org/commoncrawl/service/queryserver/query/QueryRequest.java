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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.rpc.base.shared.RPCStruct;
import org.commoncrawl.service.queryserver.ClientQueryInfo;
import org.commoncrawl.service.queryserver.QueryStatus;

/**
 * 
 * @author rana
 *
 * @param <DataType>
 * @param <KeyType>
 * @param <ValueType>
 */
public class QueryRequest<DataType extends RPCStruct,KeyType,ValueType> { 

  private static final Log LOG = LogFactory.getLog(QueryRequest.class);

  
  private Query<DataType,KeyType,ValueType>            _querySource;
  private ClientQueryInfo _clientQueryObj;
  private QueryStatus _queryStatus;
  private QueryCompletionCallback<DataType,KeyType,ValueType> _completionCallback;
  private File _localCacheDirectory;
  
  public enum RunState { 
    IDLE,
    RUNNING_REMOTE,
    RUNNING_LOCAL,
    RUNNING_CACHE
  }
  
  private RunState _runState 		 = RunState.IDLE;
  private RunState _lastRunningState = RunState.IDLE;
  
  public QueryRequest(Query<DataType,KeyType,ValueType> query,ClientQueryInfo ClientQueryInfo,File localCacheDirectory,QueryCompletionCallback<DataType,KeyType,ValueType> callback) { 
    _querySource = query;
    _clientQueryObj = ClientQueryInfo;
    _completionCallback = callback;
    _queryStatus = new QueryStatus();
    _queryStatus.setQueryId(ClientQueryInfo.getClientQueryId());
    _queryStatus.setStatus(QueryStatus.Status.PENDING);
    _localCacheDirectory = localCacheDirectory;
  }
  
  public Query<DataType,KeyType,ValueType>           getSourceQuery() { return _querySource; }
  public ClientQueryInfo getClientQueryInfo() { return _clientQueryObj; }
  public QueryStatus getQueryStatus() { return _queryStatus; }
  public QueryCompletionCallback<DataType,KeyType,ValueType> getCompletionCallback() { return _completionCallback; }
  public File getLocalCacheDirectory() { return _localCacheDirectory; }
  public boolean setRunState(RunState runState) { 
  	if (runState != RunState.IDLE) { 
  		if (runState.ordinal() > _lastRunningState.ordinal()){ 
  			_lastRunningState = runState;
  		}
  		else { 
  			return false;
  		}
  	}
  	_runState = runState;
  	return true;
  }
  public RunState getRunState() { return _runState; }
  
  
  
}
