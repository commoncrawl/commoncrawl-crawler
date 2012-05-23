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

import java.util.Vector;

/**
 * 
 * @author rana
 *
 * @param <KeyType>
 * @param <ValueType>
 */
public class QueryResult<KeyType,ValueType> {
  
  Vector<QueryResultRecord<KeyType,ValueType>> results = new Vector<QueryResultRecord<KeyType,ValueType>>(); 
  int                       pageNumber = 0;                         
  long                      totalRecordCount = 0;
  
  public QueryResult() { 
    results = new Vector<QueryResultRecord<KeyType,ValueType>>();
  }
  
  public int  getPageNumber() { return pageNumber; }
  public void setPageNumber(int pageNumber) { this.pageNumber = pageNumber; }
  
  public long  getTotalRecordCount() { return totalRecordCount; }
  public void  setTotalRecordCount(long totalRecordCount) { this.totalRecordCount = totalRecordCount; }

  public int getResultCount() { return results.size(); }
  public QueryResultRecord<KeyType,ValueType> getResultAt(int index) { return results.get(index); }
  public Vector<QueryResultRecord<KeyType,ValueType>> getResults() { return results; }
  
}
