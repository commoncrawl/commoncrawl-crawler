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

import java.io.IOException;
import java.util.ArrayList;

import org.commoncrawl.service.queryserver.ShardIndexHostNameTuple;

/** 
 * 
 * Interface used to do a mapping from index(name) and shard id  to host name
 * 
 * @author rana
 *
 */

public interface ShardMapper {
	/**
	 * takes an index and returns a list of shard-index to host name mappings
	 */
	public ArrayList<ShardIndexHostNameTuple> mapShardIdsForIndex(String indexName)throws IOException;
}
