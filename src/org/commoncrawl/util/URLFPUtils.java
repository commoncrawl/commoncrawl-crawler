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

package org.commoncrawl.util;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.protocol.URLFPV2;

/**
 * 
 * @author rana
 *
 */
public class URLFPUtils {

	/**
	 * return partition for given fingerprint using the NUM_SHARDS constant 
	 * 
	 * @param fingeprint
	 * @return
	 */
	public static int getPartitionGivenFP(URLFPV2 fingeprint) { 
		return (fingeprint.hashCode() & Integer.MAX_VALUE) % CrawlEnvironment.NUM_DB_SHARDS;
	}
	
	
	/**
	 * partition by subdomain 
	 * 
	 * @author rana
	 *
	 */
	public static class SubDomainPartitioner implements Partitioner<URLFPV2, Writable> {

		@Override
    public int getPartition(URLFPV2 key, Writable value,int numPartitions) {
			return (((int)key.getDomainHash()) & Integer.MAX_VALUE) % numPartitions;
    }

		@Override
    public void configure(JobConf job) {
    
    } 
	}

	/**
	 * partition by root domain 
	 * 
	 * @author rana
	 *
	 */
	
	public static class RootDomainPartitioner implements Partitioner<URLFPV2, Writable> {

		@Override
    public int getPartition(URLFPV2 key, Writable value,int numPartitions) {
			return (((int)key.getRootDomainHash()) & Integer.MAX_VALUE) % numPartitions;
    }

		@Override
    public void configure(JobConf job) {
    
    } 
	}
}
