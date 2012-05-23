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

package org.commoncrawl.service.listcrawler;


import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.commoncrawl.service.statscollector.Month;
import org.commoncrawl.service.statscollector.SerialDate;
import org.commoncrawl.util.Tuples.Pair;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

/** 
 * Utility code 
 * 
 * @author rana
 *
 */
public class ProxyPurgeUtils {

  public static void main(String[] args)throws IOException {

    Configuration conf = new Configuration();

    conf.addResource("nutch-default.xml");
    conf.addResource("nutch-site.xml");

    if (args[0].equalsIgnoreCase("list")) { 
      listCandidates(conf, caculateCutOffMillisecond());
    }
  }

  static long caculateCutOffMillisecond() { 
    // convert it to a serial date 
    SerialDate today = SerialDate.createInstance(new Date(System.currentTimeMillis()));
    // get date 4 months back 
    SerialDate cutOffDate = SerialDate.addDays(-120, today);
    // convert to month 
    Month cutOffMonth = new Month(cutOffDate.toDate());
    // get first cut off millisecond
    long cutOffMillisecond = cutOffMonth.getFirstMillisecond();
    
    return cutOffMillisecond;
  }
  
  static class Range extends Pair<Long,Long> implements  Comparable<Pair<Long,Long>> {

    public Range(Long e0, Long e1) {
      super(e0, e1);
    }

    @Override
    public int compareTo(Pair<Long, Long> o2) {
      return (e0 < o2.e0) ? -1 : (e0 > o2.e0) ? 1 : 0; 
    }

  }
  
  //1312941988792
  static void listCandidates(Configuration conf,final long cutOffTimeMillisecond)throws IOException { 
    FileSystem fs = FileSystem.get(conf);
    FileSystem localFS = FileSystem.getLocal(conf);
    
    final Multimap<Long,Range> rangeMap = TreeMultimap.create();
    FileStatus candidateDirs[] = fs.globStatus(new Path("crawl/proxy/cacheExport/processed/*"));
        
    for (FileStatus candidate : candidateDirs) { 
      String fileName = candidate.getPath().getName();
      // get scaled timestamp start 
      long timestampStart = Long.parseLong(fileName) * 1000000000;
      // ok see if exceeds our cutoff time 
      if (timestampStart < cutOffTimeMillisecond) { 
        FileStatus ranges[] = fs.globStatus(new Path(candidate.getPath(),"*"));
        for (FileStatus range : ranges) { 
          String rangeName = range.getPath().getName();
          long rangeStart = Long.parseLong(rangeName.substring(0,rangeName.indexOf("-")));
          long rangeEnd   = Long.parseLong(rangeName.substring(rangeName.indexOf("-") + 1));
          
          rangeMap.put(Long.parseLong(fileName), new Range(rangeStart,rangeEnd));
        }
      }
    }
    
    PathFilter cacheDataFilter = new PathFilter() {
      
      @Override
      public boolean accept(Path path) {
        if (path.getName().startsWith("cacheData-") || path.getName().startsWith("cacheIndex-")) { 
          long timestamp = Long.parseLong(path.getName().substring(path.getName().indexOf("-") + 1));
          long timestampPrefix = timestamp / 1000000000L;
          //System.out.println("timestamp:" + timestamp + " prefix:" + timestampPrefix);
          for (Range range : rangeMap.get(timestampPrefix)) { 
            if (timestamp >= range.e0 && timestamp <= range.e1) { 
              return true;
            }
          }
        }
        return false;
      }
    };
    
    PathFilter historyDataFilter = new PathFilter() {
      
      @Override
      public boolean accept(Path path) {
        if (path.getName().startsWith("historyData-") || path.getName().startsWith("historyBloomFilter-")) { 
          int indexOfDot = path.getName().indexOf(".");
          long timestamp = -1L;
          if (indexOfDot != -1) { 
            timestamp = Long.parseLong(path.getName().substring(path.getName().indexOf("-") + 1,indexOfDot)); 
          }
          else { 
            timestamp = Long.parseLong(path.getName().substring(path.getName().indexOf("-") + 1));
          }
          
          
          if (timestamp < cutOffTimeMillisecond) { 
            return true;
          }
        }
        return false;
      }
    };
    
    FileStatus purgeCandidates[] = fs.globStatus(new Path("crawl/proxy/cache/*"),cacheDataFilter);
    
    
    for (FileStatus candidate : purgeCandidates) { 
      System.out.println("Purging Candidate:" + candidate.getPath());
      fs.delete(candidate.getPath());
    }

    
    FileStatus localcacheDataPurgeCandidates[] = localFS.globStatus(new Path("/home/rana/ccprod/data/proxy_data/ccn01-Prod/*"),cacheDataFilter);
    
    
    for (FileStatus candidate : localcacheDataPurgeCandidates) { 
      System.out.println("Purging Candidate:" + candidate.getPath());
      localFS.delete(candidate.getPath());
    }
    
    // now delete bloom filter data
    FileStatus historyPurgeCandidates[] = fs.globStatus(new Path("crawl/proxy/history/*"),historyDataFilter);
    
    for (FileStatus candidate : historyPurgeCandidates) { 
      System.out.println("Purging Candidate:" + candidate.getPath());
      fs.delete(candidate.getPath(),true);
    }
    
    // now delete bloom filter data
    FileStatus localHistoryPurgeCandidates[] = localFS.globStatus(new Path("/home/rana/ccprod/data/proxy_data/ccn01-Prod/historyData/*"),historyDataFilter);
    
    for (FileStatus candidate : historyPurgeCandidates) { 
      System.out.println("Purging Candidate:" + candidate.getPath());
      fs.delete(candidate.getPath(),true);
    }

    for (FileStatus candidate : localHistoryPurgeCandidates) { 
      System.out.println("Purging Candidate:" + candidate.getPath());
      localFS.delete(candidate.getPath(),true);
    }
    
  }    
}
