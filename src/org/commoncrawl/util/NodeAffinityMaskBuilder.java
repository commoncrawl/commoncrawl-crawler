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

import java.io.IOException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;

/**
 * 
 * @author rana
 *
 */
public class NodeAffinityMaskBuilder {

  private static final Log LOG = LogFactory.getLog(NodeAffinityMaskBuilder.class);

  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }    

  public static void setNodeAffinityMask(Configuration jobConfig,String mask) { 
    jobConfig.set("mapred.node.affinity.mask", mask);
  }

  public static String getNodeAffinityMask(Configuration jobConfig) { 
    return jobConfig.get("mapred.node.affinity.mask");
  }

  public static String buildNodeAffinityMask(FileSystem fileSystem,Path partFileDirectory,Map<Integer,String> optionalRootMapHint)throws IOException {
    return buildNodeAffinityMask(fileSystem, partFileDirectory, optionalRootMapHint,null);
  }
  
  public static String buildNodeAffinityMask(FileSystem fileSystem,Path partFileDirectory,Map<Integer,String> optionalRootMapHint,Set<String> excludedNodeList)throws IOException {
    return buildNodeAffinityMask(fileSystem, partFileDirectory, optionalRootMapHint, excludedNodeList, -1, false);
  }
  
  public static String buildNodeAffinityMask(FileSystem fileSystem,Path partFileDirectory,Map<Integer,String> optionalRootMapHint,Set<String> excludedNodeList,int maxReducersPerNode,boolean skipBalance)throws IOException {
  
    TreeMap<Integer,String> partitionToNodeMap = new TreeMap<Integer,String>();
    FileStatus paths[] = fileSystem.globStatus(new Path(partFileDirectory,"part-*"));

    if (paths.length == 0) { 
      throw new IOException("Invalid source Path:" + partFileDirectory);
    }
    
    Multimap<String,Integer> inverseMap = TreeMultimap.create();
    Map<Integer,List<String>> paritionToDesiredCandidateList = new TreeMap<Integer,List<String>>();

    // iterate paths 
    for (FileStatus path : paths) { 
      
      String currentFile = path.getPath().getName();
      int partitionNumber;
      try {
        if (currentFile.startsWith("part-r")) { 
          partitionNumber = NUMBER_FORMAT.parse(currentFile.substring("part-r-".length())).intValue();
        }
        else { 
          partitionNumber = NUMBER_FORMAT.parse(currentFile.substring("part-".length())).intValue();
        }
      } catch (ParseException e) {
        throw new IOException("Invalid Part Name Encountered:" + currentFile);
      }

      // get block locations 
      BlockLocation locations[] = fileSystem.getFileBlockLocations(path, 0, path.getLen());
      
      // if passed in root map is not null, then validate that all blocks for the current file reside on the desired node 
      if (optionalRootMapHint != null) {
        // the host all blocks should reside on 
        String desiredHost = optionalRootMapHint.get(partitionNumber);

        ArrayList<String> misplacedBlocks = new ArrayList<String>();
        // ok walk all blocks 
        for (BlockLocation location : locations) { 
          boolean found = false;
          for (String host : location.getHosts()) { 
            if (host.compareTo(desiredHost) == 0) { 
              found = true;
              break;
            }
          }
          if (!found) { 
            misplacedBlocks.add("Block At:" + location.getOffset() + " for File:" + path.getPath() + " did not contain desired location:" + desiredHost);
          }

        }
        // ok pass test at a certain threshold 
        if (misplacedBlocks.size() != 0 && ((float)misplacedBlocks.size() / (float)locations.length) > .50f) {
          LOG.error("Misplaced Blocks Exceed Threshold");
          for (String misplacedBlock : misplacedBlocks) { 
            LOG.error(misplacedBlock);
          }
          // TODO: SKIP THIS STEP FOR NOW ??? 
          //throw new IOException("Misplaced Blocks Exceed Threshold!");
        }
        partitionToNodeMap.put(partitionNumber, desiredHost);
      }
      else { 
        if (excludedNodeList != null) { 
          // LOG.info("Exclued Node List is:" + Lists.newArrayList(excludedNodeList).toString());
        }
        // ok ask file system for block locations
        TreeMap<String,Integer> nodeToBlockCount = new TreeMap<String,Integer>();

        for (BlockLocation location : locations) {
          for (String host : location.getHosts()) {
            if (excludedNodeList == null || !excludedNodeList.contains(host)) { 
              Integer nodeHitCount = nodeToBlockCount.get(host);
              if (nodeHitCount == null) { 
                nodeToBlockCount.put(host, 1);
              }
              else { 
                nodeToBlockCount.put(host, nodeHitCount.intValue() + 1);
              }
            }
          }
        }
        
        if (nodeToBlockCount.size() == 0) { 
          throw new IOException("No valid nodes found for partition number:" + path);
        }

        Map.Entry<String,Integer> entries[] = nodeToBlockCount.entrySet().toArray(new Map.Entry[0]);
        Arrays.sort(entries, new Comparator<Map.Entry<String,Integer> >() {

          @Override
          public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
            return o1.getValue().intValue() < o2.getValue().intValue() ? 1 : o1.getValue().intValue() == o2.getValue().intValue() ? 0 : -1; 
          } 
        });
        
        // build a list of nodes by priority ... 
        List<String> nodesByPriority = Lists.transform(Lists.newArrayList(entries),
            new Function<Map.Entry<String,Integer>,String>() {

              @Override
              public String apply(Entry<String, Integer> entry) {
                return entry.getKey();
              }
            });
        
        
        // stash it away ... 
        paritionToDesiredCandidateList.put(partitionNumber, nodesByPriority);
        //LOG.info("Mapping Partition:" + partitionNumber + " To Node:" + entries[0].getKey() + " BlockCount" + entries[0].getValue().intValue());
        partitionToNodeMap.put(partitionNumber, entries[0].getKey());
        // store the inverse mapping ... 
        inverseMap.put(entries[0].getKey(),partitionNumber);
      }
    }
    
    if (skipBalance) { 
      // walk partition map to make sure everything is assigned ...
      /*
      for (String node : inverseMap.keys()) { 
        if (inverseMap.get(node).size() > maxReducersPerNode) { 
          throw new IOException("Node:" + node + " has too many partitions! ("+inverseMap.get(node).size());
        }
      }
      */
    }
    
    // now if optional root map hint is null 
    if (optionalRootMapHint == null && !skipBalance) {
      // figure out if there is an imbalance
      int avgRegionsPerNode = (int) Math.floor((float) paths.length / (float) inverseMap.keySet().size());
      int maxRegionsPerNode = (int) Math.ceil((float) paths.length / (float) inverseMap.keySet().size());
      LOG.info("Attempting to ideally balance nodes. Avg paritions per node:" + avgRegionsPerNode);
      
      // two passes .. 
      for (int pass=0;pass<2;++pass) {
        LOG.info("Pass:" + pass);
        // iterate nodes ... 
        for (String node : ImmutableSet.copyOf(inverseMap.keySet())) { 
          // get paritions in map  
          Collection<Integer> paritions = ImmutableList.copyOf(inverseMap.get(node));
          // if parition count exceeds desired average ... 
          if (paritions.size() > maxRegionsPerNode) { 
            // first pass, assign based on preference 
            if (pass == 0) { 
              LOG.info("Node:" + node + " parition count:" + paritions.size() +" exceeds avg:" + avgRegionsPerNode);
              // walk partitions trying to find a node to discrard the parition to 
              for (int partition : paritions) {
                for (String candidate : paritionToDesiredCandidateList.get(partition)) { 
                  if (!candidate.equals(node)) { 
                    // see if this candidate has room ..
                    if (inverseMap.get(candidate).size() < avgRegionsPerNode) {
                      LOG.info("REASSIGNING parition:" + partition + " from Node:" + node + " to Node:" + candidate);
                      // found match reassign it ... 
                      inverseMap.remove(node,partition);
                      inverseMap.put(candidate, partition);
                      break;
                    }
                  }
                }
                // break out if reach our desired number of paritions for this node 
                if (inverseMap.get(node).size() == avgRegionsPerNode)
                  break;
              }
            }
            // second pass ... assign based on least loaded node ... 
            else {
              int desiredRelocations = paritions.size() - maxRegionsPerNode;
              LOG.info("Desired Relocation for node:" + node + ":" + desiredRelocations + " partitions:"+ paritions.size());
              for (int i=0;i<desiredRelocations;++i) { 
                String leastLoadedNode = null;
                int    leastLoadedNodePartitionCount = 0;
                
                for (String candidateNode : inverseMap.keySet()) { 
                  if (leastLoadedNode == null || inverseMap.get(candidateNode).size() < leastLoadedNodePartitionCount) { 
                    leastLoadedNode = candidateNode;
                    leastLoadedNodePartitionCount = inverseMap.get(candidateNode).size();
                  }
                }
                int bestPartition = -1;
                int bestParitionOffset = -1;
                
                for (int candidateParition : inverseMap.get(node)) {
                  int offset = 0;
                  for (String nodeCandidate : paritionToDesiredCandidateList.get(candidateParition)) { 
                    if (nodeCandidate.equals(leastLoadedNode)) { 
                      if (bestPartition == -1 || bestParitionOffset > offset) { 
                        bestPartition = candidateParition;
                        bestParitionOffset = offset;
                      }
                      break;
                    }
                    offset++;
                  }
                }
                if (bestPartition == -1) { 
                  bestPartition = Iterables.get(inverseMap.get(node), 0);
                }
                LOG.info("REASSIGNING parition:" + bestPartition + " from Node:" + node + " to Node:" + leastLoadedNode);
                // found match reassign it ... 
                inverseMap.remove(node,bestPartition);
                inverseMap.put(leastLoadedNode, bestPartition);
              }
            }
          }
        }
      }      
      LOG.info("Rebuilding parition to node map based on ideal balance");
      for (String node : inverseMap.keySet()) { 
        LOG.info("Node:" + node + " has:" + inverseMap.get(node).size() + " partitions:" +
            inverseMap.get(node).toString());
      }
      
      partitionToNodeMap.clear();
      for (Map.Entry<String, Integer> entry : inverseMap.entries()) { 
        partitionToNodeMap.put(entry.getValue(), entry.getKey());
      }
    }

    StringBuilder builder = new StringBuilder();
    int itemCount =0;
    for (Map.Entry<Integer,String> entry : partitionToNodeMap.entrySet()) { 
      if (itemCount++ != 0) 
        builder.append("\t");
      builder.append(entry.getKey().intValue()+"," +entry.getValue());
    }

    return builder.toString();
  }

  static public Map<Integer,String> parseAffinityMask(String mask) { 
    HashMap<Integer,String> mapOut =new HashMap<Integer,String>();
    String parts[] = mask.split("\t");
    for (String part : parts) { 
      String partitionAndHostName[] = part.split(",");
      if (partitionAndHostName.length == 2) { 
        mapOut.put(Integer.parseInt(partitionAndHostName[0]),partitionAndHostName[1]);
      }
    }
    return mapOut;
  }

  public static void main(String[] args) {
    LOG.info("Initializing Hadoop Config");

    Configuration conf = new Configuration();

    conf.addResource("nutch-default.xml");
    conf.addResource("nutch-site.xml");
    conf.addResource("mapred-site.xml");
    conf.addResource("hdfs-site.xml");
    conf.addResource("commoncrawl-default.xml");
    conf.addResource("commoncrawl-site.xml");

    CrawlEnvironment.setHadoopConfig(conf);
    CrawlEnvironment.setDefaultHadoopFSURI("hdfs://ccn01:9000/");

    try { 
      FileSystem fs = CrawlEnvironment.getDefaultFileSystem();

      String affinityMask = buildNodeAffinityMask(fs,new Path(args[0]),null,Sets.newHashSet("ccd001.commoncrawl.org"));
      Map<Integer,String> affinityMap = parseAffinityMask(affinityMask);

      for (Map.Entry<Integer,String> entry : affinityMap.entrySet()){  
        LOG.info("Parition:" + entry.getKey().intValue() + " Host:" + entry.getValue());
      }

    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
    }
  }
}
