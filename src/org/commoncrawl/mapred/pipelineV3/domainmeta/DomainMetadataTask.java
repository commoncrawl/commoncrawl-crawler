/**
 * Copyright 2012 - CommonCrawl Foundation
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

package org.commoncrawl.mapred.pipelineV3.domainmeta;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
import org.commoncrawl.mapred.pipelineV3.crawllistgen.CrawlListGeneratorTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.blogs.feedurlid.FeedUrlIdStep;
import org.commoncrawl.mapred.pipelineV3.domainmeta.blogs.postfrequency.GenPostFrequencyStep;
import org.commoncrawl.mapred.pipelineV3.domainmeta.crawlstats.CrawlStatsCollectorTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.fuzzydedupe.CrossDomainDupes;
import org.commoncrawl.mapred.pipelineV3.domainmeta.fuzzydedupe.FindBadIPsFromDupes;
import org.commoncrawl.mapred.pipelineV3.domainmeta.fuzzydedupe.FuzzyDedupeStep1;
import org.commoncrawl.mapred.pipelineV3.domainmeta.fuzzydedupe.FuzzyDedupeStep2;
import org.commoncrawl.mapred.pipelineV3.domainmeta.fuzzydedupe.HostBlacklistByDupesStep;
import org.commoncrawl.mapred.pipelineV3.domainmeta.iptohost.DomainIPCollectorStep;
import org.commoncrawl.mapred.pipelineV3.domainmeta.iptohost.IPAddressToHostMappingStep;
import org.commoncrawl.mapred.pipelineV3.domainmeta.iptohost.QuantcastIPListStep;
import org.commoncrawl.mapred.pipelineV3.domainmeta.linkstats.CountInLinksStep;
import org.commoncrawl.mapred.pipelineV3.domainmeta.quantcast.ImportQuantcastStep;
import org.commoncrawl.mapred.pipelineV3.domainmeta.rank.RankTask;
import org.commoncrawl.mapred.pipelineV3.domainmeta.subdomaincounts.SubDomainCountsStep;
import org.commoncrawl.mapred.pipelineV3.domainmeta.subdomaincounts.SubDomainToQuantcastJoinStep;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

/**
 * 
 * @author rana
 *
 */
public class DomainMetadataTask extends CrawlPipelineTask {

  private List<Integer> _partitionList = null;
  private String        _runMode = "DEFAULT";
  private Properties    _properties = null;
  Configuration _conf = new Configuration();
  private static final Log LOG = LogFactory.getLog(DomainMetadataTask.class);

  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();

  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }
  
  public static final String OUTPUT_DIR_NAME = "domainMetadata"; 

  static final Options options;

  static {
    options = new Options();
    options.addOption(OptionBuilder.withArgName("dbroot").hasArg(true).isRequired().withDescription("Database Root Path").create("dbroot"));
    options.addOption("partitions", true, "comma separated list of partitions");
    options.addOption("rebuild", false, "rebuild all the outputs");
    options.addOption(OptionBuilder.withArgName("fsuri").hasArg(true).withDescription("set file system uri").create("fsuri"));
    options.addOption(OptionBuilder.withArgName("tempdir").hasArg(true).withDescription("Hadoop Temp Dir").create("tempdir"));
    options.addOption(OptionBuilder.withArgName("mode").hasArg(true).withDescription("Run Mode").create("mode"));
    options.addOption(OptionBuilder.withArgName( "property=value" )
        .hasArgs(2)
        .withValueSeparator()
        .withDescription( "use value for given property" )
        .create( "D" ));
    
  }
  
  
  public static void main(String[] args) throws Exception {
    DomainMetadataTask task = new DomainMetadataTask();
    task.run(args);
  }

  public DomainMetadataTask() throws IOException {
    super(new Configuration(), "Domain Metdata Task",OUTPUT_DIR_NAME);
  }
  
  public DomainMetadataTask(String alternateTaskDescription) throws IOException {
    super(new Configuration(), alternateTaskDescription,OUTPUT_DIR_NAME);
  }
  

  public DomainMetadataTask(CrawlPipelineTask parentTask) throws IOException {
    super(parentTask, "Domain Metdata Task",OUTPUT_DIR_NAME);
  }
  

  public List<Path> getMergeDBDataPaths() throws IOException {
    return getParitionPaths(getTaskIdentityBasePath(), getLatestDatabaseTimestamp(), "part-");
  }

  public List<Path> getParitionPaths(Path basePath, long databaseId, String prefix) throws IOException {
    ArrayList<Path> paths = new ArrayList<Path>();
    for (FileStatus fileStatus : getFileSystem().globStatus(
        new Path(basePath, Long.toString(databaseId) + "/" + prefix + "*"))) {
      paths.add(fileStatus.getPath());
    }
    return paths;
  }

  public final List<Integer> getPartitionList() {
    return _partitionList;
  }
  
  public final Properties getProperties() { 
    return _properties;
  }
  
  public final Configuration getConf(){ 
    return _conf;
  }

  public List<Path> getRedirectDataPaths() throws IOException {
    return getParitionPaths(getTaskIdentityBasePath(), getLatestDatabaseTimestamp(), "redirect-");
  }

  public List<Path> getRestrictedMergeDBDataPaths() throws IOException {
    if (_partitionList == null || _partitionList.size() == 0) { 
      throw new IOException("Invalid or NULL Partition List!");
    }
    return getRestrictedParitionPaths(_partitionList, getTaskIdentityBasePath(), getLatestDatabaseTimestamp(), "part-");
  }

  public List<Path> getRestrictedRedirectPaths() throws IOException {
    if (_partitionList == null || _partitionList.size() == 0) { 
      throw new IOException("Invalid or NULL Partition List!");
    }
    return getRestrictedParitionPaths(_partitionList, getTaskIdentityBasePath(), getLatestDatabaseTimestamp(),
        "redirect-");
  }


  Pattern rangePartitionPattern = Pattern.compile("([0-9]*)-([0-9]*)");
  @Override
  protected void parseArgs() throws IOException {

    CommandLineParser parser = new GnuParser();
    try {
      // parse the command line arguments
      CommandLine line = parser.parse(options, _args,false);

      // default to single partition - partition zero
      List<Integer> partitions = ImmutableList.of(0);

      if (line.hasOption("partitions")) {
        
        ArrayList<Integer> restrictedSet = Lists.newArrayList();
        for (String partitionSpec : line.getOptionValue("partitions").split(",")) { 
          Matcher rangeMatcher = rangePartitionPattern.matcher(partitionSpec);
          if (rangeMatcher.matches()) { 
            int rangeStart = Integer.parseInt(rangeMatcher.group(1));
            int rangeEnd   = Integer.parseInt(rangeMatcher.group(2));
            if (rangeEnd < rangeStart) { 
              throw new IOException("Invalid Range!");
            }
            else { 
              for (int i=rangeStart;i<rangeEnd;++i) { 
                restrictedSet.add(i);
              }
            }
          }
          else { 
            int partition = Integer.parseInt(partitionSpec);
            restrictedSet.add(partition);
          }
          partitions = restrictedSet;
        }
        if (partitions.size() == 0) {
          throw new IOException("One Parition Required At a Minimum!");
        }
        _partitionList = partitions;

      }

      if (line.hasOption("rebuild")) {
        LOG.info("Rebuild Option Specified. Deleting Outputs");
        for (CrawlPipelineStep step : getSteps()) {
          LOG.info("Deleting Output Dir:" + step.getOutputDir() + " for Step:" + step.getName());
          getFileSystem().delete(step.getOutputDir(), true);
        }
      }
      if (line.hasOption("dbroot")) {
        setTaskIdentityBasePath(new Path(line.getOptionValue("dbroot")));
        setRootOutputDir(getTaskIdentityBasePath());
      }
      
      if (line.hasOption("fsuri")) { 
        CrawlEnvironment.setDefaultHadoopFSURI(line.getOptionValue("fsuri"));
      }
      
      if (line.hasOption("tempdir")) {
        System.out.println("tempdir is:"+ line.getOptionValue("tempdir"));
        System.out.println("HadoopConf is:"+ CrawlEnvironment.getHadoopConfig());
        
        CrawlEnvironment.getHadoopConfig().set("mapred.temp.dir", line.getOptionValue("tempdir"));
      }
      if (line.hasOption("mode")) { 
        _runMode = line.getOptionValue("mode");
      }
      
      _properties = line.getOptionProperties("D");
      if (_properties != null) { 
        for (Map.Entry<Object,Object> property : _properties.entrySet()) { 
          _conf.set(property.getKey().toString(), property.getValue().toString());
        }
      }
      
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new IOException(e);
    }
  }

  @Override
  protected boolean promoteFinalStepOutput() {
    return false;
  }
  
  private static List<Path> getRestrictedParitionPaths(List<Integer> partitions, Path basePath, long databaseId,
      String prefix) throws IOException {
    ArrayList<Path> paths = new ArrayList<Path>();
    for (int partition : partitions) {
      paths.add(new Path(basePath,Long.toString(databaseId) + "/" + prefix
          + NUMBER_FORMAT.format(partition)));
    }
    return paths;
  }

  
  @Override
  public Log getLogger() {
    return LOG;
  }

  @Override
  public void initTask(String[] args)throws IOException {
    super.initTask(args);
    if (_runMode.equalsIgnoreCase("DEFAULT")) { 
      //addStep(new ImportQuantcastStep(this));
      //addStep(new SubDomainCountsStep(this));
      //addStep(new CountInLinksStep(this));
      //addStep(new IPAddressToHostMappingStep(this));
      //addStep(new QuantcastIPListStep(this));
      //addStep(new DomainIPCollectorStep(this));
      //addStep(new FuzzyDedupeStep1(this));
      //addStep(new FuzzyDedupeStep2(this));
      //addStep(new CrossDomainDupes(this));
      //addStep(new FindBadIPsFromDupes(this));
      //addStep(new HostBlacklistByDupesStep(this));
      //addStep(new SubDomainToQuantcastJoinStep(this));
      //addStep(new RankTask(this));
      //addStep(new GenPostFrequencyStep(this));
      //addStep(new FeedUrlIdStep(this));
      //addStep(new CrawlStatsCollectorTask(this));
    }
    else if (_runMode.equalsIgnoreCase("rank")) { 
      addStep(new RankTask(this));
    }
    else if (_runMode.equalsIgnoreCase("dedupe")) { 
      addStep(new FuzzyDedupeStep1(this));
      addStep(new FuzzyDedupeStep2(this));
      addStep(new CrossDomainDupes(this));
    }
    else if (_runMode.equalsIgnoreCase("stats")) {
      addStep(new RankTask(this));
      addStep(new CrawlStatsCollectorTask(this));
    }
    else if (_runMode.equalsIgnoreCase("list")) {
      addStep(new CrawlStatsCollectorTask(this));
      addStep(new CrawlListGeneratorTask(this));
    }
    
    

  }

}
