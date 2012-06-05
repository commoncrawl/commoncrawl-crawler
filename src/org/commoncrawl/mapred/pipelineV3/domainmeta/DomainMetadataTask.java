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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineStep;
import org.commoncrawl.mapred.pipelineV3.CrawlPipelineTask;
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

/**
 * 
 * @author rana
 *
 */
public class DomainMetadataTask extends CrawlPipelineTask {

  private List<Integer> _partitionList = null;

  private static final Log LOG = LogFactory.getLog(DomainMetadataTask.class);

  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();

  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }

  static final Options options;

  static {
    options = new Options();
    options.addOption("partitions", true, "comma separated list of partitions");
    options.addOption("rebuild", false, "rebuild all the outputs");
  }

  private static List<Path> getRestrictedParitionPaths(List<Integer> partitions, Path basePath, long databaseId,
      String prefix) throws IOException {
    ArrayList<Path> paths = new ArrayList<Path>();
    for (int partition : partitions) {
      paths.add(new Path("crawl/ec2Import/mergedDB/" + Long.toString(databaseId) + "/" + prefix
          + NUMBER_FORMAT.format(partition)));
    }
    return paths;
  }

  public static void main(String[] args) throws Exception {
    DomainMetadataTask task = new DomainMetadataTask();
    task.run(args);
  }

  public DomainMetadataTask() throws IOException {

    super(null, new Configuration(), "Domain Metdata Task");

    init();
  }

  public DomainMetadataTask(CrawlPipelineTask parentTask) throws IOException {
    super(parentTask, parentTask.getConf(), "Domain Metdata Task");

    init();
  }

  @Override
  public Log getLogger() {
    return LOG;
  }

  public List<Path> getMergeDBDataPaths() throws IOException {
    return getParitionPaths(getTaskIdentityBasePath(), getLatestDatabaseTimestamp(), "part-");
  }

  private List<Path> getParitionPaths(Path basePath, long databaseId, String prefix) throws IOException {
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

  public List<Path> getRedirectDataPaths() throws IOException {
    return getParitionPaths(getTaskIdentityBasePath(), getLatestDatabaseTimestamp(), "redirect-");
  }

  public List<Path> getRestrictedMergeDBDataPaths() throws IOException {
    return getRestrictedParitionPaths(_partitionList, getTaskIdentityBasePath(), getLatestDatabaseTimestamp(), "part-");
  }

  public List<Path> getRestrictedRedirectPaths() throws IOException {
    return getRestrictedParitionPaths(_partitionList, getTaskIdentityBasePath(), getLatestDatabaseTimestamp(),
        "redirect-");
  }

  @Override
  public Path getTaskIdentityBasePath() throws IOException {
    return new Path("crawl/ec2Import/mergedDB");
  }

  @Override
  public Path getTaskOutputBaseDir() {
    return new Path("crawl/ec2Import/domainMetadata");
  }

  private void init() throws IOException {
    addStep(new ImportQuantcastStep(this));
    addStep(new SubDomainCountsStep(this));
    addStep(new CountInLinksStep(this));
    addStep(new IPAddressToHostMappingStep(this));
    addStep(new QuantcastIPListStep(this));
    addStep(new DomainIPCollectorStep(this));
    addStep(new FuzzyDedupeStep1(this));
    addStep(new FuzzyDedupeStep2(this));
    addStep(new CrossDomainDupes(this));
    addStep(new FindBadIPsFromDupes(this));
    addStep(new HostBlacklistByDupesStep(this));
    addStep(new SubDomainToQuantcastJoinStep(this));
    addStep(new RankTask(this));
    addStep(new GenPostFrequencyStep(this));
    addStep(new FeedUrlIdStep(this));
    addStep(new CrawlStatsCollectorTask(this));

  }

  @Override
  protected void parseArgs() throws IOException {

    CommandLineParser parser = new GnuParser();
    try {
      // parse the command line arguments
      CommandLine line = parser.parse(options, _args);

      // default to single partition - partition zero
      ImmutableList<Integer> partitions = ImmutableList.of(0);

      if (line.hasOption("partitions")) {
        partitions = ImmutableList.copyOf(Iterators.transform(Iterators.forArray(line.getOptionValue("partitions")
            .split(",")), new Function<String, Integer>() {

          @Override
          public Integer apply(String arg0) {
            return Integer.parseInt(arg0);
          }
        }));
      }

      if (partitions.size() == 0) {
        throw new IOException("One Parition Required At a Minimum!");
      }
      _partitionList = partitions;

      if (line.hasOption("rebuild")) {
        LOG.info("Rebuild Option Specified. Deleting Outputs");
        for (CrawlPipelineStep step : getSteps()) {
          LOG.info("Deleting Output Dir:" + step.getOutputDir() + " for Step:" + step.getName());
          getFileSystem().delete(step.getOutputDir(), true);
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

  @Override
  public int run(String[] args) throws Exception {
    super.run(args);
    return 0;
  }

}
