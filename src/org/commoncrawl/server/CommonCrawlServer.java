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
package org.commoncrawl.server;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.common.Environment;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.rpc.base.internal.Server;
import org.commoncrawl.util.RuntimeStatsCollector;

/**
 * 
 * @author rana
 *
 */
public abstract class CommonCrawlServer extends Server {

  protected static final class CommonConfig {

    public String _className;
    public String _hostName;
    public String _rpcInterface;
    public int _rpcPort = -1;
    public String _webInterface;
    public int _webPort = -1;
    public String _configName;
    public String _dataDir = null;
    public int _dnsThreadPoolSize = -1;
  }

  public static final Log LOG = LogFactory.getLog(CommonCrawlServer.class);

  private static final int DEFAULT_MAX_THREADPOOL_THREADS = 5;
  private static final int DEFAULT_DNS_THREAD_POOL_SIZE = 50;

  public static final String DNS_POOL_NAME = "dns";

  /** return the ip address for a given network interface **/
  protected static String[] getIPs(String strInterface) throws UnknownHostException {

    Pattern pattern = Pattern.compile("([0-9]+).([0-9]+).([0-9]+).([0-9]+)");

    Matcher m = pattern.matcher(strInterface);

    // if this is an ip address ...
    if (m.matches()) {
      LOG.info("getIPs detected ip address as interface name. returned: " + strInterface);
      return new String[] { strInterface };
    } else {
      if (strInterface.equalsIgnoreCase("localhost") || strInterface.equalsIgnoreCase("lo")) {
        LOG.info("getIPs for localhost name:" + strInterface + " returned: 127.0.0.1");
        return new String[] { "127.0.0.1" };
      }

      try {

        NetworkInterface netIF = NetworkInterface.getByName(strInterface);

        if (netIF == null) {
          return null;
        } else {
          Vector<String> ips = new Vector<String>();

          Enumeration<InetAddress> e = netIF.getInetAddresses();

          while (e.hasMoreElements()) {

            InetAddress address = e.nextElement();
            // only allow ipv4 addresses for now ...
            if (address.getAddress().length == 4) {
              LOG.info("getIPs for name:" + strInterface + " found:" + address.getHostAddress());
              ips.add(address.getHostAddress());
            }
          }
          return ips.toArray(new String[] {});
        }
      } catch (SocketException e) {
        return null;
      }
    }
  }

  public static void main(String argv[]) throws Exception {

    try {
      Configuration conf = new Configuration();

      conf.addResource("nutch-default.xml");
      conf.addResource("nutch-site.xml");
      conf.addResource("core-site.xml");
      conf.addResource("mapred-site.xml");
      conf.addResource("hdfs-site.xml");
      // conf.addResource("hadoop-site.xml");
      // conf.addResource("commoncrawl-default.xml");
      // conf.addResource("commoncrawl-site.xml");

      /*
       * conf.setClassLoader( new ClassLoader() {
       * 
       * @Override protected Class<?> findClass(String name) throws
       * ClassNotFoundException { if (name.startsWith("org.crawlcommons")) { //
       * this is a hack to deal with the problem of having a bunch of serialized
       * data in hdfs sequence files that referes to // protocol buffer with the
       * old crawler package name of org.crawlcommons instead of the new package
       * name of org.commoncrawl // we replace the crawlcommons string and call
       * back into the class loader to re-resolve the name... name =
       * name.replaceFirst("org.crawlcommons", "org.commoncrawl"); return
       * loadClass(name); }
       * 
       * return super.findClass(name); }
       * 
       * });
       */

      // set this config object as our global config
      CrawlEnvironment.setHadoopConfig(conf);

      _commonConfig = parseCommonConfig(argv);

      if (_commonConfig._className == null) {
        printCommonUsage();
        return;
      }

      if (_commonConfig._hostName != null) {
        conf.set("org.commoncrawl.hostname", _commonConfig._hostName);
      }

      if (_commonConfig._rpcInterface != null) {
        conf.set("org.commoncrawl.rpcInterface", _commonConfig._rpcInterface);
      }

      if (_commonConfig._webInterface != null) {
        conf.set("org.commoncrawl.httpInterface", _commonConfig._webInterface);
      }

      if (_commonConfig._rpcPort != -1) {
        conf.setInt("org.commoncrawl.rpcPort", _commonConfig._rpcPort);
      }
      if (_commonConfig._webPort != -1) {
        conf.setInt("org.commoncrawl.httpPort", _commonConfig._webPort);
      }
      if (_commonConfig._dataDir != null) {
        conf.set("org.commoncrawl.dataDir", _commonConfig._dataDir);
      }
      if (_commonConfig._dnsThreadPoolSize != -1) {
        conf.setInt("org.commoncrawl.dnsThreadPoolSize", _commonConfig._dnsThreadPoolSize);
      }
      LOG.info("Log File Is:" + System.getProperty("commoncrawl.log.file"));

      LOG.info("Instantiating Class:" + _commonConfig._className);
      Class theClass = conf.getClassByName(_commonConfig._className);

      Object serverInstance = theClass.newInstance();
      CommonCrawlServer server = CommonCrawlServer.class.cast(serverInstance);

      StringUtils.startupShutdownMessage(theClass, argv, LOG);

      if (server.init(argv, conf)) {
        try {

          server.start();
          server.join();

        } catch (IOException e) {
          LOG.error(StringUtils.stringifyException(e));
          throw e;
        } finally {
          server.stopDaemons();
          server.stop();
        }
      }

    } catch (Throwable e) {
      LOG.error(StringUtils.stringifyException(e));
      e.printStackTrace();
      System.exit(-1);
    }
  }

  public static CommonConfig parseCommonConfig(String argv[]) {

    CommonConfig configOut = new CommonConfig();
    if (argv != null) {
      for (int i = 0; i < argv.length; ++i) {
        if (argv[i].equalsIgnoreCase("--server")) {
          if (i + 1 < argv.length) {
            configOut._className = argv[++i];
          }
        } else if (argv[i].equalsIgnoreCase("--hostname")) {
          if (i + 1 < argv.length) {
            configOut._hostName = argv[++i];
          }
        } else if (argv[i].equalsIgnoreCase("--rpcIntfc")) {
          if (i + 1 < argv.length) {
            configOut._rpcInterface = argv[++i];
          }
        } else if (argv[i].equalsIgnoreCase("--httpIntfc")) {
          if (i + 1 < argv.length) {
            configOut._webInterface = argv[++i];
          }
        } else if (argv[i].equalsIgnoreCase("--rpcPort")) {
          if (i + 1 < argv.length) {
            configOut._rpcPort = Integer.parseInt(argv[++i]);
          }
        } else if (argv[i].equalsIgnoreCase("--httpPort")) {
          if (i + 1 < argv.length) {
            configOut._webPort = Integer.parseInt(argv[++i]);
          }
        } else if (argv[i].equalsIgnoreCase("--conf")) {
          if (i + 1 < argv.length) {
            configOut._configName = argv[++i];
          }
        } else if (argv[i].equalsIgnoreCase("--dataDir")) {
          if (i + 1 < argv.length) {
            configOut._dataDir = argv[++i];
          }
        } else if (argv[i].equalsIgnoreCase("--dnsPoolSize")) {
          configOut._dnsThreadPoolSize = Integer.parseInt(argv[++i]);
        }

        else if (argv[i].equalsIgnoreCase("--detailLogging")) {
          int value = Integer.parseInt(argv[++i]);
          Environment.enableDetailLog((value == 1));
        }

      }
    }
    return configOut;
  }

  private static void printCommonUsage() {
    System.err
        .println("Usage: java CommonCrawlServer --server [server type] --hostname [hostname] --rpcInterface [rpc server interface] --rpcPort [rpc server port] --httpInterface [web server interface] --httpPort [web server port] --dataDir [data directory] --conf [config file] [server args]");

  }

  protected Configuration _configuration;
  private static CommonCrawlServer _serverSingleton;
  protected static CommonConfig _commonConfig;
  protected String _hostName;
  protected InetSocketAddress _serverAddress;
  protected File _dataDir;
  protected WebServer _webServer;
  protected boolean _useAsyncWebDispatch = false;

  private int _dnsThreadPoolSize = DEFAULT_DNS_THREAD_POOL_SIZE;

  protected EventLoop _eventLoop = null;

  /** default thread pool */
  private ExecutorService _defaultThreadPool;

  /** registered thread pools */
  private TreeMap<String, ExecutorService> _threadPoolMap = new TreeMap<String, ExecutorService>();
  private static ThreadLocal _serverObject = new ThreadLocal() {
    @Override
    protected Object initialValue() {
      return _serverSingleton;
    }
  };

  public static CommonCrawlServer getServerSingleton() {
    return (CommonCrawlServer) _serverObject.get();
  }

  public CommonCrawlServer() {
    _serverSingleton = this;
  }

  public void collectStats(RuntimeStatsCollector collector) {
    StringBuffer sb = new StringBuffer();
    sb.append("\n*****THREAD-POOL-STATE*****\n");

    sb.append(String.format("%1$10.10s ", "POOLNAME"));
    sb.append(String.format("%1$8.8s ", "TOTAL"));
    sb.append(String.format("%1$6.6s ", "ACTIVE"));
    sb.append(String.format("%1$8.8s\n", "PENDING"));

    long totalTaskCount = ((ThreadPoolExecutor) _defaultThreadPool).getTaskCount();
    long completedTaskCount = ((ThreadPoolExecutor) _defaultThreadPool).getCompletedTaskCount();
    long activeCount = ((ThreadPoolExecutor) _defaultThreadPool).getActiveCount();

    // print out default thread pool stats first ...
    sb.append(String.format("%1$10.10s ", "default"));
    sb.append(String.format("%1$8.8s ", totalTaskCount));
    sb.append(String.format("%1$6.6s ", activeCount));
    sb.append(String.format("%1$8.8s\n", totalTaskCount - completedTaskCount - activeCount));

    synchronized (_threadPoolMap) {

      for (String poolName : _threadPoolMap.keySet()) {

        ThreadPoolExecutor executor = (ThreadPoolExecutor) _threadPoolMap.get(poolName);

        totalTaskCount = executor.getTaskCount();
        completedTaskCount = executor.getCompletedTaskCount();
        activeCount = executor.getActiveCount();

        sb.append(String.format("%1$10.10s ", poolName));
        sb.append(String.format("%1$8.8s ", totalTaskCount));
        sb.append(String.format("%1$6.6s ", activeCount));
        sb.append(String.format("%1$8.8s\n", totalTaskCount - completedTaskCount - activeCount));

      }
    }
    collector.setStringValue(ServerStats.ID, ServerStats.Name.CommonServer_ThreadPoolInfo, sb.toString());
  }

  /**
   * dispatch an async web request on the server's main event loop thread
   * 
   * @param request
   *          object
   * @throws IOException
   */
  public void dispatchAsyncWebRequest(final AsyncWebServerRequest request) throws IOException {
    request.dispatch(_eventLoop);
    if (request.getException() != null) {
      throw request.getException();
    }
  }

  public Configuration getConfig() {
    return _configuration;
  }

  public File getDataDirectory() {
    return _dataDir;
  }

  protected abstract String getDefaultDataDir();

  protected abstract String getDefaultHttpInterface();

  protected abstract int getDefaultHttpPort();

  protected abstract String getDefaultLogFileName();

  protected abstract String getDefaultRPCInterface();

  protected abstract int getDefaultRPCPort();

  public synchronized ExecutorService getDefaultThreadPool() {
    return _defaultThreadPool;
  }

  public EventLoop getEventLoop() {
    return _eventLoop;
  }

  public String getHostName() {
    return _hostName;
  }

  public File getLogDirectory() {
    return new File(System.getProperty("commoncrawl.log.dir"));
  }

  public InetSocketAddress getServerAddress() {
    return _serverAddress;
  }

  public String getServerName() {
    return this.getClass().getSimpleName();
  }

  public ExecutorService getThreadPool(String threadPoolId) {

    ExecutorService service = null;
    synchronized (_threadPoolMap) {
      service = _threadPoolMap.get(threadPoolId);
    }
    return service;
  }

  protected abstract String getWebAppName();

  public WebServer getWebServer() {
    return _webServer;
  }

  protected final boolean init(String argv[], Configuration conf) throws IOException {

    _configuration = conf;

    if (!parseArguments(argv)) {
      printUsage();
      return false;
    }

    overrideConfig(conf);

    // initialize the default thread pool
    _defaultThreadPool = Executors.newFixedThreadPool(_configuration.getInt("org.commoncrawl.threadpool.max.threads",
        DEFAULT_MAX_THREADPOOL_THREADS));

    _hostName = _configuration.get("org.commoncrawl.hostname");

    if (_hostName == null) {

      // get host name via other meands
      InetAddress localHostAddr = InetAddress.getLocalHost();
      _hostName = localHostAddr.getHostName();
      _configuration.set("org.commoncrawl.hostname", _hostName);
    }
    if (Environment.detailLogEnabled())
      LOG.info("Hostname is: " + _hostName);

    String rpcInterface = _configuration.get("org.commoncrawl.rpcInterface", getDefaultRPCInterface());
    String webInterface = _configuration.get("org.commoncrawl.httpInterface", getDefaultHttpInterface());
    int rpcPort = _configuration.getInt("org.commoncrawl.rpcPort", getDefaultRPCPort());
    int httpPort = _configuration.getInt("org.commoncrawl.httpPort", getDefaultHttpPort());
    String dataDirectory = _configuration.get("org.commoncrawl.dataDir", getDefaultDataDir());
    _dnsThreadPoolSize = _configuration.getInt("org.commoncrawl.dnsThreadPoolSize", DEFAULT_DNS_THREAD_POOL_SIZE);

    // validate data directory
    _dataDir = new File(dataDirectory);

    if (!_dataDir.exists()) {
      if (!_dataDir.mkdirs()) {
        System.out.println("Unable to create data directory: " + dataDirectory);
        return false;
      }
    } else if (!_dataDir.isDirectory()) {
      System.out.println("Invalid data directory:" + dataDirectory);
      return false;
    }

    // update properties ...
    LOG.info("Config says rpcInterface is:" + rpcInterface);
    conf.set("org.commoncrawl.rpcInterface", rpcInterface);

    LOG.info("Config says httpInterface is:" + webInterface);
    conf.set("org.commoncrawl.httpInterface", webInterface);

    LOG.info("Config says rpcPort is:" + rpcPort);
    conf.setInt("org.commoncrawl.rpcPort", rpcPort);

    LOG.info("Config says httpPort is:" + httpPort);
    conf.setInt("org.commoncrawl.httpPort", httpPort);

    LOG.info("Config says dataDir  is:" + dataDirectory);
    conf.set("org.commoncrawl.dataDir", dataDirectory);

    // extract ip address for rpc / web interfaces
    String rpcIPS[] = getIPs(rpcInterface);
    String webIPS[] = getIPs(webInterface);

    if (rpcIPS == null || rpcIPS.length == 0) {
      LOG.error("No Valid IP Addresses found for RPC Interface:" + rpcInterface);
      return false;
    }

    if (webIPS == null || webIPS.length == 0) {
      LOG.error("No Valid IP Addresses found for Web Interface:" + webInterface);
      return false;
    }

    String selectedRPCInterface = rpcIPS[0];
    if (rpcIPS.length > 1) {
      for (String rpcIPAddress : rpcIPS) {
        if (!rpcIPAddress.endsWith(".1")) {
          selectedRPCInterface = rpcIPAddress;
          break;
        }
      }
    }

    String selectedWebInterface = webIPS[0];
    if (webIPS.length > 1) {
      for (String webIPAddress : rpcIPS) {
        if (!webIPAddress.endsWith(".1")) {
          selectedWebInterface = webIPAddress;
          break;
        }
      }
    }

    LOG.info("RPC Interface translates to IP:" + selectedRPCInterface);
    LOG.info("Web Interface translates to IP:" + selectedWebInterface);

    // initialize the server address
    _serverAddress = new InetSocketAddress(selectedRPCInterface, rpcPort);

    // init the event loop
    _eventLoop = new EventLoop(registerThreadPool(DNS_POOL_NAME, _dnsThreadPoolSize));

    // and the Web Server
    _webServer = new WebServer(this, selectedWebInterface, httpPort, false, _useAsyncWebDispatch);
    _webServer.setAttribute("commoncrawl.server", this);

    // initialize the underlying server
    if (initServer()) {

      // log the startup time
      LOG.info(_commonConfig._className + " up at: " + _serverAddress);

      return true;
    }
    return false;
  }

  protected abstract boolean initServer();

  private void join() throws InterruptedException {
    if (_eventLoop.getEventThread() != null) {
      _eventLoop.getEventThread().join();
    }
  }

  protected void overrideConfig(Configuration conf) {
  }

  protected abstract boolean parseArguments(String argv[]);

  protected abstract void printUsage();

  public ExecutorService registerThreadPool(String threadPoolId, int maxThreads) {
    ExecutorService service = null;
    synchronized (_threadPoolMap) {
      service = _threadPoolMap.get(threadPoolId);

      if (service == null) {
        service = Executors.newFixedThreadPool(maxThreads);
        _threadPoolMap.put(threadPoolId, service);
      }
    }
    return service;
  }

  public void setAsyncWebDispatch(boolean asyncWebDispatch) {
    _useAsyncWebDispatch = asyncWebDispatch;
  }

  @Override
  public void start() throws IOException {
    // add a shutdown hook ...
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

      @Override
      public void run() {
        stop();
      }

    }));

    _eventLoop.start();

    if (startDaemons()) {
      // start RPC server
      super.start();
      // and finally start web server ...
      _webServer.start();
    }
  }

  protected abstract boolean startDaemons();

  /** called to initiate an orderly shutdown of the service **/
  @Override
  public void stop() {

    // stop RPC Server
    super.stop();
    /*
     * try { // and web server _webServer.stop(); } catch (InterruptedException
     * e) {
     * 
     * }
     */
    // and finally stop daemons
    stopDaemons();

    _eventLoop.stop();
  }

  protected abstract void stopDaemons();

  public void terminateDefaultThreadPool() {

    ExecutorService oldThreadPool = null;
    synchronized (this) {

      oldThreadPool = _defaultThreadPool;
      _defaultThreadPool = Executors.newFixedThreadPool(_configuration.getInt("org.commoncrawl.threadpool.max.threads",
          DEFAULT_MAX_THREADPOOL_THREADS));

    }
    oldThreadPool.shutdown();
    try {
      while (!oldThreadPool.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
        LOG.info("Awaiting shutdown for Default ThreadPool");
      }
    } catch (InterruptedException e) {
      LOG.error(StringUtils.stringifyException(e));
    }

  }

  public void terminateThreadPool(String threadPoolId) {
    ExecutorService service = null;
    synchronized (_threadPoolMap) {
      service = _threadPoolMap.remove(threadPoolId);

    }

    if (service != null) {
      LOG.info("Terminating ThreadPool:" + threadPoolId);
      service.shutdown();
      try {
        while (!service.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
          LOG.info("Awaiting shutdown for ThreadPool:" + threadPoolId);
        }
      } catch (InterruptedException e) {
        LOG.error(StringUtils.stringifyException(e));
      }
    }
  }
}
