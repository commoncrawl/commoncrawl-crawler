package org.commoncrawl.service.crawlmasterV2;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.async.Timer;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.protocol.CrawlMaster;
import org.commoncrawl.protocol.SlaveHello;
import org.commoncrawl.protocol.SlaveRegistration;
import org.commoncrawl.rpc.base.internal.AsyncContext;
import org.commoncrawl.rpc.base.internal.AsyncRequest.Status;
import org.commoncrawl.rpc.base.internal.AsyncServerChannel;
import org.commoncrawl.rpc.base.internal.NullMessage;
import org.commoncrawl.rpc.base.shared.RPCException;
import org.commoncrawl.server.CommonCrawlServer;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.IPAddressUtils;

import com.google.common.collect.DiscreteDomains;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ranges;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;
import com.google.gson.JsonObject;

public class CrawlMasterServer extends CommonCrawlServer  implements CrawlMaster, Timer.Callback {

  String _s3AccessKey;
  String _s3Secret;
  static CrawlMasterServer _server;
  Multimap<Integer, SlaveRegistration> _registry = TreeMultimap.create();
  Map<Integer,Integer> _ipToInstanceIdMap = Maps.newTreeMap();
  Set<Integer> _instanceIds  = Sets.newTreeSet();
  private static final int WATCHDOG_DELAY = 1000;
  private static final int MAX_TIME_BETWEEN_HEARTBEATS = 120000;
  Timer _watchDogTimer = new Timer(WATCHDOG_DELAY,true,this);
  
  public static final Log LOG = LogFactory.getLog(CrawlMasterServer.class);
   
  // RPCS 
  
  @Override
  public void registerSlave(
      AsyncContext<SlaveHello, SlaveRegistration> rpcContext)
      throws RPCException {
    LOG.info("Received RegisterSlave Request from:" + IPAddressUtils.IntegerToIPAddressString(rpcContext.getInput().getIpAddress())
        + " Service:" + rpcContext.getInput().getServiceName());
    Integer instanceId = _ipToInstanceIdMap.get(rpcContext.getInput().getIpAddress());

    // assume failure ... 
    rpcContext.setStatus(Status.Error_RequestFailed);

    if (instanceId == null) {
      LOG.info("No Instance Id Mapping Found for IP:"+ IPAddressUtils.IntegerToIPAddressString(rpcContext.getInput().getIpAddress()));
      instanceId = Iterables.getFirst(_instanceIds,-1);
      if (instanceId != -1) { 
        // remove from id pool 
        _instanceIds.remove(instanceId);
        // assign to ip pool 
        _ipToInstanceIdMap.put(rpcContext.getInput().getIpAddress(), instanceId);
      }
    }
    if (instanceId != null && instanceId != -1) { 
      // create service registration ... 
      rpcContext.getOutput().setIpAddress(rpcContext.getInput().getIpAddress());
      rpcContext.getOutput().setServiceName(rpcContext.getInput().getServiceName());
      rpcContext.getOutput().setCookie(rpcContext.getInput().getCookie());
      rpcContext.getOutput().setInstanceId(instanceId);
      rpcContext.getOutput().setLastTimestamp(System.currentTimeMillis());
      rpcContext.getOutput().setPropertiesHash(_properties.toString());
      // stash it away ... 
      _registry.put(instanceId, rpcContext.getOutput());
      // and echo it back to sender ... 
      rpcContext.setStatus(Status.Success);
      // log it 
      LOG.info("Successfully bound Service:" + rpcContext.getInput().getServiceName() 
          +" IP:" + IPAddressUtils.IntegerToIPAddressString(rpcContext.getInput().getIpAddress())
          + " Cookie:" + rpcContext.getInput().getCookie()
          + " to InstanceId:" + instanceId);
    }
    else { 
      LOG.error("Unable to obtain instance Id for IP:"+ IPAddressUtils.IntegerToIPAddressString(rpcContext.getInput().getIpAddress()));
      rpcContext.setErrorDesc("No Instance Id Available for Specified IP Address");
    }
    
    rpcContext.completeRequest();
  }

  @Override
  public void extendRegistration(
      AsyncContext<SlaveRegistration, NullMessage> rpcContext)
      throws RPCException {
    
    
    // assume failure 
    rpcContext.setStatus(Status.Error_RequestFailed);
    
    Integer instaceIdMapping = _ipToInstanceIdMap.get(rpcContext.getInput().getIpAddress());
    // if ip address to instace id mapping matches ... 
    if (instaceIdMapping != null && instaceIdMapping == rpcContext.getInput().getInstanceId()) { 
      Collection<SlaveRegistration> registrations = _registry.get(rpcContext.getInput().getInstanceId());

      // if the registration exists ...  
      if (registrations != null && registrations.contains(rpcContext.getInput())) { 
        // extend the registration ... 
        rpcContext.getInput().setLastTimestamp(System.currentTimeMillis());
        // store it ... 
        registrations.remove(rpcContext.getInput());
        registrations.add(rpcContext.getInput());
        
        // set status bit 
        rpcContext.setStatus(Status.Success);
        
        LOG.info("Extended registration for Service:" + rpcContext.getInput().getServiceName() 
            +" IP:" + IPAddressUtils.IntegerToIPAddressString(rpcContext.getInput().getIpAddress())
            + " Cookie:" + rpcContext.getInput().getCookie()
            + " InstanceId:" + rpcContext.getInput().getInstanceId()
            + " TS: " + rpcContext.getInput().getLastTimestamp());
      }
    }
    rpcContext.completeRequest();
  }

  @Override
  public void expireRegistration(
      AsyncContext<SlaveRegistration, NullMessage> rpcContext)
      throws RPCException {
    
    // assume failure 
    rpcContext.setStatus(Status.Error_RequestFailed);
    
    Integer instaceIdMapping = _ipToInstanceIdMap.get(rpcContext.getInput().getIpAddress());
    // if ip address to instace id mapping matches ... 
    if (instaceIdMapping != null && instaceIdMapping == rpcContext.getInput().getInstanceId()) { 

      
      Collection<SlaveRegistration> registrations = _registry.get(rpcContext.getInput().getInstanceId());

      // if the registration exists ...  
      if (registrations != null) {
        
        // remove the specified registration ... 
        registrations.remove(rpcContext.getInput());

        LOG.info("Released registration for Service:" + rpcContext.getInput().getServiceName() 
            +" IP:" + IPAddressUtils.IntegerToIPAddressString(rpcContext.getInput().getIpAddress())
            + " Cookie:" + rpcContext.getInput().getCookie()
            + " InstanceId:" + rpcContext.getInput().getInstanceId());

        rpcContext.setStatus(Status.Success);
      }
    }
    rpcContext.completeRequest();    
  }

  //@Override
  protected String   getDefaultLogFileName() { 
    return "crawldb";
  }
  
  @Override
  protected String getDefaultDataDir() {
    return CrawlEnvironment.DEFAULT_DATA_DIR;
  }

  @Override
  protected String getDefaultHttpInterface() {
    return CrawlEnvironment.DEFAULT_HTTP_INTERFACE;
  }

  @Override
  protected int getDefaultHttpPort() {
    return CrawlEnvironment.DEFAULT_DATABASE_HTTP_PORT;
  }

  @Override
  protected String getDefaultRPCInterface() {
    return CrawlEnvironment.DEFAULT_RPC_INTERFACE;
  }

  @Override
  protected int getDefaultRPCPort() {
    return CrawlEnvironment.DEFAULT_DATABASE_RPC_PORT;
  }

  @Override
  protected String getWebAppName() {
    return CrawlEnvironment.CRAWLMASTER_WEBAPP_NAME;
  }

  JsonObject _properties = new JsonObject();
  
  @Override
  protected boolean parseArguements(String[] argv) {
    
    
    for(int i=0; i < argv.length;++i) {
      if (argv[i].equalsIgnoreCase("--awsAccessKey")) { 
        if (i+1 < argv.length) { 
          _s3AccessKey = argv[++i];
        }
      }
      else if (argv[i].equalsIgnoreCase("--awsSecret")) { 
        if (i+1 < argv.length) { 
          _s3Secret = argv[++i];
        }
      }
      else if (argv[i].equalsIgnoreCase("--segmentDataDir")) {
        _properties.addProperty(CrawlEnvironment.PROPERTY_SEGMENT_DATA_DIR, argv[++i]);
      }
      else if (argv[i].equalsIgnoreCase("--contentDataDir")) {
        _properties.addProperty(CrawlEnvironment.PROPERTY_CONTENT_DATA_DIR, argv[++i]);
      }
      
    }
    return true;
  }  
  
  @Override
  protected void printUsage() {
    System.out.println("Database Startup Args: --dataDir [data directory]");
  }

  @Override
  protected boolean startDaemons() {
    getEventLoop().setTimer(_watchDogTimer);
    return true;
  }

  @Override
  protected void stopDaemons() {
    getEventLoop().cancelTimer(_watchDogTimer);
  }
  
  
  
  @Override
  protected boolean initServer() {
    
    _server = this;
    // populate instance ids ... 
    _instanceIds.addAll(Ranges.open(-1, CrawlEnvironment.NUM_CRAWLERS).asSet(DiscreteDomains.integers()));
    
    LOG.info("Available Instance Ids Are: "+ _instanceIds);
    
    try { 

      // create server channel ... 
      AsyncServerChannel channel = new AsyncServerChannel(this, getEventLoop(), getServerAddress(),null);
      // register RPC services it supports ... 
      registerService(channel,CrawlMaster.spec);
      // open the server channel ..
      channel.open();
    }
    catch (IOException e) { 
      LOG.fatal(CCStringUtils.stringifyException(e));
      return false;
    }
    return true;

  }

  @Override
  public void timerFired(Timer timer) {
    //LOG.info("Heartbeat timer fired.");
    // ok walk registry expiring stuff ...  
    Iterator<Entry<Integer, SlaveRegistration>> registrations =  _registry.entries().iterator();
    while (registrations.hasNext()) {
      Entry<Integer, SlaveRegistration> registration = registrations.next();
      
      if ((System.currentTimeMillis() - registration.getValue().getLastTimestamp()) >= MAX_TIME_BETWEEN_HEARTBEATS) { 
        LOG.info("Released registration for Service:" + registration.getValue().getServiceName() 
            +" IP:" + IPAddressUtils.IntegerToIPAddressString(registration.getValue().getIpAddress())
            + " Cookie:" + registration.getValue().getCookie()
            + " InstanceId:" + registration.getValue().getInstanceId()
            + " Current TS:" + System.currentTimeMillis() 
            + " Last TS:" + registration.getValue().getLastTimestamp());
        registrations.remove();
      }
    }
    
    // now walk address mappings 
    Iterator<Entry<Integer,Integer>> ipToInstanceIdIterator = _ipToInstanceIdMap.entrySet().iterator();
    
    while (ipToInstanceIdIterator.hasNext()) { 
      Entry<Integer,Integer> mapping = ipToInstanceIdIterator.next();
      
      if (_registry.get(mapping.getValue()).size() == 0) { 
        LOG.info("Instance Id:" + mapping.getValue() 
            + " has no more registrations associated with IP:" 
            + IPAddressUtils.IntegerToIPAddressString(mapping.getKey())
            + " - Expiring.");
               
        ipToInstanceIdIterator.remove();
        // reclaim id
        _instanceIds.add(mapping.getValue());
        LOG.info("Recalimed Instance Id:" + mapping.getValue()+ " New Set is:" + _instanceIds);
            
      }
    }
  }
}
