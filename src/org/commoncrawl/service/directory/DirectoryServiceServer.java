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
package org.commoncrawl.service.directory;

import java.io.BufferedReader;
import java.io.CharArrayReader;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.commoncrawl.crawl.common.internal.CrawlEnvironment;
import org.commoncrawl.rpc.base.internal.AsyncClientChannel;
import org.commoncrawl.rpc.base.internal.AsyncContext;
import org.commoncrawl.rpc.base.internal.AsyncRequest;
import org.commoncrawl.rpc.base.internal.AsyncServerChannel;
import org.commoncrawl.rpc.base.internal.NullMessage;
import org.commoncrawl.rpc.base.shared.BinaryProtocol;
import org.commoncrawl.rpc.base.shared.RPCException;
import org.commoncrawl.server.CommonCrawlServer;
import org.commoncrawl.service.directory.DirectoryService;
import org.commoncrawl.service.directory.DirectoryServiceItem;
import org.commoncrawl.service.directory.DirectoryServiceItemList;
import org.commoncrawl.service.directory.DirectoryServiceQuery;
import org.commoncrawl.service.directory.DirectoryServiceRegistrationInfo;
import org.commoncrawl.service.directory.DirectoryServiceSubscriptionInfo;
import org.commoncrawl.util.CCStringUtils;

/**
 * 
 * @author rana
 *
 */
public class DirectoryServiceServer 
  extends CommonCrawlServer 
  implements DirectoryService,
             AsyncServerChannel.ConnectionCallback {

  
  private static final String SYSTEM_DATA_ROOT = "sys";
  private static final String SYSTEM_DATA_ROOT_PATH =  CrawlEnvironment.HDFS_CrawlDBBaseDir + "/" + CrawlEnvironment.DIRECTORY_SERVICE_HDFS_ROOT + "/sys";

  private static final String USER_DATA_ROOT = "user";
  private static final String USER_DATA_ROOT_PATH =  CrawlEnvironment.HDFS_CrawlDBBaseDir + "/" + CrawlEnvironment.DIRECTORY_SERVICE_HDFS_ROOT + "/user";
  
  private static final String IN_MEMORYPATHS_FILE = "/InMemoryPaths.txt";
  
  private FileSystem _fileSystem = null;
  private File _tempFileDir = null;
  
  private Vector<Pattern> memoryOnlyPaths = new Vector<Pattern>();

  private static class ClientConnection { 
    
  }
  
  
  private Map<String,DirectoryServiceItem> _userItems = new TreeMap<String,DirectoryServiceItem>();
  private Map<String,DirectoryServiceItem> _systemItems = new TreeMap<String,DirectoryServiceItem>();
  private Map<AsyncClientChannel,DirectoryServiceListener> _listeners = new TreeMap<AsyncClientChannel,DirectoryServiceListener>();
  private Map<AsyncClientChannel,DirectoryServiceListener> _pendingListeners = new TreeMap<AsyncClientChannel,DirectoryServiceListener>();
  
 
  public FileSystem getFileSystem() { 
    return _fileSystem;
  }
  
  @Override
  protected String getDefaultHttpInterface() {
    return CrawlEnvironment.DEFAULT_HTTP_INTERFACE;
  }

  @Override
  protected int getDefaultHttpPort() {
    return CrawlEnvironment.DIRECTORY_SERVICE_HTTP_PORT;
  }

  @Override
  protected String getDefaultLogFileName() {
    return "dservice.log";
  }

  @Override
  protected String getDefaultRPCInterface() {
    return CrawlEnvironment.DEFAULT_RPC_INTERFACE;
  }

  @Override
  protected int getDefaultRPCPort() {
    return CrawlEnvironment.DIRECTORY_SERVICE_RPC_PORT;
  }

  @Override
  protected String getWebAppName() {
    return CrawlEnvironment.DIRECTORY_SERVICE_WEBAPP_NAME;
  }

  @Override
  protected boolean initServer() {
    
    try { 
      _fileSystem = CrawlEnvironment.getDefaultFileSystem();
      
      // load system files 
      loadSystemPaths();
      
      LOG.info("Processing System Files");
      // process system files 
      processSystemFiles();
      
      LOG.info("Loading User Items");
      // load user files 
      loadUserItems();
      
      // create server channel ... 
      AsyncServerChannel channel = new AsyncServerChannel(this, this.getEventLoop(), this.getServerAddress(),this);
      
      // register RPC services it supports ... 
      registerService(channel,DirectoryService.spec);
      
      return true;
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
    }
    return false;
  }

  @Override
  protected boolean parseArguements(String[] argv) {
    return true;
  }
  
  @Override
  protected void overrideConfig(Configuration conf) { 
    
  }
  
  @Override
  protected void printUsage() {
   
    
  }

  @Override
  protected boolean startDaemons() {
    return true;
  }

  @Override
  protected void stopDaemons() {
    
  }



  @Override
  protected String getDefaultDataDir() {
    // TODO Auto-generated method stub
    return null;
  }


  final boolean isItemPersistent(DirectoryServiceItem item) { 
    for (Pattern pattern : memoryOnlyPaths) { 
      if (pattern.matcher(item.getItemPath()).matches()) { 
        return false;
      }
    }
    return true;
  }

  @Override
  public void publish(AsyncContext<DirectoryServiceItem, DirectoryServiceItem> rpcContext) throws RPCException {
    DirectoryServiceItem item = rpcContext.getInput();
    LOG.info("Received publish request for item:" + item.getItemPath());
    
    if (!item.isFieldDirty(DirectoryServiceItem.Field_ITEMPATH) || !item.getItemPath().startsWith("/") || item.getItemPath().endsWith("/")) { 
      rpcContext.setStatus(AsyncRequest.Status.Error_RequestFailed);
      rpcContext.setErrorDesc("Invalid Path");
      LOG.error("Request Failed:Invalid Path(" + item.getItemPath()+")");
    }
    else if (!item.isFieldDirty(DirectoryServiceItem.Field_ITEMDATA) || item.getItemData().getCount() == 0) { 
      rpcContext.setStatus(AsyncRequest.Status.Error_RequestFailed);
      rpcContext.setErrorDesc("Invalid Data Buffer");
      LOG.error("Request Failed:Invalid Data Buffer(" + item.getItemPath()+")");
    }
    else { 
      DirectoryServiceItem existingItem = _userItems.get(item.getItemPath());
      if (existingItem != null) { 
        item.setVersionNumber(existingItem.getVersionNumber() + 1);
        LOG.info("Incrementing Item:" + item.getItemPath() + " Version Number to:" + item.getVersionNumber());
      }
      try {
        
        if (isItemPersistent(item)) { 

          Path fullPath = buildFullUserItemPath(item.getItemPath(),item.getVersionNumber());
          LOG.info("Publish Reuest. Item:" + item.getItemPath()+" is persistent. Persisting to disk. Path is:" + fullPath);
          FSDataOutputStream outputStream = _fileSystem.create(fullPath);
          try { 
            item.serialize(outputStream, new BinaryProtocol());
          }
          catch (IOException e) { 
            LOG.error("Error writing item:" + item.getItemPath() + " to disk");
            LOG.error(CCStringUtils.stringifyException(e));
            _fileSystem.delete(fullPath,false);
            throw e;
          }
          finally { 
            outputStream.flush();
            outputStream.close();
          }
        }
        _userItems.put(item.getItemPath(),item);
        rpcContext.setStatus(AsyncRequest.Status.Success);
        try {
          rpcContext.getOutput().merge(item);
        } catch (CloneNotSupportedException e) {
        }
        
        rpcContext.getOutput().setFieldClean(DirectoryServiceItem.Field_ITEMDATA);
        rpcContext.setStatus(AsyncRequest.Status.Success);
        
        try {
          broadcastToSubscribers((DirectoryServiceItem)item.clone());
        } catch (CloneNotSupportedException e) {
        }
        
      }
      catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
        rpcContext.setStatus(AsyncRequest.Status.Error_RequestFailed);
        rpcContext.setErrorDesc(CCStringUtils.stringifyException(e));
      }
    }
    rpcContext.completeRequest();
  }

  private void broadcastToSubscribers(DirectoryServiceItem item) {
    DirectoryServiceItemList list = new DirectoryServiceItemList();
    
    list.getItems().add(item);
    
    LOG.info("Searching Listener List for match for changed path:" + item.getItemPath());
    for (DirectoryServiceListener listener: _listeners.values()) {
      Map<String,Pattern> patternMap = listener.getSubscriptions();
      for (Pattern pattern : patternMap.values()) {
        LOG.info("Comparing Against Pattern:" + pattern.toString() + " for Listener:" + listener.getName());
        if (pattern.matcher(item.getItemPath()).matches()) { 
          LOG.info("Pattern Matched. Dispatching Request to Listener");
          listener.dispatchItemsChangedMessage(list);
        }
      }
    }
  }

  private static Path buildFullUserItemPath(String itemName,long versionNumber) {
    return new Path(USER_DATA_ROOT_PATH + itemName + "$" + Long.toString(versionNumber));
  }


  @Override
  public void register(AsyncContext<DirectoryServiceRegistrationInfo, NullMessage> rpcContext)throws RPCException {
    LOG.info("Received Register Request from:" + rpcContext.getInput().getConnectionString());
    DirectoryServiceListener existingListener = _listeners.get(rpcContext.getClientChannel());
    // remove existing listener if any 
    if (existingListener != null) { 
      _listeners.remove(rpcContext.getClientChannel());
      existingListener.disconnect();
    }
    // allocte new listener object 
    InetSocketAddress address = CCStringUtils.parseSocketAddress(rpcContext.getInput().getConnectionString());
    
    if (address == null) { 
      RPCException e = new RPCException("Invalid Connection String in Client Registration Request: " + rpcContext.getInput().getConnectionString());
      LOG.error(CCStringUtils.stringifyException(e));
      throw e;
    }
    
    DirectoryServiceListener listener = new DirectoryServiceListener(this,rpcContext.getClientChannel(),address,rpcContext.getInput().getRegistrationCookie());
    try {  
      listener.connect();
      _pendingListeners.put(rpcContext.getClientChannel(),listener);
      rpcContext.completeRequest();
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
    }
  }
  
  @Override
  public void query(AsyncContext<DirectoryServiceQuery, DirectoryServiceItemList> rpcContext) throws RPCException {
    LOG.info("Received Query Request from: " + rpcContext.getClientChannel());
    queryItems(rpcContext.getInput().getItemPath(),rpcContext.getOutput());
    rpcContext.setStatus(AsyncRequest.Status.Success);
    rpcContext.completeRequest();
  }



  @Override
  public void subscribe(AsyncContext<DirectoryServiceSubscriptionInfo, DirectoryServiceItemList> rpcContext)throws RPCException {
    LOG.info("Received Subscription Request from: " + rpcContext.getClientChannel());
    DirectoryServiceListener listener = _listeners.get(rpcContext.getClientChannel());
    if (listener != null) { 
      listener.addSubscription(rpcContext.getInput().getSubscriptionPath());
    }
    queryItems(rpcContext.getInput().getSubscriptionPath(),rpcContext.getOutput());
    rpcContext.setStatus(AsyncRequest.Status.Success);
    rpcContext.completeRequest();
  }



  @Override
  public void unscubscribe(AsyncContext<DirectoryServiceSubscriptionInfo, NullMessage> rpcContext) throws RPCException {
    LOG.info("Received Unsubscribe Request from: " + rpcContext.getClientChannel());
    DirectoryServiceListener listener = _listeners.get(rpcContext.getClientChannel());
    if (listener != null) { 
      listener.removeSubscription(rpcContext.getInput().getSubscriptionPath());
    }
    rpcContext.setStatus(AsyncRequest.Status.Success);
    rpcContext.completeRequest();
  }



  @Override
  public void IncomingClientConnected(AsyncClientChannel channel) {
    
  }



  @Override
  public void IncomingClientDisconnected(AsyncClientChannel channel) {
    removeListeners(channel);
  }
  
  private void queryItems(String queryString,DirectoryServiceItemList listOut) { 
    Pattern pattern = Pattern.compile(queryString);
    for (DirectoryServiceItem item : _userItems.values()) { 
      if (pattern.matcher(item.getItemPath()).matches()) { 
        listOut.getItems().add(item);
      }
    }
  }

  private void loadSystemPaths() throws IOException { 
    loadItems(new Path(SYSTEM_DATA_ROOT_PATH),new Path(SYSTEM_DATA_ROOT_PATH),_systemItems,false);
  }
  
  private void loadUserItems() throws IOException {
    loadItems(new Path(USER_DATA_ROOT_PATH),new Path(USER_DATA_ROOT_PATH),_userItems,true);    
  }
  
  private void loadItems(Path itemRootPath,Path currentPath,Map<String,DirectoryServiceItem> map,boolean hasVersioning) throws IOException {
    
    FileStatus paths[] = _fileSystem.globStatus(new Path(currentPath,"*"));
    
    LOG.info("Pre-Loading Items from FileSystem at root path:" + currentPath);
    for (FileStatus itemPath : paths) { 
      if (!itemPath.isDir()) { 
        DirectoryServiceItem item = preLoadItemInfoFromPath(itemRootPath,itemPath.getPath(),hasVersioning);
        
        if (item != null) {
          
          LOG.info("Found Item:" + item.getItemPath() + " Version:" + item.getVersionNumber());
          
          DirectoryServiceItem existingItem = map.get(item.getItemPath());
          if (existingItem == null){
            map.put(item.getItemPath(), item);
          }
          else { 
            if (existingItem.getVersionNumber() < item.getVersionNumber()) { 
              map.put(item.getItemPath(), item);
            }
          }
        }
      }
      else { 
        loadItems(itemRootPath,itemPath.getPath(),map,hasVersioning);
      }
    }
    
    // now load the actual versions 
    for (DirectoryServiceItem item : map.values()) {
      try { 
        Path path = new Path(itemRootPath + item.getItemPath()+ "$" + item.getVersionNumber());
        LOG.info("Loading Item:" + item.getItemPath() + " Verison:" + item.getVersionNumber() + " from Path:" + path);        
        FSDataInputStream inputStream = _fileSystem.open(path);
        try { 
          item.deserialize(inputStream,new BinaryProtocol());
          LOG.info("Loaded Item:" + item.getItemPath() + " Version:" + item.getVersionNumber() + " BufferSize:" + item.getItemData().getCount());
        }
        finally { 
          inputStream.close();
        }
      }
      catch (IOException e) { 
       LOG.error(CCStringUtils.stringifyException(e));
       item.setFlags(DirectoryServiceItem.Flags.ItemLoadFailed);
      }
    }
  }
  
  public final String normalizeOutputPath(Path rootPath,String inputPath) {
    String rootPathStr = rootPath.toString();
    int indexOfRoot = inputPath.indexOf(rootPathStr);
    return inputPath.substring(indexOfRoot+rootPathStr.length());
  }
  
  public DirectoryServiceItem preLoadItemInfoFromPath(Path rootPath,Path path,boolean hasVersioning) throws IOException { 
    // extract version information
    String strPath = path.toString();
    LOG.info("preloading item:" + strPath);
    long versionNumber = -1;
    if (hasVersioning) { 
      int versionDelimiter = strPath.lastIndexOf('$');
      String versionStr = strPath.substring(versionDelimiter + 1);
      strPath = strPath.substring(0,versionDelimiter);
      try {
        versionNumber = Long.parseLong(versionStr);
      } catch (NumberFormatException e) {
        LOG.error(CCStringUtils.stringifyException(e));
        return null;
      }
    }
      
    DirectoryServiceItem itemOut = new DirectoryServiceItem();
    itemOut.setItemPath(normalizeOutputPath(rootPath,strPath));
    itemOut.setVersionNumber(versionNumber);
    return itemOut;
  }
  
  private void processSystemFiles() throws IOException  {
    for (DirectoryServiceItem item : _systemItems.values()) { 
      if (item.getItemPath().equals(IN_MEMORYPATHS_FILE)) { 
        processInMemoryPathsFile(item.getItemData().getReadOnlyBytes());
      }
    }
    _systemItems.clear();
  }
  
  private void processInMemoryPathsFile(byte[] inMemoryFileData)throws IOException {
    CharBuffer charBuf = Charset.forName("UTF8").decode(ByteBuffer.wrap(inMemoryFileData));
    BufferedReader reader = new BufferedReader(new CharArrayReader(charBuf.array(),0,charBuf.limit()));
    
    String nextLine = null;
    while ((nextLine = reader.readLine()) != null) {
      LOG.info("Compiling in memory path pattern:" + nextLine);
      memoryOnlyPaths.add(Pattern.compile(nextLine));
    }
    
  }
  
  void activateListener(AsyncClientChannel sourceChannel,DirectoryServiceListener listener) {
    if (_pendingListeners.get(sourceChannel) == listener)
    _pendingListeners.remove(sourceChannel);
    _listeners.put(sourceChannel, listener);
  }
  
  void removeListeners(AsyncClientChannel sourceChannel) {
    DirectoryServiceListener pendingListener = _pendingListeners.get(sourceChannel);
    _pendingListeners.remove(sourceChannel);
    DirectoryServiceListener registeredListener = _listeners.get(sourceChannel);
    _listeners.remove(sourceChannel);
    if (pendingListener != null) { 
      pendingListener.disconnect();
    }
    if (registeredListener != null) { 
      registeredListener.disconnect();
    }
  }
  
  void removeListener(AsyncClientChannel sourceChannel,DirectoryServiceListener listener) {
    if (_pendingListeners.get(sourceChannel) == listener) { 
      _pendingListeners.remove(sourceChannel);
    }
    if (_listeners.get(sourceChannel) == listener) { 
      _listeners.remove(sourceChannel);
    }
  }
}
