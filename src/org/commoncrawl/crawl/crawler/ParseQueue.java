/**
 * Copyright 2008 - CommonCrawl Foundation
 * 
 * CommonCrawl licenses this file to you under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.commoncrawl.crawl.crawler;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.util.internal.Tuples.Triple;
import org.commoncrawl.util.shared.CCStringUtils;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBComparator;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.Snapshot;

import redis.clients.jedis.Jedis;

/** 
 * A queue used to manage on-demand remote parsing of select crawled documents
 * 
 * @author rana
 *
 */
public class ParseQueue {
  
  private String _redisQueueName = null;
  private File   _entryDBName = null;
  
  public static final Log LOG = LogFactory.getLog(ParseQueue.class);
  
  public static final String QUEUE_DB = "parse_queue_db";
  public static final String ENTRY_DB = "parse_entry_db";
  public static final byte[] REDIS_DOMAIN_COUNTS_KEY = "dc".getBytes();
  public static final byte[] REDIS_PRIORITY_QUEUE_LT10_KEY = "pq_lt10".getBytes();
  public static final byte[] REDIS_PRIORITY_QUEUE_GT10_KEY = "pq_gt10".getBytes();
  private DB entryDB;
  Jedis redis;
  private long queueEpoch = System.currentTimeMillis();
  private AtomicLong sequenceNo = new AtomicLong(System.currentTimeMillis());
  private int workUnitsPerTimespan;
  
  public static final int ITEM_STATE_QUEUED = 2;
  public static final int ITEM_STATE_ACTIVE = 1;
  
  public static class Item { 
    public long    _domainId;
    public long    _sequenceId;
    public byte[]  _data;
    
    public Item() {
      
    }

    /**
     * create new Item for insertion into queue 
     * @param domainId
     * @param data
     */
    public Item(long domainId, byte[] data) { 
      _domainId = domainId;
      _sequenceId = 0;
      _data     = data;
    }

    /** 
     * used by queue to populate item during pop
     * @param domainId
     * @param sequenceId
     * @param data
     */
    Item(long domainId,long sequenceId, byte[] data) { 
      _domainId = domainId;
      _sequenceId = sequenceId;
      _data     = data;
    }
  }
  private LinkedList<Item> _scheduledItems = new LinkedList<Item>();
  
  public ParseQueue(File dbPath,int redisPort,int workUnitsPerTimespan) throws IOException {
    LOG.info("Database Path is:" + dbPath);
    dbPath.mkdirs();
    Options options = new Options();
    options.createIfMissing(true);
    options.comparator(new DomainStateAndTimestampKey());
    options.paranoidChecks(true);
    entryDB = factory.open(new File(dbPath,ENTRY_DB), options);
    this.workUnitsPerTimespan = workUnitsPerTimespan;
    redis = new Jedis("localhost",redisPort);
    // clear redis 
    LOG.info("CLEARING REDIS");
    redis.flushAll();
    LOG.info("RELOAD ORPHANED RECORDS");
    reloadOrphanedRecords();
    LOG.info("REBUILD REDIS STATE ");
    loadRedis();
  }
  
  public void close() throws IOException { 
    if (entryDB != null) {
      entryDB.close();
    }
    
  }
  
  private static byte[] domainIdToBytes(long domainId) {
    return  new byte[] {
    (byte)((domainId >> 56) & 0xff),
    (byte)((domainId >> 48) & 0xff),
    (byte)((domainId >> 40) & 0xff),
    (byte)((domainId >> 32) & 0xff),
    (byte)((domainId >> 24) & 0xff),
    (byte)((domainId >> 16) & 0xff),
    (byte)((domainId >> 8) & 0xff),
    (byte)((domainId >> 0) & 0xff) 
    };
  }
  
  private static long bytesToDomainId(byte[] bytes) { 
      return (long)(0xff & bytes[0]) << 56  |
        (long)(0xff & bytes[1]) << 48  |
        (long)(0xff & bytes[2]) << 40  |
        (long)(0xff & bytes[3]) << 32  |
        (long)(0xff & bytes[4]) << 24  |
        (long)(0xff & bytes[5]) << 16  |
        (long)(0xff & bytes[6]) << 8   |
        (long)(0xff & bytes[7]) << 0;
    
  }
  
  /**
   * 
   * @param urlObject
   * @throws IOException
   */
  public long insertItemIntoQueue(Item item) throws IOException {
    // create id ... 
    long sequenceId = sequenceNo.addAndGet(1);
    // create composite key 
    byte[] key = DomainStateAndTimestampKey.createCompositeKey(item._domainId,ITEM_STATE_QUEUED,sequenceId);
    
    entryDB.put(key, item._data);
    // talk to redis 
    byte[] dominIdBytes = domainIdToBytes(item._domainId);
    long itemCount = redis.hincrBy(REDIS_DOMAIN_COUNTS_KEY,dominIdBytes,1);
    LOG.info("insertItemIntoQueue for domainId:" + item._domainId + " adding entry to DB. itemCount:" + itemCount);
    // update redis queue if necessary
    if (itemCount == 1) {
      double score = (double) (System.currentTimeMillis() - queueEpoch) / 1000;
      //LOG.info("insertItemIntoQueue domainId:" + item._domainId + " itemCount:" + itemCount + " adding to LT10 queue with score:" + score);
      redis.zadd(REDIS_PRIORITY_QUEUE_LT10_KEY,score,dominIdBytes);
    }
    // if gt > 10 .. then move queues ... 
    else if (itemCount == 11) {
      //LOG.info("insertItemIntoQueue domainId:" + item._domainId + " itemCount:" + itemCount + " moving from LT10 to GT10 queue");
      double oldScore = redis.zscore(REDIS_PRIORITY_QUEUE_LT10_KEY,dominIdBytes);
      redis.zrem(REDIS_PRIORITY_QUEUE_LT10_KEY,dominIdBytes);
      redis.zadd(REDIS_PRIORITY_QUEUE_GT10_KEY,oldScore,dominIdBytes);
    }
    else { 
      //LOG.info("insertItemIntoQueue domainId:" + item._domainId + " itemCount:" + itemCount);
    }
    return sequenceId;
  }
  
  
  
  /**
   * 
   * @param urlObject
   * @throws IOException
   */
  public Item popItemIntoFromQueue() throws IOException {
    // if queue empty 
    if (_scheduledItems.size() == 0) {
      // load the queue ...  
      fillQueue();
    }
    // now if queue is non empty ... 
    if (_scheduledItems.size() != 0) { 
      return _scheduledItems.remove();
    }
    return null;
  }
  
  
  /** 
   * delete the previously pop'ed item from the database  
   * 
   * @param item
   * @throws IOException
   */
  public void deleteItem(Item item) throws IOException { 
    // delete it from the database 
    entryDB.delete(DomainStateAndTimestampKey.createCompositeKey(item._domainId, ITEM_STATE_ACTIVE, item._sequenceId));
  }

  private void reloadOrphanedRecords() throws IOException { 
    Snapshot snapshot = entryDB.getSnapshot();
    try { 
      ReadOptions options = new ReadOptions();
      options.snapshot(snapshot);
      DBIterator iterator = entryDB.iterator(options);
     
      try { 
        for (iterator.seekToFirst();iterator.hasNext();iterator.next()) { 
          Triple<Long,Long,Integer> oldKey 
            = DomainStateAndTimestampKey.fromBytes(iterator.peekNext().getKey());
          if (oldKey.e2 == ITEM_STATE_ACTIVE) {
            LOG.info("Found Orphaned Record for Domain:" + oldKey.e0);
            // flip to inactive state 
            byte[] newKey 
              = DomainStateAndTimestampKey.createCompositeKey(
                  oldKey.e0, ITEM_STATE_QUEUED, oldKey.e1);
            // delete using old key ...  
            entryDB.delete(iterator.peekNext().getKey());
            // reinsert using new key ... 
            entryDB.put(newKey,iterator.peekNext().getValue());
          }
        }
      }
      finally { 
        iterator.close();
      }
    }
    finally { 
      snapshot.close();
    }
  }
  
  private void loadRedis()throws IOException { 
    DBIterator iterator = entryDB.iterator();
    
    try {
      
      long lastDomainId = 0;
      long lastDomainCount = 0;
      long firstTimestamp = 0;
      for (iterator.seekToFirst();iterator.hasNext();iterator.next()) { 
        long currentDomainId = bytesToDomainId(iterator.peekNext().getKey());
        if (currentDomainId != lastDomainId) { 
          if (lastDomainCount != 0) {
            double score = (double) (firstTimestamp - queueEpoch) / 1000;
            LOG.info("Inserting DomainId:" + lastDomainId + " score:" + score);
            redis.hincrBy(REDIS_DOMAIN_COUNTS_KEY,domainIdToBytes(lastDomainId), lastDomainCount);
            
            redis.zadd(
                (lastDomainCount <= 10) ? 
                    REDIS_PRIORITY_QUEUE_LT10_KEY:REDIS_PRIORITY_QUEUE_GT10_KEY,
                    score,
                    domainIdToBytes(lastDomainId));
          }
          lastDomainCount = 1;
          lastDomainId = currentDomainId;
          firstTimestamp = DomainStateAndTimestampKey.getTimestampFromKey(iterator.peekNext().getKey());
        }
      }
      if (lastDomainCount != 0) { 
        redis.hincrBy(REDIS_DOMAIN_COUNTS_KEY,domainIdToBytes(lastDomainId), lastDomainCount);
        double score = (double) (firstTimestamp - queueEpoch) / 1000;
        redis.zadd(
            (lastDomainCount <= 10) ? 
                REDIS_PRIORITY_QUEUE_LT10_KEY:REDIS_PRIORITY_QUEUE_GT10_KEY,
                score,
                domainIdToBytes(lastDomainId));
        LOG.info("Inserting DomainId:" + lastDomainId + " score:" + score);
        
      }
    }
    finally { 
      iterator.close();
    }
  }
  
  private void fillQueue() throws IOException {
    LOG.info("In fillQueue");
    int unitsForSmallDomains = workUnitsPerTimespan / 3;

    // figure out timespan units
    int unitsRemaining = workUnitsPerTimespan;
    // make two passes to populate queues ... 
    for (int pass=0;pass<2;++pass) {
      // figure out queue name based on pass 
      byte[] queueName = (pass == 0) ? REDIS_PRIORITY_QUEUE_LT10_KEY : REDIS_PRIORITY_QUEUE_GT10_KEY;
      // get up to max possible keys ... 
      Set<byte[]> keys = redis.zrange(queueName,0,unitsRemaining);
      // figure out units to try and acquire
      int unitsToAcquire = Math.min(unitsRemaining,(pass == 0) ? unitsForSmallDomains : unitsRemaining);
      // special case .. if LT queue, see if go all out if large queue is empty ... 
      if (pass == 0 && redis.zcard(REDIS_PRIORITY_QUEUE_GT10_KEY) == 0) { 
        unitsToAcquire = unitsRemaining;
      }
      LOG.info("Pass:" + pass + " redisSetSize: " + keys.size() + " unitsToAcquire:" + unitsToAcquire);
      
      // keep counts by domain 
      HashMap<byte[],Integer> counts = new HashMap<byte[],Integer>();
      
      HashSet<byte[]> emptyDomainSet = new HashSet<byte[]>();
      int itemsAcquiredThisPass = 0;
      while (unitsToAcquire > 0 && emptyDomainSet.size() != keys.size()) {
        // walk keys ... 
        for (byte[] domainKey : keys) {
          if (!emptyDomainSet.contains(domainKey)) { 
            // pop item from database ... 
            Item item = popNextItemFromDatabase(domainKey);
            if (item != null) { 
              // schedule it... 
              _scheduledItems.add(item);
              // decerement aggregate count ...
              unitsToAcquire--;
              itemsAcquiredThisPass++;
              // increment localized count ..
              Integer existingCount = counts.get(domainKey);
              if (existingCount == null) { 
                counts.put(domainKey,1);
              }
              else{ 
                counts.put(domainKey,existingCount.intValue() + 1);
              }
              if (unitsToAcquire == 0) 
               break;
            }
            else { 
              // add to empty domain set ... 
              emptyDomainSet.add(domainKey);
            }
          }
        }
      }
      unitsRemaining -= itemsAcquiredThisPass;
      LOG.info("update RedisQueues for pass:" + pass + " itemsAcquired:" + itemsAcquiredThisPass + " unitsRemaining:" + unitsRemaining);
      // ok clear redis counts for domains operated on ..
      updateRedisQueues(queueName,counts);
    }      
  }
  private void updateRedisQueues(byte[] queueName,HashMap<byte[],Integer> counts)throws IOException {
    for (Map.Entry<byte[],Integer> countEntry : counts.entrySet()) { 
      // decrement redis count 
      long newCount = redis.hincrBy(REDIS_DOMAIN_COUNTS_KEY,countEntry.getKey(),-countEntry.getValue());
      if (newCount <= 0) { 
        LOG.info("Count for Domain:" + bytesToDomainId(countEntry.getKey()) + " is zero. Removing from sets/maps");
        // ok the domain is empty ...remove from both counts map and queue 
        redis.hdel(REDIS_DOMAIN_COUNTS_KEY, countEntry.getKey());
        // delete from source queue as well 
        redis.zrem(queueName,countEntry.getKey());
      }
      else if (newCount <=10 && queueName != REDIS_PRIORITY_QUEUE_LT10_KEY) {
        LOG.info("Count for Domain:" + bytesToDomainId(countEntry.getKey()) + " LTEQ 10 but in wrong queue. moving");
        // need to remove from high priority queue ...  
        redis.zrem(REDIS_PRIORITY_QUEUE_GT10_KEY, countEntry.getKey());
      }
      // ok now add back to queue if count != 0
      if (newCount > 0) { 
        byte[] finalQueueName = (newCount <= 10) ?  REDIS_PRIORITY_QUEUE_LT10_KEY:REDIS_PRIORITY_QUEUE_GT10_KEY;
        // set new score value 
        double score = (double) (System.currentTimeMillis() - queueEpoch) / 1000;
        redis.zadd(finalQueueName,score,countEntry.getKey());
        LOG.info("Count for Domain:" + bytesToDomainId(countEntry.getKey()) 
            + " is:" 
            + newCount 
            + " assinging Scroe:" 
            + score 
            + " Queue:" 
            + new String(finalQueueName));
      }
    }
  }
  
  private Item popNextItemFromDatabase(byte[] targetIdBytes) throws IOException {
    // allocate space for a potential item ... 
    Item itemOut = null;
    // construct an iterator 
    DBIterator iterator = entryDB.iterator();
    try { 
      // 
      iterator.seek(DomainStateAndTimestampKey.createCompositeKey(targetIdBytes,ITEM_STATE_QUEUED,Long.MIN_VALUE));
      
      if (iterator.hasNext()) { 
        long targetId = bytesToDomainId(targetIdBytes);
        Triple<Long,Long,Integer> compositeKey = DomainStateAndTimestampKey.fromBytes(iterator.peekNext().getKey());
        if (targetId == compositeKey.e0) { 
          // create item using domain id and timestamp from composite key and value bytes ... 
          itemOut = new Item(compositeKey.e0,compositeKey.e1,iterator.peekNext().getValue());
          // delete it from the database 
          entryDB.delete(iterator.peekNext().getKey());
          // and then reinsert with a new key ...
          entryDB.put(
              DomainStateAndTimestampKey.createCompositeKey(
                  compositeKey.e0,ITEM_STATE_ACTIVE,compositeKey.e1),iterator.peekNext().getValue());
        }
      }
    }
    finally { 
      iterator.close();
    }
    LOG.info("popNextItemFromDatabase for domainId:" + bytesToDomainId(targetIdBytes) + " returned Item:" + itemOut);
    return itemOut;
  }

  static class DomainStateAndTimestampKey implements DBComparator {
    
    public int compare(byte[] key1, byte[] key2) {
      long domainId1 
        = (long)(0xff & key1[0]) << 56  |
          (long)(0xff & key1[1]) << 48  |
          (long)(0xff & key1[2]) << 40  |
          (long)(0xff & key1[3]) << 32  |
          (long)(0xff & key1[4]) << 24  |
          (long)(0xff & key1[5]) << 16  |
          (long)(0xff & key1[6]) << 8   |
          (long)(0xff & key1[7]) << 0;

      long domainId2 
        = (long)(0xff & key2[0]) << 56  |
          (long)(0xff & key2[1]) << 48  |
          (long)(0xff & key2[2]) << 40  |
          (long)(0xff & key2[3]) << 32  |
          (long)(0xff & key2[4]) << 24  |
          (long)(0xff & key2[5]) << 16  |
          (long)(0xff & key2[6]) << 8   |
          (long)(0xff & key2[7]) << 0;
      
      int result = 
          (domainId1<domainId2) ? -1 : 
              (domainId1 > domainId2) ? 1 :0;
      if (result == 0) { 
        int state1 = (0xff & key1[8]);
        int state2 = (0xff & key2[8]);
        
        result = (state1 < state2) ? -1 : (state1 > state2) ? 1: 0;
        if (result == 0) { 
          long timestamp1 
          = (long)(0xff & key1[9]) << 56  |
            (long)(0xff & key1[10]) << 48  |
            (long)(0xff & key1[11]) << 40  |
            (long)(0xff & key1[12]) << 32  |
            (long)(0xff & key1[13]) << 24  |
            (long)(0xff & key1[14]) << 16  |
            (long)(0xff & key1[15]) << 8   |
            (long)(0xff & key1[16]) << 0;
  
          long timestamp2 
            = (long)(0xff & key2[9]) << 56  |
              (long)(0xff & key2[10]) << 48  |
              (long)(0xff & key2[11]) << 40  |
              (long)(0xff & key2[12]) << 32  |
              (long)(0xff & key2[13]) << 24  |
              (long)(0xff & key2[14]) << 16  |
              (long)(0xff & key2[15]) << 8   |
              (long)(0xff & key2[16]) << 0;
         
          result = 
              (timestamp1<timestamp2) ? -1 : 
                  (timestamp1 > timestamp2) ? 1 :0;
        }
      }
      return result;
    }
    
    public byte[] findShortestSeparator(byte[] start, byte[] limit) {
        return start;
    }
    public byte[] findShortSuccessor(byte[] key) {
        return key;
    }
    @Override
    public String name() {
      return "WorkQueue_Comparator";
    }
    
    public static final int COMPOSITE_KEY_SIZE = 8 + 1 + 8;
    public static byte[] createCompositeKey(byte[] targetArray,long domainId,int state,long timestamp)throws IOException {
      if (targetArray == null) { 
        targetArray = new byte[COMPOSITE_KEY_SIZE];
      }
      else { 
        if (targetArray.length < COMPOSITE_KEY_SIZE)
          throw new IOException("Invalid Target Array Size!");
      }
      
      targetArray[0] = (byte)((domainId >> 56) & 0xff);
      targetArray[1] = (byte)((domainId >> 48) & 0xff);
      targetArray[2] = (byte)((domainId >> 40) & 0xff);
      targetArray[3] = (byte)((domainId >> 32) & 0xff);
      targetArray[4] = (byte)((domainId >> 24) & 0xff);
      targetArray[5] = (byte)((domainId >> 16) & 0xff);
      targetArray[6] = (byte)((domainId >> 8) & 0xff);
      targetArray[7] = (byte)((domainId >> 0) & 0xff);
      
      targetArray[8] = (byte)(state & 0xff);
      
      targetArray[9] = (byte)((timestamp >> 56) & 0xff);
      targetArray[10] = (byte)((timestamp >> 48) & 0xff);
      targetArray[11] = (byte)((timestamp >> 40) & 0xff);
      targetArray[12] = (byte)((timestamp >> 32) & 0xff);
      targetArray[13] = (byte)((timestamp >> 24) & 0xff);
      targetArray[14] = (byte)((timestamp >> 16) & 0xff);
      targetArray[15] = (byte)((timestamp >> 8) & 0xff);
      targetArray[16] = (byte)((timestamp >> 0) & 0xff);
      
      return targetArray;
    }
    
    public static byte[] createCompositeKey(long domainId,int state,long timestamp) {
      return new byte[] { 
          (byte)((domainId >> 56) & 0xff),
          (byte)((domainId >> 48) & 0xff),
          (byte)((domainId >> 40) & 0xff),
          (byte)((domainId >> 32) & 0xff),
          (byte)((domainId >> 24) & 0xff),
          (byte)((domainId >> 16) & 0xff),
          (byte)((domainId >> 8) & 0xff),
          (byte)((domainId >> 0) & 0xff),
          
          (byte)(state & 0xff),
          
          (byte)((timestamp >> 56) & 0xff),
          (byte)((timestamp >> 48) & 0xff),
          (byte)((timestamp >> 40) & 0xff),
          (byte)((timestamp >> 32) & 0xff),
          (byte)((timestamp >> 24) & 0xff),
          (byte)((timestamp >> 16) & 0xff),
          (byte)((timestamp >> 8) & 0xff),
          (byte)((timestamp >> 0) & 0xff)
      };
    }

    public static byte[] createCompositeKey(byte[] domainId,int state,long timestamp) {
      return new byte[] { 
          (byte)domainId[0],
          (byte)domainId[1],
          (byte)domainId[2],
          (byte)domainId[3],
          (byte)domainId[4],
          (byte)domainId[5],
          (byte)domainId[6],
          (byte)domainId[7],
          
          (byte) (state & 0xff),
          
          (byte)((timestamp >> 56) & 0xff),
          (byte)((timestamp >> 48) & 0xff),
          (byte)((timestamp >> 40) & 0xff),
          (byte)((timestamp >> 32) & 0xff),
          (byte)((timestamp >> 24) & 0xff),
          (byte)((timestamp >> 16) & 0xff),
          (byte)((timestamp >> 8) & 0xff),
          (byte)((timestamp >> 0) & 0xff)
      };
    }
    
    public static long getTimestampFromKey(byte[] key) { 
      return 
          (long)(0xff & key[9]) << 56  |
          (long)(0xff & key[10]) << 48  |
          (long)(0xff & key[11]) << 40  |
          (long)(0xff & key[12]) << 32  |
          (long)(0xff & key[13]) << 24  |
          (long)(0xff & key[14]) << 16  |
          (long)(0xff & key[15]) << 8   |
          (long)(0xff & key[16]) << 0;
    }
    
    public static Triple<Long, Long, Integer> fromBytes(byte[] key) { 
      long domainId
      = (long)(0xff & key[0]) << 56  |
        (long)(0xff & key[1]) << 48  |
        (long)(0xff & key[2]) << 40  |
        (long)(0xff & key[3]) << 32  |
        (long)(0xff & key[4]) << 24  |
        (long)(0xff & key[5]) << 16  |
        (long)(0xff & key[6]) << 8   |
        (long)(0xff & key[7]) << 0;
      
      int state = (0xff & key[8]);
      
      long timestamp
      = (long)(0xff & key[9]) << 56  |
        (long)(0xff & key[10]) << 48  |
        (long)(0xff & key[11]) << 40  |
        (long)(0xff & key[12]) << 32  |
        (long)(0xff & key[13]) << 24  |
        (long)(0xff & key[14]) << 16  |
        (long)(0xff & key[15]) << 8   |
        (long)(0xff & key[16]) << 0;
      
      return new Triple<Long,Long,Integer>(domainId,timestamp,state);
      
    }
  };
  
  public static String createTestValue(int index,int count) { 
    StringBuffer buf = new StringBuffer();
    for (int i=0;i<count+1;++i) { 
      buf.append(index);
    }
    return buf.toString();
  }
  

  
  
  
  public static void main(String[] args) throws IOException {
    ParseQueue queue = new ParseQueue(new File("/home/rana/ccprod/data/ParseQueue_Test"),6379,20);
    
    try { 
      for (int i=0;i<10000;++i) { 
        long domainId = (long)(Math.random() * 1000);
        long timestamp = System.currentTimeMillis();
        String dataStr = Long.toString(domainId) + ":"+ Long.toString(timestamp);
        LOG.info("Inserting:" + dataStr);
        queue.insertItemIntoQueue(new Item(domainId,dataStr.getBytes()));
      }
      
      Item itemOut = null;
      while ((itemOut = queue.popItemIntoFromQueue()) != null) { 
        LOG.info("Got Item:" + new String(itemOut._data) + " DELETING IT");
        queue.deleteItem(itemOut);
      }
      
      LOG.info("Done");
    }
    catch (Exception e) { 
      LOG.error(CCStringUtils.stringifyException(e));
    }
    finally { 
      LOG.info("Closing Database");
      queue.close();
      LOG.info("Closed Database");
    }
    LOG.info("Sleeping");
    try {
      Thread.sleep(20000);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    LOG.info("Done Sleeping");
  }
  
}
