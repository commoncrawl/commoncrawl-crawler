package org.commoncrawl.service.statscollector;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.record.Buffer;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.commoncrawl.async.CallbackWithResult;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.async.Timer;
import org.commoncrawl.async.ConcurrentTask.CompletionCallback;
import org.commoncrawl.rpc.base.internal.AsyncRequest.Callback;
import org.commoncrawl.rpc.base.shared.BinaryProtocol;
import org.commoncrawl.rpc.base.shared.RPCStruct;
import org.commoncrawl.service.statscollector.TestRecord;
import org.commoncrawl.util.AsyncAppender;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.TimeSeriesDataFile;
import org.commoncrawl.util.TimeSeriesDataFile.KeyValueTuple;
import org.commoncrawl.util.time.Day;
import org.commoncrawl.util.time.Hour;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;

/** abstract class representing a collection of hourly & daily stats
 * 
 * @author rana
 *
 */
public abstract class StatsCollection<ValueType extends Comparable> {
  
  private StatsLogManager _logManager;
  protected String    _groupKey;
  protected String    _uniqueKey;
  private TreeMultimap<Hour,ValueType> _hourlyValues = TreeMultimap.create();
  private Hour      _lowestEventTime = null;
  private Hour      _highestEventTime = null;
  private Day       _lastDailyValue = null;
  TimeSeriesDataFile<BytesWritable> _sequentialEventsFile = null;
  TimeSeriesDataFile<BytesWritable> _dailyEventsFile = null;
  private static final Log LOG = LogFactory.getLog(StatsCollection.class);
  
  
  private static final String PERIODIC_FILE_TYPE = "events";
  private static final String DAILY_FILE_TYPE = "daily";
  
  public StatsCollection(StatsLogManager logFileManager,String groupKey,String uniqueKey) throws IOException { 
    _groupKey = groupKey;
    _uniqueKey = uniqueKey;
    _logManager = logFileManager;
    _sequentialEventsFile   = logFileManager.getFileGivenName(_groupKey, _uniqueKey, PERIODIC_FILE_TYPE);
    _dailyEventsFile        = logFileManager.getFileGivenName(_groupKey, _uniqueKey, DAILY_FILE_TYPE);
    
    loadLastStateFromDisk();
    
  }
  
  /** add an event to the collection 
   * 
   * @param hour
   * @param event
   */
  public final void addValue(long timestamp,ValueType value)throws IOException { 

    Hour hour = new Hour(new Date(timestamp));
    
    // first add value to set 
    SortedSet<ValueType> values = _hourlyValues.get(hour);
    if (values.size() != 0) { 
      combineHourlyValues(values.first(),value);
    }
    else { 
      values.add(value);
    }

    if (_highestEventTime != null && _highestEventTime.getDay().compareTo(hour.getDay()) != 0) {
      if (_lastDailyValue == null || _highestEventTime.getDay().compareTo(_lastDailyValue) != 0) { 
        // potentially flush or truncate ... 
        flushPreviosuDaysEvents((Day)hour.getDay().previous());
      }
    }

    // if locally cached events span more than a 24 hour time
    if (_lowestEventTime != null && 
        hour.getSerialIndex() - _lowestEventTime.getSerialIndex() > 24) {
    
      // now truncate hourly events if necessary 
      truncateEvents(hour);
    }
        
    // update highest lowest event times
    updateEventTimes(hour);
    
    // flush to event log 
    writeToSequentialEventLog(timestamp,value);
  }

  /** return hourly values sorted 
   * 
   * @return sorted multi map containing hourly values 
   */
  public TreeMultimap<Hour,ValueType> getHourlyValues() {
    return _hourlyValues;   
  }
  
  
  public void getDailyValues(final EventLoop eventLoop,final int maxDays,final CompletionCallback<ImmutableSortedMap<Day,ValueType>> callback) {
    // schedule a disk read 
    _logManager.queueDiskIORequest(new Runnable() {

      @Override
      public void run() {
        try {
          // get the result set ... 
          ArrayList<KeyValueTuple<Long, BytesWritable>> tuples = _dailyEventsFile.readFromTail(maxDays, -1);
          // walk items adding to builder 
          final ImmutableSortedMap.Builder<Day,ValueType> builder = ImmutableSortedMap.naturalOrder();
          
          for (KeyValueTuple<Long,BytesWritable> tuple : tuples) { 
            ValueType value = bufferToValueType(new Buffer(tuple.value.get()));
            builder.put(new Day(new Date(tuple.key)), value);
          }
          
          eventLoop.setTimer(new Timer(0,false,new Timer.Callback() {
            
            @Override
            public void timerFired(Timer timer) {
              callback.taskComplete(builder.build());
            }
          }));
        } catch (final IOException e) {
          LOG.error(CCStringUtils.stringifyException(e));

          eventLoop.setTimer(new Timer(0,false,new Timer.Callback() {
            
            @Override
            public void timerFired(Timer timer) {
              callback.taskFailed(e);
            }
          }));
        }
      } 
    });
  }

  private final void truncateEvents(final Hour newEventTime) {
    final long newEventSerialIndex = newEventTime.getSerialIndex();
    // create a filtered set 
    Hour hoursToRemove[]  = Sets.filter(_hourlyValues.keySet(), new Predicate<Hour>() {

      @Override
      public boolean apply(Hour hour) {
        return (newEventSerialIndex - hour.getSerialIndex() > 24);
      }
    
    }).toArray(new Hour[0]);

    // remove items from set
    for (Hour hour : hoursToRemove)
      _hourlyValues.removeAll(hour);
    // update lowest / higest values ... 
    SortedSet<Hour> resultSet = _hourlyValues.keySet();
    if (resultSet.size() != 0) { 
      _lowestEventTime = resultSet.first();
      _highestEventTime = resultSet.last();
    }
    else { 
      _lowestEventTime = null;
      _highestEventTime = null;
    }
  }
  
  private final void flushPreviosuDaysEvents(final Day dayToFlush) throws IOException { 
        
    // flush previous days event to disk
    Set<Map.Entry<Hour,ValueType>> previousDaysEvents = Sets.filter(_hourlyValues.entries(), new Predicate<Map.Entry<Hour,ValueType>>() {
  
      @Override
      public boolean apply(Entry<Hour, ValueType> entry) {
        return dayToFlush.equals(entry.getKey().getDay());
      }
  
    });
    
    // ok now consolidate previous days events into one event 
    if (previousDaysEvents.size() != 0) { 
      
      ValueType finalValue = createDailyValue(previousDaysEvents);
      
      // ok potentially flush this event to disk 
      writeToDailyEventLog(dayToFlush,finalValue);
      
      // update last daily value ...
      _lastDailyValue = dayToFlush;
    }
  }
  
  private final void updateEventTimes(Hour newestEventTime) { 
    // update event times 
    if (_highestEventTime == null ) { 
      _lowestEventTime = newestEventTime;
    }
    _highestEventTime = newestEventTime;
  }
  
  private void loadLastStateFromDisk() throws IOException { 
    // ok, first see if we can extract last timestamp out of the daily events file 
    long lastDailyTimestamp = _dailyEventsFile.getLastRecordKey();
    long restrictByTime = -1;
    if (lastDailyTimestamp != -1) { 
      // convert to Day if valid ... 
      _lastDailyValue = new Day(new Date(lastDailyTimestamp));
    }
    // ok now read up to 1000 events from events file  
    ArrayList<KeyValueTuple<Long,BytesWritable>> events = _sequentialEventsFile.readFromTail(1000, -1);
    
    // ok find latest day that is not today 
    Day   today = new Day(new Date(System.currentTimeMillis()));
    Hour  thisHour = new Hour(new Date(System.currentTimeMillis()));
    Day   yesterday = (Day) today.previous();
     
    // walk events in reverse order  
    Iterable<KeyValueTuple<Long, BytesWritable>> reverseList = Iterables.reverse(events);
    for (KeyValueTuple<Long, BytesWritable> event : reverseList) {
      // collect all events up to yesterday's events 
      Day eventDay = new Day(new Date(event.key));
      
      if (eventDay.compareTo(yesterday) != -1) { 
        // process the event ... 
        Hour eventHour = new Hour(new Date(event.key));
        // create the typed object ... 
        ValueType value = bufferToValueType(new Buffer(event.value.get()));
        // add it to event list ... 
        SortedSet<ValueType> values = _hourlyValues.get(eventHour);
        if (values.size() != 0) { 
          combineHourlyValues(values.first(),value);
        }
        else { 
          values.add(value);
        }        
      }
      else { 
        break;
      }
    }
    
    // flush preivous days events ... 
    if (_lastDailyValue != null && _lastDailyValue.compareTo(yesterday) != 0) { 
      flushPreviosuDaysEvents(yesterday);
    }
    // truncate to 24 hours period 
    truncateEvents(thisHour);

  }
  
 
  
  private final void writeToSequentialEventLog(final long timestamp,ValueType value) throws IOException { 
    final Buffer buffer = valueTypeToBuffer(value);
    BytesWritable dataOut = new BytesWritable(buffer.get());
    dataOut.setSize(buffer.getCount());
    _logManager.queueDiskIORequest(new Runnable() {

      @Override
      public void run() {
        try {
          _sequentialEventsFile.appendRecordToLogFile(timestamp, new BytesWritable(buffer.get()));
        } catch (IOException e) {
          LOG.error(CCStringUtils.stringifyException(e));
        }
      }
    });
  }
  
  /**
   * flush this value to the daily values log 
   * 
   * @param day
   * @param finalValue
   */
  private final void writeToDailyEventLog(final Day day,final ValueType finalValue) throws IOException { 
    final Buffer buffer = valueTypeToBuffer(finalValue);
    BytesWritable dataOut = new BytesWritable(buffer.get());
    dataOut.setSize(buffer.getCount());
    _logManager.queueDiskIORequest(new Runnable() {

      @Override
      public void run() {
        try {
          _dailyEventsFile.appendRecordToLogFile(day.getFirstMillisecond(), new BytesWritable(buffer.get()));
        } catch (IOException e) {
          LOG.error(CCStringUtils.stringifyException(e));
        }
      } 
    });
  }
  
  /**
   * combine two values into one value
   * @param sourceValue the value to collapse into 
   * @param otherValue  the other value 
   */
  public abstract void combineHourlyValues(ValueType sourceValue,ValueType otherValue);
  
  
  /**
   * aggregate hourly values to create a daily value 
   * 
   * @return new ValueType daily value instance 
   */
  public abstract ValueType createDailyValue(Set<Map.Entry<Hour,ValueType>> hourlyValueSet);
  
  
  /** 
   * 
   * @param incomingBuffer incoming serialized data buffer 
   * @return deserialized value type
   * @throws IOException
   */
  public abstract ValueType bufferToValueType(Buffer incomingBuffer) throws IOException;
  
  /**
   * 
   * @param value value type instance 
   * @return buffer containing serialied value type
   * @throws IOException
   */
  public abstract Buffer    valueTypeToBuffer(ValueType value) throws IOException;
  
  /**
   * 
   * @param value
   * @throws IOException
   */
  public abstract void setUniqueKeyInValue(ValueType value);
  
  
  /** allocate empty hourly value **/
  public abstract ValueType allocateValueType();
  
  
  private static class TestStatsCollection extends StatsCollection<TestRecord> {

    public TestStatsCollection(StatsLogManager logFileManager, String groupKey,String uniqueKey) throws IOException {
      super(logFileManager, groupKey, uniqueKey);
    }

    @Override
    public TestRecord bufferToValueType(Buffer incomingBuffer) throws IOException {
      DataInputBuffer buffer = new DataInputBuffer();
      buffer.reset(incomingBuffer.get(),0, incomingBuffer.getCount());
      TestRecord recordOut = new TestRecord();
      recordOut.deserialize(buffer,new BinaryProtocol());
      return recordOut;
    }

    @Override
    public void combineHourlyValues(TestRecord sourceValue, TestRecord otherValue) {
      sourceValue.setAverageValue( (sourceValue.getAverageValue() + otherValue.getAverageValue()) / 2.0f );
      sourceValue.setCumilativeValue(sourceValue.getCumilativeValue() + otherValue.getCumilativeValue());
    }

    @Override
    public TestRecord createDailyValue(Set<Entry<Hour, TestRecord>> hourlyValueSet) {
      TestRecord recordOut = new TestRecord();
      
      float averageValue = 0.0f;
      for (Entry<Hour, TestRecord> entry : hourlyValueSet) { 
        averageValue += entry.getValue().getAverageValue();
        recordOut.setCumilativeValue(recordOut.getCumilativeValue() + entry.getValue().getCumilativeValue());
      }
      recordOut.setAverageValue(averageValue / hourlyValueSet.size());
      
      return recordOut;
    }

    @Override
    public Buffer valueTypeToBuffer(TestRecord value) throws IOException {
      DataOutputBuffer bufferOut = new DataOutputBuffer();
      value.serialize(bufferOut, new BinaryProtocol());
      return new Buffer(bufferOut.getData(),0,bufferOut.getLength());
    }

    @Override
    public void setUniqueKeyInValue(TestRecord value) {
      
    }

    @Override
    public TestRecord allocateValueType() {
      return new TestRecord();
    } 
  }
  
  public void dumpHourlyToJSON(OutputStream stream)throws IOException { 
    JsonFactory f = new JsonFactory();
    JsonGenerator g = f.createJsonGenerator(stream,JsonEncoding.UTF8);
    ObjectMapper mapper = new ObjectMapper();
    
    g.writeStartArray();
    Hour now = new Hour(new Date(System.currentTimeMillis()));
    
    Set<Entry<Hour,ValueType>> set = _hourlyValues.entries();
    
    for (Entry<Hour,ValueType> item : set) { 
      // skip latest hour 
      if (item.getKey().compareTo(now) != 0) { 
        mapper.writeValue(g, item.getValue());
       }
    }
    g.writeEndArray();
  }
  
  public void collectHourlyStats(Multimap<Date,ValueType> multiMap) throws IOException { 
    
    Hour lastHourToCollect = (Hour) new Hour(new Date(System.currentTimeMillis())).previous();
    Hour firstHourToCollect = new Hour(lastHourToCollect.getHour(),(Day)lastHourToCollect.getDay().previous()); 
    
    while (lastHourToCollect.getSerialIndex() >= firstHourToCollect.getSerialIndex()) {
      SortedSet<ValueType> values = _hourlyValues.get(lastHourToCollect);
      ValueType value = null;
      if (values.size() == 0) {
        value = allocateValueType();
      }
      else { 
        value = values.first();
      }
      setUniqueKeyInValue(value);
      multiMap.put(new Date(lastHourToCollect.getFirstMillisecond()),value);
      lastHourToCollect = (Hour) lastHourToCollect.previous();
    }
    /*
    Set<Entry<Hour,ValueType>> set = _hourlyValues.entries();
    
    for (Entry<Hour,ValueType> item : set) { 
      // skip latest hour 
      if (item.getKey().compareTo(now) != 0) {
        setUniqueKeyInValue(item.getValue());
        multiMap.put(new Date(item.getKey().getFirstMillisecond()),item.getValue());
       }
    }
    */
  }
  
  public void  dumpDailyToJSON(final OutputStream stream,final CallbackWithResult<Boolean> completionCallback) throws IOException { 

    getDailyValues(_logManager.getEventLoop(), 200, new CompletionCallback<ImmutableSortedMap<Day,ValueType>>() {

      @Override
      public void taskComplete(ImmutableSortedMap<Day, ValueType> loadResult) {
        try { 
          JsonFactory f = new JsonFactory();
          JsonGenerator g = f.createJsonGenerator(stream,JsonEncoding.UTF8);
          ObjectMapper mapper = new ObjectMapper();
          
          g.writeStartArray();
  
          for (Entry<Day,ValueType> entry : loadResult.entrySet()) { 
            mapper.writeValue(g, entry.getValue());
          }
          g.writeEndArray();
          
          completionCallback.execute(new Boolean(true));
        }
        catch (IOException e){ 
          LOG.error(CCStringUtils.stringifyException(e));
          completionCallback.execute(new Boolean(false));
        }
      }

      @Override
      public void taskFailed(Exception e) {
        LOG.error(CCStringUtils.stringifyException(e));
        completionCallback.execute(new Boolean(false));
      }
    });
  }
  
  public void collectDailyStats(final Multimap<Date,ValueType> multiMap,final CallbackWithResult<Boolean> completionCallback) throws IOException { 
    
    LOG.info("CollectDailyStats called for:" + _groupKey+"-" + _uniqueKey);
    getDailyValues(_logManager.getEventLoop(), 200, new CompletionCallback<ImmutableSortedMap<Day,ValueType>>() {

      @Override
      public void taskComplete(ImmutableSortedMap<Day, ValueType> loadResult) {
        LOG.info("Daily Value Load completed for:" + _groupKey+"-" + _uniqueKey + " resultCount:" + loadResult.size());
        synchronized (multiMap) { 
          for (Entry<Day,ValueType> entry : loadResult.entrySet()) {
            LOG.info("Adding Daily Entry for:" +  _groupKey+"-" + _uniqueKey + " Date:" + new Date(entry.getKey().getFirstMillisecond()));
            // set record affinity 
            setUniqueKeyInValue(entry.getValue());
            multiMap.put(new Date(entry.getKey().getFirstMillisecond()), entry.getValue());
          }
        }
        completionCallback.execute(new Boolean(true));
      }

      @Override
      public void taskFailed(Exception e) {
        LOG.error(CCStringUtils.stringifyException(e));
        completionCallback.execute(new Boolean(false));
      }
    });
  }
  
  /******* TEST CODE ********/
  public static void main(String[] args) {
    
    EventLoop eventLoop = new EventLoop();
    eventLoop.start();
    
    try { 
      StatsLogManager logManager = new StatsLogManager(null,new File("/tmp"));
      TestStatsCollection statsCollection = new TestStatsCollection(logManager, "test", "001");
      
      Day today = new Day(new Date(System.currentTimeMillis()));
      Day yesterday = (Day) today.previous();
      Day dayBeforeYesterday = (Day) yesterday.previous();
      
      TestRecord recordTest = new TestRecord();
      
      
      
      for (int i=0;i<=47;++i) { 
      
        Hour hour = new Hour(i,dayBeforeYesterday);
        
        recordTest.setCumilativeValue(1);
        recordTest.setAverageValue(10.0f);
        
        try { 
          statsCollection.addValue(hour.getFirstMillisecond(),(TestRecord) recordTest.clone());
          statsCollection.addValue(hour.getFirstMillisecond()+1,(TestRecord) recordTest.clone());
          statsCollection.addValue(hour.getFirstMillisecond()+2,(TestRecord) recordTest.clone());
        }
        catch (CloneNotSupportedException e ) { 
          e.printStackTrace();
        }
        
      }
      
      Thread.sleep(100);
      
      statsCollection = new TestStatsCollection(logManager, "test", "001");
      
      Hour now = new Hour(new Date(System.currentTimeMillis()));
      
      for (int i=0;i<=now.getHour();++i) { 
        
        Hour hour = new Hour(i,today);
        
        recordTest.setCumilativeValue(1);
        recordTest.setAverageValue(10.0f);
        
        try { 
          statsCollection.addValue(hour.getFirstMillisecond(),(TestRecord) recordTest.clone());
          statsCollection.addValue(hour.getFirstMillisecond()+1,(TestRecord) recordTest.clone());
          statsCollection.addValue(hour.getFirstMillisecond()+2,(TestRecord) recordTest.clone());
        }
        catch (CloneNotSupportedException e ) { 
          e.printStackTrace();
        }
      }
      
      Thread.sleep(100);

      statsCollection = new TestStatsCollection(logManager, "test", "001");
      
      TreeMultimap<Hour,TestRecord> hourlyValues = statsCollection.getHourlyValues();
      
      for (Map.Entry<Hour,TestRecord> entry : hourlyValues.entries()) { 
        System.out.println(entry.getKey().toString() + ":" + entry.getValue().getAverageValue() + "," + entry.getValue().getCumilativeValue());
      }

      final Semaphore blockingSemaphore = new Semaphore(0);
      
      statsCollection.getDailyValues(eventLoop, 30, new CompletionCallback<ImmutableSortedMap<Day,TestRecord>>() {

        @Override
        public void taskComplete(ImmutableSortedMap<Day, TestRecord> loadResult) {
          for (Entry<Day,TestRecord> entry : loadResult.entrySet()) { 
            System.out.println("Daily Record. Day:" + entry.getKey().toString() + ":" + entry.getValue().getAverageValue() + "," + entry.getValue().getCumilativeValue());
          }
          blockingSemaphore.release();
        }

        @Override
        public void taskFailed(Exception e) {
          LOG.error(CCStringUtils.stringifyException(e));
          blockingSemaphore.release();
        }
      });
      
      blockingSemaphore.acquireUninterruptibly();
      
      eventLoop.stop();
      logManager.shutdown();
    }
    catch (IOException e) { 
     e.printStackTrace(); 
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    
  }
  
}
