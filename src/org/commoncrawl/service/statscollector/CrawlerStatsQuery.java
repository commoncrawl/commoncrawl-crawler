package org.commoncrawl.service.statscollector;


import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;
import com.google.visualization.datasource.Capabilities;
import com.google.visualization.datasource.DataSourceHelper;
import com.google.visualization.datasource.DataSourceRequest;
import com.google.visualization.datasource.QueryPair;
import com.google.visualization.datasource.base.DataSourceException;
import com.google.visualization.datasource.base.ReasonType;
import com.google.visualization.datasource.base.ResponseStatus;
import com.google.visualization.datasource.base.StatusType;
import com.google.visualization.datasource.base.TypeMismatchException;
import com.google.visualization.datasource.datatable.ColumnDescription;
import com.google.visualization.datasource.datatable.DataTable;
import com.google.visualization.datasource.datatable.TableCell;
import com.google.visualization.datasource.datatable.TableRow;
import com.google.visualization.datasource.datatable.value.DateTimeValue;
import com.google.visualization.datasource.datatable.value.DateValue;
import com.google.visualization.datasource.datatable.value.TimeOfDayValue;
import com.google.visualization.datasource.datatable.value.Value;
import com.google.visualization.datasource.datatable.value.ValueType;
import com.google.visualization.datasource.query.AbstractColumn;
import com.google.visualization.datasource.query.Query;
import com.ibm.icu.util.GregorianCalendar;
import com.ibm.icu.util.TimeZone;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.async.CallbackWithResult;
import org.commoncrawl.async.Timer;
import org.commoncrawl.rpc.base.shared.RPCStruct;
import org.commoncrawl.service.statscollector.CrawlerStats;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.RPCStructIntrospector;
import org.commoncrawl.util.time.Day;
import org.commoncrawl.util.time.Hour;

import java.io.IOException;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;



public class CrawlerStatsQuery extends HttpServlet {

  /**
   * The log used throughout the data source library.
   */
  private static final Log LOG = LogFactory.getLog(CrawlerStatsQuery.class.getName());
  private static RPCStructIntrospector _crawlStatsHelper = new RPCStructIntrospector(CrawlerStats.class);
  

  private static final int HourlyTimestampColumnIndex = 1;
  private static final int DailyTimestampColumnIndex  = 2;
  
  private static final ColumnDescription[] URLSTATS_TABLE_COLUMNS =
      new ColumnDescription[] {
    new ColumnDescription("timeframe", ValueType.TEXT,  "timeframe"),
    new ColumnDescription("tshourly", ValueType.DATETIME,  "datetime"),
    new ColumnDescription("tsdaily", ValueType.DATE,  "datetime"),
    new ColumnDescription("queued",ValueType.NUMBER,"queued"),
    new ColumnDescription("loading",ValueType.NUMBER,"loading"),
    new ColumnDescription("crawled",ValueType.NUMBER,"crawled"),
    new ColumnDescription("success",ValueType.NUMBER,"success"),
    new ColumnDescription("failed",ValueType.NUMBER,"failed"),
    new ColumnDescription("HTTP200",ValueType.NUMBER,"(20x)"),
    new ColumnDescription("HTTP300",ValueType.NUMBER,"(30x)"),
    new ColumnDescription("HTTP301",ValueType.NUMBER,"(301)"),
    new ColumnDescription("HTTP302",ValueType.NUMBER,"(302)"),
    new ColumnDescription("HTTP304",ValueType.NUMBER,"(304)"),
    new ColumnDescription("HTTP400",ValueType.NUMBER,"(40x)"),
    new ColumnDescription("HTTP403",ValueType.NUMBER,"(403)"),
    new ColumnDescription("HTTP404",ValueType.NUMBER,"(404)"),
    new ColumnDescription("HTTP500",ValueType.NUMBER,"(50x)"),
    new ColumnDescription("HTTPOther",ValueType.NUMBER,"Other"),
    new ColumnDescription("ERRUNKNOWN",ValueType.NUMBER,"Uknown Error"),
    new ColumnDescription("ERRUnknownProtocol",ValueType.NUMBER,"Unk Proto"),
    new ColumnDescription("ERRMalformedURL",ValueType.NUMBER,"Malform URL"),
    new ColumnDescription("ERRTimeout",ValueType.NUMBER,"Timeout"),
    new ColumnDescription("ERRDNSFailure",ValueType.NUMBER,"DNS Failed"),
    new ColumnDescription("ERRResolverFailure",ValueType.NUMBER,"Resolver Fail"),
    new ColumnDescription("ERRIOException",ValueType.NUMBER,"IO Excep"),
    new ColumnDescription("ERRRobotsExcluded",ValueType.NUMBER,"Robots Excl"),
    new ColumnDescription("ERRNoData",ValueType.NUMBER,"No Data"),
    new ColumnDescription("ERRRobotsParseError",ValueType.NUMBER,"Robots Parse Err"),
    new ColumnDescription("ERRRedirectFailed",ValueType.NUMBER,"Redirect Fail"),
    new ColumnDescription("ERRRuntimeError",ValueType.NUMBER,"Runtime Err"),
    new ColumnDescription("ERRConnectTimeout",ValueType.NUMBER,"Connect Timeout"),
    new ColumnDescription("ERRBlackListedHost",ValueType.NUMBER,"BlackListed Host"),
    new ColumnDescription("ERRBlackListedURL",ValueType.NUMBER,"BlackListed URL"),
    new ColumnDescription("ERRTooManyErrors",ValueType.NUMBER,"TooMany Errors"),
    new ColumnDescription("ERRInCache",ValueType.NUMBER,"InCache"),
    new ColumnDescription("ERRInvalidResponseCode",ValueType.NUMBER,"Invalid Resp"),
    new ColumnDescription("ERRBadRedirectData",ValueType.NUMBER,"Bad Redirect"),
    new ColumnDescription("HTTP301_1HOP",ValueType.NUMBER,"HTTP301-ONE-HOP"),
    new ColumnDescription("HTTP301_2HOPS",ValueType.NUMBER,"HTTP301-TWO-HOPS"),
    new ColumnDescription("HTTP301_3HOPS",ValueType.NUMBER,"HTTP301-THREE-HOPS"),
    new ColumnDescription("HTTP301_GT3HOPS",ValueType.NUMBER,"HTTP301-GT_3-HOPS"),
  };

  private static ImmutableMap<String,String> columnNameToFieldNameMap = new ImmutableMap.Builder()
  .put("queued","urlsInFetcherQueue")
  .put("loading","urlsInLoaderQueue")
  .put("crawled","urlsProcessed")
  .put("success","urlsSucceeded")
  .put("failed","urlsFailed")
  .put("HTTP200","http200Count")
  .put("HTTP300","http300Count")
  .put("HTTP301","http301Count")
  .put("HTTP302","http302Count")
  .put("HTTP304","http304Count")
  .put("HTTP400","http400Count")
  .put("HTTP403","http403Count")
  .put("HTTP404","http404Count")
  .put("HTTP500","http500Count")
  .put("HTTPOther","httpOtherCount")
  .put("ERRUNKNOWN","httpErrorUNKNOWN")
  .put("ERRUnknownProtocol","httpErrorUnknownProtocol")
  .put("ERRMalformedURL","httpErrorMalformedURL")
  .put("ERRTimeout","httpErrorTimeout")
  .put("ERRDNSFailure","httpErrorDNSFailure")
  .put("ERRResolverFailure","httpErrorResolverFailure")
  .put("ERRIOException","httpErrorIOException")
  .put("ERRRobotsExcluded","httpErrorRobotsExcluded")
  .put("ERRNoData","httpErrorNoData")
  .put("ERRRobotsParseError","httpErrorRobotsParseError")
  .put("ERRRedirectFailed","httpErrorRedirectFailed")
  .put("ERRRuntimeError","httpErrorRuntimeError")
  .put("ERRConnectTimeout","httpErrorConnectTimeout")
  .put("ERRBlackListedHost","httpErrorBlackListedHost")
  .put("ERRBlackListedURL","httpErrorBlackListedURL")
  .put("ERRTooManyErrors","httpErrorTooManyErrors")
  .put("ERRInCache","httpErrorInCache")
  .put("ERRInvalidResponseCode","httpErrorInvalidResponseCode")
  .put("ERRBadRedirectData","httpErrorBadRedirectData")
  .put("HTTP301_1HOP","redirectResultAfter1Hops")
  .put("HTTP301_2HOPS","redirectResultAfter2Hops")
  .put("HTTP301_3HOPS","redirectResultAfter3Hops")
  .put("HTTP301_GT3HOPS","redirectResultAfterGT3Hops")
  .build();
  
  private static ImmutableSet<String> tablesWithCrawlerStats = new ImmutableSet.Builder()
  .add("urlspersec")
  .add("mbytes")
  .add("downloadsz")
  .build();
  
  private static ImmutableSet<String> isAverageStatTable = new ImmutableSet.Builder()
  .add("downloadsz")
  .build();
    
  private static ImmutableMap<String,String> crawlerStatTableToField = new ImmutableMap.Builder()
  .put("urlspersec", "urlsPerSecond")
  .put("mbytes", "mbytesDownPerSecond")
  .put("downloadsz", "averageDownloadSize")
  .build();
  
  private static ImmutableSet<String> excludedCrawlers = new ImmutableSet.Builder()
  .add("ccn01-PROXY-Dbg")
  .add("ccn01-PROXY-Prod")
  .build();
  
  
  
  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    
    LOG.info("Got Request:" + req.toString());
    
    DataSourceRequest dsRequest = null;

    try {
      // Extract the request parameters.
      dsRequest = new DataSourceRequest(req);

      // NOTE: If you want to work in restricted mode, which means that only
      // requests from the same domain can access the data source, you should
      // uncomment the following call.
      //
      // DataSourceHelper.verifyAccessApproved(dsRequest);

      // Split the query.
      QueryPair query = DataSourceHelper.splitQuery(dsRequest.getQuery(), Capabilities.SELECT);

      // Generate the data table.
      DataTable data = generateMyDataTable(query.getDataSourceQuery(), req);

      // Apply the completion query to the data table.
      DataTable newData = DataSourceHelper.applyQuery(query.getCompletionQuery(), data,
          dsRequest.getUserLocale());
      
      newData.setCustomProperty("tableid", req.getParameter("tableId"));

      DataSourceHelper.setServletResponse(newData, dsRequest, resp);
    } catch (RuntimeException rte) {
      LOG.error("A runtime exception has occured", rte);
      ResponseStatus status = new ResponseStatus(StatusType.ERROR, ReasonType.INTERNAL_ERROR,
          rte.getMessage());
      if (dsRequest == null) {
        dsRequest = DataSourceRequest.getDefaultDataSourceRequest(req);
      }
      DataSourceHelper.setServletErrorResponse(status, dsRequest, resp);
    } catch (DataSourceException e) {
      if (dsRequest != null) {
        DataSourceHelper.setServletErrorResponse(e, dsRequest, resp);
      } else {
        DataSourceHelper.setServletErrorResponse(e, req, resp);
      }
    }
  }

  /**
   * Returns true if the given column name is requested in the given query.
   * If the query is empty, all columnNames returns true.
   *
   * @param query The given query.
   * @param columnName The requested column name.
   *
   * @return True if the given column name is requested in the given query.
   */
  private boolean isColumnRequested(Query query, String columnName) {
    // If the query is empty return true.
    if (query.isEmpty()) {
      return true;
    }

    List<AbstractColumn> columns = query.getSelection().getColumns();
    for (AbstractColumn column : columns) {
      if (column.getId().equalsIgnoreCase(columnName)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Generates a data table - according to the provided tableId url parameter.
   *
   * @param query The query to operate on the underlying data.
   * @param req The HttpServeltRequest.
   *
   * @return The generated data table.
   */
  private DataTable generateMyDataTable(Query query, HttpServletRequest req)
      throws TypeMismatchException {
    String tableID = req.getParameter("tableId");
    if ((tableID != null) && (tableID.endsWith("daily") || tableID.endsWith("hourly"))) {
      LOG.info("calling generate table stats");
      return generateTableStatsTable(query,tableID);
    }
    LOG.error("Received request for unknown table:" + tableID);
    return null;
  }
  
  void addCellToRow(CrawlerStats struct,TableRow row,ColumnDescription column,boolean isHouryValue) { 
    String columnName = column.getId();
    
    if (columnName.equals("timeframe")) { 
      if (isHouryValue) { 
        row.addCell(new Hour(new Date(struct.getTimestamp())).toString());
      }
      else { 
        row.addCell(new Day(new Date(struct.getTimestamp())).toString());
      }
    }
    else if (columnName.equals("tsdaily")) {
      Date date = new Date(struct.getTimestamp());
      GregorianCalendar calendar = new GregorianCalendar(TimeZone.getTimeZone("GMT"));
      calendar.setTime(date);
      row.addCell(new DateValue(calendar));
    }
    else if (columnName.equals("tshourly")) {
      Date date = new Date(struct.getTimestamp());
      GregorianCalendar calendar = new GregorianCalendar(TimeZone.getTimeZone("GMT"));
      calendar.setTime(date);
      row.addCell(new DateTimeValue(calendar));
    }
    else { 
      String propertyName = columnNameToFieldNameMap.get(columnName);
      if (propertyName != null) {
        switch (column.getType()) { 
          case BOOLEAN:
            row.addCell(_crawlStatsHelper.getDoubleValueGivenName(struct, propertyName) == 1.0);
            break;
          case NUMBER:
            row.addCell(Math.max(0.0, _crawlStatsHelper.getDoubleValueGivenName(struct, propertyName)));
            break;
          case TEXT:
            row.addCell(_crawlStatsHelper.getStringValueGivenName(struct, propertyName));
            break;
          case DATE: { 
            long date = (long)_crawlStatsHelper.getDoubleValueGivenName(struct, propertyName);
            GregorianCalendar calendar = new GregorianCalendar(TimeZone.getTimeZone("GMT"));
            calendar.setTime(new Date(date));
            row.addCell(new DateValue(calendar));
          }
          break;
          case DATETIME: { 
            long date = (long)_crawlStatsHelper.getDoubleValueGivenName(struct, propertyName);
            GregorianCalendar calendar = new GregorianCalendar(TimeZone.getTimeZone("GMT"));
            calendar.setTime(new Date(date));
            row.addCell(new DateTimeValue(calendar));
          }break;
          
          case TIMEOFDAY: { 
            long date = (long)_crawlStatsHelper.getDoubleValueGivenName(struct, propertyName);
            GregorianCalendar calendar = new GregorianCalendar(TimeZone.getTimeZone("GMT"));
            calendar.setTime(new Date(date));
            row.addCell(new TimeOfDayValue(calendar));
          }
          break;
        }
      }
      else { 
        LOG.error("propertyName for Column Name:" + columnName + " not found!");
      }
    }
  }

  /**
   * Returns urlstats table 
   * 
   *
   * @param query The selection query.
   *
   * @return A data table of url stats by crawler
   */
  private DataTable generateTableStatsTable(Query query,final String tableID) throws TypeMismatchException {
    
    final Multimap<Date,CrawlerStats> values = ArrayListMultimap.create();
    ImmutableSortedSet.Builder<String> builder = ImmutableSortedSet.naturalOrder();
    
    LOG.info("Collecting Collection Names");
    // populate the map .. 
    synchronized (CrawlStatsCollectorService._statsCollectionMap) {
      for (String collectionName : CrawlStatsCollectorService._statsCollectionMap.keySet()) {
        LOG.info("Encountered Collection:" + collectionName);
        if (collectionName.startsWith(CrawlerStatsCollection.GROUP_KEY)) {
          String uniqueName = StatsLogManager.getUniqueKeyGivenName(collectionName);
          if (!excludedCrawlers.contains(uniqueName)) { 
            LOG.info("Adding Collection:" + collectionName);
            builder.add(uniqueName);
          }
        }
      }
    }
    
    final ImmutableSet<String> crawlerNamesSet = builder.build();
    
    LOG.info("Key Set Size is:" + crawlerNamesSet.size());
    
    if (crawlerNamesSet.size() != 0) { 
      LOG.info("Queueing urlstats collection for:" + crawlerNamesSet.size() + " rows");
      // ok setup semaphore ...
      final Semaphore blockingSemaphore = new Semaphore(-(crawlerNamesSet.size() - 1));
      // and schedule an async request ... 
      CrawlStatsCollectorService.getSingleton().getEventLoop().setTimer(new Timer(0,false,new Timer.Callback() {
        
        @Override
        public void timerFired(Timer timer) {
          LOG.info("Async Timer Event Fired");
          if (tableID.endsWith("hourly")) {
            LOG.info("Table Type is hourly - Collecting Hourly Stats");
            try { 
              synchronized (CrawlStatsCollectorService._statsCollectionMap) { 
                for (Entry<String,StatsCollection> item : CrawlStatsCollectorService._statsCollectionMap.entrySet()) { 
                  if (crawlerNamesSet.contains(StatsLogManager.getUniqueKeyGivenName(item.getKey()))) { 
                    item.getValue().collectHourlyStats(values);
                  }
                }
              }
            }
            catch (IOException e) { 
              LOG.error(CCStringUtils.stringifyException(e));
            }
            finally { 
              LOG.info("Releasing Semaphores");
              blockingSemaphore.release(crawlerNamesSet.size());
            }
          }
          else { 
            LOG.info("Table Type is daily - Collecting Daily Stats");
            synchronized (CrawlStatsCollectorService._statsCollectionMap) {
              for (Entry<String,StatsCollection> item : CrawlStatsCollectorService._statsCollectionMap.entrySet()) {
                if (crawlerNamesSet.contains(StatsLogManager.getUniqueKeyGivenName(item.getKey()))) { 
                  try {
                    item.getValue().collectDailyStats(values, new CallbackWithResult<Boolean>() {
  
                      @Override
                      public void execute(Boolean result) {
                        LOG.info("Daily Query Completed");
                        blockingSemaphore.release();
                      }
                    });
                  } catch (IOException e) {
                    LOG.error(CCStringUtils.stringifyException(e));
                    blockingSemaphore.release();
                  }
                }
              }
            }
          }
        }
      }));
      
      LOG.info("Waiting for Async Completion of request");
      // ok wait for async response .. 
      blockingSemaphore.acquireUninterruptibly();

      // create new data table instance 
      DataTable data = new DataTable();
      // set table id property 
      data.setCustomProperty("tableid", tableID);
      // figure out hourly or daily 
      boolean hourly = tableID.endsWith("hourly");
      String tableNameParts[] = tableID.split("-");
      // if this is a crawler specific table 
      if (tablesWithCrawlerStats.contains(tableNameParts[0])) { 
        
        // get corresponding property name in rpc struct 
        String propertyName = crawlerStatTableToField.get(tableNameParts[0]);
        
        // pivot on crawler name ...
        if (hourly) { 
          data.addColumn(URLSTATS_TABLE_COLUMNS[HourlyTimestampColumnIndex]);
        }
        else { 
          data.addColumn(URLSTATS_TABLE_COLUMNS[DailyTimestampColumnIndex]);
        }
        // add total column 
        data.addColumn(new ColumnDescription("allcrawlers", ValueType.NUMBER, "all crawlers"));
        // add crawler names as columns ... 
        for (String crawlerName : crawlerNamesSet) { 
          data.addColumn(new ColumnDescription(crawlerName, ValueType.NUMBER, crawlerName));
        }
        // now walk date values in order 
        for (Date date : values.keySet()) { 

          // add new row for each timestamp 
          TableRow row = new TableRow();
          
          // add timestamp cell 
          if (hourly) { 
            GregorianCalendar calendar = new GregorianCalendar(TimeZone.getTimeZone("GMT"));
            calendar.setTime(date);
            row.addCell(new DateTimeValue(calendar));
          }
          else { 
            GregorianCalendar calendar = new GregorianCalendar(TimeZone.getTimeZone("GMT"));
            calendar.setTime(date);
            row.addCell(new DateValue(calendar));
          }
          double aggregateValue = 0.0;
          // now extract values sorted by crawler for this date 
          HashMap<String,CrawlerStats> crawlStatsByCrawler = new HashMap<String,CrawlerStats>();
          for (CrawlerStats stats : values.get(date)) { 
            LOG.info("Got Daily Stats for Date:" + date + " Crawler:" + stats.getCrawlerName());
            crawlStatsByCrawler.put(stats.getCrawlerName(),stats);
            aggregateValue += _crawlStatsHelper.getDoubleValueGivenName(stats, propertyName);
          }
          
          if (isAverageStatTable.contains(tableNameParts[0])) { 
            aggregateValue /= values.get(date).size(); 
          }
          // add aggregate results 
          row.addCell(aggregateValue);
          // now walk in crawler name order 
          for (String crawlerName : crawlerNamesSet) { 
            CrawlerStats crawlerStats = crawlStatsByCrawler.get(crawlerName);
            
            if (crawlerStats != null) { 
              row.addCell((double)_crawlStatsHelper.getDoubleValueGivenName(crawlerStats, propertyName));
            }
            else { 
              row.addCell((double)0.0);
            }
          }
          data.addRow(row);
        }
      }
      else { 
        ImmutableSortedSet.Builder<CrawlerStats> aggregatedStatsBuilder = ImmutableSortedSet.naturalOrder();
        
        LOG.info("Request Completed. Building Aggregated Stats");
        
        // ok aggregate stats 
        for (Date date : values.keySet()) { 
          // aggregate values for date 
          CrawlerStats finalStat = CrawlerStatsCollection.combineValues(values.get(date));
          // set final timestamp 
          finalStat.setTimestamp(date.getTime());
          // add to builder 
          aggregatedStatsBuilder.add(finalStat);
        }
        // build final set ... 
        ImmutableSortedSet<CrawlerStats> aggregatedStats = aggregatedStatsBuilder.build();
        
        LOG.info("Aggregation Completed Result Count:" + aggregatedStats.size());
        
      
        // get requested columns
        List<ColumnDescription> requiredColumns = getRequiredColumns(query,URLSTATS_TABLE_COLUMNS);
        // add columns to data table
        data.addColumns(requiredColumns);

        // now collect results 
        for (CrawlerStats object : aggregatedStats) {
  
          TableRow row = new TableRow();
  
          for (ColumnDescription selectionColumn : requiredColumns) { 
            addCellToRow(object,row,selectionColumn,hourly);         
          }
          LOG.info("Added Row:" + row.toString() + " " + row.getCell(0).getValue().toString());
          int cellIndex = 0;
          for (TableCell cell : row.getCells()) { 
            LOG.info("Cell:" + cellIndex++ + " Value:" + cell.getValue());
          }
          data.addRow(row);
        }
      }
      return data;
    }
    return null;
  }


  /**
   * Returns a list of required columns based on the query and the actual
   * columns.
   *
   * @param query The user selection query.
   * @param availableColumns The list of possible columns.
   *
   * @return A List of required columns for the requested data table.
   */
  private List<ColumnDescription> getRequiredColumns(Query query,
      ColumnDescription[] availableColumns) {
    // Required columns
    List<ColumnDescription> requiredColumns = Lists.newArrayList();
    for (ColumnDescription column : availableColumns) {
      if (isColumnRequested(query, column.getId())) {
        requiredColumns.add(column);
      }
    }
    return requiredColumns;
  }
}
