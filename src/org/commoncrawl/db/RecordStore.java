package org.commoncrawl.db;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.*;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

import java.util.Vector;
import SQLite.*;
import SQLite.Exception;

import java.rmi.dgc.VMID;
import org.commoncrawl.rpc.base.internal.UnitTestStruct1;
import org.commoncrawl.rpc.base.shared.BinaryProtocol;
import org.commoncrawl.rpc.base.shared.RPCStruct;
import org.commoncrawl.rpc.base.shared.RPCStructWithId;

public class RecordStore {

  public static final Log LOG = LogFactory.getLog(RecordStore.class);

  private static int DATABASE_VERSION = 1;
  
  public static class RecordStoreException extends IOException { 
    
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public RecordStoreException(String reason) {
      super(reason);
    } 
  }

  public static class ReplicationServer { 
	    public String _serverName;
	    public int    _port;
	    
	    ReplicationServer(String serverName,int port) { 
	      _serverName = serverName;
	      _port       = port;
	    }
  }
  
  private class TransactionState { 

    public TransactionState() { 
      _activeThread = Thread.currentThread();
      _txnNumber    = ++RecordStore.this._lastTxnNumber; 
      _refCount = 0;
    }
    
    public Thread _activeThread;
    public int    _refCount;
    public long   _txnNumber;
    
  }
  
  private enum RecordStatus{ ALIVE,DELETED };
  
  private int   		_databaseVersion = 0;
  private long   	_lastCheckpointNumber = 0;
  private long   	_lastTxnNumber = 0;
  private long   	_lastRecordId = 0;
  private String _databaseId;
  private Database _database;
  TransactionState _txnState;
  private static BinaryProtocol binaryProtocol = new BinaryProtocol();
  
  
  public synchronized void initialize(File localFilePath,
                         Vector<ReplicationServer> servers)throws RecordStoreException { 
  
    try { 
      _database = new Database();
      _database.open(localFilePath.getAbsolutePath(), 0666);
      if (checkTables()) {
        queryMetadata();
      }
      else {
        createTables();
      }
      
    }
    catch (SQLite.Exception e) {
      throw wrapSQLiteException(e);
    }
  }
  
  public String getDatabaseId() { 
	  return _databaseId;
  }
  
  private RecordStoreException wrapSQLiteException(SQLite.Exception e ) { 
    return new RecordStoreException(e.getMessage());
  }
  
  
  public synchronized void shutdown() { 
	  if (_database != null) { 
		  try {
			_database.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		  _database = null;
	  }
  }
  
  /**
   * 
   * @return true if tables actually created
   * @throws RecordStoreException
   */
  private final void createTables()throws RecordStoreException {
    Stmt stmt = null;
    
      try { 
        // create the tables ...
        String createMasterSQL = "Create Table metadata ( database_id TEXT NOT NULL," +
                                                    "database_version INTEGER NOT NULL," +
                                                    "last_checkpoint_id INTEGER(8) NOT NULL," +
                                                    "last_txn_number INTEGER(8) NOT NULL);";
        
        // record_id,record_type,parent_id,record_key,txn_number,record_status,record_data
        String createRecordTableSQL = "Create Table records ( record_id INTEGER(8) NOT NULL," +
        													  "record_type TEXT NOT NULL,"+
        													  "parent_id TEXT,"+
        													  "record_key TEXT,"+
                                                              "txn_number INTEGER(8) NOT NULL," +
                                                              "record_status INTEGER NOT NULL," +
                                                              "record_data BLOB," +
                                                              "PRIMARY KEY(record_id) );";
          
        
        String createIndexSQL = "Create Index records_key_idx on records (record_key);";
        String createIndexSQL2 = "Create Index records_parent_id_idx on records (parent_id);";
        	
        _database.exec(createMasterSQL, null);
        _database.exec(createRecordTableSQL, null);
        _database.exec(createIndexSQL, null);
        _database.exec(createIndexSQL2, null);
        
        
        _databaseId = new VMID().toString();
        _lastCheckpointNumber = 0;
        _lastTxnNumber = 0;
        
        
        String insertMasterSQL = "Insert Into metadata (database_id,database_version,last_checkpoint_id,last_txn_number)" +
                                  " Values (?,?,?,?);";
        
        stmt = _database.prepare(insertMasterSQL);
        
        stmt.bind(1,_databaseId);
        stmt.bind(2,DATABASE_VERSION);
        stmt.bind(3,_lastCheckpointNumber);
        stmt.bind(4,_lastTxnNumber);
        stmt.step();
      }
      catch (SQLite.Exception e) { 
        throw wrapSQLiteException(e);
      }
      finally  {
        if (stmt != null){
          try {
            stmt.close();
          }
          catch (SQLite.Exception e) { 
            throw wrapSQLiteException(e);
          }
        }
      }
  }
  
  private synchronized final void queryMetadata() throws RecordStoreException { 
    try { 
 
      // query the master table ...
      String queryMasterSQL = "Select database_id,database_version,last_checkpoint_id,last_txn_number from metadata;";
      String recordIdSQL = "Select Max(record_id) from records;";
      
      Stmt stmt = null;
      Stmt stmt2 = null;
      
      try { 
        stmt = _database.prepare(queryMasterSQL);
        stmt2 = _database.prepare(recordIdSQL);
        
        if (stmt.step() && stmt2.step()) { 
          _databaseId = stmt.column_string(0);
          //TODO: VALIDATE DATABASE VERSION HERE ...           
          int databaseVersion = stmt.column_int(1);
          _lastCheckpointNumber = stmt.column_long(2);
          _lastTxnNumber = stmt.column_long(3);
          _lastRecordId = stmt2.column_long(0);
        }
      }
      finally{ 
        stmt.close();
        stmt2.close();
      }
    }
    catch (SQLite.Exception e) { 
      throw wrapSQLiteException(e);
    }
  }
  private synchronized final boolean checkTables()throws RecordStoreException {
    
    SQLite.TableResult result = null;
    
    try { 
      result = _database.get_table("SELECT name FROM sqlite_master where type IN('table','view') AND name IN" +
        "('metadata','records');");

      if (result != null && result.nrows == 2) { 
        return true;
      }
      return false;
      
    }
    catch (SQLite.Exception e) { 
      throw wrapSQLiteException(e);
    }
    finally {
      if (result != null) { 
        result.clear();
      }
    }
  }
  
  public synchronized final boolean inTransaction() {
    if (_txnState != null && _txnState._activeThread == Thread.currentThread())
      return true;
    else
      return false;
  }
  
  public final synchronized void beginTransaction() throws RecordStoreException { 
      
    
    if (_txnState != null && _txnState._activeThread != Thread.currentThread()) { 
      throw new RecordStoreException("Invalid State. TXN already open in another thread!");
    }
    if (_txnState == null) {
      _txnState = new TransactionState();
    }
    if (_txnState._refCount++ == 0) { 
      try { 
        _database.exec("BEGIN EXCLUSIVE;", null);
      }
      catch (SQLite.Exception e){
        if (--_txnState._refCount == 0) { 
          _txnState = null;
        }
        throw wrapSQLiteException(e);
      }
    }
    
  }
  
  public final synchronized void commitTransaction() throws RecordStoreException { 

    if (_txnState == null || _txnState._activeThread != Thread.currentThread()) { 
      throw new RecordStoreException("Invalid State. commit called on non-existen txn or from different thread.");
    }
    if (--_txnState._refCount == 0) {
      _txnState = null;
      try { 
        _database.exec("COMMIT;", null);
      }
      catch (SQLite.Exception e) { 
        throw wrapSQLiteException(e);
      }
    }
  }

  public final synchronized void abortTransaction(){ 

    if (_txnState == null || _txnState._activeThread != Thread.currentThread()) { 
      LOG.error("Invalid Call to abortTransaction. No Transaction is Active");
    }
    // abort transaction irrespective of ref count ... 
    _txnState = null;

    try { 
      _database.exec("ROLLBACK;", null);
    }
    catch (SQLite.Exception e) { 
      LOG.error("ABORT TRANSACTION FAILED WITH ERROR:" + e.toString());
      throw new RuntimeException(wrapSQLiteException(e));
    }
    
  }

  public synchronized final <Type> long  insertRecord(String parentId,String key,RPCStructWithId struct) throws RecordStoreException{ 

    Stmt stmt = null;
            
    if (struct == null || key == null) { 
      throw new RecordStoreException("NULL data value not allowed.");
    }
    
    try { 
      // validate txn status 
      validateTransaction();
      
      // allocate a new record id ... 
      struct.setRecordId(++_lastRecordId);
      
      // serialize the data 
      ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
      DataOutputStream outputStream = new DataOutputStream (byteStream);
      
      try { 
    	  struct.serialize(outputStream, binaryProtocol);
    	  outputStream.flush();
      }
      catch (IOException e){
    	  throw new RecordStoreException("Serialization Error");
      }
      
      String strInsertSQL = "Insert Into records (record_id,record_type,parent_id,record_key,txn_number,record_status,record_data) " +
                            "Values (?,?,?,?,?,?,?);";
      
      stmt = _database.prepare(strInsertSQL);
      
      stmt.bind(1,struct.getRecordId());
      stmt.bind(2,struct.getClass().getName());
      stmt.bind(3,parentId);
      if (key == null) { 
    	  stmt.bind(4);
      }
      else { 
    	  stmt.bind(4,key);
      }
      stmt.bind(5,_txnState._txnNumber);
      stmt.bind(6,RecordStatus.ALIVE.ordinal());
      stmt.bind(7,byteStream.toByteArray());
      
      stmt.step();
      
    }
    catch (SQLite.Exception e) { 
      throw wrapSQLiteException(e);
    }
    finally { 
     if (stmt != null) {
       try { 
         stmt.close();
       }
       catch (SQLite.Exception e) { 
         throw wrapSQLiteException(e);
       }
     }
    }
    return struct.getRecordId();
  }
  
  public synchronized final void updateRecordById(long recordId, RPCStructWithId struct) throws RecordStoreException{ 

    Stmt stmt = null;
        
    if (struct== null || recordId == 0) { 
      throw new RecordStoreException("NULL data value not allowed.");
    }

    // serialize the data 
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream (byteStream);
    
    try { 
  	  struct.serialize(outputStream, binaryProtocol);
  	  outputStream.flush();
    }
    catch (IOException e){
  	  throw new RecordStoreException("Serialization Error");
    }
    
    try { 
      // validate txn status 
      validateTransaction();
      
    	  
      String strInsertSQL = "Update records Set record_type=?,txn_number=?,record_status=?,record_data=? where record_id=?;";
      
      stmt = _database.prepare(strInsertSQL);
      
      stmt.bind(1,struct.getClass().getName());
      stmt.bind(2,_txnState._txnNumber);
      stmt.bind(3,RecordStatus.ALIVE.ordinal());
      stmt.bind(4,byteStream.toByteArray());
      stmt.bind(5,struct.getRecordId());

      stmt.step();
        
    }
    catch (SQLite.Exception e) { 
      throw wrapSQLiteException(e);
    }
    finally { 
     if (stmt != null) {
       try { 
         stmt.close();
       }
       catch (SQLite.Exception e) { 
         throw wrapSQLiteException(e);
       }
     }
    }
  }
  
  public synchronized final void updateRecordByKey(String key, RPCStruct struct) throws RecordStoreException { 

	    Stmt stmt = null;
	        
	    if (struct== null || key == null) { 
	      throw new RecordStoreException("NULL data value not allowed.");
	    }

	    // serialize the data 
	    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
	    DataOutputStream outputStream = new DataOutputStream (byteStream);
	    
	    try { 
	  	  struct.serialize(outputStream, binaryProtocol);
	  	  outputStream.flush();
	    }
	    catch (IOException e){
	  	  throw new RecordStoreException("Serialization Error");
	    }
	    
	    try { 
	      // validate txn status 
	      validateTransaction();
	      
	    	  
	      String strInsertSQL = "Update records Set record_type=?,txn_number=?,record_status=?,record_data=? where record_key=?;";
	      
	      stmt = _database.prepare(strInsertSQL);
	      
	      stmt.bind(1,struct.getClass().getName());
	      stmt.bind(2,_txnState._txnNumber);
	      stmt.bind(3,RecordStatus.ALIVE.ordinal());
	      stmt.bind(4,byteStream.toByteArray());
	      stmt.bind(5,key);

	      stmt.step();
	        
	    }
	    catch (SQLite.Exception e) { 
	      throw wrapSQLiteException(e);
	    }
	    finally { 
	     if (stmt != null) {
	       try { 
	         stmt.close();
	       }
	       catch (SQLite.Exception e) { 
	         throw wrapSQLiteException(e);
	       }
	     }
	    }
  }

  public synchronized final void deleteRecordById(long recordId) throws RecordStoreException{ 

    Stmt stmt = null;
   
    try { 
      // validate txn status 
      validateTransaction();

/*      
      String strInsertSQL = "Update records Set txn_number=?,record_status=?,record_data=? where record_id=?;";
      
      stmt = _database.prepare(strInsertSQL);
      
      stmt.bind(1,_txnState._txnNumber);
      stmt.bind(2,RecordStatus.DELETED.ordinal());
      stmt.bind(3);
      stmt.bind(4,recordId);
*/

      String strDeleteSQL = "Delete from records where record_id=?;";
      
      stmt = _database.prepare(strDeleteSQL);
      
      stmt.bind(1,recordId);

      stmt.step();
        
    }
    catch (SQLite.Exception e) { 
      throw wrapSQLiteException(e);
    }
    finally { 
    if (stmt != null) {
       try { 
         stmt.close();
       }
       catch (SQLite.Exception e) { 
         throw wrapSQLiteException(e);
       }
     }
    }
    
  }

  public synchronized final void deleteRecordByKey(String recordKey) throws RecordStoreException{ 

	    Stmt stmt = null;
	   
	    try { 
	      // validate txn status 
	      validateTransaction();
/*	      
	      String strInsertSQL = "Update records Set txn_number=?,record_status=?,record_data=? where record_key=?;";
	      
	      stmt = _database.prepare(strInsertSQL);
	      
	      stmt.bind(1,_txnState._txnNumber);
	      stmt.bind(2,RecordStatus.DELETED.ordinal());
	      stmt.bind(3);
	      stmt.bind(4,recordKey);
*/
	      
	      String strDeleteSQL = "Delete from records where record_key=?;";

        stmt = _database.prepare(strDeleteSQL);
	      
	      stmt.bind(1,recordKey);
	      
	      stmt.step();
	        
	    }
	    catch (SQLite.Exception e) { 
	      throw wrapSQLiteException(e);
	    }
	    finally { 
	     if (stmt != null) {
	       try { 
	         stmt.close();
	       }
	       catch (SQLite.Exception e) { 
	         throw wrapSQLiteException(e);
	       }
	     }
	    }
	    
  }
  
  public synchronized final void deleteChildRecords(String parentId) throws RecordStoreException{ 

	    Stmt stmt = null;
	   
	    try { 
	      // validate txn status 
	      validateTransaction();

/*	      
	      String strInsertSQL = "Update records Set txn_number=?,record_status=?,record_data=? where parent_id=?;";
	      
	      stmt = _database.prepare(strInsertSQL);
	      
	      stmt.bind(1,_txnState._txnNumber);
	      stmt.bind(2,RecordStatus.DELETED.ordinal());
	      stmt.bind(3);
	      stmt.bind(4,parentId);
*/
	      String strDeleteSQL = "Delete from records where parent_id=?;";

        stmt = _database.prepare(strDeleteSQL);
	      
	      stmt.bind(1,parentId);
	      
	      stmt.step();
	        
	    }
	    catch (SQLite.Exception e) { 
	      throw wrapSQLiteException(e);
	    }
	    finally { 
	     if (stmt != null) {
	       try { 
	         stmt.close();
	       }
	       catch (SQLite.Exception e) { 
	         throw wrapSQLiteException(e);
	       }
	     }
	    }
  }
  
  public synchronized RPCStruct getRecordByKey(String key) throws RecordStoreException { 
	  return getRecordByKeyOrId(key,0);
  }
  
  public synchronized RPCStruct getRecordById(long recordId) throws RecordStoreException { 
	  return getRecordByKeyOrId(null,recordId);
  }
  
  static String org_crawlcommons = "org.crawlcommons";

  private synchronized final RPCStruct getRecordByKeyOrId(String key,long recordId) throws RecordStoreException  { 

	  String strSQL;
	  
	  if (key != null)
		  strSQL = "Select record_id,record_type,record_data from records where record_key = ? and record_status=?;";
	  else 
		  strSQL = "Select record_id,record_type,record_data from records where record_id = ? and record_status=?;";
	  
      Stmt stmt = null;
      
      RPCStructWithId struct = null;
      
      try { 
        stmt = _database.prepare(strSQL);
        
        if (key != null)
        	stmt.bind(1,key);
        else 
        	stmt.bind(1,recordId);
        
        stmt.bind(2,RecordStatus.ALIVE.ordinal());
        
        if (stmt.step()) {
        	
        	recordId                 = stmt.column_long(0);
        	String recordType = stmt.column_string(1);
        	byte[] data             =  stmt.column_bytes(2);
        	if (recordType.startsWith(org_crawlcommons)) {
        	  recordType = recordType.replaceFirst(org_crawlcommons, "org.commoncrawl");
        	}
        	// allocate an instance of the struct 
        	struct = (RPCStructWithId) Class.forName(recordType).newInstance();
        	// create the necessary stream 
        	DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
        	// and deserialize the struct ... 
        	struct.deserialize(in, binaryProtocol);
        	// set the record id before returning the struct to the caller 
        	struct.setRecordId(recordId);
        }
      }
      catch (SQLite.Exception e) { 
        throw wrapSQLiteException(e);
      }
      catch (ClassNotFoundException e){
    	  e.printStackTrace();
    	  throw new RuntimeException(e);
      } catch (InstantiationException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      } catch (IllegalAccessException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      } catch (IOException e) {
        e.printStackTrace();
        throw new RecordStoreException("DeSerialization Failure");
      }
      finally{ 
    	
      	try { 
      		if (stmt != null)
      		  stmt.close();
      	}
  	    catch (SQLite.Exception e) { 
  	    	throw wrapSQLiteException(e);
  	    }    	
      }
      return struct;
  	}
  
  public synchronized Vector<String> getChildRecordKeysByParentId(String  parentId) throws RecordStoreException { 
    String strSQL = "Select record_key from records where parent_id = ? and record_status=?;";
    
      Stmt stmt = null;
      
      Vector<String> children = new Vector<String>();
      
      try { 
        stmt = _database.prepare(strSQL);
        
        stmt.bind(1,parentId);
        stmt.bind(2,RecordStatus.ALIVE.ordinal());
        
        while (stmt.step()) {
          children.add(stmt.column_string(0));
        }
      }
      catch (SQLite.Exception e) { 
          throw wrapSQLiteException(e);
      }
      finally{ 
        try { 
          stmt.close();
        }
        catch (SQLite.Exception e) { 
          throw wrapSQLiteException(e);
        }     
      }
      return children;
  }
  
  public synchronized Vector<Long> getChildRecordsByParentId(String  parentId) throws RecordStoreException { 
	  String strSQL = "Select record_id from records where parent_id = ? and record_status=?;";
	  
      Stmt stmt = null;
      
      Vector<Long> children = new Vector<Long>();
      
      try { 
        stmt = _database.prepare(strSQL);
        
       	stmt.bind(1,parentId);
        stmt.bind(2,RecordStatus.ALIVE.ordinal());
        
        while (stmt.step()) {
        	
        	children.add(stmt.column_long(0));
        }
      }
      catch (SQLite.Exception e) { 
          throw wrapSQLiteException(e);
      }
      finally{ 
    	
    	try { 
    		stmt.close();
    	}
	    catch (SQLite.Exception e) { 
	    	throw wrapSQLiteException(e);
		}    	
        
      }
      return children;
  }
  private final synchronized void validateTransaction() throws RecordStoreException { 
    if (_txnState == null || _txnState._activeThread != Thread.currentThread()) {
      throw new RecordStoreException("Invalid State.No current Transaction or Txn opened in another thread.");
    }
  }
  
  /** generic helper routine to persist RPCStruct to disk **/
  public synchronized void insertUpdatePersistentObject ( RPCStructWithId object,String  parentKey,String keyPrefix,boolean update) throws RecordStoreException  { 
    if (update)
      updateRecordByKey(keyPrefix+object.getKey(), object);
    else 
      insertRecord(parentKey, keyPrefix+object.getKey(), object);
  }  

  /*
  TODO: GARBAGE .. GET RID OF IT ... 
  static char seedPath[] 	= new char[24];
  static char lastPath[]   	= new char[24];
  static char lastDomain[] = { 'a','a','a','a','a','a','a','a','a','a'-1};
  
  static void   generateSeeds() { 
	  for (int i =0;i<seedPath.length;++i) {
		  seedPath[i] = (char) ('a' + Math.random() * 26);
	  }
  }
  
  static String getNextDomain() {
	  // reset seed path 
	  System.arraycopy(seedPath, 0, lastPath, 0, seedPath.length);
	  // increment domain path 
	  for (int i=lastDomain.length-1;i>=0;--i){
		  if (lastDomain[i] != 'z') { 
			  lastDomain[i] += 1;
			  break;
		  }
		  else { 
			lastDomain[i]='a';  
		  }
	  }
	  
	  return new String(lastDomain);
  }
  
  // get next path ... 
  static String getNextPath() { 

	  for (int i=lastPath.length-1;i>=0;--i){
		  if (lastPath[i] != 'z') { 
			  lastPath[i] += 1;
			  break;
		  }
		  else { 
			  lastPath[i]='a';  
		  }
	  }
	  return new String(lastPath);	  
  }

  
  
 @Test
 public void testDatabaseSize() throws java.lang.Exception {

      File db1 = new File("/tmp/test.db");
      File db2 = new File("/tmp/test2.db");
      File db3 = new File("/tmp/test3.db");
      File db4 = new File("/tmp/test4.db");

      RecordStore recordStore[] = new RecordStore[1];
      for (int j=0;j<recordStore.length;++j)
    	  recordStore[j] = new RecordStore();
      recordStore[0].initialize(db1, null); 
      
      //recordStore[1].initialize(db2, null);
      //recordStore[2].initialize(db3, null);
      //recordStore[3].initialize(db4, null);
      
      System.out.println("Database Ready");
      
      CrawlURL data = new CrawlURL();

      boolean doInsert = false;
      
      if (doInsert) { 
	      boolean inTxn = false;
	      long totalTimeStart = System.currentTimeMillis();
	      long timeStart = 0;
	      
	      for (int i=0;i<1;){ 
	      
	    	  if (i%10000 == 0){ 
	    		  if (inTxn) {
	    			  for (int j=0;j<recordStore.length;++j)
	    				  recordStore[j].commitTransaction();
	    			  long timeSpent = System.currentTimeMillis() - timeStart;
	        		  System.out.println("Inserted "+i+" Records in " +((Long)timeSpent).toString() +" Milliseconds");
	    		  }
	
	    		  inTxn = true;
				  for (int j=0;j<recordStore.length;++j)
					  recordStore[j].beginTransaction();
	    		  timeStart = System.currentTimeMillis();
	    	  }
	    	  String domain = getNextDomain();
	    	  String path = getNextPath();
	    	  
	  
	    	  for (int j=0;j<16;++j) { 
	    		  data.setUrl("http://"+domain+"/"+path);
	    		  data.setFingerprint((long) (Math.random() * Long.MAX_VALUE));
	    		  path = getNextPath();
				  for (int k=0;k<recordStore.length;++k)
					  recordStore[k].insertRecord(0, ((Long)data.getFingerprint()).toString(),data);
	    		  ++i;
	    		  
	    	  }    	  
	      }
	      
	      if (inTxn){
			  for (int j=0;j<recordStore.length;++j)
				  recordStore[j].commitTransaction();
	      }
	      
	      System.out.println("Total time expended:"+((Long)(System.currentTimeMillis() - totalTimeStart)));
      }
      else {
    	  long seekStart = System.currentTimeMillis();
    	  CrawlURL returnedData = (CrawlURL)recordStore[0].getRecordById(1000500);
    	  System.out.println("Seek took "+((Long)System.currentTimeMillis()-seekStart));
    	  System.out.println("Seek returned URL:"+returnedData.getUrl());
      }
}
 */
  
  @Test
  public void testRecordStore() throws java.lang.Exception {
   
      System.out.println(System.getProperty("java.library.path"));
      File tempDBFile = File.createTempFile("sqlite",".db");
      
      RecordStore recordStore = new RecordStore();
      recordStore.initialize(tempDBFile, null);
    
      UnitTestStruct1 unitTest = new UnitTestStruct1();
      
      unitTest.setStringType("foo");
      
      long timeStart = System.currentTimeMillis();
      recordStore.beginTransaction();
      for (int i=0;i<10000;++i) { 
    	  recordStore.insertRecord(((Integer)(i/1000)).toString(), Integer.toString(i), unitTest);
      }
      recordStore.commitTransaction();

      long timeEnd = System.currentTimeMillis();

      System.out.println("10000 record insertion took:"+Long.toString(timeEnd-timeStart)+" millisecs.");

      timeStart = System.currentTimeMillis();
      recordStore.beginTransaction();
      for (int i=0;i<1000;++i) {
    	  int key = (int) (10000.00 * Math.random());
    	  unitTest.setStringType(Integer.toString(key));
    	  recordStore.updateRecordByKey(Integer.toString(key), unitTest);
      }
      recordStore.commitTransaction();

      timeEnd = System.currentTimeMillis();

      System.out.println("random update of 1000 records took:"+Long.toString(timeEnd-timeStart)+" millisecs.");
      
      
      int randomKey = (int) (1000.00 * Math.random());
      
      timeStart = System.currentTimeMillis();
      UnitTestStruct1 unitTest2 = (UnitTestStruct1)recordStore.getRecordByKey(Integer.toString(randomKey));
      timeEnd = System.currentTimeMillis();

      System.out.println("random seek took:"+Long.toString(timeEnd-timeStart)+" millisecs.");

      randomKey = (int) (1000000.00 * Math.random());
      
      timeStart = System.currentTimeMillis();
      recordStore.beginTransaction();
      recordStore.deleteRecordByKey(Integer.toString(randomKey));
      recordStore.commitTransaction();
      timeEnd = System.currentTimeMillis();

      System.out.println("random delete took:"+Long.toString(timeEnd-timeStart)+" millisecs.");

      long randomId = (long) (1000.00 * Math.random());

      Vector<Long> children = recordStore.getChildRecordsByParentId("0");
      
      timeStart = System.currentTimeMillis();
      for (Long childId : children) { 
    	  UnitTestStruct1 unitTest3 = (UnitTestStruct1) recordStore.getRecordById(childId);
      }
      timeEnd = System.currentTimeMillis();

      System.out.println("read of:"+Integer.toString(children.size())+" records took:"+Long.toString(timeEnd-timeStart)+" millisecs.");
  }
  
}
