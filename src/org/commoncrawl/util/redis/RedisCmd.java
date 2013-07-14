package org.commoncrawl.util.redis;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.LinkedList;

import org.commoncrawl.util.ByteArrayUtils;


import com.google.common.base.Charsets;
import com.google.common.collect.Lists;

/** 
 * Redis Cmd Wrapper 
 * 
 * @author rana
 *
 */
public class RedisCmd {

  
  public enum Commands {
    // Connection

    AUTH, ECHO, PING, QUIT, SELECT,

    // Server

    BGREWRITEAOF, BGSAVE, CLIENT, CONFIG, DBSIZE, DEBUG, FLUSHALL,
    FLUSHDB, INFO, LASTSAVE, MONITOR, SAVE, SHUTDOWN, SLAVEOF,
    SLOWLOG, SYNC,

    // Keys

    DEL, DUMP, EXISTS, EXPIRE, EXPIREAT, KEYS, MIGRATE, MOVE, OBJECT, PERSIST,
    PEXPIRE, PEXPIREAT, PTTL, RANDOMKEY, RENAME, RENAMENX, RESTORE, TTL, TYPE,

    // String

    APPEND, GET, GETRANGE, GETSET, MGET, MSET, MSETNX, SET, SETEX, SETNX,
    SETRANGE, STRLEN,

    // Numeric

    DECR, DECRBY, INCR, INCRBY, INCRBYFLOAT,

    // List

    BLPOP, BRPOP, BRPOPLPUSH,
    LINDEX, LINSERT, LLEN, LPOP, LPUSH, LPUSHX, LRANGE, LREM, LSET, LTRIM,
    RPOP, RPOPLPUSH, RPUSH, RPUSHX, SORT,

    // Hash

    HDEL, HEXISTS, HGET, HGETALL, HINCRBY, HINCRBYFLOAT, HKEYS, HLEN,
    HMGET, HMSET, HSET, HSETNX, HVALS,

    // Transaction

    DISCARD, EXEC, MULTI, UNWATCH, WATCH,

    // Pub/Sub

    PSUBSCRIBE, PUBLISH, PUNSUBSCRIBE, SUBSCRIBE, UNSUBSCRIBE,

    // Sets

    SADD, SCARD, SDIFF, SDIFFSTORE, SINTER, SINTERSTORE, SISMEMBER,
    SMEMBERS, SMOVE, SPOP, SRANDMEMBER, SREM, SUNION, SUNIONSTORE,

    // Sorted Set

    ZADD, ZCARD, ZCOUNT, ZINCRBY, ZINTERSTORE, ZRANGE, ZRANGEBYSCORE,
    ZRANK, ZREM, ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREVRANGE,
    ZREVRANGEBYSCORE, ZREVRANK, ZSCORE, ZUNIONSTORE,

    // Scripting

    EVAL, EVALSHA, SCRIPT,

    // Bits

    BITCOUNT, BITOP, GETBIT, SETBIT;

    public byte[] bytes;

    private Commands() {
        bytes = name().getBytes(Charsets.US_ASCII);
    }
  }
  
  
  byte[] type;
  String args[];
  LinkedList<RedisCmd> nestedCommands;
  int multiWriteCursor = 0;
  int multiReadCursor = -1;
  RedisResponse response = null;
  RedisClientCallback callback = null;

  public RedisCmd(Commands type,String...args) { 
    this.type = type.bytes;
    this.args = args;
  }

  public RedisCmd(byte[] type,String...args) { 
    this.type = type;
    this.args = args;
  }
  
  void addChild(RedisCmd cmd) { 
    if (nestedCommands == null)
      nestedCommands = Lists.newLinkedList();
    nestedCommands.add(cmd);
  }
  
  boolean isMulti() { 
    return nestedCommands != null; 
  }
  
  boolean isExec() { 
    return ByteArrayUtils.compareBytes(Commands.EXEC.bytes, 0, Commands.EXEC.bytes.length,type, 0, type.length) == 0;
  }
  
  boolean gotMultiAck() { 
    return (multiWriteCursor > -1); 
  }
  
  boolean allChildrenEncoded() {
    return (nestedCommands == null || multiWriteCursor == nestedCommands.size()); 
  }
  
  boolean committed() { 
    return (nestedCommands == null || multiWriteCursor == nestedCommands.size() + 1);
  }
  
  void resetTransmissionState() { 
    multiWriteCursor = -1;
    multiReadCursor = -1;
  }
  
  boolean waitingForMultiStartACK() { 
    return multiReadCursor == -1;
  }
  
  void incMultiReadCursor() { 
    multiReadCursor++;
  }
  
  void setFailedMultiChildResponse(RedisResponse response) { 
    nestedCommands.get(multiReadCursor).response = response;
  }
  
  boolean waitingForExecResponse() {
    return (multiReadCursor == nestedCommands.size());
  }
  
  void encode(OutputStream stream)throws IOException { 
    stream.write('*');
    writeInt(stream, 1 + (args != null ? args.length : 0));
    stream.write(RedisClient.CRLF);
    stream.write('$');
    writeInt(stream, type.length);
    stream.write(RedisClient.CRLF);
    stream.write(type);
    stream.write(RedisClient.CRLF);
    if (args != null) {
      for (String arg : args) {
        byte[] bytes = arg.getBytes(Charset.forName("UTF-8"));
        stream.write('$');
        writeInt(stream, bytes.length);
        stream.write(RedisClient.CRLF);
        stream.write(bytes);
        stream.write(RedisClient.CRLF);
      }
    }
  }
  
  void encodeNextMultiChild(OutputStream stream)throws IOException { 
    if (nestedCommands != null && multiWriteCursor > -1 && multiWriteCursor < nestedCommands.size()) { 
      nestedCommands.get(multiWriteCursor++).encode(stream);
    }
    else { 
      throw new IOException("Invalid Encode call to MULTI!");
    }
  }
  
  void commitMulti(OutputStream stream)throws IOException { 
    if (nestedCommands == null) { 
      throw new IOException("Attempting to Commit a MULTI with NULL Children!");
    }
    else { 
      stream.write('*');
      writeInt(stream, 1);
      stream.write(RedisClient.CRLF);
      stream.write('$');
      writeInt(stream, Commands.EXEC.bytes.length);
      stream.write(RedisClient.CRLF);
      stream.write(Commands.EXEC.bytes);
      stream.write(RedisClient.CRLF);
      multiWriteCursor++;
    }
  }
  
  protected static void writeInt(OutputStream stream, int value)throws IOException {
    if (value < 10) {
      stream.write('0' + value);
      return;
    }

    StringBuilder sb = new StringBuilder(8);
    while (value > 0) {
        int digit = value % 10;
        sb.append((char) ('0' + digit));
        value /= 10;
    }

    for (int i = sb.length() - 1; i >= 0; i--) {
        stream.write(sb.charAt(i));
    }
  }

  @Override
  public String toString() {
    return toStringInternal(0);
  }
  
  String toStringInternal(int level) {
    char temp[] = new char[level * 3];
    Arrays.fill(temp,' ');
    String pad = new String(temp);
    
    StringBuffer sb = new StringBuffer(pad + "Type:" + new String(type));
    if (args != null) {
      for (String arg : args) { 
        sb.append(" Arg:" + arg);
      }
    }
    sb.append('\n');
    
    if (nestedCommands != null) { 
      for (RedisCmd cmd : nestedCommands) { 
        sb.append(cmd.toStringInternal(level + 1));
      }
    }
    return sb.toString();
  }  
}