package org.commoncrawl.util.redis;

import org.commoncrawl.util.redis.RedisCmd.Commands;

/** 
 * Construct a Redis Command
 * 
 * @author rana
 *
 */
public class RedisCmdBuilder { 
  
  RedisCmd activeCmd = null;
  
  public RedisCmdBuilder cmd(String commandType,String keyValue,long lValue) {
    return cmd(commandType.getBytes(),keyValue,Long.toString(lValue));
  }

  public RedisCmdBuilder cmd(Commands commandType,String keyValue,long lValue) {
    return cmd(commandType.bytes,keyValue,Long.toString(lValue));
  }
  
  public RedisCmdBuilder cmd(Commands commandType,String keyValue) { 
    return cmd(commandType.bytes,keyValue);
  }

  public RedisCmdBuilder cmd(Commands commandType, String ... args) { 
    return cmd(commandType.bytes,args);
  }
  
  public RedisCmdBuilder cmd(byte[] commandType,String ... args) {
    RedisCmd cmd = new RedisCmd(commandType, args);
    if (activeCmd == null)
      activeCmd = cmd;
    else 
      activeCmd.addChild(cmd);
    return this;
  }

  /** 
   * 
   * @return the current command under construction 
   */
  public RedisCmd build() { 
    RedisCmd temp = activeCmd;
    activeCmd = null;
    return temp;
  }

  
  /** 
   * start a multi transaction 
   * 
   * @return
   */
  public RedisCmdBuilder mutli() { 
    if (activeCmd != null) { 
      throw new RuntimeException("Multi Command has to be top level command!");
    }
    else{ 
      return cmd(Commands.MULTI);
    }
  }

  
  /************* typed convenience methods *************/

  public RedisCmdBuilder incr(String keyValue) {
    cmd(Commands.INCR,keyValue);
    return this;
  }
  
  public RedisCmdBuilder incrBy(String keyValue,long lValue) {
    cmd(Commands.INCR,keyValue,lValue);
    return this;
  }
  
  public RedisCmdBuilder expire(String keyValue,long lSeconds) {
    cmd(Commands.EXPIRE,keyValue,lSeconds);
    return this;
  }

  public RedisCmdBuilder expireAt(String keyValue,long lTimestamp) {
    cmd(Commands.EXPIRE,keyValue,lTimestamp);
    return this;
  }
  
}