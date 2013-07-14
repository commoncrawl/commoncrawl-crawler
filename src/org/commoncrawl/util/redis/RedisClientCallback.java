package org.commoncrawl.util.redis;

public interface RedisClientCallback {
  
  /** 
   * a redis command was successfully executed on the server 
   * @param cmd the original RedisCmd object that was sent to the server 
   * @param response the top level response encapsulating the response from the server
   */
  public void CmdComplete(RedisClient client,RedisCmd cmd,RedisResponse response);
  
  public void CmdFailed(RedisClient client,RedisCmd cmd, RedisResponse response);
}
