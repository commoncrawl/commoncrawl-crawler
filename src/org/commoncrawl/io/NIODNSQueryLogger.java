package org.commoncrawl.io;

import java.net.InetAddress;

/** dns query logger */
public interface NIODNSQueryLogger {
  public void logDNSQuery(String hostName, InetAddress address, long ttl,
      String opCName);

  public void logDNSFailure(String hostName, String errorCode);

  public void logDNSException(String hostName, String exceptionDesc);
}