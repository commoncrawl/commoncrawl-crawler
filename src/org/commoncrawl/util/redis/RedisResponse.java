package org.commoncrawl.util.redis;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.commoncrawl.util.ByteArrayUtils;

/**
 * An object encapsulating a single response line or 
 * a set of responses from a multi response
 * 
 * 
 * @author rana
 *
 */
public class RedisResponse { 
  
  static enum Type { 
    Status,
    Error,
    Integer,
    Buffer,
    Multi
  }
  
  RedisResponse.Type type;
  long lValue;
  ByteBuffer bValue;
  RedisResponse values[];
  int valueIndex=0;

  
  /** 
   * constructor 
   * @param type
   */
  RedisResponse(RedisResponse.Type type) { 
    this.type = type;
  }
  
  RedisResponse(RedisResponse.Type type,byte[] responseData) { 
    this.type = type;
    this.bValue = ByteBuffer.wrap(RedisClient.OK);
  }

  
  public boolean isMulti() { 
    return type == Type.Multi;
  }
  
  public boolean isResponseOK() { 
    return (type == Type.Status && bValue != null &&
        ByteArrayUtils.compareBytes(RedisClient.OK, 0, RedisClient.OK.length, bValue.array(),bValue.arrayOffset() + bValue.position(),bValue.remaining()) == 0);
  }
  
  public boolean isResponseQUEUED() { 
    return (type == Type.Status && bValue != null &&
        ByteArrayUtils.compareBytes(RedisClient.QUEUED, 0, RedisClient.QUEUED.length, bValue.array(),bValue.arrayOffset() + bValue.position(),bValue.remaining()) == 0);
  }
  
  /** 
   * add a child value to this multi response object
   * 
   * @param response
   * @return true if all expected values have been added to this object
   * @throws IOException
   */
  boolean addValue(RedisResponse response)throws IOException  { 
    if (values == null || valueIndex == values.length) { 
      throw new IOException("Invalid attempt to add value to existing RedisResponse!");
    }
    values[valueIndex++] = response;
    
    return (valueIndex == values.length);
  }
  
  RedisResponse currentResponseValue() throws IOException  { 
    if (valueIndex == 0) 
      throw new IOException("Invalid valueIndex!");
    return values[valueIndex-1];
  }
  
  @Override
  public String toString() {
    return toStringInternal(0);
  }
  
  String toStringInternal(int level) {
    char temp[] = new char[level * 3];
    Arrays.fill(temp,' ');
    String pad = new String(temp);
    
    StringBuffer sb = new StringBuffer(pad + "Type:" + type + " ");
    switch (type) { 
      case Integer: sb.append(pad + "Value:" + lValue);break;
      case Buffer:  sb.append(pad + "Value:" + ((bValue == null) ? "<NULL>" : new String(bValue.array(),bValue.arrayOffset()+bValue.position(),bValue.remaining())));break;
      case Multi: { 
        sb.append(pad + "Child Count:" + ((values == null) ? 0 : values.length) + "\n");
        if (values != null) { 
          for (int i=0;i<values.length;++i) { 
            sb.append(pad + "  [" + i + "]" + values[i].toStringInternal(0));
            sb.append(values[i].toStringInternal(level + 1) + "\n");
          }
        }
      }
      break;
      case Error: sb.append(pad  + "ERROR:" + new String(bValue.array(),bValue.arrayOffset()+bValue.position(),bValue.remaining())); break;
      case Status: sb.append(pad  + "STATUS:" + new String(bValue.array(),bValue.arrayOffset()+bValue.position(),bValue.remaining())); break;
    }
    return sb.toString();
  }
}