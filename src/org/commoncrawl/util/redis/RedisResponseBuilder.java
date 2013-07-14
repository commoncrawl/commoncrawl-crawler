package org.commoncrawl.util.redis;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Stack;

import org.apache.hadoop.io.DataOutputBuffer;
import org.commoncrawl.io.NIOBufferList;
import org.commoncrawl.util.redis.RedisResponse.Type;

/** 
 * state machine used to build a redis response
 * 
 * @author rana
 *
 */
public class RedisResponseBuilder { 
  enum WaitState { 
    WAITING_FOR_TYPE,
    WAITING_FOR_CR_BYTE,
    WAITING_FOR_LF_BYTE,
    WAITING_FOR_BULK_BYTES,
    DONE
  }
  
  RedisResponseBuilder.WaitState _state = WaitState.WAITING_FOR_TYPE;
  Stack<RedisResponse> _responseStack = new Stack<RedisResponse>();
  RedisResponse _current = null;
  RedisResponse _final = null;
  NIOBufferList _valueBuffer = new NIOBufferList();
  long _longValue = 0L;
  boolean _numericType = false;
  boolean _processingBufferBytes = false;
  void reset() { 
    _state = WaitState.WAITING_FOR_TYPE;
    _responseStack.clear();
    _current = null;
    _final = null;
    _processingBufferBytes=false;
    _numericType = false;
    _valueBuffer.reset();
    _longValue = 0L;
  }
  
  
  RedisResponse processBuffer(ByteBuffer bytes)throws IOException {
    
    while (bytes.remaining() != 0 || _state == WaitState.DONE) { 
      switch (_state) { 
        case WAITING_FOR_TYPE: {
          if (_current != null)
            _responseStack.push(_current);
          _current = new RedisResponse(readType(bytes));
          _numericType = (_current.type != Type.Error && _current.type != Type.Status);
          _state = WaitState.WAITING_FOR_CR_BYTE;
          _valueBuffer.reset();
          _longValue = 0L;
        }
        break;
        
        case WAITING_FOR_CR_BYTE: {

          if (_processingBufferBytes) { 
            byte b = bytes.get();
            
            if (b != '\r') { 
                throw new IOException("Expected CR waiting for BufferBytes Terminator!");
            }
            else { 
              _state = WaitState.WAITING_FOR_LF_BYTE;
            }
          }
          else { 
            byte b = 0;
            bytes.mark();
            
            if (_numericType) { 
              while (bytes.remaining() != 0) {
                b = bytes.get();
                if (b == '\r')
                  break;
                _longValue = (_longValue*10)+(b - '0');
              }
            }
            else {
              while (bytes.remaining() != 0) { 
                b = bytes.get();
                if (b == '\r')
                  break;
              }
              int savedPosition = bytes.position();
              bytes.reset();
              int bytesAvailabe  = savedPosition - bytes.position() - 1;
              if (bytesAvailabe != 0) { 
                ByteBuffer sliced = bytes.slice();
                sliced.position(bytesAvailabe);
                sliced.limit(bytesAvailabe);
                _valueBuffer.write(sliced);
              }
              bytes.position(savedPosition);
            }
          
            if (b == '\r') {
              _state = WaitState.WAITING_FOR_LF_BYTE;
            }
          }
        }
        break;
          
        case WAITING_FOR_LF_BYTE: {
          
          byte b = bytes.get();
          
          if (b == '\n') { 
            if (!_processingBufferBytes) { 
              _state = processReply();
            }
            else { 
              _processingBufferBytes = false;
              _state = collapseCurrentNode();
            }
          }
          else { 
            throw new IOException("Expected \\n GOT:" + b);
          }
        }
        break;
        
        case WAITING_FOR_BULK_BYTES: { 
          // we store buffer(string) len in lvalue 
          if (_current.lValue != 0) {
            
            // first, if previously allocated storage is null 
            // check if we can satisfy the request from incoming payload ... 
            // if NOT, we are going to pre-allocate the buffer here so that 
            // we can reuse the buffer copy code below ... 
            if (bytes.remaining() < _current.lValue) { 
              _current.bValue = ByteBuffer.allocate((int)_current.lValue);
            }
            
            // previously allocated storage means
            // that the payload spanned buffers... 
            // in this case copy incoming data into payload buffer ...              
            if (_current.bValue != null) { 
              int bytesToCopy = Math.min(_current.bValue.remaining(), bytes.remaining());
              _current.bValue.put(bytes.array(),bytes.arrayOffset() + bytes.position(),bytesToCopy);
              bytes.position(bytes.position() + bytesToCopy);
            }
            // otherwise, if storage is null, then based on the assumption 
            // above, we should have enough data in the payload buffer to 
            // satisfy the request ... 
            else { 
              // first see if we have enough data in incoming buffer to satisfy request ... 
              if (bytes.remaining() >= _current.lValue) { 
                // ok, incoming buffer satisfies request ... 
                // slice it ... 
                ByteBuffer sliced = bytes.slice();
                // and position / limit it appropriately ... 
                sliced.position((int)_current.lValue);
                sliced.limit((int)_current.lValue);
                // set the buffer variable 
                _current.bValue = sliced; 
                // advance payload buffer cursor ... 
                bytes.position(bytes.position() + (int)_current.lValue);
              }
              else { 
                throw new IOException("Invalid State. Response Buffer NULL but incoming payload doesn't satisfy request");
              }
            }
            
            if (_current.bValue.remaining() == 0) {
              _current.bValue.flip();
              _processingBufferBytes = true;
              _state = WaitState.WAITING_FOR_CR_BYTE;
            }
          }
          else { 
            throw new IOException("In WAITING for BULK BYTES State but no storage allocated!");
          }
        }
        break;
        
        case DONE: { 
          if (_final == null) { 
            throw new IOException("Entered DONE state but no FINAL node!");
          }
          else { 
            // a response is ready ... 
            RedisResponse responseOut = _final;
            // reset state variables ...  
            _final = null;
            _state = WaitState.WAITING_FOR_TYPE;
            // return top level response ... 
            return responseOut;
          }
        }
      }
    }
    return null;
  }
  
  ByteBuffer getValueBytes() throws IOException  {
    _valueBuffer.flush();
    ByteBuffer currentReadBuf = _valueBuffer.read();
    if (_valueBuffer.available() == 0) { 
      return currentReadBuf;
    }
    else { 
      @SuppressWarnings("resource")
      DataOutputBuffer finalBuffer = new DataOutputBuffer();
      
      while (currentReadBuf != null) { 
        finalBuffer.write(currentReadBuf.array(),currentReadBuf.arrayOffset() + currentReadBuf.position(),currentReadBuf.remaining());
        currentReadBuf = _valueBuffer.read();
      }
      
      if (finalBuffer.getLength() != 0) { 
        return ByteBuffer.wrap(finalBuffer.getData(),0,finalBuffer.getLength());
      }
    }
    return null;
  }    
  
  RedisResponseBuilder.WaitState processReply() throws IOException  {      
    // first process response line payload 
    switch (_current.type) { 
      case Status: 
      case Error: { 
        _current.bValue = getValueBytes();
      }
      break;
      case Integer: { 
        _current.lValue = _longValue;
      }
      break;
      
      case Buffer:
      case Multi: { 
        int lenValue = (int) _longValue;
        
        if (lenValue > 0) {
          if (_current.type == Type.Buffer) {
            _current.lValue = lenValue;
          }
          else { 
            _current.values = new RedisResponse[lenValue];
          }
        }
      }
      break;
    }

    switch (_current.type) {
      // collapse simple nodes ...
      case Status:
      case Error:
      case Integer: { 
        return collapseCurrentNode();
      }
      
      // buffer nodes need to wait for buffer value ... 
      case Buffer: { 
        // if NULL string, collapse node now ... 
        if (_current.lValue == 0) { 
          return collapseCurrentNode();
        }
        else { 
          // transition to a waiting for buffer state ... 
          return WaitState.WAITING_FOR_BULK_BYTES;
        }
      }

      // multi nodes need to wait for child nodes ... 
      default: {
        // if multi node has no children 
        if (_current.values == null) { 
          // collapse it ... 
          return collapseCurrentNode();
        }
        // otherwise ... 
        else { 
          // push multi node onto context stack 
          _responseStack.push(_current);
          // null out current node 
          _current = null;
          // back to default state ... 
          return WaitState.WAITING_FOR_TYPE;
        }
      }
    }
  }
  
  private RedisResponseBuilder.WaitState collapseCurrentNode()throws IOException { 
    // see if current node has a parent .. 
    // if so, collapse the node into the parent 
    while (!_responseStack.isEmpty()) {  
      RedisResponse parent = _responseStack.pop();
      // add current node to parent node 
      boolean nodeComplete = parent.addValue(_current);
      // see if the parent node is complete (has all of its required children)
      if (nodeComplete) { 
        _current = parent;
      }
      else {
        // no, still more children to come ... 
        // push it back onto the stack 
        _responseStack.push(parent);
        // null current child 
        _current = null;
        break;
      }
    }
    
    // if we have a node in context, then it is the final node, promote it as such ... 
    if (_current != null) { 
      _final = _current;
      _current = null;
    }
    // revert to waiting for type or done, depending on whether we have closed the final node or not ... 
    return (_final == null) ? WaitState.WAITING_FOR_TYPE : WaitState.DONE;
  }

  
  private static RedisResponse.Type readType(ByteBuffer buffer) throws IOException {
    switch (buffer.get()) {
        case '+': return Type.Status;
        case '-': return Type.Error;
        case ':': return Type.Integer;
        case '$': return Type.Buffer;
        case '*': return Type.Multi;
        default:  throw new IOException("Invalid first byte");
    }
  }
}