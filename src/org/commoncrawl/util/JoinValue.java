/**
 * Copyright 2008 - CommonCrawl Foundation
 * 
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 **/
package org.commoncrawl.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * 
 * @author rana
 *
 */
public class JoinValue implements Writable {
  
  public static final int TEXT_TYPE_JOIN_VALUE = 1;
  public static final int INT_TYPE_JOIN_VALUE = 2;
  public static final int LONG_TYPE_JOIN_VALUE = 3;
  public static final int DOUBLE_TYPE_JOIN_VALUE = 4;
  public static final int BOOLEAN_TYPE_JOIN_VALUE = 5;
  public static final int NULL_TYPE_JOIN_VALUE = 10;
  
  TextBytes   _tag;
  int         _type;
  long        _longValue;
  double      _doubleValue;
  boolean     _boolValue;
  TextBytes   _textValue;

  public JoinValue() { 
    
  }
  
  public JoinValue(TextBytes tag) { 
    _tag = tag;
    _type = NULL_TYPE_JOIN_VALUE;
  }
  
  public JoinValue(TextBytes tag,Text value) { 
    _tag = tag;
    _type = TEXT_TYPE_JOIN_VALUE;
    _textValue = new TextBytes();
    _textValue.set(value.getBytes(),0,value.getLength());
  }
  
  public JoinValue(TextBytes tag,TextBytes value) { 
    _tag = tag;
    _type = TEXT_TYPE_JOIN_VALUE;
    _textValue = value;
  }

  public JoinValue(TextBytes tag,IntWritable value) { 
    _tag = tag;
    _type = INT_TYPE_JOIN_VALUE;
    _longValue = value.get();
  }

  public JoinValue(TextBytes tag,LongWritable value) { 
    _tag = tag;
    _type = LONG_TYPE_JOIN_VALUE;
    _longValue = value.get();
  }
  
  public JoinValue(TextBytes tag,DoubleWritable value) { 
    _tag = tag;
    _type = DOUBLE_TYPE_JOIN_VALUE;
    _doubleValue = value.get();
  }
  
  public JoinValue(TextBytes tag,BooleanWritable value) { 
    _tag = tag;
    _type = BOOLEAN_TYPE_JOIN_VALUE;
    _boolValue = value.get();
  }  
  
  
  @Override
  public void readFields(DataInput in) throws IOException {
    _tag = new TextBytes();
    _tag.readFields(in);
    _type = in.readByte();
    switch (_type) {
      case NULL_TYPE_JOIN_VALUE: break;
      case INT_TYPE_JOIN_VALUE: _longValue = in.readInt();break;
      case LONG_TYPE_JOIN_VALUE: _longValue = in.readLong();break;
      case TEXT_TYPE_JOIN_VALUE: {
        _textValue = new TextBytes();
        _textValue.readFields(in);
      }
      break;
      case DOUBLE_TYPE_JOIN_VALUE: _doubleValue = in.readDouble();break;
      case BOOLEAN_TYPE_JOIN_VALUE: _boolValue = in.readBoolean();break;
      default: throw new IOException("Unknown Join Value Type");
    }
  }

  public String    getTag() { return _tag.toString(); }
  public int       getType() { return _type; }
  
  public boolean   isTextType() { return _type == TEXT_TYPE_JOIN_VALUE; }
  public TextBytes getTextValue() { return _textValue; } 
  public long      getLongValue() { return _longValue; }
  public double    getDoubleValue() { return _doubleValue; }
  public boolean   getBooleanValue() { return _boolValue; }
  
  @Override
  public void write(DataOutput out) throws IOException {
    _tag.write(out);
    out.writeByte(_type);
    switch (_type) { 
      case NULL_TYPE_JOIN_VALUE:break; 
      case INT_TYPE_JOIN_VALUE: out.writeInt((int)_longValue);break;
      case LONG_TYPE_JOIN_VALUE: out.writeLong(_longValue);break;
      case TEXT_TYPE_JOIN_VALUE: _textValue.write(out);break;
      case DOUBLE_TYPE_JOIN_VALUE: out.writeDouble(_doubleValue);break;
      case BOOLEAN_TYPE_JOIN_VALUE: out.writeBoolean(_boolValue);break;
      default: throw new IOException("Unknown Join Value Type");
    }
  }
  
  public static JoinValue getJoinValue(TextBytes tag,Writable type)throws IOException { 
    if (type.getClass() == Text.class) { 
      return new JoinValue(tag,(Text)type);
    }
    else if (type.getClass() == TextBytes.class) { 
      return new JoinValue(tag,(TextBytes)type);
    }
    else if (type.getClass() == IntWritable.class) { 
      return new JoinValue(tag,(IntWritable)type);
    }
    else if (type.getClass() == LongWritable.class) { 
      return new JoinValue(tag,(LongWritable)type);
    }
    else if (type.getClass() == DoubleWritable.class) { 
      return new JoinValue(tag,(DoubleWritable)type);
    }
    else if (type.getClass() == BooleanWritable.class) { 
      return new JoinValue(tag,(BooleanWritable)type);
    }
    else if (type.getClass() == NullWritable.class) { 
      return new JoinValue(tag);
    }
    throw new IOException("Unsupported Join Value Type:"+ type.getClass());
  }
}