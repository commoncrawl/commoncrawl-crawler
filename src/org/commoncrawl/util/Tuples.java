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

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.junit.Assert;

/**
 * 
 * @author rana
 *
 */
public class Tuples {
	
  
  /**
   * A basic Pair Tuple 
   * @author rana
   *
   */
  public static class Pair<T1,T2> { 
    public T1 e0;
    public T2 e1;
    
    public Pair(T1 e0,T2 e1) { 
      this.e0 = e0;
      this.e1 = e1;
    }
  }
  
  public static class Triple<T1,T2,T3> extends Pair<T1,T2> { 
    public T3 e2;
    
    public Triple(T1 e0,T2 e1,T3 e2) {
      super(e0,e1);
      this.e2 = e2;
    }    
  }

  public static class Quad<T1,T2,T3,T4> extends Triple<T1,T2,T3> { 
    public T4 e3;
    
    public Quad(T1 e0,T2 e1,T3 e2,T4 e3) {
      super(e0,e1,e2);
      this.e3 = e3;
    }    
  }  
  
	/**
	 * a tuple consisting of an Long and an Integer
	 * 
	 * @author rana
	 *
	 */
	public static class LongAndIntTuple implements WritableComparable<LongAndIntTuple> {

		long  longValue = -1;
		int 	intValue = -1;
		
		public void setLongValue(long value) { 
			longValue = value;
		}
		
		public long getLongValue() { 
			return longValue; 
		}

		public void setIntValue(int value) { 
			intValue = value;
		}
		
		public int getIntValue() { 
			return intValue; 
		}
		
		
		@Override
    public void readFields(DataInput in) throws IOException {
			longValue = WritableUtils.readVLong(in);
			intValue  = WritableUtils.readVInt(in);
    }

		@Override
    public void write(DataOutput out) throws IOException {
			WritableUtils.writeVLong(out, longValue);
			WritableUtils.writeVInt(out, intValue);
    }

		@Override
    public int compareTo(LongAndIntTuple o) {
	    int result = (longValue < o.longValue) ? -1 : (longValue > o.longValue) ? 1 : 0;
	    if (result == 0) { 
	    	result = (intValue < o.intValue) ? -1 : (intValue > o.intValue) ? 1 : 0;
	    }
			return result;
    }
	}
	
	/**
	 * 
	 * RawComparator for LongIntWriable
	 * 
	 * @author rana
	 *
	 */
	public static class LongIntRawComparator implements RawComparator<LongAndIntTuple> {

		DataInputBuffer stream1 = new DataInputBuffer();
		DataInputBuffer stream2 = new DataInputBuffer();
		
		@Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			
			stream1.reset(b1,s1,l1);
			stream2.reset(b2,s2,l2);
			
			try { 
				long longVal1 = WritableUtils.readVLong(stream1);
				long longVal2 = WritableUtils.readVLong(stream2);
				
				if (longVal1 < longVal2)
					return -1;
				else if (longVal1 > longVal2)
					return 1;
				else { 
					int intVal1 = WritableUtils.readVInt(stream1);
					int intVal2 = WritableUtils.readVInt(stream2);
					return (intVal1 < intVal2) ? -1 : (intVal1 > intVal2) ? 1 : 0;
				}
			}
			catch (IOException e) { 
				throw new RuntimeException(e);
			}
    }

		@Override
    public int compare(LongAndIntTuple o1, LongAndIntTuple o2) {
	    return o1.compareTo(o2);
    } 
		
	}
	
	
	/**
	 * a tuple consisting of an Integer and a Text field
	 * 
	 * @author rana
	 *
	 */
	public static class IntTextBytesTuple implements WritableComparable<IntTextBytesTuple> {

		int 	intValue = -1;
		TextBytes	textValue = new TextBytes();
		
		public void setIntValue(int value) { 
			intValue = value;
		}
		
		public int getIntValue() { 
			return intValue; 
		}
		
		public void setTextValueBytes(TextBytes txtValue) { 
			this.textValue = txtValue;
		}

		public TextBytes getTextValueBytes() { 
			return this.textValue;
		}
		
		
		@Override
    public void readFields(DataInput in) throws IOException {
			intValue = WritableUtils.readVInt(in);
			textValue.readFields(in);
    }

		@Override
    public void write(DataOutput out) throws IOException {
	    WritableUtils.writeVInt(out, intValue);
	    textValue.write(out);
    }

		@Override
    public int compareTo(IntTextBytesTuple o) {
	    int result = ((Integer)intValue).compareTo(o.intValue);
	    if (result == 0) { 
	    	result = textValue.compareTo(o.textValue);
	    }
			return result;
    } 
	}
	
	
	/**
	 * a tuple consisting of an float value and a Text field
	 * 
	 * @author rana
	 *
	 */
	public static class FloatTextBytesTuple implements WritableComparable<FloatTextBytesTuple> {

		float 	floatValue = 0.0f;
		TextBytes	textValue = new TextBytes();
		
		public void setFloatValue(float value) { 
			floatValue = value;
		}
		
		public float getFloatValue() { 
			return floatValue; 
		}
		
		public void setTextValueBytes(TextBytes txtValue) { 
			this.textValue = txtValue;
		}

		public TextBytes getTextValueBytes() { 
			return this.textValue;
		}
		
		
		@Override
    public void readFields(DataInput in) throws IOException {
			floatValue = in.readFloat();
			textValue.readFields(in);
    }

		@Override
    public void write(DataOutput out) throws IOException {
	    out.writeFloat(floatValue);
	    textValue.write(out);
    }

		@Override
    public int compareTo(FloatTextBytesTuple o) {
	    int result = ((Float)floatValue).compareTo(o.floatValue);
	    if (result == 0) { 
	    	result = textValue.compareTo(o.textValue);
	    }
			return result;
    } 
	}
	
	
	/**
	 * a tuple consisting of a Long and a Text field
	 * 
	 * @author rana
	 *
	 */
	public static class LongTextBytesTuple implements WritableComparable<LongTextBytesTuple> {

		long 	longValue = -1;
		TextBytes	textValue = new TextBytes();
		
		public void setLongValue(long value) { 
			longValue = value;
		}
		
		public long getLongValue() { 
			return longValue; 
		}
		
		public void setTextValueBytes(TextBytes txtValue) { 
			this.textValue = txtValue;
		}

		public TextBytes getTextValueBytes() { 
			return this.textValue;
		}
		
		
		@Override
    public void readFields(DataInput in) throws IOException {
			longValue = WritableUtils.readVLong(in);
			textValue.readFields(in);
    }

		@Override
    public void write(DataOutput out) throws IOException {
	    WritableUtils.writeVLong(out, longValue);
	    textValue.write(out);
    }

		@Override
    public int compareTo(LongTextBytesTuple o) {
	    int result = (longValue < o.longValue) ? -1 : (longValue == o.longValue) ? 0 : 1; 
	    if (result == 0) { 
	    	result = textValue.compareTo(o.textValue);
	    }
			return result;
    } 
	}

	public static class IntAndTwoTextByteTuples extends IntTextBytesTuple { 

		TextBytes	secondTextValue = new TextBytes();

		public void setSecondTextValueBytes(TextBytes txtValue) { 
			this.secondTextValue = txtValue;
		}

		public TextBytes getSecondTextValueBytes() { 
			return this.secondTextValue;
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
		  super.write(out);
		  secondTextValue.write(out);
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
		  super.readFields(in);
		  secondTextValue.readFields(in);
		}
		
		@Override
		public int compareTo(IntTextBytesTuple o) {
		  
		  int result = super.compareTo(o);
		  
		  if (result == 0) { 
		  	if (o instanceof IntAndTwoTextByteTuples) { 
		  		result = secondTextValue.compareTo(((IntAndTwoTextByteTuples)o).secondTextValue);
		  	}
		  }
		  return result;
		}
	}
	
	public static class TriTextBytesTuple implements WritableComparable<TriTextBytesTuple> {

		TextBytes firstValue = new TextBytes();
		TextBytes secondValue = new TextBytes();
		TextBytes thirdValue = new TextBytes();
		
		
		public void setFirstValue(TextBytes value) { 
			firstValue = value;
		}

		public void setSecondValue(TextBytes value) { 
			secondValue = value;
		}
		
		public void setThirdValue(TextBytes value) { 
			thirdValue = value;
		}

		public TextBytes getFirstValue() { 
			return firstValue;
		}

		public TextBytes getSecondValue() { 
			return secondValue;
		}

		public TextBytes getThirdValue() { 
			return thirdValue;
		}
		
		@Override
    public void readFields(DataInput in) throws IOException {
	   firstValue.readFields(in);
	   secondValue.readFields(in);
	   thirdValue.readFields(in);
    }

		@Override
    public void write(DataOutput out) throws IOException {
		   firstValue.write(out);
		   secondValue.write(out);
		   thirdValue.write(out);
    }

		@Override
    public int compareTo(TriTextBytesTuple o) {
	    int result = firstValue.compareTo(o.firstValue);
	    if (result == 0)
	    	result = secondValue.compareTo(o.secondValue);
	    if (result == 0) 
	    	result = thirdValue.compareTo(o.thirdValue);
	    
	    return result;
    } 
		
	}
	
	/**
	 * A tuple consisting of an integer field and a buffer 
	 * 
	 * @author rana
	 *
	 */
	public static class IntBufferTuple implements WritableComparable<IntBufferTuple> {

		int 		intValue = -1;
		FlexBuffer	bufferValue = new FlexBuffer();
		
		
		public void setIntValue(int value) { 
			intValue = value;
		}
		
		public int getIntValue() { 
			return intValue; 
		}
		
		public void setBuffer(FlexBuffer bufferValue) { 
			this.bufferValue = bufferValue;
		}

		public FlexBuffer getBuffer() { 
			return this.bufferValue;
		}		
		
		@Override
    public int compareTo(IntBufferTuple o) {

	    int result = ((Integer)intValue).compareTo(o.intValue);

	    if (result == 0) { 
	    	result = bufferValue.compareTo(o.bufferValue);
	    }
			return result;
    }

		@Override
    public void readFields(DataInput in) throws IOException {
			intValue = WritableUtils.readVInt(in);
			int bufferSize = WritableUtils.readVInt(in);
			bufferValue.setCapacity(bufferSize);
			in.readFully(bufferValue.get(), bufferValue.getOffset(),bufferSize);
			bufferValue.setCount(bufferSize);
    }

		@Override
    public void write(DataOutput out) throws IOException {
			WritableUtils.writeVInt(out,intValue);
			WritableUtils.writeVInt(out,bufferValue.getCount());
			if (bufferValue.getCount() > 0) { 
				out.write(bufferValue.get(), bufferValue.getOffset(), bufferValue.getCount());
			}
    } 
		
	}

	
	static void validateTextTuple() { 
		// validate tuple code 
		IntAndTwoTextByteTuples tuple1 = new IntAndTwoTextByteTuples();
		IntAndTwoTextByteTuples tuple2 = new IntAndTwoTextByteTuples();
		
		tuple1.setIntValue(1);
		tuple2.setIntValue(1);
		tuple1.setTextValueBytes(new TextBytes("AAAAA"));
		tuple2.setTextValueBytes(new TextBytes("AAAAA"));
		tuple1.setSecondTextValueBytes(new TextBytes("AAAAA"));
		tuple2.setSecondTextValueBytes(new TextBytes("AAAAB"));
		
		// compare the two 
		Assert.assertTrue(tuple1.compareTo(tuple2) == -1);

		tuple1.setTextValueBytes(new TextBytes("BAAAA"));
		Assert.assertTrue(tuple1.compareTo(tuple2) == 1);
		tuple2.setIntValue(2);
		Assert.assertTrue(tuple1.compareTo(tuple2) == -1);
		// ok restore ... 
		tuple1.setTextValueBytes(new TextBytes("AAAAA"));
		tuple2.setTextValueBytes(new TextBytes("AAAAA"));
		tuple1.setSecondTextValueBytes(new TextBytes("AAAAA"));
		tuple2.setSecondTextValueBytes(new TextBytes("AAAAB"));
		
		DataOutputBuffer outputBuffer = new DataOutputBuffer();
		
		try {
	    
			tuple1.write(outputBuffer);
	    tuple2.write(outputBuffer);
	    
	    IntAndTwoTextByteTuples tuple3 = new IntAndTwoTextByteTuples();
	    IntAndTwoTextByteTuples tuple4 = new IntAndTwoTextByteTuples();
	    
	    DataInputBuffer inputBuffer = new DataInputBuffer();
	    inputBuffer.reset(outputBuffer.getData(),0,outputBuffer.getLength());
	    
	    tuple3.readFields(inputBuffer);
	    tuple4.readFields(inputBuffer);
	    
	    Assert.assertTrue(tuple3.compareTo(tuple1) == 0);
	    Assert.assertTrue(tuple4.compareTo(tuple2) == 0);
	    
	    
	    
    } catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
    }
		
	}
	
	static void validateBufferTuple() {
		
  	// run some tests on the new code 
  	String aTestString = new String("A Test Strnig");
  	// convert it to bytes
  	byte bytes[] = aTestString.getBytes();
  	// over allocate an array 
  	byte overAllocated[] = new byte[bytes.length * 2];
  	// copy source 
  	System.arraycopy(bytes, 0, overAllocated, bytes.length, bytes.length);
  	
		IntBufferTuple tuple1 = new IntBufferTuple();
		IntBufferTuple tuple2 = new IntBufferTuple();
		
		tuple1.setIntValue(1);
		tuple2.setIntValue(1);
		tuple1.getBuffer().set(overAllocated,bytes.length,bytes.length);
		tuple2.getBuffer().set(overAllocated,bytes.length,bytes.length);
		
		Assert.assertTrue(tuple1.compareTo(tuple2) == 0);
		
		DataOutputBuffer outputBuffer = new DataOutputBuffer();
		
		try { 
			tuple1.write(outputBuffer);
			tuple2.write(outputBuffer);
			
			DataInputBuffer inputBuffer = new DataInputBuffer();
			
			inputBuffer.reset(outputBuffer.getData(),0,outputBuffer.getLength());
			
			tuple1.readFields(inputBuffer);
			tuple2.readFields(inputBuffer);
			
			Assert.assertTrue(tuple1.compareTo(tuple2) == 0);
			
			DataOutputBuffer outputBuffer2 = new DataOutputBuffer();
			
			tuple1.write(outputBuffer2);
			tuple2.write(outputBuffer2);
			
			Assert.assertTrue(WritableComparator.compareBytes(outputBuffer.getData(),0,outputBuffer.getLength(),outputBuffer2.getData(),0,outputBuffer2.getLength()) == 0);
		}
		catch (IOException e) { 
			e.printStackTrace();
		}
		
	}
	
	public static void main(String[] args) {
	  
		validateTextTuple();
		validateBufferTuple();
		
  }
}
