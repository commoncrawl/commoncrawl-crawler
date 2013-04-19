package org.commoncrawl.hadoop.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.mapred.InputSplit;

public class TextDatumInputSplit implements InputSplit {

  String datum;
 
  public TextDatumInputSplit(String data) { 
    datum = data;
  }
  
  
  public TextDatumInputSplit() { 
    datum = new String();
  }
  
  public String getDatum() { 
    return datum;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(datum);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    datum = in.readUTF();
  }

  @Override
  public long getLength() throws IOException {
    return 1;
  }

  @Override
  public String[] getLocations() throws IOException {
    return new String[0];
  }

}
