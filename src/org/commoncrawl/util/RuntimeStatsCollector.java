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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

import javax.servlet.jsp.JspWriter;

/**
 * 
 * @author rana
 *
 */
public class RuntimeStatsCollector {

  public RuntimeStatsCollector() { 

  }
  
  public static abstract class Namespace { 
    
    
  }
  
  
  @SuppressWarnings("unchecked")
  public static void registerNames(Namespace namespaceId, Enum[] names) { 
      for (int id=0;id<names.length;++id) { 
        _idToNameMap.put(makeId(namespaceId,names[id]),names[id].toString());
        _nameToIdMap.put(names[id].toString(),makeId(namespaceId,names[id]));
      }
  }
 
  @SuppressWarnings("unchecked")
  private static long makeId(Namespace namespace, Enum value) { 
    return (((long)namespace.getClass().hashCode()) << 32) | (long)value.ordinal();
  }
  
  public static class StringList implements Iterable<String>{ 
    
    private int maxItemLimit;
    private LinkedList<String> list = new LinkedList<String>();
   
    StringList(int maxItemLimit) { 
      this.maxItemLimit = maxItemLimit;
    }
    
    public void add(String e) {
      if (list.size() == maxItemLimit) { 
        list.removeFirst();
      }
      list.addLast(e);
    }

    public Iterator<String> iterator() {
      return list.iterator();
    }
    
    @Override
    public String toString() {
      StringBuffer buf  = new StringBuffer();
      
      for (String value : list) { 
        buf.append(value + "\n");
      }
      
      return buf.toString();
    }
  }
  
  public static class StringVector implements Iterable<String>{ 
    
    private String array[];
   
    StringVector(int arraySize) { 
      array = new String[arraySize];
    }
    
    public void setValue(int index,String value) {
      if (index < array.length) { 
        array[index] = value;
      }
    }

    public Iterator<String> iterator() {
      return Arrays.asList(array).iterator();
    }
    
    @Override
    public String toString() {
      StringBuffer buf  = new StringBuffer();
      
      for (int i=0;i<array.length;++i) { 
        
        if (array[i] != null) { 
          
          buf.append("[");
          buf.append(String.format("%1$4.4s",i ));
          buf.append("] - ");
          buf.append(array[i]);
          buf.append("\n");
        }
      }
      return buf.toString();
    }
  }  
  
  protected HashMap<Long,Object> _values = new HashMap<Long,Object>();
  protected static HashMap<Long,String> _idToNameMap = new HashMap<Long,String>();
  protected static HashMap<String,Long> _nameToIdMap = new HashMap<String,Long>();

  
  @SuppressWarnings("unchecked")
  public synchronized int getIntValue(Enum name) { 
    Integer value = (Integer)_values.get(name.ordinal());
    if (value != null)
      return value;
    return -1;
  }
  
  @SuppressWarnings("unchecked")
  public synchronized long getLongValue(Namespace namespace,Enum name) { 
    Long value = (Long)_values.get(makeId(namespace,name));
    if (value != null)
      return value;
    return -1;
  }

  @SuppressWarnings("unchecked")
  public synchronized String getStringValue(Namespace namespace,Enum name) { 
    String value = (String) _values.get(makeId(namespace,name));
    if (value != null)
      return value;
    return "";
  }
  
  @SuppressWarnings("unchecked")
  public Iterator<String> getListValues(Namespace namespace,Enum name) { 
    StringList list = (StringList)_values.get(makeId(namespace,name));
    if (list == null) { 
      return Collections.EMPTY_LIST.iterator();
    }
    else { 
      return list.iterator();
    }
  }
  
  @SuppressWarnings("unchecked")
  public synchronized void addListValue(Namespace namespace,Enum name,int maxItems,String value) { 
    StringList list = (StringList) _values.get(makeId(namespace,name));
    if (list == null) { 
      list = new StringList(maxItems);
      _values.put(makeId(namespace,name), list);
    }
    list.add(value);
  }
  
  @SuppressWarnings("unchecked")
  public synchronized void setArrayValue(Namespace namespace,Enum name,int arraySize,int index,String value) { 
    StringVector array = (StringVector)_values.get(makeId(namespace,name));
    if (array == null) { 
      array = new StringVector(arraySize);
      _values.put(makeId(namespace,name), array);
    }
    array.setValue(index, value);
  }
  
  
  @SuppressWarnings("unchecked")
  public synchronized void setIntValue(Namespace namespace,Enum name,int value) { 
    _values.put(makeId(namespace,name), new Integer(value));
  }

  @SuppressWarnings("unchecked")
  public synchronized void incIntValue(Namespace namespace,Enum name) { 
    Integer value = (Integer) _values.get(makeId(namespace,name));
    if (value != null) { 
      _values.put(makeId(namespace,name), (Integer)(value + 1));
    }
    else {
      _values.put(makeId(namespace,name),(Integer)1);
    }
  }

  @SuppressWarnings("unchecked")
  public synchronized void incLongValue(Namespace namespace,Enum name) { 
    Long value = (Long)_values.get(name.ordinal());
    if (value != null) { 
      _values.put(makeId(namespace,name), (Long)(value + 1));
    }
    else {
      _values.put(makeId(namespace,name),(Long)1L);
    }
  }
  
  
  @SuppressWarnings("unchecked")
  public synchronized void setLongValue(Namespace namespace,Enum name,long  value) { 
    _values.put(makeId(namespace,name), value);
  }

  @SuppressWarnings("unchecked")
  public synchronized void setDoubleValue(Namespace namespace,Enum name,double value) { 
    _values.put(makeId(namespace,name), value);
  }
  
  @SuppressWarnings("unchecked")
  public synchronized void setStringValue(Namespace namespace,Enum name,String value) { 
    _values.put(makeId(namespace,name), value);
  }
  
  public void dumpStatsToHTML(JspWriter out)throws IOException { 
    out.write("<table border=1>");
    out.write("<tr><td>Name</td><td width=50%>Value</td><td>Name</td><td width=50%>Value</td></tr>");
    
    Object[] keys = _nameToIdMap.keySet().toArray();
    Arrays.sort(keys);
    
    int dumpedKeyCount = 0;
    out.write("<tr>");
    for (Object key : keys) { 
      long id = _nameToIdMap.get(key);
      Object value = _values.get(id);
      dumpValue(out,id,value);
      if (++dumpedKeyCount % 2 == 0) { 
        out.write("</tr><tr>");
      }
    }
    if (dumpedKeyCount %2 != 0) { 
      out.write("<td>&nbsp;</td><td>&nbsp;</td></tr>");
    }
    out.write("</table>");
  }
  
  public void dumpValue(JspWriter out,Object  key, Object value)throws IOException { 
      out.write("<td>");
      
      long keyId = (Long)key;
      String keyName = _idToNameMap.get(keyId);
      if (keyName == null)
        keyName = "[" +(int) (keyId>> 32) + "]:["+(int)keyId+"]";
      
      out.write(keyName);
      out.write("</td>");
      out.write("<td><pre>");
      if (value != null)
        out.write(value.toString());
      else 
        out.write("NULL");
      out.write("</pre></td>");
  }
}
