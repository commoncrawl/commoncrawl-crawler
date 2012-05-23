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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.protocol.CrawlURLMetadata;
import org.commoncrawl.rpc.base.shared.RPCStruct;

/**
 * 
 * @author rana
 *
 */
public class RPCStructIntrospector {


  private static final Log LOG = LogFactory.getLog(RPCStructIntrospector.class);
  HashSet<String> propertyNames = new HashSet<String>();
  HashMap<String,Method> getters = new HashMap<String,Method>();
  Class theClass;
  public RPCStructIntrospector(Class classType) {
    theClass = classType;
    for (Field field : classType.getDeclaredFields()) {
      System.out.println("Found Field:" + field.getName());
      if (!field.isEnumConstant()) { 
        propertyNames.add(field.getName());
      }
    }
    
    for (Method m : classType.getDeclaredMethods()) { 
      System.out.println("Found Method:" + m.getName());
      if (m.getName().startsWith("get")) { 
        getters.put(m.getName(),m);
      }
    }
  }
  
  public Set<String> getPropertyNames() { 
  	return propertyNames; 
  }
  
  static String toCamelCase(String name) {
    char firstChar = name.charAt(0);
    if (Character.isLowerCase(firstChar)) {
      return ""+Character.toUpperCase(firstChar) + name.substring(1);
    }
    return name;
  }
  
  public String getStringValueGivenName(RPCStruct object,String propertyName) { 
    Method m;
    try {
      m = theClass.getMethod("get"+ toCamelCase(propertyName));
      if (m != null) { 
        try {
          return m.invoke(object,new Object[0]).toString();
        } catch (IllegalArgumentException e) {
          LOG.error(CCStringUtils.stringifyException(e));
        } catch (IllegalAccessException e) {
          LOG.error(CCStringUtils.stringifyException(e));
        } catch (InvocationTargetException e) {
          LOG.error(CCStringUtils.stringifyException(e));
        }
      }
      
    } catch (Exception e) {
      LOG.error(CCStringUtils.stringifyException(e));
    }
    return "NOT-FOUND";
  }
  
  public double getDoubleValueGivenName(RPCStruct object,String propertyName) { 
    Method m;
    try {
      m = theClass.getMethod("get"+ toCamelCase(propertyName));
      
      if (m != null) { 
      try {
        Object value = m.invoke(object,new Object[0]).toString();
        if (value instanceof String) { 
          return Double.parseDouble((String)value);
        }
        else if (value instanceof Integer) { 
          return (double)((Integer)value).doubleValue();
        }
        else if (value instanceof Long) { 
          return (double)((Long)value).doubleValue();
        }
        else if (value instanceof Short) { 
          return (double)((Short)value).doubleValue();
        }
        else if (value instanceof Float) { 
          return (double)((Float)value).doubleValue();
        }
        else if (value instanceof Byte) { 
          return (double)((Byte)value).doubleValue();
        }
        else if (value instanceof Boolean) { 
          return (double) (((Boolean)value).booleanValue() ? 1.0f : 0.0f);
        }
        else { 
          throw new RuntimeException("Unable to convert Return Value to double:" + value.toString());
        }
      } catch (IllegalArgumentException e) {
        LOG.error(CCStringUtils.stringifyException(e));
      } catch (IllegalAccessException e) {
        LOG.error(CCStringUtils.stringifyException(e));
      } catch (InvocationTargetException e) {
        LOG.error(CCStringUtils.stringifyException(e));
      }
    }
    }
    catch (Exception e) { 
      LOG.error(CCStringUtils.stringifyException(e));
    }
    return Double.NaN;
  }
  
  public void dumpGetters() { 
    for (String name : getters.keySet()) { 
      System.out.println(name);
    }
  }
  
  public void dumpProperties(Object o) { 
    for (Method m : getters.values()) { 
      try {
        System.out.println(m.invoke(o,null));
      } catch (IllegalArgumentException e) {
        e.printStackTrace();
      } catch (IllegalAccessException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (InvocationTargetException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }
  
  public static void main(String[] args) {
    RPCStructIntrospector test = new RPCStructIntrospector(CrawlURLMetadata.class);
    CrawlURLMetadata metadata = new CrawlURLMetadata();
    test.dumpGetters();
    test.dumpProperties(metadata);
  }
}
