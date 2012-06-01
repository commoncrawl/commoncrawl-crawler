/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.commoncrawl.rpc.compiler;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;
import java.io.StringWriter;
import java.math.BigInteger;

import org.commoncrawl.rpc.base.shared.BinaryProtocol;

import com.google.gson.JsonObject;
import com.google.gson.stream.JsonWriter;

/**
 */
public class JRecord extends JCompType {
  
  public static class Modifiers { 
    public static int HAS_RECORDID = 1 << 0;
    public static int HAS_NO_DIRTY_TRACKING = 1 << 1;
    public static int IS_ANONYMOUS_RECORD = 1 << 2;
  }  
  
  class JavaRecord extends JavaCompType {
   
    private String fullName;
    private String name;
    private String module;
    private ArrayList<JField<JavaType>> fields = new ArrayList<JField<JavaType>>();
    private ArrayList<JEnum> enums = new ArrayList<JEnum>(); 
    private Vector < JField<JavaType> >  keys = new Vector < JField<JavaType> >();
    private int modifiers = 0;
    
    JavaRecord(String name, ArrayList<JField<JType>> flist,ArrayList<JEnum> enums,Set<String> modifiers) {
      super(name, "Record", name);
      this.fullName = name;
      int idx = name.lastIndexOf('.');
      this.name = name.substring(idx+1);
      this.module = name.substring(0, idx);
      for (Iterator<JField<JType>> iter = flist.iterator(); iter.hasNext();) {
        JField<JType> f = iter.next();
        JField<JavaType> newField = new JField<JavaType>(f.getName(), f.getType().getJavaType(),f.getOrdinal(),f.getModifiers());
       fields.add(newField);
       if ((newField.getModifiers() & JField.Modifiers.KEY)!= 0) {
         if (!newField.getType().isComparable()) { 
           throw new Error("key attribute assigned to a non Comparable Field:" + newField + " Record:" + getFullName());
         }
         keys.add(newField);
       }
      }
      this.enums = enums;
      parseModifiers(modifiers);
    }
    
    @Override
    boolean isIntrinsicType() {
      return false;
    }
    
    private void parseModifiers(Set<String> modifierSet) {
      if (modifierSet != null) { 
        for (String modifier : modifierSet) { 
          if (modifier.equals("recordid")) { 
            modifiers |= Modifiers.HAS_RECORDID;
          }
          else if (modifier.equals("nodirty")) { 
            modifiers |= Modifiers.HAS_NO_DIRTY_TRACKING;
          }
          else if (modifier.equals("anonymous")) {
            modifiers |= Modifiers.IS_ANONYMOUS_RECORD;
          }
          else { 
            throw new Error("Invalid Record Modifier token:" + modifier + " encountered while parsing Record:" + getFullName());
          }
        }
      }
    }
    
    @Override
    boolean isComparable() {
      return keys.size() != 0;
    }
    
    private boolean trackDirtyFields() { 
      return (modifiers & Modifiers.HAS_NO_DIRTY_TRACKING) == 0; 
    }
    
    public String getFullName() { 
    	return fullName;
    }
    
    public String getShortName() { 
    	return name;
    }
    
    public Vector< JField<JavaType> > getKeys() { 
    	return keys; 
    }
    
    public int getKeyCount() { 
      return keys.size();
    }
    
    @Override
    void genReadMethod(CodeBuffer cb, String fname, String tag, boolean decl) {
      if (decl) {
        cb.append(fullName+" "+fname+";\n");
      }
      cb.append(fname+"= new "+fullName+"();\n");
      cb.append(fname+".deserialize(input,decoder);\n");
    }
    
    @Override
    void genSkipMethod(CodeBuffer cb) {
      cb.append(fullName+".skipTo(input,decoder,Integer.MAX_VALUE);\n");
    } 
    
    @Override
    void genWriteMethod(CodeBuffer cb, String fname, String tag) {
      cb.append(fname+".serialize(output,encoder);\n");
    }
    
    void genCode(String destDir, ArrayList<String> options) throws IOException {
      String pkg = module;
      String pkgpath = pkg.replaceAll("\\.", "/");
      File pkgdir = new File(destDir, pkgpath);
      if (!pkgdir.exists()) {
        // create the pkg directory
        boolean ret = pkgdir.mkdirs();
        if (!ret) {
          throw new IOException("Cannnot create directory: "+pkgpath);
        }
      } else if (!pkgdir.isDirectory()) {
        // not a directory
        throw new IOException(pkgpath+" is not a directory.");
      }
      File jfile = new File(pkgdir, name+".java");
      FileWriter jj = new FileWriter(jfile);
      
      CodeBuffer cb = new CodeBuffer();
      cb.append("// File generated by rpc compiler. Do not edit.\n\n");
      cb.append("package "+module+";\n\n");
      cb.append("import org.apache.hadoop.io.DataInputBuffer;\n");
      cb.append("import java.io.DataInput;\n");
      cb.append("import java.io.DataOutput;\n");
      cb.append("import java.util.BitSet;\n");
      cb.append("import java.io.IOException;\n");
      cb.append("import org.apache.hadoop.io.Writable;\n");
      cb.append("import org.apache.hadoop.io.WritableComparable;\n");
      cb.append("import org.apache.hadoop.record.Buffer;\n");
      cb.append("import org.commoncrawl.util.FlexBuffer;\n");
      cb.append("import org.commoncrawl.util.TextBytes;\n");
      cb.append("import org.commoncrawl.util.MurmurHash;\n");
      cb.append("import org.commoncrawl.util.ImmutableBuffer;\n");
      cb.append("import org.commoncrawl.rpc.base.shared.BinaryProtocol;\n");
      cb.append("import org.apache.hadoop.util.ReflectionUtils;\n");
      cb.append("import org.apache.hadoop.conf.Configuration;\n");
      cb.append("import java.io.StringWriter;\n");
      cb.append("import com.google.gson.JsonObject;\n");
      cb.append("import com.google.gson.stream.JsonWriter;\n");
      
      
      cb.append("// Generated File: "+name+"\n");
      cb.append("public class "+name);
      if ((modifiers & Modifiers.HAS_RECORDID) != 0) { 
        cb.append(" extends org.commoncrawl.rpc.base.shared.RPCStructWithId ");
      }
      else { 
        cb.append(" extends org.commoncrawl.rpc.base.shared.RPCStruct ");
      }
                
      
      // if the field has no key then implement Writable only  
      if (this.getKeyCount() == 0) { 
    	  cb.append(" implements Writable");
      }
      // otherwise implement WritableComparable
      else {
        // this check has moved to initialization 
        /*
        if (!this.getKey().getType().isComparable()) { 
    		  throw new Error("Invalid Key Type in Record:"+this.fullName);
    	  }
    	  */
    	  cb.append(" implements WritableComparable ");
      }
      cb.append("{\n\n");
      
      cb.append("// optimized constructor helper \n");
      cb.append("public static " + name + " newInstance(Configuration conf) {\n");
      cb.append("  return ReflectionUtils.newInstance(" + name + ".class,conf);\n");
      cb.append("}\n");
      		
      
      cb.append("// Writable Implementation\n");
      cb.append("public void write(DataOutput out) throws IOException{ \n");
      cb.append("this.serialize(out,new BinaryProtocol());\n");
      cb.append("}\n\n");
      cb.append("public void readFields(DataInput  in) throws IOException{ \n");
      cb.append("this.deserialize(in,new BinaryProtocol());\n");
      cb.append("}\n\n");
      
      // now if sortable filed is available ... 
      if (this.getKeyCount() != 0) { 
    	  cb.append("// Comparable Implementation\n");
    	  cb.append("public int compareTo(Object other) {\n");
    	  int keyCount = 0;
    	  for (JField<JavaType> field : getKeys()) {
    	    if (keyCount++ == 0) { 
    	      cb.append("int result = ");
    	    }
    	    else { 
    	      cb.append("if (result == 0) result = ");
    	    }
      	  cb.append(field.getType().genCompareTo(field.getName(),"other",this.getFullName()) + ";\n");
    	  }
    	  cb.append("return result;\n");
    	  cb.append("}\n");
    	  cb.append("// getKey Implementation\n");
    	  cb.append("public String getKey(){\n");
    	  if (getKeyCount() == 1) { 
    	    cb.append(" return " + this.getKeys().get(0).getType().genGetKey(getKeys().get(0).getName()) + ";\n");
    	  }
    	  else { 
    	    cb.append(" return");
    	    keyCount = 0;
          for (JField<JavaType> field : getKeys()) {
            if (keyCount++ != 0)   
              cb.append("+ \"_\" +");
            cb.append("\"" + field.getName() + "_\"+" + field.getType().genGetKey(field.getName()));
          }
          cb.append(";\n");
    	  }
    	 cb.append("}\n\n");
      }
      
      cb.append("\n");
      cb.append("// Field Constants\n");
      
      int fieldOrdinalMax = 0;
      JField<JavaType> fieldMax = null;
      
      // generate constants for field ids ...
      for (Iterator<JField<JavaType>> i = fields.iterator(); i.hasNext();) {
    	  JField<JavaType> jf = i.next();
    	  if ((jf.getModifiers() & JField.Modifiers.TRANSIENT) == 0) { 
      	  if (jf.getOrdinal() > fieldOrdinalMax) { 
      		  fieldOrdinalMax = jf.getOrdinal();
      		  fieldMax           = jf;
      	  }
      	  cb.append("public static final int Field_"+ jf.getName().toUpperCase()+" = " + Integer.toString(jf.getOrdinal()) + ";\n");
    	  }
      }
      cb.append("static final int FieldID_MAX=Field_"+fieldMax.getName().toUpperCase()+";\n");
      cb.append("\n");
      
      if (this.enums.size() != 0) { 
    	  
    	  cb.append("// Enumerations\n\n");
    	  
    	  for (JEnum e : this.enums) { 
    		  
    		  cb.append("// Enum:"+e.getName()+"\n");
    		  cb.append("public static final class "+e.getName() + " {\n");
    		  
    		  for (JEnumValue v : e.getValues()) { 
    			  cb.append("public static final int "+v.getName()+" = "+Integer.toString(v.getValue()) + ";\n");
    		  }
    		  
    		  // next generate a enum value to desc method 
    		  cb.append("\n");
    		  cb.append("public static String toString(int enumValue){\n");
    		  HashSet<Integer> visited = new HashSet<Integer>();
    		  cb.append("switch (enumValue) {\n");
    		  for (JEnumValue v : e.getValues()) { 
    		    if (!visited.contains(v.getValue())) { 
    		      cb.append("case " + v.getValue()+": return \""+v.getName() + "\";\n");
    		      visited.add(v.getValue());
    		    }
          }
    		  cb.append("default: return \"\";\n");
    		  cb.append("}\n");
    		  cb.append("}\n");
    		  
    		  cb.append("}\n");
    	  }
      }
      
      cb.append("// Field Declarations\n");
      if (trackDirtyFields()){ 
        cb.append("private BitSet __validFields = new BitSet(FieldID_MAX+1);\n\n");
      }
      //TODO: FIX FIELD DECLARATIONS AS NECESSARY
      for (Iterator<JField<JavaType>> i = fields.iterator(); i.hasNext();) {
        JField<JavaType> jf = i.next();
        String name = jf.getName();
        JavaType type = jf.getType();
        type.genDecl(cb, name);
      }
      cb.append("\n");
      
      cb.append("// Default Constructor\n");
      //TODO: FIX DEFAULT CONSTRUCTOR AS NECESSARY 
      cb.append("public "+name+"() { }\n\n");
      
      
      cb.append("// Accessors\n\n");
      if (trackDirtyFields()) { 
        cb.append("public final boolean isFieldDirty(int fieldId) { return __validFields.get(fieldId); }\n");	
        cb.append("public final void setFieldDirty(int fieldId) { __validFields.set(fieldId); }\n\n");
        cb.append("public final void setFieldClean(int fieldId) { __validFields.clear(fieldId); }\n\n");
      }
      else { 
        cb.append("public final boolean isFieldDirty(int fieldId) { return true; }\n"); 
        cb.append("public final void setFieldDirty(int fieldId) { }\n\n");
        cb.append("public final void setFieldClean(int fieldId) { }\n\n");
      }
      
      //TODO: FIX GET SET IF NECESSARY
      for (Iterator<JField<JavaType>> i = fields.iterator(); i.hasNext();) {
        JField<JavaType> jf = i.next();
        String name = jf.getName();
        JavaType type = jf.getType();
        type.genGetSet(cb, name,(jf.getModifiers() & JField.Modifiers.TRANSIENT) == 0 && trackDirtyFields());
      }
      
      cb.append("// Object Dirty support \n\n");
      if (trackDirtyFields()) { 
        cb.append("public final boolean isObjectDirty(){\n");
        cb.append("boolean isDirty = !__validFields.isEmpty();\n");
        boolean requiresNestedCheck = false;
        // now if the object has nested complex types ... 
        for (Iterator<JField<JavaType>> i = fields.iterator(); i.hasNext();) {
          JField<JavaType> jf = i.next();
          if ((jf.getModifiers() & JField.Modifiers.TRANSIENT) == 0) { 
            String name = jf.getName();
            JavaType type = jf.getType();
            
            if (type.hasDirtyState()) { 
              if (requiresNestedCheck == false) { 
                cb.append("if (!isDirty){\n");
                requiresNestedCheck = true;
              }
              type.genDirtyCheck(cb,name);
            }
          }
        }
        if (requiresNestedCheck) { 
          cb.append("}\n");
        }
        cb.append("return isDirty;\n");
        cb.append("}\n");
      }
      else { 
        cb.append("public final boolean isObjectDirty(){ return true; } \n");
      }
      cb.append("\n");
      
      cb.append("// serialize implementation \n");
      //TODO: FIX SERIALIZE
      cb.append("public final void serialize("+
                "DataOutput output,BinaryProtocol encoder)\n"+
                "throws java.io.IOException {\n");
      
      
      
      cb.append("encoder.beginFields(output);\n");
      for (Iterator<JField<JavaType>> i = fields.iterator(); i.hasNext();) {
        JField<JavaType> jf = i.next();
        if ((jf.getModifiers() & JField.Modifiers.TRANSIENT) == 0) { 
          String name = jf.getName();
          JavaType type = jf.getType();
          int ordinal = jf.getOrdinal();
          cb.append("// serialize field:"+name+"\n");
          
          if (trackDirtyFields()) { 
            // use type specific validity check (to address dirtying of child objects properly)
            if (type.hasDirtyState()) { 
              type.genValidFieldCheck(cb, name);
            }
            else { 
              cb.append("if (__validFields.get(Field_"+name.toUpperCase()+"))");
            }
          }
          cb.append("{\n");
          // ok if field tracks dirty state and we are serializing dirty state 
          if (trackDirtyFields() && type.hasDirtyState()) { 
          	// modify source object's dirty flags 
          	cb.append("__validFields.set(Field_"+name.toUpperCase()+");\n");
          }
          cb.append("encoder.beginField(output,\""+name+"\",Field_"+name.toUpperCase()+");\n");
          type.genWriteMethod(cb, name, name);
          //cb.append("encoder.endField(output,\""+name+"\",Field_"+name.toUpperCase()+");\n");
          cb.append("}\n");
          if (trackDirtyFields() && type.hasDirtyState()) {
            cb.append("else {\n");
          	
            // synchronize dirty state for this complex type with parent object 
          	cb.append("__validFields.clear(Field_"+name.toUpperCase()+");\n");

          	cb.append("}\n");
          }
        }
      }
      
      cb.append("encoder.endFields(output);\n");
      
      cb.append("}\n");
      
      cb.append("// skip to implementation \n");
      cb.append("public static final int skipTo(DataInputBuffer input, BinaryProtocol decoder,int targetFieldId) throws java.io.IOException {\n");
      //cb.append("System.out.println(\"skipTo callendOn Type:" + fullName + " targetFieldId:\" + targetFieldId);\n");
      cb.append("// keep reading fields until terminator (-1) is located \n");
      cb.append("int fieldId;\n");
      cb.append("input.mark(-1);\n");
      cb.append("while ((fieldId = decoder.readFieldId(input)) != -1) { \n");
      //cb.append("System.out.println(\"got fieldId:\" + fieldId);\n");
      // terminate early if field id is located ... 
      cb.append("  if (targetFieldId == fieldId) {\n");
      //cb.append("    System.out.println(\"match! Exiting\");\n");
      cb.append("    return fieldId;\n");
      cb.append("  }\n");
      cb.append("  else if (fieldId > targetFieldId) {\n");
      //cb.append("    System.out.println(\"Exceeded Bounds.Pre Reset:\" + input.getPosition());\n");
      cb.append("    input.reset();\n");
      //cb.append("    System.out.println(\"Exceeded Bounds.Post Reset:\" + input.getPosition());\n");
      cb.append("    return fieldId;\n");
      cb.append("  }\n");      
      cb.append("  else {\n");       
      cb.append("switch (fieldId) { \n");
      for (Iterator<JField<JavaType>> i = fields.iterator(); i.hasNext();) {
        JField<JavaType> jf = i.next();
        if ((jf.getModifiers() & JField.Modifiers.TRANSIENT) == 0) { 
          String name = jf.getName();
          JavaType type = jf.getType();
          cb.append("case Field_"+name.toUpperCase()+":{\n");
          	type.genSkipMethod(cb);
          cb.append("}\n");
          cb.append("break;\n");
        }
      }

      cb.append("}\n");

      cb.append("}\n");
      cb.append("input.mark(-1);\n");
      cb.append("}\n");
      //cb.append("System.out.println(\"exiting got terminating -1 fieldId\");\n");
      cb.append("return -1;\n");
      cb.append("}\n");
      
      
      cb.append("// deserialize implementation \n");
      //TODO: FIX DESERIALIZE 
      cb.append("public final void deserialize("+
                "DataInput input, BinaryProtocol decoder)\n"+
                "throws java.io.IOException {\n");
      cb.append("// clear existing data first  \n");
      cb.append("clear();\n\n");
      cb.append("// reset protocol object to unknown field id enconding mode (for compatibility)\n");
      cb.append("decoder.pushFieldIdEncodingMode(BinaryProtocol.FIELD_ID_ENCODING_MODE_UNKNOWN);\n");
      cb.append("// keep reading fields until terminator (-1) is located \n");
      cb.append("int fieldId;\n");
      cb.append("while ((fieldId = decoder.readFieldId(input)) != -1) { \n");
      
      cb.append("switch (fieldId) { \n");
      for (Iterator<JField<JavaType>> i = fields.iterator(); i.hasNext();) {
        JField<JavaType> jf = i.next();
        if ((jf.getModifiers() & JField.Modifiers.TRANSIENT) == 0) { 
          String name = jf.getName();
          JavaType type = jf.getType();
          cb.append("case Field_"+name.toUpperCase()+":{\n");
            if (trackDirtyFields()) { 
                cb.append("__validFields.set(Field_"+name.toUpperCase()+");\n");
            }
            type.genReadMethod(cb, name, name, false);
          cb.append("}\n");
          cb.append("break;\n");
        }
      }

      cb.append("}\n");
      cb.append("}\n");
      
      cb.append("// pop extra encoding mode off of stack \n");
      cb.append("decoder.popFieldIdEncodingMode();\n");
      
      cb.append("}\n");      
      cb.append("// clear implementation \n");
      //TODO: ADD CLEAR
  	  cb.append("public final void clear() {\n");
  	  if ((modifiers & Modifiers.HAS_RECORDID) != 0) { 
  	    cb.append("super.clear();\n");
  	  }
  	  if (trackDirtyFields()) { 
  	    cb.append("__validFields.clear();\n");
  	  }
      for (Iterator<JField<JavaType>> i = fields.iterator(); i.hasNext();) {
          JField<JavaType> jf = i.next();
          String name = jf.getName();
          JavaType type = jf.getType();
          int ordinal = jf.getOrdinal();
          type.genClearMethod(cb, name);
      }
      cb.append("}\n");
      
      cb.append("// equals implementation \n");
      //TODO: FIX EQUALS 
      cb.append("public final boolean equals(final Object peer_) {\n");
      cb.append("if (!(peer_ instanceof "+name+")) {\n");
      cb.append("return false;\n");
      cb.append("}\n");
      cb.append("if (peer_ == this) {\n");
      cb.append("return true;\n");
      cb.append("}\n");
      cb.append(name+" peer = ("+name+") peer_;\n");
      if (trackDirtyFields()) { 
        cb.append("boolean ret = __validFields.equals(peer.__validFields);\n");
        cb.append("if (!ret) return ret;\n");
      }
      else {
        cb.append("boolean ret = true;\n");
      }
      for (Iterator<JField<JavaType>> i = fields.iterator(); i.hasNext();) {
        JField<JavaType> jf = i.next();
        if ((jf.getModifiers() & JField.Modifiers.TRANSIENT) == 0) { 
          String name = jf.getName();
          JavaType type = jf.getType();
          if (trackDirtyFields()) {
            if (type.hasDirtyState()) { 
              type.genValidFieldCheck(cb, name);
            }
            else { 
              cb.append("if (__validFields.get(Field_"+name.toUpperCase()+"))");
            }
          }
          cb.append(" {\n");
          
          type.genEquals(cb, name, "peer."+name);
          cb.append("if (!ret) return ret;\n");
          cb.append("}\n");
        }
      }
      cb.append("return ret;\n");
      cb.append("}\n");
      
      

      // toString implementation 
      cb.append("public final String toString() {\n");
      cb.append("StringWriter sw = new StringWriter();\n");
      cb.append("JsonWriter writer = new JsonWriter(sw);\n");
      cb.append("toJSON(writer);\n");
      cb.append("return sw.toString();\n");
      cb.append("}\n");
      cb.append("public final void toJSON(JsonWriter writer) {\n");
      cb.append("try {\n");
      cb.append("writer.beginObject();\n");

      for (Iterator<JField<JavaType>> i = fields.iterator(); i.hasNext();) {
        JField<JavaType> jf = i.next();
        if ((jf.getModifiers() & JField.Modifiers.TRANSIENT) == 0) { 
          String name = jf.getName();
          JavaType type = jf.getType();
          if (trackDirtyFields()) {
            if (type.hasDirtyState()) { 
              type.genValidFieldCheck(cb, name);
            }
            else { 
              cb.append("if (__validFields.get(Field_"+name.toUpperCase()+"))");
            }
          }
          cb.append(" {\n");
            
          type.genJSON(cb, name);
          cb.append("}\n");
        }
      }
      cb.append("writer.endObject();\n");
      cb.append("writer.flush();\n");
      cb.append("}\n");
      cb.append("catch (IOException e){\n");
      cb.append(" throw new RuntimeException(e);\n");
      cb.append("}\n");
      cb.append("}\n");

      
      cb.append("// clone implementation \n");
      cb.append("@SuppressWarnings(\"unchecked\")\n");
      cb.append("public final Object clone() throws CloneNotSupportedException {\n");
      cb.append(name+" other = new "+name+"();\n");
      if (trackDirtyFields()) { 
        cb.append("other.__validFields.or(this.__validFields);\n");
      }
      for (Iterator<JField<JavaType>> i = fields.iterator(); i.hasNext();) {
    	 
        JField<JavaType> jf = i.next();
        String name = jf.getName();
        JavaType type = jf.getType();
        if (trackDirtyFields()) {
          if ((jf.getModifiers() & JField.Modifiers.TRANSIENT) == 0 && !type.hasDirtyState()) { 
            cb.append("if (__validFields.get(Field_"+name.toUpperCase()+"))");
          }
        }
        cb.append("{\n");
        type.genClone(cb,type.getType(), "other."+name,"this."+name);
        cb.append("}\n");
      }
      
      cb.append("return other;\n");
      cb.append("}\n");
      
      cb.append("// merge implementation \n");
      cb.append("@SuppressWarnings(\"unchecked\")\n");
      cb.append("public final void merge(Object peer_) throws CloneNotSupportedException  {\n");
      cb.append(name+" peer = ("+name+") peer_;\n");
      if (trackDirtyFields()) { 
        cb.append("__validFields.or(peer.__validFields);\n");
      }
      for (Iterator<JField<JavaType>> i = fields.iterator(); i.hasNext();) {
    	 
        JField<JavaType> jf = i.next();
        String name = jf.getName();
        JavaType type = jf.getType();
        if (trackDirtyFields()) {
          if ((jf.getModifiers() & JField.Modifiers.TRANSIENT) == 0 && !type.hasDirtyState()) { 
            cb.append("if (peer.__validFields.get(Field_"+name.toUpperCase()+"))");
          }
        }
        cb.append("{\n");
        type.genMerge(cb,type.getType(), "this."+name,"peer."+name);
        cb.append("}\n");
      }
      
      cb.append("}\n");

      
      
      cb.append("// hashCode implementation \n");
      //TODO: FIX HASH CODE 
      cb.append("public final int hashCode() {\n");
      cb.append("int result = 1;\n");
      
      Iterable<JField<JavaType>> fieldIterator = null;
      if (getKeyCount() != 0) { 
      	fieldIterator = getKeys();
      }
      else { 
      	fieldIterator = fields;
      }
      
      for (JField<JavaType> jf : fieldIterator) {
        if ((jf.getModifiers() & JField.Modifiers.TRANSIENT) == 0) { 
          String name = jf.getName();
          JavaType type = jf.getType();
          type.genHashCode(cb, name);
        }
      }
      cb.append("return result;\n");
      cb.append("}\n");
      
      
      
      cb.append("}\n");

      jj.write(cb.toString());
      jj.close();
    }

    @Override
    void genJSON(CodeBuffer cb, String fname) throws IOException {
      cb.append(fname+".toJSON(writer);\n");
    }
    @Override
    void genMerge(CodeBuffer cb, String type, String targetField,String sourceField) {
      	cb.append(targetField + ".merge(" + sourceField + ");\n");
    }
    
  	@Override
  	void genClearMethod(CodeBuffer cb, String fname) {
  		cb.append(fname+".clear();\n");
  	}
  	
    /** does this type have an independent dirty state  - ignores validFields bit**/
    boolean hasDirtyState() { 
      return true;
    }
    
    void genDirtyCheck(CodeBuffer cb,String fieldName) { 
      cb.append("if (!isDirty){\n");
      cb.append("isDirty="+fieldName+".isObjectDirty();\n");
      cb.append("}\n");
    }
    
    void genValidFieldCheck(CodeBuffer cb,String fieldName) { 
      cb.append("if ("+fieldName+".isObjectDirty())");
    }
    
  }
  
  class CppRecord extends CppCompType {
    
    private String fullName;
    private String name;
    private String module;
    private ArrayList<JField<CppType>> fields = 
      new ArrayList<JField<CppType>>();
    
    CppRecord(String name, ArrayList<JField<JType>> flist) {
      super(name.replaceAll("\\.","::"));
      this.fullName = name.replaceAll("\\.", "::");
      int idx = name.lastIndexOf('.');
      this.name = name.substring(idx+1);
      this.module = name.substring(0, idx).replaceAll("\\.", "::");
      for (Iterator<JField<JType>> iter = flist.iterator(); iter.hasNext();) {
        JField<JType> f = iter.next();
        fields.add(new JField<CppType>(f.getName(), f.getType().getCppType(),f.getOrdinal(),f.getModifiers()));
      }
    }
    
    String genDecl(String fname) {
      return "  "+name+" "+fname+";\n";
    }
    
    void genCode(FileWriter hh, FileWriter cc, ArrayList<String> options)
      throws IOException {
      CodeBuffer hb = new CodeBuffer();
      
      String[] ns = module.split("::");
      for (int i = 0; i < ns.length; i++) {
        hb.append("namespace "+ns[i]+" {\n");
      }
      
      hb.append("class "+name+" : public ::hadoop::Record {\n");
      hb.append("private:\n");
      
      for (Iterator<JField<CppType>> i = fields.iterator(); i.hasNext();) {
        JField<CppType> jf = i.next();
        String name = jf.getName();
        CppType type = jf.getType();
        type.genDecl(hb, name);
      }
      hb.append("public:\n");
      hb.append("virtual void serialize(::hadoop::OArchive& a_, const char* tag) const;\n");
      hb.append("virtual void deserialize(::hadoop::IArchive& a_, const char* tag);\n");
      hb.append("virtual const ::std::string& type() const;\n");
      hb.append("virtual const ::std::string& signature() const;\n");
      hb.append("virtual bool operator<(const "+name+"& peer_) const;\n");
      hb.append("virtual bool operator==(const "+name+"& peer_) const;\n");
      hb.append("virtual ~"+name+"() {};\n");
      for (Iterator<JField<CppType>> i = fields.iterator(); i.hasNext();) {
        JField<CppType> jf = i.next();
        String name = jf.getName();
        CppType type = jf.getType();
        type.genGetSet(hb, name);
      }
      hb.append("}; // end record "+name+"\n");
      for (int i=ns.length-1; i>=0; i--) {
        hb.append("} // end namespace "+ns[i]+"\n");
      }
      
      hh.write(hb.toString());
      
      CodeBuffer cb = new CodeBuffer();
      
      cb.append("void "+fullName+"::serialize(::hadoop::OArchive& a_, const char* tag) const {\n");
      cb.append("a_.startRecord(*this,tag);\n");
      for (Iterator<JField<CppType>> i = fields.iterator(); i.hasNext();) {
        JField<CppType> jf = i.next();
        String name = jf.getName();
        CppType type = jf.getType();
        if (type instanceof JBuffer.CppBuffer) {
          cb.append("a_.serialize("+name+","+name+".length(),\""+name+"\");\n");
        } else {
          cb.append("a_.serialize("+name+",\""+name+"\");\n");
        }
      }
      cb.append("a_.endRecord(*this,tag);\n");
      cb.append("return;\n");
      cb.append("}\n");
      
      cb.append("void "+fullName+"::deserialize(::hadoop::IArchive& a_, const char* tag) {\n");
      cb.append("a_.startRecord(*this,tag);\n");
      for (Iterator<JField<CppType>> i = fields.iterator(); i.hasNext();) {
        JField<CppType> jf = i.next();
        String name = jf.getName();
        CppType type = jf.getType();
        if (type instanceof JBuffer.CppBuffer) {
          cb.append("{\nsize_t len=0; a_.deserialize("+name+",len,\""+name+"\");\n}\n");
        } else {
          cb.append("a_.deserialize("+name+",\""+name+"\");\n");
        }
      }
      cb.append("a_.endRecord(*this,tag);\n");
      cb.append("return;\n");
      cb.append("}\n");
      
      
      cb.append("bool "+fullName+"::operator< (const "+fullName+"& peer_) const {\n");
      cb.append("return (1\n");
      for (Iterator<JField<CppType>> i = fields.iterator(); i.hasNext();) {
        JField<CppType> jf = i.next();
        String name = jf.getName();
        cb.append("&& ("+name+" < peer_."+name+")\n");
      }
      cb.append(");\n");
      cb.append("}\n");
      
      cb.append("bool "+fullName+"::operator== (const "+fullName+"& peer_) const {\n");
      cb.append("return (1\n");
      for (Iterator<JField<CppType>> i = fields.iterator(); i.hasNext();) {
        JField<CppType> jf = i.next();
        String name = jf.getName();
        cb.append("&& ("+name+" == peer_."+name+")\n");
      }
      cb.append(");\n");
      cb.append("}\n");
      
      cb.append("const ::std::string&"+fullName+"::type() const {\n");
      cb.append("static const ::std::string type_(\""+name+"\");\n");
      cb.append("return type_;\n");
      cb.append("}\n");
      
      cb.append("const ::std::string&"+fullName+"::signature() const {\n");
      cb.append("static const ::std::string sig_(\""+getSignature()+"\");\n");
      cb.append("return sig_;\n");
      cb.append("}\n");
      
      cc.write(cb.toString());
    }
  }
  
  class CRecord extends CCompType {
    
  }
  
  private String signature;
  
  /**
   * Creates a new instance of JRecord
   */
  public JRecord(String name, ArrayList<JField<JType>> flist,ArrayList<JEnum> enums,Set<String> modifiers,ArrayList<JComparator> comparators) {
    setJavaType(new JavaRecord(name,flist,enums,modifiers));
    setCppType(new CppRecord(name, flist));
    setCType(new CRecord());
    // precompute signature
    int idx = name.lastIndexOf('.');
    String recName = name.substring(idx+1);
    StringBuffer sb = new StringBuffer();
    sb.append("L").append(recName).append("(");
    for (Iterator<JField<JType>> i = flist.iterator(); i.hasNext();) {
      String s = i.next().getType().getSignature();
      sb.append(s);
    }
    sb.append(")");
    signature = sb.toString();
  }
  
  String getSignature() {
    return signature;
  }
  
  void genCppCode(FileWriter hh, FileWriter cc, ArrayList<String> options)
    throws IOException {
    ((CppRecord)getCppType()).genCode(hh, cc, options);
  }
  
  void genJavaCode(String destDir, ArrayList<String> options)
    throws IOException {
    ((JavaRecord)getJavaType()).genCode(destDir, options);
  }
}
