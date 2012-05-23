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

import java.io.IOException;

import com.google.gson.JsonObject;
import com.google.gson.stream.JsonWriter;

/**
 * Abstract Base class for all types supported by Hadoop Record I/O.
 */
abstract public class JType {
  
  static String toCamelCase(String name) {
    char firstChar = name.charAt(0);
    if (Character.isLowerCase(firstChar)) {
      return ""+Character.toUpperCase(firstChar) + name.substring(1);
    }
    return name;
  }
  
  JavaType javaType;
  CppType cppType;
  CType cType;
  
  
  boolean isComparable() { 
	  	return false;
  }
  
  
  abstract class JavaType {
	  
    private String name;
    private String methodSuffix;
    private String wrapper;
    
    JavaType(String javaname,
             String suffix,
             String wrapper) {
      this.name = javaname;
      this.methodSuffix = suffix;
      this.wrapper = wrapper;
    }
    
    boolean isVariableBitRateEncoded() { return false; }
    
    boolean isIntrinsicType() { return true; }
    

    void genDecl(CodeBuffer cb, String fname) {
      cb.append("private "+name+" "+fname+";\n");
    }
    
    void genConstructorParam(CodeBuffer cb, String fname) {
      cb.append("final "+name+" "+fname);
    }
    
    void genJSON(CodeBuffer cb, String fname) throws IOException {
      cb.append("writer.name(\""+fname+"\").value("+fname+");\n");
    }    
    
    void genGetSet(CodeBuffer cb, String fname,boolean trackDirtyFields) {
      cb.append("public "+name+" get"+toCamelCase(fname)+"() {\n");
      cb.append("return "+fname+";\n");
      cb.append("}\n");
      cb.append("public void set"+toCamelCase(fname)+"( "+name+" "+fname+") {\n");
      if (trackDirtyFields) { 
        cb.append("__validFields.set(Field_"+fname.toUpperCase()+");\n");
      }
      cb.append("this."+fname+"="+fname+";\n");
      cb.append("}\n");
    }
    
    void genValidFieldCheck(CodeBuffer cb,String fieldName) { 
      cb.append("if (__validFields.get(Field_"+fieldName.toUpperCase()+"))");
    }
    
    /** does this type have an idependent dirty state  - ignores validFields bit**/
    boolean hasDirtyState() { 
      return false;
    }
    
    void genDirtyCheck(CodeBuffer cb,String fieldName) { 
      // NOOP IN BASE ...
    }
    
    String getType() {
      return name;
    }
    
    String getWrapperType() {
      return wrapper;
    }
    
    String getMethodSuffix() {
      return methodSuffix;
    }
    
    void genClearMethod(CodeBuffer cb, String fname) { 
    	((Integer)1).equals((Integer)2);
    }
    
    void genWriteMethod(CodeBuffer cb, String fname, String tag) {
      if (!isVariableBitRateEncoded())
        cb.append("encoder.write"+methodSuffix+"(output,"+fname+");\n");
      else 
        cb.append("encoder.writeV"+methodSuffix+"(output,"+fname+");\n");
    }
    
    void genReadMethod(CodeBuffer cb, String fname, String tag, boolean decl) {
      if (decl) {
        cb.append(name+" "+fname+";\n");
      }
      if (!isVariableBitRateEncoded())
        cb.append(fname+"=decoder.read"+methodSuffix+"(input);\n");
      else 
        cb.append(fname+"=decoder.readV"+methodSuffix+"(input);\n");
    }
    void genSkipMethod(CodeBuffer cb) {
      if (!isVariableBitRateEncoded())
        cb.append("decoder.skip"+methodSuffix+"(input);\n");
      else 
        cb.append("decoder.skipV"+methodSuffix+"(input);\n");
    }
    
   
    void genEquals(CodeBuffer cb, String fname, String peer) {
      cb.append("ret = ("+fname+"=="+peer+");\n");
    }
    
    String genCompareTo(String fname,String peer,String objectType) { 
    	return "(("+getWrapperType()+")"+fname+").compareTo(("+getWrapperType()+")(("+objectType+")"+peer+")."+fname+")";
    }
    
    String genGetKey(String fname) { 
    	return "(("+getWrapperType()+")"+fname+").toString()";
    }
    
    void genHashCode(CodeBuffer cb, String fname) {
      cb.append("result = MurmurHash.hashInt((int)"+fname+",result);\n");
    }
    
    void genConstructorSet(CodeBuffer cb, String fname) {
      cb.append("this."+fname+" = "+fname+";\n");
    }
    
    void genClone(CodeBuffer cb,String type,String targetField, String sourceField) {
      cb.append(targetField+ "= "+sourceField+";\n");
    }
    
    void genMerge(CodeBuffer cb,String type,String targetField, String sourceField) {
    	// default to clone implementation
    	this.genClone(cb, type, targetField, sourceField);
    }
    
    boolean isComparable() { 
    	return JType.this.isComparable();
    }
  }
  
  class CppType {
    private String name;
    private boolean variableEncoding =false;
    
    CppType(String cppname) {
      //TODO: FIX THIS !!! NEED TO REACT TO VARIABLE ENCODING FLAG
      name = cppname;
    }
    
    void setVariableEncoding(boolean variableEncoding) { 
      this.variableEncoding = variableEncoding;
    }
    
    void genDecl(CodeBuffer cb, String fname) {
      cb.append(name+" "+fname+";\n");
    }
    
    void genGetSet(CodeBuffer cb, String fname) {
      cb.append("virtual "+name+" get"+toCamelCase(fname)+"() const {\n");
      cb.append("return "+fname+";\n");
      cb.append("}\n");
      cb.append("virtual void set"+toCamelCase(fname)+"("+name+" m_) {\n");
      cb.append(fname+"=m_;\n");
      cb.append("}\n");
    }
    
    String getType() {
      return name;
    }
  }
  
  class CType {
    
  }
  
  abstract String getSignature();
  
  void setJavaType(JavaType jType) {
    this.javaType = jType;
  }
  
  JavaType getJavaType() {
    return javaType;
  }
  
  void setCppType(CppType cppType) {
    this.cppType = cppType;
  }
  
  CppType getCppType() {
    return cppType;
  }
  
  void setCType(CType cType) {
    this.cType = cType;
  }
  
  CType getCType() {
    return cType;
  }
}
