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

/**
 * 
 * @author rana
 *
 */
public class GoogleURL {
  
  static public final String emptyString = "";
  static {
      // Ensure native JNI library is loaded
      System.loadLibrary("GoogleURL_jni");
      internal_init(GoogleURLComponent.class);
  }
  
  private native void initializeFromURL(String urlStirng);
  @SuppressWarnings("unchecked")
  private native static void internal_init(Class componentClass);
  
  public GoogleURL(String urlString) { 
    initializeFromURL(urlString);
  }
  
  // Identifies different components.
  enum ComponentType {
    SCHEME,
    USERNAME,
    PASSWORD,
    HOST,
    PORT,
    PATH,
    QUERY,
    REF,
  }

  public boolean isValid() { 
    return _isValid; 
  }
  
  public boolean has_scheme() {
    return _scheme.len >= 0;
  }
  
  public boolean has_username() {
    return _userName.len >= 0;
  }
  
  public boolean has_password() {
    return _password.len >= 0;
  }
  
  public boolean has_host() {
    // Note that hosts are special, absense of host means length 0.
    return _host.len > 0;
  }
  
  public boolean has_port() {
    return _port.len >= 0;
  }
  
  public boolean has_path() {
    // Note that http://www.google.com/" has a path, the path is "/". This can
    // return false only for invalid or nonstandard URLs.
    return _path.len >= 0;
  }
  
  public boolean has_query() {
    return _query.len >= 0;
  }
  
  public boolean has_ref() {
    return _ref.len >= 0;
  }
  
  
  // Getters for various components of the URL. The returned string will be
  // empty if the component is empty or is not present.
  public String getScheme(){  // Not including the colon. See also SchemeIs.
    return getComponentString(_scheme);
  }
  public String getUserName(){
    return getComponentString(_userName);
  }
  public String getPassword(){
    return getComponentString(_password);
  }
  public String getHost(){
    return getComponentString(_host);
  }
  public GoogleURLComponent getHostComponent() { 
    return _host;
  }
  
  public String getPort(){  // Returns -1 if "default"
    return getComponentString(_port);
  }
  public String getPath(){  // Including first slash following host
    return getComponentString(_path);
  }
  public String getQuery(){  // Stuff following '?'
    return getComponentString(_query);
  }
  
  public String getPathAndQuery() { 
    if (_canonicalURL != null) { 
      int startIndex = (_path.len > 0) ? _path.begin 
            : (_query.len >0) ? _query.begin -1 : -1;
      if (startIndex != -1) { 
        int len = (_path.len >0 ) ? _path.len : 0;
        len += (_query.len >0) ? (_query.len + 1) : 0;
        if (len != 0) { 
          return _canonicalURL.substring(startIndex,startIndex + len);
        }
      }
    }
    return emptyString;
  }
  
  
  public String getRef(){  // Stuff following '#'
    return getComponentString(_ref);
  }

  public GoogleURLComponent getRefComponent(){  // Stuff following '#'
    return _ref;
  }
  
  public String getCanonicalURL() { 
    return _canonicalURL;
  } 
  
  public void dump() { 
    System.out.println("Scheme:" + getScheme());
    System.out.println("UserName:" + getUserName());
    System.out.println("Password:" + getPassword());
    System.out.println("Host:" + getHost());
    System.out.println("Port:" + getPort());
    System.out.println("Path:" + getPath());
    System.out.println("Query:" + getQuery());
    System.out.println("Ref:" + getRef());                        
  }
  
  public   String getComponentString(GoogleURLComponent comp) {
    if (_canonicalURL == null || comp.len <= 0)
      return emptyString;
    return _canonicalURL.substring(comp.begin,comp.begin + comp.len);
  }

  
  private boolean _isValid = false;
  public GoogleURLComponent _scheme    = new GoogleURLComponent();
  public GoogleURLComponent _userName  = new GoogleURLComponent();
  public GoogleURLComponent _password  = new GoogleURLComponent();
  public GoogleURLComponent _host      = new GoogleURLComponent();
  public GoogleURLComponent  _port     =  new GoogleURLComponent();
  public GoogleURLComponent _path      = new GoogleURLComponent();
  public GoogleURLComponent _query     = new GoogleURLComponent();
  public GoogleURLComponent _ref       = new GoogleURLComponent();
  public String  _canonicalURL = null;

}
