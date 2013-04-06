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

import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.TimeZone;
import java.util.Vector;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.service.crawler.CrawlHostImpl;
import org.commoncrawl.io.HttpCookie;
import org.commoncrawl.io.NIOHttpCookieStore;
import org.junit.Test;

/**
 * 
 * @author rana
 *
 */
public class HttpCookieUtils {

  /** logging **/
  private static final Log LOG = LogFactory.getLog(HttpCookieUtils.class);  
  
  public static class ParsedCookie { 

    public static class TokenValuePair { 
      public String first;
      public String second;
    }
    
    //typedef std::pair<String, String> TokenValuePair;
    // typedef std::vector<TokenValuePair> PairList;

    // The maximum length of a cookie string we will try to parse
    public static final int kMaxCookieSize = 4096;
    // The maximum number of Token/Value pairs.  Shouldn't have more than 8.
    public static final int kMaxPairs = 16;

    // ruct from a cookie string like "BLAH=1; path=/; domain=.google.com"
    public ParsedCookie(String cookie_line) {
      if (cookie_line.length() <= kMaxCookieSize) {  
        
        ParseTokenValuePairs(cookie_line);
        if (pairs_.size() != 0) { 
          is_valid_ = true;
          SetupAttributes();
        }
      }
    }

    // You should not call any other methods on the class if !IsValid
    boolean IsValid() { return is_valid_; }

    String Name() { return pairs_.get(0).first; }
    String Token()  { return Name(); }
    String Value()  { return pairs_.get(0).second; }

    boolean HasPath()  { return path_index_ != 0; }
    String Path()  { return pairs_.get(path_index_).second; }
    boolean HasDomain()  { return domain_index_ != 0; }
    String Domain()  { return pairs_.get(domain_index_).second; }
    boolean HasExpires()  { return expires_index_ != 0; }
    String Expires()  { return pairs_.get(expires_index_).second; }
    boolean HasMaxAge()  { return maxage_index_ != 0; }
    String MaxAge()  { return pairs_.get(maxage_index_).second; }
    boolean IsSecure()  { return secure_index_ != 0; }
    boolean IsHttpOnly()  { return httponly_index_ != 0; }

    int NumberOfAttributes()  { return pairs_.size() - 1; }


     // Returns true if |c| occurs in |chars|
     // TODO maybe make this take an iterator, could check for end also?
     static boolean CharIsA(char c, String chars) {
       return chars.indexOf(c) != -1;
     }
     // Seek the iterator to the first occurrence of a character in |chars|.
     // Returns true if it hit the end, false otherwise.
     static int SeekTo(String source,int beginIndex,int endIndex,String chars) {
       for (; beginIndex < endIndex && !CharIsA(source.charAt(beginIndex), chars); ++beginIndex);
       return beginIndex;
     }
     // Seek the iterator to the first occurrence of a character not in |chars|.
     // Returns true if it hit the end, false otherwise.
     static int SeekPast(String source,int beginIndex,int endIndex,String chars) {
       for (; beginIndex < endIndex && CharIsA(source.charAt(beginIndex), chars); ++beginIndex);
       return beginIndex;
     }
     
     static int SeekBackPast(String source,int beginIndex,int endIndex,String chars) {
       for (; beginIndex != endIndex && CharIsA(source.charAt(beginIndex), chars); --beginIndex);
       return beginIndex;
     }

     static final String kTerminator      = "\n\r\0";
     static final String kWhitespace      = " \t";
     static final String kValueSeparator  = ";";
     static final String kTokenSeparator  = ";=";
     
     
     /** find the first occurrence of ANY of the tokens in patterns in the target string **/
     static int firstIndexOf(String strToSearch,int fromIndex,String tokens) { 
       int max = strToSearch.length();
       char v[] = tokens.toCharArray();

       if (fromIndex < 0) {
           fromIndex = 0;
       } else if (fromIndex >= strToSearch.length()) {
           // Note: fromIndex might be near -1>>>1.
           return -1;
       }

       int i = fromIndex;
       // handle most cases here (ch is a BMP code point or a
       // negative value (invalid code point))
       for (; i < strToSearch.length() ; i++) {
         char ch = strToSearch.charAt(i);
         for (int j=0;j<v.length;++j) { 
           if (v[j] == ch)
             return i;
         }
       }
       return -1;
     }
     
  // Parse all token/value pairs and populate pairs_.
     void ParseTokenValuePairs(String cookie_line) {

       pairs_.clear();

       // Ok, here we go.  We should be expecting to be starting somewhere
       // before the cookie line, not including any header name...
       int start = 0;
       int end   = cookie_line.length();
       int it    = start;

       // TODO Make sure we're stripping \r\n in the network code.  Then we
       // can log any unexpected terminators.
       int term_pos =  firstIndexOf(cookie_line,0,kTerminator);
       if (term_pos != -1) {
         // We found a character we should treat as an end of string.
         end = start + term_pos;
       }

       for (int pair_num = 0; pair_num < kMaxPairs && it != end; ++pair_num) {
         TokenValuePair pair = new TokenValuePair();
         
         int token_start, token_real_end, token_end;

         // Seek past any whitespace before the "token" (the name).
         // token_start should point at the first character in the token
         if ((it = SeekPast(cookie_line,it, end, kWhitespace)) == end) 
           break;  // No token, whitespace or empty.
         token_start = it;

         // Seek over the token, to the token separator.
         // token_real_end should point at the token separator, i.e. '='.
         // If it == end after the seek, we probably have a token-value.
         it = SeekTo(cookie_line,it, end, kTokenSeparator);
         token_real_end = it;

         // Ignore any whitespace between the token and the token separator.
         // token_end should point after the last interesting token character,
         // pointing at either whitespace, or at '=' (and equal to token_real_end).
         if (it != token_start) {  // We could have an empty token name.
           --it;  // Go back before the token separator.
           // Skip over any whitespace to the first non-whitespace character.
           it = SeekBackPast(cookie_line,it, token_start, kWhitespace);
           // Point after it.
           ++it;
         }
         token_end = it;

         // Seek us back to the end of the token.
         it = token_real_end;

         if (it == end || cookie_line.charAt(it) != '=') {
           // We have a token-value, we didn't have any token name.
           if (pair_num == 0) {
             // For the first time around, we want to treat single values
             // as a value with an empty name. (Mozilla bug 169091).
             // IE seems to also have this behavior, ex "AAA", and "AAA=10" will
             // set 2 different cookies, and setting "BBB" will then replace "AAA".
             pair.first = "";
             // Rewind to the beginning of what we thought was the token name,
             // and let it get parsed as a value.
             it = token_start;
           } else {
             // Any not-first attribute we want to treat a value as a
             // name with an empty value...  This is so something like
             // "secure;" will get parsed as a Token name, and not a value.
             pair.first = cookie_line.substring(token_start, token_end);
           }
         } else {
           // We have a TOKEN=VALUE.
           pair.first = cookie_line.substring(token_start, token_end);
           ++it;  // Skip past the '='.
         }

         // OK, now try to parse a value.
         int value_start, value_end;

         // Seek past any whitespace that might in-between the token and value.
         it = SeekPast(cookie_line,it, end, kWhitespace);
         
         // value_start should point at the first character of the value.
         value_start = it;

         // It is unclear exactly how quoted string values should be handled.
         // Major browsers do different things, for example, Firefox supports
         // semicolons embedded in a quoted value, while IE does not.  Looking at
         // the specs, RFC 2109 and 2965 allow for a quoted-string as the value.
         // However, these specs were apparently written after browsers had
         // implemented cookies, and they seem very distant from the reality of
         // what is actually implemented and used on the web.  The original spec
         // from Netscape is possibly what is closest to the cookies used today.
         // This spec didn't have explicit support for double quoted strings, and
         // states that ; is not allowed as part of a value.  We had originally
         // implement the Firefox behavior (A="B;C"; -> A="B;C";).  However, since
         // there is no standard that makes sense, we decided to follow the behavior
         // of IE and Safari, which is closer to the original Netscape proposal.
         // This means that A="B;C" -> A="B;.  This also makes the code much simpler
         // and reduces the possibility for invalid cookies, where other browsers
         // like Opera currently reject those invalid cookies (ex A="B" "C";).

         // Just look for ';' to terminate ('=' allowed).
         // We can hit the end, maybe they didn't terminate.
         it = SeekTo(cookie_line,it, end, kValueSeparator);

         // Will be pointed at the ; seperator or the end.
         value_end = it;

         // Ignore any unwanted whitespace after the value.
         if (value_end != value_start) {  // Could have an empty value
           --value_end;
           value_end = SeekBackPast(cookie_line,value_end, value_start, kWhitespace);
           ++value_end;
         }

         // OK, we're finished with a Token/Value.
         pair.second = cookie_line.substring(value_start, value_end);
         // From RFC2109: "Attributes (names) (attr) are case-insensitive."
         if (pair_num != 0)
           pair.first = pair.first.toLowerCase();
         pairs_.add(pair);

         // We've processed a token/value pair, we're either at the end of
         // the string or a ValueSeparator like ';', which we want to skip.
         if (it != end)
           ++it;
       }
     }

     static final String kPathTokenName      = "path";
     static final String kDomainTokenName    = "domain";
     static final String kExpiresTokenName   = "expires";
     static final String kMaxAgeTokenName    = "max-age";
     static final String kSecureTokenName    = "secure";
     static final String kHttpOnlyTokenName  = "httponly";
     
     void SetupAttributes() {

       // We skip over the first token/value, the user supplied one.
       for (int i = 1; i < pairs_.size(); ++i) {
         if (pairs_.get(i).first.equals(kPathTokenName))
           path_index_ = i;
         else if (pairs_.get(i).first.equals(kDomainTokenName))
           domain_index_ = i;
         else if (pairs_.get(i).first.equals(kExpiresTokenName))
           expires_index_ = i;
         else if (pairs_.get(i).first.equals(kMaxAgeTokenName))
           maxage_index_ = i;
         else if (pairs_.get(i).first.equals(kSecureTokenName))
           secure_index_ = i;
         else if (pairs_.get(i).first.equals(kHttpOnlyTokenName))
           httponly_index_ = i;
         else { /* some attribute we don't know or don't care about. */ }
       }
     }
    Vector<TokenValuePair> pairs_ = new Vector<TokenValuePair>();
    
    boolean is_valid_ = false;
    // These will default to 0, but that should never be valid since the
    // 0th index is the user supplied token/value, not an attribute.
    // We're really never going to have more than like 8 attributes, so we
    // could fit these into 3 bits each if we're worried about size...
    int  path_index_ =0;
    int  domain_index_=0;
    int expires_index_=0;
    int maxage_index_=0;
    int secure_index_=0;
    int httponly_index_=0;
    
    
    static public class ParsedCookieTests { 

      @Test
      public void parsedCookieTests() throws Exception {
        TestBasic();
        TestQuoted();
        TestNameless();
        TestAttributeCase();
        TestDoubleQuotedNameless();
        QuoteOffTheEnd();
        MissingName();
        MissingValue();
        Whitespace();
        MultipleEquals();
        QuotedTrailingWhitespace();
        TrailingWhitespace();
        TooManyPairs();
        InvalidWhitespace();
        InvalidTooLong();
        InvalidEmpty();
        EmbeddedTerminator();
      }
      
      void TestBasic() {
        ParsedCookie pc = new ParsedCookie("a=b");
        Assert.assertTrue(pc.IsValid());
        Assert.assertFalse(pc.IsSecure());
        Assert.assertEquals("a", pc.Name());
        Assert.assertEquals("b", pc.Value());
      }    
      
      void TestQuoted() { 

          // These are some quoting cases which the major browsers all
          // handle differently.  I've tested Internet Explorer 6, Opera 9.6,
          // Firefox 3, and Safari Windows 3.2.1.  We originally tried to match
          // Firefox closely, however we now match Internet Explorer and Safari.
          String values[] = {
            // Trailing whitespace after a quoted value.  The whitespace after
            // the quote is stripped in all browsers.
            "\"zzz \"  ",              "\"zzz \"",
            // Handling a quoted value with a ';', like FOO="zz;pp"  ;
            // IE and Safari: "zz;
            // Firefox and Opera: "zz;pp"
            "\"zz;pp\" ;",             "\"zz",
            // Handling a value with multiple quoted parts, like FOO="zzz "   "ppp" ;
            // IE and Safari: "zzz "   "ppp";
            // Firefox: "zzz ";
            // Opera: <rejects cookie>
            "\"zzz \"   \"ppp\" ",     "\"zzz \"   \"ppp\"",
            // A quote in a value that didn't start quoted.  like FOO=A"B ;
            // IE, Safari, and Firefox: A"B;
            // Opera: <rejects cookie>
            "A\"B",                    "A\"B",
          };

          for (int i = 0; i < values.length; i += 2) {
            String input = values[i];
            String expected = values[i + 1];

            ParsedCookie pc = new ParsedCookie("aBc=" + input + " ; path=\"/\"  ; httponly ");
            Assert.assertTrue(pc.IsValid());
            Assert.assertFalse(pc.IsSecure());
            Assert.assertTrue(pc.IsHttpOnly());
            Assert.assertTrue(pc.HasPath());
            Assert.assertEquals("aBc", pc.Name());
            Assert.assertEquals(expected, pc.Value());

            // If a path was quoted, the path attribute keeps the quotes.  This will
            // make the cookie effectively useless, but path parameters aren't supposed
            // to be quoted.  Bug 1261605.
            Assert.assertEquals("\"/\"", pc.Path());
          }
        }

        void TestNameless() {
          ParsedCookie pc = new ParsedCookie("BLAHHH; path=/; secure;");
          Assert.assertTrue(pc.IsValid());
          Assert.assertTrue(pc.IsSecure());
          Assert.assertTrue(pc.HasPath());
          Assert.assertEquals("/", pc.Path());
          Assert.assertEquals("", pc.Name());
          Assert.assertEquals("BLAHHH", pc.Value());
        }
  
        void TestAttributeCase() {
          ParsedCookie pc = new ParsedCookie("BLAHHH; Path=/; sECuRe; httpONLY");
          Assert.assertTrue(pc.IsValid());
          Assert.assertTrue(pc.IsSecure());
          Assert.assertTrue(pc.IsHttpOnly());
          Assert.assertTrue(pc.HasPath());
          Assert.assertEquals("/", pc.Path());
          Assert.assertEquals("", pc.Name());
          Assert.assertEquals("BLAHHH", pc.Value());
          Assert.assertEquals(3, pc.NumberOfAttributes());
        }
  
        void TestDoubleQuotedNameless() {
          ParsedCookie pc = new ParsedCookie("\"BLA\\\"HHH\"; path=/; secure;");
          Assert.assertTrue(pc.IsValid());
          Assert.assertTrue(pc.IsSecure());
          Assert.assertTrue(pc.HasPath());
          Assert.assertEquals("/", pc.Path());
          Assert.assertEquals("", pc.Name());
          Assert.assertEquals("\"BLA\\\"HHH\"", pc.Value());
          Assert.assertEquals(2, pc.NumberOfAttributes());
        }
  
        void QuoteOffTheEnd() {
          ParsedCookie pc = new ParsedCookie("a=\"B");
          Assert.assertTrue(pc.IsValid());
          Assert.assertEquals("a", pc.Name());
          Assert.assertEquals("\"B", pc.Value());
          Assert.assertEquals(0, pc.NumberOfAttributes());
        }
  
        void MissingName() {
          ParsedCookie pc = new ParsedCookie("=ABC");
          Assert.assertTrue(pc.IsValid());
          Assert.assertEquals("", pc.Name());
          Assert.assertEquals("ABC", pc.Value());
          Assert.assertEquals(0, pc.NumberOfAttributes());
        }
  
        void MissingValue() {
          ParsedCookie pc = new ParsedCookie("ABC=;  path = /wee");
          Assert.assertTrue(pc.IsValid());
          Assert.assertEquals("ABC", pc.Name());
          Assert.assertEquals("", pc.Value());
          Assert.assertTrue(pc.HasPath());
          Assert.assertEquals("/wee", pc.Path());
          Assert.assertEquals(1, pc.NumberOfAttributes());
        }
  
        void Whitespace() {
          ParsedCookie pc = new ParsedCookie("  A  = BC  ;secure;;;   httponly");
          Assert.assertTrue(pc.IsValid());
          Assert.assertEquals("A", pc.Name());
          Assert.assertEquals("BC", pc.Value());
          Assert.assertFalse(pc.HasPath());
          Assert.assertFalse(pc.HasDomain());
          Assert.assertTrue(pc.IsSecure());
          Assert.assertTrue(pc.IsHttpOnly());
          // We parse anything between ; as attributes, so we end up with two
          // attributes with an empty string name and value.
          Assert.assertEquals(4, pc.NumberOfAttributes());
        }
        void MultipleEquals() {
          ParsedCookie pc = new ParsedCookie("  A=== BC  ;secure;;;   httponly");
          Assert.assertTrue(pc.IsValid());
          Assert.assertEquals("A", pc.Name());
          Assert.assertEquals("== BC", pc.Value());
          Assert.assertFalse(pc.HasPath());
          Assert.assertFalse(pc.HasDomain());
          Assert.assertTrue(pc.IsSecure());
          Assert.assertTrue(pc.IsHttpOnly());
          Assert.assertEquals(4, pc.NumberOfAttributes());
        }
  
        void QuotedTrailingWhitespace() {
          ParsedCookie pc = new ParsedCookie("ANCUUID=\"zohNumRKgI0oxyhSsV3Z7D\"  ; "+
                                              "expires=Sun, 18-Apr-2027 21:06:29 GMT ; "+
                                              "path=/  ;  ");
          Assert.assertTrue(pc.IsValid());
          Assert.assertEquals("ANCUUID", pc.Name());
          // Stripping whitespace after the quotes matches all other major browsers.
          Assert.assertEquals("\"zohNumRKgI0oxyhSsV3Z7D\"", pc.Value());
          Assert.assertTrue(pc.HasExpires());
          Assert.assertTrue(pc.HasPath());
          Assert.assertEquals("/", pc.Path());
          Assert.assertEquals(2, pc.NumberOfAttributes());
        }
  
        void TrailingWhitespace() {
          ParsedCookie pc = new ParsedCookie("ANCUUID=zohNumRKgI0oxyhSsV3Z7D  ; "+
                                              "expires=Sun, 18-Apr-2027 21:06:29 GMT ; "+
                                              "path=/  ;  ");
          Assert.assertTrue(pc.IsValid());
          Assert.assertEquals("ANCUUID", pc.Name());
          Assert.assertEquals("zohNumRKgI0oxyhSsV3Z7D", pc.Value());
          Assert.assertTrue(pc.HasExpires());
          Assert.assertTrue(pc.HasPath());
          Assert.assertEquals("/", pc.Path());
          Assert.assertEquals(2, pc.NumberOfAttributes());
        }
  
        void TooManyPairs() {
          StringBuffer blankpairs = new StringBuffer();
          blankpairs.setLength(ParsedCookie.kMaxPairs - 1);
          for (int i=0;i<blankpairs.length();++i)
            blankpairs.setCharAt(i, ';');
  
          ParsedCookie pc1 = new ParsedCookie(blankpairs.toString() + "secure");
          Assert.assertTrue(pc1.IsValid());
          Assert.assertTrue(pc1.IsSecure());
  
          ParsedCookie pc2 = new ParsedCookie(blankpairs + ";secure");
          Assert.assertTrue(pc2.IsValid());
          Assert.assertFalse(pc2.IsSecure());
        }
  
        // TODO some better test cases for invalid cookies.
        void InvalidWhitespace() {
          ParsedCookie pc = new ParsedCookie("    ");
          Assert.assertFalse(pc.IsValid());
        }
  
        void InvalidTooLong() {
          StringBuffer maxstr = new StringBuffer();
          maxstr.setLength(ParsedCookie.kMaxCookieSize);
          for (int i=0;i<maxstr.length();++i)
            maxstr.setCharAt(i, 'a');
          
          ParsedCookie pc1 = new ParsedCookie(maxstr.toString());
          Assert.assertTrue(pc1.IsValid());
  
          ParsedCookie pc2 = new ParsedCookie(maxstr + "A");
          Assert.assertFalse(pc2.IsValid());
        }
  
        void InvalidEmpty() {
          ParsedCookie pc = new ParsedCookie("");
          Assert.assertFalse(pc.IsValid());
        }
  
        void EmbeddedTerminator() {
          // ParsedCookie pc1 = new ParsedCookie("AAA=BB\0ZYX");
          ParsedCookie pc2 = new ParsedCookie("AAA=BB\rZYX");
          ParsedCookie pc3 = new ParsedCookie("AAA=BB\nZYX");
//          Assert.assertTrue(pc1.IsValid());
//          Assert.assertEquals("AAA", pc1.Name());
//          Assert.assertEquals("BB", pc1.Value());
          Assert.assertTrue(pc2.IsValid());
          Assert.assertEquals("AAA", pc2.Name());
          Assert.assertEquals("BB", pc2.Value());
          Assert.assertTrue(pc3.IsValid());
          Assert.assertEquals("AAA", pc3.Name());
          Assert.assertEquals("BB", pc3.Value());
        }         
      }
    
    
  }
  

  /*
    @Override
    public int compareTo(RootDomainCookie o) {
      int result = _rootDomainHash - o._rootDomainHash;
      if (result == 0) { 
        return (_cookieId < o._cookieId) ? -1 : 1;
      }
      return result;
    }

   */
  

  public static class CanonicalCookie implements HttpCookie {

    
    /** 
     * the public constructor 
     * 
     * @param rootDomainName
     * @param fqDomainName
     * @param name
     * @param value
     * @param path
     * @param secure
     * @param httponly
     * @param creation
     * @param last_access
     * @param has_expires
     * @param expires
     */
    public CanonicalCookie(String rootDomainName,String fqDomainName,String name,
        String value,
        String path,
        boolean secure,
        boolean httponly,
        long creation,
        long last_access,
        boolean has_expires,
        long expires) { 
      
      _rootDomainHash = rootDomainName.hashCode();
      domain_ = fqDomainName;
      name_ = name;
      value_ = value;
      path_=path;
      creation_date_=creation;
      last_access_date_=last_access;
      expiry_date_=expires;
      has_expires_=has_expires;
      secure_=secure;
      httponly_=httponly;
    }
    
    /** 
     * internal specialized constructor 
     */
    public CanonicalCookie(int rootDomainHash) {
      _rootDomainHash = rootDomainHash;
    }
    
    @Override
    public boolean equals(Object obj) {
      if (obj instanceof CanonicalCookie) { 
          CanonicalCookie other = (CanonicalCookie)obj;
          if (_rootDomainHash == other._rootDomainHash) { 
            if (domain_.equalsIgnoreCase(other.domain_)) {
              return  IsEquivalent(other);
            }
          }
      }
      return false;
    }
    
    @Override
    public int hashCode() {
      return _rootDomainHash;
    };
    
    public int       getRootDomainHash() { return _rootDomainHash; }
    
    /** cookie attributes **/
    public String Domain() { return domain_; }
    public String Name() { return name_; }
    public String Value() { return value_; }
    public String Path() { return path_; }
    public long CreationDate() { return creation_date_; }
    public long LastAccessDate() { return last_access_date_; }
    public boolean DoesExpire() { return has_expires_; }
    public boolean IsPersistent() { return DoesExpire(); }
    public long ExpiryDate() { return expiry_date_; }
    public boolean IsSecure() { return secure_; }
    public boolean IsHttpOnly() { return httponly_; }

    boolean IsExpired(long current) {
      return has_expires_ && current >= expiry_date_;
    }

    // Are the cookies considered equivalent in the eyes of the RFC.
    // This says that the domain and path should string match identically.
    boolean IsEquivalent(CanonicalCookie ecc) {
      // It seems like it would make sense to take secure and httponly into
      // account, but the RFC doesn't specify this.
      return name_.equals(ecc.Name()) && path_.equals(ecc.Path());
    }

    void SetLastAccessDate(long date) {
      last_access_date_ = date;
    }

    boolean IsOnPath(String url_path) { 

        if (url_path.length() == 0)
          url_path = "/";
        // A zero length would be unsafe for our trailing '/' checks, and
        // would also make no sense for our prefix match.  The code that
        // creates a CanonicalCookie should make sure the path is never zero length,
        // but we double check anyway.
        if (path_.length() == 0)
          return false;

        // The Mozilla code broke it into 3 cases, if it's strings lengths
        // are less than, equal, or greater.  I think this is simpler:

        // Make sure the cookie path is a prefix of the url path.  If the
        // url path is shorter than the cookie path, then the cookie path
        // can't be a prefix.
        if (url_path.indexOf(path_) != 0)
          return false;

        // Now we know that url_path is >= cookie_path, and that cookie_path
        // is a prefix of url_path.  If they are the are the same length then
        // they are identical, otherwise we need an additional check:

        // In order to avoid in correctly matching a cookie path of /blah
        // with a request path of '/blahblah/', we need to make sure that either
        // the cookie path ends in a trailing '/', or that we prefix up to a '/'
        // in the url path.  Since we know that the url path length is greater
        // than the cookie path length, it's safe to index one byte past.
        if (path_.length() != url_path.length() &&
            path_.charAt(path_.length() - 1) != '/' &&
            url_path.charAt(path_.length()) != '/')
          return false;

        return true;
    }
    
    private static final ThreadLocal<SimpleDateFormat> dateFormat = new ThreadLocal<SimpleDateFormat>() { 
      protected SimpleDateFormat initialValue() {
        return new SimpleDateFormat("yyyy.MM.dd-HH:mm:ss.SSS ZZZ");
      };
    };
    
    @Override
    public String toString() {
      
       
      
      return 
        "domain:" + domain_ + " name:" + name_ + " path:" + path_ + "\n" 
      + "created:" + dateFormat.get().format(new Date(creation_date_))
      + " last accessed:" + dateFormat.get().format(new Date(last_access_date_))
      + " expires:" + dateFormat.get().format(new Date(expiry_date_)) +"\n" 
      + "value:" + value_ + "\n"; 
    }
    
    
    int       _rootDomainHash;
    String    domain_;
    String    name_;
    String    value_;
    String    path_;
    long      creation_date_;
    long      last_access_date_;
    long      expiry_date_;
    boolean   has_expires_;
    boolean   secure_;
    boolean   httponly_;
  }
  
    
  public static class CookieStore implements NIOHttpCookieStore { 
    
    Vector<CanonicalCookie> _cookies = new Vector<CanonicalCookie>();
    
    private static int kNumCookiesPerHost      = 70;  
    private static int kNumCookiesPerHostPurge = 20;
    
    
    public synchronized void GetAllCookies(Vector<CanonicalCookie> cookiesOut) { 
      cookiesOut.addAll(_cookies);
    }
    
    public synchronized String GetCookies(URL url) { 
      
      String cookiesOut = "";
      
      Vector<CanonicalCookie> cookieVector = findCookiesForURL(url);
      
      if (cookieVector != null) { 
        Collections.sort(cookieVector,new Comparator<CanonicalCookie>() {

         // Mozilla sorts on the path length (longest first), and then it
         // sorts by creation time (oldest first).
         // The RFC says the sort order for the domain attribute is undefined.

          @Override
          public int compare(CanonicalCookie cc1, CanonicalCookie cc2) {
            if (cc1.Path().length() == cc2.Path().length()) {
              return cc1.CreationDate() < cc2.CreationDate() ? -1: cc1.CreationDate() > cc2.CreationDate() ? 1 : 0;  
            }
            return (cc1.Path().length() > cc2.Path().length()) ? -1 : cc2.Path().length() > cc1.Path().length() ? 1: 0; 
          } 
          
        });
      
        for (int i=0;i<cookieVector.size();++i) { 
          if (i != 0)
            cookiesOut += "; ";
          
          CanonicalCookie cookie = cookieVector.elementAt(i);
          // In Mozilla if you set a cookie like AAAA, it will have an empty token
          // and a value of AAAA.  When it sends the cookie back, it will send AAAA,
          // so we need to avoid sending =AAAA for a blank token value.
          if (cookie.Name().length() != 0)
            cookiesOut += cookie.Name() + "=";
          cookiesOut += cookie.Value();
        }
      }
      return cookiesOut;
    }
    
    private static class CookieComparator implements Comparator<CanonicalCookie> {

      @Override
      public int compare(CanonicalCookie o1, CanonicalCookie o2) {
        int result = o1._rootDomainHash - o2._rootDomainHash;
        if (result == 0) {
          if (o1.domain_ == null && o2.domain_ != null)
            return -1;
          else if (o1.domain_ != null && o2.domain_ == null)
            return 1;
          else if (o1.domain_ == null && o2.domain_ == null) 
            return 0;
          else if (o1.domain_ != null && o2.domain_ != null) { 
            result = o1.domain_.compareTo(o2.domain_);
            if (result == 0) { 
              result = o1.name_.compareTo(o2.name_);
              if (result == 0) { 
                result = o1.path_.compareTo(o2.path_);
              }
            }
          }
        }
        return result;
      }        
    }
    
    public Vector<CanonicalCookie> findCookiesForURL(URL url) { 
      
      Vector<CanonicalCookie> cookiesOut = new Vector<CanonicalCookie>();
      
      long currentTime = System.currentTimeMillis();
      
      // get the url's tld name 
      String urlHost = url.getHost().toLowerCase();
      String urlRootDomainName = URLUtils.extractRootDomainName(urlHost);
      
      if (urlRootDomainName == null) { 
      	LOG.error("###FIND COOKIE GET ROOT DOMAIN NAME FOR HOST RETURNED NULL:" + urlHost);
      }
      if (urlRootDomainName != null) { 
      	
	      String urlSubDomainName  = urlHost.substring(0,urlHost.length() - urlRootDomainName.length());
	      
	      // construct a CrawlHostCookie for the query 
	      CanonicalCookie queryCookie = new CanonicalCookie(urlRootDomainName.hashCode());
	      
	      // now search in the vector 
	      int itemPosition = Collections.binarySearch(_cookies,queryCookie,new CookieComparator());
	      
	      itemPosition = -(itemPosition + 1);
	      
	      // if an entry exists ... 
	      // start walking the list ... 
	      for (int i=itemPosition;i<_cookies.size();++i) { 
	
	        CanonicalCookie currentCookie = (CanonicalCookie)_cookies.get(i);
	        
	        if (currentCookie._rootDomainHash != queryCookie._rootDomainHash) 
	          break;
	        
	        String currentCookieRootDomainName = URLUtils.extractRootDomainName(currentCookie.Domain());
	        
	        if (currentCookieRootDomainName == null) { 
	        	LOG.error("###EXTRACT ROOT DOMAIN FOR HOST RETURNED NULL:" + currentCookie.Domain());
	        }
	        
	        if (currentCookieRootDomainName != null && currentCookieRootDomainName.equalsIgnoreCase(urlRootDomainName)) { 
	
	          // if at the high level the cookies match
	          // time to dig a little deeper ... 
	          int lengthDelta = urlHost.length() - currentCookie.Domain().length();
	
	          // Ensure |url_host| is |cookie_domain| or one of its subdomains.
	          boolean validMatch = false; 
	  
	              
	          if (lengthDelta == 0) {
	            // ok both cookies domains are the same length ...
	            // probably a match ... 
	            String currentCookieSubDomainName = currentCookie.Domain().substring(0,currentCookie.Domain().length() - currentCookieRootDomainName.length());
	            if (urlSubDomainName.length() ==0) { 
	              validMatch = true;
	            }
	            else {
	              validMatch = urlSubDomainName.equals(currentCookieSubDomainName);
	            }
	          }
	          else if (lengthDelta == -1) { 
	            if (currentCookie.Domain().charAt(0) == '.')
	              validMatch = true;
	          }
	          // else if 
	          else if (lengthDelta >= 1 && currentCookie.Domain().charAt(0) == '.') { 
	            // www.google.com
	            //    .google.com
	            if (urlHost.substring(lengthDelta).compareToIgnoreCase(currentCookie.Domain()) == 0) { 
	              validMatch = true;
	            }
	          }
	          
	          // now check for valid match ... 
	          if (validMatch) { 
	            // check to see if the cookie expired ...
	            if (currentCookie.IsExpired(currentTime)) { 
	              // time to remove it from the store ... 
	              _cookies.remove(i);
	              --i;
	            }
	            else { 
	              // ok check path ... 
	              if (currentCookie.IsOnPath(url.getPath())) { 
	                // matched ... TOUCH the cookie 
	                currentCookie.SetLastAccessDate(System.currentTimeMillis());
	                // and add it to the vector ... 
	                cookiesOut.add(currentCookie);
	              }
	            }
	          }
	        }
	      }
	      return (cookiesOut.size() != 0) ? cookiesOut : null;
      }
      return null;
    }
    
    public boolean setCookie(URL urlObject,String cookie) {
      ParsedCookie cookieObj = new ParsedCookie(cookie);
      if (cookieObj.IsValid()) { 
        return setCookie(urlObject,cookieObj,System.currentTimeMillis());
      }
      return false;
    }
    
    public synchronized boolean setCookie(URL url,ParsedCookie cookie,long creation_time) {
      boolean result = false;
      
      // validate cookie domain key against url domain ...  
      String cookieDomain = getDomainKeyForCookie(url,cookie);
      
      if (cookieDomain != null) { 
       
        String urlTLDName   = URLUtils.extractRootDomainName(url.getHost().toLowerCase());

        if (urlTLDName != null) { 
          // get canonical path 
          String canonicalPath = getCanonicalPathForCookie(url, cookie);
          // and date 
          long   canonicalDate = getCanonicalTimeForCookie(cookie,creation_time);
          
          // create a canonical cookie object ... 
          CanonicalCookie canonicalCookie = new CanonicalCookie(urlTLDName,cookieDomain,
              cookie.Name(), cookie.Value(), canonicalPath,
              cookie.IsSecure(), cookie.IsHttpOnly(),
              creation_time, creation_time,
              canonicalDate != -1, canonicalDate);
          
          // and delete the cookie if it exists ... 
          int deletionIndex  = deleteAnyEquivalentCookie(canonicalCookie);
          
          // and if the new cookie is not expired ... 
          if (!canonicalCookie.IsExpired(creation_time)) { 
            insertCookie(canonicalCookie,deletionIndex);
            result = true;
          }
          // ok now garbage collect based on domain ... 
          garbageCollectCookies(urlTLDName);

        }
        else { 
          LOG.error("###COOKIE: Unable to Extract Root Domain from Cookie Domain:" + cookieDomain);
        }
      }
      return result;
    }
    
    private void garbageCollectCookies(String rootDomainName) { 
      Vector<Integer> range = collectCookieRange(rootDomainName);
      
      if (range.size() > kNumCookiesPerHost) { 
        // ok time to purge some cookies. 

        // sort range by lru 
        Collections.sort(range,new Comparator<Integer>() {

          @Override
          public int compare(Integer o1, Integer o2) {
            return ((Long)_cookies.get(o1).last_access_date_).compareTo((Long)_cookies.get(o2).last_access_date_);
          }
        });
        // Purge down to (|num_max| - |num_purge|) total cookies.
        int num_purge = kNumCookiesPerHostPurge;
        num_purge += range.size() - kNumCookiesPerHost;
        // subset
        range.setSize(num_purge);
        // resort based on index
        Collections.sort(range);
        // iterate and remove 
        int removed = 0;
        for (int index : range) { 
          _cookies.remove(index-removed);
          removed++;
        }
      }
    }
    
    private Vector<Integer> collectCookieRange(String rootDomainNameFilter) { 
      
      // construct a CrawlHostCookie for the query 
      CanonicalCookie queryCookie = new CanonicalCookie(rootDomainNameFilter.hashCode());
      
      // now search in the vector 
      int itemPosition = Collections.binarySearch(_cookies,queryCookie,new CookieComparator());
      
      Vector<Integer> rangeOut = new Vector<Integer>();

      itemPosition = -(itemPosition + 1);
      
      for (int i=itemPosition;i<_cookies.size();++i) { 

        CanonicalCookie currentCookie = (CanonicalCookie)_cookies.get(i);
        
        if (currentCookie.getRootDomainHash() == queryCookie._rootDomainHash) { 
          String currentCookieRootDomainName = URLUtils.extractRootDomainName(currentCookie.Domain());
        
          if (currentCookieRootDomainName == null) { 
          	LOG.error("#### GET ROOT DOMAIN FOR HOST RETURNED NULL:" + currentCookie);
          }
          
          if (currentCookieRootDomainName != null && currentCookieRootDomainName.equalsIgnoreCase(rootDomainNameFilter)) {
            rangeOut.add(i);
          }
        }
        else { 
          break;
        }
      }
      return rangeOut;
    }
    
    private void insertCookie(CanonicalCookie cookie,int deletionIndexHint) { 
      if (deletionIndexHint != -1) { 
        // fast path ... straightforward insertion ...
        _cookies.insertElementAt(cookie, deletionIndexHint);
      }
      else {
        // search for proper location for the insertion ...
        int insertionIndex = Collections.binarySearch(_cookies,cookie,new CookieComparator());
        
        if (insertionIndex < 0 ) {
          insertionIndex = -(insertionIndex + 1);
        }
        // insert at head of sub-list ... 
        _cookies.insertElementAt(cookie,insertionIndex);
      }
    }
        
    private int deleteAnyEquivalentCookie(CanonicalCookie cookie) { 
      
      int location = Collections.binarySearch(_cookies,cookie,new CookieComparator());
      
      if (location >= 0) { 
        _cookies.remove(location);
        return location;
      }
      return -1;
    }
    
    private static final String EXPIRES_PATTERN_1 = "EEE, dd-MMM-yyyy HH:mm:ss";
    private static final String EXPIRES_PATTERN_2 = "EEE, dd-MMM-yyyy HH:mm:ss z";


    static long getCanonicalTimeForCookie(ParsedCookie cookie, long currentTime) { 
      // First, try the Max-Age attribute.
      if (cookie.HasMaxAge()) { 
        // parse max age as a long ... 
        try { 
          long maxAgeInSeconds = Long.parseLong(cookie.MaxAge());
          return currentTime + (maxAgeInSeconds * 1000);
        }
        catch (NumberFormatException e) { 
          
        }
      }

      // Try the Expires attribute.
      if (cookie.HasExpires()) {
        return DateUtils.parseHttpDate(cookie.Expires());
      }
      return -1;
    }

    
    private static String getCanonicalPathForCookie(URL url,ParsedCookie cookie) { 
      // The path was supplied in the cookie, we'll take it.
      if (cookie.HasPath() && cookie.Path().length() !=0 && cookie.Path().charAt(0) == '/')
        return cookie.Path();
      // The path was not supplied in the cookie or invalid, we will default
      // to the current URL path.
      // """Defaults to the path of the request URL that generated the
      //    Set-Cookie response, up to, but not including, the
      //    right-most /."""
      // How would this work for a cookie on /?  We will include it then.
      String url_path = url.getPath();
      
      int idx = url_path.lastIndexOf('/');
      
      // The cookie path was invalid or a single '/'.
      if (idx == 0 || idx == -1)
        return "/";
      
      // Return up to the rightmost '/'.
      return url_path.substring(0, idx);
    }
    
    private static String getDomainKeyForCookie(URL url,ParsedCookie cookie) {
      if (cookie.Domain().length() != 0) { 
        
        String urlHost = url.getHost().toLowerCase();
        
        if (!cookie.HasDomain() || cookie.Domain().length() == 0) 
          return urlHost;
        
        String cookieHost = cookie.Domain().toLowerCase();
        
        if (cookieHost.charAt(0) != '.')
          cookieHost = '.' + cookieHost;
        
        // validate that the host contains more than tld names 
        if (URLUtils.extractRootDomainName(cookieHost) == null) { 
          return null;
        }
        // validate that there are more than one parts to the cookie host domain  
        if (cookieHost.indexOf('.', 1) == -1)
          return null;
        
        
        // now get tld name for cookie domain and url domain 
        String urlHostTLD = URLUtils.extractRootDomainName(urlHost);
        String cookieTLD  = URLUtils.extractRootDomainName(cookieHost);
        
        // they must match .. 
        if (urlHostTLD != null && cookieTLD != null && urlHostTLD.equals(cookieTLD)) {
          
          int delta = urlHost.length() - cookieHost.length();
          
          // Ensure |url_host| is |cookie_domain| or one of its subdomains.
          boolean validCookie = false; 
            
          if (delta == 0) {
            validCookie = true;
          }
          else if (delta == -1) { 
            if (cookieHost.charAt(0) == '.')
              validCookie = true;
          }
          // else if 
          else if (delta >= 1) { 
            // www.google.com
            //    .google.com
            if (urlHost.substring(delta).compareTo(cookieHost) == 0) { 
              validCookie = true;
            }
          }
          if (validCookie) { 
            return cookieHost;
          }
          
        }
      }
      return null;
    }
    
    public static class CookieStoreUnitTest { 
      
      static final String kUrlGoogle= "http://www.google.izzle";
      static final String kUrlGoogleSecure= "https://www.google.izzle";
      static final String kUrlFtp= "ftp://ftp.google.izzle/";
      static final String kValidCookieLine= "A=B; path=/";
      static final String kValidDomainCookieLine= "A=B; path=/; domain=google.izzle";
      
      public static void main(String[] args) {
        CookieStoreUnitTest  utils = new CookieStoreUnitTest();
        
        try { 
          GarbageCollectorTest();
          
          utils.TimeParseTest();
          utils.DomainTest();
          utils.DomainWithTrailingDotTest();
          utils.ValidSubdomainTest();
          utils.InvalidDomainTest();
          utils.DomainWithoutLeadingDotTest();
          utils.CaseInsensitiveDomainTest();
          //TestIpAddress();
          utils.TestNonDottedAndTLD();
          // TestHostEndsWithDot();
        }
        catch (Exception e) { 
          e.printStackTrace();
        }
      }

      
      private static class TimeParseTestCase { 
        
        long parsedValue = -1;
        long expectedValue = -1;
        boolean isValidTime = false;
        
        public TimeParseTestCase(String stringToParse,boolean isValidTime,long expectedEpochValue) { 
          this.parsedValue = DateUtils.parseHttpDate(stringToParse);
          Calendar gmtCalendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
          gmtCalendar.setTimeInMillis(this.parsedValue);
          System.out.println(gmtCalendar.toString());
          this.expectedValue = expectedEpochValue;
          this.isValidTime = isValidTime;
        }
        
        public boolean validate() { 
          if (!isValidTime) { 
            return parsedValue == -1;
          }
          else { 
            return (parsedValue/1000) == expectedValue;
          }
        }
      }
      
      void TimeParseTest()throws Exception {
        
        Assert.assertTrue(new TimeParseTestCase("Sat, 15-Apr-17 21:01:22 GMT",true,1492290082).validate());
        Assert.assertTrue(new TimeParseTestCase("Thu, 19-Apr-2007 16:00:00 GMT",true,1176998400).validate());
        Assert.assertTrue(new TimeParseTestCase("Wed, 25 Apr 2007 21:02:13 GMT",true,1177534933).validate());
        Assert.assertTrue(new TimeParseTestCase("Thu, 19/Apr\\2007 16:00:00 GMT",true,1176998400).validate());
        Assert.assertTrue(new TimeParseTestCase("Fri, 1 Jan 2010 01:01:50 GMT",true,1262307710).validate());
        Assert.assertTrue(new TimeParseTestCase("Wednesday, 1-Jan-2003 00:00:00 GMT",true,1041379200).validate());
        Assert.assertTrue(new TimeParseTestCase(", 1-Jan-2003 00:00:00 GMT",true,1041379200).validate());
        Assert.assertTrue(new TimeParseTestCase(" 1-Jan-2003 00:00:00 GMT",true,1041379200).validate());
        Assert.assertTrue(new TimeParseTestCase("1-Jan-2003 00:00:00 GMT",true,1041379200).validate());
        Assert.assertTrue(new TimeParseTestCase("Wed,18-Apr-07 22:50:12 GMT",true,1176936612).validate());
        Assert.assertTrue(new TimeParseTestCase("WillyWonka  , 18-Apr-07 22:50:12 GMT",true,1176936612).validate());
        Assert.assertTrue(new TimeParseTestCase("WillyWonka  , 18-Apr-07 22:50:12",true,1176936612).validate());
        Assert.assertTrue(new TimeParseTestCase("WillyWonka  ,  18-apr-07   22:50:12",true,1176936612).validate());
        Assert.assertTrue(new TimeParseTestCase("Mon, 18-Apr-1977 22:50:13 GMT",true,230251813).validate());
        Assert.assertTrue(new TimeParseTestCase("Mon, 18-Apr-77 22:50:13 GMT",true,230251813).validate());
        Assert.assertTrue(new TimeParseTestCase("\"Sat, 15-Apr-17\\\"21:01:22\\\"GMT\"", true, 1492290082).validate());
        Assert.assertTrue(new TimeParseTestCase("Partyday, 18- April-07 22:50:12",true,1176936612).validate());
        Assert.assertTrue(new TimeParseTestCase("Partyday, 18 - Apri-07 22:50:12",true,1176936612).validate());
        Assert.assertTrue(new TimeParseTestCase("Wednes, 1-Januar-2003 00:00:00 GMT",true,1041379200).validate());
        Assert.assertTrue(new TimeParseTestCase("Sat, 15-Apr-17 21:01:22",true,1492290082).validate());
        Assert.assertTrue(new TimeParseTestCase("Sat, 15-Apr-17 21:01:22 GMT-2",true,1492290082).validate());
        Assert.assertTrue(new TimeParseTestCase("Sat, 15-Apr-17 21:01:22 GMT BLAH",true,1492290082).validate());
        Assert.assertTrue(new TimeParseTestCase("Sat, 15-Apr-17 21:01:22 GMT-0400",true,1492290082).validate());
        Assert.assertTrue(new TimeParseTestCase("Sat, 15-Apr-17 21:01:22 GMT-0400 (EDT)",true,1492290082).validate());
        Assert.assertTrue(new TimeParseTestCase("Sat, 15-Apr-17 21:01:22 DST",true,1492290082).validate());
        Assert.assertTrue(new TimeParseTestCase("Sat, 15-Apr-17 21:01:22 -0400",true,1492290082).validate());
        Assert.assertTrue(new TimeParseTestCase("Sat, 15-Apr-17 21:01:22 (hello there)",true,1492290082).validate());
        Assert.assertTrue(new TimeParseTestCase("Sat, 15-Apr-17 21:01:22 11:22:33",true,1492290082).validate());
        Assert.assertTrue(new TimeParseTestCase("Sat, 15-Apr-17 ::00 21:01:22",true,1492290082).validate());
        Assert.assertTrue(new TimeParseTestCase("Sat, 15-Apr-17 boink:z 21:01:22",true,1492290082).validate());
        Assert.assertTrue(new TimeParseTestCase("Sat, 15-Apr-17 91:22:33 21:01:22",false,0).validate());
        Assert.assertTrue(new TimeParseTestCase("Thu Apr 18 22:50:12 2007 GMT",true,1176936612).validate());
        Assert.assertTrue(new TimeParseTestCase("22:50:12 Thu Apr 18 2007 GMT",true,1176936612).validate());
        Assert.assertTrue(new TimeParseTestCase("Thu 22:50:12 Apr 18 2007 GMT",true,1176936612).validate());
        Assert.assertTrue(new TimeParseTestCase("Thu Apr 22:50:12 18 2007 GMT",true,1176936612).validate());
        Assert.assertTrue(new TimeParseTestCase("Thu Apr 18 22:50:12 2007 GMT",true,1176936612).validate());
        Assert.assertTrue(new TimeParseTestCase("Thu Apr 18 2007 22:50:12 GMT",true,1176936612).validate());
        Assert.assertTrue(new TimeParseTestCase("Thu Apr 18 2007 GMT 22:50:12",true,1176936612).validate());
        Assert.assertTrue(new TimeParseTestCase("Sat, 15-Apr-17 21:01:22 GMT",true,1492290082).validate());
        Assert.assertTrue(new TimeParseTestCase("15-Sat, Apr-17 21:01:22 GMT",true,1492290082).validate());
        Assert.assertTrue(new TimeParseTestCase("15-Sat, Apr 21:01:22 GMT 17",true,1492290082).validate());
        Assert.assertTrue(new TimeParseTestCase("15-Sat, Apr 21:01:22 GMT 2017",true,1492290082).validate());
        Assert.assertTrue(new TimeParseTestCase("15 Apr 21:01:22 2017",true,1492290082).validate());
        Assert.assertTrue(new TimeParseTestCase("15 17 Apr 21:01:22",true,1492290082).validate());
        Assert.assertTrue(new TimeParseTestCase("Apr 15 17 21:01:22",true,1492290082).validate());
        Assert.assertTrue(new TimeParseTestCase("Apr 15 21:01:22 17",true,1492290082).validate());
        Assert.assertTrue(new TimeParseTestCase("2017 April 15 21:01:22",true,1492290082).validate());
        Assert.assertTrue(new TimeParseTestCase("15 April 2017 21:01:22",true,1492290082).validate());
        Assert.assertTrue(new TimeParseTestCase("98 April 17 21:01:22",false,0).validate());
        Assert.assertTrue(new TimeParseTestCase("Thu, 012-Aug-2008 20:49:07 GMT",false,0).validate());
        Assert.assertTrue(new TimeParseTestCase("Thu, 12-Aug-31841 20:49:07 GMT",false,0).validate());
        Assert.assertTrue(new TimeParseTestCase("Thu, 12-Aug-9999999999 20:49:07 GMT",false,0).validate());
        Assert.assertTrue(new TimeParseTestCase("Thu, 999999999999-Aug-2007 20:49:07 GMT",false,0).validate());
        Assert.assertTrue(new TimeParseTestCase("Thu, 12-Aug-2007 20:61:99999999999 GMT",false,0).validate());
        Assert.assertTrue(new TimeParseTestCase("IAintNoDateFool",false,0).validate());
      }
      
      void DomainTest() throws Exception {
        
        URL url_google = new URL(kUrlGoogle);

        CookieStore cm = new CookieStore();
        Assert.assertTrue(cm.setCookie(url_google, "A=B"));
        Assert.assertEquals("A=B", cm.GetCookies(url_google));
        Assert.assertTrue(cm.setCookie(url_google, "C=D; domain=.google.izzle"));
        Assert.assertEquals("A=B; C=D", cm.GetCookies(url_google));

        // Verify that A=B was set as a host cookie rather than a domain
        // cookie -- should not be accessible from a sub sub-domain.
        Assert.assertEquals("C=D", cm.GetCookies(new URL("http://foo.www.google.izzle")));

        // Test and make sure we find domain cookies on the same domain.
        Assert.assertTrue(cm.setCookie(url_google, "E=F; domain=.www.google.izzle"));
        Assert.assertEquals("A=B; C=D; E=F", cm.GetCookies(url_google));

        // Test setting a domain= that doesn't start w/ a dot, should
        // treat it as a domain cookie, as if there was a pre-pended dot.
        Assert.assertTrue(cm.setCookie(url_google, "G=H; domain=www.google.izzle"));
        Assert.assertEquals("A=B; C=D; E=F; G=H", cm.GetCookies(url_google));

        // Test domain enforcement, should fail on a sub-domain or something too deep.
        Assert.assertFalse(cm.setCookie(url_google, "I=J; domain=.izzle"));
        Assert.assertEquals("", cm.GetCookies(new URL("http://a.izzle")));
        Assert.assertFalse(cm.setCookie(url_google, "K=L; domain=.bla.www.google.izzle"));
        Assert.assertEquals("C=D; E=F; G=H",
                  cm.GetCookies(new URL("http://bla.www.google.izzle")));
        Assert.assertEquals("A=B; C=D; E=F; G=H", cm.GetCookies(url_google));
      }

      // FireFox recognizes domains containing trailing periods as valid.
      // IE and Safari do not. Assert the expected policy here.
      void DomainWithTrailingDotTest() throws Exception {
        CookieStore cm = new CookieStore();
        CrawlHostImpl crawlHost = new CrawlHostImpl(null,1234);

        URL url_google = new URL("http://www.google.com");

        Assert.assertFalse(cm.setCookie(url_google, "a=1; domain=.www.google.com."));
        Assert.assertFalse(cm.setCookie(url_google, "b=2; domain=.www.google.com.."));
        Assert.assertEquals("", cm.GetCookies(url_google));
      }

      // Test that cookies can bet set on higher level domains.
      // http://b/issue?id=896491
      void ValidSubdomainTest() throws Exception {
        CookieStore cm = new CookieStore();
        CrawlHostImpl crawlHost = new CrawlHostImpl(null,1234);

        URL url_abcd = new URL("http://a.b.c.d.com");
        URL url_bcd = new URL("http://b.c.d.com");
        URL url_cd = new URL("http://c.d.com");
        URL url_d = new URL("http://d.com");

        Assert.assertTrue(cm.setCookie(url_abcd, "a=1; domain=.a.b.c.d.com"));
        Assert.assertTrue(cm.setCookie(url_abcd, "b=2; domain=.b.c.d.com"));
        Assert.assertTrue(cm.setCookie(url_abcd, "c=3; domain=.c.d.com"));
        Assert.assertTrue(cm.setCookie(url_abcd, "d=4; domain=.d.com"));

        Assert.assertEquals("a=1; b=2; c=3; d=4", cm.GetCookies(url_abcd));
        Assert.assertEquals("b=2; c=3; d=4", cm.GetCookies(url_bcd));
        Assert.assertEquals("c=3; d=4", cm.GetCookies(url_cd));
        Assert.assertEquals("d=4", cm.GetCookies(url_d));

        // Check that the same cookie can exist on different sub-domains.
        Assert.assertTrue(cm.setCookie(url_bcd, "X=bcd; domain=.b.c.d.com"));
        Assert.assertTrue(cm.setCookie(url_bcd, "X=cd; domain=.c.d.com"));
        Assert.assertEquals("b=2; c=3; d=4; X=bcd; X=cd", cm.GetCookies(url_bcd));
        Assert.assertEquals("c=3; d=4; X=cd", cm.GetCookies(url_cd));
      }

      // Test that setting a cookie which specifies an invalid domain has
      // no side-effect. An invalid domain in this context is one which does
      // not match the originating domain.
      // http://b/issue?id=896472
      void InvalidDomainTest() throws Exception{
        {
          CookieStore cm = new CookieStore();
          URL url_foobar = new URL("http://foo.bar.com");
          CrawlHostImpl crawlHost = new CrawlHostImpl(null,1234);


          // More specific sub-domain than allowed.
          Assert.assertFalse(cm.setCookie(url_foobar, "a=1; domain=.yo.foo.bar.com"));

          Assert.assertFalse(cm.setCookie(url_foobar, "b=2; domain=.foo.com"));
          Assert.assertFalse(cm.setCookie(url_foobar, "c=3; domain=.bar.foo.com"));

          // Different TLD, but the rest is a substring.
          Assert.assertFalse(cm.setCookie(url_foobar, "d=4; domain=.foo.bar.com.net"));

          // A substring that isn't really a parent domain.
          Assert.assertFalse(cm.setCookie(url_foobar, "e=5; domain=ar.com"));

          // Completely invalid domains:
          Assert.assertFalse(cm.setCookie(url_foobar, "f=6; domain=."));
          Assert.assertFalse(cm.setCookie(url_foobar, "g=7; domain=/"));
          Assert.assertFalse(cm.setCookie(url_foobar, "h=8; domain=http://foo.bar.com"));
          Assert.assertFalse(cm.setCookie(url_foobar, "i=9; domain=..foo.bar.com"));
          Assert.assertFalse(cm.setCookie(url_foobar, "j=10; domain=..bar.com"));

          // Make sure there isn't something quirky in the domain canonicalization
          // that supports full URL semantics.
          Assert.assertFalse(cm.setCookie(url_foobar, "k=11; domain=.foo.bar.com?blah"));
          Assert.assertFalse(cm.setCookie(url_foobar, "l=12; domain=.foo.bar.com/blah"));
          Assert.assertFalse(cm.setCookie(url_foobar, "m=13; domain=.foo.bar.com:80"));
          Assert.assertFalse(cm.setCookie(url_foobar, "n=14; domain=.foo.bar.com:"));
          Assert.assertFalse(cm.setCookie(url_foobar, "o=15; domain=.foo.bar.com#sup"));

          Assert.assertEquals("", cm.GetCookies(url_foobar));
        }

        {
          // Make sure the cookie code hasn't gotten its subdomain string handling
          // reversed, missed a suffix check, etc.  It's important here that the two
          // hosts below have the same domain + registry.
          CookieStore cm = new CookieStore();
          CrawlHostImpl crawlHost = new CrawlHostImpl(null,1234);
          URL url_foocom = new URL("http://foo.com.com");
          Assert.assertFalse(cm.setCookie(url_foocom, "a=1; domain=.foo.com.com.com"));
          Assert.assertEquals("", cm.GetCookies(url_foocom));
        }
      }

      // Test the behavior of omitting dot prefix from domain, should
      // function the same as FireFox.
      // http://b/issue?id=889898
      void DomainWithoutLeadingDotTest() throws Exception{
        {  // The omission of dot results in setting a domain cookie.
          CookieStore cm = new CookieStore();
          CrawlHostImpl crawlHost = new CrawlHostImpl(null,1234);

          URL url_hosted = new URL("http://manage.hosted.filefront.com");
          URL url_filefront = new URL("http://www.filefront.com");
          Assert.assertTrue(cm.setCookie(url_hosted, "sawAd=1; domain=filefront.com"));
          Assert.assertEquals("sawAd=1", cm.GetCookies(url_hosted));
          Assert.assertEquals("sawAd=1", cm.GetCookies(url_filefront));
        }

        {  // Even when the domains match exactly, don't consider it host cookie.
          CookieStore cm = new CookieStore();
          CrawlHostImpl crawlHost = new CrawlHostImpl(null,1234);
          URL url = new URL("http://www.google.com");
          Assert.assertTrue(cm.setCookie(url, "a=1; domain=www.google.com"));
          Assert.assertEquals("a=1", cm.GetCookies(url));
          Assert.assertEquals("a=1", cm.GetCookies(new URL("http://sub.www.google.com")));
          Assert.assertEquals("", cm.GetCookies(new URL("http://something-else.com")));
        }
      }

      // Test that the domain specified in cookie string is treated case-insensitive
      // http://b/issue?id=896475.
      void CaseInsensitiveDomainTest()throws Exception {
        CookieStore cm = new CookieStore();
        CrawlHostImpl crawlHost = new CrawlHostImpl(null,1234);

        URL url_google = new URL("http://www.google.com");
        Assert.assertTrue(cm.setCookie(url_google, "a=1; domain=.GOOGLE.COM"));
        Assert.assertTrue(cm.setCookie(url_google, "b=2; domain=.wWw.gOOgLE.coM"));
        Assert.assertEquals("a=1; b=2", cm.GetCookies(url_google));
      }

      void TestIpAddress() throws Exception {
        URL url_ip = new URL("http://1.2.3.4/weee");
        {
          CookieStore cm = new CookieStore();
          CrawlHostImpl crawlHost = new CrawlHostImpl(null,1234);

          Assert.assertTrue(cm.setCookie(url_ip, kValidCookieLine));
          Assert.assertEquals("A=B", cm.GetCookies(url_ip));
        }

        {  // IP addresses should not be able to set domain cookies.
          CookieStore cm = new CookieStore();
          CrawlHostImpl crawlHost = new CrawlHostImpl(null,1234);

          Assert.assertFalse(cm.setCookie(url_ip, "b=2; domain=.1.2.3.4"));
          Assert.assertFalse(cm.setCookie(url_ip, "c=3; domain=.3.4"));
          Assert.assertEquals("", cm.GetCookies(url_ip));
          // It should be allowed to set a cookie if domain= matches the IP address
          // exactly.  This matches IE/Firefox, even though it seems a bit wrong.
          Assert.assertFalse(cm.setCookie(url_ip, "b=2; domain=1.2.3.3"));
          Assert.assertEquals("", cm.GetCookies(url_ip));
          Assert.assertTrue(cm.setCookie(url_ip, "b=2; domain=1.2.3.4"));
          Assert.assertEquals("b=2", cm.GetCookies(url_ip));
        }
      }

      // Test host cookies, and setting of cookies on TLD.
      void TestNonDottedAndTLD() throws Exception{
        {
          CookieStore cm = new CookieStore();
          URL url = new URL("http://com/");
          CrawlHostImpl crawlHost = new CrawlHostImpl(null,1234);

          // Allow setting on "com", (but only as a host cookie).
          Assert.assertTrue(cm.setCookie(url, "a=1"));
          Assert.assertFalse(cm.setCookie(url, "b=2; domain=.com"));
          Assert.assertFalse(cm.setCookie(url, "c=3; domain=com"));
          Assert.assertEquals("a=1", cm.GetCookies(url));
          // Make sure it doesn't show up for a normal .com, it should be a host
          // not a domain cookie.
          Assert.assertEquals("", cm.GetCookies(new URL("http://hopefully-no-cookies.com/")));
          Assert.assertEquals("", cm.GetCookies(new URL("http://.com/")));
        }

        {  // http://com. should be treated the same as http://com.
          CookieStore cm = new CookieStore();
          URL url = new URL("http://com./index.html");
          CrawlHostImpl crawlHost = new CrawlHostImpl(null,1234);

          Assert.assertTrue(cm.setCookie(url, "a=1"));
          Assert.assertEquals("a=1", cm.GetCookies(url));
          Assert.assertEquals("", cm.GetCookies(new URL("http://hopefully-no-cookies.com./")));
        }

        {  // Should not be able to set host cookie from a subdomain.
          CookieStore cm = new CookieStore();
          CrawlHostImpl crawlHost = new CrawlHostImpl(null,1234);

          URL url = new URL("http://a.b");
          Assert.assertFalse(cm.setCookie(url, "a=1; domain=.b"));
          Assert.assertFalse(cm.setCookie(url, "b=2; domain=b"));
          Assert.assertEquals("", cm.GetCookies(url));
        }

        {  // Same test as above, but explicitly on a known TLD (com).
          CookieStore cm = new CookieStore();
          CrawlHostImpl crawlHost = new CrawlHostImpl(null,1234);

          URL url = new URL("http://google.com");
          Assert.assertFalse(cm.setCookie(url, "a=1; domain=.com"));
          Assert.assertFalse(cm.setCookie(url, "b=2; domain=com"));
          Assert.assertEquals("", cm.GetCookies(url));
        }

        {  // Make sure can't set cookie on TLD which is dotted.
          CookieStore cm = new CookieStore();
          CrawlHostImpl crawlHost = new CrawlHostImpl(null,1234);

          URL url = new URL("http://google.co.uk");
          Assert.assertFalse(cm.setCookie(url, "a=1; domain=.co.uk"));
          Assert.assertFalse(cm.setCookie(url, "b=2; domain=.uk"));
          Assert.assertEquals("", cm.GetCookies(url));
          Assert.assertEquals("", cm.GetCookies(new URL("http://something-else.co.uk")));
          Assert.assertEquals("", cm.GetCookies(new URL("http://something-else.uk")));
        }

        {  // Intranet URLs should only be able to set host cookies.
          CookieStore cm = new CookieStore();
          CrawlHostImpl crawlHost = new CrawlHostImpl(null,1234);

          URL url = new URL("http://b");
          Assert.assertTrue(cm.setCookie(url, "a=1"));
          Assert.assertFalse(cm.setCookie(url, "b=2; domain=.b"));
          Assert.assertFalse(cm.setCookie(url, "c=3; domain=b"));
          Assert.assertEquals("a=1", cm.GetCookies(url));
        }
      }

      // Test reading/writing cookies when the domain ends with a period,
      // as in "www.google.com."
      void TestHostEndsWithDot()throws Exception {
        CookieStore cm = new CookieStore();
        CrawlHostImpl crawlHost = new CrawlHostImpl(null,1234);

        URL url = new URL("http://www.google.com");
        URL url_with_dot = new URL("http://www.google.com.");
        Assert.assertTrue(cm.setCookie(url, "a=1"));
        Assert.assertEquals("a=1", cm.GetCookies(url));

        // Do not share cookie space with the dot version of domain.
        // Note: this is not what FireFox does, but it _is_ what IE+Safari do.
        Assert.assertFalse(cm.setCookie(url, "b=2; domain=.www.google.com."));
        Assert.assertEquals("a=1", cm.GetCookies(url));

        Assert.assertTrue(cm.setCookie(url_with_dot, "b=2; domain=.google.com."));
        Assert.assertEquals("b=2", cm.GetCookies(url_with_dot));

        // Make sure there weren't any side effects.
        Assert.assertEquals(cm.GetCookies(new URL("http://hopefully-no-cookies.com/")), "");
        Assert.assertEquals("", cm.GetCookies(new URL("http://.com/")));
      }
      
      
      static void GarbageCollectorTest() throws Exception { 
        CookieStore cm = new CookieStore();

        int oldCookiesPerHost = kNumCookiesPerHost;
        int oldCookiesPerHostPurge = kNumCookiesPerHostPurge;
        kNumCookiesPerHost = 10;
        kNumCookiesPerHostPurge =5;
        
        cm.setCookie(new URL("http://www.google.com/foo"), "a=1; domain=www.google.com");
        cm.setCookie(new URL("http://www.google.com/foo"), "b=2; domain=www.google.com");
        cm.setCookie(new URL("http://www.google.com/foo"), "c=3; domain=www.google.com");
        cm.setCookie(new URL("http://www.google.com/foo"), "d=4; domain=www.google.com");
        cm.setCookie(new URL("http://www.google.com/foo"), "e=5; domain=www.google.com");
        cm.setCookie(new URL("http://www.google.com/foo"), "f=6; domain=www.google.com");
        cm.setCookie(new URL("http://www.google.com/foo"), "g=7; domain=www.google.com");
        cm.setCookie(new URL("http://www.google.com/foo"), "h=8; domain=www.google.com");
        cm.setCookie(new URL("http://www.google.com/foo"), "i=9; domain=www.google.com");
        cm.setCookie(new URL("http://www.google.com/foo"), "j=10; domain=www.google.com");

        Thread.currentThread().sleep(1);
        // touch first guys again ... 
        cm.setCookie(new URL("http://www.google.com/foo"), "a=1; domain=www.google.com");
        cm.setCookie(new URL("http://www.google.com/foo"), "b=2; domain=www.google.com");
        cm.setCookie(new URL("http://www.google.com/foo"), "c=3; domain=www.google.com");
        cm.setCookie(new URL("http://www.google.com/foo"), "d=4; domain=www.google.com");
        // 
        cm.setCookie(new URL("http://www.google.com/foo"), "k=11; domain=www.google.com");
        
        System.out.println(cm.GetCookies(new URL("http://www.google.com/foo")));

        Assert.assertTrue(cm.GetCookies(new URL("http://www.google.com/foo")).equals("a=1; b=2; c=3; d=4; k=11"));
        
        kNumCookiesPerHost = oldCookiesPerHost;
        kNumCookiesPerHostPurge = oldCookiesPerHostPurge;
                 
      }
    }
  }
  
  
}
