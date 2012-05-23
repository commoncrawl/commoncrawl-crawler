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

package org.commoncrawl.service.dns;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.protocol.CrawlURLMetadata;
import org.commoncrawl.service.crawler.filter.FilterResults;
import org.commoncrawl.service.crawler.filters.Filter;
import org.commoncrawl.util.CCStringUtils;
import org.junit.Test;

/**
 * 
 * @author rana
 *
 */
public class DNSRewriteFilter extends Filter {

  private static final Log LOG = LogFactory.getLog(DNSRewriteFilter.class);

  
  static class DNSRewriteItem { 
    
    public enum TestType { 
      Inclusion,
      Exclusion
    };
    
    public TestType   testType = TestType.Inclusion;
    public String     tldName;
    public Pattern    pattern;
    public String     rewriteRule;
    
  }
  
  private Vector<DNSRewriteItem> rewriteItems = new Vector<DNSRewriteItem>();
  
  public DNSRewriteFilter() { 
    
  }
  
  public DNSRewriteFilter(String filterPath)  { 
    super(filterPath,false);
  }
  
  
  @Override
  public void clear() {
    rewriteItems.clear();
  }
  
  @Override
  public void loadFilterItem(String filterItemLine) throws java.io.IOException {
    String parts[] = filterItemLine.split(",");
    
    if (parts.length == 3) { 
      DNSRewriteItem rewriteItem = new DNSRewriteItem();
      
      rewriteItem.tldName = parts[0];
      String patternStr = parts[1];
      if (patternStr.charAt(0) == '!') { 
        rewriteItem.testType = DNSRewriteItem.TestType.Exclusion;
        patternStr = patternStr.substring(1);
      }
      try { 
        rewriteItem.pattern = Pattern.compile(patternStr);
      }
      catch (PatternSyntaxException e) { 
        throw new IOException("Pattern syntax exception parsing line:" + filterItemLine + "\nException:" + CCStringUtils.stringifyException(e));
      }
      rewriteItem.rewriteRule = parts[2];
      rewriteItems.add(rewriteItem);
    }
    else { 
      throw new IOException("Invalid Filter Line:" + filterItemLine);
    }
  };
  
  @Override
  public FilterResult filterItem(String rootDomainName,String fullyQualifiedDomainName, String urlPath,CrawlURLMetadata metadata, FilterResults results) {
    
    for (DNSRewriteItem item : rewriteItems) { 
      if (rootDomainName.equals(item.tldName)) { 
        Matcher matcher = item.pattern.matcher(fullyQualifiedDomainName);
        
        boolean matches = matcher.matches();

        if (matches && item.testType == DNSRewriteItem.TestType.Inclusion) {
          StringBuffer finalString = new StringBuffer();
          int searchIndexStart = 0;
          while (searchIndexStart != item.rewriteRule.length()) { 
            int indexOfNextSlash = item.rewriteRule.indexOf('\\',searchIndexStart);
            if (indexOfNextSlash == -1) { 
              finalString.append(item.rewriteRule.substring(searchIndexStart));
              searchIndexStart = item.rewriteRule.length();
            }
            else { 
              if (indexOfNextSlash - searchIndexStart != 0) { 
                finalString.append(item.rewriteRule.substring(searchIndexStart,indexOfNextSlash));
              }
              searchIndexStart = indexOfNextSlash + 1;
              if (indexOfNextSlash + 1 != item.rewriteRule.length() && (item.rewriteRule.charAt(indexOfNextSlash+1) >= '1' && item.rewriteRule.charAt(indexOfNextSlash+1) <= '9')) {
                searchIndexStart++;
                int index = Integer.parseInt(item.rewriteRule.substring(indexOfNextSlash+1,indexOfNextSlash+2));
                if (index < matcher.groupCount()) { 
                  finalString.append(matcher.group(index));
                }
                else if (index == matcher.groupCount()) { 
                  finalString.append(rootDomainName);
                }
                else { 
                  LOG.error("Invalid group index specified in rewrite rule:" + index);
                  return FilterResult.Filter_NoAction;
                }
              }
              else { 
                finalString.append('\\');
              }
            }
          }
          results.setRewrittenDomainName(finalString.toString());
          return FilterResult.Filter_Modified;
        }
        else if (!matches && item.testType == DNSRewriteItem.TestType.Exclusion) { 
          results.setRewrittenDomainName(item.rewriteRule);
          return FilterResult.Filter_Modified;
        }
      }
    }
    return FilterResult.Filter_NoAction;
  }

  @Test
  public void testFilter() throws Exception {
    FilterResults filterResults = new FilterResults();
    loadFilterItem("blogspot.com,!(www\\.)(blogspot\\.com),blogspot.l.google.com");
    loadFilterItem("deviantart.com,(.*)(deviantart\\.com),www.deviantart.com");
    loadFilterItem("wordpress.com,!((www\\.)|(.*files\\.))(wordpress\\.com),lb.wordpress.com");
    loadFilterItem("alibaba.com,(.*)(cn\\.alibaba\\.com),cn.alibaba.com");
    loadFilterItem("alibaba.com,(.*)(blog\\.china\\.alibaba\\.com),blog.china.alibaba.com");
    loadFilterItem("alibaba.com,(.*)(en\\.alibaba\\.com),minisite.alibaba.com");
    loadFilterItem("43people.com,(.*)(43people\\.com),lb1.43people.com");
    loadFilterItem("blog.co.uk,(.*)(blog\\.co\\.uk),blog.co.uk");
    loadFilterItem("ning.com,!(www\\.)(ning\\.com),ning.com");
    loadFilterItem("dtdns.net,(.*)(vernos\\.dtdns\\.net),vernos.dtdns.net");
    loadFilterItem("typepad.com,!(www\\.)(typepad\\.com),members.typepad.com");
    loadFilterItem("blog4ever.com,!(www\\.)(blog4ever\\.com),blog4ever.com");
    loadFilterItem("yahoo.com,(.*)(zhan\\.cn\\.yahoo\\.com),cn.yahoo.com");
    loadFilterItem("hi5.com,!(www\\.)(hi5\\.com),hi5.com");
    loadFilterItem("nireblog.com,!(www\\.)(nireblog\\.com),members.nireblog.com");
    loadFilterItem("blog.com,!(www\\.)(blog\\.com),members.blog.com");
    loadFilterItem("4t.com,!(www\\.)(4t\\.com),members.4t.com");
    loadFilterItem("wordpress.com,(.*)(files\\.wordpress\\.com),lb.files.wordpress.com");
    
    loadFilterItem("somedomain.com,([^\\.]*)\\.(somedomain\\.com),\\2");
    
    assertTrue(filterItem("blogspot.com","foobar.blogspot.com",null,null,filterResults) == FilterResult.Filter_Modified);
    assertTrue(filterResults.getRewrittenDomainName().equals("blogspot.l.google.com"));
    filterResults.clear();
    assertTrue(filterItem("blogspot.com","www.blogspot.com",null,null,filterResults) == FilterResult.Filter_NoAction);
    assertTrue(filterItem("somedomain.com","joe.somedomain.com",null,null,filterResults) == FilterResult.Filter_Modified);
    assertTrue(filterResults.getRewrittenDomainName().equals("somedomain.com"));
    filterResults.clear();
    assertTrue(filterItem("deviantart.com","animefangirl11.deviantart.com",null,null,filterResults) == FilterResult.Filter_Modified);
    assertTrue(filterResults.getRewrittenDomainName().equals("www.deviantart.com"));
    filterResults.clear();
    assertTrue(filterItem("wordpress.com","brianmschoedel.wordpress.com",null,null,filterResults) == FilterResult.Filter_Modified);
    assertTrue(filterResults.getRewrittenDomainName().equals("lb.wordpress.com"));
    filterResults.clear();
    assertTrue(filterItem("alibaba.com","wxjiugongge.cn.alibaba.com",null,null,filterResults) == FilterResult.Filter_Modified);
    assertTrue(filterResults.getRewrittenDomainName().equals("cn.alibaba.com"));
    filterResults.clear();
    assertTrue(filterItem("alibaba.com","petertoy.blog.china.alibaba.com",null,null,filterResults) == FilterResult.Filter_Modified);
    assertTrue(filterResults.getRewrittenDomainName().equals("blog.china.alibaba.com"));
    filterResults.clear();
    assertTrue(filterItem("alibaba.com","tomacamera.en.alibaba.com",null,null,filterResults) == FilterResult.Filter_Modified);
    assertTrue(filterResults.getRewrittenDomainName().equals("minisite.alibaba.com"));
    filterResults.clear();
    assertTrue(filterItem("blog.co.uk","jaspalsingh.blog.co.uk",null,null,filterResults) == FilterResult.Filter_Modified);
    assertTrue(filterResults.getRewrittenDomainName().equals("blog.co.uk"));
    filterResults.clear();
    assertTrue(filterItem("ning.com","alumnosieslaasuncion.ning.com",null,null,filterResults) == FilterResult.Filter_Modified);
    assertTrue(filterResults.getRewrittenDomainName().equals("ning.com"));
    filterResults.clear();
    assertTrue(filterItem("dtdns.net","ashleeandserena-com.vernos.dtdns.net",null,null,filterResults) == FilterResult.Filter_Modified);
    assertTrue(filterResults.getRewrittenDomainName().equals("vernos.dtdns.net"));
    filterResults.clear();
    assertTrue(filterItem("typepad.com","test.typepad.com",null,null,filterResults) == FilterResult.Filter_Modified);
    assertTrue(filterResults.getRewrittenDomainName().equals("members.typepad.com"));
    filterResults.clear();
    assertTrue(filterItem("blog4ever.com","member.blog4ever.com",null,null,filterResults) == FilterResult.Filter_Modified);
    assertTrue(filterResults.getRewrittenDomainName().equals("blog4ever.com"));
    filterResults.clear();
    assertTrue(filterItem("yahoo.com","renpinwangzi.zhan.cn.yahoo.com",null,null,filterResults) == FilterResult.Filter_Modified);
    assertTrue(filterResults.getRewrittenDomainName().equals("cn.yahoo.com"));


    filterResults.clear();
    assertTrue(filterItem("hi5.com","foobar.hi5.com",null,null,filterResults) == FilterResult.Filter_Modified);
    assertTrue(filterResults.getRewrittenDomainName().equals("hi5.com"));
    filterResults.clear();
    assertTrue(filterItem("blog.com","foobar.blog.com",null,null,filterResults) == FilterResult.Filter_Modified);
    assertTrue(filterResults.getRewrittenDomainName().equals("members.blog.com"));
    filterResults.clear();
    assertTrue(filterItem("4t.com","foobar.4t.com",null,null,filterResults) == FilterResult.Filter_Modified);
    assertTrue(filterResults.getRewrittenDomainName().equals("members.4t.com"));
    filterResults.clear();
    assertTrue(filterItem("wordpress.com","foobar.files.wordpress.com",null,null,filterResults) == FilterResult.Filter_Modified);
    assertTrue(filterResults.getRewrittenDomainName().equals("lb.files.wordpress.com"));
    
  }

  public static void main(String[] args) {
	  DNSRewriteFilter filter = new DNSRewriteFilter();
	  try {
	    filter.testFilter();
    } catch (Exception e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
    }
  }
  
}
