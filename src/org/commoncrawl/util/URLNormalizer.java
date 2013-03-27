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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.CharBuffer;

import org.junit.Test;

/**
 * 
 * @author rana
 *
 */
public class URLNormalizer {
  static char slash[] = {'/'};
  static char dotSlash[] = {'.', '/'};
  static char dotDotSlash[] = {'.', '.', '/'};
  static char slashDotSlash[] = {'/', '.', '/'};
  static char slashDotDotSlash[] = {'/', '.', '.', '/'};

  static void removeCharsAt(CharBuffer buffer,int start,int count) { 
    buffer.position(start + count);
    CharBuffer trailingSequence = buffer.slice();
    buffer.position(start);
    buffer.put(trailingSequence);
    buffer.limit(buffer.limit() - count);
  }
  
  static void replaceCharsAt(CharBuffer buffer,int start,int count,char[] newCharSequence) { 
    if (count > newCharSequence.length) { 
      buffer.position(start + count);
      CharBuffer trailingSequence = buffer.slice();
      // position past new char sequence ... 
      buffer.position(start + newCharSequence.length);
      buffer.put(trailingSequence);
    }
    // reset to start position ... 
    buffer.position(start);
    for (char c : newCharSequence) { 
      buffer.put(c);
    }
    if (count > newCharSequence.length) { 
      buffer.limit(buffer.limit() - (count - newCharSequence.length));   
    }
    else { 
      buffer.limit(buffer.limit() + (newCharSequence.length - count));
    }
  }  
  
  static CharBuffer copyString(String source) { 
    // create a char buffer ... 
    CharBuffer buffer = CharBuffer.allocate(source.length());
    
    // copy in original string ... 
    buffer.put(source);
    
    return buffer;
  }
  public static String normalizeString(String input) { 
    CharSequence sequence = input;
    CharBuffer buffer = null;
    boolean modified = false;
    
    int indexOut = -1;
    int searchStart = 0;

    
    
    // remove all occurences of '/./'
    
    
    while ((indexOut = indexOf(sequence,buffer,slashDotSlash,0,slashDotSlash.length)) != -1) {
      if (!modified) { 
        buffer = copyString(input);
        modified = true;
        sequence = buffer;
      }
      searchStart = indexOut;
      // get sub sequence (advanced past pattern)
      buffer.position(indexOut + slashDotSlash.length);
      CharBuffer trailingSequence = buffer.slice();
      // and append it back into the source buffer ...
      buffer.position(indexOut);
      // append single slash replacement ... 
      buffer.put('/');
      // and trailing string ... 
      buffer.put(trailingSequence);
      // and reset limit ... 
      buffer.limit(buffer.position());
      // and reset position to search start 
      buffer.position(searchStart);
    }
    
    
    // now process occurrences of /../
    
    indexOut = -1;
    searchStart = 0;
    
    if (modified) 
      buffer.position(0);

    while ((indexOut = indexOf(sequence,buffer,slashDotDotSlash,0,slashDotDotSlash.length)) != -1) {
      if (!modified) { 
        buffer = copyString(input);
        modified = true;
        sequence = buffer;
      }
      // get sub sequence (advanced past pattern)
      buffer.position(indexOut + slashDotDotSlash.length);
      CharBuffer trailingSequence = buffer.slice();
      // now walk backwards to previous occurence of / 
      int previousPos = indexOut;
      while (--previousPos >= 0) { 
        if (buffer.get(previousPos) == '/') 
          break;
      }
      // now if we found it ... 
      if (previousPos != -1) { 
        searchStart = previousPos;
        // set position to new location ... 
        buffer.position(previousPos + 1);
      }
      else { 
        searchStart = indexOut;
        // otherwise set position to indexout (just replace pattern itself)... 
        buffer.position(indexOut);
        // append single slash replacement ... 
        buffer.put('/');
      }
      // and trailing string ... 
      buffer.put(trailingSequence);
      // and reset limit ... 
      buffer.limit(buffer.position());
      // and reset position to search start 
      buffer.position(searchStart);
    }
    
    if (modified) 
      buffer.position(0);
    // now remove leading all leading ./ 
    while ((indexOut = indexOf(sequence,buffer,dotSlash,0,dotSlash.length)) == 0) { 
      if (!modified) { 
        buffer = copyString(input);
        modified = true;
        sequence = buffer;
      }
      replaceCharsAt(buffer,indexOut,dotSlash.length,slash);
      buffer.position(0);
    }

    if (modified)
      buffer.position(0);
    // and leading dot dot slashes ../ 
    while ((indexOut = indexOf(sequence,buffer,dotDotSlash,0,dotDotSlash.length)) == 0) { 
      if (!modified) { 
        buffer = copyString(input);
        modified = true;
        sequence = buffer;
      }
      replaceCharsAt(buffer,indexOut,dotDotSlash.length,slash);
      buffer.position(0);      
    }
    
    
    if (modified) { 
      //return modified content... 
      // reset position ... 
      buffer.position(0);
      return buffer.toString();
    }
    else {
      // return original string ... 
      return input;
    }
  }
  
  /**
   * Code shared by String and StringBuffer to do searches. The
   * source is the character array being searched, and the target
   * is the string being searched for.
   *
   * @param   source       the characters being searched.
   * @param   sourceCount  count of the source string.
   * @param   target       the characters being searched for.
   * @param   targetOffset offset of the target string.
   * @param   targetCount  count of the target string.
   */
  static int indexOf(CharSequence source,CharBuffer buffer, char[] target, int targetOffset, int targetCount) {
    if (targetCount == 0) {
      return -1;
    }

    int sourceOffset = 0;
    int sourceCount = source.length();
    
    char first  = target[targetOffset];
    int max = sourceOffset + (sourceCount - targetCount);

    for (int i = sourceOffset; i <= max; i++) {
      /* Look for first character. */
      if (source.charAt(i) != first) {
        while (++i <= max && source.charAt(i) != first);
      }

      /* Found first character, now look at the rest of v2 */
      if (i <= max) {
        int j = i + 1;
        int end = j + targetCount - 1;
        for (int k = targetOffset + 1; j < end && source.charAt(j) ==
          target[k]; j++, k++);

        if (j == end) {
          /* Found whole string. */
          if (buffer != null)
            return buffer.position() + (i - sourceOffset);
          else 
            return (i - sourceOffset);
        }
      }
    }
    return -1;
  }
  
 @Test
  public void testNormalizer() throws Exception {
/*
   String tests[] = { "/aa/bb/../../cc/../foo.html", "aa/bb/../../cc/../foo.html", "/xx/../",
        "./xx/.././././foo/././bar.html",".././././bar../html","/dyn-js/backlink.js?blogID=8100929&postID=9203679950426132998"
    };
*/
   
    String tests[] = { "/build_research.htm" 
    };
    
    for (int i=0;i<tests.length;++i) { 
      long startTime = System.nanoTime();
      String normalized = normalizeString(tests[i]);
      long endTime = System.nanoTime();
      System.out.println("test:" + i +" orig:" + tests[i] + " result: "+ normalized + " took:" + (endTime-startTime) );
    }
  }
  
  // @Test
  public void testNormalizer2() throws Exception {

    URL resourceURL = ClassLoader.getSystemResource("big_list.txt");
    
    if (resourceURL == null) {
      throw new FileNotFoundException();
    }
    InputStream stream = resourceURL.openStream();
    BufferedReader reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(stream)));
    
    String line = null;
    
    do { 
      line = reader.readLine();
      if (line != null){ 
        try { 
          URL theURL = new URL(line);
          String path = theURL.getPath();
          long startTime = System.nanoTime();
          String normalizedPath = normalizeString(path);
          long endTime = System.nanoTime();
          if (!path.equals(normalizedPath)) { 
            System.out.println("URL:" + theURL.toString() + " Normalized to:" + normalizedPath + " Took:" + (endTime-startTime));
          }
        }
        catch (MalformedURLException e) {
          System.out.println("Malformed URL:" + line);
        }
      }
    } while(line != null);
    
  }
  
}
