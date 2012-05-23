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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;


import com.google.common.collect.Lists;
import com.google.common.collect.TreeMultimap;

public class URLPattern {

  public static interface URLFragment {
    public boolean isWildcard();

    public boolean isMatch(String urlFragment);

    public int getMatchCount();
  }

  /*
   * URLPathFragment definition
   */
  public static class URLPathFragment implements URLFragment {

    boolean           _isLeafNode = false;
    ArrayList<String> _matches    = new ArrayList<String>();

    public URLPathFragment(String partString, boolean isLeafNode) {
      _matches.add(partString);
      _isLeafNode = isLeafNode;
    }

    public boolean isWildcard() {
      return false;
    }

    public boolean isMatch(String urlFragment) {
      return _matches.get(0).equals(urlFragment);
    }

    public int getMatchCount() {
      return _matches.size();
    }

    public ArrayList<String> getMatches() {
      return _matches;
    }

    public void addMatch(String pathPart, boolean isLeafNode) {
      _matches.add(pathPart);
      _isLeafNode |= isLeafNode;
    }

    @Override
    public String toString() {
      if (_matches.size() == 1) {
        return "/" + _matches.get(0);
      } else {
        String partOut = "/(";
        int partCount = 0;
        for (String part : _matches) {
          if (partCount++ != 0)
            partOut += "|";
          partOut += part;
        }
        partOut += ")";

        return partOut;
      }
    }
  }

  public static class URLQueryFragment implements URLFragment {

    String      _key    = null;
    Set<String> _values = new TreeSet<String>();

    public URLQueryFragment(String key, String value) {
      _key = key;
      _values.add(value);
    }

    public static String[] separateKeyValue(String queryPart) {
      String arrayOut[] = new String[2];
      int indexOfSep = queryPart.indexOf("=");
      if (indexOfSep != -1) {
        arrayOut[0] = queryPart.substring(0, indexOfSep);
        arrayOut[1] = ((indexOfSep + 1) != queryPart.length()) ? queryPart
            .substring(indexOfSep + 1) : "";
      } else {
        arrayOut[0] = queryPart;
        arrayOut[1] = "";
      }
      return arrayOut;
    }

    public void addMatch(String value, boolean isLeafNode) {
      _values.add(value);
    }

    @Override
    public int getMatchCount() {
      return _values.size();
    }

    @Override
    public boolean isWildcard() {
      return false;
    }

    @Override
    public boolean isMatch(String pathFragment) {
      String keyValuePair[] = separateKeyValue(pathFragment);
      return keyValuePair[0].equals(_key);
    }

    public String getKey() {
      return _key;
    }

    public Set<String> getValues() {
      return _values;
    }
  }

  /*
   * Wildcard definition
   */
  public static class WildcardPathFragment extends URLPathFragment {

    public WildcardPathFragment(boolean isLeafNode) {
      super("[^/?]*", isLeafNode);
    }

    public boolean isWildcard() {
      return true;
    }
  }

  /*
   * component vector
   */
  ArrayList<URLPathFragment>        _pathFragments  = new ArrayList<URLPathFragment>();
  TreeMap<String, URLQueryFragment> _queryFragments = new TreeMap<String, URLQueryFragment>();

  ArrayList<TextBytes>              _matches        = new ArrayList<TextBytes>();

  public URLPattern(String url, ArrayList<String> pathComponents,
      ArrayList<String> queryComponents) {
    for (String pathComponenet : pathComponents) {
      _pathFragments.add(new URLPathFragment(
          escapeReservedCharacters(pathComponenet), false));
    }
    for (String queryComponent : queryComponents) {
      String keyValuePair[] = URLQueryFragment.separateKeyValue(queryComponent);

      if (queryComponents.size() == 1
          && (keyValuePair[1] == null || keyValuePair[1].length() == 0)) {
        // NOOP?
      } else {
        URLQueryFragment fragment = _queryFragments.get(keyValuePair[0]);
        if (fragment == null) {
          _queryFragments.put(keyValuePair[0], new URLQueryFragment(
              keyValuePair[0], keyValuePair[1]));
        } else {
          fragment.addMatch(keyValuePair[1], false);
        }
      }
    }
    _matches.add(new TextBytes(url));
  }

  public int isMatch(ArrayList<String> pathComponents,
      ArrayList<String> queryComponents) {
    return addOrComputeMatch(pathComponents, queryComponents, true);
  }

  public int getMatchCount() {
    return _matches.size();
  }

  public String getMatchAt(int index) {
    return _matches.get(index).toString();
  }

  public ArrayList<TextBytes> getMatches() {
    return _matches;
  }

  public void addURL(String url, ArrayList<String> pathComponents,
      ArrayList<String> queryComponents) {
    addOrComputeMatch(pathComponents, queryComponents, false);
    _matches.add(new TextBytes(url));
  }

  private int addOrComputeMatch(ArrayList<String> pathComponents,
      ArrayList<String> queryComponents, boolean checkOnly) {
    if (pathComponents.size() == _pathFragments.size()
        && queryComponents.size() == _queryFragments.size()) {

      // all query parameters must match ... ?
      for (String queryPart : queryComponents) {
        String keyValuePair[] = URLQueryFragment.separateKeyValue(queryPart);
        if (!_queryFragments.containsKey(keyValuePair[0])) {
          return 0;
        }
      }

      Set<Integer> pathMatchIndexes = new HashSet<Integer>();
      for (int i = 0; i < _pathFragments.size(); ++i) {
        if (!_pathFragments.get(i).isWildcard()
            && _pathFragments.get(i).isMatch(
                escapeReservedCharacters(pathComponents.get(i)))) {
          pathMatchIndexes.add(i);
        }
      }
      if ((pathMatchIndexes.size() != 0 || _pathFragments.size() == 0)
          && !checkOnly) {

        for (int i = 0; i < _pathFragments.size(); ++i) {
          if (!pathMatchIndexes.contains(i)) {
            if (!_pathFragments.get(i).isWildcard()) {
              _pathFragments.get(i).addMatch(
                  escapeReservedCharacters(pathComponents.get(i)), false);
            }
          }
        }
        // add query parameters
        for (String queryPart : queryComponents) {
          String keyValuePair[] = URLQueryFragment.separateKeyValue(queryPart);
          _queryFragments.get(keyValuePair[0]).addMatch(keyValuePair[1], false);
        }
      }
      return (_pathFragments.size() == 0) ? 1 : pathMatchIndexes.size();
    }
    return 0;
  }

  public int getPathLength() {
    return _pathFragments.size();
  }

  public int getQueryLength() {
    return _queryFragments.size();
  }

  public URLPathFragment getURLPartAt(int index) {
    return _pathFragments.get(index);
  }

  private void makePartAtIndexWildcard(int index) {
    if (index < _pathFragments.size()) {
      URLPathFragment partAtIndex = _pathFragments.get(index);
      if (!partAtIndex.isWildcard()) {
        _pathFragments.set(index, new WildcardPathFragment(false));
      }
    }
  }

  public void compact() {
    for (int i = 0; i < _pathFragments.size(); ++i) {
      URLPathFragment part = _pathFragments.get(i);
      if (!part.isWildcard()
          && (part.getMatchCount() > 3 || valuesLookNumeric(part.getMatches()))) {
        makePartAtIndexWildcard(i);
      }
    }
  }

  public static String escapeReservedCharacters(String incoming) {
    incoming = incoming.replaceAll("([\\?\\\\\\[\\]\\+\\.\\*\\(\\)\\{\\}])",
        "\\\\$1");
    return incoming;
  }

  static Pattern isAllDigits = Pattern.compile("^[0-9]*$");
  static Pattern isAllHex    = Pattern.compile("^[a-fA-F0-9-]*$");

  public static boolean valuesLookNumeric(Iterable<String> values) {
    for (String str : values) {
      if (!isAllDigits.matcher(str).matches()
          && !isAllHex.matcher(str).matches()) {
        return false;
      }
    }
    return true;
  }

  // destructoid.com
  static Pattern containsEscapeChar = Pattern.compile("[%+]");

  public static boolean valuesContainsEscapeChars(Iterable<String> values) {
    for (String str : values) {
      if (containsEscapeChar.matcher(str).find()) {
        return true;
      }
    }
    return false;
  }

  public String generateRegEx() {
    String regExOut = "http://[^/]*";
    for (URLPathFragment component : _pathFragments) {
      regExOut += component.toString();
    }
    if (!regExOut.endsWith("/")) {
      regExOut += "/*";
    }
    if (_queryFragments.size() != 0) {
      regExOut += "\\?";
    }
    int queryItemCount = 0;
    for (URLQueryFragment fragment : _queryFragments.values()) {
      if (queryItemCount++ != 0) {
        regExOut += "&";
      }
      regExOut += escapeReservedCharacters(fragment.getKey());

      Set<String> values = fragment.getValues();

      String firstValue = values.iterator().next();

      if (values.size() == 1 && valuesLookNumeric(values)) {
        regExOut += "=*[^&]*";
      } else {
        if (values.size() > 1 || !firstValue.equals("")) {
          if (values.size() > 3 || firstValue.equals("")
              || valuesContainsEscapeChars(values)
              || (values.size() > 1 && valuesLookNumeric(values))) {
            if (firstValue.equals("")) {
              regExOut += "[=]*[^&]*";
            } else {
              regExOut += "=[^&]+";
            }
          } else {
            if (firstValue.equals("")) {
              regExOut += "[=]*(";
            } else {
              regExOut += "=(";
            }
            int valueCount = 0;
            for (String value : values) {
              if (!value.equals("")) {
                if (valueCount++ != 0) {
                  regExOut += "|";
                }
                regExOut += escapeReservedCharacters(value);
              }
            }
            regExOut += ")";
            if (firstValue.equals("")) {
              regExOut += "*";
            }
          }
        }
      }
      regExOut += "[&]*";
    }
    regExOut += ".*";
    return regExOut;
  }

  @Override
  public String toString() {
    String debugOut = "Pattern: " + generateRegEx();
    debugOut += "\n";
    debugOut += "Matched:\n";

    for (TextBytes url : _matches) {
      debugOut += url + "\n";
    }
    debugOut += "\n";

    return debugOut;
  }

  public static TreeMultimap<String, String> extractQueryParts(String content) {
    String key = null;
    String value = null;
    int mark = -1;

    TreeMultimap<String, String> map = TreeMultimap.create();

    for (int i = 0; i < content.length(); i++) {
      char c = content.charAt(i);
      switch (c) {
        case '&':
          value = content.substring(mark + 1, i);

          mark = i;
          if (key != null) {
            map.put(key, value);
            key = null;
          }
          break;
        case '=':
          if (key != null)
            break;
          key = content.substring(mark + 1, i);
          mark = i;
          break;
        case '+':
          break;
      }
    }

    if (key != null) {
      value = content.substring(mark + 1);
      map.put(key, value);
    } else if (mark < content.length()) {
      key = content.substring(mark + 1);
      map.put(key, "");
    }
    return map;
  }

  public static ArrayList<String> extractQueryPartsAsArrayList(
      String queryFragment) {

    ArrayList<String> arrayListOut = new ArrayList<String>();

    TreeMultimap<String, String> queryMap = extractQueryParts(queryFragment);

    for (Map.Entry<String, String> entry : queryMap.entries()) {
      String keyValue = entry.getKey();
      if (entry.getValue().length() != 0) {
        keyValue += "=";
        keyValue += entry.getValue();
      }
      arrayListOut.add(keyValue);
    }
    return arrayListOut;
  }

  public static String normalizeQueryURL(String url) {
    GoogleURL urlObject = new GoogleURL(url);
    if (urlObject.isValid() && urlObject.has_query()) {
      StringBuilder urlOut = new StringBuilder();
      urlOut.append(urlObject.getScheme());
      urlOut.append("://");
      if (urlObject.getUserName() != GoogleURL.emptyString) {
        urlOut.append(urlObject.getUserName());
        if (urlObject.getPassword() != GoogleURL.emptyString) {
          urlOut.append(":");
          urlOut.append(urlObject.getPassword());
        }
        urlOut.append("@");
      }
      urlOut.append(urlObject.getHost());

      if (urlObject.getPort() != GoogleURL.emptyString) {
        urlOut.append(":");
        urlOut.append(urlObject.getPort());
      }
      if (urlObject.getPath() != GoogleURL.emptyString) {
        urlOut.append(urlObject.getPath());
      }
      if (urlObject.getQuery() != GoogleURL.emptyString) {
        urlOut.append("?");
        TreeMultimap<String, String> queryMap = extractQueryParts(urlObject
            .getQuery());
        int partCount = 0;
        for (Map.Entry<String, String> entry : queryMap.entries()) {
          if (partCount++ != 0)
            urlOut.append("&");
          urlOut.append(entry.getKey());

          if (entry.getValue().length() != 0) {
            urlOut.append("=");
            urlOut.append(entry.getValue());
          }
        }
      }
      String urlFinal = urlOut.toString();
      if (urlFinal.endsWith("=") || urlFinal.endsWith("&")) {
        urlFinal = urlFinal.substring(0, urlFinal.length() - 1);
      }
      return urlOut.toString();
    }
    return url;
  }

  public static class URLPatternBuilder {
    Vector<URLPattern> _patterns = new Vector<URLPattern>();

    public void addPath(String url) {
      GoogleURL urlObject = new GoogleURL(url);

      if (urlObject.isValid()) {
        ArrayList<String> pathPartsVector = new ArrayList<String>();
        ArrayList<String> queryPartsVector = new ArrayList<String>();

        if (!urlObject.getPath().equals("/")) {
          String path = urlObject.getPath();
          if (path.length() != 0) {
            String pathParts[] = path.substring(1).split("/");
            pathPartsVector = Lists.newArrayList(pathParts);
          }
        }
        if (urlObject.has_query()) {
          String query = urlObject.getQuery();
          queryPartsVector = extractQueryPartsAsArrayList(query);
          if (queryPartsVector.size() == 1
              && queryPartsVector.get(0).indexOf("=") == -1) {
            queryPartsVector.clear();
          }
        }

        int highestMatchCount = 0;
        URLPattern matchedPattern = null;

        for (URLPattern pattern : _patterns) {
          if (pattern.getPathLength() == pathPartsVector.size()
              && pattern.getQueryLength() == queryPartsVector.size()) {
            int matchCount = pattern.isMatch(pathPartsVector, queryPartsVector);
            if (matchCount > highestMatchCount) {
              highestMatchCount = matchCount;
              matchedPattern = pattern;
            }
          }
        }

        if (matchedPattern != null) {
          matchedPattern.addURL(url, pathPartsVector, queryPartsVector);
        } else {
          _patterns.add(new URLPattern(url, pathPartsVector, queryPartsVector));
        }
      }
    }

    public void consolidatePatterns() {
      for (URLPattern pattern : _patterns) {
        pattern.compact();
      }
    }

    public Vector<URLPattern> getPatterns() {
      return _patterns;
    }

    void dumpPatterns() {

      Collections.sort(_patterns, new Comparator<URLPattern>() {

        @Override
        public int compare(URLPattern o1, URLPattern o2) {
          return ((Integer) o2.getMatchCount()).compareTo(o1.getMatchCount());
        }
      });

      for (URLPattern pattern : _patterns) {
        if (pattern.getMatchCount() > 1) {
          System.out.println(pattern.toString());
        }
      }
    }
  }

  public static class URLPatternMatcher {
    private Pattern pattern;

    public URLPatternMatcher(String regularExpression)
        throws PatternSyntaxException {
      pattern = Pattern.compile(regularExpression);
    }

    public boolean matches(String url) {
      url = URLPattern.normalizeQueryURL(url);
      return pattern.matcher(url).matches();
    }
  }

}
