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
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.commoncrawl.protocol.URLFP;
import org.commoncrawl.protocol.URLFPV2;
import org.commoncrawl.rpc.base.shared.BinaryProtocol;
import org.commoncrawl.util.GoogleURL;
import org.junit.Assert;

import com.google.common.collect.ImmutableMultimap;

/**
 * 
 * @author rana
 * 
 */
public class URLUtils {

  private static final Log              LOG                  = LogFactory
                                                                 .getLog(URLUtils.class);

  /** session id normalizer **/
  private static SessionIDURLNormalizer _sessionIdNormalizer = new SessionIDURLNormalizer();

  /**
   * canonicalize url
   * 
   * @param incomingURL
   * @param stripLeadingWWW
   *          - set to true to string www. prefix from the domain if present
   * @return a canonical representation of the passed in URL that can be safely
   *         used as a replacement for the original url
   * @throws MalformedURLException
   */

  public static String canonicalizeURL(String incomingURL,
      boolean stripLeadingWWW) throws MalformedURLException {

    GoogleURL urlObject = new GoogleURL(incomingURL);

    if (!urlObject.isValid()) {
      throw new MalformedURLException("URL:" + incomingURL + " is invalid");
    }

    return canonicalizeURL(urlObject, stripLeadingWWW);
  }

  public static String canonicalizeURL(GoogleURL urlObject,
      boolean stripLeadingWWW) throws MalformedURLException {

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

    String host = urlObject.getHost();
    if (host.endsWith(".")) {
      host = host.substring(0, host.length() - 1);
    }

    if (stripLeadingWWW) {
      if (host.startsWith("www.")) {
        // ok now. one nasty hack ... :-(
        // if root name is null or root name does not equal full host name ...
        String rootName = extractRootDomainName(host);
        if (rootName == null || !rootName.equals(host)) {
          // striping the www. prefix
          host = host.substring(4);
        }
      }
    }
    urlOut.append(host);

    if (urlObject.getPort() != GoogleURL.emptyString
        && !urlObject.getPort().equals("80")) {
      urlOut.append(":");
      urlOut.append(urlObject.getPort());
    }
    if (urlObject.getPath() != GoogleURL.emptyString) {
      int indexOfSemiColon = urlObject.getPath().indexOf(';');
      if (indexOfSemiColon != -1) {
        urlOut.append(urlObject.getPath().substring(0, indexOfSemiColon));
      } else {
        urlOut.append(urlObject.getPath());
      }
    }
    if (urlObject.getQuery() != GoogleURL.emptyString) {
      urlOut.append("?");
      urlOut.append(urlObject.getQuery());
    }

    String canonicalizedURL = urlOut.toString();

    // phase 2 - remove common session id patterns
    canonicalizedURL = _sessionIdNormalizer.normalize(canonicalizedURL, "");
    
    // phase 3 - stir back in ref if #!
    if (urlObject.getRef().length() != 0 && urlObject.getRef().charAt(0) == '!') { 
      canonicalizedURL += "#" + urlObject.getRef();
    }
    return canonicalizedURL;
  }

  /**
   * www
   * 
   * @param a
   *          url
   * 
   * @return canonical representation of the url that can be used to identify
   *         all possible occurunces of the specified url. This is
   */

  /*
   * public static String canonicalizeURL(String incomingURL) throws
   * MalformedURLException { //TODO: Make this thread safe !!! synchronized
   * (URLUtils.class) { //phase 1 String normalizedURL =
   * URLNormalizer.normalizeString(incomingURL); // phase 2 normalize host name
   * and remove www
   * 
   * // get hostname URLUtils.fastGetResult hostNameLoc =
   * URLUtils.fastGetHostFromURL(normalizedURL);
   * 
   * if (hostNameLoc != null) { String hostName =
   * normalizedURL.substring(hostNameLoc.offset,hostNameLoc.offset +
   * hostNameLoc.length); String normalizedName =
   * URLUtils.normalizeHostName(hostName);
   * 
   * String newNormalizedURL = normalizedURL.substring(0,hostNameLoc.offset) +
   * normalizedName; newNormalizedURL +=
   * normalizedURL.substring(hostNameLoc.offset+hostNameLoc.length);
   * 
   * // phase 3 - remove common session id patterns normalizedURL =
   * _sessionIdNormalizer.normalize(newNormalizedURL, "");
   * 
   * // return the url return normalizedURL; } return null; } }
   */

  /**
   * get canonical url fingerprint for the the given url
   * 
   * @param urlString
   * @return canonicalized url's fingerprint id
   */
  public static long getCanonicalURLFingerprint(String incomingURL,
      boolean stripLeadingWWW) throws MalformedURLException {
    String canonicalURL = canonicalizeURL(incomingURL, stripLeadingWWW);
    if (canonicalURL != null) {
      return URLFingerprint.generate64BitURLFPrint(canonicalURL);
    }
    return 0;
  }

  /**
   * get a full url fingerprint (domain hash and url fingeprint) for the passed
   * in url
   * 
   * @param urlString
   * @return
   */
  public static URLFP getURLFPFromURL(String urlString, boolean stripLeadingWWW) {

    try {
      // canonicalize the incoming url ...
      String canonicalURL = URLUtils
          .canonicalizeURL(urlString, stripLeadingWWW);

      if (canonicalURL != null) {
        return getURLFPFromCanonicalURL(canonicalURL);
      }
    } catch (MalformedURLException e) {
    }
    return null;
  }

  public static URLFP getURLFPFromURLObject(GoogleURL urlObject) {
    try {
      // canonicalize the incoming url ...
      String canonicalURL = URLUtils.canonicalizeURL(urlObject, false);

      if (canonicalURL != null) {
        return getURLFPFromCanonicalURL(canonicalURL);
      }
    } catch (MalformedURLException e) {
    }
    return null;
  }

  public static URLFP getURLFPFromCanonicalURL(String canonicalURL) {
    // get url object
    GoogleURL urlObject = new GoogleURL(canonicalURL);

    if (urlObject.isValid()) {

      // create a url fp record
      URLFP urlFP = new URLFP();

      urlFP.setUrlHash(URLFingerprint.generate64BitURLFPrint(canonicalURL));

      String hostName = urlObject.getHost();
      String rootDomainName = URLUtils.extractRootDomainName(hostName);

      if (hostName != null && rootDomainName != null) {
        urlFP.setDomainHash(URLFingerprint.generate32BitHostFP(hostName));
        urlFP.setRootDomainHash(URLFingerprint
            .generate32BitHostFP(rootDomainName));
        return urlFP;
      }
    }
    LOG.warn("####FAILED TO CANONCALIZER INVALID URL:" + canonicalURL);
    return null;
  }

  /**
   * get URLFPV2 for a host
   * 
   */
  public static URLFPV2 getURLFPV2FromHost(String host) {
    return getURLFPV2FromURL("http://" + host + "/");
  }

  /**
   * get new urlfp from urstring... always string leading www
   * 
   * @param urlString
   * @return
   */
  public static URLFPV2 getURLFPV2FromURL(String urlString) {

    try {
      // canonicalize the incoming url ...
      String canonicalURL = URLUtils.canonicalizeURL(urlString, false);

      if (canonicalURL != null) {
        return getURLFPV2FromCanonicalURL(canonicalURL);
      }
    } catch (MalformedURLException e) {
    }
    return null;
  }

  public static URLFPV2 getURLFPV2FromURLObject(GoogleURL urlObject) {
    try {
      // canonicalize the incoming url ...
      String canonicalURL = URLUtils.canonicalizeURL(urlObject, false);

      if (canonicalURL != null) {
        return getURLFPV2FromCanonicalURL(canonicalURL);
      }
    } catch (MalformedURLException e) {
    }
    return null;
  }

  public static URLFPV2 getURLFPV2FromCanonicalURL(String canonicalURL) {

    // create a url fp record
    URLFPV2 urlFP = new URLFPV2();

    urlFP.setUrlHash(URLFingerprint.generate64BitURLFPrint(canonicalURL));

    String hostName = fastGetHostFromURL(canonicalURL);
    String rootDomainName = null;

    if (hostName != null)
      rootDomainName = URLUtils.extractRootDomainName(hostName);

    if (hostName != null && rootDomainName != null) {
      // ok we want to strip the leading www. if necessary
      if (hostName.startsWith("www.")) {
        // ok now. one nasty hack ... :-(
        // if root name does not equal full host name ...
        if (!rootDomainName.equals(hostName)) {
          // strip the www. prefix
          hostName = hostName.substring(4);
        }
      }
      urlFP.setDomainHash(FPGenerator.std64.fp(hostName));
      urlFP.setRootDomainHash(FPGenerator.std64.fp(rootDomainName));
      return urlFP;
    }
    return null;
  }

  public static String fastGetHostFromURL(String urlString) {

    int hostStart = urlString.indexOf(":");
    if (hostStart != -1) {

      hostStart++;

      int urlLength = urlString.length();

      while (hostStart < urlString.length()) {
        char nextChar = urlString.charAt(hostStart);
        if (nextChar != '/' && nextChar != '\\' && nextChar != '\n'
            && nextChar != '\r' && nextChar != '\t' && nextChar != ' ') {
          break;
        }
        hostStart++;
      }

      if (hostStart < urlLength) {

        int hostEnd = hostStart + 1;

        while (hostEnd < urlLength) {
          char nextChar = urlString.charAt(hostEnd);
          if (nextChar == '/' || nextChar == '?' || nextChar == ';'
              || nextChar == '#')
            break;
          hostEnd++;
        }

        int indexOfAt = urlString.indexOf("@", hostStart);
        if (indexOfAt != -1 && indexOfAt < hostEnd) {
          hostStart = indexOfAt + 1;
        }

        String host = urlString.substring(hostStart, hostEnd);

        int hostLength = host.length();
        int colonEnd = host.indexOf(":");
        if (colonEnd != -1) {
          hostLength = colonEnd;
          host = urlString.substring(hostStart, hostStart + hostLength);
        }

        GoogleURL urlObject = new GoogleURL("http://" + host);

        if (urlObject.isValid()) {
          return urlObject.getHost();
        }
      }
      /*
       * host = host.replaceAll("((%20)|\\s)", "");
       * 
       * if (!invalidDomainCharactersRegEx.matcher(host).matches()) {
       * 
       * if (host.length() >= 1) { if (host.charAt(0) >= '0' && host.charAt(0)
       * <= '9') { if (numericOnly.matcher(host).matches()) { try { int
       * ipAddress = (int) Long.parseLong(host); return
       * IPAddressUtils.IntegerToIPAddressString(ipAddress); } catch
       * (NumberFormatException e) { return null; } } } } return host; }
       */

    }
    return null;
  }

  public static class fastGetResult {

    public fastGetResult(int offset, int length) {
      this.offset = offset;
      this.length = length;
    }

    public int offset;
    public int length;
  }

  public static fastGetResult fastGetHostFromTextURL(byte[] charStream,
      int offset, int length) {

    char schemeEnd[] = { ':', '/', '/' };
    char at[] = { '@' };
    char slash[] = { '/' };
    char questionMark[] = { '?' };
    char hashMark[] = { '#' };
    char colon[] = { ':' };

    int indexOfSchemeEnd = indexOf(charStream, offset, length, schemeEnd, 0, 3,
        0);

    if (indexOfSchemeEnd != -1) {
      int hostStart = indexOfSchemeEnd + 3;
      int lengthRemaining = length - hostStart;
      int hostEnd = indexOf(charStream, offset + hostStart, lengthRemaining,
          slash, 0, 1, 0);
      if (hostEnd == -1) {
        hostEnd = indexOf(charStream, offset + hostStart, lengthRemaining,
            questionMark, 0, 1, 0);
      }
      if (hostEnd == -1) {
        hostEnd = indexOf(charStream, offset + hostStart, lengthRemaining,
            hashMark, 0, 1, 0);
      }
      if (hostEnd != -1) {
        lengthRemaining = hostEnd;
      }
      int indexOfColon = indexOf(charStream, offset + hostStart,
          lengthRemaining, colon, 0, 1, 0);

      if (indexOfColon != -1) {
        lengthRemaining = indexOfColon;
      }
      int indexOfAt = indexOf(charStream, offset + hostStart, lengthRemaining,
          at, 0, 1, 0);
      if (indexOfAt != -1) {
        lengthRemaining = lengthRemaining - (indexOfAt + 1);
        hostStart = hostStart + indexOfAt + 1;
      }

      return new fastGetResult(hostStart, lengthRemaining);
    }
    return null;
  }

  static int indexOf(byte[] source, int sourceOffset, int sourceCount,
      char[] target, int targetOffset, int targetCount, int fromIndex) {
    if (fromIndex >= sourceCount) {
      return (targetCount == 0 ? sourceCount : -1);
    }
    if (fromIndex < 0) {
      fromIndex = 0;
    }
    if (targetCount == 0) {
      return fromIndex;
    }

    char first = target[targetOffset];
    int max = sourceOffset + (sourceCount - targetCount);

    for (int i = sourceOffset + fromIndex; i <= max; i++) {
      /* Look for first character. */
      if (source[i] != first) {
        while (++i <= max && source[i] != first)
          ;
      }

      /* Found first character, now look at the rest of v2 */
      if (i <= max) {
        int j = i + 1;
        int end = j + targetCount - 1;
        for (int k = targetOffset + 1; j < end && source[j] == target[k]; j++, k++)
          ;

        if (j == end) {
          /* Found whole string. */
          return i - sourceOffset;
        }
      }
    }
    return -1;
  }

  public static String invertHostName(String hostNameIn) {
    StringBuffer hostNameOut = new StringBuffer();

    char tokens[] = hostNameIn.toCharArray();
    int lastScanStart = hostNameIn.length() - 1;
    int currentIndex = lastScanStart;

    while (currentIndex != -1) {
      if (tokens[currentIndex] == '.') {
        if (lastScanStart - currentIndex != 0) {
          hostNameOut.append(tokens, currentIndex + 1, lastScanStart
              - currentIndex);
          if (currentIndex != 0)
            hostNameOut.append('.');
        }
        lastScanStart = currentIndex - 1;
      } else if (currentIndex == 0) {
        if (lastScanStart - currentIndex + 1 != 0) {
          hostNameOut.append(tokens, 0, lastScanStart + 1);
        }
      }
      currentIndex--;
    }

    return hostNameOut.toString();
  }

  public static int invertHostNameFast(byte[] tokens, int offset, int length,
      byte[] destinationBuffer) {

    int lastScanStart = offset + length - 1;
    int currentIndex = lastScanStart;
    int destinationOffset = 0;

    while (currentIndex >= offset) {
      if (tokens[currentIndex] == '.') {
        if (lastScanStart - currentIndex != 0) {
          System.arraycopy(tokens, currentIndex + 1, destinationBuffer,
              destinationOffset, lastScanStart - currentIndex);
          destinationOffset += (lastScanStart - currentIndex);
          if (currentIndex != 0) {
            destinationBuffer[destinationOffset++] = '.';
          }
        }
        lastScanStart = currentIndex - 1;
      } else if (currentIndex == offset) {
        if (lastScanStart - currentIndex + 1 != 0) {
          System.arraycopy(tokens, offset, destinationBuffer,
              destinationOffset, (lastScanStart - currentIndex + 1));
          destinationOffset += (lastScanStart - currentIndex + 1);
        }
      }
      currentIndex--;
    }

    return destinationOffset;
  }

  public static String normalizeHostName(String hostName,
      boolean stripLeadingWWW) {

    if (ipAddressRegEx.matcher(hostName).matches()) {
      return hostName;
    }

    // we are going to normalize it first , so make a copy
    String normalizedHostName = hostName.toLowerCase();
    // next check for trailing .
    while (normalizedHostName.endsWith(".")) {
      normalizedHostName = normalizedHostName.substring(0, normalizedHostName
          .length() - 1);
    }
    while (normalizedHostName.startsWith(".")) {
      normalizedHostName = normalizedHostName.substring(1);
    }

    normalizedHostName = normalizedHostName.replaceAll("((%20)|\\s)", "");

    if (!invalidDomainCharactersRegEx.matcher(normalizedHostName).matches()) {

      if (stripLeadingWWW) {
        String rootName = extractRootDomainName(normalizedHostName);

        if (rootName != null) {
          String subDomain = "";

          if (rootName.length() != normalizedHostName.length()) {
            subDomain = normalizedHostName.substring(0, normalizedHostName
                .length()
                - rootName.length());

            if (subDomain.startsWith("www.")) {
              normalizedHostName = normalizedHostName.substring(4);
            }
          }
        }
      }
      return normalizedHostName;
    }
    return null;
  }

  public static String getHostNameFromURLKey(Text key) {

    fastGetResult result = fastGetHostFromTextURL(key.getBytes(), 0, key
        .getLength());

    if (result != null && result.length != 0) {
      String hostName = new String(key.getBytes(), result.offset, result.length);
      return hostName;
    }
    return null;
  }

  private static void testURL(String url) {
    String hostName = getHostNameFromURLKey(new Text(url));
    try {
      URL urlObject = new URL(url);
      if (hostName == null) {
        Assert.assertTrue(urlObject.getHost().length() == 0);
      } else {
        Assert.assertTrue(urlObject.getHost().equals(hostName));
      }
    } catch (MalformedURLException e) {
      Assert.assertTrue(getHostNameFromURLKey(new Text(url)) == null);
    }
  }

  private static int findTLDNameEndLength(byte[] stream, int offset, int length) {
    boolean foundTLDStartMarker = false;
    int i = 0;
    for (i = 0; i < length; ++i) {
      if (stream[offset + i] == '!' && !foundTLDStartMarker) {
        foundTLDStartMarker = true;
      } else if (stream[offset + i] == '.' && foundTLDStartMarker) {
        break;
      }
    }
    return i;
  }

  private static ImmutableMultimap<String, String> gTLDMultiMap = null;

  private static int getNextTokenPos(String candidate, int startPos) {
    while (startPos > 0) {
      if (candidate.charAt(startPos - 1) == '.') {
        break;
      }
      --startPos;
    }
    return startPos;
  }

  public static boolean isTLDStopWord(String candidate) {
    return TLDNamesCollection.getSecondaryNames(candidate).size() != 0;
  }

  private static String buildRootNameString(String candidateString,
      String[] parts, int rootNameIndex) {
    int partsToInclude = parts.length - rootNameIndex;
    int dotsToInclude = partsToInclude - 1;

    // initial root name length is dot count
    int rootNameLength = dotsToInclude;
    for (int i = rootNameIndex; i < parts.length; ++i) {
      rootNameLength += parts[i].length();
    }
    return candidateString.substring(candidateString.length() - rootNameLength);
  }

  public static String extractTLDName(String candidate) {

    // special case for ip addresses
    if (ipAddressRegEx.matcher(candidate).matches()) {
      return "inaddr-arpa.arpa";
    }

    if (candidate.endsWith(".")) {
      candidate = candidate.substring(0, candidate.length() - 1);
    }
    if (candidate.startsWith("*") && candidate.length() > 1) {
      candidate = candidate.substring(1);
    }
    if (candidate.length() != 0) {
      if (!invalidDomainCharactersRegEx.matcher(candidate).find()) {
        String parts[] = candidate.split("\\.");
        if (parts.length >= 2) {
          Collection<String> secondaryNames = TLDNamesCollection
              .getSecondaryNames(parts[parts.length - 1]);

          if (secondaryNames.size() != 0) {
            // see if second to last part matches secondary names for this TLD
            // or there is a wildcard expression for secondary name in rule set
            if (secondaryNames.contains(parts[parts.length - 2])
                || secondaryNames.contains("*")) {
              // ok secondary part is potentianlly part of secondary name ...

              // check to see the part in not explicitly excluded ...
              if (secondaryNames.contains("!" + parts[parts.length - 2])) {
                // in this case, second to last part is NOT part of secondary
                // name
                return buildRootNameString(candidate, parts, parts.length - 1);
              } else {
                // otherwise, TLD contains 2 parts
                return buildRootNameString(candidate, parts, parts.length - 2);
              }
            }
            // ok second to last part does not match set of known secondary
            // names
            else {
              // make a wildcard string matching secondary name
              String extendedWildcard = "*." + parts[parts.length - 2];
              // if match, then this implies secondary name has two components
              if (secondaryNames.contains(extendedWildcard)) {

                if (parts.length >= 3) {
                  // this implies that there must be four parts to the name to
                  // extract root
                  // unless exlusion rule applies
                  String exclusionRule2 = "!" + parts[parts.length - 3] + "."
                      + parts[parts.length - 2];

                  // if exclusion rule is present ...
                  if (secondaryNames.contains(exclusionRule2)) {
                    // third part is NOT part of secondary name
                    return buildRootNameString(candidate, parts,
                        parts.length - 2);
                  } else {
                    // ok extended wildcard matched. last 3 parts are part of
                    // the TLD
                    if (parts.length >= 4) {
                      return buildRootNameString(candidate, parts,
                          parts.length - 3);
                    }
                  }
                }
              }
              // at this point ... if the null name exists ...
              else if (secondaryNames.contains("")) {
                // only last item is part of TLD
                return buildRootNameString(candidate, parts, parts.length - 1);
              }
            }
          }
        }
      }
    }
    return null;
  }

  public static String extractRootDomainName(String candidate) {

    // special case for ip addresses
    if (ipAddressRegEx.matcher(candidate).matches()) {
      return candidate;
    }

    if (candidate.endsWith(".")) {
      candidate = candidate.substring(0, candidate.length() - 1);
    }
    if (candidate.startsWith("*") && candidate.length() > 1) {
      candidate = candidate.substring(1);
    }
    if (candidate.length() != 0) {
      if (!invalidDomainCharactersRegEx.matcher(candidate).find()) {
        String parts[] = candidate.split("\\.");
        if (parts.length >= 2) {
          Collection<String> secondaryNames = TLDNamesCollection
              .getSecondaryNames(parts[parts.length - 1]);

          if (secondaryNames.size() != 0) {
            // see if second to last part matches secondary names for this TLD
            // or there is a wildcard expression for secondary name in rule set
            if (secondaryNames.contains(parts[parts.length - 2])
                || secondaryNames.contains("*")) {
              // ok secondary part is potentianlly part of secondary name ...

              // check to see the part in not explicitly excluded ...
              if (secondaryNames.contains("!" + parts[parts.length - 2])) {
                // in this case, this is an explicit override. second to last
                // part is NOT part of secondary name
                return buildRootNameString(candidate, parts, parts.length - 2);
              } else {
                // otherwise, we need at least three parts
                if (parts.length >= 3) {
                  return buildRootNameString(candidate, parts, parts.length - 3);
                }
              }
            }
            // ok second to last part does not match set of known secondary
            // names
            else {
              // make a wildcard string matching secondary name
              String extendedWildcard = "*." + parts[parts.length - 2];
              // if match, then this implies secondary name has two components
              if (secondaryNames.contains(extendedWildcard)) {

                if (parts.length >= 3) {
                  // this implies that there must be four parts to the name to
                  // extract root
                  // unless exlusion rule applies
                  String exclusionRule2 = "!" + parts[parts.length - 3] + "."
                      + parts[parts.length - 2];

                  // if exclusion rule is present ...
                  if (secondaryNames.contains(exclusionRule2)) {
                    // third part is NOT part of secondary name
                    return buildRootNameString(candidate, parts,
                        parts.length - 3);
                  } else {
                    // ok extended wildcard matched. we need 4 parts minimum
                    if (parts.length >= 4) {
                      return buildRootNameString(candidate, parts,
                          parts.length - 4);
                    }
                  }
                }
              }
              // at this point ... if the null name exists ...
              else if (secondaryNames.contains("")) {
                // return second part as root name
                return buildRootNameString(candidate, parts, parts.length - 2);
              }
            }
          }
        }
      }
    }
    return null;
  }

  /** The maximum length of a Name */
  private static final int MAXNAME                      = 255;

  /** The maximum length of a label a Name */
  private static final int MAXLABEL                     = 63;

  /** The maximum number of labels in a Name */
  private static final int MAXLABELS                    = 128;

  static Pattern           invalidDomainCharactersRegEx = Pattern
                                                            .compile("[^0-9a-z\\-\\._]");
  static Pattern           ipAddressRegEx               = Pattern
                                                            .compile("^[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+$");
  static Pattern           numericOnly                  = Pattern
                                                            .compile("[0-9]*$");

  public static boolean isValidDomainName(String name) {

    // check for invalid length (max 255 characters)
    if (name.length() > MAXNAME) {
      return false;
    }

    String candidate = name.toLowerCase();

    // check to see if this is an ip address
    if (ipAddressRegEx.matcher(candidate).matches()) {
      return true;
    }

    // check for invalid characters
    if (invalidDomainCharactersRegEx.matcher(candidate).matches()) {
      return false;
    }
    // split into parts
    String[] parts = name.split("\\.");

    // check for max labels constraint
    if (parts.length > MAXLABELS) {
      return false;
    }
    return extractRootDomainName(candidate) != null;
  }

  public static String invertAndMarkTLDNameStartInString(String hostName) {
    // and invert it ...
    hostName = URLUtils.invertHostName(hostName);
    // LOG.info("Inverted HostName for Key:" + key.toString() +":" + hostName);
    // create a buffer
    StringBuffer tempBuffer = new StringBuffer(hostName.length());
    // now walk it skipping tld names
    StringTokenizer tokenizer = new StringTokenizer(hostName, ".");
    boolean foundTLDName = false;
    while (tokenizer.hasMoreElements()) {
      char delimiterToUse = '.';

      String token = tokenizer.nextToken();
      if (!foundTLDName) {
        if (!URLUtils.isTLDStopWord(token)) {
          foundTLDName = true;
          delimiterToUse = '!';
        }
      }
      if (tempBuffer.length() != 0)
        tempBuffer.append(delimiterToUse);
      tempBuffer.append(token);
    }
    return tempBuffer.toString();
  }

  public static int findTLDNameEndLengthInMarkedString(String markedString) {
    boolean foundTLDStartMarker = false;
    int i = 0;
    for (i = 0; i < markedString.length(); ++i) {
      if (markedString.charAt(i) == '!' && !foundTLDStartMarker) {
        foundTLDStartMarker = true;
      } else if (markedString.charAt(i) == '.' && foundTLDStartMarker) {
        break;
      }
    }
    return i;
  }

  public static int findTLDNameEndLengthInMarkedStream(byte[] stream,
      int offset, int length) {
    boolean foundTLDStartMarker = false;
    int i = 0;
    for (i = 0; i < length; ++i) {
      if (stream[offset + i] == '!' && !foundTLDStartMarker) {
        foundTLDStartMarker = true;
      } else if (stream[offset + i] == '.' && foundTLDStartMarker) {
        break;
      }
    }
    return i;
  }

  private static void testURLNameInversion(String name) {
    System.out.println("Inverting name:" + name + " result:"
        + invertHostName(name));
  }

  private static void testTLDNameDetection(String name) {
    byte[] bytes = null;
    try {
      bytes = name.getBytes("UTF8");
    } catch (UnsupportedEncodingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    int nameLen = findTLDNameEndLength(bytes, 0, bytes.length);
    System.out.println("TLD Name for:" + name + " is:"
        + name.substring(0, nameLen));
  }

  private static void testHostNameInvertFast(int offset, String hostName) {
    byte[] tokenArray = hostName.getBytes();
    byte[] duplicate = new byte[tokenArray.length + offset + 4];
    System.arraycopy(tokenArray, 0, duplicate, offset, tokenArray.length);
    byte[] destinationArray = new byte[tokenArray.length + 4];
    int destinationBufferSize = invertHostNameFast(duplicate, offset,
        tokenArray.length, destinationArray);

    System.out.println("Inverted:" + hostName + " Produced:"
        + new String(destinationArray, 0, destinationBufferSize));
  }

  static String replicateNameNormalization(String hostNameIn) {
    // and invert it ...
    String hostName = URLUtils.invertHostName(hostNameIn);
    // LOG.info("Inverted HostName for Key:" + key.toString() +":" + hostName);
    // create a buffer
    StringBuffer tempBuffer = new StringBuffer(hostName.length());
    // now walk it skipping tld names
    StringTokenizer tokenizer = new StringTokenizer(hostName, ".");
    boolean foundTLDName = false;
    while (tokenizer.hasMoreElements()) {
      char delimiterToUse = '.';

      String token = tokenizer.nextToken();
      if (!foundTLDName) {
        if (!URLUtils.isTLDStopWord(token)) {
          foundTLDName = true;
          delimiterToUse = '!';
        }
      }
      tempBuffer.append(delimiterToUse);
      tempBuffer.append(token);
    }
    hostName = tempBuffer.toString();

    return hostName;
  }

  static class CanonicalizationTestCase {
    String originalURL;
    String expectedURL;

    CanonicalizationTestCase(String originalURL, String expectedURL) {
      this.originalURL = originalURL;
      this.expectedURL = expectedURL;
    }

    void validate() {
      try {
        String resultingURL = canonicalizeURL(originalURL, false);
        Assert.assertEquals(resultingURL, expectedURL);
      } catch (MalformedURLException e) {
        if (expectedURL != null) {
          Assert.assertTrue(false);
        }
      }
    }

  }

  public static class URLFPV2RawComparator implements RawComparator<URLFPV2> {

    DataInputBuffer keyReader1 = new DataInputBuffer();
    DataInputBuffer keyReader2 = new DataInputBuffer();
    URLFPV2         fp1        = new URLFPV2();
    URLFPV2         fp2        = new URLFPV2();

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      keyReader1.reset(b1, s1, l1);
      keyReader2.reset(b2, s2, l2);

      try {
        // read first byte of both streams
        int s1FirstByte = keyReader1.read();
        int s2FirstByte = keyReader2.read();

        boolean s1IsOldFormat = false;
        boolean s2IsOldFormat = false;

        if (s1FirstByte == 0 || s1FirstByte == -1) {
          s1IsOldFormat = true;
        }

        if (s2FirstByte == 0 || s2FirstByte == -1) {
          s2IsOldFormat = true;
        }

        keyReader1.skip(1); // skip next byte
        fp1.setDomainHash(WritableUtils.readVLong(keyReader1));
        keyReader2.skip(1); // skip next byte
        fp2.setDomainHash(WritableUtils.readVLong(keyReader2));

        int result = ((Long) fp1.getDomainHash())
            .compareTo(fp2.getDomainHash());

        if (result == 0) {
          keyReader1.skip((s1IsOldFormat) ? 2 : 1); // id field only
          fp1.setUrlHash(WritableUtils.readVLong(keyReader1));

          keyReader2.skip((s2IsOldFormat) ? 2 : 1); // id field only
          fp2.setUrlHash(WritableUtils.readVLong(keyReader2));

          result = ((Long) fp1.getUrlHash()).compareTo(fp2.getUrlHash());
        }
        return result;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public int compare(URLFPV2 fp1, URLFPV2 fp2) {
      int result = ((Long) fp1.getDomainHash()).compareTo(fp2.getDomainHash());

      if (result == 0) {
        result = ((Long) fp1.getUrlHash()).compareTo(fp2.getUrlHash());
      }
      return result;
    }

    static void validateComparator() {
      URLFPV2 fp1 = new URLFPV2();
      URLFPV2 fp2 = new URLFPV2();
      URLFPV2 fp3 = new URLFPV2();
      URLFPV2 fp4 = new URLFPV2();

      fp1.setDomainHash(1L);
      fp2.setDomainHash(1L);
      fp3.setDomainHash(2L);
      fp4.setDomainHash(2L);

      fp1.setUrlHash(10L);
      fp2.setUrlHash(9L);
      fp3.setUrlHash(18L);
      fp4.setUrlHash(20L);

      DataOutputBuffer buffer1 = new DataOutputBuffer();
      DataOutputBuffer buffer2 = new DataOutputBuffer();
      DataOutputBuffer buffer3 = new DataOutputBuffer();
      DataOutputBuffer buffer4 = new DataOutputBuffer();

      URLFPV2RawComparator comparator = new URLFPV2RawComparator();

      Assert.assertTrue(comparator.compare(fp1, fp2) == 1);
      Assert.assertTrue(comparator.compare(fp2, fp1) == -1);
      Assert.assertTrue(comparator.compare(fp1, fp3) == -1);
      Assert.assertTrue(comparator.compare(fp3, fp1) == 1);
      Assert.assertTrue(comparator.compare(fp4, fp3) == 1);
      Assert.assertTrue(comparator.compare(fp3, fp4) == -1);

      try {
        BinaryProtocol.DEFAULT_PROTOCOL_ENCODING_MODE = BinaryProtocol.FIELD_ID_ENCODING_MODE_SHORT;
        fp1.write(buffer1);
        BinaryProtocol.DEFAULT_PROTOCOL_ENCODING_MODE = BinaryProtocol.FIELD_ID_ENCODING_MODE_VINT;
        fp2.write(buffer2);
        BinaryProtocol.DEFAULT_PROTOCOL_ENCODING_MODE = BinaryProtocol.FIELD_ID_ENCODING_MODE_SHORT;
        fp3.write(buffer3);
        BinaryProtocol.DEFAULT_PROTOCOL_ENCODING_MODE = BinaryProtocol.FIELD_ID_ENCODING_MODE_VINT;
        fp4.write(buffer4);

      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

      Assert.assertTrue(comparator.compare(buffer1.getData(), 0, buffer1
          .getLength(), buffer2.getData(), 0, buffer2.getLength()) == 1);
      Assert.assertTrue(comparator.compare(buffer2.getData(), 0, buffer2
          .getLength(), buffer1.getData(), 0, buffer1.getLength()) == -1);
      Assert.assertTrue(comparator.compare(buffer1.getData(), 0, buffer1
          .getLength(), buffer3.getData(), 0, buffer3.getLength()) == -1);
      Assert.assertTrue(comparator.compare(buffer3.getData(), 0, buffer3
          .getLength(), buffer1.getData(), 0, buffer1.getLength()) == 1);
      Assert.assertTrue(comparator.compare(buffer3.getData(), 0, buffer3
          .getLength(), buffer4.getData(), 0, buffer4.getLength()) == -1);
      Assert.assertTrue(comparator.compare(buffer4.getData(), 0, buffer4
          .getLength(), buffer3.getData(), 0, buffer3.getLength()) == 1);

    }

  }

  public static CanonicalizationTestCase[] testCases = {
      new CanonicalizationTestCase("http://foo.bar.com.#?",
          "http://foo.bar.com/"),
      new CanonicalizationTestCase(
          "http://foo.bar.com./;msg1234FDF FDFDFDF FDFD?param1=test",
          "http://foo.bar.com/?param1=test"),
      new CanonicalizationTestCase(
          "http://foo.bar.com./;msg1234FDF FDFDFDF FDFD", "http://foo.bar.com/"),
      new CanonicalizationTestCase(
          "http://foo.bar.com/subpath/;msg1234FDF FDFDFDF FDFD",
          "http://foo.bar.com/subpath/"),
      new CanonicalizationTestCase(
          "http://foo.bar.com/subpath/;msg1234FDF FDFDFDF FDFD?param=1",
          "http://foo.bar.com/subpath/?param=1"),
      new CanonicalizationTestCase("http://foo.bar.com.#REF=24242",
          "http://foo.bar.com/"),
      new CanonicalizationTestCase(
          "http://www.lakeshorelearning.com/order/onlineOrder.jsp;jsessionid=KxMMpRGgPpC1ktZ1pJJCZF1MmmFxZHPnyrNJhBmWJGHkhcL5Hd4p!-617247554!NONE?FOLDER%3C%3Efolder_id=2534374302096766&ASSORTMENT%3C%3East_id=1408474395181113&bmUID=1257311436941",
          "http://www.lakeshorelearning.com/order/onlineOrder.jsp?FOLDER%3C%3Efolder_id=2534374302096766&ASSORTMENT%3C%3East_id=1408474395181113&bmUID=1257311436941"),
      new CanonicalizationTestCase(
          "http://www.emeraldinsight.com/Insight/menuNavigation.do;jsessionid=A17FC93E864C2F8B3709F63558BA69DB?hdAction=InsightHome",
          "http://www.emeraldinsight.com/Insight/menuNavigation.do?hdAction=InsightHome")

                                                     };

  public static void validatateCanonicalization() {
    for (CanonicalizationTestCase testCase : testCases) {
      testCase.validate();
    }
  }

  public static void main(String[] args) {

    URLFPV2RawComparator.validateComparator();

    URLFPV2 fingerprint = getURLFPV2FromURL("http://www.gmail.fr/");
    URLFPV2 fingerprint2 = getURLFPV2FromURL("http://gmail.fr/");

    Assert.assertTrue(fingerprint.getDomainHash() == fingerprint2
        .getDomainHash());

    testRootDomainExtractor();
    Assert.assertTrue(isValidDomainName("192.168.0.1"));
    Assert.assertFalse(isValidDomainName("192.168.0.1.1"));
    Assert.assertTrue(URLUtils.normalizeHostName("192.168.0.1", false).equals(
        "192.168.0.1"));

    validatateCanonicalization();
  }

  private static void testRootDomainExtractor() {

    System.out.println(extractRootDomainName("www.ret.gov.au") + ","
        + extractTLDName("www.ret.gov.au"));
    System.out.println(extractRootDomainName("www.jobshop.ro") + ","
        + extractTLDName("www.jobshop.ro"));
    System.out.println(extractRootDomainName("www.ne.jp") + ","
        + extractTLDName("www.ne.jp"));
    System.out.println(extractRootDomainName("foo.ac.jp") + ","
        + extractTLDName("foo.ac.jp"));
    System.out.println(extractRootDomainName("aichi.jp") + ","
        + extractTLDName("aichi.jp"));
    System.out.println(extractRootDomainName("bochi.aichi.jp") + ","
        + extractTLDName("bochi.aichi.jp"));
    System.out.println(extractRootDomainName("more.bochi.aichi.jp") + ","
        + extractTLDName("more.bochi.aichi.jp"));
    System.out.println(extractRootDomainName("metro.tokyo.jp") + ","
        + extractTLDName("metro.tokyo.jp"));
    System.out.println(extractRootDomainName("fluff.metro.tokyo.jp") + ","
        + extractTLDName("fluff.metro.tokyo.jp"));
    System.out.println(extractRootDomainName("www.pref.hokkaido.jp") + ","
        + extractTLDName("www.pref.hokkaido.jp"));
    System.out.println(extractRootDomainName("www.subdomain.pref2.hokkaido.jp")
        + "," + extractTLDName("www.subdomain.pref2.hokkaido.jp"));
    System.out.println(extractRootDomainName("gigaom.com") + ","
        + extractTLDName("gigaom.com"));
    System.out.println(extractRootDomainName("www.gigaom.com.cn") + ","
        + extractTLDName("www.gigaom.com.cn"));
    System.out.println(extractRootDomainName("www.foobar.idf.il") + ","
        + extractTLDName("www.foobar.idf.il"));
    System.out.println(extractRootDomainName("192.168.0.1") + ","
        + extractTLDName("192.168.0.1"));

    Assert
        .assertTrue(extractRootDomainName(".gigaom.com").equals("gigaom.com"));
    Assert.assertTrue(extractRootDomainName("*.gigaom.com")
        .equals("gigaom.com"));
    Assert.assertTrue(extractRootDomainName("www.gigaom.com").equals(
        "gigaom.com"));
    Assert.assertTrue(extractRootDomainName("foobar.foo.cn").equals("foo.cn"));
    Assert.assertTrue(extractRootDomainName("foobar.google.com.cn").equals(
        "google.com.cn"));
    Assert.assertTrue(extractRootDomainName("google.com.cn").equals(
        "google.com.cn"));
    Assert.assertTrue(extractRootDomainName("cn") == null);
    Assert.assertTrue(extractRootDomainName("ab.ca") == null);
    Assert.assertTrue(extractRootDomainName("somedomain.ab.ca").equals(
        "somedomain.ab.ca"));
    Assert.assertTrue(extractRootDomainName("www.somedomain.ab.ca").equals(
        "somedomain.ab.ca"));
    Assert.assertTrue(extractRootDomainName("www.somedomain .ab.ca") == null);

  }

  private static void utilsTest() throws Exception {

    /*
     * testHostNameInvertFast(4,"www.google.com");
     * testHostNameInvertFast(4,"google.com.");
     * 
     * System.out.println(invertHostName("news.bbc.co.uk."));
     * System.out.println(invertHostName("www.zubia-alam.blogspot.com")+".");
     * System.out.println(invertHostName("zubia-alam.blogspot.com")+".");
     * System.out.println("compareTo returned:" +
     * (invertHostName("zubia-alam.blogspot.com"
     * )+".").compareTo((invertHostName("zubia-alam.blogspot.com"))));
     * System.out.println("compareTo returned:" +
     * "x-factor-e.".compareTo("x-factor."));
     * 
     * testHostNameInvertFast(4,"www.google.com");
     * testHostNameInvertFast(4,"google.com.");
     * testHostNameInvertFast(4,".google.com");
     * testHostNameInvertFast(4,"google.com.");
     * 
     * testHostNameInvertFast(4,"www.google.com");
     * testHostNameInvertFast(4,"google.com.");
     * testHostNameInvertFast(4,".google.com");
     * testHostNameInvertFast(4,"google.com.");
     */

    Assert.assertTrue(normalizeHostName(".gigaom. com", true).equals(
        "gigaom.com"));
    Assert.assertTrue(normalizeHostName("%20gigaom.com", true).equals(
        "gigaom.com"));
    Assert.assertTrue(normalizeHostName("www.gigaom.com", true).equals(
        "gigaom.com"));
    Assert.assertTrue(normalizeHostName("www.gigaom.com.", true).equals(
        "gigaom.com"));
    Assert.assertTrue(normalizeHostName("www.gigaom.com", false).equals(
        "www.gigaom.com"));
    Assert.assertTrue(normalizeHostName("www.gigaom.com.", false).equals(
        "www.gigaom.com"));
    Assert.assertTrue(normalizeHostName(".com.", true).equals("com"));
    Assert.assertTrue(normalizeHostName("..gigaom.com..", true).equals(
        "gigaom.com"));

    Assert.assertTrue(normalizeHostName("aisa.org.af.", true).equals(
        "aisa.org.af"));

    /*
     * testTLDNameDetection("com!google.www");
     * testTLDNameDetection("au.com!google.www");
     * testTLDNameDetection("com.google.www");
     * 
     * testURLNameInversion("www.google.com");
     * testURLNameInversion("google.com."); testURLNameInversion(".google.com");
     * testURLNameInversion("google.com.");
     * 
     * testURLNameInversion(invertHostName("www.google.com"));
     * testURLNameInversion(invertHostName("google.com."));
     * testURLNameInversion(invertHostName(".google.com"));
     * testURLNameInversion(invertHostName("google.com."));
     * 
     * testURL("http://www.google.com/"); testURL("http://google.com:8080/");
     * testURL("http://google.com:8080"); testURL("http://google.com");
     * testURL("http://ahad@google.com");
     * testURL("http://ahad:password@google.com");
     * testURL("http://ahad:password@google.com/"); testURL("http:///");
     */
  }

}
