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

import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Sets;

/**
 * very basic (character based) shingle support
 * 
 * @author rana
 * 
 */
public class Shingle {

  public static final int CHAR_GRAM_LENGTH = 3;

  public static Set<String> shingles(String line) {

    HashSet<String> shingles = new HashSet<String>();

    for (int i = 0; i < line.length() - CHAR_GRAM_LENGTH + 1; i++) {
      // extract an ngram
      String shingle = line.substring(i, i + CHAR_GRAM_LENGTH);
      // get it's index from the dictionary
      shingles.add(shingle);
    }
    return shingles;
  }

  public static float jaccard_similarity_coeff(Set<String> shinglesA,
      Set<String> shinglesB) {
    float neumerator = Sets.intersection(shinglesA, shinglesB).size();
    float denominator = Sets.union(shinglesA, shinglesB).size();
    return neumerator / denominator;
  }
}
