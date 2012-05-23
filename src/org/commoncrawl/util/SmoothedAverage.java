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
 * Basic Smoothed Average class
 * 
 * @author rana
 * 
 */
public class SmoothedAverage {

  private double _alpha;
  private int    _observationCount;
  private double _smoothedValue;

  public SmoothedAverage(double alpha) {
    _alpha = alpha;
    _observationCount = 0;
    _smoothedValue = 0;
  }

  public synchronized double addSample(double sampleValue) {
    if (_observationCount++ == 0) {
      _smoothedValue = sampleValue;
    } else {
      _smoothedValue = (_alpha * sampleValue) + ((1 - _alpha) * _smoothedValue);
    }
    return _smoothedValue;
  }

  public synchronized double getAverage() {
    return _smoothedValue;
  }

}
