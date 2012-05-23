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

import java.util.LinkedList;

/**
 * A basic moving average class
 * 
 * @author rana
 * 
 */
public class MovingAverage {

  private LinkedList<Double> _samples = new LinkedList<Double>();
  private int                _sampleSizeMax;
  private double             _average;

  public MovingAverage(int sampleSize) {
    _sampleSizeMax = sampleSize;
    _average = 0;
  }

  public synchronized double addSample(double sampleValue) {
    if (_samples.size() == _sampleSizeMax) {
      Double oldValue = _samples.removeFirst();
      oldValue = sampleValue;
      _samples.addLast(oldValue);
    } else {
      _samples.addLast(sampleValue);
    }
    // now compute new average value ...
    _average = 0;
    for (double value : _samples) {
      _average += value;
    }
    _average /= (double) _samples.size();

    return _average;
  }

  public synchronized double getAverage() {
    return _average;
  }
}
