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
 * Bandwidth utilization calculation helper
 * 
 * @author rana
 * 
 */
public class BandwidthUtils {

  public static class BandwidthStats {
    public double bitsPerSecond        = 0;
    public double bytesPerSecond       = 0;
    public double scaledBytesPerSecond = 0;
    public String scaledBytesUnits;
    public double scaledBitsPerSecond  = 0;
    public String scaledBitsUnits;

  }

  public static class BandwidthHistory {

    private static final int SPEED_HISTORY_SIZE = 20;
    private static final int SPEED_SAMPLE_MIN   = 150;
    private static final int STALL_START_TIME   = 5000;
    private static String    byte_units[]       = { "B/s", "KB/s", "MB/s",
                                                    "GB/s" };
    private static String    bit_units[]        = { "b/s", "Kb/s", "Mb/s",
                                                    "Gb/s" };

    int                      pos;
    int                      times[]            = new int[SPEED_HISTORY_SIZE];
    int                      bytes[]            = new int[SPEED_HISTORY_SIZE];

    int                      total_time;
    int                      total_bytes;
    int                      recent_bytes;
    long                     recent_start;
    boolean                  stalled            = false;

    private void reset() {
      pos = 0;
      recent_bytes = 0;
      times[0] = 0;
      bytes[0] = 0;
      total_time = 0;
      total_bytes = 0;
    }

    public void update(int bytesUploaded) {
      if (recent_start == 0)
        recent_start = System.currentTimeMillis();

      long currTime = System.currentTimeMillis();
      int recentAge = (int) (currTime - recent_start);
      recent_bytes += bytesUploaded;

      if (recentAge < SPEED_SAMPLE_MIN) {
        return;
      }

      if (bytesUploaded == 0) {

        if (recentAge >= STALL_START_TIME) {
          stalled = true;
          reset();
        }
        return;
      } else {
        if (stalled) {
          stalled = false;
          recentAge = 1;
        }

        total_time -= times[pos];
        total_bytes -= bytes[pos];

        times[pos] = recentAge;
        bytes[pos] = recent_bytes;
        total_time += recentAge;
        total_bytes += recent_bytes;

        recent_start = currTime;
        recent_bytes = 0;

        if (++pos == SPEED_HISTORY_SIZE)
          pos = 0;

      }

    }

    public void calcSpeed(BandwidthStats statsOut) {

      int downloadAmount = total_bytes + recent_bytes;
      int downloadTime = total_time;
      if (recent_start != 0 && !stalled) {
        downloadTime += (int) (System.currentTimeMillis() - recent_start);
      }

      if (downloadAmount != 0 && downloadTime != 0) {

        statsOut.bytesPerSecond = (double) downloadAmount
            / ((double) downloadTime / 1000.0);
        statsOut.bitsPerSecond = statsOut.bytesPerSecond * 8.0;

        if (statsOut.bytesPerSecond < 1024.0) {
          statsOut.scaledBytesPerSecond = statsOut.bytesPerSecond;
          statsOut.scaledBytesUnits = byte_units[0];
        } else if (statsOut.bytesPerSecond < 1024.0 * 1024.0) {
          statsOut.scaledBytesPerSecond = statsOut.bytesPerSecond / 1024.0;
          statsOut.scaledBytesUnits = byte_units[1];
        } else if (statsOut.bytesPerSecond < 1024.0 * 1024.0 * 1024.0) {
          statsOut.scaledBytesPerSecond = statsOut.bytesPerSecond
              / (1024.0 * 1024.0);
          statsOut.scaledBytesUnits = byte_units[2];
        } else {
          statsOut.scaledBytesPerSecond = statsOut.bytesPerSecond
              / (1024.0 * 1024.0 * 1024.0);
          statsOut.scaledBytesUnits = byte_units[3];
        }

        if (statsOut.bitsPerSecond < 1024.0) {
          statsOut.scaledBitsPerSecond = statsOut.bitsPerSecond;
          statsOut.scaledBitsUnits = bit_units[0];
        } else if (statsOut.bitsPerSecond < 1024.0 * 1024.0) {
          statsOut.scaledBitsPerSecond = statsOut.bitsPerSecond / 1024.0;
          statsOut.scaledBitsUnits = bit_units[1];
        } else if (statsOut.bitsPerSecond < 1024.0 * 1024.0 * 1024.0) {
          statsOut.scaledBitsPerSecond = statsOut.bitsPerSecond
              / (1024.0 * 1024.0);
          statsOut.scaledBitsUnits = bit_units[2];
        } else {
          statsOut.scaledBitsPerSecond = statsOut.bitsPerSecond
              / (1024.0 * 1024.0 * 1024.0);
          statsOut.scaledBitsUnits = bit_units[3];
        }
      }
    }
  }

    public static class RateLimiter {

    // desired rate limit ...
    private int              _desiredBandwidthBytes;

    // history object to collect stats ...
    private BandwidthHistory _history = new BandwidthHistory();

    // window start time ...
    private long             _windowStartTime;
    private int              _bytesAccumulated;

    public RateLimiter(int maxBitsPerSecond) {
      _desiredBandwidthBytes = maxBitsPerSecond / 8;
    }

    /**
     * checkRateLimit
     * 
     * take in bytes available and based on desired rate limit return adjusted
     * bytes available
     * 
     * @param byteAvailable
     * @return
     */
    public int checkRateLimit(int byteAvailable) {

      long currentTime = System.currentTimeMillis();

      // if first time into call or if delta between last call and current
      // window
      if (_windowStartTime == 0 || (currentTime - _windowStartTime) >= 1000) {
        _windowStartTime = currentTime;
        _bytesAccumulated = 0;
      }

      // now calculate bandwidth available
      return Math.min((_desiredBandwidthBytes - _bytesAccumulated),
          byteAvailable);
    }

    public void updateStats(int bytesInOrOut) {
      _bytesAccumulated += bytesInOrOut;
      _history.update(bytesInOrOut);
    }

    public void getStats(BandwidthStats bandwidthStats) {
      _history.calcSpeed(bandwidthStats);
    }
  }

}
