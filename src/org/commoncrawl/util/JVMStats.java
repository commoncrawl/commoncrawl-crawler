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

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Some basic JVM stats utilities
 * 
 * @author rana
 * 
 */
public class JVMStats {

  /** logging **/
  private static final Log  LOG        = LogFactory.getLog(JVMStats.class);

  private static final long MBytes     = 1024 * 1024;

  private static long       lastGCTime = 0;

  public static void dumpMemoryStats() {

    MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage memHeap = memoryMXBean.getHeapMemoryUsage();

    List<GarbageCollectorMXBean> gcBeans = ManagementFactory
        .getGarbageCollectorMXBeans();

    long gcTime = 0;
    for (GarbageCollectorMXBean gcBean : gcBeans) {
      gcTime += gcBean.getCollectionTime();
    }

    float utilizationRatio = ((float) memHeap.getUsed())
        / ((float) memHeap.getMax());

    LOG
        .info("Heap Size:" + memHeap.getUsed() / MBytes + " (MB) CommitSize:"
            + memHeap.getCommitted() / MBytes + " (MB) Max:" + memHeap.getMax()
            + " Ratio:" + utilizationRatio + " GCTime:" + (gcTime - lastGCTime)
            + "PendingFinalCnt:"
            + memoryMXBean.getObjectPendingFinalizationCount());

    lastGCTime = gcTime;
  }

  public static float getHeapUtilizationRatio() {
    MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage memHeap = memoryMXBean.getHeapMemoryUsage();

    return ((float) memHeap.getUsed()) / ((float) memHeap.getMax());
  }

}
