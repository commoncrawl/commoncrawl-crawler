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

package org.commoncrawl.async;

import org.commoncrawl.util.RuntimeStatsCollector;

/**
 * Stats Collection Namespace
 * 
 * @author rana
 *
 */
public final  class AsyncStats extends RuntimeStatsCollector.Namespace { 
  
  public enum  Name { 

    AsyncStats_TimersInQueue,
    AsyncStats_TimersInQueueAVG,
    AsyncStats_LoopTimerFiredCountMax,
    AsyncStats_LoopTimerFiredCountAVG,
    AsyncStats_LoopTimeinFireTimer,
    AsyncStats_LoopTimeinFireTimerAVG,
    AsyncStats_FireTimerSortTimeAVG,
    AsyncStats_LoopTimeinDNSPollAVG,
    
    AsyncStats_LoopTimeinSelectorPoll,
    AsyncStats_LoopTimeinSelectorPollAVG,

    AsyncStats_LoopTimeinSelectorPoll_Blocked,
    AsyncStats_LoopTimeinSelectorPoll_BlockedAVG,

    AsyncStats_LoopTimeinSelectorPoll_UnBlocked,
    AsyncStats_LoopTimeinSelectorPoll_UnBlockedAVG,

    AsyncStats_LoopTimeinSelectorPoll_ProcessingConnect,
    AsyncStats_LoopTimeinSelectorPoll_ProcessingConnectAVG,

    AsyncStats_LoopTimeinSelectorPoll_ProcessingReadable,
    AsyncStats_LoopTimeinSelectorPoll_ProcessingReadbleAVG,

    AsyncStats_LoopTimeinSelectorPoll_ProcessingWritable,
    AsyncStats_LoopTimeinSelectorPoll_ProcessingWritableAVG,
    
  }

  public static AsyncStats ID = new AsyncStats();
  
  private AsyncStats() {
    RuntimeStatsCollector.registerNames(this, Name.values());
  }  
  
  
}
