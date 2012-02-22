/**
 * Copyright 2008 - CommonCrawl Foundation
 * 
 * CommonCrawl licenses this file to you under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.commoncrawl.async;

import org.commoncrawl.util.internal.RuntimeStatsCollector;

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
    
  };
    
  public static AsyncStats ID = new AsyncStats();
  
  private AsyncStats() {
    RuntimeStatsCollector.registerNames(this, Name.values());
  }  
  
  
}
