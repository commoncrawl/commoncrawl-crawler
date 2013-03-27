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

/**
 * A Timer that can fire inside of an event loop
 * 
 * @author rana
 *
 */
public final class Timer implements Comparable<Timer> {
	
	private long _delay;
	private boolean _periodic;
	private long _nextFireTime = 0;
	
	public static interface Callback { 
		public void timerFired(Timer timer);
	}

	private Callback _callback;
	
	public Timer(long delay,boolean periodic,Callback callback)  {
		_delay = delay;
		_periodic = periodic;
		_callback = callback;
	}
	
	public long     getDelay() { return _delay; }
	public boolean  isPeriodic() { return _periodic; }
	public boolean  isArmed() { return _nextFireTime != 0; }
	
	long	 getNextFireTime() { return _nextFireTime; }
	
	void     fire() { 
		if (_periodic) {
			// rearm
			arm();
		}
		else { 
			disarm();
		}
		_callback.timerFired(this); 
	}
	
	void   arm() { _nextFireTime = System.currentTimeMillis() + _delay; }
	void 	 disarm() { _nextFireTime = 0; }
	public void rearm() { arm(); }
	
	//@Override
	public int compareTo(Timer t) {
		if (_nextFireTime < t._nextFireTime)
			return -1;
		else if (_nextFireTime > t._nextFireTime)
			return 1;
		else 
			return 0;
	}
	
}
