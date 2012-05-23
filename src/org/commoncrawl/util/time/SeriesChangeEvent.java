/* ===========================================================
 * JFreeChart : a free chart library for the Java(tm) platform
 * ===========================================================
 *
 * (C) Copyright 2000-2009, by Object Refinery Limited and Contributors.
 *
 * Project Info:  http://www.jfree.org/jfreechart/index.html
 *
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation; either version 2.1 of the License, or
 * (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public
 * License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
 * USA.
 *
 * [Java is a trademark or registered trademark of Sun Microsystems, Inc.
 * in the United States and other countries.]
 *
 * ----------------------
 * SeriesChangeEvent.java
 * ----------------------
 * (C) Copyright 2001-2009, by Object Refinery Limited.
 *
 * Original Author:  David Gilbert (for Object Refinery Limited);
 * Contributor(s):   -;
 *
 * Changes
 * -------
 * 15-Nov-2001 : Version 1 (DG);
 * 18-Aug-2003 : Implemented Serializable (DG);
 * 09-Jun-2009 : Added change event info (DG);
 *
 */

package org.commoncrawl.util.time;

import java.io.Serializable;
import java.util.EventObject;

/**
 * An event with details of a change to a series.
 */
public class SeriesChangeEvent extends EventObject implements Serializable {

    /** For serialization. */
    private static final long serialVersionUID = 1593866085210089052L;

    /**
     * Summary info about the change.
     *
     * @since 1.2.0
     */
    private SeriesChangeInfo summary;

    /**
     * Constructs a new event.
     *
     * @param source  the source of the change event.
     */
    public SeriesChangeEvent(Object source) {
        this(source, null);
    }

    /**
     * Constructs a new change event.
     *
     * @param source  the event source.
     * @param summary  a summary of the change (<code>null</code> permitted).
     *
     * @since 1.2.0
     */
    public SeriesChangeEvent(Object source, SeriesChangeInfo summary) {
        super(source);
        this.summary = summary;
    }

    /**
     * Returns a summary of the change for this event.
     *
     * @return The change summary (possibly <code>null</code>).
     *
     * @since 1.2.0
     */
    public SeriesChangeInfo getSummary() {
        return this.summary;
}

    /**
     * Sets the change info for this event.
     *
     * @param summary  the info (<code>null</code> permitted).
     *
     * @since 1.2.0
     */
    public void setSummary(SeriesChangeInfo summary) {
        this.summary = summary;
    }

}
