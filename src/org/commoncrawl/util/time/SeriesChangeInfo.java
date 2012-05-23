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
 * ---------------------
 * SeriesChangeInfo.java
 * ---------------------
 * (C) Copyright 2009, by Object Refinery Limited.
 *
 * Original Author:  David Gilbert (for Object Refinery Limited);
 * Contributor(s):   -;
 *
 * Changes
 * -------
 * 26-Jun-2009 : Version 1 (DG);
 *
 */

package org.commoncrawl.util.time;

/**
 * Summarises a change to a series.
 *
 * @since 1.2.0
 */
public class SeriesChangeInfo {

    /** The type of change. */
    private SeriesChangeType changeType;

    /** The index of the first data item affected by the change. */
    private int index1;

    /** The index of the latest data item affected by the change. */
    private int index2;

    /**
     * Creates a new instance.
     *
     * @param t  the type of change (<code>null</code> not permitted).
     * @param index1  the index of the first data item affected by the change.
     * @param index2  the index of the last data item affected by the change.
     */
    public SeriesChangeInfo(SeriesChangeType t, int index1, int index2) {
        this.changeType = t;
        this.index1 = index1;
        this.index2 = index2;
    }

    /**
     * Returns the series change type.
     *
     * @return The series change type.
     */
    public SeriesChangeType getChangeType() {
        return this.changeType;
    }

    /**
     * Returns the index of the first item affected by the change.
     *
     * @return The index.
     */
    public int getIndex1() {
        return this.index1;
    }

    /**
     * Returns the index of the last item affects by the change.
     *
     * @return The index.
     */
    public int getIndex2() {
        return this.index2;
    }

}