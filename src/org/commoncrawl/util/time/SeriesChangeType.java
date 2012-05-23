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
 * SeriesChangeType.java
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

import java.io.ObjectStreamException;
import java.io.Serializable;

/**
 * An enumeration of the series change types.
 *
 * @since 1.2.0
 */
public class SeriesChangeType implements Serializable {

    /** Represents a change to the series key. */
    public static final SeriesChangeType CHANGE_KEY
            = new SeriesChangeType("SeriesChangeType.CHANGE_KEY");

    /**
     * Represents the addition of one or more data items to the series in a
     * contiguous range.
     */
    public static final SeriesChangeType ADD
            = new SeriesChangeType("SeriesChangeType.ADD");

    /**
     * Represents the removal of one or more data items in a contiguous
     * range.
     */
    public static final SeriesChangeType REMOVE
            = new SeriesChangeType("SeriesChangeType.REMOVE");

    /** Add one item and remove one other item. */
    public static final SeriesChangeType ADD_AND_REMOVE
            = new SeriesChangeType("SeriesChangeType.ADD_AND_REMOVE");

    /**
     * Represents a change of value for one or more data items in a contiguous
     * range.
     */
    public static final SeriesChangeType UPDATE
            = new SeriesChangeType("SeriesChangeType.UPDATE");


    /** The name. */
    private String name;

    /**
     * Private constructor.
     *
     * @param name  the name.
     */
    private SeriesChangeType(String name) {
        this.name = name;
    }

    /**
     * Returns a string representing the object.
     *
     * @return The string.
     */
    public String toString() {
        return this.name;
    }

    /**
     * Returns <code>true</code> if this object is equal to the specified
     * object, and <code>false</code> otherwise.
     *
     * @param obj  the object (<code>null</code> permitted).
     *
     * @return A boolean.
     */
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof SeriesChangeType)) {
            return false;
        }
        SeriesChangeType style = (SeriesChangeType) obj;
        if (!this.name.equals(style.toString())) {
            return false;
        }
        return true;
    }

    /**
     * Returns a hash code for this instance.
     *
     * @return A hash code.
     */
    public int hashCode() {
        return this.name.hashCode();
    }

    /**
     * Ensures that serialization returns the unique instances.
     *
     * @return The object.
     *
     * @throws ObjectStreamException if there is a problem.
     */
    private Object readResolve() throws ObjectStreamException {
        Object result = null;
        if (this.equals(SeriesChangeType.ADD)) {
            result = SeriesChangeType.ADD;
        }
        // FIXME: Handle other types
        return result;
    }

}
