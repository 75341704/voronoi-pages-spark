package uq.spatial;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Comparator to sort trajectory points by time-stamp.
 * 
 * @author uqdalves
 */
@SuppressWarnings("serial")
public class TimeComparator<T> implements Serializable, Comparator<Point>{

	/**
	 * Compare points by time stamp by ascending order.
	 */
	public int compare(Point p1, Point p2) {
		return p1.time > p2.time ? 1 : (p1.time < p2.time ? -1 : 0);
	}
}
