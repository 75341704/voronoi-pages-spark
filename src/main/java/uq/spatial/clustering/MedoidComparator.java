package uq.spatial.clustering;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Comparator to sort Medoids by cost in ascending order.
 */
@SuppressWarnings("serial")
public class MedoidComparator<T> implements Serializable, Comparator<Medoid> {
	/**
	 * Compare Medoids by cost in ascending order.
	 */
	public int compare(Medoid m1, Medoid m2) {
		return (m1.cost < m2.cost) ? -1 : (m1.cost == m2.cost ? 0 : 1);
	}
}
