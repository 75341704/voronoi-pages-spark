package uq.spatial.clustering;

import java.io.Serializable;

import uq.spatial.Point;

/**
 * A Medoid object (a trajectory point with distance 
 * cost to its partition neighbors)
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class Medoid extends Point implements Serializable {
	public double cost = 0.0;
	
	public Medoid() {}
	public Medoid(Point point) {
		super(point.x, point.y, point.time);
		this.cost = 0.0;
	}
	public Medoid(Point point, double cost) {
		super(point.x, point.y, point.time);
		this.cost = cost;
	}
}
