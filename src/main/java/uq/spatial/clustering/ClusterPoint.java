package uq.spatial.clustering;

import java.io.Serializable;

import uq.spatial.Point;

/**
 * A Point object with a classification label  
 * to be used in the clustering algorithms.
 * 
 * @author uqdalves
 */
@SuppressWarnings("serial")
public class ClusterPoint extends Point implements Serializable{
	public boolean isClassified = false;

	public ClusterPoint() {
		super();
	}
	public ClusterPoint(Point p) {
		super(p.x, p.y, p.time);
	}
	public ClusterPoint(double x, double y, long time) {
		super(x, y, time);
	}
	public ClusterPoint(double x, double y) {
		super(x, y);
	}
}
