package uq.spatial.distance;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import uq.spatial.Point;
import uq.spatial.Trajectory;

/**
 * EDC: Euclidean Distance for 2D Point Series (Trajectories).
 * 
 * </br> Uniform sampling rates only.
 * </br> Spatial dimension only.
 * </br> ??Cope with local time shift.
 * </br> ??Not sensitive to noise.
 * </br> ??Not a metric.
 * 
 * @author uqdalves
 */
@SuppressWarnings("serial")
public class EDCDistanceCalculator implements Serializable, TrajectoryDistanceCalculator{

	public double getDistance(Trajectory t1, Trajectory t2) {
		return EDC(t1.getPointsList(), t2.clone().getPointsList());
	}
	
	/**
	 * EDC calculation, based on the shortest trajectory.
	 */
	private double EDC(List<Point> t1, List<Point> t2){
		if (t1.size() == 0 && t2.size() == 0){
			return 0;
		}
		if (t1.size() == 0 || t2.size() == 0){
			return INFINITY;
		}

		// get the shortest and longest trajectories
		List<Point> longest  = new ArrayList<Point>();
		List<Point> shortest = new ArrayList<Point>();
		if (t1.size() < t2.size()){
			shortest = t1;
			longest = t2;
		}
		else{
			shortest = t2;
			longest = t1;
		}
		
		int n = shortest.size();
		double dist = 0.0;
		for(int i=0; i<n; i++){
			Point p1 = shortest.get(i);
			Point p2 = longest.get(i);
			dist += p1.dist(p2);
		}
		dist = dist / n;
		
		return dist;
	}
}
