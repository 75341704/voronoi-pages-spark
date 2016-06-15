package uq.spark.query;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import uq.spark.EnvironmentVariables;
import uq.spatial.Box;
import uq.spatial.Point;
import uq.spatial.Trajectory;
import uq.spatial.distance.EDwPDistanceCalculator;
import uq.spatial.distance.TrajectoryDistanceCalculator;

/**
 * Process queries using brute force.
 * Check one by one for the query answer. 
 * </br>
 * Used as benchmark.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class GreedyQueryService implements Serializable, EnvironmentVariables{
	// the RDD with the trajectories to be used in this service
	private JavaRDD<Trajectory> trajectoryRDD;
	// the trajectory distance measure to use
	private TrajectoryDistanceCalculator distMeasure =
			new EDwPDistanceCalculator();
	// neighborhood distance comparator for trajectories
	private NeighborComparator<NearNeighbor> nnComparator = 
			new NeighborComparator<NearNeighbor>();
	/**
	 * Service Constructor
	 */
	public GreedyQueryService(JavaRDD<Trajectory> trajectoryRDD) {
		this.trajectoryRDD = trajectoryRDD;
	}

	/**
	 * Given a spatial region, return all trajectories
	 * that intersect the given region.
	 * 
	 * @return A list of trajectories
	 */
	public List<Trajectory> getSpatialSelectionTr(final Box region){
		System.out.println("\n[GREEDY QUERY SERVICE] Running Spatial Selection "
				+ "Query for Trajectories..\n");
		List<Trajectory> trajList = 
			trajectoryRDD.filter(new Function<Trajectory, Boolean>() {
				public Boolean call(Trajectory t) throws Exception {
					for(Point p : t.getPointsList()){
						if(region.contains(p)){
							return true;
						}
					}
					return false;
				}
			}).collect();
		return trajList;
	}

	/**
	 * Given a  time interval, from t0 to t1, return all trajectories
	 * active during the time interval [t0,t1].
	 * 
	 * @return A list of trajectories
	 */
	public List<Trajectory> getTemporalSelectionTr(
			final Box region, final long t0, final long t1){
		System.out.println("\n[GREEDY QUERY SERVICE] Running Temporal "
				+ "Selection Query for Trajectories..\n");
		List<Trajectory> trajList = 
			trajectoryRDD.filter(new Function<Trajectory, Boolean>() {
				public Boolean call(Trajectory t) throws Exception {
					for(Point p : t.getPointsList()){
						if(p.time >= t0 && p.time <= t1){
							return true;
						}
					}
					return false;
				}
			}).collect();
		return trajList;
	}
	
	/**
	 * Given a spatial region and time interval, from t0 to t1, 
	 * return all trajectories that intersect the given region
	 * during the time [t0,t1].
	 * 
	 * @return A list of trajectories
	 */
	public List<Trajectory> getSpatialTemporalSelectionTr(
			final Box region, final long t0, final long t1){
		System.out.println("\n[GREEDY QUERY SERVICE] Running Spatial-Temporal "
				+ "Selection Query for Trajectories..\n");
		List<Trajectory> trajList = 
			trajectoryRDD.filter(new Function<Trajectory, Boolean>() {
				public Boolean call(Trajectory t) throws Exception {
					for(Point p : t.getPointsList()){
						if(region.contains(p) && p.time >= t0 && p.time <= t1){
							return true;
						}
					}
					return false;
				}
			}).collect();
		return trajList;
	}
	
	/**
	 * Given a spatial region, return all trajectory points
	 * inside the given region.
	 * 
	 * @return A list of point
	 */
	public List<Point> getSpatialSelectionPt(final Box region){
		System.out.println("\n[GREEDY QUERY SERVICE] Running  Spatial "
				+ "Selection Query for Points..\n");
		List<Point> pointList = 
			trajectoryRDD.flatMap(new FlatMapFunction<Trajectory, Point>() {
				public Iterable<Point> call(Trajectory t) throws Exception {
					List<Point> list = new ArrayList<Point>();
					for(Point p : t.getPointsList()){
						if(region.contains(p)){
							list.add(p);
						}
					}
					return list;
				}
			}).collect();
		return pointList;
	}
	
	/**
	 * Given a time interval, from t0 to t1, return all trajectory points
	 * active during the given time interval [t0,t1].
	 * 
	 * @return A list of point
	 */
	public List<Point> getTemporalSelectionPt(
			final long t0, final long t1){
		System.out.println("\n[GREEDY QUERY SERVICE] Running Temporal "
				+ "Selection Query for Points..\n");
		List<Point> pointList = 
			trajectoryRDD.flatMap(new FlatMapFunction<Trajectory, Point>() {
				public Iterable<Point> call(Trajectory t) throws Exception {
					List<Point> list = new ArrayList<Point>();
					for(Point p : t.getPointsList()){
						if(p.time >= t0 && p.time <= t1){
							list.add(p);
						}
					}
					return list;
				}
			}).collect();
		return pointList;
	}
	
	/**
	 * Given a time interval, from t0 to t1, return all trajectory points
	 * active during the given time interval [t0,t1].
	 * 
	 * @return A list of point
	 */
	public List<Point> getSpatialTemporalSelectionPt(
			final Box region, final long t0, final long t1){
		System.out.println("\n[GREEDY QUERY SERVICE] Running Spatial-Temporal "
				+ "Selection Query for Points..\n");
		List<Point> pointList = 
			trajectoryRDD.flatMap(new FlatMapFunction<Trajectory, Point>() {
				public Iterable<Point> call(Trajectory t) throws Exception {
					List<Point> list = new ArrayList<Point>();
					for(Point p : t.getPointsList()){
						if(region.contains(p) && p.time >= t0 && p.time <= t1){
							list.add(p);
						}
					}
					return list;
				}
			}).collect();
		return pointList;
	}
	
	/**
	 * Given a query trajectory Q and a integer K, return the K
	 * closest trajectories to Q.
	 * 
	 * @return A list of NearNeaighbor elements (trajectories)
	 */
	public List<NearNeighbor> getKNNQuery(
			final Trajectory q, 
			final int k){
		System.out.println("\n[GREEDY QUERY SERVICE] Running k-NN Query..\n");
		// get the distance from Q to all trajectories
		List<NearNeighbor> nnResultList = 
			trajectoryRDD.map(new Function<Trajectory, NearNeighbor>() {
				public NearNeighbor call(Trajectory t) throws Exception {
					t.sort();
					double d = distMeasure.getDistance(t, q);
					NearNeighbor nn = new NearNeighbor(t, d);
					return nn;
				}
			}).takeOrdered(k, nnComparator);
		return nnResultList;
	}
	
	/**
	 * Given a query trajectory Q, a time interval [t0,t1], 
	 * and a integer K, return the K closest trajectories to Q
	 * during [t0,t1]. 
	 * 
	 * @return A list of NearNeaighbor elements (trajectories)
	 */
	public List<NearNeighbor> getKNNQuery(
			final Trajectory q, 
			final long t0, final long t1, 
			final int k){
		System.out.println("\n[GREEDY QUERY SERVICE] Running k-NN Query..\n");
		// filter trajectories active during [t0,t1]
		JavaRDD<Trajectory> filteredRDD = 
			trajectoryRDD.filter(new Function<Trajectory, Boolean>() {
				public Boolean call(Trajectory t) throws Exception {
					if(t.timeIni() > t1 || t.timeEnd() < t0){
						return false;
					} return true;
				}
		});
		// get the distance from Q to all filtered trajectories
		List<NearNeighbor> nnResultList = 
			filteredRDD.map(new Function<Trajectory, NearNeighbor>() {
				public NearNeighbor call(Trajectory t) throws Exception {
					t.sort();
					double d = distMeasure.getDistance(q, t);
					NearNeighbor nn = new NearNeighbor(t, d);
					return nn;
				}
			}).takeOrdered(k, nnComparator);
		return nnResultList;
	}
}
