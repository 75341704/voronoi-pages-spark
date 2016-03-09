package uq.spark.query;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import uq.fs.DataConverter;
import uq.fs.FileReader;
import uq.spark.Logger;
import uq.spark.EnvironmentVariables;
import uq.spatial.Box;
import uq.spatial.Point;
import uq.spatial.STBox;
import uq.spatial.Trajectory;
import uq.spatial.distance.EDwPDistanceCalculator;
import uq.spatial.distance.TrajectoryDistanceCalculator;

/**
 * Process queries using a greedy algorithm (brute force).
 * Check one by one for the query answer. 
 * </br>
 * Used as benchmark.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class GreedyQueryCalculator implements Serializable, EnvironmentVariables{
	// trajectory distance measure
	private static TrajectoryDistanceCalculator edwp = 
			new EDwPDistanceCalculator();
	// experiments log
	private static final Logger LOG = 
			new Logger();
	 
	
	/**
	 * Given a spatial region, return all trajectories
	 * that intersect the given region.
	 * 
	 * @return A list of trajectories
	 */
	public static List<Trajectory> getSpatialSelectionTr(
			JavaRDD<Trajectory> trajRDD, final Box region){
		System.out.println("\n[GREEDY SERVICE] Running Spatial Selection Query TR..\n");
		List<Trajectory> trajList = 
			trajRDD.filter(new Function<Trajectory, Boolean>() {
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
	public static List<Trajectory> getTemporalSelectionTr(
			JavaRDD<Trajectory> trajRDD, 
			final Box region, final long t0, final long t1){
		System.out.println("\n[GREEDY SERVICE] Running Temporal Selection Query TR..\n");
		List<Trajectory> trajList = 
			trajRDD.filter(new Function<Trajectory, Boolean>() {
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
	public static List<Trajectory> getSpatialTemporalSelectionTr(
			JavaRDD<Trajectory> trajRDD, 
			final Box region, final long t0, final long t1){
		System.out.println("\n[GREEDY SERVICE] Running Spatial-Temporal Selection Query TR..\n");
		List<Trajectory> trajList = 
			trajRDD.filter(new Function<Trajectory, Boolean>() {
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
	public static List<Point> getSpatialSelectionPt(
			JavaRDD<Trajectory> trajRDD, final Box region){
		System.out.println("\n[GREEDY SERVICE] Running Spatial Selection Query PT..\n");
		List<Point> pointList = 
			trajRDD.flatMap(new FlatMapFunction<Trajectory, Point>() {
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
	public static List<Point> getTemporalSelectionPt(
			JavaRDD<Trajectory> trajRDD, 
			final long t0, final long t1){
		System.out.println("\n[GREEDY SERVICE] Running Temporal Selection Query PT..\n");
		List<Point> pointList = 
			trajRDD.flatMap(new FlatMapFunction<Trajectory, Point>() {
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
	public static List<Point> getSpatialTemporalSelectionPt(
			JavaRDD<Trajectory> trajRDD, 
			final Box region, final long t0, final long t1){
		System.out.println("\n[GREEDY SERVICE] Running Spatial-Temporal Selection Query PT..\n");
		List<Point> pointList = 
			trajRDD.flatMap(new FlatMapFunction<Trajectory, Point>() {
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
	public static List<NearNeighbor> getKNNQuery(
			JavaRDD<Trajectory> trajRDD, 
			final Trajectory q, final int k){
		System.out.println("\n[GREEDY SERVICE] Running KNN Query..\n");
		NeighborComparator<NearNeighbor> comparator = 
				new NeighborComparator<NearNeighbor>();
		// get the distance from Q to all trajectories
		List<NearNeighbor> nnList = 
			trajRDD.map(new Function<Trajectory, NearNeighbor>() {
				public NearNeighbor call(Trajectory t) throws Exception {
					double d = edwp.getDistance(t, q);
					NearNeighbor nn = new NearNeighbor(t, d);
					return nn;
				}
			}).takeOrdered(k, comparator);
		return nnList;
	}
	
	/**
	 * Given a query trajectory Q, a time interval [t0,t1], 
	 * and a integer K, return the K closest trajectories to Q
	 * during [t0,t1]. 
	 * 
	 * @return A list of NearNeaighbor elements (trajectories)
	 */
	public static List<NearNeighbor> getKNNQuery(
			JavaRDD<Trajectory> trajRDD, 
			final Trajectory q, final long t0, final long t1, final int k){
		System.out.println("\n[GREEDY SERVICE] Running KNN Query..\n");
		// filter trajectories active during [t0,t1]
		JavaRDD<Trajectory> filteredRDD = 
				trajRDD.filter(new Function<Trajectory, Boolean>() {
			public Boolean call(Trajectory t) throws Exception {
				if(t.timeIni() > t1 || t.timeEnd() < t0){
					return false;
				} return true;
			}
		});
		// get the distance from Q to all filtered trajectories		
		NeighborComparator<NearNeighbor> comparator = 
				new NeighborComparator<NearNeighbor>();
		List<NearNeighbor> nnList = 
			filteredRDD.map(new Function<Trajectory, NearNeighbor>() {
				public NearNeighbor call(Trajectory t) throws Exception {
					double d = edwp.getDistance(t, q);
					return new NearNeighbor(t, d);
				}
			}).takeOrdered(k, comparator);
		return nnList;
	}
	
	/**
	 * Main, for testing
	 */
	public static void main(String[] arg0){
		System.out.println("\n[GREEDY SERVICE] Running Greedy Query Service..\n");
		
		JavaRDD<String> fileRDD = SC.textFile(DATA_PATH);
		DataConverter rdd = new DataConverter();
		JavaRDD<Trajectory> trajRDD = rdd.mapRawDataToTrajectoryRDD(fileRDD);
		
		LOG.appendln("Greedy Test Results.");
		LOG.appendln();
		
		// Run spatial-temporal selection test
		List<STBox> stTestCases = 
				FileReader.readSpatialTemporalTestCases();
		LOG.appendln("Spatial-Temporal Selection Result.");
		LOG.appendln();
		int i=1;
		for (STBox stObj : stTestCases){
			List<Point> ptList = 
					getSpatialTemporalSelectionPt(trajRDD, stObj, stObj.timeIni, stObj.timeEnd);
			List<Trajectory> tList = 
					getSpatialTemporalSelectionTr(trajRDD, stObj, stObj.timeIni, stObj.timeEnd);
			LOG.appendln("Query " + i++ + " Result.");
			LOG.appendln("Number of Points: "  + ptList.size());
			LOG.appendln("Trajectories Returned: " + tList.size());
		}

		// Run KNN test
		List<Trajectory> nnTestCases = 
				FileReader.readNearestNeighborTestCases();
		LOG.appendln("K-NN Result.");
		LOG.appendln();
		int j=1;
		for(Trajectory q : nnTestCases){ // run only 10 queries
			// params
			long tIni 	= q.timeIni();
			long tEnd 	= q.timeEnd();
			final int k = 20; // 20-NN
			// run query
			List<NearNeighbor> result = 
					getKNNQuery(trajRDD, q, tIni, tEnd, k);
			LOG.appendln("Query " + j++ + " Result.");
			LOG.appendln("Query Trajectory: " + q.id);
			LOG.appendln("Trajectories Returned: " + result.size());
			int n=1;
			for(NearNeighbor nn : result){
				LOG.appendln(n++ + "-NN: " + nn.id);
				LOG.appendln("Dist: " + nn.distance);
			}
		}

		// save the result log to HDFS
		LOG.save("greedy-experiments-result");
	}
}
