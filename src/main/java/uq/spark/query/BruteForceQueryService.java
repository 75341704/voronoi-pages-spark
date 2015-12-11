package uq.spark.query;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import uq.fs.FileToObjectRDDService;
import uq.fs.HDFSFileService;
import uq.spark.Logger;
import uq.spark.SparkEnvInterface;
import uq.spatial.Box;
import uq.spatial.Point;
import uq.spatial.STBox;
import uq.spatial.Trajectory;
import uq.spatial.distance.DistanceService;

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
public class BruteForceQueryService implements Serializable, SparkEnvInterface{
	private static final HDFSFileService HDFS = new HDFSFileService();
	// experiments log
	private static final Logger LOG = new Logger();
	
	/**
	 * Given a spatial region, return all trajectories
	 * that intersect the given region.
	 * 
	 * @return A list of trajectories
	 */
	public static List<Trajectory> getSpatialSelectionTr(
			JavaRDD<Trajectory> trajRDD, final Box region){
		System.out.println("\nRunning Spatial Selection Query TR Brute Force..\n");
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
		System.out.println("\nRunning Temporal Selection Query TR Brute Force..\n");
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
		System.out.println("\nRunning Spatial-Temporal Selection Query TR Brute Force..\n");
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
		System.out.println("\nRunning Spatial Selection Query PT Brute Force..\n");
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
		System.out.println("\nRunning Temporal Selection Query PT Brute Force..\n");
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
		System.out.println("\nRunning Spatial-Temporal Selection Query PT Brute Force..\n");
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
		System.out.println("\nRunning KNN Query Brute Force..\n");
		NeighborComparator<NearNeighbor> comparator = 
				new NeighborComparator<NearNeighbor>();
		final DistanceService dist = new DistanceService();
		// get the distance from Q to all trajectories
		List<NearNeighbor> nnList = 
			trajRDD.map(new Function<Trajectory, NearNeighbor>() {
				public NearNeighbor call(Trajectory t) throws Exception {
					double d = dist.EDwP(t, q);
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
		System.out.println("\nRunning KNN Query Brute Force..\n");
		// filter trajectories active during [t0,t1]
		JavaRDD<Trajectory> filteredRDD = 
				trajRDD.filter(new Function<Trajectory, Boolean>() {
			public Boolean call(Trajectory t) throws Exception {
				if(t.timeIni() > t1 || t.timeEnd() < t0){
					return false;
				} return true;
			}
		});
		// get the distance from Q to all trajectories		
		NeighborComparator<NearNeighbor> comparator = 
				new NeighborComparator<NearNeighbor>();
		final DistanceService dist = new DistanceService();
		List<NearNeighbor> nnList = 
			filteredRDD.map(new Function<Trajectory, NearNeighbor>() {
				public NearNeighbor call(Trajectory t) throws Exception {
					double d = dist.EDwP(q, t);
					NearNeighbor nn = new NearNeighbor(t, d);
					return nn;
				}
			}).takeOrdered(k, comparator);
		return nnList;
	}
	
	/**
	 * Main, for testing
	 */
	public static void main(String[] arg0){
		System.out.println("\nRunning Brute Force Query..\n");
		
		JavaRDD<String> fileRDD = SC.textFile(DATA_PATH);
		FileToObjectRDDService rdd = new FileToObjectRDDService();
		JavaRDD<Trajectory> trajRDD = rdd.mapRawDataToTrajectoryRDD(fileRDD);
		
		LOG.appendln("Brute Force Test Result.");
		
		// Run spatial-temporal selection test
		List<STBox> stUseCases = readSpatialTemporalUseCases();
		LOG.appendln("Spatial-Temporal Selection Result.");
		for(int i=1; i<=10; i++){
			STBox stObj = stUseCases.get(i);
			List<Point> ptList = 
					getSpatialTemporalSelectionPt(trajRDD, stObj, stObj.timeIni, stObj.timeEnd);
			List<Trajectory> tList = 
					getSpatialTemporalSelectionTr(trajRDD, stObj, stObj.timeIni, stObj.timeEnd);
			LOG.appendln("Query " + i + " Result.");
			LOG.appendln("Number of Points: "  + ptList.size());
			LOG.appendln("Trajectories Returned: " + tList.size());
		}
		
		// Run KNN test
/*		List<Trajectory> nnUseCases = readNearestNeighborUseCases();
		LOG.appendln("K-NN Result.");
		for(int i=1; i<=13; i++){
			// params
			Trajectory q = nnUseCases.get(i);
			long tIni = q.timeIni();
			long tEnd = q.timeEnd();
			int k = 10;
			// run query
			List<NearNeighbor> result = 
					getKNNQuery(trajRDD, q, tIni, tEnd, k);
			LOG.appendln("Query " + i + " Result.");
			LOG.appendln("Query Trajectory: " + q.id);
			LOG.appendln("Trajectories Returned: " + result.size());
			int n=1;
			for(NearNeighbor nn : result){
				LOG.appendln(n++ + "-NN: " + nn.id);
				LOG.appendln("Dist: " + nn.distance);
			}
		}*/

		// save the result log to HDFS
		LOG.save("brute-force-experiments");
	}
	
	/**
	 * Read the uses cases for spatial-temporal selection queries
	 */
	private static List<STBox> readSpatialTemporalUseCases(){
		List<String> lines = 
				HDFS.readFileHDFS("/spark-data/use-cases/spatial-temporal-use-cases");
		// process lines
		long timeIni, timeEnd;
		double left, right, bottom, top;
		List<STBox> stList = new LinkedList<STBox>(); 
		for(String line : lines){
			if(line.length() > 3){
				String[] tokens = line.split(" ");
				left	= Double.parseDouble(tokens[0]);
				right	= Double.parseDouble(tokens[1]);
				bottom	= Double.parseDouble(tokens[2]);
				top		= Double.parseDouble(tokens[3]);
				timeIni = Long.parseLong(tokens[4]);
				timeEnd = Long.parseLong(tokens[5]); 
				
				stList.add(new STBox(left, right, bottom, top, timeIni, timeEnd));
			}
		}
		return stList;
	}
	
	/**
	 * Read the uses cases for Nearest Neighbors queries.
	 */
	private static List<Trajectory> readNearestNeighborUseCases(){
		List<String> lines = 
				HDFS.readFileHDFS("/spark-data/use-cases/nn-use-cases");
		// process lines
		int id=1;
		double x, y;
		long time;
		List<Trajectory> list = new LinkedList<Trajectory>();
		for(String line : lines){
			if(line.length() > 4){
				String[] tokens = line.split(" ");
				// first tokens is the id
				Trajectory t = new Trajectory("Q" + id++);
				for(int i=1; i<=tokens.length-3; i+=3){
					x = Double.parseDouble(tokens[i]);
					y = Double.parseDouble(tokens[i+1]);
					time = Long.parseLong(tokens[i+2]);
					t.addPoint(new Point(x, y, time));
				}
				list.add(t);
			}
		}
		return list;
	}
}
