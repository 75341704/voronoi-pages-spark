package uq;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import uq.spatial.Point;

/**
 * Examples of some operations over trajectories 
 * on Spark.
 * 
 * @author uqdalves
 *
 */
public class TrajectorySparkExample {
	
	public static void main(String[] args) {
		// tells Spark how to access a cluster
    	SparkConf conf = new SparkConf().setAppName("TrajectorySparkExample").setMaster("local");
    	JavaSparkContext sc = new JavaSparkContext(conf);
    	
    	// Read a trajectory file (from the HDFS folder)
    	//JavaRDD<String> lines = sc.textFile("file:/usr/local/hdfs/datanode/trajectories");      // single file with many trajectories with id
     	//JavaRDD<String> lines = sc.textFile("file:/usr/local/hdfs/datanode/trajectory.txt");    // single file with only one trajectory
     	JavaRDD<String> lines = sc.textFile("file:/usr/local/hdfs/datanode/trajectories.gz");     // single compacted file with many trajectories with id
     	
     	singleTrajectoryLengthSpark(lines);
	}
	
	/**
	 * Read a trajectory file (single trajectory) and sum the time-stamps using Spark
	 */
	public static void sumTimeStampsSpark(JavaRDD<String> lines){
		// Map Function:
     	// Get each line of the file and get the time-stamp.
     	// Creates a key/value pair using "T" as key, so that all 
     	// lines of the file (time-stamp) will have the same key "T"
     	PairFunction<String, String, Long> getKeyValue = 
     			new PairFunction<String, String, Long>() {
		  public Tuple2<String, Long> call(String line) {
			  String[] tokens = line.split(","); 
       		  return new Tuple2("T", Long.parseLong(tokens[2]));
		  }
		};
		// Map each line to a key/value <'T',Time-Stamp>
		JavaPairRDD<String, Long> pairs = lines.mapToPair(getKeyValue);

		// Reduce function:
		// Get the time period between every two points
		Function2<Long, Long, Long> getTime = 
				new Function2<Long, Long, Long>() {
			public Long call(Long t1, Long t2) throws Exception {
				return (t1 + t2);
			}
		};
		// reduce, sum the time between every point
		JavaPairRDD<String, Long> result = pairs.reduceByKey(getTime);
		
		//pairs.aggregateByKey(0, seqFunc, combFunc);
		
		// Run the jobs and collect the values from the map/reduce functions
		// List<Tuple2<String, Point>> tuples = result.collect();
		
		long numElements = result.count();
		Tuple2<String, Long> elem = result.first();
		String key = elem._1;
		long value = elem._2;

		System.out.println("Sum of Time-Stamps:");
		System.out.println("Num. Elements: " + numElements); // 1
		System.out.println("Key: " + key);
		System.out.println("Value: " + value);
	}
	
	/**
	 * Read a trajectory and calculate its length using Spark
	 */
	public static void singleTrajectoryLengthSpark(JavaRDD<String> lines){
		// Map Function:
     	// Get each line of the file and map it to a Point entity.
     	// Creates a key/value pair using "T" as key, so that all 
     	// lines of the file (Points) will have the same key "T"
     	PairFunction<String, String, Point> keyValues = 
     			new PairFunction<String, String, Point>() {
		  public Tuple2<String, Point> call(String line) {
			  String[] tokens = line.split(","); 
			  Point p = new Point(
					  Double.parseDouble(tokens[0]), 
					  Double.parseDouble(tokens[1]), 
					  Long.parseLong(tokens[2]));
       		  return new Tuple2("T", p);
		  }
		};
		// Map each line to a key/value <'T',Point>
		JavaPairRDD<String, Point> pairs = lines.mapToPair(keyValues);

		// Reduce Function:
		// Calculate the Euclidean distance between every two points.
		Function2<Point, Point, Point> getDistance = 
				new Function2<Point, Point, Point>() {
			public Point call(Point p1, Point p2) throws Exception {
				double dist = (p2.x-p1.x)*(p2.x-p1.x) + 
							  (p2.y-p1.y)*(p2.y-p1.y);
				dist = Math.sqrt(dist);
				//p2.distAccum = p1.distAccum + dist;				
				return p2;
			}
		};
		// Reduce, sum of the distances between every point (accum. distance)
		// <'T',P1> <'T',P2> ... <'T',Pn>
		JavaPairRDD<String, Point> result = pairs.reduceByKey(getDistance);

		Tuple2<String, Point> elem = result.first();
		String key = elem._1;
		//double value = elem._2.distAccum;

		System.out.println("Trajectory Length:");
		System.out.println("Key: " + key);
		//System.out.println("Value: " + value);
	}
	
	/**
	 * Read many trajectories with id, from a single file, 
	 * and calculate their lengths using Spark
	 */
	public static void trajectoriesLenghtSpark(JavaRDD<String> lines){
		// Map Function:
     	// Get each line of the file and map it to a Point entity.
     	// Creates a key/value pair using the trajectory id as key
     	PairFunction<String, String, Point> keyValues = 
     			new PairFunction<String, String, Point>() { 
		  public Tuple2<String, Point> call(String line) {
			  // read line fields
			  String[] tokens = line.split(" "); 
			  
			  String trajId = tokens[0];
			  double x = Double.parseDouble(tokens[1]);
			  double y = Double.parseDouble(tokens[2]);
			  long time = Long.parseLong(tokens[3]);
			  
			  Point p = new Point(x,y,time);
			  
			  // return pair <TrajectoryID, Point>
       		  return new Tuple2(trajId, p);
		  }
		};
		// Map each line to a key/value <TrajectoryID, Point>
		JavaPairRDD<String, Point> pairs = lines.mapToPair(keyValues);
		
		// Cache the RDD into memory before run an action
		pairs.cache();
		
		// Reduce Function:
		// Calculate the Euclidean distance between every two points.
		Function2<Point, Point, Point> distance = 
				new Function2<Point, Point, Point>() {
			public Point call(Point p1, Point p2) throws Exception {
				double dist = (p2.x-p1.x)*(p2.x-p1.x) + 
							  (p2.y-p1.y)*(p2.y-p1.y);
				dist = Math.sqrt(dist);
				//p2.distAccum = p1.distAccum + dist;				
				return p2;
			}
		};
		// Reduce, sum of the distances between every point (accum. distance)
		// <'T1', {p1,p2, ...,pn}>, <'T2', {p1,p2,...,pk}> ...
		JavaPairRDD<String, Point> result = pairs.reduceByKey(distance);
		
		// get the results
		System.out.println("Number of Elements: " + result.count());
		List<Tuple2<String, Point>> tuples = result.collect();
		
		// result.take(5); // get 5 elements
		
		for(Tuple2<String, Point> elem : tuples){
			String key = elem._1;
			//double value = elem._2.distAccum;
			
			System.out.println("Key: " + key);
			//System.out.println("Lenght: " + value);
			System.out.println();
		}
		
		// Save the result to the HDFS
		result.saveAsTextFile("file:/usr/local/hdfs/datanode/distance-results");
	}
}
