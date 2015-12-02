package uq.spatial.clustering;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import uq.fs.HDFSFileService;
import uq.spark.SparkEnvInterface;
import uq.spark.index.IndexParamInterface;
import uq.spatial.Point;

import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

/**
 * Implements a K-Means clustering algorithms using
 * Spark's Machine Learning Library (MLib). 
 * 
 * Cluster a set of trajectory points into a given 
 * number of clusters.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class KMeansSpark implements Serializable, SparkEnvInterface, IndexParamInterface {
	// KMeans++ initial mode
	private static final String INIT_MODE = KMeans.K_MEANS_PARALLEL();
	// The fraction of the data to sample 
	private static final double FRACTION = 1.0;
 	// Number of KMeans to run in parallel
	private static final int K_MEANS_RUNS = 2;
	// Maximum number of iterations in the KMeans
	private static final int K_MEANS_ITR = 100;
	
	/**
	 * Cluster a set of points into a given
	 * number of clusters.
	 * 
	 * @param pointsRDD a RDD of points to cluster
	 * @param numClusters number of cluster to return
	 */
	public static KMeansModel cluster(JavaRDD<Point> pointsRDD, final int numClusters){
		return runKMeans(pointsRDD, numClusters);
	}
	
	/**
	 * Perform the K-Means clustering and return the
	 * clusters centers.
	 * 
	 * @param pointsRDD a RDD of points to cluster
	 * @param numClusters number of cluster to return
	 * 
	 * @return return as vector with points coordinates (clusters centroids)
	 */
	public static Vector[] clusterCenters(JavaRDD<Point> pointsRDD, 
			final int numClusters){
		return cluster(pointsRDD, numClusters).clusterCenters();
	}
	
	/**
	 * Cluster a set of points into a given
	 * number of clusters.
	 * 
	 * @param pointsRDD a RDD of points to cluster
	 * @param numClusters number of cluster to return
	 * @param maxIterations maximum number of iterations to run.
	 * @param runs number of runs of the algorithm to execute in parallel.
	 * @param fraction the fraction of the data to use (0.0 to 1.0 = 100%)
	 */
	private static KMeansModel runKMeans(
			JavaRDD<Point> pointsRDD, final int numClusters){
		
		System.out.println("Running K Means..\n");
		// Parse data
	    JavaRDD<Vector> parsedData = pointsRDD.sample(false, FRACTION)
	    		.map(new Function<Point, Vector>() {
	        public Vector call(Point point) {
	          double[] values = new double[2];
	          values[0] = point.x; 
	          values[1]	= point.y;	
	          return Vectors.dense(values);
	        }
	      }
	    );
	    parsedData.cache();

	    // Cluster the data into numClusters classes using KMeans
	    KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, K_MEANS_ITR, K_MEANS_RUNS, INIT_MODE);
	        
	    // Evaluate clustering by computing Within Set Sum of Squared Errors
	    double WSSSE = clusters.computeCost(parsedData.rdd());
	    System.out.println("Within Set Sum of Squared Errors = " + WSSSE + "\n");
	    
		HDFSFileService serv = new HDFSFileService();
		String script = numClusters + "Clusters - Within Set Sum of Squared Errors = " + WSSSE;
		serv.saveFileHDFS(script, numClusters+"-means-"+FRACTION+"-error.txt");

	    // Save and load model
	    /*clusters.save(SC.sc(), "myModelPath");
	    KMeansModel sameModel = KMeansModel.load(sc.sc(), "myModelPath");
	    */
	    
	    return clusters;
	}
}
