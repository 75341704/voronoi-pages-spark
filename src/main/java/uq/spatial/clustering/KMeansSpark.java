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
	// The fraction of the data to sample in the Approximate mode
	private static final double FRACTION = 1.0;
 	// Number of KMeans to run in parallel (in the deterministic mode)
	private static final int K_MEANS_RUNS = 5;
	// Maximum number of iterations in the KMeans (for both
	// approximate and deterministic mode)
	private static final int K_MEANS_ITR = 100;
	
	/**
	 * Cluster a set of points into a given
	 * number of clusters.
	 * 
	 * @param pointsRDD a RDD of points to cluster
	 * @param numClusters number of cluster to return
	 */
	public static KMeansModel cluster(JavaRDD<Point> pointsRDD, final int numClusters){
		return runKMeans(pointsRDD, numClusters, K_MEANS_ITR, K_MEANS_RUNS, 1.0);
	}
	
	/**
	 * Cluster a set of points into a given
	 * number of clusters. Calculate the approximate
	 * K-Means, using a sample of the data and the
	 * K-Means++ Heuritic.
	 * 
	 * @param pointsRDD a RDD of points to cluster
	 * @param numClusters number of cluster to return
	 */
	public static KMeansModel clusterApprox(JavaRDD<Point> pointsRDD, final int numClusters){
		// runs only once for a fraction of the dataset
		return runKMeans(pointsRDD, numClusters, K_MEANS_ITR, 1, FRACTION);
	}
	
	/**
	 * Perform the K-Means clustering and return the
	 * clusters centers.
	 * 
	 * @param pointsRDD a RDD of points to cluster
	 * @param numClusters number of cluster to return
	 * @param approx True if wants to run approximate KMeans
	 * 
	 * @return return as vector with points coordinates (clusters centroids)
	 */
	public static Vector[] clusterCenters(JavaRDD<Point> pointsRDD, 
			final int numClusters, boolean approx){
		if(approx){
			return clusterApprox(pointsRDD, numClusters).clusterCenters();
		} else{
			return cluster(pointsRDD, numClusters).clusterCenters();
		}
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
	private static KMeansModel runKMeans(JavaRDD<Point> pointsRDD, final int numClusters, 
			final int maxIterations, final int runs, final double fraction){
		System.out.println("Running K Means..\n");
		
		// Parse data
	    JavaRDD<Vector> parsedData = pointsRDD.sample(false, fraction)
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
	    KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, maxIterations, runs, INIT_MODE);
	        
	    // Evaluate clustering by computing Within Set Sum of Squared Errors
	    double WSSSE = clusters.computeCost(parsedData.rdd());
	    System.out.println("Within Set Sum of Squared Errors = " + WSSSE + "\n");
	    
		HDFSFileService serv = new HDFSFileService();
		String script = numClusters + "Clusters - Within Set Sum of Squared Errors = " + WSSSE;
		serv.saveFileHDFS(script, numClusters+"-means-"+fraction+"-error.txt");

	    // Save and load model
	    /*clusters.save(SC.sc(), "myModelPath");
	    KMeansModel sameModel = KMeansModel.load(sc.sc(), "myModelPath");
	    */
	    
	    return clusters;
	}
}
