package uq.spark.indexing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;

import uq.fs.FileToObjectRDDService;
import uq.fs.HDFSFileService;
import uq.spark.SparkEnvInterface;
import uq.spatial.Point;
import uq.spatial.clustering.KMeansSpark;
import uq.spatial.clustering.Medoid;
import uq.spatial.clustering.PartitioningAroundMedoids;

/**
 * Service to handle sample pivots.
 * Read data and select pivots (Random, K-Means, Medoids, etc.).
 * Use Spark MapReduce.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class PivotService implements Serializable, SparkEnvInterface, IndexParamInterface {

	/**
	 * Randomly choose a given number of trajectory 
	 * points from the dataset.
	 * 
	 * @param num The number of random points to choose.
	 */
	public static List<Point> selectRandomPoints(final JavaRDD<Point> pointsRDD, final int num){
		System.out.println("\nSampling " + num + " Random Pivots..");
		return pointsRDD.takeSample(false, num);
	}

	/**
	 * Select the given number of approximate medoids points,  
	 * using a distance cost heuristic.
	 * 
	 * @param num The number of points to select.
	 */
	public static List<Point> selectApproxMedoids(final JavaRDD<Point> pointsRDD, final int num){
		System.out.println("\nSampling " + num + " Approx. Medoids..");

		// call of PAM heuristic
		PartitioningAroundMedoids pam = 
				new PartitioningAroundMedoids();
		List<Medoid> medoidsList = 
				pam.selectKApproxMedoids(num, pointsRDD);
		
		System.out.println("Selected Medoids Cost: ");
		List<Point> pointsList = new ArrayList<Point>();
		for(Medoid medoid : medoidsList){
			System.out.println(medoid.cost);
			pointsList.add(medoid);
		}
		return pointsList;
	}
	
	/**
	 * Cluster the given RDD of points into K groups
	 * and select the groups centroids.
	 * 
	 * @param k The number of clusters.
	 */
	public static List<Point> selectKMeans(final JavaRDD<Point> pointsRDD, final int k){
		System.out.println("\nSelecting " + k + "-Means..");
		System.out.println();
		
		// call of k-means algorithm
		Vector[] centersVec = 
				KMeansSpark.clusterCenters(pointsRDD, k, false);
		
		List<Point> centroids = new ArrayList<Point>();
		for(int i=0; i < centersVec.length; i++){
			double[] coord = centersVec[i].toArray();
			centroids.add(new Point(coord[0], coord[1]));
		}
		
		return centroids;
	}
	
	/**
	 * Select a small sample of the given RDD of points
	 * and cluster the sample points into K groups, then
	 * select the groups centroids.
	 * 
	 * @param k The number of clusters.
	 */
	public static List<Point> selectKMeansApprox(final JavaRDD<Point> pointsRDD, final int k){
		System.out.println("\nSelecting " + k + "-Means Approx. ..");
		System.out.println();
		
		// call of k-means algorithm
		Vector[] centersVec = 
				KMeansSpark.clusterCenters(pointsRDD, k, true);
		
		List<Point> centroids = new ArrayList<Point>();
		for(int i=0; i < centersVec.length; i++){
			double[] coord = centersVec[i].toArray();
			centroids.add(new Point(coord[0], coord[1]));
		}
		
		return centroids;
	}
	
	/**
	 * Main
	 */
	public static void main(String [] args0){
		System.out.println();
		System.out.println("Running Pivot Service..\n");

    	// read trajectory data files
     	JavaRDD<String> fileRDD = SC.textFile(DATA_PATH, MIN_PARTITIONS);

     	// convert the input dataset to point objects
     	FileToObjectRDDService rddService = new FileToObjectRDDService();
     	JavaRDD<Point> pointsRDD = rddService.mapRawDataToPointMercRDD(fileRDD);
     	pointsRDD.cache();
     	
     	System.out.println("Number of RDD points partitions: " + pointsRDD.partitions().size());

     	// save result to HDFS
     	HDFSFileService hdfsService = new HDFSFileService();
	
     	// ramdom selection
     	List<Point> pointsList = 
     			selectRandomPoints(pointsRDD, 250);
     	hdfsService.savePointListHDFS(pointsList, "pivots-random-250.txt");
     	pointsList = 
     			selectRandomPoints(pointsRDD, 500);
     	hdfsService.savePointListHDFS(pointsList, "pivots-random-500.txt");
     	pointsList = 
     			selectRandomPoints(pointsRDD, 1000);
     	hdfsService.savePointListHDFS(pointsList, "pivots-random-1000.txt");
     	pointsList = 
     			selectRandomPoints(pointsRDD, 2000);
     	hdfsService.savePointListHDFS(pointsList, "pivots-random-2000.txt");
     	pointsList = 
     			selectRandomPoints(pointsRDD, 4000);
     	hdfsService.savePointListHDFS(pointsList, "pivots-random-4000.txt");
	
     	// approximated k-means selection 
     	pointsList = 
     			selectKMeansApprox(pointsRDD, 250);
		hdfsService.savePointListHDFS(pointsList, "pivots-kmeans-approx-250.txt");
     	pointsList = 
     			selectKMeansApprox(pointsRDD, 500);
		hdfsService.savePointListHDFS(pointsList, "pivots-kmeans-approx-500.txt");
		pointsList = 
				selectKMeansApprox(pointsRDD, 1000);
		hdfsService.savePointListHDFS(pointsList, "pivots-kmeans-approx-1000.txt");
     	pointsList = 
     			selectKMeansApprox(pointsRDD, 2000);
		hdfsService.savePointListHDFS(pointsList, "pivots-kmeans-approx-2000.txt");
		pointsList = 
     			selectKMeansApprox(pointsRDD, 4000);
		hdfsService.savePointListHDFS(pointsList, "pivots-kmeans-approx-4000.txt");
		
     	// k-means selection 
     	pointsList = 
     			selectKMeans(pointsRDD, 250);
		hdfsService.savePointListHDFS(pointsList, "pivots-kmeans-250.txt");
     	pointsList = 
     			selectKMeans(pointsRDD, 500);
		hdfsService.savePointListHDFS(pointsList, "pivots-kmeans-500.txt");
		pointsList = 
				selectKMeans(pointsRDD, 1000);
		hdfsService.savePointListHDFS(pointsList, "pivots-kmeans-1000.txt");
     	pointsList = 
     			selectKMeans(pointsRDD, 2000);
		hdfsService.savePointListHDFS(pointsList, "pivots-kmeans-2000.txt");
     	pointsList = 
     			selectKMeans(pointsRDD, 4000);
		hdfsService.savePointListHDFS(pointsList, "pivots-kmeans-4000.txt");
		
		// clear cache
		fileRDD.unpersist();
		pointsRDD.unpersist();
	}
}
