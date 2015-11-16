package uq.spatial.clustering; 

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;

import uq.spark.SparkEnvInterface;
import uq.spark.indexing.IndexParamInterface;
import uq.spatial.Point;

/**
 * k-Medoids algorithm PAM: Partitioning Around Medoids.
 * 
 * Clusters a data set of n objects into k clusters. 
 * A Medoid is the object of a cluster whose average dissimilarity to all 
 * the objects in the cluster is minimal. i.e. it is a most centrally 
 * located point in the cluster.
 * 
 * Use Spark to calculate Medoids.
 * Calculate approximations and precise Medoids.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class PartitioningAroundMedoids implements Serializable, SparkEnvInterface, IndexParamInterface {
	// fraction of the data to use in the heuristic
	private static final double FRACTION = 0.3; // 1.0 = 100%
	// the number of points to select in each partition (top 1)
	private static final int NUM_PTS = 1;
	
	/**
	 * TODO
	 */
	public Integer[] kMedoids(int k, ArrayList<Point> pointsList){
		Integer[] medoids = new Integer[k];
		
		// randomly select the percentage of data to be sampled		
		
		// Selects the initial medoids, based on distance cost
		//Integer[] initialMedoids = selectKInitialMedoids(k, pointsList);
		
		// 2) Map each object to its closest Medoid

		return medoids;
	}

	/**
	 * Select K initial approximated Medoids (heuristic).
	 * Given a dataset (RDD) of trajectory points, select
	 * the K medoids (points) with smallest distance cost
	 * inside each RDD partition.
	 */
	public List<Medoid> selectKApproxMedoids(final int K, JavaRDD<Point> pointsRDD){

/*	
		final List<Medoid> zeroValue = new ArrayList<Medoid>();
		
		 final Function2<List<Medoid>, Point, List<Medoid>> seqOp = new Function2<List<Medoid>, Point, List<Medoid>>() {
			
			public List<Medoid> call(List<Medoid> list, Point point) throws Exception {
				// TODO Auto-generated method stub
				return null;
			}
		};
		final Function2<List<Medoid>, List<Medoid>, List<Medoid>> combOp = 
				new Function2<List<Medoid>, List<Medoid>, List<Medoid>>() {
			public List<Medoid> call(List<Medoid> list1, List<Medoid> list2) throws Exception {
				list1.addAll(list2);
				return list1;
			}
		};
		
		pointsRDD.aggregate(zeroValue, a, combOp);
*/

		// sample a small portion of the entire dataset and 
		// calculate the approx medoids in each RDD partition
		List<Medoid> medoidsList = pointsRDD.sample(false, FRACTION)
				.mapPartitions(new FlatMapFunction<Iterator<Point>, Medoid>() {
			
			public Iterable<Medoid> call(Iterator<Point> pointsPartition) throws Exception {
				// get the list of points in this partition
				List<Medoid> medoidsList = new ArrayList<Medoid>();
				while(pointsPartition.hasNext()){
					medoidsList.add(new Medoid(pointsPartition.next()));
				}
				// distances between every pair of points in this partition
				Double[][] distMatrix = getDistancesMatix(medoidsList);
				
				// cost vector
				Double[] costVec = getCostVector(distMatrix);
				
				// set the cost for every medoid
				for(int i=0; i<costVec.length; i++){
					medoidsList.get(i).cost = costVec[i];
				}
				
				// sort medoids by cost
				Collections.sort(medoidsList, new MedoidComparator<Medoid>());

				return medoidsList.subList(0, NUM_PTS);
			}
		}).collect();

		/*************
		 * REFINEMENT
		 *************/

		// select the top K elements from all partitions
		Collections.sort(medoidsList, new MedoidComparator<Medoid>());
		
		return medoidsList.subList(0, K);
	}
	
	/**
	 * Calculate the distance "cost" vector for a given distance matrix.
	 * Calculate cost vector for every element (matrix row).
	 */
	private Double[] getCostVector(Double[][] distMatrix){
		int n = distMatrix[0].length;
		
		Double[] sumCol = new Double[n];
		// get sum of the columns
		for(int i=0; i<n; i++){
			sumCol[i] = 0.0;
			for(int j=0; j<n; j++){
				sumCol[i] += distMatrix[j][i]; // sum of columns
			}
		}

		// calculate cost vector for every element (matrix row)
		Double[] costVector = new Double[n];
		for(int i=0; i<n; i++){
			costVector[i] = 0.0;     // cost
			for(int j=0; j<n; j++){
				// cost: distance [i][j] divided by the column [j] sum
				costVector[i] += distMatrix[i][j] / sumCol[j];
			}
		}
		
		return costVector;
	}
	
	/**
	 * Calculates the distances between every pair of point.
	 * Returns a [n x n] matrix of distances. 
	 * n - number of elements.
	 * Each row of the matrix holds distances for one element.
	 */
	private Double[][] getDistancesMatix(List<Medoid> pointsList){
		final int n = pointsList.size();
		Double[][] distMatrix = new Double[n][n];
		// Symmetric matrix, calculates only the superior diagonal
		for(int i=0; i<n; i++){
			Point p = pointsList.get(i);
			distMatrix[i][i] = 0.0; // matrix diagonal equals zero
			for(int j=i+1; j<n; j++){ 
				Point q = pointsList.get(j);
				double dist = p.dist(q);
				distMatrix[i][j] = dist;
				distMatrix[j][i] = dist;
			}
		}
		
		return distMatrix;
	}
}
