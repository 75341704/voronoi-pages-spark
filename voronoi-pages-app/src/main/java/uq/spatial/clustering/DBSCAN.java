package uq.spatial.clustering;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import uq.spatial.Point;

/**
 * Implements the DBSCAN clustering algorithm.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class DBSCAN implements Serializable {

	/**
	 * DBSCAN: density-based algorithm to discovery clusters of 
	 * points.
	 * 
	 * @param pointsList A list of points to cluster
	 * @param eps Epslon: distance threshold
	 * @param minPoints The minimum number of points 
	 * in each cluster
	 * 
	 * @return A list of clusters of points.
	 */
	public static List<Cluster> cluster(final List<Point> pointsList,
				final double eps, final int minPoints){
			List<ClusterPoint> clusterPointsList = 
					new LinkedList<ClusterPoint>();
			for(Point p : pointsList){
				clusterPointsList.add(new ClusterPoint(p));
			}
			return runDBSCAN(clusterPointsList, eps, minPoints);
	 }
	 
	/**
	 * Perform the DBSCAN and return the clusters centers.
	 * 
	 * @return A list points (centroids).
	 */
	public static List<Point> clusterCenters(
				final List<Point> pointsList,
				final double eps, final int minPoints){
			List<Point> centroidList = 
					new LinkedList<Point>();
			List<Cluster> clusterList = cluster(pointsList, eps, minPoints);
			// extract centroids
			for(Cluster cluster : clusterList){
				centroidList.add(cluster.centroid());
			}
			return centroidList;
	 }
	
	/**
	 * Run the clustering.
	 * @return List of clusters.
	 */
	private static List<Cluster> runDBSCAN(List<ClusterPoint> pointsList,
			final double eps, final int minPoints){
			List<Cluster> clusterList = new ArrayList<Cluster>();
					
			int clusterId=1;
			for(ClusterPoint point : pointsList){
				// if point unclassified
				if(!point.isClassified){
					Cluster cluster = new Cluster(clusterId);
					// check if all points in Eps-Neighborhood of  
					// point are density-reachable from it
					if(expandCluster(pointsList, point, cluster, eps, minPoints)){
						clusterList.add(cluster);
						clusterId++;
					}
				}
			}
			
			return clusterList;
	}
	
	/**
	 * Mark the density-reachable points from a given point.
	 */
	private static boolean expandCluster(List<ClusterPoint> pointList, ClusterPoint point, 
			Cluster cluster, final double eps, final int minPoints){
			// seeds
			LinkedList<ClusterPoint> epsNeighborhood = 
					epsNeighborhood(point, eps, pointList);
			
			// no core point
			if(epsNeighborhood.size() < minPoints){
				// mark all points as Noise
				for(ClusterPoint p : epsNeighborhood){
					p.isClassified=false;
				}
				return false;
			}
			// all points in epsNeighborhood are 
			// density-reachable from Point
			else{
				// mark points as classified
				for(ClusterPoint p : epsNeighborhood){
					cluster.addPoint(p);
					p.isClassified=true;
				}
				epsNeighborhood.remove(point);
				
				while(!epsNeighborhood.isEmpty()){
					ClusterPoint currentPt = epsNeighborhood.getFirst();
					// get the EpsNeighbourhood of currentP
					LinkedList<ClusterPoint> result = 
							epsNeighborhood(currentPt, eps, pointList);
					for(ClusterPoint p : result){
						// if it is not in the Eps-Neighborhood
						if(!p.isClassified){
							epsNeighborhood.add(p);
							cluster.addPoint(p);
							p.isClassified = true;
						}
					}
					epsNeighborhood.remove(currentPt);
				}
				return true;
			}
	}
	
	/**
	 * Calculate the Eps-Neighborhood for a given point and
	 * eps threshold. That it, given a point p, a set of points,
	 * and a distance threshold eps, return all the points in 
	 * the set which distance to p is <= eps.
	 * 
	 * @return Eps-Neighborhood
	 */
	private static LinkedList<ClusterPoint> epsNeighborhood(ClusterPoint point, 
			final double eps, List<ClusterPoint> pointList){
		
			LinkedList<ClusterPoint> epsNeighborhood = 
					new LinkedList<ClusterPoint>();
			// TODO: Use R-Tree later to improve the performance
			double distance;
			for(ClusterPoint p : pointList){
				// distance function here
				distance = point.dist(p);
				if(distance <= eps){
					epsNeighborhood.add(p);
				}
			}
			return epsNeighborhood;
	}
}
