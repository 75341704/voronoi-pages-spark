package uq.spatial.clustering;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import uq.spatial.Point;

/**
 * A cluster of points.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class Cluster implements Serializable {
		// the list of points in this cluster
		private List<Point> clusterPointsList = 
				new LinkedList<Point>();
		/**
		 * This cluster Identifier
		 */
		public int clusterId;
		
		/**
		 * Standard constructor.
		 */
		public Cluster(int clusterId) {
			this.clusterId = clusterId;
		}
	
		/**
		 * The list of points in this cluster
		 */
		public List<Point> getPointList() {
			return clusterPointsList;
		}
	
		/**
		 * Add a point to this cluster.
		 */
		public void addPoint(Point point){
			clusterPointsList.add(point);
		}
	
		/** 
		 * Removes the first occurrence of the specified point from 
		 * this cluster, if it is present. 
		 * If this cluster does not contain the element, it is unchanged. 
		 * Returns true if this list contained the specified element.
		 */
		public boolean removePoint(Point p){
			return clusterPointsList.remove(p);
		}
		
		/**
		 * Calculate and return the centroid (point coordinate) 
		 * of this cluster. The centroid is calculated as the
		 * mean of the coordinates of the points in this cluster.
		 * 
		 * @return This cluster center
		 */
		public Point centroid() {
			double xSum = 0.0;
			double ySum = 0.0;
			for(Point point : clusterPointsList){
				xSum += point.x;
				ySum += point.y;
			}
			Point centroid = new Point();
			centroid.x = xSum / size();
			centroid.y = ySum / size();
			
			return centroid;
		}
	
		/**
		 * Number of points in this cluster.
		 */
		public int size(){
			return clusterPointsList.size();
		}
		
		/**
		 * Get the closest point in this 
		 * cluster from its centroid.
		 */
		public Point getClosest(){
			double distance;
			double distanceMax = 0.0;
			Point centroid = centroid();
			Point furthermost = new Point();
			for(Point p : clusterPointsList){
				distance = p.dist(centroid);			
				if(distance > distanceMax){
					distanceMax = distance;
					furthermost = p;
				}
			}
			return furthermost;
		}
		
		/**
		 * Get the distance from the closest point 
		 * in this cluster to its centroid.
		 */
		public double getClosestDistance(){
			double distance;
			double distanceMin = Double.MAX_VALUE;
			Point centroid = centroid();
			for(Point p : clusterPointsList){
				distance = p.dist(centroid);			
				if(distance < distanceMin){
					distanceMin = distance;
				}
			}
			return distanceMin;
		}
		
		/**
		 * Get the furthermost point in this 
		 * cluster from its centroid.
		 */
		public Point getFurthermost(){
			double distance;
			double distanceMin = Double.MAX_VALUE;
			Point centroid = centroid();
			Point closest = new Point();
			for(Point p : clusterPointsList){
				distance = p.dist(centroid);			
				if(distance < distanceMin){
					distanceMin = distance;
					closest = p;
				}
			}
			return closest;
		}
		
		/**
		 * Get the distance from the furthermost point 
		 * in this cluster to its centroid.
		 */
		public double getFurthermostDistance(){
			double distance;
			double distanceMax = 0.0;
			Point centroid = centroid();
			for(Point p : clusterPointsList){
				distance = p.dist(centroid);			
				if(distance > distanceMax){
					distanceMax = distance;
				}
			}
			return distanceMax;
		}
}
