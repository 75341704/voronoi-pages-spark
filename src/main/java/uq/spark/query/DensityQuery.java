package uq.spark.query;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.broadcast.Broadcast;

import uq.spark.SparkEnvInterface;
import uq.spark.indexing.IndexParamInterface;
import uq.spark.indexing.VoronoiDiagram;
import uq.spark.indexing.VoronoiPagesRDD;
import uq.spatial.Box;
import uq.spatial.Point;
import uq.spatial.Trajectory;
import uq.spatial.clustering.Cluster;
import uq.spatial.clustering.DBSCAN;

/**
 * Implement Density queries over the RDD
 * queries. Uses DBSCAN and POIs approaches to
 * calculate clusters.
 * 
 * @author uqdalves
 */
@SuppressWarnings("serial")
public class DensityQuery implements Serializable, SparkEnvInterface, IndexParamInterface {
	private VoronoiPagesRDD pagesRDD;
	private Broadcast<VoronoiDiagram> diagram;
	
	/**
	 * Constructor. Receives the PagesRDD and an 
	 * instance of the Voronoi diagram.
	 */
	public DensityQuery(
			final VoronoiPagesRDD pagesRDD, 
			final Broadcast<VoronoiDiagram> diagram) {
		this.pagesRDD = pagesRDD;
		this.diagram = diagram;
	}

	/**
	 * Density Query:
	 * Given a geographic region (area) and a density threshold, 
	 * return the regions with trajectory points density greater
	 * than the given density threshold. within the given region.
	 * </br></br>
	 * Density threshold is given by distance threshold and number
	 * of points.
	 *  
	 * @param region The query region to search.
	 * @param distanceThresold The maximum distance between points in the clusters.
	 * @param minPoints The minimum number of points in each cluster.
	 * 
	 * @return A list of clusters of trajectory points.
	 */
	public List<Cluster> runDensityQuery(final Box region, 
			final double distanceThresold, 
			final int minPoints){
		
		/*******************
		 *  FILTERING STEP:
		 *******************/

		// run a selection query to return trajectories that
		// overlap with the query region
		SelectionQuery query = 
				new SelectionQuery(pagesRDD, diagram);
		List<SelectObject> selectedList = 
				query.runSpatialSelection(region);
		
		/*******************
		 *  FILTERING STEP:
		 *******************/
		
		// the list of points to cluster
		List<Point> pointsList = new LinkedList<Point>();
		for(SelectObject obj : selectedList){
			for(Trajectory t : obj.getSubTrajectoryList()){
				pointsList.addAll(t.getPointsList());
			}
		}
		// calculate clusters
		List<Cluster> clusterList = 
				DBSCAN.cluster(pointsList, distanceThresold, minPoints);

		return clusterList;
	}

	/**
	 * Density Query:
	 * Given a geographic region (area), a time interval from t0 to t1,
	 * and a density threshold, return the regions with trajectory 
	 * points density greater than the given density threshold, within 
	 * the given region and time interval [t0, t1]. 
	 * </br></br>
	 * Density threshold is given by distance threshold and number
	 * of points.
	 *  
	 * @param region The query region to search.
	 * @param distanceThresold The maximum distance between points in the clusters.
	 * @param minPoints The minimum number of points in each cluster.
	 * 
	 * @return A list of clusters of trajectory points.
	 */
	public List<Cluster> runDensityQuery(final Box region, 
			final long t0, final long t1, 
			final double distanceThresold, 
			final int minPoints){
		
		/*******************
		 *  FILTERING STEP:
		 *******************/
/*
		// run a selection query to return trajectories that
		// overlap with the query region and time interval
		SelectionQuery query = 
				new SelectionQuery(pagesRDD, diagram);
		List<SelectObject> selectedList = 
				query.runSelectionQuery(region, t0, t1);
		
		/*******************
		 *  FILTERING STEP:
		 *******************/
/*		
		// the list of points to cluster
		List<Point> pointsList = new LinkedList<Point>();
		for(SelectObject obj : selectedList){
			for(Trajectory t : obj.getSubTrajectoryList()){
				pointsList.addAll(t.getPointsList());
			}
		}
		// calculate clusters
		List<Cluster> clusterList = 
				DBSCAN.cluster(pointsList, distanceThresold, minPoints);

		return clusterList;
*/		return null;
	}
}
