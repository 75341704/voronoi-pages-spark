package uq.spark.query; 

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.broadcast.Broadcast;

import uq.spark.indexing.TrajectoryCollector;
import uq.spark.indexing.TrajectoryTrackTable;
import uq.spark.indexing.VoronoiDiagram;
import uq.spark.indexing.VoronoiPagesRDD;
import uq.spatial.Box;
import uq.spatial.Trajectory;
import uq.spatial.clustering.Cluster;

/**
 * Service to process trajectory queries
 * on a previously built Voronoi index.
 * 
 * @author uqdalves
 */
@SuppressWarnings("serial")
public class QueryProcessingService implements Serializable {
	// trajectory collector service
	private TrajectoryCollector collector = null;
	
	// Query services
	private SelectionQuery selectionQuery = null;
	private CrossQuery crossQuery = null;
	private NearestNeighborQuery nnQuery = null;
	private DensityQuery densityQuery = null;
	
	/**
	 * Service Constructor
	 */
	public QueryProcessingService(final VoronoiPagesRDD voronoiPagesRDD, 
								  final TrajectoryTrackTable trajectoryTrackTable,
								  final Broadcast<VoronoiDiagram> voronoiDiagram){
		// initialize trajectory collector service
		collector = new TrajectoryCollector(voronoiPagesRDD, trajectoryTrackTable);
		
		// initialize query services
		selectionQuery = new SelectionQuery(voronoiPagesRDD, voronoiDiagram.value());
		crossQuery = new CrossQuery(voronoiPagesRDD, voronoiDiagram.value());
		nnQuery = new NearestNeighborQuery(voronoiPagesRDD, voronoiDiagram.value(), trajectoryTrackTable);
		densityQuery = new DensityQuery(voronoiPagesRDD, voronoiDiagram.value());
	}

	/**
	 * Given a rectangular geographic region, and a time window
	 * from t0 to t1, return all trajectories that overlap with
	 * the given region and time window [t0, t1]. 
	 * 
	 * @param whole True if wants to return the whole trajectories.
	 */
	public List<Trajectory> getSpatialTemporalSelection(
			final Box region, 
			final long t0, final long t1, 
			final boolean whole){

		System.out.println("\nRunning Spatial Temporal Selection Query..\n");

		// sub-trajectories only
		List<Trajectory> trajectoryList = new ArrayList<Trajectory>();
		if(!whole){
			List<SelectObject> resultList = // query result
				selectionQuery.runSelectionQuery(region, t0, t1);
			for(SelectObject obj : resultList){
				trajectoryList.addAll(obj.getSubTrajectoryList());
			}
			return trajectoryList;
		}
		
		// collect whole trajectories
		List<String> resultIdList = // query result
				selectionQuery.runSelectionQueryId(region, t0, t1);
		trajectoryList = 
				collector.collectTrajectoriesById(resultIdList).collect();

		return trajectoryList;	
	}
	
	/**
	 * Given a rectangular geographic region, return all trajectories 
	 * that overlap with the given region. 
	 * 
	 * @param whole True if wants to return the whole trajectories.
	 */
	public List<Trajectory> getSpatialSelection(
			final Box region,
			final boolean whole){

		System.out.println("\nRunning Spatial Selection Query..\n");

		// sub-trajectories only
		List<Trajectory> trajectoryList = new ArrayList<Trajectory>();
		if(!whole){
			List<SelectObject> resultList = // query result
					selectionQuery.runSelectionQuery(region);
			for(SelectObject obj : resultList){
				trajectoryList.addAll(obj.getSubTrajectoryList());
			}
			return trajectoryList;
		}
		
		// collect whole trajectories
		List<String> resultIdList = // query result
				selectionQuery.runSelectionQueryId(region);
		trajectoryList = 
				collector.collectTrajectoriesById(resultIdList).collect();
		
		return trajectoryList;
	}
	
	/**
	 * Given a a time window from t0 to t1, return all trajectories 
	 * that overlap with the time window, that is, return all trajectories 
	 * that have been active during [t0, t1].
	 * 
	 * @param whole True if wants to return the whole trajectories.
	 */
	public List<Trajectory> getTimeSlice(
			final long t0, final long t1, 
			final boolean whole){
		
		System.out.println("\nRunning Time Slice Query..\n");

		// sub-trajectories only
		List<Trajectory> trajectoryList = new ArrayList<Trajectory>();
		if(!whole){
			List<SelectObject> resultList =  // query result
				selectionQuery.runSelectionQuery(t0, t1);
			for(SelectObject obj : resultList){
				trajectoryList.addAll(obj.getSubTrajectoryList());
			}
			return trajectoryList;
		}
		
		// collect whole trajectories
		List<String> resultIdList = // query result
				selectionQuery.runSelectionQueryId(t0, t1);
		trajectoryList = 
				collector.collectTrajectoriesById(resultIdList).collect();

		return trajectoryList;	
	}
	
	/**
	 * Given a query trajectory Q, not necessarily in the data set, 
	 * return all trajectories in the data set that crosses with Q.
	 * 
	 * @param whole True if wants to return the whole trajectories.
	 */
	public List<Trajectory> getCrossSelection(
			final Trajectory q, 
			final boolean whole){
		
		System.out.println("\nRunning Cross Selection Query..\n");

		// sub-trajectories only
		List<Trajectory> trajectoryList = new ArrayList<Trajectory>();
		if(!whole){
			List<SelectObject> resultList = // query result
				crossQuery.runCrossQuery(q);
			for(SelectObject obj : resultList){
				trajectoryList.addAll(obj.getSubTrajectoryList());
			}		
			return trajectoryList;
		}
		
		// collect whole trajectories
		List<String> resultIdList = // query result
				crossQuery.runCrossQueryId(q);
		trajectoryList = 
				collector.collectTrajectoriesById(resultIdList).collect();

		return trajectoryList;	
	}	
	
	/**
	 * Given a query trajectory Q, a time interval t0 to t1,
	 * and a integer K, return the K Nearest Neighbors (Most  
	 * Similar Trajectories) from Q, within the interval [t0,t1]. 
	 * 
	 * @param whole True if wants to return the whole trajectories.
	 */
	public Trajectory getNearestNeighbor(
			final Trajectory q, 
			final long t0, final long t1){

		System.out.println("\nRunning NN Query..\n");
		
		// query result
		NearNeighbor nnResult = 
				nnQuery.runNearestNeighborQuery(q, t0, t1);

		return nnResult;
	}
	
	/**
	 * Given a query trajectory Q, a time interval t0 to t1,
	 * and a integer K, return the K Nearest Neighbors (Most  
	 * Similar Trajectories) from Q, within the interval [t0,t1]. 
	 */
	public List<NearNeighbor> getKNearestNeighbors(
			final Trajectory q, 
			final long t0, final long t1, 
			final int k){

		System.out.println("\nRunning " + k + "-NN Query..\n");
		
		// query result
		List<NearNeighbor> resultList = 
				nnQuery.runKNearestNeighborsQuery(q, t0, t1, k);

		return resultList;
	}

	/**
	 * Given a query trajectory Q and a time interval t0 to t1, 
	 * return all trajectories that have Q as their Nearest Neighbor
	 * (Most Similar Trajectory), within the interval [t0,t1].
	 */
	public List<NearNeighbor> getReverseNearestNeighbors(
			final Trajectory q, 
			final long t0, final long t1){
		
		System.out.println("\nRunning Reverse NN Query..\n");
		
		// query result
		List<NearNeighbor> resultList = 
				nnQuery.runReverseNearestNeighborsQuery(q, t0, t1);

		return resultList;		
	}

	/**
	 * Given a geographic region and a density threshold, 
	 * return the regions with trajectory points density 
	 * greater than the given density threshold, within the
	 * given spatial region.
	 * </br></br>
	 * Density threshold is given by distance threshold and number
	 * of points.
	 *  
	 * @param region The query region to search.
	 * @param distanceThresold The maximum distance between points in the clusters.
	 * @param minPoints The minimum number of points in each cluster.
	 * @return 
	 * 
	 * @return A list of clusters of trajectory points.
	 */
	public List<Cluster> getSpatialDensityClusters(
			final Box region, 
			final double distanceThresold, 
			final int minPoints){
		
		System.out.println("\nRunning Spatial Density Query..\n");
		
		// query result
		List<Cluster> resultList = 
				densityQuery.runDensityQuery(region, distanceThresold, minPoints);
			
		return resultList;	
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
	 * @return 
	 * 
	 * @return A list of clusters of trajectory points.
	 */
	public List<Cluster> getSpatialTemporalDensityClusters(
			final Box region, 
			final long t0, final long t1,
			final double distanceThresold, 
			final int minPoints){
		
		System.out.println("\nRunning Spatial Temporal Density Query..\n");
		
		// query result
		List<Cluster> resultList = 
				densityQuery.runDensityQuery(region, t0, t1, distanceThresold, minPoints);
			
		return resultList;	
	}

}