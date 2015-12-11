package uq.spark.query;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import uq.spark.SparkEnvInterface;
import uq.spark.index.IndexParamInterface;
import uq.spark.index.PageIndex;
import uq.spark.index.TrajectoryCollector;
import uq.spark.index.TrajectoryTrackTable;
import uq.spark.index.VoronoiDiagram;
import uq.spark.index.VoronoiPagesRDD;
import uq.spatial.Trajectory;
import uq.spatial.distance.DistanceService;

/**
 * Implement Most Similar Trajectory (nearest neighbor) 
 * queries over the RDD. Uses a Edit distance based 
 * measure (EDwP).
 * 
 * @author uqdalves
 */
@SuppressWarnings("serial")
public class NearestNeighborQuery implements Serializable, SparkEnvInterface, IndexParamInterface {
	private VoronoiPagesRDD pagesRDD;
	private TrajectoryTrackTable trackTable;
	private Broadcast<VoronoiDiagram> diagram;
	
	// trajectory collector service
	private TrajectoryCollector collector = null;
	
	// service to calculate trajectory distances
	private final DistanceService distService = 
			new DistanceService();
	
	// NN comparator
	private final NeighborComparator<NearNeighbor> nnComparator = 
			new NeighborComparator<NearNeighbor>();
	
	/**
	 * Constructor. Receives the PagesRDD, a instance of  
	 * the Voronoi diagram and the track table.
	 */
	public NearestNeighborQuery(
			final VoronoiPagesRDD pagesRDD, 
			final Broadcast<VoronoiDiagram> diagram,
			final TrajectoryTrackTable trackTable) {
		this.pagesRDD = pagesRDD;
		this.diagram = diagram;
		this.trackTable = trackTable;
		collector = new TrajectoryCollector(this.pagesRDD, this.trackTable);
	}

	/**
	 * NN Query:
	 * Given a query trajectory Q and a time interval t0 to t1,
	 * return the Nearest Neighbor (Most Similar Trajectory) 
	 * from Q, within the interval [t0,t1]. 
	 */
	public NearNeighbor runNearestNeighborQuery(
			final Trajectory q, 
			final long t0, final long t1){
		return runKNearestNeighborsQuery(q, t0, t1, 1).get(0);
	}
	
	/**
	 * K-NN Query:
	 * Given a query trajectory Q, a time interval t0 to t1,
	 * and a integer K, return the K Nearest Neighbors (Most  
	 * Similar Trajectories) from Q, within the interval [t0,t1]. 
	 */
	public List<NearNeighbor> runKNearestNeighborsQuery(
			final Trajectory q, 
			final long t0, final long t1, 
			final int k){
		/*******************
		 * FIRST FILTER STEP:
		 * 
		 * Find the first k-NN candidates
		 * in the neighborhood of Q
		 *******************/
		// retrieve candidate polygons IDs = VSIs
		// check for polygons that intersect with Q
		HashSet<Integer> candidatePolygons = 
				diagram.value().getClosestPolygons(q);

		// for every polygon that overlap with Q, 
		// add their neighbors to the candidates list
		HashSet<Integer> neighbors = new HashSet<Integer>();
		for(int candidate : candidatePolygons){
			neighbors.addAll(diagram.value().getPolygonByPivotId(candidate)
					.getAdjacentList());
		} candidatePolygons.addAll(neighbors);

		// get page(s) time index to retrieve
		final int TPIini = (int)(t0 / TIME_WINDOW_SIZE) + 1;
		final int TPIend = (int)(t1 / TIME_WINDOW_SIZE) + 1;

		// collect candidate trajectories inside the given pages (whole trajectories)
		JavaRDD<Trajectory> candidateRDD = 
				collector.collectTrajectoriesByPageIndex(candidatePolygons, TPIini, TPIend);	

		/*******************
		 * FIRST REFINEMENT:
		 *******************/
		// get first candidates
		List<NearNeighbor> candidatesList = new LinkedList<NearNeighbor>();
		candidatesList = getCandidatesNN(candidateRDD, candidatesList, q, t0, t1);

		// if this is a 1-NN query, return the first
		if(k == 1){
			return candidatesList.subList(1, 2);
		} 
		
		/*******************
		 * SECOND FILTER STEP:
		 * 
		 * Find the next (k-1)-NN iteratively
		 *******************/
		// collect the first k
		if(candidatesList.size() > k){
			candidatesList = 
					new ArrayList<NearNeighbor>(candidatesList.subList(0, k));
		}
		for(int i=0; i<k-1; i++){
			// get the previous NN
			NearNeighbor nn_i = candidatesList.get(i);

			// retrieve the neighbor polygons of VPs(nn_i)
			// except those already retrieved.
			HashSet<PageIndex> nnIndexList = 
					trackTable.collectPageIndexListByTrajectoryId(nn_i.id);
			neighbors = new HashSet<Integer>();
			for(PageIndex index : nnIndexList){
				// add new polygons
				if(!candidatePolygons.contains(index.VSI)){
					neighbors.add(index.VSI);
				}
				// add adjacent of new polygons
				for(int vsi : diagram.value().getPolygonByPivotId(index.VSI).getAdjacentList()){
					if(!candidatePolygons.contains(vsi)){
						neighbors.add(vsi);
					}
				}	
			} candidatePolygons.addAll(neighbors);
		
			// if there is any new neighbor partition to retrieve
			if(!neighbors.isEmpty()){
				// collect trajectories inside the given neighbor 
				// pages (whole trajectories)
				candidateRDD = 
						collector.collectTrajectoriesByPageIndex(neighbors, TPIini, TPIend);
				
				/*******************
				 * SECOND REFINEMENT:
				 *******************/
				if(candidateRDD != null){ 
					// collect next NN candidates
					candidatesList = getCandidatesNN(candidateRDD, candidatesList, q, t0, t1);
				}
				// collect the first k
				if(candidatesList.size() > k){
					candidatesList = new ArrayList<NearNeighbor>(candidatesList.subList(0, k));	
				}
 			}
		}

		return candidatesList;
	}

	/**
	 * RNN Query:
	 * Given a query trajectory Q, a time interval t0 to t1,
	 * return all trajectories that have Q as their Nearest Neighbor
	 * (Most Similar Trajectory), within the interval [t0,t1]. 
	 * 
	 * @return Return a iterator stream of trajectories.
	 */
	public Iterator<Trajectory> runReverseNearestNeighborsQuery( 
			final Trajectory q, 
			final long t0, final long t1){

		/*******************
		 * FILTER STEP:
		 * 
		 * Find the first k-NN candidates
		 * in the neighborhood of Q
		 *******************/
		// retrieve candidate polygons IDs = VSIs
		// check for polygons that overlaps with Q
		HashSet<Integer> candidatePolygons = 
				diagram.value().getClosestPolygons(q);

		// for every polygon that overlap with Q, 
		// add their neighbors to the candidates list
		HashSet<Integer> neighbors = new HashSet<Integer>();
		for(int candidate : candidatePolygons){
			neighbors.addAll(diagram.value().getPolygonByPivotId(candidate)
					.getAdjacentList());
		} candidatePolygons.addAll(neighbors);

		// get page(s) time index to retrieve
		final int TPIini = (int)(t0 / TIME_WINDOW_SIZE) + 1;
		final int TPIend = (int)(t1 / TIME_WINDOW_SIZE) + 1;

		// collect candidate trajectories inside the given pages (whole trajectories)
		JavaRDD<Trajectory> candidateRDD = collector
				.collectTrajectoriesByPageIndex(candidatePolygons, TPIini, TPIend);	
		
		/*******************
		 * REFINEMENT:
		 * 
		 * For each trajectory in the neighborhood of Q,  
		 * find its NN and collect those with NN = Q
		 *******************/
		// collect the candidate trajectories as a parallel stream
		/*Stream<Trajectory> candidatesList = 
				candidateRDD.collect().parallelStream();
		System.out.println("Candidates RNN Size: " + candidatesList.count());
		Iterator<Trajectory> rnnItr = 	
			candidatesList.filter(new Predicate<Trajectory>() {
				public boolean test(Trajectory t) {
					// get the NN of this trajectory
					NearNeighbor nn = runNearestNeighborQuery(t, t0, t1);
					double dist = distService.EDwP(q, t);
					// check if the query object is closer to T than NN(T)
					if(dist <= nn.distance){
						return true;
					}
					return false;
				}
			}).iterator();
			
		return rnnItr;*/

		return null;
	}
	
	/**
	 * Check the trajectories time-stamp and 
	 * calculate the distance between every trajectory in the list to
	 * the query trajectory, return a sorted list of NN by distance.
	 * </br>
	 * Calculate the NN object only for the new trajectories (i.e.  
	 * trajectories not contained in current list).
	 * 
	 * @return Return the updated current NN list.
	 */
	private List<NearNeighbor> getCandidatesNN(
			final JavaRDD<Trajectory> candidateRDD, 
			final List<NearNeighbor> currentList,
			final Trajectory q,
			final long t0, final long t1){

		List<NearNeighbor> nnCandidatesList = 
			// filter out new trajectories, and refine time
			candidateRDD.filter(new Function<Trajectory, Boolean>() {
				public Boolean call(Trajectory t) throws Exception {
					if(t.timeIni() > t1 || t.timeEnd() < t0){
						return false;
					}
					return (!currentList.contains(t));
				}
			// map each new trajectory in the candidates list to a NN
			}).map(new Function<Trajectory, NearNeighbor>() {
				public NearNeighbor call(Trajectory t) throws Exception {
					NearNeighbor nn = new NearNeighbor(t);
					nn.distance = distService.EDwP(q, t);
					return nn;			}
			}).collect();
		// add new candidates
		currentList.addAll(nnCandidatesList);
		// sort by distance to Q
		Collections.sort(currentList, nnComparator);
		
		return currentList;
	}

}
