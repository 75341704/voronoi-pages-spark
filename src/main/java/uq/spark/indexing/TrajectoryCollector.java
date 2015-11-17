package uq.spark.indexing;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;
import uq.spatial.Trajectory;

/**
 * Service responsible to collect trajectories from
 * the index structure (PagesRDD). Collect trajectories
 * using the Page index structure and the Trajectory
 * Track Table. Post-process trajectories after collection.
 * 
 * The collection process of trajectories is done in 3 steps:
 * (1) Filter: filter pages containing the trajectories
 * (2) Collect: collect the sub-trajectories from the pages
 * (3) Post-processing: Merge sub-trajectories and remove duplicate points
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class TrajectoryCollector implements Serializable{
	private VoronoiPagesRDD pagesRDD;
	private TrajectoryTrackTable trackTable;
	
	/**
	 * Creates a new collector.
	 */
	public TrajectoryCollector(
			final VoronoiPagesRDD pagesRDD, 
			final TrajectoryTrackTable trackTable) {
		this.pagesRDD = pagesRDD;
		this.trackTable = trackTable;
	}
	
	/**
	 * Given a trajectory ID, retrieve the given 
	 * trajectory from the Pages RDD. Retrieve the
	 * whole trajectory. Post-process after
	 * retrieval.
	 * 
	 * @return Return the given trajectory.
	 * (Note: the given trajectories must be in the dataset)
	 */
	public Trajectory collectTrajectoryById(
			final String id){
		// retrieve from the TTT the indexes of all pages that 
		// contains the trajectories in the list
		HashSet<PageIndex> indexSet = 
				trackTable.collectPageIndexListByTrajectoryId(id);

		// filter pages that contains the specified trajectory
		JavaPairRDD<PageIndex, Page> filteredPagesRDD = 
				pagesRDD.filterPagesByIndex(indexSet);
		
		// map each page to a list of sub-trajectories, and merge them
		Trajectory trajectory =
			filteredPagesRDD.flatMap(new FlatMapFunction<Tuple2<PageIndex,Page>, Trajectory>() {
				public Iterable<Trajectory> call(Tuple2<PageIndex, Page> page) throws Exception {
					return page._2.getSubTrajectoriesById(id);
				}
			}).reduce(new Function2<Trajectory, Trajectory, Trajectory>() {
				public Trajectory call(Trajectory sub1, Trajectory sub2) throws Exception {
					sub1.merge(sub2);
					return sub1;
				}
			});
		
		//post processing
		trajectory = postProcess(trajectory);
		
		return trajectory;
	}
	
	/**
	 * Given a set of trajectory IDs, retrieve 
	 * the given trajectories from the Pages RDD. 
	 * Retrieve whole trajectories.
	 * Post-process after collection.
	 * 
	 * @return Return a distributed dataset (RDD) of trajectories.
	 * (Note: the given trajectories must be in the dataset)
	 */
	public JavaRDD<Trajectory> collectTrajectoriesById(
			final Collection<String> trajectoryIdSet){		
		// retrieve from the TTT the indexes of all pages that 
		// contains the trajectories in the list
		HashSet<PageIndex> indexSet = 
				trackTable.collectPageIndexListByTrajectoryId(trajectoryIdSet);

		// filter pages that contains the specified trajectories
		JavaPairRDD<PageIndex, Page> filteredPagesRDD = 
				pagesRDD.filterPagesByIndex(indexSet);

		// map each page to a list key value pairs containing 
		// the desired trajectories
		// final int NUM_REDUCE_TASKS = trajectoryIdSet.size();
		JavaRDD<Trajectory> trajectoryRDD =
			filteredPagesRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<PageIndex,Page>, String, Trajectory>() {
				public Iterable<Tuple2<String, Trajectory>> call(Tuple2<PageIndex, Page> page) throws Exception {
					// iterable list to return
					List<Tuple2<String, Trajectory>> list = 
							new LinkedList<Tuple2<String, Trajectory>>();
					for(Trajectory sub : page._2.getTrajectoryList()){
						if(trajectoryIdSet.contains(sub.id)){
							list.add(new Tuple2<String, Trajectory>(sub.id, sub));
						}
					}
					return list;
				}
				// merge trajectories by key
			}).reduceByKey(new Function2<Trajectory, Trajectory, Trajectory>() {
				public Trajectory call(Trajectory sub1, Trajectory sub2) throws Exception {
					sub1.merge(sub2);
					return sub1;
				}
			}/*, NUM_REDUCE_TASKS*/).values();
		
		//post processing
		trajectoryRDD = postProcess(trajectoryRDD);
		
		return trajectoryRDD;
	}
	
	/**
	 * Given a set of page index, collect from the RDD the trajectories 
	 * inside the given pages. That is, trajectories inside pages with 
	 * spatial index VSI in VSIlist and time page TPI in [TPIini, TPIend].
	 * </br>
	 * Return whole trajectories (also filter from the PagesRDD other pages
	 * that might contain trajectories in the given pages set.

	 * @return Return a distributed dataset (RDD) of trajectories.
	 * If there is no trajectories in the given page set, then return null. 
	 */
	public JavaRDD<Trajectory> collectTrajectoriesByPageIndex(
			final Collection<Integer> VSIlist, 
			final int TPIini, final int TPIend) {
		// get the list of indexes to filter out
		List<PageIndex> indexList = 
				getIndexCombination(VSIlist, TPIini, TPIend);
		
		return collectTrajectoriesByPageIndex(indexList);
	}
	
	/**
	 * Given a set of page index, collect from the RDD the trajectories 
	 * inside the given pages.
	 * </br>
	 * Return whole trajectories (also filter from the PagesRDD other pages
	 * that might contain trajectories in the given pages set.

	 * @return Return a distributed dataset (RDD) of trajectories.
	 * If there is no trajectories in the given page set, then return null. 
	 */
	public JavaRDD<Trajectory> collectTrajectoriesByPageIndex(
			final Collection<PageIndex> indexSet) {
		// Filter the given pages
		JavaPairRDD<PageIndex, Page> filteredPagesRDD = 
				pagesRDD.filterPagesByIndex(indexSet);

		// check if there is any page to for the given parameters
		// Note: (it might be there is no page in the given time interval for the given polygon)
		if(filteredPagesRDD.count() > 0){
			// Collect the IDs of the trajectories inside the given pages.
			// for each page filtered, return the set of trajectories inside the page.
			final HashSet<String> trajectoryIdSet = 
				filteredPagesRDD.map(new Function<Tuple2<PageIndex,Page>, HashSet<String>>() {
					public HashSet<String> call(Tuple2<PageIndex, Page> page) throws Exception {
						return page._2.getTrajectoryIdSet();
					}
				}).reduce(new Function2<HashSet<String>, HashSet<String>, HashSet<String>>() {
					public HashSet<String> call(HashSet<String> set1, HashSet<String> set2) throws Exception {
						set1.addAll(set2);
						return set1;
					}
				});

			// retrieve from the TTT the indexes of all pages that 
			// contains the trajectories in the list.
			// skip the pages already retrieved (collect the difference set)
			HashSet<PageIndex> diffIndexSet = 
					trackTable.collectPageIndexListByTrajectoryId(trajectoryIdSet, indexSet);

			// filter the other pages that contain the trajectories (difference set)
			JavaPairRDD<PageIndex, Page> diffPagesRDD = 
					pagesRDD.filterPagesByIndex(diffIndexSet);

			// groups the two Page RDDs (union set)
			filteredPagesRDD = filteredPagesRDD.union(diffPagesRDD);

			// retrieved the desired trajectories
			// map each page to a list of key value pairs containing 
			// the desired trajectories
			JavaRDD<Trajectory> trajectoryRDD =
				filteredPagesRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<PageIndex,Page>, String, Trajectory>() {
					public Iterable<Tuple2<String, Trajectory>> call(Tuple2<PageIndex, Page> page) throws Exception {
						// iterable list to return
						List<Tuple2<String, Trajectory>> list = 
								new LinkedList<Tuple2<String, Trajectory>>();
						for(Trajectory sub : page._2.getTrajectoryList()){
							if(trajectoryIdSet.contains(sub.id)){
								list.add(new Tuple2<String, Trajectory>(sub.id, sub));
							}
						}
						return list;
					}
					// merge trajectories by key
				}).reduceByKey(new Function2<Trajectory, Trajectory, Trajectory>() {
					public Trajectory call(Trajectory sub1, Trajectory sub2) throws Exception {
						sub1.merge(sub2);
						return sub1;
					}
				}).values();
			
			// post processing
			trajectoryRDD = postProcess(trajectoryRDD);		
			
			return trajectoryRDD;
		}
		return null;
	}
		
	/**
	 * Given a set of page index, collect from the RDD the IDs of the trajectories
	 * inside the given pages. That is, return the IDs of trajectories
	 * in page with VSI index in VSIlist and TPI index in [TPIini, TPIend].
	 *
	 * @return A set of distinct trajectory IDs.
	 * If there is no trajectories in the given page set, 
	 * then return an empty set.
	 */
	public HashSet<String> collectTrajectoriesIdByPageIndex(
			final Collection<Integer> VSIlist, 
			final int TPIini, final int TPIend) {
		// get the list of indexes to filter out
		List<PageIndex> indexList = 
				getIndexCombination(VSIlist, TPIini, TPIend);
		
		return collectTrajectoriesIdByPageIndex(indexList);
	}
	
	/**
	 * Given a set of page index, collect from the RDD 
	 * the IDs of the trajectories inside the given pages.
	 * 
	 * @return A set of distinct trajectory IDs.
	 * If there is no trajectories in the given page set, 
	 * then return an empty set.
	 */
	public HashSet<String> collectTrajectoriesIdByPageIndex(
			final Collection<PageIndex> indexSet) {
		// Filter the given pages
		JavaPairRDD<PageIndex, Page> filteredPagesRDD = 
				pagesRDD.filterPagesByIndex(indexSet);

		// for each page filtered, return the set of trajectories inside the page.
		HashSet<String> trajectoryIdSet = new HashSet<String>();
		if(filteredPagesRDD.count() > 0){
			trajectoryIdSet = 
				filteredPagesRDD.map(new Function<Tuple2<PageIndex,Page>, HashSet<String>>() {
					public HashSet<String> call(Tuple2<PageIndex, Page> page) throws Exception {
						return page._2.getTrajectoryIdSet();
					}
				}).reduce(new Function2<HashSet<String>, HashSet<String>, HashSet<String>>() {
					public HashSet<String> call(HashSet<String> set1, HashSet<String> set2) throws Exception {
						set1.addAll(set2);
						return set1;
					}
				});
		}
		return trajectoryIdSet;
	}
	
	/**
	 * Given a list of spatial indexes, and a
	 * time index interval, return the page index
	 * combination.
	 */
	private List<PageIndex> getIndexCombination(
			final Collection<Integer> VSIlist, 
			final int TPIini, final int TPIend){
		List<PageIndex> combineList = 
				new LinkedList<PageIndex>();
		for(Integer vsi : VSIlist){
			for(int tpi = TPIini; tpi <= TPIend; tpi++){
				combineList.add(new PageIndex(vsi, tpi));
			}
		}
		return combineList;
	}
	
	/**
	 * Post processing operation.
	 * </br>
	 * Sorts the trajectory points by time stamp
	 * and removes any duplicates form the map phase.
	 * 
	 * @return A post-processed trajectory
	 */
	private Trajectory postProcess(Trajectory t) {
		t.sort();
		int size = t.size();
		for(int i = 0; i < size-1; i++){
			if(t.get(i).equals(t.get(i+1))){
				t.removePoint(i);
				size--;
				--i;
			}
		}
		return t;
	}

	/**
	 * Post processing operation. Done in parallel.
	 * </br>
	 * Sorts the trajectory points by time stamp
	 * and removes any duplicates from the map phase.
	 * 
	 * @return A post-processed RDD of trajectories
	 */
	private JavaRDD<Trajectory> postProcess(
			JavaRDD<Trajectory> trajectoryRDD){
		// map each trajec to its post-process version
		trajectoryRDD = 
			trajectoryRDD.map(new Function<Trajectory, Trajectory>() {
				public Trajectory call(Trajectory t) throws Exception {
					return postProcess(t);
				}
			});
		return trajectoryRDD;
	}

}
