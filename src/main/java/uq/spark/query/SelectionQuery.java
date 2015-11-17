 package uq.spark.query;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;
import uq.spark.SparkEnvInterface;
import uq.spark.indexing.IndexParamInterface;
import uq.spark.indexing.Page;
import uq.spark.indexing.PageIndex;
import uq.spark.indexing.VoronoiDiagram;
import uq.spark.indexing.VoronoiPagesRDD;
import uq.spatial.Box;
import uq.spatial.Point;
import uq.spatial.Trajectory;

/**
 * Implement Selection queries over the RDD.
 * 
 * @author uqdalves
 */
@SuppressWarnings("serial")
public class SelectionQuery implements Serializable, SparkEnvInterface, IndexParamInterface {
	private VoronoiPagesRDD pagesRDD;
	private VoronoiDiagram diagram;

	/**
	 * Constructor. Receives the PagesRDD and a copy of the Voronoi diagram.
	 */
	public SelectionQuery(
			final VoronoiPagesRDD pagesRDD, 
			final VoronoiDiagram diagram) {
		this.pagesRDD = pagesRDD;
		this.diagram = diagram;
	}

	/**
	 * Given a rectangular geographic region, and a time window
	 * from t0 to t1, return all trajectories that overlap with
	 * the given region and time window [t0, t1].
	 * </br>
	 * Return a list of trajectory/sub-trajectories (SelectObjects) 
	 * that satisfy the query.
	 */
	public List<SelectObject> runSelectionQuery(final Box region, final long t0, final long t1){

		/*******************
		 *  FILTERING STEP:
		 *******************/
		
		// retrieve candidate polygons IDs = VSIs
		// check for polygons that overlaps with the query range
		List<Integer> candidatesVSI =
				diagram.getOverlapingPolygons(region);

		// get page(s) time index to retrieve
		final int TPIini = (int)(t0 / TIME_WINDOW_SIZE) + 1;
		final int TPIend = (int)(t1 / TIME_WINDOW_SIZE) + 1;
				
		// pages to retrieve <VSI, TPI>
		List<PageIndex> indexList = new ArrayList<PageIndex>();
		for(Integer VSI : candidatesVSI){
			for(int TPI = TPIini; TPI <= TPIend; TPI++){
				indexList.add(new PageIndex(VSI, TPI));
			}
		}

		// Filter pages from the Voronoi RDD (filter)
		JavaPairRDD<PageIndex, Page> filteredPagesRDD = 
				pagesRDD.filterPagesByIndex(indexList);
		
		/*******************
		 *  REFINEMENT STEP:
		 *******************/

		// A MapReduce function to check which sub-trajectories in the page satisfy the query.
		// Emit a <Tid, SubTrajectory> pair for every sub-trajectory satisfying the query
		List<SelectObject> selectObjectList = filteredPagesRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<PageIndex,Page>, String, SelectObject>() {
			public Iterable<Tuple2<String, SelectObject>> call(Tuple2<PageIndex, Page> page) throws Exception {
				// a iterable map to return
				List<Tuple2<String, SelectObject>> iterableMap = 
						new LinkedList<Tuple2<String,SelectObject>>();
				// check in the page the sub-trajectories that satisfy the query. 
				for(Trajectory t : page._2.getTrajectoryList()){
					for(Point p : t.getPointsList()){
						// refinement
						if(region.contains(p) && p.time >= t0 && p.time <= t1){
							SelectObject obj = new SelectObject(t, page._1);
							iterableMap.add(new Tuple2<String, SelectObject>(t.id, obj));
							break;
						}
					}
				}				
				return iterableMap;
			}
		}).reduceByKey(new Function2<SelectObject, SelectObject, SelectObject>() {
			// Group/Merge sub-trajectories by time-stamp, remove duplicate points. 
			public SelectObject call(SelectObject obj1, SelectObject obj2) throws Exception {
				// group sub-trajectories by id: post-processing
				obj1.group(obj2);
				return obj1;
			}
		}/*, NUM_TASKS_QUERY*/).values().collect();

		return selectObjectList;
	}

	/**
	 * Given a rectangular geographic region, return all trajectories 
	 * that overlap with the given region.
	 * </br>
	 * Return a list of trajectory/sub-trajectories (SelectObjects) 
	 * that satisfy the query.
	 */
	public List<SelectObject> runSelectionQuery(final Box region){
		
		/*******************
		 *  FILTERING STEP:
		 *******************/
		
		// retrieve candidate polygons IDs = VSIs
		// check for polygons that overlaps with the query range
		List<Integer> candidatesVSI = 
				diagram.getOverlapingPolygons(region);

		// Retrieve pages from the Voronoi RDD (filter and collect)
		JavaPairRDD<PageIndex, Page> filteredPagesRDD = 
				pagesRDD.filterPagesBySpatialIndex(candidatesVSI);
		
		/*******************
		 *  REFINEMENT STEP:
		 *******************/
		
		// A MapReduce function to check which sub-trajectories in the page satisfy the query.
		// Emit a <Tid, SubTrajectory> pair for every sub-trajectory satisfying the query
		List<SelectObject> selectObjectList = filteredPagesRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<PageIndex,Page>, String, SelectObject>() {
			public Iterable<Tuple2<String, SelectObject>> call(Tuple2<PageIndex, Page> page) throws Exception {
				// a iterable map to return
				List<Tuple2<String, SelectObject>> iterableMap = 
						new LinkedList<Tuple2<String,SelectObject>>();
				// check in the page the sub-trajectories that satisfy the query. 
				for(Trajectory t : page._2.getTrajectoryList()){
					for(Point p : t.getPointsList()){
						// refinement
						if(region.contains(p)){
							SelectObject obj = new SelectObject(t, page._1);
							iterableMap.add(new Tuple2<String, SelectObject>(t.id, obj));
							break;
						}
					}
				}				
				return iterableMap;
			}
		}).reduceByKey(new Function2<SelectObject, SelectObject, SelectObject>() {
			// Group/Merge sub-trajectories by time-stamp, remove duplicate points. 
			public SelectObject call(SelectObject obj1, SelectObject obj2) throws Exception {
				// group sub-trajectories by id: post-processing
				obj1.group(obj2);
				return obj1;
			}
		}/*, NUM_TASKS_QUERY*/).values().collect();

		return selectObjectList;
	}

	/**
	 * Given a time window from t0 to t1, return all trajectories that
	 * overlap with the time window, that is, return all trajectories 
	 * that have been active during [t0, t1].
	 * </br>
	 * Return a list of trajectory/sub-trajectories (SelectObjects) 
	 * that satisfy the query.
	 */
	public List<SelectObject> runSelectionQuery(final long t0, final long t1){

		/*******************
		 *  FILTERING STEP:
		 *******************/

		// get page(s) time index to retrieve
		final int TPIini = (int)(t0 / TIME_WINDOW_SIZE) + 1;
		final int TPIend = (int)(t1 / TIME_WINDOW_SIZE) + 1;
				
		// Retrieve pages from the Voronoi RDD (filter)
		JavaPairRDD<PageIndex, Page> filteredPagesRDD = 
				pagesRDD.filterPagesByTimeIndex(TPIini, TPIend);

		/*******************
		 *  REFINEMENT STEP:
		 *******************/
		
		// A MapReduce function to check which sub-trajectories in the page satisfy the query.
		// Emit a <Tid, SubTrajectory> pair for every sub-trajectory satisfying the query
		List<SelectObject> selectObjectList = filteredPagesRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<PageIndex,Page>, String, SelectObject>() {
			public Iterable<Tuple2<String, SelectObject>> call(Tuple2<PageIndex, Page> page) throws Exception {
				// a iterable map to return
				List<Tuple2<String, SelectObject>> iterableMap = 
						new LinkedList<Tuple2<String,SelectObject>>();
				// check in the page the sub-trajectories that satisfy the query. 
				for(Trajectory t : page._2.getTrajectoryList()){
					for(Point p : t.getPointsList()){
						// refinement
						if(p.time >= t0 && p.time <= t1){
							SelectObject obj = new SelectObject(t, page._1);
							iterableMap.add(new Tuple2<String, SelectObject>(t.id, obj));
							break;
						}
					}
				}				
				return iterableMap;
			}
		}).reduceByKey(new Function2<SelectObject, SelectObject, SelectObject>() {
			// Group/Merge sub-trajectories by time-stamp, remove duplicate points. 
			public SelectObject call(SelectObject obj1, SelectObject obj2) throws Exception {
				// group sub-trajectories by id: post-processing
				obj1.group(obj2);
				return obj1;
			}
		}/*, NUM_TASKS_QUERY*/).values().collect();

		return selectObjectList;
	}
	
	/**
	 * Given a rectangular geographic region, and a time window
	 * from t0 to t1, return the ID of the trajectories that overlap 
	 * with the given region and time window [t0, t1].
	 * </br>
	 * Return a list of trajectory IDs
	 */
	public List<String> runSelectionQueryId(final Box region, final long t0, final long t1){

		/*******************
		 *  FILTERING STEP:
		 *******************/
		
		// retrieve candidate polygons IDs = VSIs
		// check for polygons that overlaps with the query range
		List<Integer> candidatesVSI =
				diagram.getOverlapingPolygons(region);

		// get page(s) time index to retrieve
		final int TPIini = (int)(t0 / TIME_WINDOW_SIZE) + 1;
		final int TPIend = (int)(t1 / TIME_WINDOW_SIZE) + 1;
				
		// pages to retrieve <VSI, TPI>
		List<PageIndex> indexList = new ArrayList<PageIndex>();
		for(Integer VSI : candidatesVSI){
			for(int TPI = TPIini; TPI <= TPIend; TPI++){
				indexList.add(new PageIndex(VSI, TPI));
			}
		}

		// Filter pages from the Voronoi RDD (filter)
		JavaPairRDD<PageIndex, Page> filteredPagesRDD = 
				pagesRDD.filterPagesByIndex(indexList);
		
		/*******************
		 *  REFINEMENT STEP:
		 *******************/

		List<String> trajectoryIdList = 
			// map each page to a list of sub-trajectory IDs that satisfy the query
			filteredPagesRDD.flatMap(new FlatMapFunction<Tuple2<PageIndex,Page>, String>() {
				public Iterable<String> call(Tuple2<PageIndex, Page> page) throws Exception {
					// a iterable list of selected objects IDs to return
					List<String> selectedList = 
							new LinkedList<String>();
					// check in the page the sub-trajectories that satisfy the query. 
					for(Trajectory t : page._2.getTrajectoryList()){
						for(Point p : t.getPointsList()){
							// refinement
							if(region.contains(p) && p.time >= t0 && p.time <= t1){
								selectedList.add(t.id);
								break;
							}
						}
					}	
					return selectedList;
				}
			}).distinct().collect();
				
		return trajectoryIdList;
	}
	
	/**
	 * Given a rectangular geographic region, return the IDs of
	 * the trajectories that overlap with the given region.
	 * </br>
	 * Return a list of trajectory IDs.
	 */
	public List<String> runSelectionQueryId(final Box region){
		
		/*******************
		 *  FILTERING STEP:
		 *******************/
		
		// retrieve candidate polygons IDs = VSIs
		// check for polygons that overlaps with the query range
		List<Integer> candidatesVSI = 
				diagram.getOverlapingPolygons(region);

		// Retrieve pages from the Voronoi RDD (filter and collect)
		JavaPairRDD<PageIndex, Page> filteredPagesRDD = 
				pagesRDD.filterPagesBySpatialIndex(candidatesVSI);
		
		/*******************
		 *  REFINEMENT STEP:
		 *******************/
		
		List<String> trajectoryIdList = 
			// map each page to a list of sub-trajectory IDs that satisfy the query
			filteredPagesRDD.flatMap(new FlatMapFunction<Tuple2<PageIndex,Page>, String>() {
				public Iterable<String> call(Tuple2<PageIndex, Page> page) throws Exception {
					// a iterable list of selected objects IDs to return
					List<String> selectedList = 
							new LinkedList<String>();
					// check in the page the sub-trajectories that satisfy the query. 
					for(Trajectory t : page._2.getTrajectoryList()){
						for(Point p : t.getPointsList()){
							// refinement
							if(region.contains(p)){
								selectedList.add(t.id);
								break;
							}
						}
					}	
					return selectedList;
				}
			}).distinct().collect();
					
		return trajectoryIdList;
	}
	
	/**
	 * Given a time window from t0 to t1, return the IDs of  
	 * trajectories that overlap with the time window, that is, 
	 * trajectories that have been active during [t0, t1].
	 * </br>
	 * Return a list of trajectory IDs.
	 */
	public List<String> runSelectionQueryId(final long t0, final long t1){

		/*******************
		 *  FILTERING STEP:
		 *******************/

		// get page(s) time index to retrieve
		final int TPIini = (int)(t0 / TIME_WINDOW_SIZE) + 1;
		final int TPIend = (int)(t1 / TIME_WINDOW_SIZE) + 1;
				
		// Retrieve pages from the Voronoi RDD (filter)
		JavaPairRDD<PageIndex, Page> filteredPagesRDD = 
				pagesRDD.filterPagesByTimeIndex(TPIini, TPIend);

		/*******************
		 *  REFINEMENT STEP:
		 *******************/
		
		List<String> trajectoryIdList = 
			// map each page to a list of sub-trajectory IDs that satisfy the query
			filteredPagesRDD.flatMap(new FlatMapFunction<Tuple2<PageIndex,Page>, String>() {
				public Iterable<String> call(Tuple2<PageIndex, Page> page) throws Exception {
					// a iterable list of selected objects IDs to return
					List<String> selectedList = 
							new LinkedList<String>();
					// check in the page the sub-trajectories that satisfy the query. 
					for(Trajectory t : page._2.getTrajectoryList()){
						for(Point p : t.getPointsList()){
							// refinement
							if(p.time >= t0 && p.time <= t1){
								selectedList.add(t.id);
								break;
							}
						}
					}	
					return selectedList;
				}
			}).distinct().collect();
					
		return trajectoryIdList;
	}
}
