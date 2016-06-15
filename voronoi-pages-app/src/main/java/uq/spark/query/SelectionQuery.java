 package uq.spark.query;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;
import uq.spark.EnvironmentVariables;
import uq.spark.index.IndexParameters;
import uq.spark.index.Page;
import uq.spark.index.PageIndex;
import uq.spark.index.VoronoiDiagram;
import uq.spark.index.VoronoiPagesRDD;
import uq.spatial.Box;
import uq.spatial.Point;
import uq.spatial.Trajectory;

/**
 * Implement Selection queries over the RDD.
 * 
 * @author uqdalves
 */
@SuppressWarnings("serial")
public class SelectionQuery implements Serializable, EnvironmentVariables, IndexParameters {
	private VoronoiPagesRDD pagesRDD;
	private Broadcast<VoronoiDiagram> diagram;

	/**
	 * Constructor. Receives the PagesRDD and a copy of the Voronoi diagram.
	 */
	public SelectionQuery(
			final VoronoiPagesRDD pagesRDD, 
			final Broadcast<VoronoiDiagram> diagram) {
		this.pagesRDD = pagesRDD;
		this.diagram = diagram;
	}

	/**
	 * Given a rectangular geographic region, and a time window
	 * from t0 to t1, return all trajectories that overlap with
	 * the given region and time window [t0, t1].
	 * </br>
	 * Return a list of sub-trajectories (SelectObjects) 
	 * that satisfy the query.
	 */
	public List<Trajectory> runSpatialTemporalSelection(
			final Box region, final long t0, final long t1){
		/*******************
		 *  FILTERING STEP:
		 *******************/
		// Filter pages from the Voronoi RDD (filter)
		JavaPairRDD<PageIndex, Page> filteredPagesRDD = 
				filterSpatialTemporal(region, t0, t1);
		
		/*******************
		 *  REFINEMENT STEP:
		 *******************/
		// Emit, from every page, a list of sub-trajectory that satisfy the query
		JavaPairRDD<String, Trajectory> subTrajectoryRDD = 
			filteredPagesRDD.values().flatMapToPair(new PairFlatMapFunction<Page, String, Trajectory>() {
				public Iterable<Tuple2<String, Trajectory>> call(Page page) throws Exception {
					// the query result
					List<Tuple2<String, Trajectory>> resultList = 
							new ArrayList<Tuple2<String, Trajectory>>();
					// second filter: by MBR
					List<Trajectory> mbrFilterList = page.getTrajectoryTree()
							.getTrajectoriesByMBR(region);
					// refinement:
					// check for sub-trajectories in the given area during [t0,t1].
					for(Trajectory t : mbrFilterList){
						if(t.timeIni() > t1 || t.timeEnd() < t0){continue;}
						Trajectory sub = new Trajectory(t.id);
						for(Point p : t.getPointsList()){
							if(region.contains(p) && p.time >= t0 && p.time <= t1){
								sub.addPoint(p);
							} else if(!sub.isEmpty()){
								resultList.add(new Tuple2<String, Trajectory>(t.id, sub));
								sub = new Trajectory(t.id);
							}
						}
						// last sub-trajectory not add
						if(!sub.isEmpty()){
							resultList.add(new Tuple2<String, Trajectory>(t.id, sub));
						}
					}
					return resultList;	
				}
			});
		
		// aggregate the sub-trajectories by key, and post-process
		return postProcess(subTrajectoryRDD);
	}

	/**
	 * Given a rectangular geographic region, return all trajectories 
	 * that overlap with the given region.
	 * </br>
	 * Return a list of sub-trajectories (SelectObjects) 
	 * that satisfy the query.
	 */
	public List<Trajectory> runSpatialSelection(
			final Box region){
		/*******************
		 *  FILTERING STEP:
		 *******************/
		// Filter pages from the Voronoi RDD (filter)
		JavaPairRDD<PageIndex, Page> filteredPagesRDD = 
				filterSpatial(region);
		
		/*******************
		 *  REFINEMENT STEP:
		 *******************/		
		// Emit, from every page, a list of sub-trajectory that satisfy the query
		JavaPairRDD<String, Trajectory> subTrajectoryRDD = 
			filteredPagesRDD.values().flatMapToPair(new PairFlatMapFunction<Page, String, Trajectory>() {
				public Iterable<Tuple2<String, Trajectory>> call(Page page) throws Exception {
					// the query result
					List<Tuple2<String, Trajectory>> resultList = 
							new ArrayList<Tuple2<String, Trajectory>>();
					// second filter: by MBR
					List<Trajectory> mbrFilterList = page.getTrajectoryTree()
							.getTrajectoriesByMBR(region);
					// refinement:
					// check for sub-trajectories that lies inside the given area.
					for(Trajectory t : mbrFilterList){
						Trajectory sub = new Trajectory(t.id);
						for(Point p : t.getPointsList()){
							if(region.contains(p)){
								sub.addPoint(p);
							} else if(!sub.isEmpty()){
								resultList.add(new Tuple2<String, Trajectory>(t.id, sub));
								sub = new Trajectory(t.id);
							}
						}
						// last sub-trajectory not add
						if(!sub.isEmpty()){
							resultList.add(new Tuple2<String, Trajectory>(t.id, sub));
						}
					}
					return resultList;
				}
			});

		// aggregate the sub-trajectories by key, and post-process
		return postProcess(subTrajectoryRDD);
	}

	/**
	 * Given a time window from t0 to t1, return all trajectories that
	 * overlap with the time window, that is, return all trajectories 
	 * that have been active during [t0, t1].
	 * </br>
	 * Return a list of trajectory/sub-trajectories (SelectObjects) 
	 * that satisfy the query.
	 */
	public List<Trajectory> runTemporalSelection(
			final long t0, final long t1){
		/*******************
		 *  FILTERING STEP:
		 *******************/
		// Filter pages from the Voronoi RDD (filter)
		JavaPairRDD<PageIndex, Page> filteredPagesRDD = 
				filterTemporal(t0, t1);
		
		/*******************
		 *  REFINEMENT STEP:
		 *******************/
		// Emit, from every page, a list of sub-trajectory that satisfy the query
		JavaPairRDD<String, Trajectory> subTrajectoryRDD = 
			filteredPagesRDD.values().flatMapToPair(new PairFlatMapFunction<Page, String, Trajectory>() {
				public Iterable<Tuple2<String, Trajectory>> call(Page page) throws Exception {
					// the query result
					List<Tuple2<String, Trajectory>> resultList = 
							new ArrayList<Tuple2<String, Trajectory>>();
					// refinement:
					// check for sub-trajectory points active during [t0,t1].
					for(Trajectory t : page.getTrajectoryList()){
						if(t.timeIni() > t1 || t.timeEnd() < t0){continue;}
						Trajectory sub = new Trajectory(t.id);
						for(Point p : t.getPointsList()){
							if(p.time >= t0 && p.time <= t1){
								sub.addPoint(p);
							} else if(!sub.isEmpty()){
								resultList.add(new Tuple2<String, Trajectory>(t.id, sub));
								sub = new Trajectory(t.id);
							}
						}
						// last sub-trajectory not add
						if(!sub.isEmpty()){
							resultList.add(new Tuple2<String, Trajectory>(t.id, sub));
						}
					}
					return resultList;			
				}
			});

		// aggregate the sub-trajectories by key, and post-process
		return postProcess(subTrajectoryRDD);
	}
	
	/**
	 * Given a rectangular geographic region, and a time window
	 * from t0 to t1, return the ID of the trajectories that overlap 
	 * with the given region and time window [t0, t1].
	 * </br>
	 * Return a list of trajectory IDs
	 */
	public List<String> runSpatialTemporalSelectionId(
			final Box region, 
			final long t0, final long t1){
		/*******************
		 *  FILTERING STEP:
		 *******************/
		// Filter pages from the Voronoi RDD (filter)
		JavaPairRDD<PageIndex, Page> filteredPagesRDD = 
				filterSpatialTemporal(region, t0, t1);

		/*******************
		 *  REFINEMENT STEP:
		 *******************/
		List<String> trajectoryIdList = 
			// map each page to a list of sub-trajectory IDs that satisfy the query
			filteredPagesRDD.values().flatMap(new FlatMapFunction<Page, String>() {
				public Iterable<String> call(Page page) throws Exception {
					// filter by MBR first
					List<Trajectory> mbrFilterList = page.getTrajectoryTree()
							.getTrajectoriesByMBR(region);
					// a iterable list to return
					List<String> selectedList = 
							new LinkedList<String>();
					// check the sub-trajectories that satisfy the query. 
					for(Trajectory t : mbrFilterList){
						// refinement
						if(t.timeIni() > t1 || t.timeEnd() < t0){continue;}
						for(Point p : t.getPointsList()){
							if(region.contains(p) && 
							   p.time >= t0 && p.time <= t1){
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
	public List<String> runSpatialSelectionId(
			final Box region){
		/*******************
		 *  FILTERING STEP:
		 *******************/
		// Filter pages from the Voronoi RDD (filter)
		JavaPairRDD<PageIndex, Page> filteredPagesRDD = 
				filterSpatial(region);
		
		/*******************
		 *  REFINEMENT STEP:
		 *******************/
		List<String> trajectoryIdList = 
			// map each page to a list of sub-trajectory IDs that satisfy the query
			filteredPagesRDD.values().flatMap(new FlatMapFunction<Page, String>() {
				public Iterable<String> call(Page page) throws Exception {
					// filter by MBR first
					List<Trajectory> mbrFilterList = page.getTrajectoryTree()
							.getTrajectoriesByMBR(region);
					// a iterable list to return
					List<String> selectedList = 
							new LinkedList<String>();
					// check the sub-trajectories that satisfy the query. 
					for(Trajectory t : mbrFilterList){
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
	public List<String> runTemporalSelectionId (
			final long t0, final long t1){
		/*******************
		 *  FILTERING STEP:
		 *******************/
		// Filter pages from the Voronoi RDD (filter)
		JavaPairRDD<PageIndex, Page> filteredPagesRDD = 
				filterTemporal(t0, t1);
		
		/*******************
		 *  REFINEMENT STEP:
		 *******************/
		List<String> trajectoryIdList = 
			// map each page to a list of sub-trajectory IDs that satisfy the query
			filteredPagesRDD.values().flatMap(new FlatMapFunction<Page, String>() {
				public Iterable<String> call(Page page) throws Exception {
					// a iterable list of selected objects IDs to return
					List<String> selectedList = 
							new LinkedList<String>();
					// check in the page the sub-trajectories that satisfy the query. 
					for(Trajectory t : page.getTrajectoryList()){
						// refinement
						if(t.timeIni() > t1 || t.timeEnd() < t0){continue;}
						selectedList.add(t.id);
					}	
					return selectedList;
				}
			}).distinct().collect();

		return trajectoryIdList;
	}

	/**
	 * Filter step of the spatial selection query.
	 * 
	 * @return Return a RDD of candidate Pages.
	 */
	private JavaPairRDD<PageIndex, Page> filterSpatial(
			final Box region){
		// retrieve candidate polygons IDs = VSIs
		// check for polygons that overlaps with the query range
		List<Integer> candidatesVSI = 
				diagram.value().getOverlapingPolygons(region);

		// Retrieve pages from the Voronoi RDD (filter and collect)
		return pagesRDD.filterPagesBySpatialIndex(candidatesVSI);
	}
	
	/**
	 * Filter step of temporal selection query.
	 * 
	 * @return Return a RDD of candidate Pages.
	 */
	private JavaPairRDD<PageIndex, Page> filterTemporal(
			final long t0, final long t1){
		// get page(s) time index to retrieve
		final int TPIini = (int)(t0 / TIME_WINDOW_SIZE) + 1;
		final int TPIend = (int)(t1 / TIME_WINDOW_SIZE) + 1;
				
		// Retrieve pages from the Voronoi RDD (filter)
		return pagesRDD.filterPagesByTimeIndex(TPIini, TPIend);
	}
	
	/**
	 * Filter step of the spatial temporal selection query.
	 * 
	 * @return Return a RDD of candidate Pages.
	 */
	private JavaPairRDD<PageIndex, Page> filterSpatialTemporal(
			final Box region, final long t0, final long t1){
		// retrieve candidate polygons IDs = VSIs
		// check for polygons that overlaps with the query range
		List<Integer> candidatesVSI =
				diagram.value().getOverlapingPolygons(region);

		// get page(s) time index to retrieve
		final int TPIini = (int)(t0 / TIME_WINDOW_SIZE) + 1;
		final int TPIend = (int)(t1 / TIME_WINDOW_SIZE) + 1;
				
		// Filter pages from the Voronoi RDD (filter)
		return pagesRDD.filterPagesByIndex(candidatesVSI, TPIini, TPIend);
	}
	
	/**
	 * The post-processing phase of the selection query (not whole).
	 * </br>
	 * Aggregate sub-trajectories by key and post-process.
	 */
	private List<Trajectory> postProcess(
			final JavaPairRDD<String, Trajectory> subTrajectoryRDD){
		// an empty list of sub-trajectories to start aggregating
		SelectObject emptyObj = new SelectObject();
		// group sub-trajectories belonging to the same parent trajectory
		Function2<SelectObject, Trajectory, SelectObject> seqFunc = 
				new Function2<SelectObject, Trajectory, SelectObject>() {
			public SelectObject call(SelectObject obj, Trajectory t) throws Exception {
				obj.add(t);
				return obj;
			}
		};
		Function2<SelectObject, SelectObject, SelectObject> combFunc = 
				new Function2<SelectObject, SelectObject, SelectObject>() {
			public SelectObject call(SelectObject obj1, SelectObject obj2) throws Exception {
				return obj1.merge(obj2);
			}
		};
		// aggregate the sub-trajectories by key, and post-process
		List<Trajectory> selectList =
			subTrajectoryRDD.aggregateByKey(emptyObj, seqFunc, combFunc)
				.values().flatMap(new FlatMapFunction<SelectObject, Trajectory>() {
					public Iterable<Trajectory> call(SelectObject obj) throws Exception {
						// post-process and return
						return obj.postProcess();
					}
			}).collect();

		return selectList;			
	}
}
