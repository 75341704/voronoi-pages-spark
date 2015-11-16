package uq.spark.query;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;
import uq.spark.SparkEnvInterface;
import uq.spark.indexing.Page;
import uq.spark.indexing.PageIndex;
import uq.spark.indexing.VoronoiDiagram;
import uq.spark.indexing.VoronoiPagesRDD;
import uq.spatial.Box;
import uq.spatial.Trajectory;

/**
 * Implement Cross queries over the RDD.
 * Check for trajectories intersections.
 * Use selection query as filter step.
 * 
 * @author uqdalves
 */
@SuppressWarnings("serial")
public class CrossQuery implements Serializable, SparkEnvInterface {
	private VoronoiPagesRDD pagesRDD;
	private VoronoiDiagram diagram;
	
	/**
	 * Constructor. Receives the PagesRDD and a copy of the Voronoi diagram.
	 */
	public CrossQuery(
			final VoronoiPagesRDD pagesRDD, 
			final VoronoiDiagram diagram) {
		this.pagesRDD = pagesRDD;
		this.diagram = diagram;
	}
	
	/**
	 * Given a query trajectory Q, not necessarily from the data set, 
	 * return all trajectories in the data set that crosses with Q.
	 * </br>
	 * Return a list of trajectory/sub-trajectories (SelectObjects) 
	 * that satisfy the query.
	 */
	public List<SelectObject> runCrossQuery(final Trajectory q){

		/*******************
		 *  FILTERING STEP:
		 *******************/
		
		// Uses the trajectory bounding box to return 
		// candidate trajectories.
		Box region = q.mbr();

		// run a selection query to return trajectories that
		// overlap with the MBR of Q
		SelectionQuery query = 
				new SelectionQuery(pagesRDD, diagram);
		List<SelectObject> candidateList = 
				query.runSelectionQuery(region);
		
		/*******************
		 *  FILTERING STEP:
		 *******************/
		
		// check if the trajectories really intersect
		List<SelectObject> resultList = 
				new LinkedList<SelectObject>();
		for(SelectObject obj : candidateList){
			for(Trajectory t : obj.getSubTrajectoryList()){
				// refine
				if(t.intersect(q)){
					resultList.add(obj);
					break;
				}
			}
		}

		return resultList;
	}

	/**
	 * Given a rectangular geographic region, return the IDs of
	 * the trajectories that overlap with the given region.
	 * </br>
	 * Return a list of trajectory IDs.
	 */
	public List<String> runCrossQueryId(final Trajectory q){
		
		/*******************
		 *  FILTERING STEP:
		 *******************/
		
		// Uses the trajectory bounding box to return 
		// candidate trajectories.
		Box region = q.mbr();
		
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
						// refine
						if(t.intersect(q)){
							selectedList.add(t.id);
							break;
						}
					}	
					return selectedList;
				}
			}).distinct().collect();
					
		return trajectoryIdList;
	}
}
