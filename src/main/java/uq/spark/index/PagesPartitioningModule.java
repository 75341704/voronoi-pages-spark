package uq.spark.index;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

import uq.fs.FileReader;
import uq.spark.EnvironmentVariables;
import uq.spatial.Point;
import uq.spatial.Trajectory;

/**
 * Service to partition and index trajectory sample points.
 * Build the Voronoi diagram, the Voronoi pages and
 * the trajectory track table.
 * 
 * @author uqdalves
 */
@SuppressWarnings("serial")
public class PagesPartitioningModule implements Serializable, EnvironmentVariables, IndexParamInterface {
	// The Voronoi diagram partition itself.
	private VoronoiPagesRDD voronoiPagesRDD = null;

	// Hash table to keep track of trajectories within partitions
	private TrajectoryTrackTable trajectoryTrackTable = null;
	
	// the Voronoi diagram with a list of polygons. Read-only variable to be
	// cached on each machine. Contains the pivots and edges of each polygon.
	private Broadcast<VoronoiDiagram> voronoiDiagram = null;
	
	/**
	 * Return the Voronoi RDD with Voronoi pages
	 * created in this service.
	 */
	public VoronoiPagesRDD getVoronoiPagesRDD(){
		return voronoiPagesRDD;
	}
	
	/**
	 * Return the Voronoi Diagram built in this service.
	 * Broadcasted diagram.
	 * <br>
	 * Abstract representation only, with pivots and edges.
	 */
	public Broadcast<VoronoiDiagram> getVoronoiDiagram(){
		return voronoiDiagram;
	}
	
	/**
	 * Return a Trajectory Track Table to keep track of 
	 * trajectories across partitions.
	 */
	public TrajectoryTrackTable getTrajectoryTrackTable(){
		return trajectoryTrackTable;
	}
	
	/**
	 * Run data partitioning and indexing.
	 * Build the Voronoi diagram, assign trajectory points to
	 *  Voronoi pages, and build the trajectory track table.
	 */
	public void run(){
     	/**
     	 * BUILD THE VORONOI DIAGRAM
     	 */
     	// Read the pivots and generate the diagram and broadcast.
 		VoronoiDiagram diagram = new VoronoiDiagram(FileReader.readPivots(K));
     	voronoiDiagram = SC.broadcast(diagram);
  	
		/**
		 * READ DATA AND BUILD VORONOI INDEX (MAP)
		 */
     	// First map to convert the input file to trajectory objects
		JavaRDD<Trajectory> trajectoryRDD = FileReader.readDataAsTrajectoryRDD();
     	// Second map to assign each sub-trajectory to a Voronoi Page index
     	JavaPairRDD<PageIndex, Trajectory> trajectoryToPageIndexRDD = 
     			mapTrajectoriesToPageIndex(trajectoryRDD, getVoronoiDiagram());

     	/**
     	 * BUILD VORONOI PAGES RDD (REDUCE)
     	 */
     	// group pages by Index (buid the pages)
      	voronoiPagesRDD = new VoronoiPagesRDD();
		voronoiPagesRDD.build(trajectoryToPageIndexRDD);
		
		/**
		 * BUILD TRAJECTORY TRACK TABLE (MAP/REDUCE)
		 */
     	trajectoryTrackTable = new TrajectoryTrackTable();
     	trajectoryTrackTable.build(trajectoryToPageIndexRDD);
	}
	
	/**
	 * Construct the Voronoi page index:
	 * </br>
	 * Map a input RDD of trajectories to Voronoi Pages.
	 * </br>
     * Split the trajectory and map each sub-trajectory 
     * to its closest pivot and time window (page).
     * </br>
     * Note: Boundary trajectory segments are assigned to both 
     * pages it crosses with.
     * 
     * @return Return a RDD of pairs: 
     * (PageIndex = (VSI,TPI), Sub-Trajectory)
	 */
	public JavaPairRDD<PageIndex, Trajectory> mapTrajectoriesToPageIndex(
				final JavaRDD<Trajectory> trajectoryRDD, 
				final Broadcast<VoronoiDiagram> voronoiDiagram){

		// Split and map trajectories to page indexes
		JavaPairRDD<PageIndex, Trajectory> trajectoriesToPagesRDD = trajectoryRDD
	     	.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Trajectory>, PageIndex, Trajectory>() {
	     		
	     		// collect pivots, inside each map job (broadcasted variable)
				final List<Point> pivotList = 
						voronoiDiagram.value().getPivots();
				
				public Iterable<Tuple2<PageIndex, Trajectory>> call(
						Iterator<Trajectory> trajectoryItr) throws Exception {
					
					// the result pairs (PageIndex, Sub-trajectory)
					List<Tuple2<PageIndex, Trajectory>> resultPairs = 
							  new ArrayList<Tuple2<PageIndex,Trajectory>>();
					
					// read each trajectory in this partition
					//int VSI, TPI, prevVSI, prevTPI;
					while(trajectoryItr.hasNext()){
						// current trajectory
						Trajectory trajectory = trajectoryItr.next();

						// info of the previous point
						Point prev = null;
						int prevVSI = 0;
						int prevTPI = 0;
						
						// an empty sub-trajectory
						String id = trajectory.id;
						Trajectory sub = new Trajectory(id);
						
						// split the trajectory into sub-trajectories
						// for each page it intersects with
						for(Point point : trajectory.getPointsList()){
							// current point indexes
							int VSI = 1; // Voronoi Spatial Index 
							int TPI = (int)(point.time / TIME_WINDOW_SIZE) + 1; // Time Page Index  
								
							// find the closest pivot to this point
							double min  = INF;
							double dist = 0.0;
							for(Point pivot : pivotList){
								dist = point.dist(pivot);
								if(dist < min){
									min = dist;
									VSI = pivot.pivotId;
								}
							}
							point.pivotId = VSI;
							
							// check for boundary objects
							if(prev == null){
								sub.addPoint(point);
							} else if(VSI == prevVSI && TPI == prevTPI){
								sub.addPoint(point);
							} 
							// space/time boundary segment
							else { 
								// the current sub-trajectory also receives this boundary point
								sub.addPoint(point);
								 
								// the page index for the previous sub-trajectory
								PageIndex index = new PageIndex(prevVSI, prevTPI);
							
								// add pair <(VSI,TPI), Sub-Trajectory>
								resultPairs.add(new Tuple2<PageIndex, Trajectory>(index, sub));
								
								// new sub-trajectory for this boundary segment
								sub = new Trajectory(id);
								sub.addPoint(prev);
								sub.addPoint(point);
							}
							prev = point;
							prevVSI = VSI;
							prevTPI = TPI;
						}
						// add the page for the last sub-trajectory read
						PageIndex index = new PageIndex(prevVSI, prevTPI);
						// add pair <PageIndex, Page>
						resultPairs.add(new Tuple2<PageIndex, Trajectory>(index, sub));
					}
					
					// the iterable map list
					return resultPairs;
				}
			});

		return trajectoriesToPagesRDD;
	}
}
