package uq.spark.indexing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;
import uq.fs.FileToObjectRDDService;
import uq.fs.HDFSFileService;
import uq.spark.SparkEnvInterface;
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
public class PartitioningIndexingService implements Serializable, SparkEnvInterface, IndexParamInterface {
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
    	 * READ DATA
    	 */
    	// trajectory data files (from the HDFS folder)
     	JavaRDD<String> fileRDD = SC.textFile(DATA_PATH, NUM_PARTITIONS_DATA);
     	fileRDD.persist(STORAGE_LEVEL_PARTITIONIG);
     	
     	/**
     	 * BUILD THE VORONOI DIAGRAM
     	 */
     	// Read the pivots from the HDFS
		HDFSFileService hdfsService = 
				new HDFSFileService();
     	// Generate the diagram and broadcast.
 		VoronoiDiagram diagram = new VoronoiDiagram();
 		diagram.build(hdfsService.readPivotsHDFS(K));
     	voronoiDiagram = SC.broadcast(diagram);
  	
		/**
		 * BUILD VORONOI INDEX (MAP)
		 */
     	// First map to convert the input file to trajectory objects
		FileToObjectRDDService rddService = new FileToObjectRDDService();
		JavaRDD<Trajectory> trajectoryRDD = 
				rddService.mapRawDataToTrajectoryMercRDD(fileRDD);
     	// Second map to assign each sub-trajectory to a Voronoi Page
     	JavaPairRDD<PageIndex, Page> trajectoryToPagePairsRDD = 
     			mapTrajectoriesToPages(trajectoryRDD, getVoronoiDiagram().value());

     	/**
     	 * BUILD VORONOI PAGES RDD (REDUCE)
     	 */
     	// group pages by Index
      	voronoiPagesRDD = new VoronoiPagesRDD();
		voronoiPagesRDD.build(trajectoryToPagePairsRDD);
		voronoiPagesRDD.persist(STORAGE_LEVEL_PAGES);
		
		/**
		 * BUILD TRAJECTORY TRACK TABLE (MAP/REDUCE)
		 */
     	trajectoryTrackTable = new TrajectoryTrackTable();
     	trajectoryTrackTable.build(trajectoryToPagePairsRDD);
     	trajectoryTrackTable.persist(STORAGE_LEVEL_TTT);
     	
     	/**
     	 * OUTPUT
     	 */
    /* 	// estimate the size of the objects in memory
     	long pagesSize  	= voronoiPagesRDD.estimateSize();
     	long diagramSize    = SizeEstimator.estimate(voronoiDiagram);
     	long trackTableSize = SizeEstimator.estimate(trajectoryTrackTable);
		
		// PRINT: estimated data size
     	System.out.println();
     	System.out.println("Estimated size of pages RDD: " + pagesSize);
     	System.out.println("Estimated size of the Diagram: " + diagramSize);
     	System.out.println("Estimated size of the Track Table: " + trackTableSize);
     */	
     	// PRINT: number of partition
     	System.out.println();
     	System.out.println("Number of RDD partitions: " + voronoiPagesRDD.getNumPartitions());
     	System.out.println();
	}
	
	/**
	 * Convert a input dataset file RDD to Voronoi Page objects.
     * Read each line of the fileRDD (trajectory), and map each trajectory
     * piece (sub-trajectory) to its closest pivot and time window (page).
     * Creates a page entity for each sub-trajectory.
     * </br>
     * Each page initially contains one sub-trajectory only.
     * Creates key/value pairs using the PageIndex as keys
     * and a Page composed of a sub-trajectory as value.
     * </br>
     * Note 1: Boundary trajectory segments are assigned to both partitions
     * it crosses with.
     * </br>
     * Note 2: This function reads the large dataset, not the test one.
     * Use two maps to convert the data.
     * 
     * @return Return a RDD of Iterable pairs: 
     * <PageIndex = (Pivot ID, Time Page), Page = {sub-trajectory}>
     * 
	 */
	public JavaPairRDD<PageIndex, Page>
		mapTrajectoriesToPages(JavaRDD<Trajectory> trajectoryRDD, final VoronoiDiagram voronoiDiagram){

		// Map trajectories to pages (sub-trajectories)
		JavaPairRDD<PageIndex, Page> trajToPagePairsRDD = trajectoryRDD

	     	.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Trajectory>, PageIndex, Page>() {
	
				// collect pivots, inside each map job (broadcasted variable)
				final List<Point> pivotList = 
						voronoiDiagram.getPivots();
				
				public Iterable<Tuple2<PageIndex, Page>> call(Iterator<Trajectory> trajectoryItr) throws Exception {
					// An iterable list to return	
					List<Tuple2<PageIndex, Page>> iterableMap = 
							  new ArrayList<Tuple2<PageIndex,Page>>();

					// read each trajectory in this partition
					while(trajectoryItr.hasNext()){
						Trajectory trajectory = trajectoryItr.next();

						// info of the previous  point
						Point prev = null;
						int prevVSI = 0;
						int prevTPI = 0;
						
						// a empty sub-trajectory
						String id = trajectory.id;
						Trajectory sub = new Trajectory(id);
						
						// split the trajectory into sub-trajectories
						// for each page it intersects with
						for(Point point : trajectory.getPointsList()){
							// Indexes
							int VSI = 1; // Voronoi Spatial Index 
							int TPI = (int)(point.time / TIME_WINDOW_SIZE) + 1; // Time Page Index  
								
							// find the closest pivot to this point
							double min = INF;
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
								 
								// create the page for the previous sub-trajectory
								PageIndex index = new PageIndex(prevVSI, prevTPI);
								Page page = new Page();
								page.addTrajectory(sub);
								
								// add pair <PageIndex, Page>
								iterableMap.add(new Tuple2<PageIndex, Page>(index, page));
								
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
						Page page = new Page();
						page.addTrajectory(sub);
						
						// add pair <PageIndex, Page>
						iterableMap.add(new Tuple2<PageIndex, Page>(index, page));
					}
					
					// the iterable map list
					return iterableMap;
				}
			});
			
		return trajToPagePairsRDD;
	}
}
