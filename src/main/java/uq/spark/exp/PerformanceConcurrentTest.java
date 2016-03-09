package uq.spark.exp;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors; 

import org.apache.spark.broadcast.Broadcast;

import uq.fs.FileReader;
import uq.spark.EnvironmentVariables; 
import uq.spark.index.PagesPartitioningModule;
import uq.spark.index.TrajectoryTrackTable;
import uq.spark.index.VoronoiDiagram;
import uq.spark.index.VoronoiPagesRDD;
import uq.spark.query.QueryProcessingModule;
import uq.spatial.STBox;
import uq.spatial.Trajectory;

/**
 * Experiment to evaluate the performance of the algorithm.
 * Generate log result with query performance information.
 * </br>
 * Process many concurrent Threads (multi-thread, many queries 
 * at same time,  using Spark's dynamically resource allocation).
 * 
 * @author uqdalves
 *
 */
// http://stackoverflow.com/questions/28712420/how-to-run-concurrent-jobsactions-in-apache-spark-using-single-spark-context
@SuppressWarnings("serial")
public class PerformanceConcurrentTest implements Serializable, EnvironmentVariables {
	// maximum number of concurrent Threads
	private static final int NUM_THREADS = 10;
	
	/**
	 * Main: Performance testing.
	 */
	public static void main(String[] args){
		System.out.println("\n[EXPERIMENTS MODULE] Running Performance Concurrent Test..\n");
	
		/************************
		 * DATA PARTITIONING
		 ************************/
		PagesPartitioningModule partitioningService = 
				new PagesPartitioningModule();

		// run data partitioning and indexing
		partitioningService.run();
		
		// get the voronoi diagram abstraction.
		final Broadcast<VoronoiDiagram> voronoiDiagram = 
				partitioningService.getVoronoiDiagram();
		// get the RDD of Voronoi pages 
		final VoronoiPagesRDD voronoiPagesRDD = 
				partitioningService.getVoronoiPagesRDD();
		// get trajectory track table
		final TrajectoryTrackTable trajectoryTrackTable = 
				partitioningService.getTrajectoryTrackTable();
		
		// action to force building the index
		System.out.println("Num pages: " + voronoiPagesRDD.count());
		System.out.println("Num TTT tuples: " + trajectoryTrackTable.count());
		
		/************************
		 * MULTI-THREAD QUERIES PROCESING 
		 ************************/

		// read the test cases (queries)
	    final List<STBox> stTestCases = 
				FileReader.readSpatialTemporalTestCases();
	    
	    // At any point, at most NUM_THREADS Threads will be active processing tasks.
	    ExecutorService executorService = 
	    		Executors.newFixedThreadPool(NUM_THREADS);

	    // create one concurrent thread per query
	    for(final STBox stQueryObj : stTestCases){
	    	/*Future<List<Trajectory>> future = executorService.submit(new Callable<List<Trajectory>>() {
				public List<Trajectory> call() throws Exception {
					return null;
				}
			};*/
	    	executorService.submit(new Runnable() {
				public void run() {
					System.out.println("Running concurrent query.");
					
					// query service will be executed by concurrent tasks
					QueryProcessingModule queryService = new QueryProcessingModule(
							voronoiPagesRDD, trajectoryTrackTable, voronoiDiagram);
		
					// run query - whole trajectories
					List<Trajectory> result = queryService.getSpatialTemporalSelection( 
							stQueryObj, stQueryObj.timeIni, stQueryObj.timeEnd, true);
					
					System.out.println("Query Result Size: " + result.size());
				}
			});
	    }
 // usar RDD async operations?   
		// unpersist RDDs
		voronoiPagesRDD.unpersist();
		trajectoryTrackTable.unpersist();	    
	}
	
}
