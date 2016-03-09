package uq.spark.exp;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.broadcast.Broadcast;

import uq.fs.FileReader;
import uq.spark.EnvironmentVariables;
import uq.spark.Logger;
import uq.spark.index.PagesPartitioningModule;
import uq.spark.index.TrajectoryTrackTable;
import uq.spark.index.VoronoiDiagram;
import uq.spark.index.VoronoiPagesRDD;
import uq.spark.query.NearNeighbor;
import uq.spark.query.QueryProcessingModule;
import uq.spatial.STBox;
import uq.spatial.Trajectory;

/**
 * Experiment to evaluate the correctness of the algorithm.
 * Generate log result to compare with the benchmark.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class BenchmarkTest implements Serializable, EnvironmentVariables{
	// experiments log
	private static final Logger LOG = new Logger();

	/**
	 * Main: Performance testing.
	 */
	public static void main(String[] args){
		System.out.println("\n[EXPERIMENTS MODULE] Running Benchmark Experiments..");
	
		LOG.appendln("Voronoi-Pages Test Result.");
		LOG.appendln();
		
		/************************
		 * DATA INDEXING 
		 ************************/
		PagesPartitioningModule partitioningService = 
				new PagesPartitioningModule();

		// run data partitioning and indexing
		partitioningService.run();
		
		// get the voronoi diagram abstraction.
		Broadcast<VoronoiDiagram> voronoiDiagram = 
				partitioningService.getVoronoiDiagram();
		// get the RDD of Voronoi pages 
		VoronoiPagesRDD voronoiPagesRDD = 
				partitioningService.getVoronoiPagesRDD();
		// get trajectory track table
		TrajectoryTrackTable trajectoryTrackTable = 
				partitioningService.getTrajectoryTrackTable();
		
		/************************
		 * QUERIES PROCESING 
		 ************************/
		QueryProcessingModule queryService = new QueryProcessingModule(
				voronoiPagesRDD, trajectoryTrackTable, voronoiDiagram); 

		// Run spatial-temporal selection test
		List<STBox> stTestCases = 
				FileReader.readSpatialTemporalTestCases();
		LOG.appendln("Spatial-Temporal Selection Result.");
		LOG.appendln();
		for(int i=1; i<=10; i++){ // run only 10 queries
			STBox stObj = stTestCases.get(i);
			// run query - exact result
			List<Trajectory> exactResult = queryService 
					.getSpatialTemporalSelection(stObj, stObj.timeIni, stObj.timeEnd, false);
			// run query - whole result
			List<Trajectory> wholeResult = queryService 
					.getSpatialTemporalSelection(stObj, stObj.timeIni, stObj.timeEnd, true);
			// count number of points returned in the exact
			int count = 0;
			for(Trajectory t : exactResult){
				count += t.size();
			}
			LOG.appendln("Query " + i + " Result.");
			LOG.appendln("Number of Points: " + count); 
			LOG.appendln("Trajectories Returned: " + wholeResult.size());
		}

		// Run kNN test
		List<Trajectory> nnTestCases = 
				FileReader.readNearestNeighborTestCases();
		LOG.appendln("K-NN Result.");
		LOG.appendln();
		int i = 1;
		for(Trajectory q : nnTestCases){
			// params
			long tIni 	= q.timeIni();
			long tEnd 	= q.timeEnd();
			final int k = 20; // 20-NN
			// run query
			List<NearNeighbor> resultList = queryService
					.getKNearestNeighbors(q, tIni, tEnd, k);
			LOG.appendln("Query " + i++ + " Result.");
			LOG.appendln("Query Trajectory: " + q.id);
			LOG.appendln("Trajectories Returned: " + resultList.size());
			int n=1;
			for(NearNeighbor nn : resultList){
				LOG.appendln(n++ + "-NN: " + nn.id);
				LOG.appendln("Dist: " + nn.distance);
			}
		}
		
		// save the result log to HDFS
		LOG.save("experiments-benchmark-results");
		
		// unpersist
		voronoiPagesRDD.unpersist();
		trajectoryTrackTable.unpersist();
	}
}
