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
 * Experiment to evaluate the performance of the algorithm.
 * Generate log result with query performance information.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class PerformanceTest  implements Serializable, EnvironmentVariables{
	// service to read files
	private static final FileReader reader = 
			new FileReader();
	// experiments log
	private static final Logger LOG = new Logger();
	// experiment log file name
	private static final String LOG_NAME = 
			"experiments-mem-" + K + "-" + TIME_WINDOW_SIZE + "s";

	/**
	 * Main: Performance testing.
	 */
	public static void main(String[] args){
		System.out.println("\nRunning Experiments..\n");
		
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
		 * LOAD/WRITE THE INDEX 
		 ************************/
		// save the index structure
/* 		voronoiPagesRDD.save(LOCAL_PATH + "/index-structure-7");
		trajectoryTrackTable.save(LOCAL_PATH + "/index-structure-7");
		
		// load the index structure
		VoronoiPagesRDD voronoiPagesRDD = new VoronoiPagesRDD();
		voronoiPagesRDD.load(TACHYON_PATH + "/index-structure/pages-rdd");
		TrajectoryTrackTable trajectoryTrackTable = new TrajectoryTrackTable();
		trajectoryTrackTable.load(TACHYON_PATH + "/index-structure/trajectory-track-table-rdd");

		// save information regarding indexing
		voronoiPagesRDD.savePagesInfo();
		trajectoryTrackTable.saveTableInfo();
*/		
		// action to force building the index
		System.out.println("Num pages: " + voronoiPagesRDD.count());
		System.out.println("Num TTT tuples: " + trajectoryTrackTable.count());
		
		/************************
		 * QUERIES PROCESING 
		 ************************/
		QueryProcessingModule queryService = new QueryProcessingModule(
				voronoiPagesRDD, trajectoryTrackTable, voronoiDiagram); 
		
		/******
		 * SPATIAL TEMPORAL SELECTION QUERIES (WHOLE)
		 ******/
		List<STBox> stUseCases = 
				reader.readSpatialTemporalTestCases();
		{
			LOG.appendln("Spatial-Temporal Selection Query Result (Whole):\n");
			long selecQueryTime=0;
			int queryId=1;
			for(STBox stObj : stUseCases){
				System.out.println("Query " + queryId);
				long start = System.currentTimeMillis();
				// run query - whole trajectories
				List<Trajectory> result = queryService 
						.getSpatialTemporalSelection(stObj, stObj.timeIni, stObj.timeEnd, true);
				long time = System.currentTimeMillis()-start;
				LOG.appendln("Query " + queryId++ + ": " + result.size() + " trajectories in " + time + " ms.");
				selecQueryTime += time;		
			}
			LOG.appendln("Spatial-Temporal Selection ends at: " + System.currentTimeMillis() + "ms.");
			LOG.appendln("Total Spatial-Temporal Selection Query Time: " + selecQueryTime + " ms.\n");
		}
		/******
		 * SPATIAL TEMPORAL SELECTION QUERIES (EXACT)
		 ******/
		{
			LOG.appendln("Spatial-Temporal Selection Query Result (Exact):\n");
			long selecQueryTime=0;
			int queryId=1;
			for(STBox stObj : stUseCases){
				System.out.println("Query " + queryId);
				long start = System.currentTimeMillis();
				// run query - exact sub-trajectories
				List<Trajectory> result = queryService
						.getSpatialTemporalSelection(stObj, stObj.timeIni, stObj.timeEnd, false);
				long time = System.currentTimeMillis()-start;
				LOG.appendln("Query " + queryId++ + ": " + result.size() + " sub-trajectories in " + time + " ms.");
				selecQueryTime += time;		
			}
			LOG.appendln("Spatial-Temporal Selection (Exact) ends at: " + System.currentTimeMillis() + "ms.");
			LOG.appendln("Total Spatial-Temporal Selection Query Time: " + selecQueryTime + " ms.\n");
		}
		/******
		 * NN QUERIES
		 ******/
		List<Trajectory> nnUseCases = 
				reader.readNearestNeighborTestCases();
		{
			LOG.appendln("NN Query Result:\n");
			long nnQueryTime=0;
			int queryId=1;
			for(Trajectory t : nnUseCases){
				System.out.println("Query " + queryId);
				long start = System.currentTimeMillis();				
				// run query
				long tIni = t.timeIni();
				long tEnd = t.timeEnd();
				Trajectory result = queryService
						.getNearestNeighbor(t, tIni, tEnd);
				long time = System.currentTimeMillis() - start;
				LOG.appendln("NN Query " + queryId++ + ": " +  result.id + " in " + time + " ms.");
				nnQueryTime += time;
			}
			LOG.appendln("NN query ends at: " + System.currentTimeMillis() + "ms.");
			LOG.appendln("Total NN Time: " + nnQueryTime + " ms.\n");
		}
		/******
		 * K-NN QUERIES
		 ******/
		{
			LOG.appendln("K-NN Query Result:\n");
			long nnQueryTime=0;
			int queryId=1;
			int k = 10;
			for(Trajectory t : nnUseCases){
				System.out.println("Query " + queryId);
				long start = System.currentTimeMillis();				
				// run query
				long tIni = t.timeIni();
				long tEnd = t.timeEnd();
				List<NearNeighbor> resultList = queryService
						.getKNearestNeighbors(t, tIni, tEnd, k);
				long time = System.currentTimeMillis() - start;
				LOG.appendln(k + "-NN Query " + queryId++ + ": " +  resultList.size() + " in " + time + " ms.");
				// print result
				/*
				int i=1;
				for(NearNeighbor nn : resultList){
					LOG.appendln(i++ + "-NN: " + nn.id);
				}*/
				nnQueryTime += time;
			}
			LOG.appendln(k + "-NN query ends at: " + System.currentTimeMillis() + "ms.");
			LOG.appendln("Total " + k + "-NN Time: " + nnQueryTime + " ms.\n");
		}
		
		// save the result log to HDFS
		LOG.save(LOG_NAME);
		
		// unpersist
		voronoiPagesRDD.unpersist();
		trajectoryTrackTable.unpersist();
	}
}
