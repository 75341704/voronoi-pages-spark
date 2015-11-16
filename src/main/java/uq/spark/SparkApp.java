package uq.spark;

import java.util.List;

import org.apache.spark.broadcast.Broadcast;

import uq.spark.indexing.PartitioningIndexingService;
import uq.spark.indexing.TrajectoryTrackTable;
import uq.spark.indexing.VoronoiDiagram;
import uq.spark.indexing.VoronoiPagesRDD;
import uq.spark.query.NearNeighbor;
import uq.spark.query.QueryProcessingService;
import uq.spatial.Box;
import uq.spatial.Point;
import uq.spatial.Trajectory; 

/**
 * Application Main.
 * 
 * @author uqdalves
 *
 */
public class SparkApp {
	
	/**
	 * Main
	 */
	public static void main(String [] args){

		/************************
		 * DATA INDEXING 
		 ************************/
		PartitioningIndexingService partitioningService = 
				new PartitioningIndexingService();

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
		QueryProcessingService queryService = new QueryProcessingService(
				voronoiPagesRDD, trajectoryTrackTable, voronoiDiagram); 
		
		List<Trajectory> queryAnswer;
		List<NearNeighbor> nnQueryAnswer;
		
		/******
		 * SELECTION QUERIES
		 ******/
		// some query parameters
		Box range = new Box(5, 15, 0, 25);
		long timeIni = 0; // 95
		long timeEnd = 3; // 99
		
		// run spatial-temporal queries
		queryAnswer = 
				queryService.getSpatialTemporalSelection(range, timeIni, timeEnd, true);
		System.out.println("Spatial-Temporal Selection Query Result: " + queryAnswer.size());
		for(Trajectory t : queryAnswer){
			t.print();
		}
		System.out.println();
		
		// run spatial query
		queryAnswer = 
				queryService.getSpatialSelection(range, true);
		System.out.println("Spatial Selection Query Result: " + queryAnswer.size());
		for(Trajectory t : queryAnswer){
			t.print();
		}
		System.out.println();
		
		// run time slice query
		queryAnswer = 
				queryService.getTimeSlice(timeIni, timeEnd, true);
		System.out.println("Time Slice Query Result: " + queryAnswer.size());
		for(Trajectory t : queryAnswer){
			t.print();
		}
		System.out.println();

		/******
		 * CROSS QUERY
		 ******/
/*		// query trajectory Q1
		Trajectory q1 = new Trajectory("Q1");
		q1.addPoint( new Point(13, 0, 0) );
		q1.addPoint( new Point(0, 15, 1) );
		q1.addPoint( new Point(5, 15, 2) );
		
		// run cross query
		queryAnswer = 
				queryService.getCrossSelection(q1, true);
		System.out.println("Cross Query Result: " + queryAnswer.size());
		for(Trajectory t : queryAnswer){
			t.print();
		}
		System.out.println();	

		/******
		 * NEAREST NEIGHBORS QUERY
		 ******/
		// query trajectory Q2
/*		Trajectory q2 = new Trajectory("Q2");
		q2.addPoint( new Point(1.0, 211.0, 0) );
		q2.addPoint( new Point(2.0, 212.0, 1) );
		q2.addPoint( new Point(3.0, 211.0, 2) );
		q2.addPoint( new Point(4.0, 212.0, 3) );
		
		// the time window to search (the duration of Q2)
		timeIni = q2.timeIni();
		timeEnd = q2.timeEnd();
		
		// number of neighbors to retrieve
		final int k = 7;
		
		// run the query
		nnQueryAnswer = 
				queryService.getKNearestNeighbors(q2, timeIni, timeEnd, k);
		System.out.println(k + "-NN Query Result: " + nnQueryAnswer.size());
		for(Trajectory t : nnQueryAnswer){
			t.print();
		}
		System.out.println();
		
		/******
		 * REVERSE NEAREST NEIGHBORS QUERY
		 ******/
		// run the query
/*		nnQueryAnswer = 
				queryService.getReverseNearestNeighbors(q2, timeIni, timeEnd);
		System.out.println("RNN Query Result: " + nnQueryAnswer.size());
		for(Trajectory t : nnQueryAnswer){
			t.print();
		}
		System.out.println();
	*/
	}
}
