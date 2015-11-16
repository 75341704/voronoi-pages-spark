package uq.exp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
 
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import uq.fs.HDFSFileService;
import uq.spark.SparkEnvInterface;
import uq.spark.indexing.IndexParamInterface;
import uq.spark.indexing.PartitioningIndexingService;
import uq.spark.indexing.TrajectoryTrackTable;
import uq.spark.indexing.VoronoiDiagram;
import uq.spark.indexing.VoronoiPagesRDD;
import uq.spark.query.NearNeighbor;
import uq.spark.query.QueryProcessingService;
import uq.spatial.Box;
import uq.spatial.GeoInterface;
import uq.spatial.Point;
import uq.spatial.Trajectory;
import uq.spatial.transformation.ProjectionTransformation;

/**
 * Service to perform experiments
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class ExperimentsService implements Serializable, SparkEnvInterface, IndexParamInterface, GeoInterface {
	private static HDFSFileService hdfs = new HDFSFileService();
	// experiments log
	private static List<String> log = new LinkedList<String>();
	// experiment output file name
	private static final String FILE_NAME = "experiments-"+ K + "-" + TIME_WINDOW_SIZE + "s";
	// time to run queries
	private static long selecQueryTime=0;
	private static long nnQueryTime=0;
	
	/**
	 * Main
	 */
	public static void main(String[] args){
		System.out.println();
		System.out.println("Running Experiments..");
		System.out.println();		
		
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

		// save information regarding indexing
/*		voronoiPagesRDD.savePagesInfo();
		voronoiPagesRDD.savePagesHistory();
		trajectoryTrackTable.saveTableInfo();
*/		
		/************************
		 * QUERIES PROCESING 
		 ************************/
		QueryProcessingService queryService = new QueryProcessingService(
				voronoiPagesRDD, trajectoryTrackTable, voronoiDiagram); 
		//trajectories returned from the query
		List<Trajectory> queryResult;
		List<NearNeighbor> nnResult;
		int numTrajectories;
		
		/******
		 * USE CASES
		 ******/
		List<STObject> stUseCases = readSpatialTemporalUseCases();
		List<Trajectory> nnUseCases = readNearestNeighborUseCases();
		
		/******
		 * SPATIAL TEMPORAL SELECTION QUERIES
		 ******/
		{
			log.add("Spatial-Temporal Selection Query Result:\n");
			int i=1;
			for(STObject stObj : stUseCases){
				long start = System.currentTimeMillis();
				// run query
				queryResult = queryService
						.getSpatialTemporalSelection(stObj.region, stObj.timeIni, stObj.timeEnd, true);
				numTrajectories = queryResult.size();

				long time = System.currentTimeMillis()-start;
				log.add("Query " + i++ + ": " + numTrajectories + " trajectories in " + time + " ms.");
				selecQueryTime += time;
			}
			log.add("\nSpatial-Temporal Selection ends at: " + System.currentTimeMillis() + "ms.");
			log.add("Total Spatial-Temporal Selection Query Time: " + selecQueryTime + " ms.");
		}

		/******
		 * K-NN QUERIES
		 ******/
		{
			log.add("\nKNN Query Result:\n");
			int i=1;
			final int k = 10; 
			for(Trajectory t : nnUseCases){
				long start = System.currentTimeMillis();				
				// run query
				long tIni = t.timeIni();
				long tEnd = t.timeEnd();
				nnResult = queryService
						.getKNearestNeighbors(t, tIni, tEnd, k);
				numTrajectories = nnResult.size();
				
				long time = System.currentTimeMillis()-start;
				log.add("NN Query " + i++ + ": " + numTrajectories + " trajectories in " + time + " ms.");
				nnQueryTime += time;
				}
			log.add("\nNN query ends at: " + System.currentTimeMillis() + "ms.");
			log.add("Total Spatial-Temporal Selection Query Time: " + nnQueryTime + " ms.");
		}
		/******
		 * REVERSE NN QUERIES
		 ******/
		/*{
			log.add("Reverse-NN Query Result:");
			int i=1;
			long begin = System.currentTimeMillis();
			for(Trajectory t : nnUseCases){
				// run query
				long tIni = t.timeIni();
				long tEnd = t.timeEnd();
				nnResult = queryService
						.getReverseNearestNeighbors(t, tIni, tEnd);
				numTrajectories = nnResult.size();
				
				log.add("Query " + i++ + ": " + numTrajectories + " trajectories at " + System.currentTimeMillis() + " ms.");
			}
			long end = System.currentTimeMillis();
			log.add("Total Reverse-NN Query Time: "+ (end-begin) + "ms.\n");
			System.out.println("Reverse-Nearest-Neigbors Time: " + (end-begin) + "ms.");
		}*/

		// save the result log to HDFS
		hdfs.saveStringListHDFS(log, FILE_NAME);
	}
	
	/**
	 * Read the uses cases for time slice queries
	 */
	public static List<TimeWindow> readTimeSliceUseCases(){
		List<String> lines = 
				hdfs.readFileHDFS("/spark-data/use-cases/time-slice-use-cases");
		// process lines
		long timeIni, timeEnd;
		List<TimeWindow> timeWindowList = new LinkedList<TimeWindow>(); 
		for(String line : lines){
			if(line.length() > 1){
				String[] tokens = line.split(" ");
				timeIni = Long.parseLong(tokens[0]);
				timeEnd = Long.parseLong(tokens[1]); 
				
				timeWindowList.add(new TimeWindow(timeIni, timeEnd));
			}
		}
		return timeWindowList;
	}
	
	/**
	 * Read the uses cases for spatial selection queries
	 */
	public static List<Box> readSpatialUseCases(){
		List<String> lines = 
				hdfs.readFileHDFS("/spark-data/use-cases/spatial-use-cases");
		// process lines
		double left, right, bottom, top;
		List<Box> boxList = new LinkedList<Box>(); 
		for(String line : lines){
			if(line.length() > 3){
				String[] tokens = line.split(" ");
				left	= Double.parseDouble(tokens[0]);
				right	= Double.parseDouble(tokens[1]);
				bottom	= Double.parseDouble(tokens[2]);
				top		= Double.parseDouble(tokens[3]);
				
				boxList.add(new Box(left, right, bottom, top));
			}
		}
		return boxList;
	}
	
	/**
	 * Read the uses cases for spatial-temporal selection queries
	 */
	public static List<STObject> readSpatialTemporalUseCases(){
		List<String> lines = 
				hdfs.readFileHDFS("/spark-data/use-cases/spatial-temporal-use-cases");
		// process lines
		long timeIni, timeEnd;
		double left, right, bottom, top;
		List<STObject> stList = new LinkedList<STObject>(); 
		for(String line : lines){
			if(line.length() > 3){
				String[] tokens = line.split(" ");
				left	= Double.parseDouble(tokens[0]);
				right	= Double.parseDouble(tokens[1]);
				bottom	= Double.parseDouble(tokens[2]);
				top		= Double.parseDouble(tokens[3]);
				timeIni = Long.parseLong(tokens[4]);
				timeEnd = Long.parseLong(tokens[5]); 
				
				stList.add(new STObject(left, right, bottom, top, timeIni, timeEnd));
			}
		}
		return stList;
	}
	
	/**
	 * Read the uses cases for Nearest Neighbors queries.
	 */
	public static List<Trajectory> readNearestNeighborUseCases(){
		List<String> lines = 
				hdfs.readFileHDFS("/spark-data/use-cases/nn-use-cases");
		// process lines
		int id=1;
		double x, y;
		long time;
		List<Trajectory> list = new LinkedList<Trajectory>();
		for(String line : lines){
			if(line.length() > 4){
				String[] tokens = line.split(" ");
				// first tokens is the id
				Trajectory t = new Trajectory("Q" + id++);
				for(int i=1; i<=tokens.length-3; i+=3){
					x = Double.parseDouble(tokens[i]);
					y = Double.parseDouble(tokens[i+1]);
					time = Long.parseLong(tokens[i+2]);
					t.addPoint(new Point(x, y, time));
				}
				list.add(t);
			}
		}
		return list;
	}
	
	/**
	 * Read a pivots file and convert to Mercator.
	 */
	public static void convertPivotsToMercator(){ 
		// file to read
		File file = new File("C:/lol/file.txt");
		BufferedReader buffer;
		try {
			buffer = new BufferedReader(
					new FileReader(file));
			// each line of the current file
			String line;

			while(buffer.ready()){
				line = buffer.readLine();
				String[] tokens = line.split(" ");
				
				double lon = Double.parseDouble(tokens[0]);
				double lat = Double.parseDouble(tokens[1]);
				String time = tokens[2];

				double[] merc = ProjectionTransformation.getMercatorProjection(lon, lat);
				double x = merc[0];
				double y = merc[1];
				
				System.out.println(x + " " + y + " " + time);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}		
	} 

	/**
	 * Read a pivots file and convert to Mercator.
	 */
	public static void convertUseCasesToMercator(){
		// file to read
		File file = new File("C:/lol/file.txt");
		try {
			BufferedReader buffer = new BufferedReader(
					new FileReader(file));
			// each line of the current file
			String line;

			while(buffer.ready()){
				line = buffer.readLine();
				String[] tokens = line.split(" ");
				
				double lon1 = Double.parseDouble(tokens[0]);
				double lon2 = Double.parseDouble(tokens[1]);
				double lat1 = Double.parseDouble(tokens[2]);
				double lat2 = Double.parseDouble(tokens[3]);
				String time1 = tokens[4];
				String time2 = tokens[5];
				
				double[] merc1 = ProjectionTransformation.getMercatorProjection(lon1, lat1);
				double[] merc2 = ProjectionTransformation.getMercatorProjection(lon2, lat2);
				double x1 = merc1[0];
				double y1 = merc1[1];
				double x2 = merc2[0];
				double y2 = merc2[1];
				
				System.out.println(x1 + " " + x2 + " " + y1 + " " + y2 + " " + time1 + " " + time2);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/** Re-sample the input dataset trajectories
	 *  1gb to 2gb, 4gb, and 8gb
	 */
	public static void reSample(JavaRDD<String> fileRDD, String fileName){
		List<String> newLines = 
				fileRDD.map(new Function<String, String>() {
		
					public String call(String line) throws Exception {
						line = "2_" + line;
						return line;
					}
				}).collect();
			hdfs.saveStringListHDFS(newLines, fileName + "-2");	
			
			newLines = 
				fileRDD.map(new Function<String, String>() {
		
					public String call(String line) throws Exception {
						line = "3_" + line;
						return line;
					}
				}).collect();
			hdfs.saveStringListHDFS(newLines, fileName + "-3");
			
			newLines = 
				fileRDD.map(new Function<String, String>() {
		
					public String call(String line) throws Exception {
						line = "4_" + line;
						return line;
					}
				}).collect();
			hdfs.saveStringListHDFS(newLines, fileName + "-4");
			
			newLines = 
					fileRDD.map(new Function<String, String>() {
			
						public String call(String line) throws Exception {
							line = "5_" + line;
							return line;
						}
					}).collect();
			hdfs.saveStringListHDFS(newLines, fileName + "-5");		
			
			newLines = 
					fileRDD.map(new Function<String, String>() {
			
						public String call(String line) throws Exception {
							line = "6_" + line;
							return line;
						}
					}).collect();
			hdfs.saveStringListHDFS(newLines, fileName + "-6");		
			
			newLines = 
					fileRDD.map(new Function<String, String>() {
			
						public String call(String line) throws Exception {
							line = "7_" + line;
							return line;
						}
					}).collect();
			hdfs.saveStringListHDFS(newLines, fileName + "-7");	
				
			newLines = 
					fileRDD.map(new Function<String, String>() {
			
						public String call(String line) throws Exception {
							line = "8_" + line;
							return line;
						}
					}).collect();
			hdfs.saveStringListHDFS(newLines, fileName + "-8");		
	}

}
