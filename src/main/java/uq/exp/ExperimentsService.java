package uq.exp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
 
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import uq.fs.FileToObjectRDDService;
import uq.fs.HDFSFileService;
import uq.spark.SparkEnvInterface;
import uq.spark.index.IndexParamInterface;
import uq.spark.index.PartitioningIndexingService;
import uq.spark.index.TrajectoryTrackTable;
import uq.spark.index.VoronoiDiagram;
import uq.spark.index.VoronoiPagesRDD;
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
	private static final HDFSFileService HDFS = new HDFSFileService();
	// experiments log
	private static final StringBuffer LOG = new StringBuffer();
	// experiment LOG file name
	private static final String FILE_NAME = 
			"experiments-" + K + "-" + TIME_WINDOW_SIZE + "s";

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

System.out.println("Num polygons: " + voronoiDiagram.value().size());
System.out.println("Num pages: " + voronoiPagesRDD.count());
System.out.println("Num TTT tuples: " + trajectoryTrackTable.count());

/*		// save the index structure
 		voronoiPagesRDD.save(LOCAL_PATH + "/index-structure-1");
		trajectoryTrackTable.save(LOCAL_PATH + "/index-structure-1");
		
		// save information regarding indexing
		voronoiPagesRDD.savePagesInfo();
		voronoiPagesRDD.savePagesHistory();
		trajectoryTrackTable.saveTableInfo();
*/
		/************************
		 * QUERIES PROCESING 
		 ************************/
		QueryProcessingService queryService = new QueryProcessingService(
				voronoiPagesRDD, trajectoryTrackTable, voronoiDiagram); 
		
		/******
		 * K-NN QUERIES
		 ******/
/*		List<Trajectory> nnUseCases = readNearestNeighborUseCases();
		{
			LOG.append("NN Query Result:\n\n");
			long nnQueryTime=0;
			int queryId=1;
			//int k = 10;
			for(Trajectory t : nnUseCases){
				System.out.println("Query " + queryId);
				long start = System.currentTimeMillis();				
				// run query
				long tIni = t.timeIni();
				long tEnd = t.timeEnd();
				Trajectory result = queryService
						.getNearestNeighbor(t, tIni, tEnd);
				if(result != null){	
					System.out.println("NN retornou: " + result.id);
						long time = System.currentTimeMillis() - start;
						LOG.append("NN Query " + queryId++ + ": " +  result.id + " in " + time + " ms.\n");
						nnQueryTime += time;
				}
			}
			LOG.append("\nNN query ends at: " + System.currentTimeMillis() + "ms.");
			LOG.append("\nTotal NN Time: " + nnQueryTime + " ms.");
		}

		/******
		 * SPATIAL TEMPORAL SELECTION QUERIES
		 ******/
		List<STObject> stUseCases = readSpatialTemporalUseCases();
		{
			LOG.append("Spatial-Temporal Selection Query Result:\n\n");
			long selecQueryTime=0;
			int queryId=1;
			for(STObject stObj : stUseCases){
				System.out.println("Query " + queryId);
				long start = System.currentTimeMillis();
				// run query
				List<Trajectory> result = queryService
						.getSpatialTemporalSelection(stObj.region, stObj.timeIni, stObj.timeEnd, true);

			long time = System.currentTimeMillis()-start;
			LOG.append("Query " + queryId++ + ": " + result.size() + " trajectories in " + time + " ms.\n");
			selecQueryTime += time;
	
System.out.println("res: " +result.size());
			}
			LOG.append("\nSpatial-Temporal Selection ends at: " + System.currentTimeMillis() + "ms.");
			LOG.append("\nTotal Spatial-Temporal Selection Query Time: " + selecQueryTime + " ms.\n\n");
		}

		// save the result log to HDFS
		HDFS.saveLogFileHDFS(LOG, FILE_NAME);
	
		// unpersist
		voronoiPagesRDD.unpersist();
		trajectoryTrackTable.unpersist();
	}
	
	/**
	 * Read the uses cases for time slice queries
	 */
	public static List<TimeWindow> readTimeSliceUseCases(){
		List<String> lines = 
				HDFS.readFileHDFS("/spark-data/use-cases/time-slice-use-cases");
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
				HDFS.readFileHDFS("/spark-data/use-cases/spatial-use-cases");
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
				HDFS.readFileHDFS("/spark-data/use-cases/spatial-temporal-use-cases");
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
				HDFS.readFileHDFS("/spark-data/use-cases/nn-use-cases");
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
		BufferedReader buffer;
		try {
			File file = new File("C:/lol/file.txt");
			buffer = new BufferedReader(
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
			HDFS.saveStringListHDFS(newLines, fileName + "-2");	
			
			newLines = 
				fileRDD.map(new Function<String, String>() {
		
					public String call(String line) throws Exception {
						line = "3_" + line;
						return line;
					}
				}).collect();
			HDFS.saveStringListHDFS(newLines, fileName + "-3");
			
			newLines = 
				fileRDD.map(new Function<String, String>() {
		
					public String call(String line) throws Exception {
						line = "4_" + line;
						return line;
					}
				}).collect();
			HDFS.saveStringListHDFS(newLines, fileName + "-4");
			
			newLines = 
					fileRDD.map(new Function<String, String>() {
			
						public String call(String line) throws Exception {
							line = "5_" + line;
							return line;
						}
					}).collect();
			HDFS.saveStringListHDFS(newLines, fileName + "-5");		
			
			newLines = 
					fileRDD.map(new Function<String, String>() {
			
						public String call(String line) throws Exception {
							line = "6_" + line;
							return line;
						}
					}).collect();
			HDFS.saveStringListHDFS(newLines, fileName + "-6");		
			
			newLines = 
					fileRDD.map(new Function<String, String>() {
			
						public String call(String line) throws Exception {
							line = "7_" + line;
							return line;
						}
					}).collect();
			HDFS.saveStringListHDFS(newLines, fileName + "-7");	
				
			newLines = 
					fileRDD.map(new Function<String, String>() {
			
						public String call(String line) throws Exception {
							line = "8_" + line;
							return line;
						}
					}).collect();
			HDFS.saveStringListHDFS(newLines, fileName + "-8");		
	}

	/**
	 * Read the dataset from local file and sample
	 * 16gb of data, save to HDFS.
	 */
	public static void reSample16(){
		//for(int i=1; i<=4; i++){
		String splitFolder = "/trajectory-data/split";
		JavaRDD<String> fileRDD = SC.textFile("file:/media/bigdata/uqdalves/my-data" + splitFolder + "7," +
				 							  "file:/media/bigdata/uqdalves/my-data" + splitFolder + "8").sample(false, 0.265);
		
		// map to trajectory object
		FileToObjectRDDService rdd = new FileToObjectRDDService();
		JavaRDD<Trajectory> trajectoryRDD = rdd.mapRawDataToTrajectoryMercRDD(fileRDD);
		
		// map to trajectory as string
		JavaRDD<String> trajAsStringRDD = 
			trajectoryRDD.map(new Function<Trajectory, String>() {
				public String call(Trajectory t) throws Exception {
					return t.toString();
				}
			});
		
		// save the RDD to HDFS
		trajAsStringRDD.saveAsTextFile(HDFS_PATH + "/spark-data" + splitFolder + "4");
		//}
	}
}
