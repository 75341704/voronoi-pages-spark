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

import uq.fs.FileToObjectRDDService;
import uq.fs.HDFSFileService;
import uq.spark.Logger;
import uq.spark.SparkEnvInterface;
import uq.spark.index.IndexParamInterface;
import uq.spark.index.PartitioningIndexingService;
import uq.spark.index.TrajectoryTrackTable;
import uq.spark.index.VoronoiDiagram;
import uq.spark.index.VoronoiPagesRDD;
import uq.spark.query.NearNeighbor;
import uq.spark.query.QueryProcessingService;
import uq.spatial.GeoInterface;
import uq.spatial.Point;
import uq.spatial.STBox;
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
	private static final Logger LOG = new Logger();
	// experiment log file name
	private static final String LOG_NAME = 
			"experiments-mem-" + K + "-" + TIME_WINDOW_SIZE + "s";
	/**
	 * Main
	 */
	public static void main(String[] args){
		System.out.println("\nRunning Experiments..\n");
		
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
		QueryProcessingService queryService = new QueryProcessingService(
				voronoiPagesRDD, trajectoryTrackTable, voronoiDiagram); 
		
		/******
		 * SPATIAL TEMPORAL SELECTION QUERIES (WHOLE)
		 ******/
		List<STBox> stUseCases = readSpatialTemporalUseCases();
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
		List<Trajectory> nnUseCases = readNearestNeighborUseCases();
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
				int i=1;
				for(NearNeighbor nn : resultList){
					LOG.appendln(i++ + "-NN: " + nn.id);
				}
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
		
	/**
	 * Read the uses cases for spatial-temporal selection queries
	 */
	public static List<STBox> readSpatialTemporalUseCases(){
		List<String> lines = 
				HDFS.readFileHDFS("/spark-data/use-cases/spatial-temporal-use-cases");
		// process lines
		long timeIni, timeEnd;
		double left, right, bottom, top;
		List<STBox> stList = new LinkedList<STBox>(); 
		for(String line : lines){
			if(line.length() > 3){
				String[] tokens = line.split(" ");
				left	= Double.parseDouble(tokens[0]);
				right	= Double.parseDouble(tokens[1]);
				bottom	= Double.parseDouble(tokens[2]);
				top		= Double.parseDouble(tokens[3]);
				timeIni = Long.parseLong(tokens[4]);
				timeEnd = Long.parseLong(tokens[5]); 
				
				stList.add(new STBox(left, right, bottom, top, timeIni, timeEnd));
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
