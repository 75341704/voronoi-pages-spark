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
import org.apache.spark.broadcast.Broadcast;

import uq.fs.DataConverter;
import uq.fs.HDFSFileService;
import uq.spark.EnvironmentVariables;
import uq.spark.MyLogger;
import uq.spark.index.DataPartitioningModule;
import uq.spark.index.TrajectoryTrackTableRDD;
import uq.spark.index.VoronoiDiagram;
import uq.spark.index.VoronoiPagesRDD;
import uq.spark.query.GreedyQueryService;
import uq.spark.query.NearNeighbor;
import uq.spark.query.QueryProcessingModule;
import uq.spatial.Point;
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
	private static final HDFSFileService HDFS = 
			new HDFSFileService();
	// number of nearest neighbors
	private static final int NUM_K = 100;
	
	/**
	 * Main: Performance testing.
	 */
	public static void main(String[] args){
		System.out.println("\n[EXPERIMENTS MODULE] "
				+ "Running Benchmark Experiments..");
		
		// read benchmark test cases
/*		final List<Trajectory> nnBenchmarkCases = 
				readNearestNeighborTestCases();
		
		/************************
		 * GREEDY ALGORITHM TEST
		 ************************/
		// Read and parse data to trajectories RDD
/*		JavaRDD<String> fileRDD = SC.textFile(DATA_PATH, NUM_PARTITIONS_DATA);
		DataConverter converter = new DataConverter();
		JavaRDD<Trajectory> trajectoryRDD = 
				converter.mapRawDataToTrajectoryRDD(fileRDD);
		// query processing
		GreedyQueryService greedyService = 
				new GreedyQueryService(trajectoryRDD);
		
		// run and save benchmark tests
		runGreedyTest("greedy-benchmark-EDwP-1h", 0, 3600, greedyService, nnBenchmarkCases);
		runGreedyTest("greedy-benchmark-EDwP-2h", 3600, 3600, greedyService, nnBenchmarkCases);
		runGreedyTest("greedy-benchmark-EDwP-4h", 7200, 7200, greedyService, nnBenchmarkCases);
		runGreedyTest("greedy-benchmark-EDwP-8h", 14400, 14400, greedyService, nnBenchmarkCases);
		
		trajectoryRDD.unpersist();
		fileRDD.unpersist();
		
		/************************
		 * VPAGES ALGORITHM TEST
		 ************************/
/*		DataPartitioningModule partitioningService = 
				new DataPartitioningModule();
		// run data partitioning and indexing
		partitioningService.run();
		
		// get the voronoi diagram abstraction.
		Broadcast<VoronoiDiagram> voronoiDiagram = 
				partitioningService.getVoronoiDiagram();
		// get the RDD of Voronoi pages 
		VoronoiPagesRDD voronoiPagesRDD = 
				partitioningService.getVoronoiPagesRDD();
		// get trajectory track table
		TrajectoryTrackTableRDD trajectoryTrackTable = 
				partitioningService.getTrajectoryTrackTable();
		// query processing
		QueryProcessingModule queryService = new QueryProcessingModule(
				voronoiPagesRDD, trajectoryTrackTable, voronoiDiagram); 
		
		// run and save benchmark tests
		runVoronoiTest("voronoi-benchmark-EDwP-1h", 0, 3600, queryService, nnBenchmarkCases);
		runVoronoiTest("voronoi-benchmark-EDwP-2h", 3600, 3600, queryService, nnBenchmarkCases);
		runVoronoiTest("voronoi-benchmark-EDwP-4h", 7200, 7200, queryService, nnBenchmarkCases);
		runVoronoiTest("voronoi-benchmark-EDwP-8h", 14400, 14400, queryService, nnBenchmarkCases);
		
		trajectoryTrackTable.unpersist();
		voronoiPagesRDD.unpersist();
		
		/************************
		 * COMPARE RESULTS
		 ************************/
		compareResults();
	}
	
	/**
	 * Run and save greedy benchmark tests.
	 */
	private static void runGreedyTest(
			String name, long t0, long t1,
			GreedyQueryService greedyService,
			List<Trajectory> nnBenchmarkCases){
		MyLogger LOG = new MyLogger();
		LOG.appendln("Greedy Test Results.\n");
		LOG.appendln(NUM_K + "-NN Result.\n");
		int queryId = 1;
		for(Trajectory t : nnBenchmarkCases){
			// params 1h 2h 4h 8h
			long tIni = t.timeIni() - t0;
			long tEnd = t.timeIni() + t1;
			// run query
			List<NearNeighbor> resultList = 
					greedyService.getKNNQuery(t, tIni, tEnd, NUM_K);
			LOG.appendln("Query " + queryId++ + ": [" + t.id + "] Result.");
			int n=1;
			for(NearNeighbor nn : resultList){
				LOG.appendln(n++ + "-NN: " + nn.id + " [d=" + nn.distance + "]");
			}
			LOG.appendln();
		}
		
		// save the result log to HDFS
		LOG.save(name);
	}
	
	/**
	 * Run and save VPages benchmark tests.
	 */
	private static void runVoronoiTest(
			String name, long t0, long t1,
			QueryProcessingModule queryService,
			List<Trajectory> nnBenchmarkCases){
		MyLogger LOG = new MyLogger();
		LOG.appendln("Voronoi-Pages Test Results.\n");
		LOG.appendln(NUM_K + "-NN Result.\n");
		int queryId = 1;
		for(Trajectory t : nnBenchmarkCases){
			// params 1h 2h 4h 8h
			long tIni = t.timeIni() - t0;
			long tEnd = t.timeIni() + t1;
			// run query
			List<NearNeighbor> resultList = 
					queryService.getKNearestNeighbors(t, tIni, tEnd, NUM_K);
			LOG.appendln("Query " + queryId++ + ": [" + t.id + "] Result.");
			int n=1;
			for(NearNeighbor nn : resultList){
				LOG.appendln(n++ + "-NN: " + nn.id + " [d=" + nn.distance + "]");
			}
			LOG.appendln();
		}
		
		// save the result log to HDFS
		LOG.save(name);		
	}
	
	/**
	 * Read the uses cases for spatial-temporal selection queries
	 */
	private static List<STBox> readSpatialTemporalTestCases(){
		List<String> lines = 
				HDFS.readFileHDFS("/spark-data/test-cases/spatial-temporal-benchmark-test-cases");
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
	private static List<Trajectory> readNearestNeighborTestCases(){
		List<String> lines = 
				HDFS.readFileHDFS("/spark-data/test-cases/nn-benchmark-test-cases");
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
	 * Read and compare the files generated in this experiment. 
	 */
	@SuppressWarnings("resource")
	private static void compareResults(){
		// files to read
		String benchmarkPath = "C:/lol/greedy";
		String comparePath   = "C:/lol/voronoi";
		File fileBenchmark = new File(benchmarkPath);
		File fileCompare   = new File(comparePath);
		// number of NNs on each tests
		int kBenchmark = 100;
		int kCompare  = 10;
		// total of queries to read
		int numQueries = 100;
		try {
			BufferedReader bufferBenchmark = new BufferedReader(
					new FileReader(fileBenchmark));
			BufferedReader bufferCompare = new BufferedReader(
					new FileReader(fileCompare));
			// line of the current files
			String lineBenchmark;
			String lineCompare;
			int sum = 0;
			// numQueries queries to read
			for(int i=0; i<numQueries; i++){
				// skip first line
				lineCompare = bufferCompare.readLine();
				lineBenchmark = bufferBenchmark.readLine();
				// read query results (10-NN)
				for(int j=0; j<kCompare; j++){
					// read voronoi
					lineCompare = bufferCompare.readLine();
					String[] tokensVoronoi = lineCompare.split(" ");
					
					// read greedy
					lineBenchmark = bufferBenchmark.readLine();
					String[] tokensGreedy = lineBenchmark.split(" ");
					
					// compare
					if(tokensVoronoi[1].equals(tokensGreedy[1])){
						sum++;
					} else{
						System.out.println("Not equal!");
						System.out.println("Benchmark: " + lineBenchmark);
						System.out.println("Compare: " + lineCompare);
						System.out.println();
					}
				}
				// read the next () lines of the benchmark
				for(int j=0; j<(kBenchmark-kCompare); j++){
					lineBenchmark = bufferBenchmark.readLine();
				}

				// read last blank line
				lineCompare   = bufferCompare.readLine();
				lineBenchmark = bufferBenchmark.readLine();
			}
			// print total 
			double similarity = ((double)sum / (double)(numQueries*kCompare)) * 100;
			System.out.println("Total Comparisons: " + (numQueries*kCompare));
			System.out.println("Total Similar: " + sum);
			System.out.println("Similarity: " + similarity + "%");
		
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
