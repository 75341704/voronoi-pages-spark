package uq.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

/**
 * Environment interface.
 * Setup Spark, HDFS, MapReduce and cluster access variables.
 * 
 * @author uqdalves
 *
 */
public interface SparkEnvInterface {
	// Spark context
	static final JavaSparkContext SC = 
			MySparkContext.getInstance();
	
	// path to HDFS
	static final String HDFS_PATH =
			//"hdfs://master:54310";  // Cluster
			"hdfs://localhost:9000";  // Local
	
	// root path to data locally
	static final String LOCAL_PATH =
			"file:/media/bigdata/uqdalves/my-data";
	
	// path to the data set folders/files 
	static final String DATA_PATH =
			//HDFS_PATH + "/spark-data/trajectory-data/sample-1gb";
			/*
			LOCAL_PATH + "/trajectory-data/split1," + 
			LOCAL_PATH + "/trajectory-data/split2," +
			LOCAL_PATH + "/trajectory-data/split3," +
			LOCAL_PATH + "/trajectory-data/split4," +
			LOCAL_PATH + "/trajectory-data/split5," +
			LOCAL_PATH + "/trajectory-data/split6," +
			LOCAL_PATH + "/trajectory-data/split7," +
			LOCAL_PATH + "/trajectory-data/split8";
			 */
			HDFS_PATH + "/spark-data/trajectory-data/split1," + 
			HDFS_PATH + "/spark-data/trajectory-data/split2," +
			HDFS_PATH + "/spark-data/trajectory-data/split3," +
			HDFS_PATH + "/spark-data/trajectory-data/split4," +
			HDFS_PATH + "/spark-data/trajectory-data/split5," +
			HDFS_PATH + "/spark-data/trajectory-data/split6," +
			HDFS_PATH + "/spark-data/trajectory-data/split7," +
			HDFS_PATH + "/spark-data/trajectory-data/split8";

	// path to pivots file inside HDFS
	static final String PIVOTS_PATH = 
			"/spark-data/pivots/mercator/128g-pivots-kmeans-approx-2000.txt";
	
	// path to output folder inside HDFS
	static final String HDFS_OUTPUT = 
			"/spark-data/output/";
	
	// path to output log folder inside HDFS
	static final String APP_LOG = 
			"/spark-data/applog/";
	
	// Hadoop home path
	static final String HADOOP_HOME = 
			//"/usr/share/hadoop/hadoop-2.7.1";    // Cluster
			"/home/uqdalves/hadoop/hadoop-2.7.1";  // Local
	
	// the min number of partitions of the input
	static final int MIN_PARTITIONS = 2000; // 2000 120
		
	// number of reduce tasks for the indexing process
	static final int NUM_TASKS_INDEX = 1000; // 1000 120
	
	// number of reduce tasks for the query processing
	static final int NUM_TASKS_QUERY = 1000;  // 1000 120
	
	// Spark storage level for the partitioning process
	static final StorageLevel STORAGE_LEVEL_PARTITIONIG = 
			StorageLevel.MEMORY_ONLY();
	
	// Spark storage level for the query process
	static final StorageLevel STORAGE_LEVEL_QUERY = 
			StorageLevel.MEMORY_ONLY();
	
	// Spark storage level of the Pages RDD
	static final StorageLevel STORAGE_LEVEL_PAGES = 
			StorageLevel.MEMORY_ONLY();
	
	// Spark storage level of the Trajectory Track Table
	static final StorageLevel STORAGE_LEVEL_TTT = 
			StorageLevel.MEMORY_ONLY();
	
	// an infinity value
	static final double INF = Double.MAX_VALUE;
}