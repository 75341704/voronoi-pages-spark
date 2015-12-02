package uq.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import uq.spark.index.IndexParamInterface;

/**
 * Environment interface.
 * Setup Spark, HDFS, MapReduce and cluster access variables.
 * 
 * @author uqdalves
 *
 */
public interface SparkEnvInterface extends IndexParamInterface {
	// Spark context
	static final JavaSparkContext SC = 
			MySparkContext.getInstance();
	
	// path to HDFS
	static final String HDFS_PATH =
			"hdfs://localhost:9000";
	
	// path to Tachyon memory storage
	static final String TACHYON_PATH =
			"tachyon://localhost:19998"; 
	
	// path to data locally
	static final String LOCAL_PATH =
			"file:/home/uqdalves/my-data";
	
	// path to the data set folders/files 
	static final String DATA_PATH =
			//"file:/media/bigdata/uqdalves/my-data/trajectory-data/sample-4gb";
			/*
			TACHYON_PATH + "/trajectory-data/split1," + 
			TACHYON_PATH + "/trajectory-data/split2," +
			TACHYON_PATH + "/trajectory-data/split3," +
			TACHYON_PATH + "/trajectory-data/split4";
			*//*
			HDFS_PATH + "/spark-data/trajectory-data/split1," + 
			HDFS_PATH + "/spark-data/trajectory-data/split2," +
			HDFS_PATH + "/spark-data/trajectory-data/split3," +
			HDFS_PATH + "/spark-data/trajectory-data/split4";
			*/
			LOCAL_PATH + "/trajectory-data/split1," + 
			LOCAL_PATH + "/trajectory-data/split2," +
			LOCAL_PATH + "/trajectory-data/split3," +
			LOCAL_PATH + "/trajectory-data/split4";
	
	// path to pivots file inside HDFS
	static final String PIVOTS_PATH = 
			"/spark-data/pivots/mercator/16g-pivots-kmeans-approx-" + K + ".txt";
	
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
	static final int NUM_PARTITIONS_DATA = 60; // number of data blocks
		
	// number of reduce tasks for the indexing process
	static final int NUM_PARTITIONS_PAGES = 4 * NUM_PARTITIONS_DATA;
	
	// number of reduce tasks for the indexing process
	static final int NUM_PARTITIONS_TTT = 2 * NUM_PARTITIONS_DATA;
		
	// RDD storage level for the partitioning process
	static final StorageLevel STORAGE_LEVEL_PARTITIONIG = 
			StorageLevel.MEMORY_ONLY();
	
	// RDD storage level of the Pages RDD
	static final StorageLevel STORAGE_LEVEL_PAGES = 
			StorageLevel.MEMORY_ONLY();
	
	// RDD storage level of the Trajectory Track Table
	static final StorageLevel STORAGE_LEVEL_TTT = 
			StorageLevel.MEMORY_ONLY();

	// an infinity value
	static final double INF = Double.MAX_VALUE;
}