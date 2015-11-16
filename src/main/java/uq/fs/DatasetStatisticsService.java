package uq.fs;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;

import uq.spark.SparkEnvInterface;
import uq.spatial.Trajectory;

/**
 * Service to calculate some statistics on the 
 * trajectory dataset (length, speed, etc.)
 * 
 * Use Spark MapReduce functions
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class DatasetStatisticsService implements Serializable, SparkEnvInterface{

	/**
	 * Number of trajectories in this dataset (RDD count).
	 */
	public static long numTrajectories(JavaRDD<Trajectory> trajectoryRDD){
			return trajectoryRDD.count();
	}
	
	/**
	 * Number of trajectories points in this dataset.
	 */
	public static long numTrajectoryPoints(JavaRDD<Trajectory> trajectoryRDD){
		final long totalPoints = 
			trajectoryRDD.map(new Function<Trajectory, Long>() {
				// map each trajectory to its size
				public Long call(Trajectory trajectory) throws Exception {
					return (long)trajectory.size();
				}
			}).reduce(new Function2<Long, Long, Long>() {
				// sum the trajectory lengths
				public Long call(Long size1, Long size2) throws Exception {
					return (size1 + size2);
				}
			});

		return totalPoints;
	}
	
	/**
	 * Statistics about length of trajectories in this dataset.
	 * 
	 * @return A double vector containing the mean: [0], min: [1]
	 * and max: [2] trajectories length in this dataset. 
	 */
	public static double[] trajectoryLengthStats(JavaRDD<Trajectory> trajectoryRDD){
		// total trajectories in this dataset
		final double numTrajectories = numTrajectories(trajectoryRDD);
	
		// get min, max and mean lenght of trajectories
		double[] lenghtVector = 
			trajectoryRDD.map(new Function<Trajectory, double[]>() {
					public double[] call(Trajectory t) throws Exception {
						double[] vec = new double[3];
						double lenght = t.length();
						vec[0] = lenght; vec[1] = lenght; vec[2] = lenght;
						return vec;
					}
			}).reduce(new Function2<double[], double[], double[]>() {
				public double[] call(double[] vec1, double[] vec2) throws Exception {
					vec1[0] = vec1[0] + vec2[0];
					vec1[1] = Math.min(vec1[1], vec2[1]);
					vec1[2] = Math.max(vec1[2], vec2[2]);
					return vec1;
				}
			});
		lenghtVector[0] = lenghtVector[0]/numTrajectories;

		return lenghtVector;
	}
	
	/**
	 * Statistics about duration of trajectories in this dataset.
	 * 
	 * @return A double vector containing the mean: [0], min: [1]
	 * and max: [2] trajectories duration (time) in this dataset. 
	 */
	public static double[] trajectoryDurationStats(JavaRDD<Trajectory> trajectoryRDD){
		// total trajectories in this dataset
		final double numTrajectories = numTrajectories(trajectoryRDD);		

		// get min, max and mean duration of trajectories
		double[] durationVector = 
			trajectoryRDD.map(new Function<Trajectory, double[]>() {
					public double[] call(Trajectory t) throws Exception {
						double[] vec = new double[3];
						double duration = t.duration();
						vec[0] = duration; vec[1] = duration; vec[2] = duration;
						return vec;
					}
			}).reduce(new Function2<double[], double[], double[]>() {
				public double[] call(double[] vec1, double[] vec2) throws Exception {
					vec1[0] = vec1[0] + vec2[0];
					vec1[1] = Math.min(vec1[1], vec2[1]);
					vec1[2] = Math.max(vec1[2], vec2[2]);
					return vec1;
				}
			});
		durationVector[0] = durationVector[0]/numTrajectories;

		return durationVector;
	}
	
	/**
	 * Statistics about speed of trajectories in this dataset.
	 * 
	 * @return A double vector containing the mean: [0], min: [1]
	 * and max: [2] trajectories average speed in this dataset. 
	 */
	public static double[] trajectorySpeedStats(JavaRDD<Trajectory> trajectoryRDD){
		// total trajectories in this dataset
		final double numTrajectories = numTrajectories(trajectoryRDD);

		// get min, max and mean speed of trajectories
		double[] speedVector = 
			trajectoryRDD.map(new Function<Trajectory, double[]>() {
					public double[] call(Trajectory t) throws Exception {
						double[] vec = new double[3];
						double speed = t.speed();
						vec[0] = speed; vec[1] = speed; vec[2] = speed;
						return vec;
					}
			}).reduce(new Function2<double[], double[], double[]>() {
				public double[] call(double[] vec1, double[] vec2) throws Exception {
					vec1[0] = vec1[0] + vec2[0];
					vec1[1] = Math.min(vec1[1], vec2[1]);
					vec1[2] = Math.max(vec1[2], vec2[2]);
					return vec1;
				}
			});
		speedVector[0] = speedVector[0]/numTrajectories;

		return speedVector;
	}
	
	/**
	 * Statistics about sampling rate of trajectories in this dataset.
	 * 
	 * @return A double vector containing the mean: [0], min: [1]
	 * and max: [2] trajectories average sampling rate in this dataset. 
	 */
	public static double[] trajectorySamplingRateStats(JavaRDD<Trajectory> trajectoryRDD){
		// total trajectories in this dataset
		final double numTrajectories = numTrajectories(trajectoryRDD);
		
		// get min, max and mean sampling rates
		double[] rateVector =
			trajectoryRDD.map(new Function<Trajectory, double[]>() {
					public double[] call(Trajectory t) throws Exception {
						double[] vec = new double[3];
						double rate = t.samplingRate();
						vec[0] = rate; vec[1] = rate; vec[2] = rate;
						return vec;
					}
			}).reduce(new Function2<double[], double[], double[]>() {
				public double[] call(double[] vec1, double[] vec2) throws Exception {
					vec1[0] = vec1[0] + vec2[0];
					vec1[1] = Math.min(vec1[1], vec2[1]);
					vec1[2] = Math.max(vec1[2], vec2[2]);
					return vec1;
				}
			});
		rateVector[0] = rateVector[0]/numTrajectories;

		return rateVector;
	}
	
	/**
	 * Statistics about the average number of the points per 
	 * trajectory in this dataset.
	 * 
	 * @return A double vector containing the mean: [0], min: [1]
	 * and max: [2] average points per trajectory in this dataset. 
	 */
	public static double[] trajectoryNumberOfPointsStats(JavaRDD<Trajectory> trajectoryRDD){
		// total trajectories in this dataset
		final double numTrajectories = numTrajectories(trajectoryRDD);

		// get min, max and mean number of points 
		double[] numPtsVector = 
			trajectoryRDD.map(new Function<Trajectory, double[]>() {
					public double[] call(Trajectory t) throws Exception {
						double[] vec = new double[3];
						double numPts = t.size();
						vec[0] = numPts; vec[1] = numPts; vec[2] = numPts;
						return vec;
					}
			}).reduce(new Function2<double[], double[], double[]>() {
				public double[] call(double[] vec1, double[] vec2) throws Exception {
					vec1[0] = vec1[0] + vec2[0];
					vec1[1] = Math.min(vec1[1], vec2[1]);
					vec1[2] = Math.max(vec1[2], vec2[2]);
					return vec1;
				}
			});
		numPtsVector[0] = numPtsVector[0]/numTrajectories;

		return numPtsVector;
	}
	
	/**
	 * Calculate and save the dataset statistics as a file to HDFS.
	 * </br>
	 * Save as "dataset-statistics" in the HDFS output folder.
	 */
	public static void saveDatasetStatistics(JavaRDD<Trajectory> trajectoryRDD){
     	long numTraj = numTrajectories(trajectoryRDD);
     	long numPts = numTrajectoryPoints(trajectoryRDD);
     	double[] avgPts = trajectoryNumberOfPointsStats(trajectoryRDD);
     	double[] avgDur = trajectoryDurationStats(trajectoryRDD);
     	double[] avgLen = trajectoryLengthStats(trajectoryRDD);
     	double[] avgSpeed = trajectorySpeedStats(trajectoryRDD);
     	double[] avgRate = trajectorySamplingRateStats(trajectoryRDD);
     	
     	// save results to HDFS
     	String script ="";
     	script += "Total Number of Trajectories: " + numTraj + "\n";
     	script += "Total Number of Points: " + numPts + "\n";
     	script += "Avg. Number of Points per Trajectory: " + avgPts[0] + "\n";
     	script += "Min. Number of Points per Trajectory: " + avgPts[1] + "\n";
     	script += "Max. Number of Points per Trajectory: " + avgPts[2] + "\n";
     	script += "Avg. Trajectory Length: " + avgLen[0] + "\n";
     	script += "Min. Trajectory Length: " + avgLen[1] + "\n";
     	script += "Max. Trajectory Length: " + avgLen[2] + "\n";
     	script += "Avg. Trajectory Duration: " + avgDur[0] + "\n";
     	script += "Min. Trajectory Duration: " + avgDur[1] + "\n"; 
     	script += "Max. Trajectory Duration: " + avgDur[2] + "\n"; 
     	script += "Avg. Trajectory Speed: " + avgSpeed[0] + "\n";
     	script += "Min. Trajectory Speed: " + avgSpeed[1] + "\n";
     	script += "Max. Trajectory Speed: " + avgSpeed[2] + "\n";
     	script += "Avg. Sampling Rate: " + avgRate[0] + "\n";
     	script += "Min. Sampling Rate: " + avgRate[1] + "\n";
     	script += "Max. Sampling Rate: " + avgRate[2];
     	
     	// save to HDFS
     	HDFSFileService hdfs = new HDFSFileService();
     	hdfs.saveFileHDFS(script, "dataset-statistics");
	}
	
	/**
	 * Calculate ands save the dataset statistics as a History file to HDFS.
	 * </br>
	 * "id size length duration speed sampling-rate"
	 * </br>
	 * This is useful for histogram graph construction.
	 * </br>
	 * Save as "dataset-statistics-histogram" in the HDFS output folder.
	 */
	public static void saveDatasetStatisticsHist(JavaRDD<Trajectory> trajectoryRDD){
		// id, num pts, length, duration, speed, sampling rate
		List<String> emptyList = new LinkedList<String>();
		Function2<List<String>, Trajectory, List<String>> seqOp = 
				new Function2<List<String>, Trajectory, List<String>>() {
			public List<String> call(List<String> list, Trajectory t) throws Exception {
				String script="";
				script += t.id + " ";
				script += t.size() + " ";
				script += t.length() + " ";
				script += t.duration() + " ";
				script += t.speed() + " ";
				script += t.samplingRate();
				list.add(script);
				return list;
			}
		};
		Function2<List<String>, List<String>, List<String>> combOp = 
				new Function2<List<String>, List<String>, List<String>>() {
			
			public List<String> call(List<String> list1, List<String> list2) throws Exception {
				list1.addAll(list2);
				return list1;
			}
		};
		String header = "id size length duration speed sampling-rate";
		List<String> hist = new LinkedList<String>();
		hist.add(header);
		hist.addAll(trajectoryRDD.aggregate(emptyList, seqOp, combOp));
				
		// save to HDFS
		HDFSFileService hdfs = new HDFSFileService();
     	hdfs.saveStringListHDFS(hist, "dataset-statistics-histogram");
	}
	
	/**
	 * Main
	 */
	public static void main(String[] arg0){
		System.out.println();
		System.out.println("Running Dataset Statistics Service..\n");
		
    	// read trajectory data files
     	JavaRDD<String> fileRDD = SC.textFile(DATA_PATH);
     	fileRDD.persist(StorageLevel.MEMORY_AND_DISK());
     	
     	// convert the input dataset to trajectory objects (read the dataset in lat/lon)
     	FileToObjectRDDService rddService = new FileToObjectRDDService();
     	JavaRDD<Trajectory> trajectoryRDD = rddService.mapRawDataToTrajectoryRDD(fileRDD);
   	
     	// calculate and save statistics to HDFS
     	saveDatasetStatistics(trajectoryRDD);
    // 	saveDatasetStatisticsHist(trajectoryRDD);
     	
     	// clear cache
     	fileRDD.unpersist();
     	trajectoryRDD.unpersist();
	}
}
