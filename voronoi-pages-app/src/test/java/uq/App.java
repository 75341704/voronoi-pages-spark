package uq;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;
import uq.fs.DataConverter;
import uq.fs.DataStatisticsService;
import uq.spark.EnvironmentVariables;
import uq.spatial.Trajectory;

@SuppressWarnings("serial")
public class App implements Serializable, EnvironmentVariables{
	static final String PATH = "file:/media/bigdata/uqdalves/my-data/";
	
	/**
	 * Main
	 */
	public static void main(String[] args){
		System.out.println("\nStarting..\n");
	
    	// read data
     	JavaRDD<String> fileRDD = SC.textFile(
				PATH + "trajectory-data-lat-long/day1," +
				PATH + "trajectory-data-lat-long/day2," +
				PATH + "trajectory-data-lat-long/day3," +
				PATH + "trajectory-data-lat-long/day4," +
				PATH + "trajectory-data-lat-long/day5");

     	// First map to convert the input file to trajectory objects
		DataConverter rddService = new DataConverter();
		JavaRDD<Trajectory> trajectoryRDD = 
				rddService.mapRawDataToTrajectoryRDD(fileRDD);

		trajectoryRDD.saveAsTextFile("/home/uqdalves/my-data/trajectory-data-lat-long/day5");
		
		System.out.println("\nFinished..\n");
	}
	
	public static void saveSample(List<Trajectory> list){
		String script = "";
		for(Trajectory t : list){
			script += t.toString() + "\n";
		}
		BufferedWriter writer;
		try {
			writer = new BufferedWriter(
					new FileWriter(new File("/home/uqdalves/my-data/sample-trajectories")));
			writer.write(script);
			writer.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
