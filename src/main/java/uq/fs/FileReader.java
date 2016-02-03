package uq.fs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import uq.spark.EnvironmentVariables;
import uq.spatial.Point;
import uq.spatial.STBox;
import uq.spatial.Trajectory;

/**
 * Service to read data files (Spark).
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class FileReader implements Serializable, EnvironmentVariables{
	private static final HDFSFileService hdfs = 
			new HDFSFileService();
	private static final DataConverter converter = 
			new DataConverter();

	/**
	 * Read the uses cases for spatial-temporal selection queries
	 */
	public static List<STBox> readSpatialTemporalTestCases(){
		List<String> lines = 
				hdfs.readFileHDFS("/spark-data/test-cases/spatial-temporal-test-cases");
		// process lines
		long timeIni, timeEnd;
		double minX, maxX, minY, maxY;
		List<STBox> stList = new LinkedList<STBox>(); 
		for(String line : lines){
			if(line.length() > 3){
				String[] tokens = line.split(" ");
				minX	= Double.parseDouble(tokens[0]);
				maxX	= Double.parseDouble(tokens[1]);
				minY	= Double.parseDouble(tokens[2]);
				maxY		= Double.parseDouble(tokens[3]);
				timeIni = Long.parseLong(tokens[4]);
				timeEnd = Long.parseLong(tokens[5]); 
				
				stList.add(new STBox(minX, maxX, minY, maxY, timeIni, timeEnd));
			}
		}
		return stList;
	}
	
	/**
	 * Read the uses cases for Nearest Neighbors queries.
	 */
	public static List<Trajectory> readNearestNeighborTestCases(){
		List<String> lines = 
				hdfs.readFileHDFS("/spark-data/test-cases/nn-test-cases");
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
	 * Read pivots file.
	 * 
	 * @param num The number of pivots to read..
	 * @return Return a list of pivots (points).
	 */
	public static List<Point> readPivots(final int num){	
		// read file lines
        List<String> lines = 
        		hdfs.readFileHDFS(PIVOTS_PATH);
		
		// fields to be read from the file
		double x, y;
		long time;
		String line; 

		List<Point> pivots = new ArrayList<Point>();

		// read K pivots (lines)
        int pivotId = 1;
		for(int i=0; i<num; i++){
			line = lines.get(i);
			if(line.length() > 2){
				String[] tokens = line.split(" ");
			
				x = Double.parseDouble(tokens[0]);
				y = Double.parseDouble(tokens[1]);
				time = Long.parseLong(tokens[2]);
				
				// new pivot for this input
				Point pivot = new Point(x,y,time);
				pivot.pivotId = pivotId++;
				
				pivots.add(pivot);
			}	
		}
		
		return pivots;
	}

	/**
	 * Read input dataset as RDD.
	 * 
	 * @return Return a RDD of string lines.
	 */
	public static JavaRDD<String> readDataAsRDD(){
		System.out.println("[FILE READER] Reading data..");
		
		// read raw data to Spark RDD
		JavaRDD<String> fileRDD = 
				SC.textFile(DATA_PATH, NUM_PARTITIONS_DATA);
		fileRDD.persist(STORAGE_LEVEL_DATA);
		
		return fileRDD;
	}
	
	/**
	 * Read input dataset and convert to a RDD of trajectories.
	 * 
	 * @return Return a RDD of trajectories.
	 */
	public static JavaRDD<Trajectory> readDataAsTrajectoryRDD(){
		System.out.println("[FILE READER] Reading data..");
		
		// read raw data to Spark RDD
		JavaRDD<String> fileRDD = 
				SC.textFile(DATA_PATH, NUM_PARTITIONS_DATA);
		fileRDD.persist(STORAGE_LEVEL_DATA);
		
		// convert the input data to a RDD of trajectory objects
		JavaRDD<Trajectory> trajectoryRDD = 
				converter.mapRawDataToTrajectoryRDD(fileRDD);
		
		return trajectoryRDD;
	}
	
	/**
	 * Read input dataset and convert to a RDD of points.
	 * Read each trajectory sample point as a single point object.
	 * 
	 * @return Return a RDD of points.
	 */
	public static JavaRDD<Point> readDataAsPointRDD(){
		System.out.println("[FILE READER] Reading data..");
		
		// read raw data to Spark RDD
		JavaRDD<String> fileRDD = 
				SC.textFile(DATA_PATH, NUM_PARTITIONS_DATA);
		fileRDD.persist(STORAGE_LEVEL_DATA);
		
		// convert the input data to a RDD of point objects
		JavaRDD<Point> pointRDD = 
				converter.mapRawDataToPointRDD(fileRDD);
		
		return pointRDD;
	}
}
