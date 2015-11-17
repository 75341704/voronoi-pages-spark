package uq.fs;

import java.io.BufferedReader;
import java.io.BufferedWriter; 
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;

import uq.spark.SparkEnvInterface;
import uq.spark.indexing.IndexParamInterface;
import uq.spatial.Point;
import uq.spatial.Trajectory;

/**
 * Service to read/write from/to the 
 * Hadoop File System (HDFS).
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class HDFSFileService implements SparkEnvInterface, Serializable, IndexParamInterface {
	
	/**
	 * Hadoop access configuration.
	 */
	private Configuration config = null;
	
	/**
	 * Constructor, setup HDFS configuration
	 */
	public HDFSFileService() {
		config = new Configuration();
		config.addResource(new Path(HADOOP_HOME + "/etc/hadoop/core-site.xml"));
		config.addResource(new Path(HADOOP_HOME + "/etc/hadoop/hdfs-site.xml"));
	}

	/**
	 * Read pivots from HDFS file system.
	 * Return a list of pivots (points).
	 * 
	 * @param num Number of pivots to read. Must be
	 * smaller or equal the number of lines in the 
	 * pivots file
	 */
	public List<Point> readPivotsHDFS(final int num){			
		// fields to be read from the file
		double x, y;
		long time;
		String line; 

		List<Point> pivots = new ArrayList<Point>();

		// read file lines
        List<String> lines = 
        		readFileHDFS(PIVOTS_PATH);
			
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
	 * Read file from HDFS file system.
	 * 
	 * @param filePath the path to the file, inside the HDFS
	 * @return Return a list of Stings with the file lines,
	 * one line per String in the list.
	 */
	public List<String> readFileHDFS(String filePath){			
		// fields to be read from the file
		try {
            FileSystem hdfs = FileSystem.get(config);
            Path path = new Path(HDFS_PATH + filePath);
            BufferedReader buffer = 
            		new BufferedReader(new InputStreamReader(hdfs.open(path)));
            // read file lines as string
            List<String> lines = new LinkedList<String>();
            while (buffer.ready()) {
            	lines.add(buffer.readLine());
            }
            System.out.println("File '" + filePath + "' successfully read from HDFS."); 
            buffer.close();
            
            return lines;
        } catch (IOException e) {
        	System.out.println("File '" + filePath + "' not found in the HDFS.");
            e.printStackTrace();
        } 
		return null;
	}
	
	/**
	 * Save this list of points to the HDFS /output folder.
	 */
	public void savePointListHDFS(List<Point> pointList, 
			String fileName){
		List<String> scriptList = new LinkedList<String>();
		
		String script;
		for(Point p : pointList){
			script = p.x + " " + p.y + " " + p.time;
			scriptList.add(script);
		}
		// save to HDFS
		saveStringListHDFS(scriptList, fileName);
	}
	
	/**
	 * Save this list of trajectories to the HDFS /output folder.
	 */
	public void saveTrajectoryListHDFS(List<Trajectory> trajectoryList, 
			String fileName){
		List<String> scriptList = new LinkedList<String>();
		
		String script = "";
		for(Trajectory t : trajectoryList){
			script += t.id;
			for(Point p : t.getPointsList()){
				script +=  " " + p.x + " " + p.y + " " + p.time;
			}
			scriptList.add(script);
		}
		// save to HDFS
		saveStringListHDFS(scriptList, fileName);
	}
	
	/**
	 * Save this list of Objects to the HDFS folder.
	 * One element per line.
	 * Objects should override toString() method.
	 * 
	 * @param listObj A list of object to save.
	 * @param fileName Name of the file, with its extension.
	 */
	public void saveObjectListHDFS(List<Object> listObj, String fileName){
		List<String> scriptList = new LinkedList<String>();
		for(Object obj : listObj){
			scriptList.add(obj.toString());
		}
		// save to HDFS
		saveStringListHDFS(scriptList, fileName);
	}
	
	/**
	 * Save this list of Strings to the HDFS folder.
	 * One String per line.
	 * 
	 * @param listObj A list of Strings to save.
	 * @param fileName Name of the file, with its extension.
	 */
	public void saveStringListHDFS(List<String> stringList, String fileName){
		String name = fileName;
        try {
			Path file = new Path(HDFS_PATH + HDFS_OUTPUT + name);
			FileSystem fs = FileSystem.get(new URI(HDFS_PATH), config);
			
			int i = 1;
			while(fs.isFile(file)){
				name = fileName + "-" + i++;
				file = new Path(HDFS_PATH + HDFS_OUTPUT + name);
			}
			
			BufferedWriter writer =
					new BufferedWriter(new OutputStreamWriter(fs.create(file,true)));
			//FSDataOutputStream out = fs.create(file);
			for(String record : stringList){
				writer.write(record + "\n");
			}
			writer.close();
			System.out.println("File '" + HDFS_OUTPUT + name + "' successfully saved to HDFS."); 
	    } catch(Exception e){
	    	System.out.println("ERROR when writing '" + name + "' to HDFS.");
	        e.printStackTrace();
	    }       
	}
	
	/**
	 * Save this String buffer to a file in the HDFS folder.
	 * If the file already exists, then save as 
	 * 'fileName-1', 'fileName-2', and so on.
	 * 
	 * @param scriptBuffer The buffer with the content of the file.
	 * @param fileName Name of the file, with its extension.
	 */
	public void saveStringBufferHDFS(StringBuffer scriptBuffer, String fileName) {
		String name = fileName;
		try {
			FileSystem fs = FileSystem.get(new URI(HDFS_PATH), config);
			Path file = new Path(HDFS_PATH + HDFS_OUTPUT + name);
			
			int i = 1;
			while(fs.isFile(file)){
				name = fileName + "-" + i++;
				file = new Path(HDFS_PATH + HDFS_OUTPUT + name);
			}
			
			BufferedWriter writer =
					new BufferedWriter(new OutputStreamWriter(fs.create(file,true)));
			writer.write(scriptBuffer.toString());
			writer.close();
			fs.close();
			System.out.println("File '" + HDFS_OUTPUT + name + "' successfully saved to HDFS."); 
	    } catch(Exception e){
	    	System.out.println("ERROR when writing '" + name + "' to HDFS.");
	        e.printStackTrace();
	    }
	}
	
	/**
	 * Save the application log file to the HDFS folder.
	 * If the file already exists, then save as 
	 * 'AppName-log-1', 'AppName-log-2', and so on.
	 * 
	 * @param logList The content of the log file.
	 */
	public void saveLogFileHDFS(List<String> logList, String appName){
		String baseName = appName + "-log";
		String name = baseName;
        try {
        	Path file = new Path(HDFS_PATH + APP_LOG + name);
        	FileSystem fs = FileSystem.get(new URI(HDFS_PATH), config);

        	int i = 1;
			while(fs.isFile(file)){
				name = baseName + "-" + i++;
				file = new Path(HDFS_PATH + APP_LOG + name);
			}

			BufferedWriter writer =
					new BufferedWriter(new OutputStreamWriter(fs.create(file,true)));
			//FSDataOutputStream out = fs.create(file);
			for(String record : logList){
				writer.write(record + "\n");
			}
			writer.close();
			System.out.println("File '" + APP_LOG + name + "' successfully saved to HDFS."); 
	    } catch(Exception e){
	    	System.out.println("ERROR when writing '" + name + "' to HDFS.");
	        e.printStackTrace();
	    }   
	}
	
	/**
	 * Save the file to the HDFS folder.
	 * If the file already exists, then save as 
	 * 'fileName-1', 'fileName-2', and so on.
	 * 
	 * @param script The content of the file
	 * @param fileName Name of the file, with its extension.
	 */
	public void saveFileHDFS(String script, String fileName){        
		String name = fileName;
		try {
			FileSystem fs = FileSystem.get(new URI(HDFS_PATH), config);
			Path file = new Path(HDFS_PATH + HDFS_OUTPUT + name);
			
			int i = 1;
			while(fs.isFile(file)){
				name = fileName + "-" + i++;
				file = new Path(HDFS_PATH + HDFS_OUTPUT + name);
			}
			
			BufferedWriter writer =
					new BufferedWriter(new OutputStreamWriter(fs.create(file,true)));
			writer.write(script);
			writer.close();
			fs.close();
			System.out.println("File '" + HDFS_OUTPUT + name + "' successfully saved to HDFS."); 
	    } catch(Exception e){
	    	System.out.println("ERROR when writing '" + name + "' to HDFS.");
	        e.printStackTrace();
	    }
	}

	/**
	 * Save this RDD to the HDFS output folder.
	 * 
	 * @param infoRDD The RDD with the file content.
	 * @param fileName The file name.
	 */
	public void saveRDDToHDFS(JavaRDD<String> infoRDD, String fileName) {
		String name = fileName;
		try {
			FileSystem fs = FileSystem.get(new URI(HDFS_PATH), config);
			Path file = new Path(HDFS_PATH + HDFS_OUTPUT + name);
			
			int i = 1;
			while(fs.isFile(file)){
				name = fileName + "-" + i++;
				file = new Path(HDFS_PATH + HDFS_OUTPUT + name);
			}
			
			// save the RDD
			infoRDD.saveAsTextFile(HDFS_PATH + HDFS_OUTPUT + name);
		}catch(Exception e){
	    	System.out.println("ERROR when writing '" + name + "' to HDFS.");
	        e.printStackTrace();
	    }
		
	}
}
