package uq;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.SortedSet;
import java.util.TreeSet;

import junit.framework.TestCase;
import net.sf.jsi.Rectangle;
import uq.fs.LocalFileService;
import uq.spark.index.Page;
import uq.spark.index.PageIndex;
import uq.spark.index.TrajectoryTrackTable;
import uq.spark.query.QueryProcessingModule;
import uq.spark.query.SelectObject;
import uq.spark.query.SelectionQueryCalculator;
import uq.spatial.Box;
import uq.spatial.Point;
import uq.spatial.PointComparator;
import uq.spatial.TimeComparator;
import uq.spatial.Trajectory;
import uq.spatial.TrajectoryRTree;
import uq.spatial.clustering.PartitioningAroundMedoids;
import uq.spatial.distance.EDwPDistanceCalculator;
import uq.spatial.distance.TrajectoryDistanceCalculator;
import uq.spatial.transformation.ProjectionTransformation;
import uq.spatial.voronoi.VoronoiDiagramGenerator;
import uq.spatial.voronoi.VoronoiEdge;
import uq.spatial.voronoi.VoronoiPolygon;

/**
 * Examples of some operations over trajectories 
 * running Locally.
 * 
 * @author uqdalves
 *
 */
public class LocalTestService  extends TestCase {
	static PartitioningAroundMedoids PAM = 
			new PartitioningAroundMedoids();
	
	public static void main(String[] args) { 
		/*LocalFileService serv = new LocalFileService();
		List<Trajectory> testCases = 
			serv.readTrajectories("C:/lol/");
		
		System.out.println("Size: " + testCases.size());
		
		Trajectory query = testCases.get(0);
		TrajectoryDistanceCalculator edwp = 
				new EDwPDistanceCalculator();
		for(Trajectory t : testCases){
			System.out.println("Dist: " + t.dist(query, edwp));
		}*/
		
		List<Point> list = null;
		if(!list.isEmpty()&& list!=null ){
			System.out.println("empty");
		} 
		list = new ArrayList<Point>();
		if(list.isEmpty()){
			System.out.println("empty");
		} 
		
	}
	
	/**
	 * Get the approximate area and time of the query in mercator
	 */
	public static void getAreaAndTimeMerc(){
		// file to read
		File file = new File("C:/lol/spatial-temporal-use-cases");
		try {
			BufferedReader buffer = new BufferedReader(
					new FileReader(file));
			// each line of the current file
			String line;
			double x1, y1, x2, y2;
			long t1, t2;
			while(buffer.ready()) {
				line = buffer.readLine();
				String[] tokens = line.split(" ");
				
				x1 = Double.parseDouble(tokens[0]);
				x2 = Double.parseDouble(tokens[1]);
				y1 = Double.parseDouble(tokens[2]);
				y2 = Double.parseDouble(tokens[3]);
				t1 = Long.parseLong(tokens[4]);
				t2 = Long.parseLong(tokens[5]);
				
				double area = Math.abs(x2-x1)*Math.abs(y2-y1);
				area = (area / (1.5*Math.pow(10, -8))) / Math.pow(10, 6);
				long time = t2-t1;
				System.out.println("Area: " + area + "(km2) Time: " + time + " (s)");
			}	
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * Read a file of points and convert to Mercator.
	 */
	public static void convertToMercator(){
		// file to read
		File file = new File("C:/lol/lol");
		try {
			BufferedReader buffer = new BufferedReader(
					new FileReader(file));
			// each line of the current file
			String line;
			String script = "";
			while(buffer.ready()) {
				line = buffer.readLine();
				String[] tokens = line.split(":");
				line = tokens[1].substring(1);
				System.out.println(line);
				
/*				line = buffer.readLine();
				String[] tokens = line.split(" ");
				
				double lon = Double.parseDouble(tokens[0]);
				double lat = Double.parseDouble(tokens[1]);
				long time = Long.parseLong(tokens[2]);

				double[] merc = ProjectionTransformation.getMercatorProjection(lon, lat);
				double x = merc[0];
				double y = merc[1];
				script += x + " " + y + " " + time + "\n";
				System.out.println(x + " " + y + " " + time);*/
			}	
		/*	script = script.substring(0, script.length()-1);
			BufferedWriter writer = new BufferedWriter(
					new FileWriter(new File("C:/lol/128g-pivots-random-4000.txt") ));
			writer.write(script);
			writer.flush();*/
			
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
		File file = new File("C:/lol/lol");
		try {
			BufferedReader buffer = new BufferedReader(
					new FileReader(file));
			// each line of the current file
			String line;
			String script = "";
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
				
				script += x1 + " " + x2 + " " + y1 + " " + y2 + " " + time1 + " " + time2 + "\n";
				System.out.println(x1 + " " + x2 + " " + y1 + " " + y2 + " " + time1 + " " + time2);
			}
			script = script.substring(0, script.length()-1);
			BufferedWriter writer = new BufferedWriter(
					new FileWriter(new File("C:/lol/spatial-temporal-use-cases") ));
			writer.write(script);
			writer.flush();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Read a pivots file and convert to Mercator.
	 */
	public static void convertNNUseCasesToMercator(){
		// file to read
		File file = new File("C:/lol/lol");
		try {
			BufferedReader buffer = new BufferedReader(
					new FileReader(file));
			// each line of the current file
			String line;
			String script = "";
			int q=1;
			while(buffer.ready()){
				line = buffer.readLine();
				String[] tokens = line.split(" ");
				// traj ID
				script += "Q_" + q++;
				
				for(int i=1; i<tokens.length-2; i+=3){
					double lon = Double.parseDouble(tokens[i]);
					double lat = Double.parseDouble(tokens[i+1]);
					String time = tokens[i+2];

					double[] merc = ProjectionTransformation.getMercatorProjection(lon, lat);
					double x = merc[0];
					double y = merc[1];
					script += " " + x + " " + y + " " + time;
				}
				script += "\n";
			}
			script = script.substring(0, script.length()-1);
			BufferedWriter writer = new BufferedWriter(
					new FileWriter(new File("C:/lol/nn-use-cases") ));
			writer.write(script);
			writer.flush();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void generateUseCases(){
		// file to read
		File file = new File("C:/lol/lol.txt");
		try {
			BufferedReader buffer = new BufferedReader(
					new FileReader(file));
			// each line of the current file
			String line;
	
			String script="";
			
			// 1000 inputs
			for(int l=1; l<=10; l++){
			
			// 0.01
			for(int i=1; i<=5; i++) {
				line = buffer.readLine();
				String[] tokens = line.split(" ");
				
				double x1 = Double.parseDouble(tokens[0]);
				double x2 = Double.parseDouble(tokens[0]) + 0.1;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) + 0.15;
				long t1 = Long.parseLong(tokens[2]);
				long t2 = Long.parseLong(tokens[2]) + 7200;
				
				script += x1 + " " + x2 + " " + y1 + " " + y2 + " " + t1 + " " + t2 +"\n";
				System.out.format("%.5f %.5f %.5f %.5f",x1,x2,y1,y2);
				System.out.println(" " + t1 + " " + t2);
			}
			for(int i=1; i<=5; i++) {
				line = buffer.readLine();
				String[] tokens = line.split(" ");
				
				double x1 = Double.parseDouble(tokens[0]);
				double x2 = Double.parseDouble(tokens[0]) - 0.1;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) - 0.15;
				long t1 = Long.parseLong(tokens[2]);
				long t2 = Long.parseLong(tokens[2]) + 1200;
				script += x1 + " " + x2 + " " + y1 + " " + y2 + " " + t1 + " " + t2 +"\n";
				System.out.format("%.5f %.5f %.5f %.5f",x1,x2,y1,y2);
				System.out.println(" " + t1 + " " + t2);
			}
			for(int i=1; i<=5; i++) {
				line = buffer.readLine();
				String[] tokens = line.split(" ");
				
				double x1 = Double.parseDouble(tokens[0]);
				double x2 = Double.parseDouble(tokens[0]) + 0.1;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) - 0.15;
				long t1 = Long.parseLong(tokens[2]);
				long t2 = Long.parseLong(tokens[2]) + 2400;
				script += x1 + " " + x2 + " " + y1 + " " + y2 + " " + t1 + " " + t2 +"\n";
				System.out.format("%.5f %.5f %.5f %.5f",x1,x2,y1,y2);
				System.out.println(" " + t1 + " " + t2);
			}
			for(int i=1; i<=5; i++) {
				line = buffer.readLine();
				String[] tokens = line.split(" ");
				
				double x1 = Double.parseDouble(tokens[0]);
				double x2 = Double.parseDouble(tokens[0]) - 0.1;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) + 0.15;
				long t1 = Long.parseLong(tokens[2]);
				long t2 = Long.parseLong(tokens[2]) + 3600;
				script += x1 + " " + x2 + " " + y1 + " " + y2 + " " + t1 + " " + t2 +"\n";
				System.out.format("%.5f %.5f %.5f %.5f",x1,x2,y1,y2);
				System.out.println(" " + t1 + " " + t2);
			}
			
			// 0.03
			for(int i=1; i<=5; i++) {
				line = buffer.readLine();
				String[] tokens = line.split(" ");
				
				double x1 = Double.parseDouble(tokens[0]);
				double x2 = Double.parseDouble(tokens[0]) + 0.35;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) + 0.35;
				long t1 = Long.parseLong(tokens[2]);
				long t2 = Long.parseLong(tokens[2]) + 7200;
				script += x1 + " " + x2 + " " + y1 + " " + y2 + " " + t1 + " " + t2 +"\n";
				System.out.format("%.5f %.5f %.5f %.5f",x1,x2,y1,y2);
				System.out.println(" " + t1 + " " + t2);
			}
			for(int i=1; i<=5; i++) {
				line = buffer.readLine();
				String[] tokens = line.split(" ");
				
				double x1 = Double.parseDouble(tokens[0]);
				double x2 = Double.parseDouble(tokens[0]) - 0.35;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) - 0.35;
				long t1 = Long.parseLong(tokens[2]);
				long t2 = Long.parseLong(tokens[2]) + 1200;
				script += x1 + " " + x2 + " " + y1 + " " + y2 + " " + t1 + " " + t2 +"\n";
				System.out.format("%.5f %.5f %.5f %.5f",x1,x2,y1,y2);
				System.out.println(" " + t1 + " " + t2);
			}
			for(int i=1; i<=5; i++) {
				line = buffer.readLine();
				String[] tokens = line.split(" ");
				
				double x1 = Double.parseDouble(tokens[0]);
				double x2 = Double.parseDouble(tokens[0]) + 0.35;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) - 0.35;
				long t1 = Long.parseLong(tokens[2]);
				long t2 = Long.parseLong(tokens[2]) + 2400;
				script += x1 + " " + x2 + " " + y1 + " " + y2 + " " + t1 + " " + t2 +"\n";
				System.out.format("%.5f %.5f %.5f %.5f",x1,x2,y1,y2);
				System.out.println(" " + t1 + " " + t2);
			}
			for(int i=1; i<=5; i++) {
				line = buffer.readLine();
				String[] tokens = line.split(" ");
				
				double x1 = Double.parseDouble(tokens[0]);
				double x2 = Double.parseDouble(tokens[0]) - 0.35;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) + 0.35;
				long t1 = Long.parseLong(tokens[2]);
				long t2 = Long.parseLong(tokens[2]) + 3600;
				script += x1 + " " + x2 + " " + y1 + " " + y2 + " " + t1 + " " + t2 +"\n";
				System.out.format("%.5f %.5f %.5f %.5f",x1,x2,y1,y2);
				System.out.println(" " + t1 + " " + t2);
			}
			
			// 0.05
			for(int i=1; i<=5; i++) {
				line = buffer.readLine();
				String[] tokens = line.split(" ");
				
				double x1 = Double.parseDouble(tokens[0]);
				double x2 = Double.parseDouble(tokens[0]) + 0.55;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) + 0.55;
				long t1 = Long.parseLong(tokens[2]);
				long t2 = Long.parseLong(tokens[2]) + 7200;
				script += x1 + " " + x2 + " " + y1 + " " + y2 + " " + t1 + " " + t2 +"\n";
				System.out.format("%.5f %.5f %.5f %.5f",x1,x2,y1,y2);
				System.out.println(" " + t1 + " " + t2);
			}
			for(int i=1; i<=5; i++) {
				line = buffer.readLine();
				String[] tokens = line.split(" ");
				
				double x1 = Double.parseDouble(tokens[0]);
				double x2 = Double.parseDouble(tokens[0]) - 0.55;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) - 0.55;
				long t1 = Long.parseLong(tokens[2]);
				long t2 = Long.parseLong(tokens[2]) + 1200;
				script += x1 + " " + x2 + " " + y1 + " " + y2 + " " + t1 + " " + t2 +"\n";
				System.out.format("%.5f %.5f %.5f %.5f",x1,x2,y1,y2);
				System.out.println(" " + t1 + " " + t2);
			}
			for(int i=1; i<=5; i++) {
				line = buffer.readLine();
				String[] tokens = line.split(" ");
				
				double x1 = Double.parseDouble(tokens[0]);
				double x2 = Double.parseDouble(tokens[0]) + 0.55;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) - 0.55;
				long t1 = Long.parseLong(tokens[2]);
				long t2 = Long.parseLong(tokens[2]) + 2400;
				script += x1 + " " + x2 + " " + y1 + " " + y2 + " " + t1 + " " + t2 +"\n";
				System.out.format("%.5f %.5f %.5f %.5f",x1,x2,y1,y2);
				System.out.println(" " + t1 + " " + t2);
			}
			for(int i=1; i<=5; i++) {
				line = buffer.readLine();
				String[] tokens = line.split(" ");
				
				double x1 = Double.parseDouble(tokens[0]);
				double x2 = Double.parseDouble(tokens[0]) - 0.55;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) + 0.55;
				long t1 = Long.parseLong(tokens[2]);
				long t2 = Long.parseLong(tokens[2]) + 3600;
				script += x1 + " " + x2 + " " + y1 + " " + y2 + " " + t1 + " " + t2 +"\n";
				System.out.format("%.5f %.5f %.5f %.5f",x1,x2,y1,y2);
				System.out.println(" " + t1 + " " + t2);
			}
			
			// 0.07
			for(int i=1; i<=5; i++) {
				line = buffer.readLine();
				String[] tokens = line.split(" ");
				
				double x1 = Double.parseDouble(tokens[0]);
				double x2 = Double.parseDouble(tokens[0]) + 0.75;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) + 0.80;
				long t1 = Long.parseLong(tokens[2]);
				long t2 = Long.parseLong(tokens[2]) + 600;
				script += x1 + " " + x2 + " " + y1 + " " + y2 + " " + t1 + " " + t2 +"\n";
				System.out.format("%.5f %.5f %.5f %.5f",x1,x2,y1,y2);
				System.out.println(" " + t1 + " " + t2);
			}
			for(int i=1; i<=5; i++) {
				line = buffer.readLine();
				String[] tokens = line.split(" ");
				
				double x1 = Double.parseDouble(tokens[0]);
				double x2 = Double.parseDouble(tokens[0]) - 0.75;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) - 0.80;
				long t1 = Long.parseLong(tokens[2]);
				long t2 = Long.parseLong(tokens[2]) + 1200;
				script += x1 + " " + x2 + " " + y1 + " " + y2 + " " + t1 + " " + t2 +"\n";
				System.out.format("%.5f %.5f %.5f %.5f",x1,x2,y1,y2);
				System.out.println(" " + t1 + " " + t2);
			}
			for(int i=1; i<=5; i++) {
				line = buffer.readLine();
				String[] tokens = line.split(" ");
				
				double x1 = Double.parseDouble(tokens[0]);
				double x2 = Double.parseDouble(tokens[0]) + 0.75;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) - 0.80;
				long t1 = Long.parseLong(tokens[2]);
				long t2 = Long.parseLong(tokens[2]) + 1200;
				script += x1 + " " + x2 + " " + y1 + " " + y2 + " " + t1 + " " + t2 +"\n";
				System.out.format("%.5f %.5f %.5f %.5f",x1,x2,y1,y2);
				System.out.println(" " + t1 + " " + t2);
			}
			for(int i=1; i<=5; i++) {
				line = buffer.readLine();
				String[] tokens = line.split(" ");
				
				double x1 = Double.parseDouble(tokens[0]);
				double x2 = Double.parseDouble(tokens[0]) - 0.75;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) + 0.80;
				long t1 = Long.parseLong(tokens[2]);
				long t2 = Long.parseLong(tokens[2]) + 2400;
				script += x1 + " " + x2 + " " + y1 + " " + y2 + " " + t1 + " " + t2 +"\n";
				System.out.format("%.5f %.5f %.5f %.5f",x1,x2,y1,y2);
				System.out.println(" " + t1 + " " + t2);
			}
			
			// 0.1
			for(int i=1; i<=5; i++) {
				line = buffer.readLine();
				String[] tokens = line.split(" ");
				
				double x1 = Double.parseDouble(tokens[0]);
				double x2 = Double.parseDouble(tokens[0]) + 1.1;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) + 1.15;
				long t1 = Long.parseLong(tokens[2]);
				long t2 = Long.parseLong(tokens[2]) + 600;
				script += x1 + " " + x2 + " " + y1 + " " + y2 + " " + t1 + " " + t2 +"\n";
				System.out.format("%.5f %.5f %.5f %.5f",x1,x2,y1,y2);
				System.out.println(" " + t1 + " " + t2);
			}
			for(int i=1; i<=5; i++) {
				line = buffer.readLine();
				String[] tokens = line.split(" ");
				
				double x1 = Double.parseDouble(tokens[0]);
				double x2 = Double.parseDouble(tokens[0]) - 1.1;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) - 1.15;
				long t1 = Long.parseLong(tokens[2]);
				long t2 = Long.parseLong(tokens[2]) + 1200;
				script += x1 + " " + x2 + " " + y1 + " " + y2 + " " + t1 + " " + t2 +"\n";
				System.out.format("%.5f %.5f %.5f %.5f",x1,x2,y1,y2);
				System.out.println(" " + t1 + " " + t2);
			}
			for(int i=1; i<=5; i++) {
				line = buffer.readLine();
				String[] tokens = line.split(" ");
				
				double x1 = Double.parseDouble(tokens[0]);
				double x2 = Double.parseDouble(tokens[0]) + 1.1;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) - 1.15;
				long t1 = Long.parseLong(tokens[2]);
				long t2 = Long.parseLong(tokens[2]) + 600;
				script += x1 + " " + x2 + " " + y1 + " " + y2 + " " + t1 + " " + t2 +"\n";
				System.out.format("%.5f %.5f %.5f %.5f",x1,x2,y1,y2);
				System.out.println(" " + t1 + " " + t2);
			}
			for(int i=1; i<=5; i++) {
				line = buffer.readLine();
				String[] tokens = line.split(" ");
				
				double x1 = Double.parseDouble(tokens[0]);
				double x2 = Double.parseDouble(tokens[0]) - 1.1;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) + 1.15;
				long t1 = Long.parseLong(tokens[2]);
				long t2 = Long.parseLong(tokens[2]) + 1200;
				script += x1 + " " + x2 + " " + y1 + " " + y2 + " " + t1 + " " + t2 +"\n";
				System.out.format("%.5f %.5f %.5f %.5f",x1,x2,y1,y2);
				System.out.println(" " + t1 + " " + t2);
			}
		}
			script = script.substring(0, script.length()-1);
			BufferedWriter writer = new BufferedWriter(
					new FileWriter(new File("C:/lol/spatial-temporal-use-cases") ));
			writer.write(script);
			writer.flush();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	
	}
	
}
