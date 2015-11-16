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
import java.util.SortedSet;
import java.util.TreeSet;

import junit.framework.TestCase;
import uq.fs.LocalFileService;
import uq.spark.indexing.PageIndex;
import uq.spark.indexing.TrajectoryTrackTable;
import uq.spark.query.QueryProcessingService;
import uq.spark.query.SelectionQuery;
import uq.spatial.Box;
import uq.spatial.Point;
import uq.spatial.PointComparator;
import uq.spatial.Trajectory;
import uq.spatial.clustering.PartitioningAroundMedoids;
import uq.spatial.distance.DistanceService;
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

		/*
		Trajectory traj = FileService.readSparkTrajectoryById("T10");
		System.out.println("Trajectory Size: " + traj.size());
		System.out.println("Trajectory Length: " + traj.length());
		*/
		
		/*
		ArrayList<Trajectory> trajList = 
	    		FileService.generateSyntheticTrajectories(10000, 50, 100, 200); 
	    FileService.saveTrajectoriesFiles(trajList, 1000);
	    System.out.println("Finished");
		 */
		
		/*
		// test select Medoids
		long begin = System.currentTimeMillis();
		
		ArrayList<Point> samplePoints = 
				FileService.readSparkTrajectoriesAsPointList();
		for(Point p : samplePoints){
			System.out.println(p.x + ", " + p.y + ", " + p.time);
		}
		System.out.println("\n Size: " + samplePoints.size());
		
		long endRead = System.currentTimeMillis();  
		
		System.out.println("\n Running PAM..");
		Integer[] medoids = PAM.selectKInitialMedoids(10, samplePoints);
		
		long endSelect = System.currentTimeMillis(); 
		
		// Given the IDs, select the medoids (points)
		System.out.println("Selected Medoids:");
		for(int i=0; i<medoids.length; i++){
			Point p = samplePoints.get(medoids[i]);
			System.out.println(medoids[i] + ": (" + p.x + "," + p.y + "," + p.time + ")");
		}
		
		System.out.println();
		System.out.println("Took: " + (endRead - begin) + " milliseconds to read data.");
		System.out.println("Took: " + (endSelect - endRead) + " milliseconds to select medoids.");
		 */
		
		/*
		// test TrajectoryTrackTable
		TrajectoryTrackTable tb = new TrajectoryTrackTable();
		tb.add("T1", 1); 
		tb.add("T1", 2); 
		tb.add("T1", 5); 
		tb.add("T1", 2); 
		tb.add("T1", 1); 
		
		tb.add("T2", 2); 
		tb.add("T2", 4);
		tb.add("T2", 4);
		tb.add("T2", 3);
		
		tb.add("T3", 3);
		
		tb.print();
		*/

		/*
		// test HashSet 
		HashSet<Index> hash = new HashSet<Index>();
		Index i1 = new Index(1, 1);
		Index i2 = new Index(1, 2);
		Index i3 = new Index(2, 1);
		Index i4 = new Index(2, 2);
		Index i5 = new Index(1, 1);
		Index i6 = new Index(2, 2);
		hash.add(i1);
		hash.add(i2);
		hash.add(i3);
		hash.add(i4);
		hash.add(i5);
		hash.add(i6);
		
		Iterator<Index> itr = hash.iterator();
		Index i;
		while(itr.hasNext()){
			i = itr.next();
			System.out.println("(" + i.VSI + "," + i.TPI + ")");
		}
		*/
		
		/*
		// test MBR
		Point p1 = new Point(0, 3, 0);
		Point p2 = new Point(1, 4, 0);
		Point p3 = new Point(1, 0, 0);
		Point p4 = new Point(2, 2, 0);
		Point p5 = new Point(3, 1, 0);
		Trajectory t = new Trajectory();
		t.addPoint(p1);
		t.addPoint(p2);
		t.addPoint(p3);
		t.addPoint(p4);
		t.addPoint(p5);
		
		MBR mbr = t.MBR();
		mbr.print();
		*/
		
		/*
		// test tree
		TreeSet<Point> pivotsTree = new TreeSet<Point>();
		final Point p1 = new Point(0, 2, 0);
		Point p2 = new Point(0, 0.5, 0);
		Point p3 = new Point(-1, 0, 0);
		Point p4 = new Point(0, 1, 0);
		Point p5 = new Point(0, -1, 0);
		Point p6 = new Point(1, 0, 0);
		Point p7 = new Point(0, 3, 0);
		
		pivotsTree.add(p1);
		pivotsTree.add(p2);
		pivotsTree.add(p3);
		pivotsTree.add(p4);
		pivotsTree.add(p5);
		pivotsTree.add(p6);
		pivotsTree.add(p7);

		System.out.println();
		System.out.println("Pivots:");
		for (Point p : pivotsTree) {
			p.print();
		}
		System.out.println();
		
		// new comparator for the given point as origin
		Comparator<Point> comparator = new Comparator<Point>() {
			public int compare(Point o1, Point o2) {
				if(o1.equals(o2)){
					return 0;
				}
				if(o1.dist(p1) > o2.dist(p1)){
					return 1;
				}
				return -1;
			}
		};
				
		// new sorted tree
		TreeSet<Point> sortedTree = new TreeSet<Point>(comparator);

		for(Point point : pivotsTree){
			sortedTree.add(point);
		}
		System.out.println();
		System.out.println("Pivots:");
		for (Point p : sortedTree) {
			p.print();
		}
		System.out.println();
		*/
		
		/*
		// test range selection
		RangeQuery query = new RangeQuery();
		MBR mbr = new MBR(25, 35, 0, 15);
		
		Point p1 = new Point(10, 10, 0);p1.pivotId=1;
		Point p2 = new Point(10, 30, 0);p2.pivotId=2;
		Point p3 = new Point(20, 20, 0);p3.pivotId=3;
		Point p4 = new Point(30, 10, 0);p4.pivotId=4;
		Point p5 = new Point(30, 30, 0);p5.pivotId=5;
		List<Point> pivots = new ArrayList<Point>();
		pivots.add(p1); pivots.add(p2); pivots.add(p3); pivots.add(p4); pivots.add(p5);
		
		query.runRangeQuery(mbr, 2, 6, pivots);
		*/

		
	/*
		// test edges intersection
		VoronoiEdge ve = new VoronoiEdge(0, 0, 1, 1);
		
		if(ve.intersect(-1, 0.1, 2, 0.1)){
			System.out.println("Yes, i intersect!");
		} else{
			System.out.println("Nope!");
		}

*/		
/*		// test point in polygon
		VoronoiPolygon vp = new VoronoiPolygon(new Point(0,0));
		vp.addEdge(new VoronoiEdge(0, 0, 1, 1));
		vp.addEdge(new VoronoiEdge(1, 1, 2, 1));
		vp.addEdge(new VoronoiEdge(2, 1, 3, 0));
		vp.addEdge(new VoronoiEdge(3, 0, 2, -1));
	//	vp.addEdge(new VoronoiEdge(2, 1, 2, -1));
		vp.addEdge(new VoronoiEdge(2, -1, 1, -1));
		vp.addEdge(new VoronoiEdge(1, -1, 0, 0));
		
		Point p = new Point(1,0);
		
		if(vp.contains(p)){
			System.out.println("Is Inside!");
		} else{
			System.out.println("Nope!");
		}
		*/
	
/*
		// test polygon everlaping
		VoronoiPolygon vp = new VoronoiPolygon(new Point(1.5,0));
		vp.addEdge(new VoronoiEdge(0, 0, 1, 1));
		vp.addEdge(new VoronoiEdge(1, 1, 2, 1));
		vp.addEdge(new VoronoiEdge(2, 1, 3, 0));
		vp.addEdge(new VoronoiEdge(3, 0, 2, -1));
		vp.addEdge(new VoronoiEdge(2, -1, 1, -1));
		vp.addEdge(new VoronoiEdge(1, -1, 0, 0));
		
		Box box = new Box(1, 2, 1, 10);
		
		if(box.overlap(vp)){
			System.out.println("Yes, Overlap!");
		} else{
			System.out.println("Nope!");
		}

	
		// test points sort - clockwise
		Point p1 = new Point(1, 1);p1.pivotId=1;
		Point p2 = new Point(-1, 2);p2.pivotId=2;
		Point p3 = new Point(2, -3);p3.pivotId=3;
		Point p4 = new Point(-1, -10);p4.pivotId=4;
		Point p5 = new Point(-0.5, 0);p5.pivotId=5;
		Point p6 = new Point(1.5, 0);p6.pivotId=6;
		Point p7 = new Point(0, -8);p7.pivotId=7;
		Point p8 = new Point(0, 5);p8.pivotId=8;
				
		List<Point> list = new ArrayList<Point>();
		list.add(p1); list.add(p2); list.add(p3); list.add(p4);
		list.add(p5); list.add(p6); list.add(p7); list.add(p8);
		
		Point center = new Point(0.0, 0.0);
		PointComparator<Point> comp = new PointComparator<Point>(center);
		
		Collections.sort(list, comp);
		
		for(Point p : list){
			System.out.println(p.pivotId);
			p.print();
		}
*/
		
/*	
		// test edges clockwise
		VoronoiPolygon vp = new VoronoiPolygon(new Point(0,0));
		VoronoiEdge e1 = new VoronoiEdge(1, 1, 1.5, 0);
		e1.pivot1 = 1; e1.pivot2 = 6;
		VoronoiEdge e2 = new VoronoiEdge(2, -3, 1.5, 0);
		e2.pivot1 = 3; e2.pivot2 = 6;
		VoronoiEdge e3 = new VoronoiEdge(2, -3, 0, -8);
		e3.pivot1 = 3; e3.pivot2 = 7;
		VoronoiEdge e4 = new VoronoiEdge(-1, 2, 0, 5);
		e4.pivot1 = 2; e4.pivot2 = 8;
		VoronoiEdge e5 = new VoronoiEdge(-1, 2, -0.5, 0);
		e5.pivot1 = 2; e5.pivot2 = 5;
		VoronoiEdge e6 = new VoronoiEdge(-1, -10, -0.5, 0);
		e6.pivot1 = 4; e6.pivot2 = 5;
		VoronoiEdge e7 = new VoronoiEdge(-1, -10, 0, -8);
		e7.pivot1 = 4; e7.pivot2 = 7;
		VoronoiEdge e8 = new VoronoiEdge(0, 5, 1, 1);
		e8.pivot1 = 8; e8.pivot2 = 1;
		
		vp.addEdge(e1);
		vp.addEdge(e2);
		vp.addEdge(e3);
		vp.addEdge(e4);
		vp.addEdge(e5);
		vp.addEdge(e6);
		vp.addEdge(e7);
		vp.addEdge(e8);

		for(VoronoiEdge e : vp.getEdgeListClockwise()){
			e.print();
		}
*/

		/*
		/// test point in polygon - using sideness
		VoronoiPolygon vp = new VoronoiPolygon(new Point(0,0));
		VoronoiEdge e1 = new VoronoiEdge(1, 1, 1.5, 0);
		VoronoiEdge e2 = new VoronoiEdge(2, -3, 1.5, 0);
		VoronoiEdge e3 = new VoronoiEdge(2, -3, 0, -8);
		VoronoiEdge e4 = new VoronoiEdge(-1, 2, 0, 5);
		VoronoiEdge e5 = new VoronoiEdge(-1, 2, -0.5, 0);
		VoronoiEdge e6 = new VoronoiEdge(-1, -10, -0.5, 0);
		VoronoiEdge e7 = new VoronoiEdge(-1, -10, 0, -8);
		VoronoiEdge e8 = new VoronoiEdge(0, 5, 1, 1);

		
		vp.addEdge(e1);
		vp.addEdge(e2);
		vp.addEdge(e3);
		vp.addEdge(e4);
		vp.addEdge(e5);
		vp.addEdge(e6);
		vp.addEdge(e7);
		vp.addEdge(e8);

		Point p = new Point(1,1);
		
		if(vp.contains(p)){
			System.out.println("Yes, I am inside!");
		} else{
			System.out.println("Nope!");
		}
*/
		
/*		/// test box overlap polygon
		VoronoiPolygon vp = new VoronoiPolygon(new Point(10,10));
		VoronoiEdge e1 = new VoronoiEdge(1000000.0,-999990.0,-999990.0,1000000.0);
		VoronoiEdge e2 = new VoronoiEdge(1000000.0,-999970.0,-999970.0,1000000.0);
		
		vp.addEdge(e1);
		vp.addEdge(e2);

		Box box = new Box(15, 25, 0, 25);
		
		if(box.overlap(vp)){
			System.out.println("Yes, I ovelap!");
		} else {
			System.out.println("Nope!");
		}
*/
		
/*		// TEST crossing trajectories
		Trajectory q = new Trajectory();
		q.addPoint( new Point(13, 0, 0) );
		q.addPoint( new Point(0, 15, 1) );
		q.addPoint( new Point(5, 15, 2) );
		
		Trajectory t1 = new Trajectory("T1");
		t1.addPoint( new Point(1, 1, 0) );
		t1.addPoint( new Point(2, 2, 1) );
		t1.addPoint( new Point(3, 3, 2) );

		Trajectory t2 = new Trajectory("T2");
		t2.addPoint( new Point(10, 11, 3) );
		t2.addPoint( new Point(10, 12, 4) );
		t2.addPoint( new Point(10, 13, 5) );
		
		Trajectory t3 = new Trajectory("T3");
		t3.addPoint( new Point(20, 20, 0) );
		t3.addPoint( new Point(21, 20, 1) );
		t3.addPoint( new Point(22, 20, 2) );

		Trajectory t4 = new Trajectory("T4");
		t4.addPoint( new Point(1, 1, 4) );
		t4.addPoint( new Point(11, 11, 5) );
		t4.addPoint( new Point(21, 21, 6) );
		
		Trajectory t5 = new Trajectory("T4");
		t5.addPoint( new Point(1, 1, 2) );
		t5.addPoint( new Point(2, 2, 3) );
		t5.addPoint( new Point(11, 11, 4) );
		t5.addPoint( new Point(12, 12, 5) );

		if(q.intersect(q)){
			System.out.println("Yes, I intersect!");
		} else{
			System.out.println("Nope!");
		}
		*/
/*
		// test voronoi polygon edges construction
		Point p1 = new Point(0, 0);p1.pivotId=1;
		Point p2 = new Point(10, 10);p2.pivotId=2;
		Point p3 = new Point(15, -10);p3.pivotId=3;
		Point p4 = new Point(20, 3);p4.pivotId=4;
		Point p5 = new Point(25, 0);p5.pivotId=5;
		Point p6 = new Point(30, 20);p6.pivotId=6;
		Point p7 = new Point(38, 8);p7.pivotId=7;
		Point p8 = new Point(45, -5);p8.pivotId=8;
	
		List<Point> pivots = new ArrayList<Point>();
		pivots.add(p1); pivots.add(p2); pivots.add(p3); pivots.add(p4); 
		pivots.add(p5); pivots.add(p6); pivots.add(p7); pivots.add(p8);

		VoronoiDiagramGenerator vor = new VoronoiDiagramGenerator();

		List<VoronoiPolygon> list = 
		vor.generateVoronoiPolygons(pivots);
		
		for(VoronoiPolygon poly : list){
			poly.print();
			System.out.println();
		}	
			*/
	
		// KNN test
	/*	Trajectory q1 = new Trajectory();
		q1.addPoint( new Point(1.0, 211.0, 0) );
		q1.addPoint( new Point(2.0, 212.0, 1) );
		q1.addPoint( new Point(3.0, 211.0, 2) );
		q1.addPoint( new Point(4.0, 212.0, 3) );
		
		Trajectory t1 = new Trajectory();
		t1.addPoint( new Point(1.0, 1.0, 0) );
		t1.addPoint( new Point(2.0, 2.0, 1) );
		t1.addPoint( new Point(3.0, 1.0, 2) );
		t1.addPoint( new Point(4.0, 2.0, 3) );
		
		Trajectory t2 = new Trajectory();
		t2.addPoint( new Point(1.0, 101.0, 0) );
		t2.addPoint( new Point(2.0, 102.0, 1) );
		t2.addPoint( new Point(3.0, 101.0, 2) );
		t2.addPoint( new Point(4.0, 102.0, 3) );
		
		Trajectory t3 = new Trajectory();
		t3.addPoint( new Point(1.0, 201.0, 0) );
		t3.addPoint( new Point(2.0, 202.0, 1) );
		t3.addPoint( new Point(3.0, 201.0, 2) );
		t3.addPoint( new Point(4.0, 202.0, 3) );
 
		Trajectory t4 = new Trajectory();
		t4.addPoint( new Point(1.0, 301.0, 0) );
		t4.addPoint( new Point(2.0, 302.0, 1) );
		t4.addPoint( new Point(3.0, 301.0, 2) );
		t4.addPoint( new Point(4.0, 302.0, 3) );
		
		Trajectory t5 = new Trajectory();
		t5.addPoint( new Point(1.0, 231.0, 0) );
		t5.addPoint( new Point(2.0, 232.0, 1) );
		t5.addPoint( new Point(3.0, 231.0, 2) );
		t5.addPoint( new Point(4.0, 232.0, 3) );
		
		Trajectory t6 = new Trajectory();
		t6.addPoint( new Point(1.0, 141.0, 0) );
		t6.addPoint( new Point(2.0, 142.0, 1) );
		t6.addPoint( new Point(3.0, 141.0, 2) );
		t6.addPoint( new Point(4.0, 142.0, 3) );
		
		Trajectory t7 = new Trajectory();
		t7.addPoint( new Point(1.0, -102.0, 0) );
		t7.addPoint( new Point(2.0, -101.0, 1) );
		t7.addPoint( new Point(3.0, -102.0, 2) );
		t7.addPoint( new Point(4.0, -101.0, 3) );
		
		DistanceService distServ = new DistanceService();
		System.out.println("Dist Q to T1: " + distServ.EDwP(q1, t1));
		System.out.println("Dist Q to T2: " + distServ.EDwP(q1, t2));
		System.out.println("Dist Q to T3: " + distServ.EDwP(q1, t3));
		System.out.println("Dist Q to T4: " + distServ.EDwP(q1, t4));
		System.out.println("Dist Q to T5: " + distServ.EDwP(q1, t5));
		System.out.println("Dist Q to T6: " + distServ.EDwP(q1, t6));
		System.out.println("Dist Q to T7: " + distServ.EDwP(q1, t7));
		
		//RESULT: [T3][T5][T6][T4][T2][T1][T7]

		// test trajectory post process
/*		
		Trajectory q1 = new Trajectory();
		q1.addPoint( new Point(1.0, 201.0, 0) );
		q1.addPoint( new Point(2.0, 202.0, 1) );
		q1.addPoint( new Point(3.0, 201.0, 2) );
		q1.addPoint( new Point(2.0, 202.0, 1) );
		q1.addPoint( new Point(3.0, 201.0, 2) );
		q1.addPoint( new Point(4.0, 202.0, 3) );
		
		q1.sort();
		int size = q1.size();
		for(int i = 0; i < size-1; i++){
			if(q1.get(i).isSamePosition(q1.get(i+1))){
				q1.removePoint(i);
				size--;
			}
		}
		q1.print();
*/
		convertToMercator();
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
			
			for(int l=1; l<=10; l++){
			// 0.01
			for(int i=1; i<=5; i++) {
				line = buffer.readLine();
				String[] tokens = line.split(" ");
				
				double x1 = Double.parseDouble(tokens[0]);
				double x2 = Double.parseDouble(tokens[0]) + 0.2;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) + 0.2;
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
				double x2 = Double.parseDouble(tokens[0]) - 0.2;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) - 0.2;
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
				double x2 = Double.parseDouble(tokens[0]) + 0.2;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) - 0.2;
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
				double x2 = Double.parseDouble(tokens[0]) - 0.2;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) + 0.2;
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
				double x2 = Double.parseDouble(tokens[0]) + 0.3;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) + 0.3;
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
				double x2 = Double.parseDouble(tokens[0]) - 0.3;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) - 0.3;
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
				double x2 = Double.parseDouble(tokens[0]) + 0.3;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) - 0.3;
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
				double x2 = Double.parseDouble(tokens[0]) - 0.3;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) + 0.3;
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
				double x2 = Double.parseDouble(tokens[0]) + 0.5;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) + 0.5;
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
				double x2 = Double.parseDouble(tokens[0]) - 0.5;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) - 0.5;
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
				double x2 = Double.parseDouble(tokens[0]) + 0.5;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) - 0.5;
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
				double x2 = Double.parseDouble(tokens[0]) - 0.5;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) + 0.5;
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
				double x2 = Double.parseDouble(tokens[0]) + 0.7;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) + 0.7;
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
				double x2 = Double.parseDouble(tokens[0]) - 0.7;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) - 0.7;
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
				double x2 = Double.parseDouble(tokens[0]) + 0.7;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) - 0.7;
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
				double x2 = Double.parseDouble(tokens[0]) - 0.7;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) + 0.7;
				long t1 = Long.parseLong(tokens[2]);
				long t2 = Long.parseLong(tokens[2]) + 3600;
				script += x1 + " " + x2 + " " + y1 + " " + y2 + " " + t1 + " " + t2 +"\n";
				System.out.format("%.5f %.5f %.5f %.5f",x1,x2,y1,y2);
				System.out.println(" " + t1 + " " + t2);
			}
			
			// 0.1
			for(int i=1; i<=5; i++) {
				line = buffer.readLine();
				String[] tokens = line.split(" ");
				
				double x1 = Double.parseDouble(tokens[0]);
				double x2 = Double.parseDouble(tokens[0]) + 1.0;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) + 1.0;
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
				double x2 = Double.parseDouble(tokens[0]) - 1.0;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) - 1.0;
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
				double x2 = Double.parseDouble(tokens[0]) + 1.0;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) - 1.0;
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
				double x2 = Double.parseDouble(tokens[0]) - 1.0;
				double y1 = Double.parseDouble(tokens[1]);
				double y2 = Double.parseDouble(tokens[1]) + 1.0;
				long t1 = Long.parseLong(tokens[2]);
				long t2 = Long.parseLong(tokens[2]) + 3600;
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
