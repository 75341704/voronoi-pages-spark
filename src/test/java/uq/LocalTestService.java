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
import uq.spark.query.QueryProcessingService;
import uq.spark.query.SelectObject;
import uq.spark.query.SelectionQuery;
import uq.spatial.Box;
import uq.spatial.Point;
import uq.spatial.PointComparator;
import uq.spatial.TimeComparator;
import uq.spatial.Trajectory;
import uq.spatial.TrajectoryRTree;
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
		VoronoiPolygon vp = new VoronoiPolygon(new Point(5,5));
		vp.addEdge(new VoronoiEdge(0, 0, 10, 0));
		vp.addEdge(new VoronoiEdge(10, 10, 0, 10));
		vp.addEdge(new VoronoiEdge(0, 0, 0, 10));
		vp.addEdge(new VoronoiEdge(10, 10, 10, 0));

		Point p = new Point(5,15);
		
		if(vp.contains(p)){
			System.out.println("Is Inside!");
		} else{
			System.out.println("Nope!");
		}
		
		// test point in polygon
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

		String s = "(117,1189905,T_10000018_1427884720_15936251155122583926 516.7622983162232 224.76965756991802 1427884781 516.7493997047326 224.78549471439976 1427884850 516.7482877554661 224.78525833812822 1427884862 516.7518459931188 224.7798216920864 1427884867 516.7559602054046 224.77485781160448 1427884872 516.759852027837 224.7703666929374 1427884876 516.7638550451964 224.7658755849985 1427884880 516.7687476219687 224.76138448778727 1427884884 516.7719722748415 224.75677521496192 1427884889 516.7681916473355 224.75098409332634 1427884895 516.7638550451964 224.74542936076122 1427884899 516.7592960532038 224.73999282988464 1427884903 516.755515425698 224.73408357502055 1427884907 516.7531803322383 224.7291198152854 1427884910 516.7514012134121 224.7236833315619 1427884913 516.7481765605394 224.71682865709454 1427884916 516.7453966873734 224.71127402542206 1427884919 516.7415048649408 224.7057194101559 1427884922 516.7369458729484 224.70087390810912 1427884925 516.7326092708093 224.6954375060395 1427884928 516.72827266867 224.68988293754663 1427884931 516.7243808462375 224.6850374763011 1427884933 516.7199330491717 224.67936475704616 1427884936 516.715485252106 224.67416477942626 1427884939 516.7112598448936 224.66967390123247 1427884942 516.7074792173876 224.66530121434675 1427884945 516.7019194710552 224.6588012931676 1427884949)";
		String[] tokens = s.split("\\(+|\\s|,|\\)");
		
		for(String a : tokens){
			System.out.println(a);
		}*//*
		Trajectory t1 = new Trajectory("T1");
		t1.addPoint( new Point(0,0) );
		t1.addPoint( new Point(1,1) );
		t1.addPoint( new Point(2,0) );
		t1.addPoint( new Point(3,1) );
		
		Trajectory t2 = new Trajectory("T2");
		t2.addPoint( new Point(0,0) );
		t2.addPoint( new Point(1,1) );
		t2.addPoint( new Point(2,0) );
		t2.addPoint( new Point(3,1) );
		t2.addPoint( new Point(4,1) );
		
		Trajectory t3 = new Trajectory("T3");
		t3.addPoint( new Point(0,10) );
		t3.addPoint( new Point(1,11) );
		t3.addPoint( new Point(2,10) );
		t3.addPoint( new Point(3,11) );
		
		Trajectory t4 = new Trajectory("T4");
		t4.addPoint( new Point(0,0) );
		t4.addPoint( new Point(3,11) );

		TrajectoryRTree tree = new TrajectoryRTree();
		tree.add(t1);
		tree.add(t2);
		tree.add(t3);
		tree.add(t4);

		System.out.println("T1 BOX:");	
		t1.mbr().print();
		System.out.println("\nT1 Rectangle:");
		Rectangle r = t1.mbr().rectangle();
		System.out.println("("+r.minX+", "+r.maxY+") " + " ("+r.maxX+", "+r.maxY+")");
		System.out.println("("+r.minX+", "+r.minY+") " + " ("+r.maxX+", "+r.minY+")");
	
		Box region = new Box(0, 5, 5, 8);
		List<Trajectory> list = tree.getTrajectoriesByMBR(region);//getKNearest(region, 10);//TrajectoriesInRange(region);

		System.out.println("In the box");
		for(Trajectory t : list){
			System.out.println(t.id);
		}*/
		
	/*	
		Trajectory t1a = new Trajectory("T1");
		t1a.addPoint( new Point(0,0,0) );
		t1a.addPoint( new Point(1,1,1) );
		t1a.addPoint( new Point(2,0,2) );
		t1a.addPoint( new Point(3,1,3) );
		
		Trajectory t1b = new Trajectory("T1");
	//	t1b.addPoint( new Point(2,0,2) );
	//	t1b.addPoint( new Point(3,1,3) );
		t1b.addPoint( new Point(4,0,4) );
		t1b.addPoint( new Point(4,0,4) );
		t1b.addPoint( new Point(4,0,4) );
	//	t1b.addPoint( new Point(5,1,5) );
		t1b.addPoint( new Point(6,0,6) );
		
		Trajectory t1c = new Trajectory("T1");
	//	t1c.addPoint( new Point(5,1,5) );
	    t1c.addPoint( new Point(6,0,6) );
	//	t1c.addPoint( new Point(7,0,7) );
	//	t1c.addPoint( new Point(8,0,8) );
	/*		
		List<Trajectory> list = new ArrayList<Trajectory>();
		list.add(t1b);list.add(t1a);list.add(t1c);
	
		System.out.println("Before sort:");
		for(Trajectory t : list){
			t.print();
		}
		
		TimeComparator<Trajectory> comp = new TimeComparator<Trajectory>();
		Collections.sort(list, comp);
		
		System.out.println("\n\nAfter sort:");
		for(Trajectory t : list){
			t.print();
		}
		
		SelectObject obj = new SelectObject();
		obj.add(t1b);
		obj.add(t1a);
		obj.add(t1c);
		
		List<Trajectory> list = obj.postProcess();
		System.out.println("After Post-process:");
		for(Trajectory t : list){
			t.print();
		}*/
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
