package uq;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import uq.spatial.Point;
import uq.spatial.delaunay.DelaunayTriangulation;
import uq.spatial.delaunay.TriangleEdge;
import uq.spatial.delaunay.Triangle;

/**
 * Build a Voronoi diagram from a set of points (pivots).
 * Given a list of points, first calculate the Delaunay 
 * triangulation, then calculate the dual graph of the 
 * triagulation. Return the Voronoi polygon edges.
 * 
 * Note that the list of points must not contain  
 * repeated points, i.e. same X and Y coordinates.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class VoronoiDiagramGenerator implements Serializable {
	
	/**
	 * Test
	 */
	public static void main(String []arg0){
	/*	Point p1 = new Point(0, 40, 1);p1.pivotId=1;
		Point p2 = new Point(40, -10, 2);p2.pivotId=2;
		Point p3 = new Point(50, 100, 3);p3.pivotId=3;
		Point p4 = new Point(95, 0, 4);p4.pivotId=4;
		Point p5 = new Point(120, 60, 5);p5.pivotId=5;*/
		
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
		
		List<TriangleEdge> edges = build(pivots);
		
		System.out.println("Voronoi Edges: " + edges.size());
		for(TriangleEdge e : edges){
			e.print();
			System.out.println();
		}
	}
	
	/**
	 * Build a Voronoi diagram from a set of points (pivots).
	 * Return the Voronoi polygon edges.
	 */
	public static List<TriangleEdge> build(List<Point> pointsList){
		// computes the Delaunay triangulation
		DelaunayTriangulation delaunay = 
				new DelaunayTriangulation();
		List<Triangle> triangleList = 
				delaunay.triangulate(pointsList);

		// max bounding rectangle
	/*	double minX = -100000;
		double maxX = 100000;
		double minY = -100000;
		double maxY = 100000;
	*/	
		// build an edge between every triangle's circumcenter
		// and its adjacent's triangle circumcenter.
		List<TriangleEdge> edgeList = new ArrayList<TriangleEdge>();
		for(int i=0; i<triangleList.size()-1; i++){
			Triangle tri_i = triangleList.get(i);
			int numAdjacent = 0;
			// compute internal edges
			for(int j=i+1; j<triangleList.size(); j++){
				Triangle tri_j = triangleList.get(j);
				if(tri_j.isAdjacent(tri_i)){
					edgeList.add(new TriangleEdge(tri_i.circumcenter(), 
							  			  tri_j.circumcenter()));
					numAdjacent++;
				}
				// already computed all edges
				if(numAdjacent == 3){
					break;
				}
			}
			// TODO:
			// if tri_i is a border triangle, 
			// compute external edges.
			if(numAdjacent == 2){
				
				// get the outer edge of this triangle
				
				// computa o vetor que eh perpendicular (ortogonal) a esta aresta
				
			}
			if(numAdjacent == 1){
				
			}
		}
		// se o triangulo so tivetr dois adjacentes, 
		// entao ele eh um triangulo de borda,
		// tem que fazer uma aresta do circuncentro dele indo pro infinito		

		
		return edgeList;
	}
}
