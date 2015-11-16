package uq.spatial.delaunay;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import uq.spatial.GeoInterface;
import uq.spatial.Point;

/**
 * Implements a Delaunay Triangulation using
 * Bowyer-Watson algorithm.
 *  
 * Note that the list of points must not contain  
 * repeated points, i.e. same X and Y coordinates. 
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class DelaunayTriangulation implements Serializable, GeoInterface {
	// epsilon
	private static final double EPS = 0.000001;

	/**
	 * Compute the Delaunay triangulation for a finite set of points.
	 * Return a list of Delaunay triangles.
	 */
	public static List<Triangle> triangulate(List<Point> pointsList){
		// clone the points list (to avoid changes on the original)
		List<Point> clone_list = new ArrayList<Point>();
		for(Point p : pointsList){
			clone_list.add(p.clone());
		}
		clone_list = pointsList;

		return BowyerWatson(clone_list);
	}
	
	/**
	 * The Bowyer-Watson algorithm for Delaunay triangulation.
	 * Compute the Delaunay triangulation for a finite set of points
	 * by inserting each point at a time in the triangles mesh.
	 * Return a list of triangles.
	 * 
	 * Bowyer, Adrian (1981). "Computing Dirichlet tessellations".
	 * Watson, David F. (1981). "Computing the n-dimensional Delaunay 
	 * tessellation with application to Voronoi polytopes".
	 */
	private static List<Triangle> BowyerWatson(List<Point> pointsList){
	    // initialize triangle list
		List<Triangle> triList = new ArrayList<Triangle>();

		// create super triangle, must be large enough to
		// completely contain all the points in pointsList
		Point p1 = new Point(-3*MIN_X, -MIN_Y); p1.pivotId = 100001;
		Point p2 = new Point(0, MIN_Y);        p2.pivotId = 100002;
		Point p3 = new Point(MIN_X, -3*MIN_Y);  p3.pivotId = 100003;
		Triangle superTriangle = new Triangle(p1, p2, p3);
		
		// add super-triangle to triangulation
		triList.add(superTriangle); 
	    
		// add all the points one at a time to the triangulation
	    for(Point p : pointsList){
	    	List<Triangle> badTriangles = new ArrayList<Triangle>();
	    	for(Triangle tri : triList){
	    		// check if the point is inside circumcircle of the triangle
	    		double radius = tri.radius();
	    		Point circ_center = tri.circumcenter();
	    		double dist = p.dist(circ_center);
	    		// find all the triangles that are no longer 
	    		// valid due to the point insertion
	    		if((dist + EPS) < radius){
	    			badTriangles.add(tri);
	    		}
	    	}
	    	// find the boundary of the polygonal hole
	    	List<TriangleEdge> edgeList = new ArrayList<TriangleEdge>(); 	
	    	for(int i=0; i < badTriangles.size(); i++){
	    		Triangle bad_i = badTriangles.get(i);
	    		for(TriangleEdge edge : bad_i.edges()){
	    			// if edge is not shared by any other triangles in badTriangles
	    			boolean shared = false;
	    			for(int j=0; j < badTriangles.size(); j++){
	    				Triangle bad_j = badTriangles.get(j);
	    				if(j!=i && bad_j.contains(edge)){
	    					shared = true; 
	    					break;
	    				}
		    		}	
		    		if(!shared){
		    			edgeList.add(edge);
		    		}
	    		}
	    	}
	    	// remove bad triangles from triangulation
	    	for(Triangle tri : badTriangles){
	    		triList.remove(tri);
	    	}
	    	// re-triangulate the polygonal hole.
	    	// for each edge, form a triangle from edge to point	
	    	for(TriangleEdge edge : edgeList){
	    		Triangle newTri = new Triangle(p, edge.p1, edge.p2);
	    		triList.add(newTri);
	    	} 
	    }
	    // done inserting points, now clean up
	    List<Triangle> triangulation = new ArrayList<Triangle>();
	    for(Triangle tri : triList){
	    	if(!tri.contains(p1) && !tri.contains(p2) && !tri.contains(p3)){
	    		triangulation.add(tri);
	    	}
	    }

	    return triangulation;
	}
}
