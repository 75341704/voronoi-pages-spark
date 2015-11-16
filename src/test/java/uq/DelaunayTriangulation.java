package uq;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import uq.spatial.Point;
import uq.spatial.delaunay.TriangleEdge;
import uq.spatial.delaunay.Triangle;

/**
 * Implements a Delaunay Triangulation using Guibas-Stolfi and
 * Bowyer-Watson algorithms.
 *  
 * Note that the list of points must not contain  
 * repeated points, i.e. same X and Y coordinates. 
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class DelaunayTriangulation implements Serializable {
	// infinity
	private final static double INF = Double.MAX_VALUE;
	
	// epsilon
	private static final double EPS = 0.000001;
	
	/**
	 * Comparator to sort points by X coordinate
	 */
	private static final Comparator<Point> POINT_COMPARATOR = new Comparator<Point>() {
		public int compare(Point p1, Point p2) {
			if(p1.x == p2.x){
				return (p1.y == p2.y) ? 0 : (p1.y > p2.y ? 1: -1);
			}
			return p1.x > p2.x ? 1 : -1;
		}
	};
	
	/**
	 * Compute the Delaunay triangulation for a finite set of points.
	 * Return a list of Delaunay triangles.
	 */
	public static List<Triangle> triangulate(List<Point> pointsList){
		// clone the list (to avoid changes on the original)
		List<Point> clone_list = new ArrayList<Point>();
		for(Point p : pointsList){
			clone_list.add(p.clone());
		}
		clone_list = pointsList;

		return BowyerWatson(clone_list);
		//return GuibasStolfi(clone_list);
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
		Point p1 = new Point(-300000, -100000); p1.pivotId = 100001;
		Point p2 = new Point(0, 100000);        p2.pivotId = 100002;
		Point p3 = new Point(100000, -300000);  p3.pivotId = 100003;
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
	
	/**
	 * The Guibas and Stolfi divide-and-conquer algorithm for Delaunay triangulation.
	 * Sort the points list by X coordinate and divide it into halves until we are
	 * left with subsets containing no more than three points. 
	 * Then merges the triangles and segments recursively. 
	 * Return a list of triangle edges.
	 * 
	 * Guibas, Leonidas, and Jorge Stolfi. (1985) "Primitives for the manipulation of 
	 * general subdivisions and the computation of Voronoi Diagrams." 
	 * 
	 * //http://www.geom.uiuc.edu/~samuelp/del_project.html
	 */
	@Deprecated
	private List<TriangleEdge> GuibasStolfi(List<Point> pointsList){
		// sort points by increasing X-coordinates
		Collections.sort(pointsList, POINT_COMPARATOR);
		
		// an initial empty list of edges
		List<TriangleEdge> edges = new ArrayList<TriangleEdge>();
		
		// Delaunay triangulate of the points, 
		// return a list of triangle edges.
		edges = triangulate(pointsList, edges);
		
		return edges;
	}
	
	/**
	 * Receive a sorted list of points, and a list of Edges initially empty,
	 * and recursively build the Delaunay triangulation. Recursive function.
	 * Return a list of triangle edges.
	 */
	private static List<TriangleEdge> triangulate(List<Point> pointList, List<TriangleEdge> edges){
		int size = pointList.size();
		
		// divide into halves until we are left with subsets 
		// containing no more than three points
		if(size > 3){
			int half = (size / 2) + (size % 2);
			
			// LL edges
			List<TriangleEdge> left_half =
					triangulate(pointList.subList(0, half), edges);
			// RR edges
			List<TriangleEdge> right_half =
					triangulate(pointList.subList(half, size), edges);
			
			// merges the two halves.
			// compute LR edges
			List<TriangleEdge> lrEdges = merge(left_half, right_half);
System.out.println("\n LR Edges Add After Merge: ");	
for(TriangleEdge e : lrEdges){
	e.print();
}
System.out.println();

			return lrEdges;
		}
		
		// Stop Condition.
		// This subset is instantly triangulated 
		// as a segment in the case of two points, 
		// or a triangle in the case of three points. 
		
		else{
			List<TriangleEdge> new_edges = new ArrayList<TriangleEdge>();
			for(int i=0; i<size; i++){
				for(int j=i+1; j<size; j++){
					new_edges.add(new TriangleEdge(pointList.get(i), 
									   pointList.get(j)));
				}
			}
			return new_edges;	
		}
	}

	/**
	 * Merge two sub-graphs. Compute LR edges.
	 * Maintain Delaunayhood when computing LR edges.
	 */
	private static List<TriangleEdge> merge(List<TriangleEdge> left_half, List<TriangleEdge> right_half) {
		List<TriangleEdge> lrEdges = new ArrayList<TriangleEdge>();
	System.out.println("\n Merging...");
	System.out.println("Left Edges:");
	for(TriangleEdge e : left_half){
		e.print();
	}
	System.out.println("Right Edges:");
	for(TriangleEdge e : right_half){
		e.print();
	}
	
		// get bottom-most left point
		Point ll_bottom = new Point();
		double min = INF;
		for(TriangleEdge edge : left_half){
			if(edge.p1.y < min){
				min = edge.p1.y;
				ll_bottom = edge.p1;
			}
			if(edge.p2.y < min){
				min = edge.p2.y;
				ll_bottom = edge.p2;
			}
		}

		// get bottom-most right point
		Point rr_bottom = new Point();
		min = INF;
		for(TriangleEdge edge : right_half){
			if(edge.p1.y < min){
				min = edge.p1.y;
				rr_bottom = edge.p1;
			}
			if(edge.p2.y < min){
				min = edge.p2.y;
				rr_bottom = edge.p2;
			}
		}
		
		// Insert the base LR-edge. The base LR-edge is the bottom-most  
		// LR-edge which does not intersect any LL or RR-edges.
		TriangleEdge base_lr = new TriangleEdge(ll_bottom, rr_bottom);
		lrEdges.add(base_lr.clone());
System.out.println("Base LR:");	
base_lr.print();

		// determine the next LR-edges to be added just above the base LR-edge.
		boolean found_right_candidate = true;
		boolean found_left_candidate = true;
		
		while(found_right_candidate || found_left_candidate){
// RIGHT SIDE	
			// select candidate points from the Right subset.
			// the two left-most points not belonging to the base LR
			List<Point> rrList = new ArrayList<Point>();
			for(TriangleEdge edge : right_half){
				if(edge.isVertex(rr_bottom)){
					if(!edge.p1.isSamePosition(rr_bottom)){
						rrList.add(edge.p1);
					}
					if(!edge.p2.isSamePosition(rr_bottom)){
						rrList.add(edge.p2);
					}	
				}
			}	
//TODO	ordenar por angulo		
// base_lr;	
		//	Collections.sort(rrList, POINT_COMPARATOR);
			 
			// The circumcircle defined by the two endpoints of the base LR-edge and 
			// the potential candidate must not contain the next potential candidate 
			// in its interior.
			Point rr_candidate = new Point();
			Point rr_next_candidate = new Point();
			found_right_candidate = true;
			if(rrList.size() == 0){
				found_right_candidate = false;
			}
			for(int i=0; i < rrList.size(); i++){
				if(i == (rrList.size()-1)){
					rr_candidate = rrList.get(i);
					rr_next_candidate = rrList.get(i);
				} else{
					rr_candidate = rrList.get(i);
					rr_next_candidate = rrList.get(i+1);
				}
				
				// check the angle between Right candidate and base-LR
				// if less than 180 
				if(checkAngle(rr_bottom, ll_bottom, rr_candidate)){
					// circumcircle parameters
					Point circ_center = circumcenter(rr_candidate, ll_bottom, rr_bottom);
					double circ_radius = radius(rr_candidate, ll_bottom, rr_bottom);
				
					double dist = rr_next_candidate.dist(circ_center);
	
					// check if the next candidate is inside the circle
					if(dist+0.0000001 < circ_radius){
						// remove the bad edge 
						TriangleEdge bad_edge = new TriangleEdge(rr_candidate , rr_bottom);
						right_half.remove(bad_edge);
						rr_candidate = rr_next_candidate;
		System.out.println("Removed bad edge right: ");
		bad_edge.print();
					} else break;
				} else {
					found_right_candidate = false;
					break; // first criteria does not hold
				}
			}

	// LEFT SIDE
			// select candidate points from the Left subset.
			List<Point> llList = new ArrayList<Point>();
			for(TriangleEdge edge : left_half){
				if(edge.isVertex(ll_bottom)){
					if(!edge.p1.isSamePosition(ll_bottom)){
						llList.add(edge.p1);
					}
					if(!edge.p2.isSamePosition(ll_bottom)){
						llList.add(edge.p2);
					}	
				}
			}
// TODO ordenar por angulo
// 	base_lr;
		//	Collections.sort(llList, POINT_COMPARATOR);
	
			Point ll_candidate = new Point();
			Point ll_next_candidate = new Point();
			found_left_candidate = true;
			if(llList.size() == 0){
				found_left_candidate = false;
			}
			for(int i = llList.size()-1; i >= 0; i--){ // backwards
				if(i == 0){
					ll_candidate = llList.get(0);
					ll_next_candidate = llList.get(0);
				} else{
					ll_candidate = llList.get(i);
					ll_next_candidate = llList.get(i-1);
				}
				
				// check the angle between Left candidate and base-LR
				// if less than 180 
				if(checkAngle(ll_bottom, ll_candidate, rr_bottom)){
					// circumcircle parameters
					Point circ_center = circumcenter(ll_candidate, ll_bottom, rr_bottom);
					double circ_radius = radius(ll_candidate, ll_bottom, rr_bottom);
					
					double dist = ll_next_candidate.dist(circ_center);	
					
					// check if the next candidate is inside the circle
					if(dist+0.0000001 < circ_radius){ // second criteria does not hold
						// remove the bad edge
						TriangleEdge bad_edge = new TriangleEdge(ll_candidate , ll_bottom);
						left_half.remove(bad_edge);
		System.out.println("Removed bad edge left: ");
		bad_edge.print();
					} else break; // both criteria hold
				} else{
					found_left_candidate = false;
					break; // first criteria does not hold
				}
			}
 		
			// If only one candidate is submitted, 
			// it automatically defines the LR-edge to be added.
			if(found_right_candidate && !found_left_candidate){
				rr_bottom = rr_candidate;
				base_lr = new TriangleEdge(ll_bottom, rr_bottom);
				lrEdges.add(base_lr.clone());
	System.out.println("Add Edge: ");
	base_lr.print();			
			} else if(!found_right_candidate && found_left_candidate){
				ll_bottom = ll_candidate;
				base_lr = new TriangleEdge(ll_bottom, rr_bottom);
				lrEdges.add(base_lr.clone());
	System.out.println("Add Edge: ");
	base_lr.print();	
			} 
			// In the case where both candidates are submitted
			else if(found_right_candidate && found_left_candidate){
				// circumcircle parameters
				Point circ_center = circumcenter(ll_candidate, ll_bottom, rr_bottom);
				double circ_radius = radius(ll_candidate, ll_bottom, rr_bottom);
				
				double dist = rr_candidate.dist(circ_center);
				
				// if the right candidate is not contained in interior of the circle
				// then the left candidate defines the LR-edge and vice-versa
				if(dist > circ_radius){
					ll_bottom = ll_candidate;
				} else{
					rr_bottom = rr_candidate;
				}
				base_lr = new TriangleEdge(ll_bottom, rr_bottom);
				lrEdges.add(base_lr.clone());
	System.out.println("Add Edge: ");
	base_lr.print();
			}
System.out.println("New Base LR ");		
base_lr.print();
System.out.println();
		} // end while
		
		return lrEdges;
	}

	/**
	 * True if angle between the vectors made by these 
	 * three points is less than 180 degrees.
	 * Vectors: u = p0---->p1, v = p0---->p2
	 */
	private static boolean checkAngle(Point p0, Point p1, Point p2){ 
		double ux = p1.x - p0.x; 
		double uy = p1.y - p0.y;
		double vx = p2.x - p0.x;
		double vy = p2.y - p0.y;
		
		// determinant
		double det = ux*vy - vx*uy;
		
		// +u and +v
		if(uy > 0 && vy > 0){
			return true;
		}
		// -u and -v
		else if(uy <= 0 && vy <= 0){
			return false;
		}
		// -u and +v or +u and -v
		else if(det < 0){
			return true;
		} else {
			return false; 
		}
	}
	
	/**
	 * Calculates the coordinates of the circumcenter of a triangle.
	 */
	private static Point circumcenter(Point p1, Point p2, Point p3){
		double ax = p1.x; double ay = p1.y;
		double bx = p2.x; double by = p2.y;
		double cx = p3.x; double cy = p3.y;

		double d = 2 * (ax*(by - cy) + bx*(cy - ay) + cx*(ay - by));
		
		double center_x = (ax*ax + ay*ay)*(by - cy) + 
					(bx*bx + by*by)*(cy - ay) + 
					(cx*cx + cy*cy)*(ay - by);
			   center_x = center_x / d;
			   
		double center_y = (ax*ax + ay*ay)*(cx - bx) + 
					(bx*bx + by*by)*(ax - cx) + 
					(cx*cx + cy*cy)*(bx - ax);
			   center_y = center_y / d;
			   
		return new Point(center_x, center_y);
	}
	
	/**
	 * Calculates the radius of the circumcircle of a triangle.
	 */
	private static double radius(Point p1, Point p2, Point p3){
		// triangle sides length
		double a = (p1.x-p2.x)*(p1.x-p2.x) + (p1.y-p2.y)*(p1.y-p2.y);
			   a = Math.sqrt(a); // |p1--p2|
		double b = (p1.x-p3.x)*(p1.x-p3.x) + (p1.y-p3.y)*(p1.y-p3.y);
			   b = Math.sqrt(b); // |p1--p3|
		double c = (p2.x-p3.x)*(p2.x-p3.x) + (p2.y-p3.y)*(p2.y-p3.y);
			   c = Math.sqrt(c); // |p2--p3|
		
		double radius = (a + b + c) * (b + c - a) * 
				        (c + a - b) * (a + b - c);
		radius = Math.sqrt(radius);
		radius = (a * b * c) / radius;
		
		return radius;
	}

	/**
	 * Test
	 */
	public static void main(String []arg0){
		Point p1 = new Point(0, 40, 1);p1.pivotId=1;
		Point p2 = new Point(40, -10, 2);p2.pivotId=2;
		Point p3 = new Point(50, 100, 3);p3.pivotId=3;
		Point p4 = new Point(95, 0, 4);p4.pivotId=4;
		Point p5 = new Point(120, 60, 5);p5.pivotId=5;
		List<Point> pivots = new ArrayList<Point>();
		pivots.add(p1); pivots.add(p2); pivots.add(p3); pivots.add(p4); pivots.add(p5);
		
		List<Triangle> triangulation = triangulate(pivots);
		System.out.println("Num. Triangles: " + triangulation.size());
		for(Triangle tri : triangulation){
			tri.print();
			System.out.println();
		}
	}
}
