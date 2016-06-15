package uq.spatial.delaunay;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import uq.spatial.Point;

/**
 * A triangle entity.
 * 
 * @author uqdalves
 * 
 */
@SuppressWarnings("serial")
public class Triangle implements Serializable { 
	public Point vertex1;
	public Point vertex2;
	public Point vertex3;
	
	// the list of edges of this triangle
	private List<TriangleEdge> edgeList = null;
	
	// triangle circumcircle parameters
	private Point circumcenter = null;
	private double radius = 0.0;
	
	public Triangle(){};
	public Triangle(Point p1, Point p2, Point p3) {
		this.vertex1 = p1;
		this.vertex2 = p2;
		this.vertex3 = p3;
	}
	
	/**
	 * The edges of this triangles, in the following order:
	 * p1---p2, p1---p3, p2---p3.
	 */
	public List<TriangleEdge> edges(){
		if(edgeList == null){
			edgeList = new LinkedList<TriangleEdge>();
			edgeList.add(new TriangleEdge(vertex1, vertex2)); 
			edgeList.add(new TriangleEdge(vertex1, vertex3)); 
			edgeList.add(new TriangleEdge(vertex2, vertex3));	
		}
		return edgeList;
	}
	
	/**
	 * The coordinates of the circumcenter of this triangle.
	 */
	public Point circumcenter(){
		if(circumcenter == null){
			double ax = vertex1.x; double ay = vertex1.y;
			double bx = vertex2.x; double by = vertex2.y;
			double cx = vertex3.x; double cy = vertex3.y;
	
			double d = 2 * (ax*(by - cy) + bx*(cy - ay) + cx*(ay - by));
			
			double center_x = (ax*ax + ay*ay)*(by - cy) + 
						      (bx*bx + by*by)*(cy - ay) + 
						      (cx*cx + cy*cy)*(ay - by);
				   center_x = center_x / d;
				   
			double center_y = (ax*ax + ay*ay)*(cx - bx) + 
							  (bx*bx + by*by)*(ax - cx) + 
							  (cx*cx + cy*cy)*(bx - ax);
				   center_y = center_y / d;
			
			circumcenter = new Point(center_x, center_y);
		}
		return circumcenter;
	}
	
	/**
	 * This triangle circumcircle's radius.
	 */
	public double radius(){
		if(radius == 0.0){
			// triangle sides length
			double a = (vertex1.x-vertex2.x)*(vertex1.x-vertex2.x) + 
					   (vertex1.y-vertex2.y)*(vertex1.y-vertex2.y);
				   a = Math.sqrt(a); // |p1--p2|
			double b = (vertex1.x-vertex3.x)*(vertex1.x-vertex3.x) + 
					   (vertex1.y-vertex3.y)*(vertex1.y-vertex3.y);
				   b = Math.sqrt(b); // |p1--p3|
			double c = (vertex2.x-vertex3.x)*(vertex2.x-vertex3.x) + 
					   (vertex2.y-vertex3.y)*(vertex2.y-vertex3.y);
				   c = Math.sqrt(c); // |p2--p3|
			
			radius = (a + b + c) * (b + c - a) * 
					        (c + a - b) * (a + b - c);
			radius = Math.sqrt(radius);
			radius = (a * b * c) / radius;	
		}
		return radius;
	}
/*
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Triangle) {
			Triangle tri = (Triangle) obj;
			// check the three possible configurations
			if((tri.p1.isSamePosition(this.p1) || 
					tri.p1.isSamePosition(this.p2) || 
					tri.p1.isSamePosition(this.p3)) &&
			   (tri.p2.isSamePosition(this.p2) ||
					   tri.p2.isSamePosition(this.p1) || 
					   tri.p2.isSamePosition(this.p3)) &&
			   (tri.p3.isSamePosition(this.p3) || 
					   tri.p3.isSamePosition(this.p1) || 
					   tri.p3.isSamePosition(this.p2))
			   ){
				return true;
			}
	     }
	     return false;
	}
*/	
	/**
	 * True if this triangle contains the specified vertex.
	 */
	public boolean contains(Point p){
		if(p.isSamePosition(this.vertex1)) return true;
		if(p.isSamePosition(this.vertex2)) return true;
		if(p.isSamePosition(this.vertex3)) return true;
		return false;
	}
	
	/**
	 * True if this triangle contains the specified edge.
	 */
	public boolean contains(TriangleEdge e){
		for(TriangleEdge edge : edges()){
			if(e.equals(edge))
				return true;
		}
		return false;
	}
	
	/**
	 * True if these two triangles are adjacent, 
	 * share any edge.
	 */
	public boolean isAdjacent(Triangle tri) {
		for(TriangleEdge e1 : this.edges()){
			for(TriangleEdge e2 : tri.edges()){
				if(e1.equals(e2)){
					return true;
				}
			}
		}
		return false;
	}
	
	/**
	 * Print triangle vertexes: System out.
	 */
	public void print(){
		System.out.println(vertex1.pivotId + ": (" + vertex1.x + "," + vertex1.y + ")");
		System.out.println(vertex2.pivotId + ": (" + vertex2.x + "," + vertex2.y + ")");
		System.out.println(vertex3.pivotId + ": (" + vertex3.x + "," + vertex3.y + ")");
	}

}
