package uq.spatial.delaunay;

import java.io.Serializable;

import uq.spatial.Point;

/**
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class TriangleEdge implements Serializable, Cloneable {
	public Point p1;
	public Point p2;
		
	public TriangleEdge() {}
	public TriangleEdge(Point p1, Point p2) {
		this.p1 = p1;
		this.p2 = p2;
	}
	public TriangleEdge(double x1, double y1, double x2, double y2) {
		this.p1 = new Point(x1, y1);
		this.p2 = new Point(x2, y2);
	}
	
	/**
	 * True if the given point is a vertex of this edge.
	 */
	public boolean isVertex(Point p){
		return isVertex(p.x, p.y);
	}
	
	/**
	 * True if the given point is a vertex of this edge.
	 * Point given by x,y coordinates.
	 */
	public boolean isVertex(double x, double y){
		return (p1.isSamePosition(x, y) || 
				p2.isSamePosition(x, y));
	}
	
	/**
	 * True if these two edges segment intersects each other.
	 */
	public boolean intersect(TriangleEdge edge){
		return intersect(edge.p1.x, edge.p1.y, edge.p2.x, edge.p2.y);
	}
	
	/**
	 * True if the given line segment intersects this edge.
	 * Line segment given by end point coordinates.
	 */
	public boolean intersect(double x1, double y1, double x2, double y2){
		// vectors r and s
		double rx = x2 - x1;
		double ry = y2 - y1;		
		double sx = p2.x - p1.x;
		double sy = p2.y - p1.y;
		
		// cross product r x s
		double cross = (rx*sy) - (ry*sx);
			
		// they are parallel or colinear
		if(cross == 0.0) return false;
	
		double t = (p1.x - x1)*sy - (p1.y - y1)*sx;
			   t = t / cross;
		double u = (p1.x - x1)*ry - (p1.y - y1)*rx;
			   u = u / cross;

	    if(t > 0 && t < 1 && 
	       u > 0 && u < 1){
	    	return true;
	    }
	    return false;
	}
	
	/**
	 * True if these two edges segment touch each other.
	 */
	public boolean touch(TriangleEdge edge){
		return touch(edge.p1.x, edge.p1.y, edge.p2.x, edge.p2.y);
	}
	
	/**
	 * True if the given line segment touches this edge.
	 * Line segment is given by end point coordinates.
	 */
	public boolean touch(double x1, double y1, double x2, double y2){
		// vectors r and s
		double rx = x2 - x1;
		double ry = y2 - y1;		
		double sx = p2.x - p1.x;
		double sy = p2.y - p1.y;
		
		// cross product r x s
		double cross = (rx*sy) - (ry*sx);
		double t = (p1.x - x1)*sy - (p1.y - y1)*sx;
		double u = (p1.x - x1)*ry - (p1.y - y1)*rx;
		
		// they are colinear
		if(cross == 0.0 && u == 0.0){
			return true;
		}
		// they are parallel
		else if(cross == 0.0){
			return false;
		}
		
		t = t / cross;
		u = u / cross;
				
	    if(t == 0.0 || t == 1.0 || 
	       u == 0.0 || u == 1.0){
	    	return true;
	    }
	    return false;
	}
	
	/**
	 * True if this edge touches the given point.
	 */
	public boolean touch(Point p){
		double cross = (p2.x - p1.x)*p.y - (p2.y - p1.y)*p.x;
		return (cross == 0 ? true : false);
	}
	
	/**
	 * True if this edge touches the given point.
	 * Point given by X and Y coordinates.
	 */
	public boolean touch(double x1, double y1){
		double cross = (p2.x - p1.x)*y1 - (p2.y - p1.y)*x1;
		return (cross == 0 ? true : false);
	}
	
	
	/**
	 * True if this edge is parallel to the given line segment.
	 * Line segment is given by end point coordinates.
	 */
	public boolean isParallel(double x1, double y1, double x2, double y2){
		// vectors r and s
		double rx = x2 - x1;
		double ry = y2 - y1;		
		double sx = p2.x - p1.x;
		double sy = p2.y - p1.y;
		
		// cross product r x s
		double cross = (rx*sy) - (ry*sx);
		
		if(cross == 0.0) return true;
		
		return false;
	}
	
	/**
	 * True if this edge is parallel to the Y axis.
	 */
	public boolean isVertical(){
		return ((p2.x - p1.x) == 0);
	}
	
	/**
	 * True if this edge is parallel to the X axis.
	 */
	public boolean isHorizontal(){
		return ((p2.y - p1.y) == 0);
	}

	@Override
	public boolean equals(Object obj) {
		 if (obj instanceof TriangleEdge) {
			 TriangleEdge e = (TriangleEdge) obj;
			 if(e.p1.isSamePosition(p1) && e.p2.isSamePosition(p2))
				 return true;
			 if(e.p1.isSamePosition(p2) && e.p2.isSamePosition(p1))
				 return true;
	     }
	     return false;
	}
	
	/**
     * Makes an identical copy of this element
     */
    @Override
    public TriangleEdge clone() {    	
		return new TriangleEdge(p1.clone(), p2.clone());
    }	
    
    /**
	 * Print this edge: System out.
	 */
	public void print(){
		System.out.format("(%.3f,%.3f",p1.x,p1.y);
		System.out.print(" ----- ");
		System.out.format("(%.3f,%.3f",p2.x,p2.y);
	}
}
