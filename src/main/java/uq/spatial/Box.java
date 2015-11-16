package uq.spatial;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * A Box is defined as a 2D rectangle whose edges  
 * are parallel to the X and Y axis.
 * 
 * @author uqdalves
 */
@SuppressWarnings("serial")
public class Box implements Serializable {
	// X and Y axis position
	public double left;
	public double right;
	public double top;
	public double bottom;

	public Box(){}
	public Box(double left, double right, double bottom, double top) {
		this.top = top;
		this.bottom = bottom;
		this.left = left;
		this.right = right;
	}

	/**
	 * The perimeter of this box
	 */
	public double perimeter(){
		return (top + bottom + left + right);
	}
	
	/**
	 * The area of this box
	 */
	public double area(){
		return (top-bottom)*(right-left);
	}
	
	/**
	 * Returns the center of this box as a coordinate point.
	 */
	public Point center(){
		double xCenter = left + (right - left)/2;
		double yCenter = bottom + (top - bottom)/2; 
		return new Point(xCenter, yCenter);
	}

	/**
	 * True is this box contains the given point inside its perimeter.
	 * Check if the point lies inside the box area.
	 */
	public boolean contains(Point p){
		return contains(p.x, p.y);
	}
	
	/**
	 * True is this box contains the given point inside its perimeter.
	 * Check if the point lies inside the box area.
	 * Point given by X and Y coordinates
	 */
	public boolean contains(double x, double y){
		if(x > left && x < right &&
		   y > bottom && y < top){
			return true;
		}
		return false;
	}
	
	/**
	 * True is this box touches the specified point.
	 * Check if the point touches the box edges.
	 */
	public boolean touch(Point p){
		return touch(p.x, p.y);
	}
	
	/**
	 * True is this box touches the specified point.
	 * Check if the point touches the box edges.
	 * Point given by X and Y coordinates.
	 */
	public boolean touch(double x, double y){
		// check top and bottom edges
		if( x >= left && x <= right && 
		   (y == top || y == bottom) ){
			return true;
		}
		// check left and right edges
		if( y >= bottom && y <= top && 
		   (x == left || x == right) ){
			return true;
		}
		return false;
	}
	
	/**
	 * True is this box overlaps with the given line segment.
	 * Line segment given by end points X and Y coordinates.
	 */
	public boolean overlap(double x1, double y1, double x2, double y2){
		if(contains(x1, y1) || touch(x1, y1)){
			if(contains(x2, y2) || touch(x2, y2)){
				return true;
			}
		}
		return false;
	}
	
	/**
	 * True if the given line segment intersects this Box.
	 * Line segment is given by end point coordinates.
	 * </br></br>
	 * If the line segment do not cross or only touches the
	 *  box edges or vertexes than return false.
	 */
	public boolean intersect(double x1, double y1, double x2, double y2){
		// check box LEFT edge
		if(intersect(x1, y1, x2, y2, 
				left, bottom, left, top)){
			return true;
		}
		// check RIGHT edge
		if(intersect(x1, y1, x2, y2, 
				right, bottom, right, top)){
			return true;
		}
		// check TOP edge
		if(intersect(x1, y1, x2, y2, 
				left, top, right, top)){
			return true;
		}
		// check BOTTOM edge
		if(intersect(x1, y1, x2, y2, 
				left, bottom, right, bottom)){
			return true;
		}
    
	    return false;
	}
	
	/**
	 * Return the coordinates of the four vertexes of this Box.
	 */
	public List<Point> getVertexList(){
		List<Point> corners = new ArrayList<Point>();
		Point p1 = new Point(left, top);
		Point p2 = new Point(right, top);
		Point p3 = new Point(left, bottom);
		Point p4 = new Point(right, bottom);
		corners.add(p1);	corners.add(p2);
		corners.add(p3);	corners.add(p4);
		
		return corners;
	}
	
	/**
	 * Print this Box: System out.
	 */
	public void print(){
		System.out.println("Box:");
		System.out.println("("+left+", "+top+") " + " ("+right+", "+top+")");
		System.out.println("("+left+", "+bottom+") " + " ("+right+", "+bottom+")");
		System.out.println("Area: " + area());
		System.out.println("Perimeter: " + perimeter());
	}
	
	/**
	 * True if these two line segments R and S intersect.
	 * Line segments given by end points X and Y coordinates.
	 */
	private boolean intersect(double r_x1, double r_y1, double r_x2, double r_y2,
							  double s_x1, double s_y1, double s_x2, double s_y2){
		// vectors r and s
		double rx = r_x2 - r_x1;
		double ry = r_y2 - r_y1;		
		double sx = s_x2 - s_x1;
		double sy = s_y2 - s_y1;
		
		// cross product r x s
		double cross = (rx*sy) - (ry*sx);
			
		// they are parallel or colinear
		if(cross == 0.0) return false;
	
		double t = (s_x1 - r_x1)*sy - (s_y1 - r_y1)*sx;
			   t = t / cross;
		double u = (s_x1 - r_x1)*ry - (s_y1 - r_y1)*rx;
			   u = u / cross;

	    if(t > 0.0 && t < 1.0 && 
	       u > 0.0 && u < 1.0){
	    	return true;
	    }
	    return false;
	}
}
