package uq;

import java.io.Serializable;

/**
 * Implements some geometric functions.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class GeoService implements Serializable{
	
	/**
	 * True if these two line segments R and S intersect.
	 * Line segments given by end points X and Y coordinates.
	 * </br></br>
	 * If the line segments do no intersect or only touches  
	 * then return false.
	 */
	public static boolean intersect(double r_x1, double r_y1, double r_x2, double r_y2,
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
	
	/**
	 * True if the point P touches the line segment E.
	 * Point and line segment given by X and Y coordinates.
	 */
	public boolean touch(double p_x, double p_y, 
					     double e_x1, double e_y1, double e_x2, double e_y2){
		double cross = (e_x2 - e_x1)*p_y - (e_y2 - e_y1)*p_x;
		return (cross == 0 ? true : false);
	}
}
