/*
  Copyright 2011 James Humphreys. All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are
permitted provided that the following conditions are met:

   1. Redistributions of source code must retain the above copyright notice, this list of
      conditions and the following disclaimer.

   2. Redistributions in binary form must reproduce the above copyright notice, this list
      of conditions and the following disclaimer in the documentation and/or other materials
      provided with the distribution.

THIS SOFTWARE IS PROVIDED BY James Humphreys ``AS IS'' AND ANY EXPRESS OR IMPLIED
WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

The views and conclusions contained in the software and documentation are those of the
authors and should not be interpreted as representing official policies, either expressed
or implied, of James Humphreys.
 */

package uq.spatial.voronoi;

import java.io.Serializable;

import uq.spatial.Point;

/**
 * A Voronoi Edge is composed by its end points X and
 * Y coordinates, and the two polygons (pivots) it
 * belongs to. Each Voronoi Edge is shared by 
 * exactly two polygons.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class VoronoiEdge implements Serializable {
    public double x1, y1, x2, y2;

    public int pivot1;
    public int pivot2;
    
    public VoronoiEdge(){}
	public VoronoiEdge(double x1, double y1, double x2, double y2) {
		this.x1 = x1;
		this.y1 = y1;
		this.x2 = x2;
		this.y2 = y2;
	}

	@Override
	public boolean equals(Object obj) {
		 if (obj instanceof VoronoiEdge) {
			 VoronoiEdge e = (VoronoiEdge) obj;
			 if(this.x1 == e.x1 && this.x2 == e.x2 && 
				this.y1 == e.y1 && this.y2 == e.y2)
				 return true;
			 if(this.x1 == e.x2 && this.x2 == e.x1 && 
				this.y1 == e.y2 && this.y2 == e.y1)
				 return true;
	     }
	     return false;
	}	
	
	/**
	 * True if the given line segment intersects this edge.
	 * Line segment is given by end point coordinates.
	 * If the line segment only touches the edge or its vertexes 
	 * than also return false.
	 */
	public boolean intersect(double x1, double y1, double x2, double y2){
		// vectors r and s
		double rx = x2 - x1;
		double ry = y2 - y1;		
		double sx = this.x2 - this.x1;
		double sy = this.y2 - this.y1;
		
		// cross product r x s
		double cross = (rx*sy) - (ry*sx);
			
		// they are parallel or colinear
		if(cross == 0.0) return false;
	
		double t = (this.x1 - x1)*sy - (this.y1 - y1)*sx;
			   t = t / cross;
		double u = (this.x1 - x1)*ry - (this.y1 - y1)*rx;
			   u = u / cross;

	    if(t > 0.0 && t < 1.0 && 
	       u > 0.0 && u < 1.0){
	    	return true;
	    }
	    return false;
	}
	
	/**
	 * True if this edge touches the given point.
	 */
	public boolean touch(Point p){
		double cross = (x2 - x1)*p.y - (y2 - y1)*p.x;
		return (cross == 0 ? true : false);
	}
	
	/**
	 * True if this edge is parallel to the Y axis.
	 */
	public boolean isVertical(){
		return (x1 == x2);
	}
	
	/**
	 * True if this edge is parallel to the X axis.
	 */
	public boolean isHorizontal(){
		return (y1 == y2);
	}
	
	/**
	 * Print this edge: System out.
	 */
	public void print(){
		System.out.print("Edge: " + "[" + (pivot1+1) + "]" + "[" + (pivot2+1) + "] ");
		System.out.format("(%.3f, %.3f)",x1,y1);
		System.out.print(" <-----> ");
		System.out.format("(%.3f, %.3f)%n",x2,y2);
	}
}
