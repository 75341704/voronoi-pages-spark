package uq.spatial.voronoi;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import uq.spatial.Box;
import uq.spatial.Circle;
import uq.spatial.GeoInterface;
import uq.spatial.Point;
import uq.spatial.PointComparator;

/**
 * Implements a planar Voronoi polygon element. 
 * Each polygon is composed by its pivot, edges
 * and a list of adjacent polygons.
 * 
 * Note: May not be a closed polygon, 
 * i.e. border diagram polygons.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class VoronoiPolygon implements Serializable, GeoInterface {
		/**
		 * The pivot point of this polygon
		 */
		public Point pivot = new Point();
		
		/**
		 * The list of edges of this polygon
		 */
		private List<VoronoiEdge> edges = 
				new LinkedList<VoronoiEdge>();
		
		/**
		 * The list of neighbor polygons. Adjacent polygons IDs.
		 */
		private List<Integer> adjacentList =
				new LinkedList<Integer>();
		
		/**
		 * Constructor. Receives the pivot as parameter.
		 */
		public VoronoiPolygon(Point pivot){
			this.pivot = pivot;
		}
		
		/**
		 * The list of edges of this polygon
		 */
		public List<VoronoiEdge> getEdgeList() {
			return edges;
		}
		
		/**
		 * Add an edge to this polygon.
		 * Add only if the edge is not already in the polygon.
		 */
		public void addEdge(VoronoiEdge edge) {
			if(!edges.contains(edge)){
				edges.add(edge);
			}
		}	
		
		/**
		 * The list of the adjacent (neighbors) polygons IDs.
		 */
		public List<Integer> getAdjacentList(){
			return adjacentList;
		}
		
		/**
		 * Add a adjacent (neighbor) polygon to this polygon.
		 * Adjacent Polygon ID.
		 */
		public void addAdjacent(int adjacentId){
			if(!adjacentList.contains(adjacentId)){
				adjacentList.add(adjacentId);
			}
		}

		/**
		 * The list of vertex of this polygon
		 */
		public List<Point> getVertexList(){
			List<Point> vertexList = new ArrayList<Point>();
			for(VoronoiEdge e : edges){
				Point p1 = new Point(e.x1, e.y1);
				Point p2 = new Point(e.x2, e.y2);
				if(!vertexList.contains(p1)){
					vertexList.add(p1);
				}
				if(!vertexList.contains(p2)){
					vertexList.add(p2);
				}
			}
			return vertexList;
		}
		
		/**
		 * Return the list of vertex of this polygon 
		 * in Clockwise order.
		 */
		public List<Point> getVertexListClockwise(){
			List<Point> vertexList = getVertexList();
			
			// sort in clockwise order
			PointComparator<Point> comparator = 
					new PointComparator<Point>(pivot);
			Collections.sort(vertexList, comparator);
			
			return vertexList;
		}

		/**
		 * Return the list of edges of this polygon 
		 * in Clockwise order (copy of the edges).
		 */
		public List<VoronoiEdge> getEdgeListClockwise(){
			List<VoronoiEdge> edgesClockwise = 
					new ArrayList<VoronoiEdge>();
			List<Point> vertexes = getVertexListClockwise();
			double x1; double y1;
			double x2; double y2;
			if(vertexes.size() == 2){
				x1 = vertexes.get(0).x;
				y1 = vertexes.get(0).y;
				x2 = vertexes.get(1).x;
				y2 = vertexes.get(1).y;
				if((x1-pivot.x) >= 0 && (x2-pivot.x) < 0  && (y2-pivot.y) > 0){
					double cross = (x2-x1)*(pivot.y-y1) - (y2-y1)*(pivot.x-x1);
					if(cross > 0.0){
						VoronoiEdge edge = new VoronoiEdge(x2, y2, x1, y1);
						edgesClockwise.add(edge);
						return edgesClockwise;
					}
				}
			}
			for(int i=0; i<vertexes.size()-1; i++){
				x1 = vertexes.get(i).x;
				y1 = vertexes.get(i).y;
				x2 = vertexes.get(i+1).x;
				y2 = vertexes.get(i+1).y;
				VoronoiEdge edge = new VoronoiEdge(x1, y1, x2, y2);
				if(edges.contains(edge)){
					edgesClockwise.add(edge);
				}
			} 
			if(vertexes.size() > 2){ // only if there is more than 2 vertexes
				// the edge connection the last to the first vertex
				x1 = vertexes.get(vertexes.size()-1).x;
				y1 = vertexes.get(vertexes.size()-1).y;
				x2 = vertexes.get(0).x;
				y2 = vertexes.get(0).y;
				VoronoiEdge edge = new VoronoiEdge(x1, y1, x2, y2);
				if(edges.contains(edge)){
					edgesClockwise.add(edge);
				}
			}
			return edgesClockwise;
		}
		
		/**
		 * True is this polygon contains the given point inside its perimeter.
		 * Check if the point lies inside the polygon area.
		 */
		public boolean contains(Point p) {			
			// because the polygon may not be closed,  we must
			// check the sideness. The point must be in the same 
			// side of all edges.

			// get a list os vertex in clockwise order, for each
			// edge check the cross product signal
			List<VoronoiEdge> edgeList = 
					getEdgeListClockwise();
			for(VoronoiEdge e : edgeList){
				// vectors edge - clockwise
				double v1x = e.x2 - e.x1;
				double v1y = e.y2 - e.y1;
				double v2x = p.x - e.x1;
				double v2y = p.y - e.y1;
				
				double cross = v1x*v2y - v1y*v2x;
				
				// cross product must be negative always
				if(cross >= 0.0 ){
					return false;
				}
			}
			return true;
		}
		
		/**
		 * True is this Voronoi Polygon overlaps with the given Circle.
		 */
		public boolean overlap(Circle circle){
			// check if the circle center is inside the polygon.
			if(contains(circle.center())){ 
				return true;
			}
			// check if any of the polygon's vertices are inside the circle
			for(Point p : this.getVertexList()){
				if(circle.contains(p)){ 
					return true;
				}
			}
			// check for edge intersections
			// check if any of the polygon's edges intersect the circle
			for(VoronoiEdge e : getEdgeList()){
				if(circle.intersect(e.x1, e.y1, e.x2, e.y2)){
					return true;
				}
			}	
			return false;
		}	
		
		/**
		 * True is this Voronoi polygon overlaps with the given Box.
		 */
		public boolean overlap(Box box){
			double x1 = box.minX;
			double y1 = box.minY;
			double x2 = box.maxX;
			double y2 = box.maxY;

			// check if the polygon's pivot is inside the box.
			if(box.contains(this.pivot)){ 
				return true;
			}
			// check if the any of the box vertexes are inside the polygon
			for(Point p : box.getVertexList()){
				if(this.contains(p)){ 
					return true;
				}
			}
			// check for edge intersections
			// check if any of the polygon's edges intersect the box
			for(VoronoiEdge edge : edges){
				// left edge
				if(edge.intersect(x1, y1, x1, y2)){
					return true;
				}
				// right edge
				else if(edge.intersect(x2, y1, x2, y2)){ 
					return true;
				}
				// bottom edge
				else if(edge.intersect(x1, y1, x2, y1)){
					return true;
				}
				// top edge
				else if(edge.intersect(x1, y2, x2, y2)){
					return true;
				}
			}	
			return false;
		}
		
		/**
		 * Print this polygon: System out.
		 */
		public void print(){
			System.out.println("Polygon: " + pivot.pivotId);
			System.out.println("Pivot Coord: (" + pivot.x + "," + pivot.y + ")");
			System.out.print("Neighbors: ");
			// print the adjacent polygons
			for(Integer vp : adjacentList){
				System.out.print("[" + vp + "]");
			}
			System.out.println();
			// print the edges
			for(VoronoiEdge e : edges){
				e.print();
			}
		}

		@Override
		public int hashCode() {
			return pivot.pivotId;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof VoronoiPolygon) {
				VoronoiPolygon vp = (VoronoiPolygon) obj;
	            return (this.pivot.pivotId == vp.pivot.pivotId);
	        }
	        return false;
		}
}
