package uq.spatial.voronoi;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

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
		 * in Clockwise order.
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
			if(vertexes.size() > 2){ // only if there are more than 2 vertexes
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
