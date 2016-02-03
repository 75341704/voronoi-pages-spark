package uq.spark.index;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import uq.spark.EnvironmentVariables;
import uq.spatial.Box;
import uq.spatial.Circle;
import uq.spatial.Point;
import uq.spatial.Trajectory;
import uq.spatial.voronoi.VoronoiDiagramGenerator;
import uq.spatial.voronoi.VoronoiEdge;
import uq.spatial.voronoi.VoronoiPolygon; 
 
/**
 * Implements a Voronoi Diagram object. A list of Voronoi polygons. 
 * 
 * This is only the abstract representation of the Voronoi diagram, only contains
 * the list of polygons with its pivot and edges, does not contain any data 
 * to be processed in the cluster.
 * 
 * Read-Only shared object to be cache into main memory of each machine.
 * 
 * @author uqdalves
 *
 */ 
@SuppressWarnings("serial")
public final class VoronoiDiagram implements Serializable, EnvironmentVariables {
	
	// the list of polygons in this Voronoi diagram
	private List<VoronoiPolygon> polygonsList = null;
	
	// the list of pivots
	private List<Point> pivotList = null;
	
	/**
	 * Creates an empty Voronoi diagram.
	 */
	public VoronoiDiagram(){}
	
	/**
	 * Creates a new a Voronoi Diagram for the given 
	 * generator pivots. Call Fortune's algorithm.
	 */
	public VoronoiDiagram(List<Point> generatorPivots){
		build(generatorPivots);
	}
	
	/**
	 * Build a Voronoi Diagram for the given 
	 * generator pivots. Call Fortune's algorithm.
	 */
	public void build(List<Point> generatorPivots){
		this.pivotList = generatorPivots;
		// generate Voronoi polygon edges
		// call of Fortune's algorithm
		VoronoiDiagramGenerator gen = 
				new VoronoiDiagramGenerator();
		this.polygonsList = gen.generateVoronoiPolygons(generatorPivots);
	}

	/**
	 * Return the list of pivot points of this diagram.
	 */
	public List<Point> getPivots(){
		return pivotList;
	}
	
	/** 
	 * The number of polygon elements
	 */
	public int size(){
		return polygonsList.size();
	}
	
	/**
	 * Return the list of polygons.
	 */
	public List<VoronoiPolygon> getPolygonList(){
		return polygonsList;
	}
	
	/**
	 * Return the polygon with pivot point Id = pivotId
	 */
	public VoronoiPolygon getPolygonByPivotId(int pivotId){
		for(VoronoiPolygon poly : polygonsList){
			if(poly.pivot.pivotId == pivotId){
				return poly;
			}
		}
		return null;
	}
	
	/**
	 * Retrieve all Voronoi polygons that overlap with the
	 * given box region.
	 * </br>
	 * Return a list of polygon IDs.
	 */
	public List<Integer> getOverlapingPolygons(Box box){
		// retrieve polygons ids
		List<Integer> overlapList = new ArrayList<Integer>();
		// check if the box area intersects the polygon.
		for(VoronoiPolygon vp : polygonsList){
			// check if the polygon's pivot is inside the box.
			if(box.contains(vp.pivot)){ 
				overlapList.add(vp.pivot.pivotId);
				continue;
			}
			// check if any of the polygon vertices are
			// inside the box, or if any edge intersect
			for(VoronoiEdge edge : vp.getEdgeList()){
				// check vertex in the box
				if(box.contains(edge.x1, edge.y1) ||
				   box.contains(edge.x2, edge.y2)){
					overlapList.add(vp.pivot.pivotId);
					break;
				}
				// check edge and box intersection 
				if(box.intersect(edge.x1, edge.y1, edge.x2, edge.y2)){
					overlapList.add(vp.pivot.pivotId);
					break;
				}
			}
		}
		// check in which polygon the box center is inside
		int closest = getClosestPolygon(box.center());
		if(!overlapList.contains(closest)){
			overlapList.add(closest);
		}
		return overlapList;
	}
	
	/**
	 * Retrieve all Voronoi polygons that overlap with 
	 * the given circle region.
	 * </br>
	 * Return a list of polygon IDs.
	 */
	public List<Integer> getOverlapingPolygons(Circle circle){
		// retrieve candidate polygons ids
		List<Integer> overlapList = new ArrayList<Integer>();
		// check if the query area intersects the polygon.
		for(VoronoiPolygon vp : polygonsList){
			// check if the polygon's pivot is inside the circle.
			if(circle.contains(vp.pivot)){ 
				overlapList.add(vp.pivot.pivotId);
				continue;
			}
			// check if any of the polygon vertices are
			// inside the circle, or if any edge intersect
			for(VoronoiEdge edge : vp.getEdgeList()){
				// check contains
				if(circle.contains(edge.x1, edge.y1) ||
				   circle.contains(edge.x2, edge.y2)){
					overlapList.add(vp.pivot.pivotId);
					break;
				}
				// check edge and circle intersection 
				if(circle.intersect(edge.x1, edge.y1, edge.x2, edge.y2)){
					overlapList.add(vp.pivot.pivotId);
					break;
				}
			}
		}
		// check in which polygon the circle center is inside
		int closest = getClosestPolygon(circle.center());
		if(!overlapList.contains(closest)){
			overlapList.add(closest);
		}
		return overlapList;
	}
	
	/**
	 * Retrieve the Voronoi polygons that overlap with the
	 * given point. Point given by X and Y coordinates.
	 * </br>
	 * Note that if the point is a border point then this 
	 * function returns the polygons whose edges contain
	 * the given point.
	 * 
	 * @return Return a list of polygon identifiers.
	 */
	/*public List<Integer> getOverlapingPolygons(double x, double y){
		double minDist = INF;
		double dist;
		List<Integer> idList = new ArrayList<Integer>();
		// check the closest pivot
		for(Point pivot : pivotList){
			dist = pivot.dist(x, y);
			if(dist <= minDist){
				minDist = dist;
				id = pivot.pivotId;
			}
		}
		return idList;
	}*/
	
	/**
	 * Retrieve the closest Voronoi polygons from this 
	 * trajectory sample points.
	 * 
	 * @return A set of polygons ID
	 */
	public HashSet<Integer> getClosestPolygons(Trajectory q) {
		HashSet<Integer> polySet = new HashSet<Integer>();
		double dist, minDist;
		int id=0;
		for(Point p : q.getPointsList()){
			minDist = INF;
			for(Point piv : pivotList){
				dist = p.dist(piv);
				if(dist < minDist){
					minDist = dist;
					id = piv.pivotId;
				}
			}
			polySet.add(id);
		}
		return polySet;
	}

	/**
	 * Retrieve the closest Voronoi polygon from this 
	 * point (i.e. the polygon this point is inside of).
	 * 
	 * @return A polygon ID
	 */
	public Integer getClosestPolygon(Point p) {
		double dist, minDist = INF;
		int id = 0;		
		// check the closest pivot from this point
		for(Point piv : pivotList){
			dist = p.dist(piv);
			if(dist < minDist){
				minDist = dist;
				id = piv.pivotId;
			}
		}
		return id;
	}
	
	/**
	 * Print polygons: System out.
	 */
	public void print(){
		System.out.println();
		System.out.println("Voronoi Polygons:");
		for (VoronoiPolygon poly : polygonsList) {
			poly.print();
			System.out.println();
		}
	}
}
