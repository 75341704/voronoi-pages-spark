package uq.spark.index;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import uq.spark.SparkEnvInterface;
import uq.spatial.Box;
import uq.spatial.Circle;
import uq.spatial.Point;
import uq.spatial.Trajectory;
import uq.spatial.voronoi.VoronoiDiagramGenerator;
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
public final class VoronoiDiagram implements Serializable, SparkEnvInterface {
	
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
		this.polygonsList = gen.generateVoronoiPolygons(pivotList);
	}

	/**
	 * Return the list of pivot points of this diagram.
	 */
	public List<Point> getPivots(){
		if(pivotList == null){
			pivotList = new ArrayList<Point>();
			for(VoronoiPolygon poly : polygonsList){
				pivotList.add(poly.pivot);
			}
		}
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
	 * Retrieve all Voronoi polygons that overlap with 
	 * the given region. Region specified by a Box perimeter.
	 * </br>
	 * Return a list of polygon IDs.
	 */
	public List<Integer> getOverlapingPolygons(Box region){
		// retrieve candidate polygons IDs = VSIs
		List<Integer> candidates = new ArrayList<Integer>();
		// check if the query area (box) intersects the polygon.
		for(VoronoiPolygon poly : polygonsList){
			if(poly.overlap(region)){
				candidates.add(poly.pivot.pivotId);
			}
		}
		return candidates;
	}

	/**
	 * Retrieve all Voronoi polygons that overlap with 
	 * the given region. Region specified by a Circle perimeter.
	 * </br>
	 * Return a list of polygon IDs.
	 */
	public List<Integer> getOverlapingPolygons(Circle region){
		// retrieve candidate polygons IDs = VSIs
		List<Integer> candidates = new ArrayList<Integer>();
		// check if the query area (box) intersects the polygon.
		for(VoronoiPolygon poly : polygonsList){
			if(poly.overlap(region)){
				candidates.add(poly.pivot.pivotId);
			}
		}
		return candidates;
	}
	
	/**
	 * Retrieve all  Voronoi polygons that overlap with this trajectory.
	 * 
	 * @return A set of polygons ID
	 */
	public HashSet<Integer> getOverlapingPolygons(Trajectory q) {
		HashSet<Integer> polySet = new HashSet<Integer>();
		for(VoronoiPolygon vp : polygonsList){
			for(Point p : q.getPointsList()){
				if(vp.contains(p)){
					polySet.add(vp.pivot.pivotId);
					break;
				}
			}
		}
		return polySet;
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
