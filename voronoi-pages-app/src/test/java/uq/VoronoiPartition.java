package uq;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import uq.spark.index.Page;
import uq.spatial.Point;

/**
 * A Voronoi partition, consisting of a Polygon ID (Pivot ID) = VSI, 
 * and Map of Time Pages to point collections within the Voronoi Polygon.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class VoronoiPartition implements Serializable, Comparable<VoronoiPartition> {
	/**
	 * Voronoi Spatial Index. 
	 * The Pivot Id of this Polygon.
	 */
	public Integer VSI;
	
	/**
	 * Pages for Point Collections,
	 * One page for each TPI within this polygon.
	 */
	private HashMap<Integer, Page> pages = 
			new HashMap<Integer, Page>();
	
	/**
	 * Add a page to this polygon. Adds the 
	 * page to the polygon TimePage = TPI. 
	 * Creates the page if it does not exist yet.
	 */
	public void put(Integer TPI, Page page){
		if(pages.containsKey(TPI)){
			pages.get(TPI).merge(page);
		} else{
			pages.put(TPI, page);
		}
	}
	
	/**
	 * True is there is no element in this polygon.
	 */
	public boolean isEmpty(){
		return pages.isEmpty();
	}
	
	/**
	 * Add a Point to this polygon Adds the point  
	 * to the polygon Time Page = TPI. 
	 * Creates the page if it does not exist yet.
	 */
	public void put(Integer TPI, Point p){
	/*	if(pages.containsKey(TPI)){
			pages.get(TPI).addPoint(p);
		} else{
			Page col = new Page();
			col.addPoint(p);
			pages.put(TPI, col);
		}*/
	}
	
	/**
	 * Given a Time Page = TPI, returns the collection of points
	 * inside the given page.
	 */
	public Page getPointsByTimePageIndex(Integer TPI){
		return pages.get(TPI);
	}
	
	/**
	 * Returns a collection of points from all pages 
	 * inside this Voronoi partition.
	 */
	public Page getAllPoints(){
		Page page = new Page(); 
		Iterator<Page> itr = 
				pages.values().iterator();
		while(itr.hasNext()){
			page.merge(itr.next());
		}
		return page;
	}
	
	/**
	 * Returns the Set of time pages inside this Voronoi Polygon.
	 * Returns a iterable Set of time pages indexes = TPIs.
	 */
	public Set<Integer> getPagesIndexes(){
		return pages.keySet();
	}
	
	/**
	 * Print this Voronoi Partition: System out.
	 */
	public void print(){
		Iterator<Integer> keyItr = pages.keySet().iterator();
		Page coll;
		
		System.out.println("Voronoi Partition: " + VSI);
		Integer page;
		while(keyItr.hasNext()){
			page = keyItr.next();
			System.out.println("Page: " + page);
			coll = pages.get(page);
			for(Point p : coll.getPointsList()){
				p.print();
			}
			System.out.println();
		}
	}
	
	/**
	 * Print this Voronoi Partition: System out.
	 */
	public void printInfo(){
		System.out.println("Voronoi Partition: " + VSI);
		System.out.println(pages.size() + " pages.");
		System.out.println(this.getAllPoints().size() + " elements.");
	}
	
	public int compareTo(VoronoiPartition obj) {		
		return VSI.compareTo(obj.VSI);
	}
	
	@Override
    public boolean equals(Object ob) {
        if (ob instanceof VoronoiPartition) {
        	VoronoiPartition vp = (VoronoiPartition) ob;
            return (vp.VSI == VSI);
        }
        return false;
   }
}
