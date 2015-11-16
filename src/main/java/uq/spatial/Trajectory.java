package uq.spatial;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

/**
 * A trajectory entity.
 * 
 * @author uqdalves
 */
@SuppressWarnings("serial")
public class Trajectory implements Serializable, Cloneable {
	// the list of Points that composes the trajectory
	private List<Point> pointsList = 
			new LinkedList<Point>();
	
	/**
	 * This trajectory Identifier.
	 */
	public String id = ""; 
	
	public Trajectory(){}
	public Trajectory(String id){
		this.id = id;
	}

	/**
	 * Comparator to sort trajectory points by time-stamp.
	 */
	private Comparator<Point> timeComparator = new TimeComparator<Point>(); 
	
	/**
	 * Sort this trajectory's sample points by time-stamp
	 * in ascending order.
	 */
	public void sort(){
		Collections.sort(pointsList, timeComparator);
	}
	
	/**
	 * The list of Points of this trajectory.
	 */
	public List<Point> getPointsList() {
		return pointsList;
	}
	
	/**
	 *  Add a Point to this trajectory (end). 
	 */
	public void addPoint(Point point){
		pointsList.add(point);
	}

	/**
	 *  Add a Point List to this trajectory (end). 
	 */
	public void addPointList(List<Point> pointsList){
		this.pointsList.addAll(pointsList);
	}
	
	/** 
	 * Removes the first occurrence of the specified point from 
	 * this trajectory, if it is present. 
	 * If this list does not contain the element, it is unchanged. 
	 * Returns true if this list contained the specified element.
	 */
	public boolean removePoint(Point p){
		return pointsList.remove(p);
	}
	
	/**
	 * Removes the point at the specified position in this trajectory. 
	 * Shifts any subsequent points to the left.
	 */
	public void removePoint(int index){
		assert(index >= 0 && index < this.size()) 
		: "Trajectory index out of bound";
		pointsList.remove(index);
	}
	
	/**
	 * Merges two trajectories.
	 * Appends a trajectory t to the end of this trajectory.
	 */
	public void merge(Trajectory t){
		pointsList.addAll(t.getPointsList());
	}
	
	/**
	 * Return the i-th point of this trajectory.
	 * Trajectory index from 0 (zero) to size - 1.
	 */
	public Point get(int i){
		assert(i >= 0 && i < this.size()) 
		: "Trajectory index out of bound";
		return pointsList.get(i);
	}
	
	/**
	 * Return the size of the trajectory. 
	 * Number of sample points.
	 */
	public int size(){
		return pointsList.size();
	}
	
	/**
	 * True if the trajectory contains no element.
	 */
	public boolean isEmpty(){
		return pointsList.isEmpty();
	}
	
	/**
	 * Return the initial time of this trajectory.
	 * Time stamp of the first sample point.
	 */
	public long timeIni(){
		if(!pointsList.isEmpty()){
			return pointsList.get(0).time;
		}
		return 0;
	}
	
	/**
	 * Return the final time of this trajectory.
	 * Time stamp of the last sample point.
	 */
	public long timeEnd(){
		if(!pointsList.isEmpty()){
			return pointsList.get(pointsList.size()-1).time;
		}
		return 0;
	}

	/**
	 * Return the length of this trajectory.
	 * Sum of the Euclidean distances between every point.
	 */
	public double length(){
		if(!isEmpty()){
			double length=0.0;
			for(int i=0; i<size()-1; i++){
				length += get(i).dist(get(i+1));
			}
			return length;	
		}
		return 0.0;
	}
	
	/**
	 * Return the time duration of this trajectory.
	 * Time taken from the beginning to the end of the
	 * trajectory.
	 */
	public long duration(){
		if(!this.isEmpty()){
			return (this.timeEnd() - this.timeIni());
		}
		return 0;
	}
	
	/**
	 * Return the average speed of this trajectory
	 * on a sphere surface (Earth).
	 */
	public double speed(){
		if(!this.isEmpty() && this.duration()!=0){
			return (this.length() / this.duration());
		}
		return 0.0;
	}
	
	/**
	 * Return the average sample rate of the points in 
	 * this trajectory (average time between every sample
	 * point).
	 */
	public double samplingRate(){
		if(!this.isEmpty()){
			double rate=0.0;
			for(int i=0; i<pointsList.size()-1; i++){
				Point pi = pointsList.get(i);
				Point pj = pointsList.get(i+1);
				rate += pj.time - pi.time;
			}
			return (rate / (this.size()-1));
		}
		return 0.0;
	}
	
	/**
	 * The 'head' of this trajectory: First sample point.
	 */
	public Point head(){
		if(!this.isEmpty()){
			return pointsList.get(0);
		}
		return null;
	}
	
	/**
	 * Return a sub-trajectory of this trajectory, from 
	 * beginIndex inclusive to endIndex exclusive.
	 * </br>
	 * Note: trajectory index starts from 0 (zero).
	 */
	public Trajectory subTrajectory(int beginIndex, int endIndex){
		assert(beginIndex >= 0 && endIndex <= this.size() && 
				beginIndex < endIndex)
		: "Trajectory index out of bound.";
		Trajectory sub = new Trajectory();
		for(int i=beginIndex; i<endIndex; i++){
			Point p = this.get(i).clone();
			sub.addPoint(p);
		}
		return sub;
	}
	
	/**
	 * Return the Minimum Boundary Rectangle (MBR) 
	 * of this trajectory.
	 */
	public Box mbr(){ 
		final double INF = Double.MAX_VALUE;
		double top=0.0, bottom=INF, left=INF, right=0.0;
		
		for(Point p : pointsList){
			if(p.x > right) right = p.x;
			if(p.x < left) left = p.x;
			if(p.y > top) top = p.y;
			if(p.y < bottom) bottom = p.y;
		}

		return new Box(left, right, bottom, top);
	}
	
	/**
	 * True if these trajectories intersect each other.
	 * (Euclidean space only)
	 * If the trajectories only touch edges or vertexes 
	 * than also returns false.
	 */
	public boolean intersect(Trajectory t){
		if(isEmpty() || t.isEmpty()){
			return false;
		}
		double sx, sy, rx, ry;
		double cross, v, u;
		
		for(int i=0; i < pointsList.size()-1; i++){
			Point i1 = pointsList.get(i);
			Point i2 = pointsList.get(i+1);
			// trajectory edge vector 
			sx = i2.x - i1.x;
			sy = i2.y - i1.y;

			for(int j=0; j < t.size()-1; j++){
				Point j1 = t.get(j);
				Point j2 = t.get(j+1);
				// trajectory edge vector
				rx = j2.x - j1.x;
				ry = j2.y - j1.y;

				// cross product r x s
				cross = (rx*sy) - (ry*sx);
					
				// they are parallel or colinear
				if(cross != 0.0){
					v = (i1.x - j1.x)*sy - (i1.y - j1.y)*sx;
					   v = v / cross;
					u = (i1.x - j1.x)*ry - (i1.y - j1.y)*rx;
						   u = u / cross;

				    if(v > 0.0 && v < 1.0 && 
				       u > 0.0 && u < 1.0){
				    	return true;
				    }
				}
			}
		}
		
	    return false;
	}

    /**
     * Makes an identical copy of this element
     */
    @Override
    public Trajectory clone() {
		Trajectory t_clone = new Trajectory();
		for(Point p : pointsList){
			Point new_p = p.clone();
			t_clone.addPoint(new_p);
		}
		return t_clone;
    }
    
	@Override
    public boolean equals(Object ob) {
        if (ob instanceof Trajectory) {
           Trajectory traj = (Trajectory) ob;
           return traj.id.equals(this.id);
        }
        return false;
	}

	@Override
	public int hashCode() {
		return id.hashCode();
	}
	
	@Override
	public String toString() {
		return id;
	}
	
	/**
	 * Print this trajectory: System out.
	 */
	public void print(){
		System.out.println(id + ": {");
		for(Point p : pointsList){
			p.print();
		}
		System.out.println("};");
	}
}
