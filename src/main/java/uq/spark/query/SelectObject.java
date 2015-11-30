package uq.spark.query;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import uq.spatial.Trajectory;

/**
 * Used in the trajectory Selection query.
 * 
 * Contain a list of sub-trajectories
 * satisfying a query Q. All sub-trajectories
 * must belong to the same trajectory (parent).
 *  
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class SelectObject implements Serializable {
	// the sub-trajectories that composes this query object
	private List<Trajectory> subTrajectoryList = 
			new LinkedList<Trajectory>();
	/**
	 * The ID of the trajectory in this Query Object.
	 */
	public String parentId;
	
	/**
	 * Initiates an empty bag.
	 */
	public SelectObject(){}
	/**
	 * Initial selected sub-trajectory.
	 */
	public SelectObject(Trajectory obj) {
		this.parentId = obj.id;
		this.subTrajectoryList.add(obj);
	}

	/** 
	 * Add a sub-trajectory to this object.
	 */
	public void add(Trajectory t){
		subTrajectoryList.add(t);
	}
	
	/**
	 * The list of sub-trajectories that compose
	 * this query object. All sub-trajectories belong 
	 * to the same trajectory (same parent ID).
	 */
	public List<Trajectory> getSubTrajectoryList(){
		return subTrajectoryList;
	}

	/**
	 * True if these two query objects have the same parent trajectory.
	 */
	public boolean isSameParent(SelectObject obj){
		return this.parentId.equals(obj.parentId);
	}
	
	/**
	 * Group these two bag elements.
	 * 
	 * @return Return this merged bag
	 */
	public SelectObject merge(SelectObject bag){
		subTrajectoryList.addAll(bag.getSubTrajectoryList());
		return this;
	}
	
	public List<Trajectory> postProcess(){
		/*for(Trajectory t :){
			
		}*/
		// TODO
		return null;
	}

	/**
	 * Post processing operation.
	 * </br>
	 * Sorts the trajectory points by time stamp
	 * and removes any duplicates form the map phase.
	 * 
	 * @return A post-processed trajectory
	 */
	private Trajectory postProcess(Trajectory t) {
		
		
		t.sort();
		int size = t.size();
		for(int i = 0; i < size-1; i++){
			if(t.get(i).equals(t.get(i+1))){
				t.removePoint(i);
				size--;
				--i;
			}
		}
		return t;
	}

	/**
	 * Post processing operation. Done in parallel.
	 * </br>
	 * Sorts the trajectory points by time stamp
	 * and removes any duplicates from the map phase.
	 * 
	 * @return A post-processed RDD of trajectories
	 */
	private JavaRDD<Trajectory> postProcess(
			JavaRDD<Trajectory> trajectoryRDD){
		// map each trajec to its post-process version
		trajectoryRDD = 
			trajectoryRDD.map(new Function<Trajectory, Trajectory>() {
				public Trajectory call(Trajectory t) throws Exception {
					return postProcess(t);
				}
			});
		return trajectoryRDD;
	}
	/**
	 * Group two query objects (sub-trajectories): 
	 * sub-trajectories must belong to the same
	 * trajectory (parent).
	 * </br>
	 * Group the sub-trajectory and page index lists.
	 * 
	 * @return Return true if the object have been 
	 * grouped (have same parent)
	 */
/*	protected boolean group(SelectObject obj){
		if(this.isSameParent(obj)){
			this.subTrajectoryList.addAll(obj.getSubTrajectoryList());
			this.pageIndexSet.addAll(obj.getPageIndexSet());
			return true;
		} else{
			System.out.println("Query Objects Have Not Same Parents!");
			return false;
		}
	}

	/**
	 * Print this query object: System out.
	 */
	public void print(){
		System.out.println("[" + parentId + "]");
		for(Trajectory t : subTrajectoryList){
			t.print();
		}
	}

}
