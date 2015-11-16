package uq.spark.query;

import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import uq.spark.indexing.PageIndex;
import uq.spatial.Trajectory;

/**
 * Used in the trajectory Selection query.
 * Object to be returned as answer to Selection queries.
 * 
 * Contain a list of sub-trajectories
 * satisfying a query Q, and a list of pages these
 * sub-trajectories overlap with. All sub-trajectories
 * must belong to the same trajectory (parent).
 *  
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class SelectObject implements Serializable {
	// the sub-trajectories that composes this query object
	protected List<Trajectory> subTrajectoryList = 
			new LinkedList<Trajectory>();
	// the set of pages this query object overlaps with
	protected HashSet<PageIndex> pageIndexSet = 
			new HashSet<PageIndex>();
	
	/**
	 * The ID of the trajectory in this Query Object.
	 */
	public String parentId;
	
	/**
	 * Initial selected sub-trajectory and page index.
	 */
	public SelectObject(Trajectory obj, PageIndex pageIndex) {
		this.parentId = obj.id;
		this.subTrajectoryList.add(obj);
		this.pageIndexSet.add(pageIndex);
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
	 * The Set of pages this query object overlaps with.
	 */
	public HashSet<PageIndex> getPageIndexSet(){
		return pageIndexSet;
	}
	
	/**
	 * True if these two query objects have the same parent trajectory.
	 */
	public boolean isSameParent(SelectObject obj){
		return this.parentId.equals(obj.parentId);
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
	protected boolean group(SelectObject obj){
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
	 * Post-processing operation.
	 * </br>
	 * Group/Merge sub-trajectories by time-stamp, 
	 * remove duplicate points.
	 **/
	public void postProcess(){
		// TODO
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
