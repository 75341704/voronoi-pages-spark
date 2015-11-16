package uq.spark.indexing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import uq.spark.SparkEnvInterface;
import uq.spatial.Point;
import uq.spatial.Trajectory; 

/**
 * Implements a Voronoi time page.
 * </br></br>
 * A page is a temporal Voronoi polygon with a list of
 * sub-trajectories (i.e. trajectory sample points) during
 * a certain time window.
 * 
 * @author uqdalves
 */
@SuppressWarnings("serial")
public class Page implements Serializable, SparkEnvInterface {
	/**
	 * The list of sub-trajectories inside this page
	 */
	private List<Trajectory> trajectoryList = 
			new LinkedList<Trajectory>();
	
	/**
	 * The list of trajectories/sub-trajectories in this page.
	 * </br></br>
	 * Note that if a trajectory is contained in more than one page, 
	 * this function will return only those sub-trajectories contained
	 * in this page specifically.
	 */
	public List<Trajectory> getTrajectoryList() {
		return trajectoryList;
	}
	
	/**
	 * A list with all trajectory points in this page.
	 */
	public List<Point> getPointsList() {
		List<Point> pointsList = 
				new ArrayList<Point>();
		for(Trajectory t : trajectoryList){
			pointsList.addAll(t.getPointsList());
		}
		return pointsList;
	}
	
	/**
	 * Adds a trajectory/sub-trajectory to this page. 
	 */
	public void addTrajectory(Trajectory trajectory){
		trajectoryList.add(trajectory);
	}
	
	/**
	 * Parallelize the trajectory collection in this page,
	 * so that the distributed trajectory dataset can be 
	 * operated on in parallel.
	 * </br>
	 * Returns a parallel RDD of this page's 
	 * trajectories/sub-trajectories.
	 */
	public JavaRDD<Trajectory> paralellize(){
		JavaRDD<Trajectory> trajecotryRDD = 
				SC.parallelize(trajectoryList);
		return trajecotryRDD;
	}
	
	/**
	 * Given a trajectory T (trajectory Id), return all 
	 * sub-trajectories in this page that belongs to T.
	 */
	public List<Trajectory> getSubTrajectoriesById(String trajectoryId){
		List<Trajectory> list = new LinkedList<Trajectory>();
		for(Trajectory t : trajectoryList){
			if(trajectoryId.equals(t.id)){
				list.add(t);
			}
		}
		return list;
	}
	
	/**
	 * Return the first trajectory/sub-trajectory element 
	 * in this page. 
	 * </br>
	 * Null if the collection is empty.
	 */
	public Trajectory first(){
		if(!trajectoryList.isEmpty()){
			return trajectoryList.get(0);
		}
		return null;
	}
	
	/**
	 * Merge two pages. 
	 * Appends the trajectory/sub-trajectory from the given 
	 * page to the end of this page's trajectory list. 
	 */
	public Page merge(Page page){
		this.trajectoryList.addAll(page.getTrajectoryList());
		return this;
	}
	
	/**
	 * Return the number of trajectories/sub-trajectories
	 * in this page.
	 */
	public int size(){
		return trajectoryList.size();
	}
	
	/**
	 * Return true is this page has no element.
	 */
	public boolean isEmpty(){
		return trajectoryList.isEmpty();
	}
	
	/**
	 * Return the set of trajectories that overlaps with this page.
	 * Return trajectories ID.
	 */
	public HashSet<String> getTrajectoryIdSet(){
		HashSet<String> ids = new HashSet<String>();
		for(Trajectory t : trajectoryList){
			ids.add(t.id);
		}
		return ids;
	}

	@Override
	public String toString() {
		String string = "Page [";
		for(Trajectory t : trajectoryList){
			string += t.id + ":{";
			for(Point p : t.getPointsList()){
				string += p.toString();
			}
			string += "}";
		}
		string += "]";
		
		return string;
	}

}
