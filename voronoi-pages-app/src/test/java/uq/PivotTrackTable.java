package uq;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext; 
import org.apache.spark.broadcast.Broadcast;

import uq.spark.MySparkContext;
import uq.spark.index.Page;

/**
 * Hash Map to keep track of Pivots within a RDD.
 * Shared object to be broadcasted to each machine node.
 * Cache to main memory in each machine.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class PivotTrackTable implements Serializable  {

		// Shared copy of Pivots Hash Table cached on each machine
		private Broadcast<HashMap<Integer, Integer>> pivotsHashTable = null;

		// Spark context for this object
		private static final JavaSparkContext sc = MySparkContext.getInstance();
				
	    /**
	     * Constructor: Build a Hash table to map each pivot to its 
	     * position within the RDD and broadcast it to each node machine. 
	     * Cache to main memory. 
	     * HashMap: <Pivot ID, RDD Index = {0,1,2,...,k-1}>
	     */
		public PivotTrackTable(
				JavaPairRDD<String, Page> voronoiPartitionsRDD) {
			// Hash table to map each pivot to its position within the RDD
			// <Pivot Id, RDD Index>
			HashMap<Integer, Integer> table = 
					new HashMap<Integer, Integer>();

			// collect partition keys
			List<String> partitionKeys = 
					voronoiPartitionsRDD.keys().collect();

			// build the hash
			int value = 0;
			for(String key : partitionKeys){
				table.put(Integer.parseInt(key), value++);
			}
			
			pivotsHashTable = sc.broadcast(table);
		}
		
		/**
		 * Given a pivot Id (partition) return the index
		 * within the RDD for this pivot.
		 */
		public Integer getRDDIndexByPivotId(Integer pivotId){
			return pivotsHashTable.getValue().get(pivotId);
		}
		
		/**
		 * Print the table: System out.
		 */
		public void print(){
			HashMap<Integer, Integer> table = pivotsHashTable.getValue();
			// Print result
			System.out.println();
			System.out.println("Pivots Track Table: <Partition, RDD Index>");
			for(int key=1; key<=table.size(); key++){
				System.out.println("<" + key + ", " + table.get(key) + ">");
			}
		}
			
}
