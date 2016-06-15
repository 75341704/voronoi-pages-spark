package uq;

import java.util.List;

import uq.spark.query.QueryProcessingModule;
import uq.spatial.STBox;
import uq.spatial.Trajectory;

/**
 * A Thread for a single Selection query execution.
 * 
 * @author uqdalves
 *
 */
// http://www.tutorialspoint.com/java/java_multithreading.htm
public class SelectionQueryThread implements Runnable{
	   private Thread thread = null;
	   private String threadName = null;
	   private STBox queryObject = null;
	   private QueryProcessingModule queryService = null;
	   
	   public SelectionQueryThread(
			   final String threadName, 
			   final STBox queryObject, 
			   final QueryProcessingModule queryService){
	       this.threadName = threadName;
	       this.queryObject = queryObject;
	       this.queryService = queryService;
	   }

	   /**
	    * Run a single job (query) as a new thread.
	    */
	   public void run() {
	      System.out.println("[THREAD] " + threadName + " Running.");
	      try { 
	    	  	// thread starts
	    	    long qStart = System.currentTimeMillis();
				// run query - exact selection trajectories
				List<Trajectory> result = queryService.getSpatialTemporalSelection( 
						queryObject, queryObject.timeIni, queryObject.timeEnd, true);
				// query finish
				int resultSize = result.size();
				long qEnd = System.currentTimeMillis();
				System.out.println("[THREAD] " + threadName + ": " + resultSize + " trajectories in " + (qEnd-qStart) + " ms.");
				System.out.println("[THREAD] " + threadName + " ends at: " + qEnd + " ms.\n");
				// let the thread sleep for a while	
				Thread.sleep(100);
	     } catch (InterruptedException e) {
	         System.out.println("[THREAD] Thread " +  threadName + " interrupted!");
	     }
	   }
	   
	   public void start () {
	      System.out.println("[THREAD] " + threadName + " Starting.");
	      if(thread == null) {
	         thread = new Thread (this, threadName);
	         thread.start ();
	      }
	   }
}
