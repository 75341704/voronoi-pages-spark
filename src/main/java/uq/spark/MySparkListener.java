package uq.spark;

import java.util.LinkedList;
import java.util.List;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerBlockManagerAdded;
import org.apache.spark.scheduler.SparkListenerBlockManagerRemoved;
import org.apache.spark.scheduler.SparkListenerBlockUpdated;
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate;
import org.apache.spark.scheduler.SparkListenerExecutorAdded;
import org.apache.spark.scheduler.SparkListenerExecutorMetricsUpdate;
import org.apache.spark.scheduler.SparkListenerExecutorRemoved;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.SparkListenerTaskGettingResult;
import org.apache.spark.scheduler.SparkListenerTaskStart;
import org.apache.spark.scheduler.SparkListenerUnpersistRDD;
import org.apache.spark.scheduler.StageInfo;

import uq.fs.HDFSFileService;

/**
 * A Spark Listener to track run time
 * information about the application
 * events.
 * 
 * Save the log to HDFS output folder
 * as 'AppName-log'
 * 
 * @author uqdalves
 *
 */
public class MySparkListener implements SparkListener {
	// HDFS
	private HDFSFileService hdfs = new HDFSFileService();
	// A information script to save after application run time
	private List<String> log = new LinkedList<String>();
	private String appName = "SparkProject";
	private long timeIni;
	private long timeEnd;
	private long stageSum=0;
	private long jobSum=0;
/*	private long[] jobIniVec = new long[50000];
*/	private long jobIni = 0;
	
	public void onApplicationStart(SparkListenerApplicationStart arg0) {
		appName = arg0.appName();
		timeIni = arg0.time();
		log.add("Application " + appName + " starts at: " + timeIni + " ms.\n");
	}
	
	public void onApplicationEnd(SparkListenerApplicationEnd arg0) {
		timeEnd = arg0.time();
		log.add("Application " + appName + " ends at: " + timeEnd + " ms.\n");
		log.add("TOTAL APPLICATION TIME: " + (timeEnd-timeIni) + " ms.");
		log.add("TOTAL STAGES TIME: " + stageSum + " ms.");
		log.add("TOTAL JOBS TIME: " + jobSum + " ms.");
		// save log to HDFS
		hdfs.saveLogFileHDFS(log, appName);
	}
	
	public void onTaskStart(SparkListenerTaskStart arg0) {
		/*TaskInfo info = arg0.taskInfo();
		logInfo += "Task " + info.taskId() + " stage " + arg0.stageId() + " started.\n\n";*/
	}	
	
	public void onTaskEnd(SparkListenerTaskEnd arg0) {
		/*TaskInfo info = arg0.taskInfo();
		//TaskMetrics metrics = arg0.taskMetrics();
		String type = arg0.taskType();
		if(info != null) {
			logInfo += "Task " + type + " " + info.taskId() + " ended.\n";
			logInfo += "Task finish time: " + info.finishTime() + " ms.\n";
			logInfo += "Task duration: " + info.duration() + " ms.\n";
			logInfo += "Task getting result time: " + info.gettingResultTime() +" ms.\n\n";
   	    }*/
	}
	
	public void onJobStart(SparkListenerJobStart arg0) {
		int jobId = arg0.jobId();
		jobIni = arg0.time();
		log.add("Job (" + jobId + ") started at: " + jobIni + " ms.\n");
		//jobIniVec[jobId] = jobIni;
	}
	
	public void onJobEnd(SparkListenerJobEnd arg0) {
		int jobId = arg0.jobId();
		long jobEnd = arg0.time();
		log.add("Job (" + jobId + ") completed at: " + jobEnd + " ms.");	
		log.add("Job (" + jobId + ") total time: " + (jobEnd - jobIni/*jobIniVec[jobId]*/) + " ms.\n");
		jobSum += (jobEnd - jobIni);
	}	
	
	public void onStageSubmitted(SparkListenerStageSubmitted arg0) {
		/*StageInfo info = arg0.stageInfo();
		if(info!=null){
			log.add("Stage ("+info.stageId()+") "  + info.name() + " submitted.");
			//log.add("Stage submission time: " + info.submissionTime() + " ms.\n");
		}*/
	}
	public void onStageCompleted(SparkListenerStageCompleted arg0) {
		StageInfo info = arg0.stageInfo();
		if(info!=null){
			long ini = Long.parseLong(info.submissionTime().get().toString());
			long end = Long.parseLong(info.completionTime().get().toString());
			log.add("Stage ("+info.stageId()+") " + info.name() + " completed.");
			log.add("Number of tasks: " + info.numTasks() + ".");
			log.add("Stage submission time: " + ini + " ms.");
			log.add("Stage completion time: " + end + " ms.");
			log.add("Stage total time: " + (end-ini) + " ms.\n");
			stageSum += (end - ini);
		}
	}
	
	// not used
	public void onTaskGettingResult(SparkListenerTaskGettingResult arg0) {}
	public void onBlockManagerAdded(SparkListenerBlockManagerAdded arg0) {}
	public void onBlockManagerRemoved(SparkListenerBlockManagerRemoved arg0) {}
	public void onBlockUpdated(SparkListenerBlockUpdated arg0) {}
	public void onEnvironmentUpdate(SparkListenerEnvironmentUpdate arg0) {}
	public void onExecutorAdded(SparkListenerExecutorAdded arg0) {}
	public void onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate arg0) {}
	public void onExecutorRemoved(SparkListenerExecutorRemoved arg0) {}
	public void onUnpersistRDD(SparkListenerUnpersistRDD arg0) {}
}
