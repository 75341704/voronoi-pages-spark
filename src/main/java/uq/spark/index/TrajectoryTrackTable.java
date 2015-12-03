package uq.spark.index;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.SizeEstimator;

import scala.Tuple2;
import uq.fs.HDFSFileService;
import uq.spark.SparkEnvInterface;
import uq.spatial.Trajectory;

/**
 * Pair RDD to keep track of trajectories across partitions.
 * Pairs: <Trajectory Id, Pages Set>.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class TrajectoryTrackTable implements Serializable, SparkEnvInterface {
	/**
	 * The RDD of this table object hash table: 
	 * (Trajectory ID, Set of {PageIndex})
	 */
	private JavaPairRDD<String, PageIndexSet> trajectoryTrackTableRDD = null;
	
	/**
	 * Build the Trajectory Track table (TTT). 
	 * </br>
	 * Assign each trajectory to a set of page 
	 * indexes (VSI, TPI) it overlaps with. 
	 * </br>
	 * Build a RDD with key-value pairs:
	 * (TrajectoryID, PageIndexSet = {(VSI,TPI)})
	 */
	public void build(
			final JavaPairRDD<PageIndex, Trajectory> trajectoryToPageIndexRDD){	
		// Map trajectories to overlapping pages.
		// Map each pair (PageIndex, Sub-Trajectory) to (TrajectoryID, PageIndexSet)
		mapTrajectoryToPageIndexSet(trajectoryToPageIndexRDD);
		trajectoryTrackTableRDD.setName("TrajectoryTrackTable");
		trajectoryTrackTableRDD.persist(STORAGE_LEVEL_TTT);
	}
	
	/**
	 * Build this track table RDD by loading it from the file system.
	 * </br>
	 * Read a previous saved copy of this RDD.
	 * 
	 * @param The absolute path to the RDD folder (HDFS, Tachyon, Local, etc.).
	 */
	public void load(final String path) {
		JavaRDD<String> inputRDD = SC.textFile(path);
		// map the input file to pages
		trajectoryTrackTableRDD = inputRDD.mapToPair(
				new PairFunction<String, String, PageIndexSet>() {
			public Tuple2<String, PageIndexSet> call(String tuple) throws Exception {
				// read a table tuple as string
				String[] tokens = tuple.split(("\\(|,|\\)"));
				// trajectory id
				String id = tokens[1];
				// read page index set
				PageIndexSet set = new PageIndexSet();
				for(int i=2; i<tokens.length; i+=2){
					int VSI = Integer.parseInt(tokens[i]);
					int TPI = Integer.parseInt(tokens[i+1]);
					set.add(new PageIndex(VSI, TPI));
				}
				return new Tuple2<String, PageIndexSet>(id, set);
			}
		});
	}

	/**
	 * Save this table RDD to a given output folder 
	 * (HDFS, Tachyon, Local, etc).
	 * </br>
	 * Save in "path/index-structure/trajectory-track-table-rdd" folder.
	 */
	public void save(final String path){
		trajectoryTrackTableRDD.saveAsTextFile(path + 
				"/index-structure/trajectory-track-table-rdd");
	}
	
	/**
	 * Persist this table object, set in the specified Storage Level:
	 * MEMORY_AND_DISK, MEMORY_ONLY, DISK_ONLY, etc.
	 */
	public void persist(StorageLevel level){
		trajectoryTrackTableRDD.persist(level);
	}
	
	/**
	 * Remove this RDD from the storage level.
	 * Clean the cache.
	 */
	public void unpersist(){
		trajectoryTrackTableRDD.unpersist();
	}
	
	/**
	 * The number of trajectories (rows) in this
	 * table RDD.
	 */
	public long count(){
		return trajectoryTrackTableRDD.count();
	}
	
	/**
	 * Return all page indexes for a given trajectory.
	 * Filter all page indexes that contain the given trajectory.
	 * 
	 * @return Return a set of partition page Indexes <VSI = PivotID, TPI = TimePage>.
	 */
	public PageIndexSet collectPageIndexListByTrajectoryId(
			final String trajectoryId){
		// Filter tuple with key = trajectoryId
		JavaRDD<PageIndexSet> filteredRDD = trajectoryTrackTableRDD.filter(
				new Function<Tuple2<String,PageIndexSet>, Boolean>() {
			public Boolean call(Tuple2<String, PageIndexSet> tuple) throws Exception {
				return trajectoryId.equals(tuple._1);
			}
		}).values();
		PageIndexSet indexSet = new PageIndexSet();
		if(!filteredRDD.isEmpty()){
			indexSet = filteredRDD.collect().get(0);
		}
		return indexSet; 
	}

	/**
	 * Return all page indexes for a given trajectory set.
	 * </br>
	 * Collect all pages indexes that contain any of the 
	 * trajectories in the set.
	 * 
	 * @return Return a set of Page Indexes (VSI = PivotID, TPI = TimePage).
	 */
	public PageIndexSet collectPageIndexListByTrajectoryId(
			final Collection<String> trajectoryIdSet){
		// Filter tuples
		JavaRDD<PageIndexSet> filteredRDD = 
			trajectoryTrackTableRDD.filter(new Function<Tuple2<String,PageIndexSet>, Boolean>() {
				public Boolean call(Tuple2<String, PageIndexSet> tuple) throws Exception {
					return trajectoryIdSet.contains(tuple._1);
				}
				// collect and merge tuple values
			}).values();
		PageIndexSet indexSet = new PageIndexSet();
		if(filteredRDD.isEmpty()){
			// return empty
			return indexSet;
		} else {
			indexSet = 
				filteredRDD.reduce(new Function2<PageIndexSet, PageIndexSet, PageIndexSet>() {
					public PageIndexSet call(PageIndexSet indexSet1, 
											 PageIndexSet indexSet2) throws Exception {
						indexSet1.addAll(indexSet2);
						return indexSet1;
					}
				});
			return indexSet; 
		}
	}

	/**
	 * Count the number of pages by trajectory ID.
	 * 
	 * @return Return a pair RDD from trajectory IDs 
	 * to number of pages.
	 */
	public JavaPairRDD<String, Integer> countByTrajectoryId(){
		// map each tuple (trajectory) to its number of pages
		JavaPairRDD<String, Integer> countByKeyRDD = 
			trajectoryTrackTableRDD.mapToPair(
					new PairFunction<Tuple2<String,PageIndexSet>, String, Integer>() {
				public Tuple2<String, Integer> call(Tuple2<String, PageIndexSet> tuple) throws Exception {
					return new Tuple2<String, Integer>(tuple._1, tuple._2.size());
				}
			}).reduceByKey(new Function2<Integer, Integer, Integer>() {
				public Integer call(Integer v1, Integer v2) throws Exception {
					return (v1 + v2);
				}
			});
		
		return countByKeyRDD;
	}
	
	/**
	 * Average number of pages per trajectory 
	 * (after the map phase).
	 */
	public double avgPagesPerTrajectory(){
		final double numTrajectories = count();
		// map each tuple (trajectory) to its number of pages
		final double total = 
			trajectoryTrackTableRDD.values().glom().map(new Function<List<PageIndexSet>, Integer>() {
				public Integer call(List<PageIndexSet> setList) throws Exception {
					int count = 0;
					for(HashSet<PageIndex> set : setList){
						count += set.size();
					}
					return count;
				}
			}).reduce(new Function2<Integer, Integer, Integer>() {
				public Integer call(Integer v1, Integer v2) throws Exception {
					return (v1 + v2);
				}
			});
		return (total / numTrajectories);
	}
	
	/**
	 * The number of partitions of this table RDD.
	 */
	public long getNumPartitions(){
		return trajectoryTrackTableRDD.partitions().size();
	}

	/**
	 * Save track table info. 
	 * Save to HDFS output folder as "trajectory-track-table-info"
	 */
	public void saveTableInfo(){
		List<String> infoList = 
				trajectoryTrackTableRDD.map(new Function<Tuple2<String,PageIndexSet>, String>() {
			public String call(Tuple2<String, PageIndexSet> tuple) throws Exception {
				String info = tuple._1 + ": " + tuple._2.size() + " pages.";
				return info;
			}
		}).collect();

		// save to hdfs
		HDFSFileService hdfs = new HDFSFileService();
		hdfs.saveStringListHDFS(infoList, "trajectory-track-table-info");
	}
	
	/**
	 * Print the table: System out.
	 */
	public void print(){
		System.out.println();
		System.out.println("Trajectory Track Table: [(VSI,TPI)]");
		System.out.println();
		
		trajectoryTrackTableRDD.foreach(new VoidFunction<Tuple2<String,PageIndexSet>>() {
			public void call(Tuple2<String, PageIndexSet> tableTuple) throws Exception {
				System.out.print(tableTuple._1 + ": [");
				for(PageIndex index : tableTuple._2){
					System.out.print("(" + index.toString() + ")");
				}
				System.out.println("]\n\n");
			}
		});
	}

	/**
	 * Estimative of this RDD size.
	 */
	public long estimateSize(){
		return SizeEstimator.estimate(trajectoryTrackTableRDD);
	}
	
	/**
	 * A MapRedcuce/Aggregate function to assign each trajectory to its 
	 * overlapping pages.
	 * </br>
	 * Return a RDD of pairs: (TrajectoryID, Set of PagesIndex)
	 */
	private JavaPairRDD<String, PageIndexSet> mapTrajectoryToPageIndexSet(
			final JavaPairRDD<PageIndex, Trajectory> trajectoryToPageIndexRDD){
		
		// map each sub-trajectory to a pair (TrajectoryID, PageIndex)
		JavaPairRDD<String, PageIndex> idToPageIndexSetRDD = 
				trajectoryToPageIndexRDD.mapToPair(
						new PairFunction<Tuple2<PageIndex,Trajectory>, String, PageIndex>() {
			public Tuple2<String, PageIndex> call(Tuple2<PageIndex, Trajectory> tuple) throws Exception {
				return new Tuple2<String, PageIndex>(tuple._2.id, tuple._1);
			}
		});
		
		// an empty index set to start aggregating
		PageIndexSet emptySet = new PageIndexSet();
		// aggregate functions
		Function2<PageIndexSet, PageIndex, PageIndexSet> seqFunc = 
				new Function2<PageIndexSet, PageIndex, PageIndexSet>() {
			public PageIndexSet call(PageIndexSet set, PageIndex index) throws Exception {
				set.add(index);
				return set;
			}
		}; 
		Function2<PageIndexSet, PageIndexSet, PageIndexSet> combFunc = 
				new Function2<PageIndexSet, PageIndexSet, PageIndexSet>() {
			public PageIndexSet call(PageIndexSet set1, PageIndexSet set2) throws Exception {
				set1.addAll(set2);
				return set1;
			}
		};
		// aggregates the index sets by trajectory ID
		trajectoryTrackTableRDD = 
				idToPageIndexSetRDD.aggregateByKey(emptySet, NUM_PARTITIONS_TTT, seqFunc, combFunc);
			
		return trajectoryTrackTableRDD;
	}

}
