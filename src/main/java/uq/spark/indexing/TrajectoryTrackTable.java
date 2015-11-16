package uq.spark.indexing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;

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
	private JavaPairRDD<String, HashSet<PageIndex>> trackTableRDD = null;
	
	/**
	 * Build the Trajectory Track table. Assign each trajectory to the partition
	 * pages (VSI, TPI) that it overlaps with. Build a RDD with key/value pairs:
	 * (Trajectory ID, PageIndex Set = {(VSI,TPI)})
	 */
	public void build(JavaPairRDD<PageIndex, Page> trajectoryToPageRDD){	
		// Map trajectories to overlapping pages.
		// Map each pair <PageIndex, Page={sub-trajectory}> to <TrajectoryID, PageIndex Set>
		trackTableRDD = 
				//assignTrajectoryToPageSet(trajectoryToPageRDD);
				mapTrajectoryToPageSet(trajectoryToPageRDD);
		trackTableRDD.setName("TrajectoryTrackTable");
	}
	
	/**
	 * A MapRedcuce/Aggregate function to assign each trajectory to its 
	 * overlapping pages.
	 * </br>
	 * Return a RDD of pairs: (TrajectoryID, Set of PagesIndex)
	 */
	private JavaPairRDD<String, HashSet<PageIndex>> mapTrajectoryToPageSet(
			final JavaPairRDD<PageIndex, Page> trajectoryToPageRDD){
		
		// map each page to a list of <TrajecrotyID, Page>
		JavaPairRDD<String, PageIndex> trajectoryToPagePairRDD =
			trajectoryToPageRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<PageIndex,Page>, String, PageIndex>() {
				public Iterable<Tuple2<String, PageIndex>> call(Tuple2<PageIndex, Page> page) throws Exception {
					List<Tuple2<String, PageIndex>> resultList = 
							new ArrayList<Tuple2<String,PageIndex>>();
					for(Trajectory t : page._2.getTrajectoryList()){
						resultList.add(new Tuple2<String, PageIndex>(t.id, page._1));
					}
					return resultList;
				}
			});
		
		// aggregate zero value
		HashSet<PageIndex> emptySet = new HashSet<PageIndex>();
		// aggregate seq operation
		Function2<HashSet<PageIndex>, PageIndex, HashSet<PageIndex>> seqFunc = 
				new Function2<HashSet<PageIndex>, PageIndex, HashSet<PageIndex>>() {
			public HashSet<PageIndex> call(HashSet<PageIndex> indexSet, PageIndex index) throws Exception {
				indexSet.add(index);
				return indexSet;
			}
		};
		// aggregate combine operation
		Function2<HashSet<PageIndex>, HashSet<PageIndex>, HashSet<PageIndex>> combFunc = 
				new Function2<HashSet<PageIndex>, HashSet<PageIndex>, HashSet<PageIndex>>() {
			public HashSet<PageIndex> call(HashSet<PageIndex> indexSet1, 
										   HashSet<PageIndex> indexSet2) throws Exception {
				indexSet1.addAll(indexSet2);
				return indexSet1;
			}
		};
		
		// aggregates the index sets by trajectory ID
		return trajectoryToPagePairRDD.aggregateByKey(emptySet, seqFunc, combFunc);
	}
	
	/**
	 * Persist this table object, set in the specified Storage Level:
	 * MEMORY_AND_DISK, MEMORY_ONLY, DISK_ONLY, etc.
	 */
	public void persist(StorageLevel level){
		trackTableRDD.persist(level);
	}
	
	/**
	 * Remove this RDD from the storage level.
	 * Clean the cache.
	 */
	public void unpersist(){
		trackTableRDD.unpersist();
	}
	
	/**
	 * The number of trajectories (rows) in this
	 * table RDD.
	 */
	public long count(){
		return trackTableRDD.count();
	}
	
	/**
	 * Return all page indexes for a given trajectory.
	 * Filter all page indexes that contain the given trajectory.
	 * 
	 * @return Return a set of partition page Indexes <VSI = PivotID, TPI = TimePage>.
	 */
	public HashSet<PageIndex> collectPageIndexListByTrajectoryId(final String trajectoryId){
		// Filter tuple with key = trajectoryId
		JavaRDD<HashSet<PageIndex>> filteredRDD = trackTableRDD.filter(
				new Function<Tuple2<String,HashSet<PageIndex>>, Boolean>() {
			public Boolean call(Tuple2<String, HashSet<PageIndex>> tuple) throws Exception {
				return trajectoryId.equals(tuple._1);
			}
		}).values();
		HashSet<PageIndex> indexSet = new HashSet<PageIndex>();
		if(filteredRDD.count() != 0){
			indexSet =  filteredRDD.collect().get(0);
		}
		return indexSet; 
	}

	/**
	 * Return all page indexes for a given trajectory, except those 
	 * pages in skipSet (return the difference set).

	 * @return Return a set of partition page Indexes <VSI = PivotID, TPI = TimePage>.
	 */
	public HashSet<PageIndex> collectPageIndexListByTrajectoryId(
			final String trajectoryId, 
			final Collection<PageIndex> skipSet){
		// Filter tuple with key = trajectoryId
		JavaRDD<HashSet<PageIndex>> filteredRDD = 
			trackTableRDD.filter(
					new Function<Tuple2<String,HashSet<PageIndex>>, Boolean>() {
				public Boolean call(Tuple2<String, HashSet<PageIndex>> tuple) throws Exception {
					return trajectoryId.equals(tuple._1);
				}
			}).values();
		HashSet<PageIndex> indexSet = new HashSet<PageIndex>();
		if(filteredRDD.count() == 0){
			return indexSet;
		}
		else if(filteredRDD.count() == 1){
			return filteredRDD.collect().get(0);
		} else {
			indexSet =
				filteredRDD.reduce(new Function2<HashSet<PageIndex>, HashSet<PageIndex>, HashSet<PageIndex>>() {
					public HashSet<PageIndex> call(HashSet<PageIndex> indexSet1, 
												   HashSet<PageIndex> indexSet2) throws Exception {
						indexSet1.addAll(indexSet2);
						return indexSet1;
					}
				});
			// skip pages in skipSet
			indexSet.removeAll(skipSet);
			return indexSet; 
		}
	}
	/**
	 * Return all page indexes for a given trajectory set.
	 * </br>
	 * Collect all pages indexes that contain any of the trajectories in the set.
	 * 
	 * @return Return a set of Page Indexes <VSI = PivotID, TPI = TimePage>.
	 */
	public HashSet<PageIndex> collectPageIndexListByTrajectoryId(
			final Collection<String> trajectoryIdSet){
		// Filter tuples
		JavaRDD<HashSet<PageIndex>> filteredRDD = 
			trackTableRDD.filter(new Function<Tuple2<String,HashSet<PageIndex>>, Boolean>() {
				public Boolean call(Tuple2<String, HashSet<PageIndex>> tuple) throws Exception {
					return trajectoryIdSet.contains(tuple._1);
				}
				// collect and merge tuple values
			}).values();
		HashSet<PageIndex> indexSet = new HashSet<PageIndex>();
		if(filteredRDD.count() == 0){
			return indexSet;
		}else if(filteredRDD.count() == 1){
			return filteredRDD.collect().get(0);
		}else {
			indexSet =
				filteredRDD.reduce(new Function2<HashSet<PageIndex>, HashSet<PageIndex>, HashSet<PageIndex>>() {
					public HashSet<PageIndex> call(HashSet<PageIndex> indexSet1, 
												   HashSet<PageIndex> indexSet2) throws Exception {
						indexSet1.addAll(indexSet2);
						return indexSet1;
					}
				});
			return indexSet; 
		}
	}
	
	/**
	 * Return all page indexes for a given trajectory set, 
	 * except those pages in skipSet (return the difference
	 * set).
	 * </br>
	 * Collect all pages indexes that contain any of the 
	 * trajectories in the set. Skip pages in skipSet.
	 * 
	 * @return Return a set of Page Indexes <VSI = PivotID, TPI = TimePage>.
	 */
	public HashSet<PageIndex> collectPageIndexListByTrajectoryId(
			final Collection<String> trajectoryIdList,
			final Collection<PageIndex> skipSet){
		// Filter tuples
		JavaRDD<HashSet<PageIndex>> filteredRDD = 
			trackTableRDD.filter(new Function<Tuple2<String,HashSet<PageIndex>>, Boolean>() {
				public Boolean call(Tuple2<String, HashSet<PageIndex>> tuple) throws Exception {
					return trajectoryIdList.contains(tuple._1);
				}
				// collect and merge tuple values
			}).values();
		HashSet<PageIndex> indexSet = new HashSet<PageIndex>();
		if(filteredRDD.count() == 0){
			return indexSet;
		}
		else if(filteredRDD.count() == 1){
			return filteredRDD.collect().get(0);
		} else {
			indexSet =
				filteredRDD.reduce(new Function2<HashSet<PageIndex>, HashSet<PageIndex>, HashSet<PageIndex>>() {
					public HashSet<PageIndex> call(HashSet<PageIndex> indexSet1, 
												   HashSet<PageIndex> indexSet2) throws Exception {
						indexSet1.addAll(indexSet2);
						return indexSet1;
					}
				});
			// skip pages in skipSet
			indexSet.removeAll(skipSet);
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
			trackTableRDD.mapToPair(
					new PairFunction<Tuple2<String,HashSet<PageIndex>>, String, Integer>() {
				public Tuple2<String, Integer> call(Tuple2<String, HashSet<PageIndex>> tuple) throws Exception {
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
		double numTrajectories = count();
		// map each tuple (trajectory) to its number of pages
		double total = 
			trackTableRDD.map(new Function<Tuple2<String,HashSet<PageIndex>>, Long>() {
				public Long call(Tuple2<String, HashSet<PageIndex>> tuple) throws Exception {
					return (long)tuple._2.size();
				}
			}).reduce(new Function2<Long, Long, Long>() {
				public Long call(Long v1, Long v2) throws Exception {
					return (v1 + v2);
				}
			});
		return (total/numTrajectories);
	}
	
	/**
	 * The number of partitions of this table RDD.
	 */
	public long getNumPartitions(){
		return trackTableRDD.partitions().size();
	}
	
	/**
	 * Save track table info. 
	 * Save to HDFS output folder as "trajectory-track-table-info"
	 */
	public void saveTableInfo(){
		List<String> info = new LinkedList<String>();
		String script = "Number of Trajectories (Tuples): " + 
				count();
		info.add(script.toString());
		
		script = "Number of RDD Partitions: " + 
				getNumPartitions();
		info.add(script.toString());
		
		script = "Avg. Number of Pages per Trajectory: " + 
				avgPagesPerTrajectory() + "\n";
		info.add(script.toString());
		
		// map each tuple (TrajectoryID, Pages Count) to a info string 
		List<String> zeroValue = new LinkedList<String>();
		Function2<List<String>, Tuple2<String, Integer>, List<String>> seqOp = 
				new Function2<List<String>, Tuple2<String,Integer>, List<String>>() {
			public List<String> call(List<String> infoList, Tuple2<String, Integer> count) throws Exception {
				infoList.add(count._1 + ": " + count._2 + " pages.");
				return infoList;
			}
		}; 
		Function2<List<String>, List<String>, List<String>> combOp = 
				new Function2<List<String>, List<String>, List<String>>() {
			public List<String> call(List<String> list1, List<String> list2) throws Exception {
				list1.addAll(list2);
				return list1;
			}
		};
		info.addAll(countByTrajectoryId().aggregate(zeroValue, seqOp, combOp));

		// save to hdfs
		HDFSFileService hdfs = new HDFSFileService();
		hdfs.saveStringListHDFS(info, "trajectory-track-table-info");
	}
	
	/**
	 * Save this RDD to HDFS output folder.
	 * Save as object in "trajectory-track-table-rdd" folder.
	 */
	public void save(){
		trackTableRDD.saveAsObjectFile(HDFS_PATH + HDFS_OUTPUT + "trajectory-track-table-rdd");
	}
	
	/**
	 * Print the table: System out.
	 */
	public void print(){
		System.out.println();
		System.out.println("Trajectory Track Table: {(VSI,TPI)}");
		System.out.println();
		
		trackTableRDD.foreach(new VoidFunction<Tuple2<String,HashSet<PageIndex>>>() {
			public void call(Tuple2<String, HashSet<PageIndex>> tableTuple) throws Exception {
				System.out.print(tableTuple._1 + ": {");
				for(PageIndex index : tableTuple._2){
					System.out.print("(" + index.VSI.toString() + "," + index.TPI.toString() + ")");
				}
				System.out.println("}\n\n");
			}
		});
	}
	
	
}
