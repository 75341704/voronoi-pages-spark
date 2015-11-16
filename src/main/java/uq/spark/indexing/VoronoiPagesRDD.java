package uq.spark.indexing;

import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.SizeEstimator;

import scala.Tuple2;

import uq.fs.HDFSFileService;
import uq.spark.SparkEnvInterface;

/**
 * The Voronoi diagram partition pages RDD itself.
 * </br>
 * This is the main data object RDD. Contains the
 * RDD of pages, and maintains a separate RDD for 
 * trajectory boundary points.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class VoronoiPagesRDD implements Serializable, SparkEnvInterface, IndexParamInterface {
	// the RDD for this Voronoi diagram. Voronoi pages.
	private JavaPairRDD<PageIndex, Page> voronoiPagesRDD = null;

	/**
	 * Build the pages RDD using Map-Reduce functions.
	 * </br>
	 * Reduce trajectories/sub-trajectories by Page Index
	 * to build the Voronoi pages RDD.
	 */
	public void build(JavaPairRDD<PageIndex, Page> pointToPagePairsRDD){
		// build the Voronoi pages RDD itself.
		reducePagesByKey(pointToPagePairsRDD);
		voronoiPagesRDD.setName("VoronoiPagesRDD");
	}
	
	/**
	 * A reduce by key function over the trajectory to Pages, to
	 * group sub-trajectories by Page Index. 
	 * </br>
	 * Receive key-value pairs of: [PageIndex = (VSI,TPI), Page = (sub-t)]  
	 * and returns a RDD of key-vale pair: 
	 * [PageIndex = (VSI,TPI), Page = (sub-t1, sub-t2, ..., sub-tn)].
	 */
	private JavaPairRDD<PageIndex, Page> reducePagesByKey(
			JavaPairRDD<PageIndex, Page> pointToPagePairsRDD){
		
		voronoiPagesRDD =
			// Reduce pairs by key function. Merge Pages.
			pointToPagePairsRDD.reduceByKey(new Function2<Page, Page, Page>() {
				public Page call(Page s1, Page s2) throws Exception {
					return s1.merge(s2);
				}
				// repartition after the reduce (group and sort pages by index)
			}).repartitionAndSortWithinPartitions(partitionerByVoronoioIndex());

		return voronoiPagesRDD;
	}
	
	/**
	 * Physical partitioner of a RDD.
	 * Group partitions with same PageIndex within the RDD.
	 * </br>
	 * Defines how the elements in a key-value pair RDD 
     * are partitioned by key. Maps each key to a partition ID, 
     * from 0 to numPartitions - 1.
	 */
	private Partitioner partitionerByVoronoioIndex(){
		// creates a new partitioner by pages index
		Partitioner partitioner = new Partitioner() {
			
			@Override
			public int numPartitions() {
				return K; // number of voronoi polygons
			}
			
			@Override
			public int getPartition(Object key) {
			   PageIndex index = (PageIndex) key;
			   // key is assumed to go continuously from 0 to elements-1.
			   return (index.VSI - 1);//hashCode();
			}
		};
		
		return partitioner;
	}
	
	/**
	 * Persist this object. Set in the specified Storage Level:
	 * MEMORY_AND_DISK, MEMORY_ONLY, DISK_ONLY, etc.
	 * </br></br>
	 * Note: this RDD might be too big to be allocated in memory only.
	 */
	public void persist(StorageLevel level){
		voronoiPagesRDD.persist(level);
	}
	
	/**
	 * Remove this RDD from the storage level.
	 * Clean the cache.
	 */
	public void unpersist(){
		voronoiPagesRDD.unpersist();
	}
	
	/**
	 * The number of Voronoi Pages in this RDD.
	 */
	public long count(){
		return voronoiPagesRDD.count();
	}
	
	/**
	 * Collect all pages in this RDD.
	 * Call of Spark collect.
	 */
	public List<Page> collectPages(){
		return voronoiPagesRDD.values().collect();
	}

	/**
	 * Filter all pages from this RDD that match the given 
	 * index list. Call Spark Filter on the RDD.
	 * 
	 * @return Returns a RDD with the filtered pages.
	 */
	public JavaPairRDD<PageIndex, Page> filterPagesByIndex(
			final Collection<PageIndex> indexList){
		// filter partitions (pages) by the given indexes
		JavaPairRDD<PageIndex, Page> pagesRDD = voronoiPagesRDD.filter(
				new Function<Tuple2<PageIndex,Page>, Boolean>() {
			public Boolean call(Tuple2<PageIndex, Page> page) throws Exception {
				return indexList.contains(page._1);
			}
		});

		// return partition with the results
		return pagesRDD;//.coalesce(1);
	}
	
	/**
	 * Filter all pages from this RDD that match the given 
	 * index list. Skip pages in skipList. 
	 * Call Spark Filter on the RDD.
	 * 
	 * @return Returns a RDD with the filtered pages.
	 */
	public JavaPairRDD<PageIndex, Page> filterPagesByIndex(
			final Collection<PageIndex> indexList, 
			final Collection<PageIndex> skipList){
		// filter partitions (pages) by the given indexes
		JavaPairRDD<PageIndex, Page> pagesRDD = voronoiPagesRDD.filter(
				new Function<Tuple2<PageIndex,Page>, Boolean>() {
			public Boolean call(Tuple2<PageIndex, Page> page) throws Exception {
				return (indexList.contains(page._1) && !skipList.contains(page._1));
			}
		});

		// return partition with the results
		return pagesRDD;//.coalesce(1);
	}
	
	/**
	 * Filter all pages from this RDD that match the given
	 * Spatial index list (VSIlist), and Time index 
	 * between [TPIini, TPIend] inclusive. 
	 * Call Spark Filter on the RDD.
	 * 
	 * @return Returns a RDD with the filtered pages.
	 */
	public JavaPairRDD<PageIndex, Page> filterPagesByIndex(
			final Collection<Integer> VSIlist, 
			final int TPIini, final int TPIend){
		// filter partitions (pages) by the given indexes
		JavaPairRDD<PageIndex, Page> pagesRDD = voronoiPagesRDD.filter(
				new Function<Tuple2<PageIndex,Page>, Boolean>() {
			public Boolean call(Tuple2<PageIndex, Page> page) throws Exception {
				return (VSIlist.contains(page._1.VSI) && 
						page._1.TPI >= TPIini && 
						page._1.TPI <= TPIend);
			}
		});

		// return partition with the results
		return pagesRDD;//.coalesce(1);
	}
	
	/**
	 * Filter all pages from this  RDD that match the given  
	 * Spatial index (VSI) list. Call Spark Filter on the RDD.
	 * 
	 * @return Returns a RDD with the filtered pages.
	 */
	public JavaPairRDD<PageIndex, Page> filterPagesBySpatialIndex(
			final Collection<Integer> VSIlist){
		// filter partitions (pages) by the given indexes
		JavaPairRDD<PageIndex, Page> pagesRDD = voronoiPagesRDD.filter(
				new Function<Tuple2<PageIndex,Page>, Boolean>() {
			public Boolean call(Tuple2<PageIndex, Page> page) throws Exception {
				return VSIlist.contains(page._1.VSI);
			}
		});

		// return partitions with the result
		return pagesRDD;//.coalesce(1);
	}

	/**
	 * Filter all pages from this RDD that match the given  
	 * Time index list (TPIlist). Call Spark Filter on the RDD.
	 * 
	 * @return Returns a RDD with the filtered pages.
	 */
	public JavaPairRDD<PageIndex, Page> filterPagesByTimeIndex(
			final Collection<Integer> TPIlist){
		// filter partitions (pages) by the given indexes
		JavaPairRDD<PageIndex, Page> pagesRDD = voronoiPagesRDD.filter(
				new Function<Tuple2<PageIndex,Page>, Boolean>() {
			public Boolean call(Tuple2<PageIndex, Page> page) throws Exception {
				return TPIlist.contains(page._1.TPI);
			}
		});

		// return partitions with the result
		return pagesRDD;//.coalesce(1);
	}
	
	/**
	 * Filter all pages from this RDD with Time index between
	 * TPIini and TPIend, that is, retrieve all pages
	 * such that page TPI = [TPIini, TPIend]. 
	 * Call Spark Filter on the RDD.
	 * 
	 * @return Returns a RDD with the filtered pages.
	 */
	public JavaPairRDD<PageIndex, Page> filterPagesByTimeIndex(
			final int TPIini, final int TPIend){
		// filter partitions (pages) by the given indexes
		JavaPairRDD<PageIndex, Page> pagesRDD = voronoiPagesRDD.filter(
				new Function<Tuple2<PageIndex,Page>, Boolean>() {
			public Boolean call(Tuple2<PageIndex, Page> page) throws Exception {
				return (page._1.TPI >= TPIini && page._1.TPI <= TPIend);
			}
		});
		
		// return partitions with the result
		return pagesRDD; //.coalesce(1);
	}
	
	/**
	 * Count the number of trajectory/sub-trajectories by page index.
	 * 
	 * @return Return a pair RDD from page index to number of elements.
	 */
	public JavaPairRDD<PageIndex, Integer> countByPageIndex(){
		JavaPairRDD<PageIndex, Integer> countByKeyRDD = 
			voronoiPagesRDD.mapToPair(new PairFunction<Tuple2<PageIndex,Page>, PageIndex, Integer>() {
				public Tuple2<PageIndex, Integer> call(Tuple2<PageIndex, Page> page) throws Exception {
					return new Tuple2<PageIndex, Integer>(page._1, page._2.size());
				}
			}).reduceByKey(new Function2<Integer, Integer, Integer>() {
				public Integer call(Integer v1, Integer v2) throws Exception {
					return (v1 + v2);
				}
			});

		return countByKeyRDD;
	}
	
	/**
	 * The average number of trajectories/sub-trajectories 
	 * per Voronoi page.
	 */
	public double avgSubTrajectoriesPerPage(){
		double total = getNumSubTrajectories();
		double numPages = count();
		return (total/numPages);
	}

	/**
	 * The average number of trajectory points 
	 * per Voronoi page.
	 */
	public double avgPointsPerPage(){
		double total = getNumPoints();
		double numPages = count();
		return (total/numPages);
	}
	
	/**
	 * The total number of trajectories/sub-trajectories 
	 * in this RDD dataset (after the map phase).
	 */
	public long getNumSubTrajectories(){
		// map each page to a number of trajectories
		final long total =  
			voronoiPagesRDD.map(new Function<Tuple2<PageIndex,Page>, Long>() {
				public Long call(Tuple2<PageIndex, Page> page) throws Exception {
					return (long)page._2.size();
				}
			}).reduce(new Function2<Long, Long, Long>() {
				public Long call(Long v1, Long v2) throws Exception {
					return (v1+v2);
				}
			});
		return total;
	}
	
	/**
	 * The total number of trajectory points
	 * in this RDD dataset.
	 */
	public long getNumPoints(){
		// map each page to a number of points
		final long total =  
			voronoiPagesRDD.map(new Function<Tuple2<PageIndex,Page>, Long>() {
				public Long call(Tuple2<PageIndex, Page> page) throws Exception {
					return (long)page._2.getPointsList().size();
				}
			}).reduce(new Function2<Long, Long, Long>() {
				public Long call(Long v1, Long v2) throws Exception {
					return (v1+v2);
				}
			});
		return total;
	}
	
	/**
	 * The number of partitions in this RDD.
	 */
	public long getNumPartitions(){
		return voronoiPagesRDD.partitions().size();
	}
	

	/**
	 * Print Voronoi pages information: System out.
	 */
	public void print(){
		// Print results
		System.out.println();
		System.out.println("Number of Voronoi Pages: " + voronoiPagesRDD.count());
		System.out.println();
		
		voronoiPagesRDD.foreach(new VoidFunction<Tuple2<PageIndex,Page>>() {
			public void call(Tuple2<PageIndex, Page> page) throws Exception {
				System.out.println("Page: <" + page._1.VSI + ", " + page._1.TPI + ">");
				System.out.println(page._2.getTrajectoryList().size() + " sub-trajectories.");
				System.out.println(page._2.getPointsList().size() + " points.\n");
			}
		});
	}
	
	/**
	 * Save pages info. 
	 * Save to HDFS output folder as "pages-rdd-info"
	 */
	public void savePagesInfo(){
		List<String> info = new LinkedList<String>();
		
		String script = "Number of Voronoi Pages: " + 
				count();
		info.add(script.toString());
		
		script = "Number of RDD Partitions: " + 
				getNumPartitions();
		info.add(script.toString());
		
		script = "Total Number of Sub-Trajectories: " + 
				getNumSubTrajectories();
		info.add(script.toString());
		
		script = "Total Number of Points: " + 
				getNumPoints();
		info.add(script.toString());
		
		script = "Avg. Number of Sub-Trajectories per Page: " + 
				avgSubTrajectoriesPerPage();
		info.add(script.toString());
		
		script = "Avg. Number of Points per Page: " + 
				avgPointsPerPage() + "\n";
		info.add(script.toString());
		
		// map each page to a String info
		List<String> zeroValue = new LinkedList<String>();
		Function2<List<String>, Tuple2<PageIndex, Page>, List<String>> seqOp = 
				new Function2<List<String>, Tuple2<PageIndex,Page>, List<String>>() {
			public List<String> call(List<String> infoList, Tuple2<PageIndex, Page> page) throws Exception {
				String script = "";
				script += page._1.toString() + "\n";
				script += page._2.getTrajectoryList().size() + " sub-trajectories.\n";
				script += page._2.getPointsList().size() + " points.\n";		
				infoList.add(script);
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
		info.addAll(voronoiPagesRDD.aggregate(zeroValue, seqOp, combOp));

		// save to hdfs
		HDFSFileService hdfs = new HDFSFileService();
		hdfs.saveStringListHDFS(info, "pages-rdd-info");
	}
	
	/**
	 * Save pages history.
	 * This is useful to print into a histogram.
	 * Save the pages info into a file, one per line, as:
	 * </br></br>
	 * "PageIndex" "NumberOfSubTrajectories" "NumberOfPoints"  
	 * </br></br>
	 * Save to HDFS output folder as "pages_history"
	 */
	public void savePagesHistory(){
		List<String> history = new LinkedList<String>();
		
		// map each page to a String info
		List<String> zeroValue = new LinkedList<String>();
		Function2<List<String>, Tuple2<PageIndex, Page>, List<String>> seqOp = 
				new Function2<List<String>, Tuple2<PageIndex,Page>, List<String>>() {
			public List<String> call(List<String> histList, Tuple2<PageIndex, Page> page) throws Exception {
				String script = "";
				script += page._1.toString() + " ";
				script += page._2.getTrajectoryList().size() + " ";
				script += page._2.getPointsList().size();	
				histList.add(script);
				return histList;
			}
		};
		Function2<List<String>, List<String>, List<String>> combOp =
				new Function2<List<String>, List<String>, List<String>>() {
			public List<String> call(List<String> list1, List<String> list2) throws Exception {
				list1.addAll(list2);
				return list1;
			}
		};
		history.addAll(voronoiPagesRDD.aggregate(zeroValue, seqOp, combOp));

		// save to hdfs
		HDFSFileService hdfs = new HDFSFileService();
		hdfs.saveStringListHDFS(history, "pages-rdd-history");
	}
	
	/**
	 * Save this RDD to HDFS output folder.
	 * Save as object in "pages-rdd" folder.
	 */
	public void save(){
		voronoiPagesRDD.saveAsObjectFile(HDFS_PATH + HDFS_OUTPUT + "pages-rdd");
	}
	
	/**
	 * Estimative of this Page RDD size.
	 */
	public long estimateSize(){
		return SizeEstimator.estimate(voronoiPagesRDD);
	}

}
