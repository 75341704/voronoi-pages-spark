package uq.spark.indexing;

/**
 * Parameters interface.
 * Setup application parameters.
 * 
 * @author uqdalves
 *
 */
public interface IndexParamInterface {
	// index parameters
	static final int TIME_WINDOW_SIZE = 1200; // seconds: 600 1200 3600 7200
	static final int K = 500;			      // number of pivots: 500 1000 2000 4000
}