package uq.spark.index;

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
	static final int K = 2000;			      // number of pivots: 250 500 1000 2000 4000
}