package uq.exp;

import java.io.Serializable;

/**
 * A time window for query experiments purposes.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class TimeWindow implements Serializable{
	public long timeIni;
	public long timeEnd;
	
	public TimeWindow(){}
	public TimeWindow(long timeIni, long timeEnd) {
		this.timeIni = timeIni;
		this.timeEnd = timeEnd;
	}
}
