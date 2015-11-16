package uq.exp;

import java.io.Serializable;

import uq.spatial.Box;

/**
 * A Spatial temporal object for query experiments purposes.
 * Composed by a spatial region and a time interval.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class STObject implements Serializable {
	public long timeIni=0;
	public long timeEnd=0;
	public Box region;
			
	public STObject() {}
	public STObject(double left, double right, double bottom, double top, long timeIni, long timeEnd) {
		this.region = new Box(left, right, bottom, top);
		this.timeIni = timeIni;
		this.timeEnd = timeEnd;
	}

	public String toString(){
		String s = region.left + " " + region.right + " " + 
				region.bottom + " " + region.top + " " + 
				timeIni + " " + timeEnd;
		return s;
	}
}
