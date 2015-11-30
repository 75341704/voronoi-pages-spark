package uq.spark.index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.WritableComparable;

/**
 * The composite key index of a page: <VSI, TPI>.
 * 
 * @author uqdalves
 *
 */
// http://tutorials.techmytalk.com/2014/11/14/mapreduce-composite-key-operation-part2/
@SuppressWarnings("serial")
public class PageIndex implements WritableComparable<PageIndex>, Serializable {
	/**
	 * Voronoi Spatial Index
	 */
	public Integer VSI;
	/**
	 * Time Page Index
	 */
	public Integer TPI;
	
	public PageIndex(){}
	public PageIndex(Integer VSI, Integer TPI) {
		this.VSI = VSI;
		this.TPI = TPI;
	}
	
	/**
	 * Print this index: System out
	 */
	public void print(){
		System.out.println("<VSI,TPI>:(" + VSI + "," + TPI + ")");
	}

	public void readFields(DataInput in) throws IOException {
		VSI = in.readInt();
		TPI = in.readInt();
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(VSI);
        out.writeInt(TPI);
	}
	
	public int compareTo(PageIndex obj) {		
		return VSI == obj.VSI ? (TPI - obj.TPI) : (VSI - obj.VSI);
	}
		
	@Override
	public int hashCode() {
		final int prime = 83;
		int result = 1;
		result = prime * result + ((TPI == null) ? 0 : TPI.hashCode());
		result = prime * result + ((VSI == null) ? 0 : VSI.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
        if (obj instanceof PageIndex) {
        	PageIndex index = (PageIndex) obj;
            return (index.VSI.equals(VSI) && index.TPI.equals(TPI));
        }
        return false;
	}

	@Override
	public String toString() {
		return (VSI + "," + TPI);
	}
}
