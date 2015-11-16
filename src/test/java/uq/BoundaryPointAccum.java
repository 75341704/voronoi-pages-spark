package uq;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.AccumulableParam;

import uq.spatial.Point;

/**
 * This point accumulator keeps track of boundary trajectory segments.
 * Accumulate points from boundary segments in a points list during
 * the Map phase.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class BoundaryPointAccum implements AccumulableParam<List<Point>, Point>{

	public List<Point> addAccumulator(List<Point> list, Point p) {
		list.add(p);		
		return list;
	}

	public List<Point> addInPlace(List<Point> list0, List<Point> list1) {
	/*	if(list0.isEmpty()){
			list0.addAll(list1);
			return list0;
		}
		if(list1.isEmpty()){
			list1.addAll(list0);
			return list1;
		}
		List<Point> newList = new ArrayList<Point>();
	//	Point maxNext = new Point(0, 0, 0);
	//	Point maxPrev = new Point(0, 0, 0);
		for(Point next : list1){
			for(Point prev : list0){
				if(next.trajectoryId == prev.trajectoryId){
					if(next.pivotId != prev.pivotId){
						newList.add(prev);
						newList.add(next);
						
					/*	
						maxPrev = prev.time > next.time ? 
								(maxPrev.time > prev.time ? maxPrev : prev) : 
								(maxPrev.time > next.time ? maxPrev : next);
					} else{
						
					}
				}
			}
		}
		list0 = newList;
		return newList;*/
		list0.addAll(list1);
		return list0;
	}

	public List<Point> zero(List<Point> arg0) {
		return new ArrayList<Point>();
	}
}
