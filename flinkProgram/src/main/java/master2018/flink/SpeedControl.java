package master2018.flink;

import java.util.Iterator;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class SpeedControl implements WindowFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, 
Tuple6<Integer, Integer, Integer, Integer, Integer, Float>, Tuple, TimeWindow> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void apply(Tuple key, TimeWindow window, Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> input,
			Collector<Tuple6<Integer, Integer, Integer, Integer, Integer, Float>> out) throws Exception {
		
		Iterator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> iterator = input.iterator();
		Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> first = iterator.next();
		
		Integer time1 = 0;
		Integer time2 = 0;
		Integer pos1 = 0;
		Integer pos2 = 0;
		Integer id = 0;
		Integer xway = 0;
		Integer dir = 0;
		Float avgSpeed = 0.0f;
		Integer initial_seg = 0;
		Integer end_seg = 0;
		if (first != null) {
			time1 = first.f0;
			time2 = time1;
			id = first.f1;
			pos1 = first.f7;
			pos2 = pos1;
			xway = first.f3;
			dir = first.f5;
			initial_seg = first.f6;
			end_seg = initial_seg;
		}
		while (iterator.hasNext()) {
			Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> next = iterator.next();
			if (next.f0 < time1) {
				time1 = next.f0;
				pos1 = next.f7;
				initial_seg = next.f6;
			}
			if (next.f0 > time2) {
				time2 = next.f0;
				pos2 = next.f7;
				end_seg = next.f6;
			}
			
		}
		avgSpeed = (float) (3600 * Math.abs(pos2-pos1) * 0.000621371192)/(time2-time1);
		
		if (avgSpeed > 60 && Math.abs(end_seg-initial_seg)==4) {
			out.collect(new Tuple6<Integer, Integer, Integer, Integer, Integer, Float>(time1, time2, id, xway, dir, avgSpeed));
		}
		
				
	}
}
