package master2018.flink;

import java.util.Iterator;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;


public class AccidentReporter implements WindowFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, 
Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple, GlobalWindow> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void apply(Tuple key, GlobalWindow window, Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> input,
			Collector<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> out) throws Exception {
		
		Iterator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> iterator = input.iterator();
		Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> first = iterator.next();
		
		Integer time1 = 0;
		Integer time2 = 0;
		Integer id = 0;
		Integer xway = 0;
		Integer seg = 0;
		Integer dir = 0;
		Integer pos = 0;
		int counter = 0;

		if (first != null) {
			time1 = first.f0;
			time2 = time1;
			id = first.f1;
			xway = first.f3;
			seg = first.f6;
			dir = first.f5;
			pos = first.f7;				
			counter++;			
		}
		while (iterator.hasNext()) {
			Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> next = iterator.next();
			if(next.f7.equals(pos)) {
				time2 = next.f0;
				counter++;
			}			
		}
		if (counter == 4) {
			out.collect(new Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>(time1, time2, id, xway, seg, dir, pos));
		}		
	}
}


