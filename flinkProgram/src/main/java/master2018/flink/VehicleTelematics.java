package master2018.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import master2018.flink.AccidentReporter;
import master2018.flink.SpeedControl;

public class VehicleTelematics {

	public static void main(String[] args) throws Exception {

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		String inFilePath = args[0];
		String outFilePath = args[1];
		
		DataStreamSource<String> source = env.readTextFile(inFilePath).setParallelism(1);
		
		
		SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> mapStream = source
				.map(new MapFunction<String, Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
					
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> map(String in) throws Exception {
						String[] fieldArray = in.split(",");
						Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> out = new Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>(
								Integer.parseInt(fieldArray[0]), 
								Integer.parseInt(fieldArray[1]),
								Integer.parseInt(fieldArray[2]), 
								Integer.parseInt(fieldArray[3]), 
								Integer.parseInt(fieldArray[4]),
								Integer.parseInt(fieldArray[5]),
								Integer.parseInt(fieldArray[6]),
								Integer.parseInt(fieldArray[7]));
						return out;
					}
				});
				
		
		//Speed Radar: detects cars that overcome the speed limit of 90 mph
		SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> speedRadar = 
				mapStream.filter(new FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
					
					private static final long serialVersionUID = 1L;

					@Override
					public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> in) throws Exception {
						if (in.f2 > 90) {
							return true;
						} else {
							return false;
						}
					}
				}).map(new MapFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>() {
					
					private static final long serialVersionUID = 1L;
					
					@Override
					public Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> map(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> in) throws Exception {
						Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> out = new Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>(
								in.f0, 
								in.f1,
								in.f3, 
								in.f6, 
								in.f5,
								in.f2);
						return out;
					}
				});
				
		
		/*Average Speed Control: detects cars with an average speed higher than 60 mph between
		segments 52 and 56 (both included) in both directions. If a car sends several reports on
		segments 52 or 56, the ones taken for the average speed are the ones that cover a longer distance.*/
		SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> segmentFilter = 
				mapStream.filter(new FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
					
					private static final long serialVersionUID = 1L;

					@Override
					public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> in) throws Exception {
						if (in.f6 > 51 && in.f6 < 57) {
							return true;
						} else {
							return false;
						}
					}
				});
		
		KeyedStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple> keyedStream = 
				segmentFilter.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>(){
			
					private static final long serialVersionUID = 1L;

			@Override
			public long extractAscendingTimestamp(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> element) {
				return element.f0*1000;
			}
		}).keyBy(1,5);
				
		
		SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Float>> avgSpeedFines = keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(31))).apply(new SpeedControl());
		
		
		/*Accident Reporter: Detects stopped vehicles on any segment. A vehicle is stopped when it reports 
		  at least 4 consecutive events from the same position.*/				
		SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> zeroSpeedFilter = 
				mapStream.filter(new FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
					
					private static final long serialVersionUID = 1L;

					@Override
					public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> in) throws Exception {
						if (in.f2.equals(0)) {
							return true;
						} else {
							return false;
						}
					}
				}).setParallelism(1);

		KeyedStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>,Tuple> accidentKeyedStream = zeroSpeedFilter.keyBy(1,5);

		SingleOutputStreamOperator<Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>> accidentReport = accidentKeyedStream.countWindow(4,1).apply(new AccidentReporter());

		speedRadar.writeAsCsv(outFilePath + "speedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
		avgSpeedFines.writeAsCsv(outFilePath + "avgspeedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
		accidentReport.writeAsCsv(outFilePath + "accidents.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
		
		try {
			env.execute("master2018.flink");
		}catch(Exception e) {
			e.printStackTrace();
		}

	}
}

