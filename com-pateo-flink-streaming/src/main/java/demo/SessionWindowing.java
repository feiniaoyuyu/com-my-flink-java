/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package demo;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;



public class SessionWindowing {

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.getConfig().setGlobalJobParameters(params);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(3);
		
		final long sessionTime = 4L ;
		final long latenTime = 5L ;

		final boolean fileOutput = params.has("output");

		final List<Tuple3<String, Long, Integer>> input = new ArrayList<>();
		//tuple( key, ts, value)
		input.add(new Tuple3<>("a", 1L, 1));
		input.add(new Tuple3<>("b", 1L, 1));
		input.add(new Tuple3<>("b", 3L, 1));
		input.add(new Tuple3<>("b", 5L, 1));
		input.add(new Tuple3<>("c", 6L, 1));
		// We expect to detect session "b" and "c" at this point as well
		input.add(new Tuple3<>("b", 10L, 1));
		// We expect to detect the session "a" earlier than this point (the old
		// functionality can only detect here when the next starts)
		input.add(new Tuple3<>("a", 14L, 1));
		input.add(new Tuple3<>("c", 14L, 1));
		input.add(new Tuple3<>("b", 8L, 1));
		input.add(new Tuple3<>("b", 8L, 1));
//		BoundedOutOfOrdernessTimestampExtractor<String>
//		BoundedOutOfOrdernessGenerator 
//		2> (a,1,1)
//		2> (c,6,1)
//		1> (b,1,3)
//		1> (b,10,1)
//		1> (b,10,2)
//		1> (b,10,3)
//		1> (b,10,3)
//		2> (a,14,1)
//		2> (c,14,1)
		DataStream<Tuple3<String, Long, Integer>> source = env
				.addSource(new SourceFunction<Tuple3<String,Long,Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void run(SourceContext<Tuple3<String, Long, Integer>> ctx) throws Exception {
						for (Tuple3<String, Long, Integer> value : input) {
							ctx.collectWithTimestamp(value, value.f1);
							
							ctx.emitWatermark(new Watermark(value.f1 - 1));
							
							if (!fileOutput) {
								System.out.println("Collected: " + value);
							}
						}
						ctx.emitWatermark(new Watermark(Long.MAX_VALUE)); // emit mark of receive all data
					}

					@Override
					public void cancel() {
					}
				});
 		// We create sessions for each id with max timeout of 3 time units
		DataStream<Tuple3<String, Long, Integer>> aggregated = source
				.keyBy(0)
				.window(EventTimeSessionWindows.withGap(Time.milliseconds(sessionTime ))) // session gap
				.allowedLateness(Time.milliseconds(latenTime)) // the max lateness milliseconds
				
				// The method apply(WindowFunction<Tuple3<String,Long,Integer>,R,Tuple,TimeWindow>) in the type
				//	WindowedStream<Tuple3<String,Long,Integer>,Tuple,TimeWindow> is not applicable for the arguments 
				//	(new WindowFunction<String,Long,Integer,TimeWindow>(){})
				 .sum(2);
//				.getSideOutput(lateOutputTag);

		if (fileOutput) {
			aggregated.writeAsText(params.get("output"));
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			aggregated.print();
		}
//		// key的指定方式
//		source.keyBy(new KeySelector<Tuple3<String,Long,Integer>, String> () {
//			private static final long serialVersionUID = 1L;
//			@Override
//			public String getKey(Tuple3<String, Long, Integer> value) throws Exception {  return value.f0; }}
//		).window( new TumblingAlignedProcessingTimeWindows(5));

		
		env.execute();
		
	}
}
