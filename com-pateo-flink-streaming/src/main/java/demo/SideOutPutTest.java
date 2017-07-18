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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutPutTest {

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);
		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();

		env.getConfig().setGlobalJobParameters(params);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		final long sessionTime = 4L;
		final long latenTime = 4L;

		final boolean fileOutput = params.has("output");

		final List<Tuple3<String, Long, Integer>> input = new ArrayList<>();
		// tuple( key, ts, value)
		input.add(new Tuple3<>("a", 1L, 1));
		input.add(new Tuple3<>("b", 1L, 1));
		input.add(new Tuple3<>("b", 3L, 1));
		input.add(new Tuple3<>("b", 5L, 1));
		input.add(new Tuple3<>("c", 6L, 1));
		// We expect to detect the session "a" earlier than this point (the old
		// functionality can only detect here when the next starts)
		input.add(new Tuple3<>("a", 14L, 1));
		// We expect to detect session "b" and "c" at this point as well
		input.add(new Tuple3<>("b", 10L, 1));
		input.add(new Tuple3<>("c", 14L, 1));

		DataStream<Tuple3<String, Long, Integer>> source = env
				.addSource(new SourceFunction<Tuple3<String, Long, Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void run(
							SourceContext<Tuple3<String, Long, Integer>> ctx)
							throws Exception {
						for (Tuple3<String, Long, Integer> value : input) {
							ctx.collectWithTimestamp(value, value.f1);

							ctx.emitWatermark(new Watermark(value.f1 - 1));

							if (!fileOutput) {
								System.out.println("Collected: " + value);
							}
						}
						ctx.emitWatermark(new Watermark(Long.MAX_VALUE)); // emit
																			// mark
																			// of
																			// receive
																			// all
																			// data
					}

					@Override
					public void cancel() {
					}
				});
		// final OutputTag<String> outputTag = new
		// OutputTag<String>("side-output") {
		// private static final long serialVersionUID = 1L;
		// };

		// final OutputTag<Tuple3<String, Long, Integer>> lateOutputTag = new
		// OutputTag<Tuple3<String, Long, Integer>>(
		// "later-element") {
		// private static final long serialVersionUID = 1L;
		// };
		// We create sessions for each id with max timeout of 3 time units
		SingleOutputStreamOperator<Tuple3<String, Long, Integer>> aggregated = source
				.keyBy(0)
				// .window(EventTimeSessionWindows.withGap(Time.milliseconds(sessionTime)))
				// // session gap
				.timeWindow(Time.milliseconds(1), Time.milliseconds(1))
				.allowedLateness(Time.milliseconds(latenTime))
				// the max
				// // lateness
				// // milliseconds

				.sideOutputLateData(
						new OutputTag<Tuple3<String, Long, Integer>>(
								"later-element") {
							private static final long serialVersionUID = 1L;
						})
				// The method
				// apply(WindowFunction<Tuple3<String,Long,Integer>,R,Tuple,TimeWindow>)
				// in the type
				// WindowedStream<Tuple3<String,Long,Integer>,Tuple,TimeWindow>
				// is not applicable for the arguments
				// (new WindowFunction<String,Long,Integer,TimeWindow>(){})
				.process(new ProcessWindowFunction<Tuple3<String,Long,Integer>, Tuple3<String, Long, Integer>, Tuple, TimeWindow>() {
					 
					private static final long serialVersionUID = 1L;

					@Override
					public void process(
							Tuple key,
							ProcessWindowFunction<Tuple3<String, Long, Integer>, Tuple3<String, Long, Integer>, Tuple, TimeWindow>.Context ctx,
							Iterable<Tuple3<String, Long, Integer>> input,
							Collector<Tuple3<String, Long, Integer>> out)
							throws Exception {
						// Tuple3<String, Long, Integer> next = counts.iterator().next();
						// Integer count = next.getField(2);
						// out.collect(new Tuple3<String, Long, Integer>(key,
						// next.f1,count));
						
					}
				});
		// (new MyReduceFunction(),new MyWindowFunction());
		DataStream<Tuple3<String, Long, Integer>> sideOutput = aggregated.getSideOutput(new OutputTag<Tuple3<String, Long, Integer>>(
								"later-element") {
							private static final long serialVersionUID = 1L;
						});
		
		sideOutput.printToErr();
		
		aggregated.print();
		// .sum(2)
		// .getSideOutput(lateOutputTag);

		if (fileOutput) {
			aggregated.writeAsText(params.get("output"));
		} else {
			System.out
					.println("Printing result to stdout. Use --output to specify output path.");
			aggregated.print();
		}
		// // key的指定方式
		// source.keyBy(new KeySelector<Tuple3<String,Long,Integer>, String> ()
		// {
		// private static final long serialVersionUID = 1L;
		// @Override
		// public String getKey(Tuple3<String, Long, Integer> value) throws
		// Exception { return value.f0; }}
		// ).window( new TumblingAlignedProcessingTimeWindows(5));

		env.execute();

	}

	private static class MyReduceFunction implements
			ReduceFunction<Tuple3<String, Long, Integer>> {

		private static final long serialVersionUID = 3572922166986743977L;

		@Override
		public Tuple3<String, Long, Integer> reduce(
				Tuple3<String, Long, Integer> v1,
				Tuple3<String, Long, Integer> v2) throws Exception {
			Integer v1v = v1.getField(2);
			Integer v2v = v2.getField(2);
			v2.setField(2, v1v + v2v);
			return v2;
		}
	}

	private static class MyWindowFunction
			implements
			WindowFunction<Tuple3<String, Long, Integer>, Tuple3<String, Long, Integer>, String, TimeWindow> {

		private static final long serialVersionUID = 1L;

		public void apply(String key, TimeWindow window,
				Iterable<Tuple3<String, Long, Integer>> counts,
				Collector<Tuple3<String, Long, Integer>> out) {
			Tuple3<String, Long, Integer> next = counts.iterator().next();
			Integer count = next.getField(2);
			out.collect(new Tuple3<String, Long, Integer>(key, next.f1, count)); // window.getEnd(),
		}
	}

	//ProcessWindowFunction<Tuple3<String,Long,Integer>,R,Tuple,TimeWindow>) in the type 
	//WindowedStream<Tuple3<String,Long,Integer>,Tuple,TimeWindow> is not applicable for the arguments 
	//(SideOutPutTest.MyProcessWindowFunction
	private static class MyProcessWindowFunction
			extends
			ProcessWindowFunction<Tuple3<String, Long, Integer>, Tuple3<String, Long, Integer>, String, TimeWindow>
			implements ReduceFunction<Tuple3<String, Long, Integer>> {

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple3<String, Long, Integer> reduce(
				Tuple3<String, Long, Integer> v1,
				Tuple3<String, Long, Integer> v2) throws Exception {
			Integer v1v = v1.getField(2);
			Integer v2v = v2.getField(2);
			v2.setField(2, v1v + v2v);
			return v2;
		}

		@Override
		public void process(
				String key,
				ProcessWindowFunction<Tuple3<String, Long, Integer>, Tuple3<String, Long, Integer>, String, TimeWindow>.Context ctx,
				Iterable<Tuple3<String, Long, Integer>> input,
				Collector<Tuple3<String, Long, Integer>> out) throws Exception {

			// Tuple3<String, Long, Integer> next = counts.iterator().next();
			// Integer count = next.getField(2);
			// out.collect(new Tuple3<String, Long, Integer>(key,
			// next.f1,count));
		}
	}
}

// new ReduceFunction<Tuple3<String, Long, Integer>>() {
//
// private static final long serialVersionUID = 3572922166986743977L;
//
// public Tuple3<String, Long, Integer> reduce(Tuple3<String, Long, Integer> v1,
// Tuple3<String, Long, Integer> v2) throws Exception {
// Integer v1v = v1.getField(2);
// Integer v2v = v2.getField(2);
// v2.setField(2, v1v + v2v);
// return v2;
// }
//
//
// }, new WindowFunction<Tuple3<String,Long,Integer>,
// Tuple3<String,Long,Integer>, Tuple, TimeWindow>() {
//
// private static final long serialVersionUID = 1L;
//
//
// @Override
// public void apply(Tuple key, TimeWindow window,
// Iterable<Tuple3<String, Long, Integer>> input,
// Collector<Tuple3<String, Long, Integer>> out)
// throws Exception {
// Tuple3<String, Long, Integer> next = input.iterator().next();
// Integer count = next.getField(2);
// out.collect(new Tuple3<String, Long, Integer>(key.getField(0),
// next.f1,count)); // window.getEnd(),
// }

// public void apply(String key, TimeWindow window,
// Iterable<Tuple3<String, Long, Integer>> counts,
// Collector<Tuple3<String, Long, Integer>> out) {
// Tuple3<String, Long, Integer> next = counts.iterator().next();
// Integer count = next.getField(2);
// out.collect(new Tuple3<String, Long, Integer>(key, next.f1,count)); //
// window.getEnd(),
// }
