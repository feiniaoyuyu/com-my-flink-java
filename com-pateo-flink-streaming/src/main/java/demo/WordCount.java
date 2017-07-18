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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.examples.java.wordcount.util.WordCountData;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingAlignedProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Implements the "WordCount" program that computes a simple word occurrence
 * histogram over text files in a streaming fashion.
 * 
 * <p>
 * The input is a plain text file with lines separated by newline characters.
 * 
 * <p>
 * Usage: <code>WordCount --input &lt;path&gt; --output &lt;path&gt;</code><br>
 * <code> or --host 10.172.10.167 --port 9999 </code><br>
 * If no parameters are provided, the program is run with default data from
 * {@link WordCountData}.
 * 
 * <p>
 * This example shows how to:
 * <ul>
 * <li>write a simple Flink Streaming program,
 * <li>use tuple data types,
 * <li>write and use user-defined functions.
 * </ul>
 */
public class WordCount {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		
		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();

		// make parameters available in the web interface
		// env.getConfig().setGlobalJobParameters(params);
		DataStream<String> text;
		// get input data
		if (params.has("input")) {
			// read the text file from given input path
			text = env.readTextFile(params.get("input"));

		} else if (params.has("host") && params.has("port")) {
			// read from socket
			text = env.socketTextStream(params.get("host"),
					params.getInt("port"), "\n", 2);
		} else {
			System.out
					.println("Executing WordCount example with default input data set.");
			System.out.println("Use --input to specify file input.");
			// get default test text data
			text = env.fromElements(WordCountData.WORDS);
		}
		// =============================== part 1
		// ===============================
		DataStream<Tuple2<String, Integer>> counts =
		// split up the lines in pairs (2-tuples) containing: (word,1)
		text.flatMap(new Tokenizer())
		// group by the tuple field "0" and sum up tuple field "1"
				.keyBy(0)
				//.timeWindow(Time.seconds(5)) // TumbleWindows
				.sum(1);//
		// emit result
		if (params.has("output")) {
			counts.writeAsText(params.get("output"));
			// counts.writeAsText(params.get("output"),WriteMode.OVERWRITE);
		} else {
			System.out
					.println("Printing result to stdout. Use --output to specify output path.");
			counts.print();
		}
		// =============================== part 2 ===============================
		SingleOutputStreamOperator<Tuple2<String, Integer>> fold = text
		// split
				.flatMap(new Tokenizer())
				// key的指定方式
				.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public String getKey(Tuple2<String, Integer> value)
							throws Exception {
						return value.f0;
					}
				}).window(new TumblingAlignedProcessingTimeWindows(5))
				// .fold(new Tuple2<String, Integer>("", 0), new
				// MyFoldFunction(),
				// new MyWindowFunction());
				.reduce(new MyReduceFunction(), new MyWindowFunction());

		// execute program
		env.execute("Streaming WordCount");

		// DataStream<Long> someIntegers = env.generateSequence(0, 1000);
		// IterativeStream<Long> iteration = someIntegers.iterate();
		// someIntegers. iterate();

	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	/**
	 * Implements the string tokenizer that splits sentences into words as a
	 * user-defined FlatMapFunction. The function takes a line (String) and
	 * splits it into multiple pairs in the form of "(word,1)" (
	 * {@code Tuple2<String,
	 * Integer>}).
	 */
	public static final class Tokenizer implements
			FlatMapFunction<String, Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out)
				throws Exception {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}
	}

	private static class MyFoldFunction implements
			FoldFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {
		private static final long serialVersionUID = 3572922166986743977L;

		@Override
		public Tuple2<String, Integer> fold(
				Tuple2<String, Integer> accumulator,
				Tuple2<String, Integer> value) throws Exception {
			Integer cur = accumulator.getField(2);
			accumulator.setField(1, cur + 1);
			return accumulator;
		}

	}

	private static class MyReduceFunction implements
			ReduceFunction<Tuple2<String, Integer>> {

		private static final long serialVersionUID = 3572922166986743977L;

		@Override
		public Tuple2<String, Integer> reduce(Tuple2<String, Integer> v1,
				Tuple2<String, Integer> v2) throws Exception {
			Integer v1v = v1.getField(1);
			Integer v2v = v2.getField(1);
			v2.setField(1, v1v + v2v);
			return v2;
		}

	}

	private static class MyWindowFunction
			implements
			WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> {

		private static final long serialVersionUID = 1L;

		public void apply(String key, TimeWindow window,
				Iterable<Tuple2<String, Integer>> counts,
				Collector<Tuple2<String, Integer>> out) {
			Integer count = counts.iterator().next().getField(2);
			out.collect(new Tuple2<String, Integer>(key, count)); // window.getEnd(),
		}
	}
}
