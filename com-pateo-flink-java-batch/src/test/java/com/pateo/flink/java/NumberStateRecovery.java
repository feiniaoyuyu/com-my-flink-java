/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pateo.flink.java;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.LongCounter;
//import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
//import com.dataartisans.flinktraining.exercises.datastream_java.sources.CheckpointedTaxiRideSource;
//import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
//import com.dataartisans.flinktraining.exercises.datastream_java.utils.TravelTimePredictionModel;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Java reference implementation for the "Travel Time Prediction" exercise of
 * the Flink training (http://dataartisans.github.io/flink-training).
 *
 * The task of the exercise is to continuously train a regression model that
 * predicts the travel time of a taxi based on the information of taxi ride end
 * events. For taxi ride start events, the model should be queried to estimate
 * its travel time.
 *
 * Parameters: -input path-to-input-file
 *
 */
public class NumberStateRecovery {

	public static void main(String[] args) throws Exception {

		// ParameterTool params = ParameterTool.fromArgs(args);
		// final String input = params.getRequired("input");

		// final int servingSpeedFactor = 600; // events of 10 minutes are
		// served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// operate in Event-time
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// try to restart 60 times with 10 seconds delay (10 Minutes)
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(60, Time.of(10, TimeUnit.SECONDS)));
		// int restartAttempts
		// Time delayInterval

		/////////////////////////////////////////
		// create a checkpoint every 1 seconds start a checkpoint every 1000 ms
		env.enableCheckpointing(1000);
		env.setParallelism(1);
		// advanced options:

		// set mode to exactly-once (this is the default)
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

		// checkpoints have to complete within one minute, or are discarded
		env.getCheckpointConfig().setCheckpointTimeout(60000);

		// allow only one checkpoint to be in progress at the same time
		env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
		// start the data generator

		ArrayList<Integer> arrayList = new ArrayList<>();
		for (int i = 0; i < 10000; i++) {
			arrayList.add(i);
		}

		DataStreamSource<Integer> predictions = env.fromCollection(arrayList);

		predictions.map(new RichMapFunction<Integer, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void open(Configuration parameters) throws Exception {
				RuntimeContext runtimeContext = getRuntimeContext();
				runtimeContext.addAccumulator("myconcat", new LongCounter(0));
				super.open(parameters);
			}

			@Override
			public String map(Integer value) throws Exception {
				RuntimeContext runtimeContext = getRuntimeContext();

				Accumulator<Long, Serializable> accumulator = runtimeContext.getAccumulator("myconcat");

				accumulator.add(Long.valueOf(value));
				Thread.currentThread().sleep(1000);
				System.out.println("--value:" + value);
				return accumulator.getLocalValue() + "";
			}
		});
		// DataStream<TaxiRide> rides = env.addSource( new
		// CheckpointedNumberSource(input, servingSpeedFactor));
		// filter out rides that do not start or stop in NYC
		// .filter(new NYCFilter())
		// map taxi ride events to the grid cell of the destination
		// .map(new GridCellMatcher())
		// organize stream by destination
		// .keyBy(0)
		// predict and refine model per destination
		// .map(new RichMapFunction<TaxiRide, String>() {
		// private static final long serialVersionUID = 1L;
		// @Override
		// public String map(TaxiRide value) throws Exception {
		// RuntimeContext runtimeContext = getRuntimeContext();
		// runtimeContext.addAccumulator("myconcat", new LongCounter(0));
		// Accumulator<Long, Serializable> accumulator =
		// runtimeContext.getAccumulator("myconcat");
		// accumulator.add(value.rideId);
		// return accumulator.getLocalValue() +"";
		// }
		// });

		// print the predictions
		predictions.print();

		// run the prediction pipeline
		env.execute("Taxi Ride Prediction");
	}

	public static class NYCFilter implements FilterFunction<TaxiRide> {

		private static final long serialVersionUID = 1L;

		@Override
		public boolean filter(TaxiRide taxiRide) throws Exception {
			return taxiRide.rideId % 2 == 0;
		}
	}

	/**
	 * Maps the taxi ride event to the grid cell of the destination location.
	 */
	public static class GridCellMatcher implements MapFunction<TaxiRide, Tuple2<Integer, TaxiRide>> {

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Integer, TaxiRide> map(TaxiRide ride) throws Exception {

			return new Tuple2<>(1, ride);
		}
	}

	/**
	 * Predicts the travel time for taxi ride start events based on distance and
	 * direction. Incrementally trains a regression model using taxi ride end
	 * events.
	 */

	public static class MyCheckpointedNumberSource implements SourceFunction<Integer>, ListCheckpointed<Long> {

		private static final long serialVersionUID = 1L;

		@Override
		public List<Long> snapshotState(long checkpointId, long timestamp) throws Exception {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void restoreState(List<Long> state) throws Exception {
			// TODO Auto-generated method stub

		}

		@Override
		public void run(org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext<Integer> ctx)
				throws Exception {
			// TODO Auto-generated method stub

		}

		@Override
		public void cancel() {
			// TODO Auto-generated method stub

		}

	}
	
	public static class MyRichMapFunction extends RichMapFunction<Integer, Integer> implements  ListCheckpointed<Integer> {
		
		private static final long serialVersionUID = 1L;
		private int count = 0;
		@Override
		public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
			// TODO Auto-generated method stub
			return new ArrayList<Integer>(count);
		}

		@Override
		public void restoreState(List<Integer> state) throws Exception {
			// TODO Auto-generated method stub
			
		}

		@Override
		public Integer map(Integer value) throws Exception {
			// TODO Auto-generated method stub
			return null;
		} 
		
	}
}
