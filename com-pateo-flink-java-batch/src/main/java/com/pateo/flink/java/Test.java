package com.pateo.flink.java;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

public class Test {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		List<Tuple2<String, Integer>> data = new ArrayList<Tuple2<String, Integer>>();
		data.add(new Tuple2<>("odd", 1));
		data.add(new Tuple2<>("even", 2));
		data.add(new Tuple2<>("odd", 3));
		data.add(new Tuple2<>("even", 4));
		DataStream<Tuple2<String, Integer>> tuples = env.fromCollection(data);
		KeyedStream<Tuple2<String, Integer>, Tuple> odd_and_evens = tuples.keyBy(0);

		DataStream<Tuple2<String, Integer>> sums = odd_and_evens.reduce(new ReduceFunction<Tuple2<String, Integer>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2)
					throws Exception {
				return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
			}
		});

		sums.print();

		DataStream<String> control = env.fromElements("DROP", "IGNORE");
		DataStream<String> data2 = env.fromElements("data", "DROP", "artisans", "IGNORE");
		DataStream<String> result = control.broadcast().connect(data2).flatMap(new MyCoFlatMap());
		result.print();

		// > listed DROP
		// > listed IGNORE
		// > passed data
		// > skipped DROP
		// > passed artisans
		// > skipped IGNORE

		// 3> listed IGNORE
		// 3> passed artisans
		// 1> listed DROP
		// 1> listed IGNORE
		// 1> passed data
		// 2> listed DROP
		// 2> listed IGNORE
		// 4> listed DROP
		// 2> skipped DROP
		// 4> listed IGNORE
		// 4> skipped IGNORE

		env.execute();
	}

	private static final class MyCoFlatMap implements CoFlatMapFunction<String, String, String> {

		private static final long serialVersionUID = 1L;
		HashSet<String> blacklist = new HashSet<String>();

		@Override
		public void flatMap1(String control_value, Collector<String> out) {
			blacklist.add(control_value);
			out.collect("listed " + control_value);
		}

		@Override
		public void flatMap2(String data_value, Collector<String> out) {
			if (blacklist.contains(data_value)) {
				out.collect("skipped " + data_value);
			} else {
				out.collect("passed " + data_value);
			}
		}
	}
	
}

class MyRichFunction22 extends RichFlatMapFunction<String,String> implements RichFunction{
	 
	private static final long serialVersionUID = 1L;
	
	@Override
	public void open(Configuration parameters) throws Exception {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void close() throws Exception {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public RuntimeContext getRuntimeContext() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public IterationRuntimeContext getIterationRuntimeContext() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public void setRuntimeContext(RuntimeContext t) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void flatMap(String value, Collector<String> out) throws Exception {
		// TODO Auto-generated method stub
		RuntimeContext runtimeContext = getRuntimeContext();
		
		runtimeContext.getIndexOfThisSubtask() ;
		runtimeContext.getNumberOfParallelSubtasks() ;
		runtimeContext.getExecutionConfig() ;
		//runtimeContext.getState(new ValueStateDescriptor<>("", typeInfo)) ; 
	}

	
}