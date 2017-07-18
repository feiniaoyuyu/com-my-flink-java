package com.pateo.flink.java;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger ;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow ;
import org.apache.flink.util.Collector;

 
public class WindowAnatomy {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment() ;

			    DataStreamSource<String> source = env.socketTextStream("10.172.10.167",9999);
			    
			    SingleOutputStreamOperator<Tuple2<String, Integer>> values = source.flatMap(new FlatMapFunction<String, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void flatMap(String value, Collector<String> out) throws Exception {
						// TODO Auto-generated method stub
						for (String string : value.split("\\s+")) {
							out.collect(string);
						}
					}
				}).map(new MapFunction<String, Tuple2<String, Integer>>() {
 
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> map(String value) throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<String, Integer>(value,1) ;
					}
					
				}); 
			    KeyedStream<Tuple2<String, Integer>, Tuple> keyValue = values.keyBy(0);
//			    val keyValue = values.keyBy(0);
//
//			    // define the count window without purge
//
//			    val countWindowWithoutPurge = 
			    WindowedStream<Tuple2<String, Integer>, Tuple, GlobalWindow> countWindowWithoutPurge = keyValue.window(GlobalWindows.create())
			    		.trigger(CountTrigger.of(2));
 
			    WindowedStream<Tuple2<String, Integer>, Tuple, GlobalWindow> countWindowWithPurge = keyValue.window(GlobalWindows.create())
			    		.trigger(PurgingTrigger.of(CountTrigger.of(2))) ;
			    countWindowWithoutPurge.sum(1).print();
//
			    countWindowWithPurge.sum(1).print();

			    try {
					env.execute() ;
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	}
}


