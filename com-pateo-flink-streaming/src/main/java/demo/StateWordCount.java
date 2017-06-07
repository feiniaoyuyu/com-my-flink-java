package demo;

import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ReduceApplyProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.util.Collector;
//flink run -c demo.StateWordCount /data/sparkjob/com-my-flink-java/com-pateo-flink-streaming/target/com-pateo-flink-streaming-1.0.jar                             
//flink cancel -s /tmp/path/sp f60258cd28e2bf40e2576130548eafee
public class StateWordCount {

	
	public static void main(String[] args) {

		// Checking input parameters
		//final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();

 		//env.getConfig().setGlobalJobParameters(params);
//		env.enableCheckpointing(1000);
//		env.setBufferTimeout(100);
//		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, org.apache.flink.api.common.time.Time.seconds(10)));
		env.setMaxParallelism(4);
		int defaultLocalParallelism = StreamExecutionEnvironment.getDefaultLocalParallelism();
		StreamExecutionEnvironment.setDefaultLocalParallelism(1);
		System.out.println( "defaultLocalParallelism :" +defaultLocalParallelism );
		// get input data
		DataStreamSource<Tuple2<String, Integer>> inputDS = env.addSource( WordSourceCheckpoint.create(1));
		
		//inputDS.countWindowAll(100).sum(1).setMaxParallelism(1).print();
		
//		env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L))
//        .keyBy(0)
//        //.flatMap(new CountWindowAverage())
//        .print();
		
		
		inputDS
		//.setParallelism(1)
//		.keyBy(0)
		//.sum(1)
//		.flatMap(new WordState())
//		.timeWindowAll(Time.seconds(2), Time.seconds(2)).process(new ReduceApplyProcessAllWindowFunction<>(new ReduceFunction<Tuple2<String, Integer>>() {
//
//			@Override
//			public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
//				// TODO Auto-generated method stub
//				return new Tuple2<String, Integer>(t1.f0 , t1.f1+ t2.f1);
//			}
//		}, ))
//		.window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(5))).reduce(new ReduceFunction<Tuple2<String,Integer>>() {
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Tuple2<String, Integer> reduce(
//					Tuple2<String, Integer> v1,
//					Tuple2<String, Integer> v2) throws Exception {
//		        return new Tuple2<>(v1.f0, v1.f1 + v2.f1);
//			}
//		})
		//.trigger(new Trigger<T, Window>)
		.print();

		//		map.keyBy(0).sum(1).writeAsText("hdfs:///tmp/path/result", WriteMode.OVERWRITE);
//		Object job;
//		OutputFormat mapreduceOutputFormat;
		//OutputFormat<Tuple2<String, Integer>> format = new HadoopOutputFormat<>(mapreduceOutputFormat, job);
		//map.keyBy(0).sum(1).writeUsingOutputFormat(format)
		try {
			System.out.println("----------------execute ------------");
			env.execute();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static class WordSourceCheckpoint  extends RichSourceFunction<Tuple2<String, Integer>>
	//implements ListCheckpointed<Tuple2<String, Integer>>
	{
		private static final long serialVersionUID = 1L;
		private static final String[] words = {
				"James", "Kobe", "Antony",
				"Jordan", "DuLante", "Zhouqi", "Kaka", "Yaoming", "Maidi",
				"YiJianlian" };
		// private Character[] chars;
		private volatile int iteratorTime = 1;
		//private Random rand = new Random();
		private volatile boolean isRunning = true;
		private volatile int sleepTime = 1 ;

		private WordSourceCheckpoint(int numOfIter) {
 			iteratorTime = numOfIter;
		}

		public static WordSourceCheckpoint create(int numOfIter) {
			return new WordSourceCheckpoint(numOfIter);
		}
//		@Override
//		public void open(Configuration parameters) throws Exception {
//			super.open(parameters);
//		}

		@Override
		public void run(
				org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext<Tuple2<String, Integer>> ctx)
				throws Exception {
			while (isRunning  && iteratorTime!=0) { // 
				Thread.sleep(sleepTime);
				for (int id = 0; id < words.length; id++) {
					Tuple2<String, Integer> record = new Tuple2<>(words[id], 1);
					ctx.collectWithTimestamp(record, System.currentTimeMillis());
					ctx.collect(record);
					System.out.println("--Thread name:" + Thread.currentThread().getName());

					ctx.emitWatermark(new Watermark(System.currentTimeMillis()));
				}
				synchronized (this) {
					iteratorTime -= 1;
					System.out.println("--Thread iteratorTime:" + Thread.currentThread().getName());
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}
	
	static class WordState extends RichFlatMapFunction<Tuple2<String ,Integer>, Tuple2<String ,Integer>> {
 
		private static final long serialVersionUID = 1L;
	    private transient MapState<String, Integer> sum;
		@Override
		    public void open(Configuration config) {
			//MapState<String, Integer> mapState 
			sum = getRuntimeContext().getMapState(new MapStateDescriptor<>("kvs", 
					TypeInformation.of(new TypeHint<String>() {}),
					TypeInformation.of(new TypeHint<Integer>() {}) ));
			
//		        ValueStateDescriptor<MapState<String, Integer>> descriptor =  new ValueStateDescriptor<>(
//			                      "average", // the state name
//			                      TypeInformation.of(new TypeHint<MapState<String, Integer>>() {})// type information
//			                      ); // default value of the state, if nothing was set
//		      getRuntimeContext().getState(descriptor);
		}

		@Override
		public void flatMap(Tuple2<String, Integer> kv,
				Collector<Tuple2<String, Integer>> out) throws Exception {
			MapState<String, Integer> kvs = sum;
			//System.out.println("------"+kv.f0);
			try {
				if (kvs == null) {
				System.out.println("kvs is null ");
				}
				
			} catch (Exception e) {
				System.exit(1);
			}
			
			if (kvs.contains(kv.f0)) {
				int cnt =  kvs.get(kv.f0) + kv.f1;
				kvs.put(kv.f0, cnt);
				out.collect(new Tuple2<>(kv.f0, cnt));
				
			}else {
				int cnt =  kv.f1;
				kvs.put(kv.f0,  kv.f1);
				out.collect(new Tuple2<>(kv.f0, cnt));
				out.collect(new Tuple2<>(kv.f0, cnt));
			}
		}
	}
	
	
	static class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
 		private static final long serialVersionUID = -4076348165894795395L;
		/**
	     * The ValueState handle. The first field is the count, the second field a running sum.
	     */
	    private transient ValueState<Tuple2<Long, Long>> sum;

	    @Override
	    public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {

	        // access the state value
	        Tuple2<Long, Long> currentSum = sum.value();

	        // update the count
	        currentSum.f0 += 1;

	        // add the second field of the input value
	        currentSum.f1 += input.f1;

	        // update the state
	        sum.update(currentSum);

	        // if the count reaches 2, emit the average and clear the state
	        if (currentSum.f0 >= 2) {
	            out.collect(new Tuple2<>(input.f0, currentSum.f1 / currentSum.f0));
	            sum.clear();
	        }
	    }

	    @Override
	    public void open(Configuration config) {
	        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
	                new ValueStateDescriptor<>(
	                        "average", // the state name
	                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {})  // type information
	                 ); // default value of the state, if nothing was set
	        sum = getRuntimeContext().getState(descriptor);
	    }
	}
//
//	// this can be used in a streaming program like this (assuming we have a StreamExecutionEnvironment env)
//	env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L))
//	        .keyBy(0)
//	        .flatMap(new CountWindowAverage())
//	        .print();
}
