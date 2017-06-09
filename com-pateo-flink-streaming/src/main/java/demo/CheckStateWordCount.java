package demo;

import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.connectors.fs.SequenceFileWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class CheckStateWordCount {

	public static void main(String[] args) {

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();

		// env.getConfig().setGlobalJobParameters(params);
		 env.enableCheckpointing(1000);
		// env.setBufferTimeout(100);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, org.apache.flink.api.common.time.Time.seconds(5)));
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		// make sure 500 ms of progress happen between checkpoints
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		
		// checkpoints have to complete within one minute, or are discarded
		env.getCheckpointConfig().setCheckpointTimeout(6000);
		
		int defaultLocalParallelism = StreamExecutionEnvironment.getDefaultLocalParallelism();
		//StreamExecutionEnvironment.setDefaultLocalParallelism(1);
		System.out.println("defaultLocalParallelism :" + defaultLocalParallelism);
		// get input data
		BucketingSink<Tuple2<Text, LongWritable>> sink = new BucketingSink<Tuple2<Text, LongWritable>> ("hdfs:///tmp/path/wc");
		sink.setBucketer(new DateTimeBucketer<Tuple2<Text, LongWritable>>("yyyy-MM-dd-HH-mm"));
		sink.setWriter(new SequenceFileWriter<Text, LongWritable>());
		
		//(new SequenceFileWriter<IntWritable, Text>("None", org.apache.hadoop.io.SequenceFile.CompressionType.NONE));
		sink.setBatchSize((long) (1024 * 1024 * 0.01)); // 1024 * 1024 * 400 this is 400 MB,

		DataStreamSource<Tuple2<String, Integer>> inputDS = env.addSource(WordSourceCheckpoint.create(10000));

		//inputDS.print();

		inputDS
		.keyBy(0)
		.countWindowAll(10) // 接收到的數量
		.process(new MyProcessAllWindowFunction())
		.map(new MapFunction<Tuple2<String,Integer>, Tuple2<Text,LongWritable>>() {
 
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<Text, LongWritable> map(Tuple2<String, Integer> value)
					throws Exception {
				return new Tuple2<>(new Text(value.f0),new LongWritable(value.f1));
			}
			
		})
		.addSink(sink) ;

		try {
			System.out.println("----------------execute ------------");
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static class WordSourceCheckpoint extends
			RichSourceFunction<Tuple2<String, Integer>> implements ListCheckpointed<Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;
		private static final String[] words = { "James", "Kobe", "Antony",
				"Jordan", "DuLante", "Zhouqi", "Kaka", "Yaoming", "Maidi",
				"YiJianlian" };
		private volatile int totalCot= 0;

//		@Override
//		public void setRuntimeContext(RuntimeContext t) {
//			super.setRuntimeContext(t);
//		}
		// private Random rand = new Random();
		private volatile boolean isRunning = true;
		private volatile int sleepTime = 3;
		private volatile int idRecord = 0;
		private volatile String exception = "0";
		private volatile Tuple2<Long, Long> snapid = new Tuple2<>(0L,0L);
		
		private WordSourceCheckpoint(int numOfIter) {
			totalCot = numOfIter;
		}

		public static WordSourceCheckpoint create(int numOfIter) {
			return new WordSourceCheckpoint(numOfIter);
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
//			mapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("kvs", 
//					TypeInformation.of(new TypeHint<String>() {}),
//					TypeInformation.of(new TypeHint<Long>() {}) ));
 		}

		@Override
		public void run(
				org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext<Tuple2<String, Integer>> ctx)
				throws Exception {
			
			final Object lock = ctx.getCheckpointLock();

			while (isRunning && idRecord <= totalCot ) { //
				Thread.sleep(sleepTime);
				
				Tuple2<String, Integer> record = new Tuple2<>(words[idRecord % words.length], 1);
				
				synchronized (lock) {
					idRecord += 1;
					ctx.collectWithTimestamp(record, System.currentTimeMillis());
					//System.out.println("--Thread name:" + Thread.currentThread().getName());
					ctx.emitWatermark(new Watermark(System.currentTimeMillis() - 1) );
 				}
				
				if (idRecord==2999 && exception.equals("10")) {
					exception = "112211";
					
					System.out.println(System.currentTimeMillis() + "===========snapid.f0 ===============" +snapid.f0  );

					snapshotState(snapid.f0 +1, System.currentTimeMillis());
					
					//Thread.sleep(2000);
					System.out.println(System.currentTimeMillis() + "===========idRecord==999=============" +idRecord);
					System.out.println(System.currentTimeMillis() + "===========exception==999============" +exception);

					throw new Exception("reach cnt " + idRecord);
				}
			}
		}

		@Override
		public void cancel() {
			System.out.println(System.currentTimeMillis() + "===========cancel idRecord=============" +idRecord);
			System.out.println(System.currentTimeMillis() + "===========cancel exception============" +exception);

			isRunning = false;
		}

		@Override
		public void close() throws Exception {
			System.out.println(System.currentTimeMillis() + "===========close idRecord ============" +idRecord);
			System.out.println(System.currentTimeMillis() + "===========close exception============" +exception);

			isRunning = false;
			super.close();
		}

		@Override
		public void restoreState(List<Tuple2<String, Integer>> paramList)
				throws Exception {
			for (Tuple2<String, Integer> tuple2 : paramList) 
			{
				System.out.println(System.currentTimeMillis() + "===========restoreState exception============" +tuple2.f0);
				System.out.println(System.currentTimeMillis() + "===========restoreState idRecord ============" +tuple2.f1);

				this.exception = tuple2.f0;
				this.idRecord = tuple2.f1;
			}
		}

		@Override
		public List<Tuple2<String, Integer>> snapshotState(long paramLong1,
				long paramLong2) throws Exception {
			this.snapid = new Tuple2<>(paramLong1,paramLong2);

//			if (paramLong1 > snapID.f0) {
//			}else {
//				this.snapID = new Tuple2<>(snapID.f0+1,snapID.f1+1);
//			}
			
			System.out.println("======"+paramLong1 + "=====" +paramLong2);
			System.out.println(System.currentTimeMillis() + "===========snapshotState idRecord ============" +idRecord);
			System.out.println(System.currentTimeMillis() + "===========snapshotState exception============" +exception);

			return Collections.singletonList(new Tuple2<String, Integer>(this.exception, this.idRecord));
		}
	}

	static class WordState
			extends
			RichFlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

		private static final long serialVersionUID = 1L;
		private transient MapState<String, Integer> sum;

		@Override
		public void open(Configuration config) {
			// MapState<String, Integer> mapState
			sum = getRuntimeContext().getMapState(
					new MapStateDescriptor<>("kvs", TypeInformation
							.of(new TypeHint<String>() {
							}), TypeInformation.of(new TypeHint<Integer>() {
					})));
		}

		@Override
		public void flatMap(Tuple2<String, Integer> kv,
				Collector<Tuple2<String, Integer>> out) throws Exception {
			MapState<String, Integer> kvs = sum;
			// System.out.println("------"+kv.f0);
			try {
				if (kvs == null) {
					System.out.println("kvs is null ");
				}

			} catch (Exception e) {
				System.exit(1);
			}

			if (kvs.contains(kv.f0)) {
				int cnt = kvs.get(kv.f0) + kv.f1;
				kvs.put(kv.f0, cnt);
				out.collect(new Tuple2<>(kv.f0, cnt));

			} else {
				int cnt = kv.f1;
				kvs.put(kv.f0, kv.f1);
				out.collect(new Tuple2<>(kv.f0, cnt));
				out.collect(new Tuple2<>(kv.f0, cnt));
			}
		}
	}
	static class MyProcessAllWindowFunction
	extends ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, GlobalWindow>
	implements RichFunction{
 
		private static final long serialVersionUID = 1L;
		private transient MapState<String, Integer> sum;

		
		@Override
		public void open(Configuration config) {
			// MapState<String, Integer> mapState
			sum = getRuntimeContext().getMapState(
					new MapStateDescriptor<>("kvs", TypeInformation
							.of(new TypeHint<String>() {
							}), TypeInformation.of(new TypeHint<Integer>() {
					})));
		}
		
		@Override
		public void process(
				ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, GlobalWindow>.Context ctx,
					Iterable<Tuple2<String, Integer>> iters,
					Collector<Tuple2<String, Integer>> out)
					throws Exception {
				
			for (Tuple2<String, Integer> kv : iters) {
					//out.collect(tuple2);
					MapState<String, Integer> kvs = sum;
					// System.out.println("------"+kv.f0);
					if (kvs.contains(kv.f0)) {
						int cnt = kvs.get(kv.f0) + kv.f1;
						kvs.put(kv.f0, cnt);
						out.collect(new Tuple2<>(kv.f0, cnt));
					} else {
						int cnt = kv.f1;
						kvs.put(kv.f0, kv.f1);
						out.collect(new Tuple2<>(kv.f0, cnt));
					}
				}
		}
		
	}
}
