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
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, org.apache.flink.api.common.time.Time.seconds(10)));
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
		.countWindowAll(1) // 接收到的數量
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
		// private Random rand = new Random();
		private volatile boolean isRunning = true;
		private volatile int sleepTime = 1;
		private volatile int idRecord = 0;
		private volatile String exception = "0";
		private WordSourceCheckpoint(int numOfIter) {
			totalCot = numOfIter;
		}

		public static WordSourceCheckpoint create(int numOfIter) {
			return new WordSourceCheckpoint(numOfIter);
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
		}

		@Override
		public void run(
				org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext<Tuple2<String, Integer>> ctx)
				throws Exception {
			
			while (isRunning && idRecord <= totalCot ) { //
				Thread.sleep(sleepTime);
				
				Tuple2<String, Integer> record = new Tuple2<>(words[idRecord % words.length], 1);
				ctx.collectWithTimestamp(record, System.currentTimeMillis());

				//System.out.println("--Thread name:" + Thread.currentThread().getName());
				ctx.emitWatermark(new Watermark(System.currentTimeMillis()));
				if (idRecord==999 && exception.equals( "0")) {
					exception = "112211";
					throw new Exception("reach cnt " + idRecord);
				}
				synchronized (this) {
					idRecord += 1;
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}

		@Override
		public void close() throws Exception {
			isRunning = false;
			super.close();
		}

		@Override
		public void restoreState(List<Tuple2<String, Integer>> paramList)
				throws Exception {
			for (Tuple2<String, Integer> tuple2 : paramList) 
			{
				this.idRecord = tuple2.f1;
				this.exception = "1234321";
			}
		}

		@Override
		public List<Tuple2<String, Integer>> snapshotState(long paramLong1,
				long paramLong2) throws Exception {
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
