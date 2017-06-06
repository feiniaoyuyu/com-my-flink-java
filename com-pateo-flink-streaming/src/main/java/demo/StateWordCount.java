package demo;

import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
//flink run -c demo.StateWordCount /data/sparkjob/com-my-flink-java/com-pateo-flink-streaming/target/com-pateo-flink-streaming-1.0.jar                             
//flink cancel -s /tmp/path/sp f60258cd28e2bf40e2576130548eafee
public class StateWordCount {

	public static void main(String[] args) {

		// Checking input parameters
		//final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();

		// make parameters available in the web interface
		//env.getConfig().setGlobalJobParameters(params);
		env.enableCheckpointing(3000);
		env.setBufferTimeout(1000);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.of(10, TimeUnit.SECONDS) ));

		// get input data
//		DataStream<String> text;
//		if (params.has("input")) {
//			// read the text file from given input path
//			text = env.readTextFile(params.get("input"));
//		} else {
//			System.out
//					.println("Executing WordCount example with default input data set.");
//			System.out.println("Use --input to specify file input.");
//			// get default test text data
//			text = env.fromElements(WordCountData.WORDS);
//		}
		
		DataStreamSource<Tuple2<String, Integer>> inputDS = env.addSource(new WordSourceCheckpoint(10000));
		
		SingleOutputStreamOperator<Tuple2<String, Integer>> map = inputDS.map(new RichMapFunction<Tuple2<String,Integer>, Tuple2<String,Integer>>() {
 
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> map(Tuple2<String, Integer> value)
					throws Exception {
 				return value;
			}
		});
		map.keyBy(0).sum(1).writeAsText("hdfs:///tmp/path/result", WriteMode.OVERWRITE);
//		Object job;
//		OutputFormat mapreduceOutputFormat;
		//OutputFormat<Tuple2<String, Integer>> format = new HadoopOutputFormat<>(mapreduceOutputFormat, job);
		//map.keyBy(0).sum(1).writeUsingOutputFormat(format)
		try {
			env.execute();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	static class WordSourceCheckpoint 
	extends RichSourceFunction<Tuple2<String, Integer>>
	//implements ListCheckpointed<Tuple2<String, Integer>>
	{
		private static final long serialVersionUID = 1L;
		private static final String[] words = {
				"James", "Kobe", "Antony",
				"Jordan", "DuLante", "Zhouqi", "Kaka", "Yaoming", "Maidi",
				"YiJianlian" };
		// private Character[] chars;
		private  int iteratorTime = 10;
		//private Random rand = new Random();
		private volatile boolean isRunning = true;

		private WordSourceCheckpoint(int numOfCars) {
			super();
			iteratorTime = numOfCars;
		}

		public static WordSourceCheckpoint create(int cars) {
			return new WordSourceCheckpoint(cars);
		}
		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
		}

		@Override
		public void run(
				org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext<Tuple2<String, Integer>> ctx)
				throws Exception {

			while (isRunning && iteratorTime!=0) {
				
				Thread.sleep(100);
				
				for (int id = 0; id < words.length; id++) {
					Tuple2<String, Integer> record = new Tuple2<>(words[id], 1);
					ctx.collect(record);
				}
				synchronized (this) {
					iteratorTime-=1;
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
		 
//		@Override
//		public List<Tuple2<String, Integer>> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
//			return Collections.singletonList(iteratorTime);
//		}
//
//		@Override
//		public void restoreState(List<Tuple2<String, Integer>> state)
//				throws Exception {
//			
//		}
	} 
}
