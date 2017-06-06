package process.function;

import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.util.Collector;

public class ProcessFunctionTest {

	/**
	 * The data type stored in the state
	 */
	public static class CountWithTimestamp {

		public String key;
		public long count;
		public long lastModified;
	}

	/**
	 * The implementation of the ProcessFunction that maintains the count and
	 * timeouts
	 */
	public static class CountWithTimeoutFunction extends
			ProcessFunction<Tuple2<String, String>, Tuple2<String, Long>>
			implements RichFunction {

		private static final long serialVersionUID = 1L;
		/** The state that is maintained by this process function */
		private ValueState<CountWithTimestamp> state;

		@Override
		public void open(Configuration parameters) throws Exception {
			state = getRuntimeContext().getState(
					new ValueStateDescriptor<>("myState",
							CountWithTimestamp.class));
		}

		@Override
		public void processElement(Tuple2<String, String> value, Context ctx,
				Collector<Tuple2<String, Long>> out) throws Exception {

			System.out.println("ctx=================:" + ctx.timestamp());
			// retrieve the current count
			CountWithTimestamp current = state.value();
			if (current == null) {
				current = new CountWithTimestamp();
				current.key = value.f0;
			}

			// update the state's count
			current.count++;

			// set the state's timestamp to the record's assigned event time
			// timestamp
			current.lastModified = ctx.timestamp();

			// write the state back
			state.update(current);

			// schedule the next timer 60 seconds from the current event time
			ctx.timerService().registerEventTimeTimer(
					current.lastModified + 60000);
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext ctx,
				Collector<Tuple2<String, Long>> out) throws Exception {

			// get the state for the key that scheduled the timer
			CountWithTimestamp result = state.value();

			// check if this is an outdated timer or the latest timer
			if (timestamp == result.lastModified + 60000) {
				// emit the state on timeout
				out.collect(new Tuple2<String, Long>(result.key, result.count));
			}
		}

	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		// ExecutionEnvironment env =
		// ExecutionEnvironment.getExecutionEnvironment();
		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();

		Tuple2<String, String> t1 = new Tuple2<String, String>("1", "2");
		Tuple2<String, String> t2 = new Tuple2<String, String>("2", "3");
		Tuple2<String, String> t3 = new Tuple2<String, String>("3", "4");
		Tuple2<String, String> t4 = new Tuple2<String, String>("4", "1");
		// the source data stream
		// class Splitter implements FlatMapFunction<String, Tuple2<String,
		// Integer>> {
		//
		// private static final long serialVersionUID = 1L;
		//
		// @Override
		// public void flatMap(String sentence, Collector<Tuple2<String,
		// Integer>> out) throws Exception {
		// for (String word: sentence.split(" ")) {
		// out.collect(new Tuple2<String, Integer>(word, 1));
		// }
		// }
		// }
		// DataStream<Tuple2<String, Integer>> dataStream = env
		// .socketTextStream("localhost", 9999)
		// .flatMap(new Splitter())
		// .keyBy(0)
		// .timeWindow(Time.seconds(5))
		// .sum(1);

		DataStream<Tuple2<String, String>> stream = env.fromElements(t1, t2,
				t3, t4);
		stream.assignTimestamps(new TimestampExtractor<Tuple2<String, String>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public long getCurrentWatermark() {
				// TODO Auto-generated method stub
				return 0;
			}

			@Override
			public long extractWatermark(Tuple2<String, String> element,
					long currentTimestamp) {
				// TODO Auto-generated method stub
				return 0;
			}

			@Override
			public long extractTimestamp(Tuple2<String, String> element,
					long currentTimestamp) {
				// TODO Auto-generated method stub
				return 0;
			}
		});
		// apply the process function onto a keyed stream
		DataStream<Tuple2<String, Long>> result = stream.keyBy(0).process(
				new CountWithTimeoutFunction());
		result.print();
		env.execute();
	}
}
