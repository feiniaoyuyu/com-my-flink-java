package side.output;

import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class SideOutPut {

	static class OutputTag<T> {

		<T> OutputTag(String name) {

		}
	}

	public static void main(String[] args) {

		final OutputTag<String> outputTag = new OutputTag("side-output") { };
		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();

		Tuple1<Integer> tuple1 = new Tuple1<Integer>(1);
		 
		DataStream<Integer> input = env.fromElements(1, 2);
		new ProcessFunction<Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void processElement(Integer input, Context ctx,
					Collector<Integer> out) throws Exception {
				// emit data to regular output
				Integer value = 1;
				out.collect(value);
				// emit data to side output
				// ctx.output(outputTag, "sideout-" + String.valueOf(value));
			}

		};
 
//				.process(new RichProcessFunction<Integer, Integer>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public void processElement(
//							Integer value,
//							org.apache.flink.streaming.api.functions.ProcessFunction.Context ctx,
//							Collector<Integer> out) throws Exception {
//						// TODO Auto-generated method stub
//
//					}
//
//					@Override
//					public void onTimer(
//							long timestamp,
//							org.apache.flink.streaming.api.functions.ProcessFunction.OnTimerContext ctx,
//							Collector<Integer> out) throws Exception {
//						// TODO Auto-generated method stub
//
//					}
//				});
	}
}
