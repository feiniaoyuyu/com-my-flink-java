package state;

import java.io.IOException;
import java.util.List;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
//import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class CountWindowAverage extends 
RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> 
implements ListCheckpointed<Tuple2<Long, Long>> {

	private static final long serialVersionUID = 1L;
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
 
	// 只要用户实现了Checkpointed接口，snapshotState(…) 和 restoreState(…)方法就会被调用，分别用于保存和恢复方法的状态。
	@Override
	public List<Tuple2<Long, Long>> snapshotState(long checkpointId,
			long timestamp) throws Exception {
		return null;
	}

	@Override
	public void restoreState(List<Tuple2<Long, Long>> state) throws Exception {
		
	}
//	@Override
//	public List<Long> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
//		return Collections.singletonList(offset); // 不可变list，表示不可re-partitionable
//	}
//	@Override
//	public void restoreState(List<Long> state) throws Exception {
//		for (Long s : state)
//			offset = s;
//	}
	@SuppressWarnings("deprecation")
	@Override
	public void open(Configuration config) {
		// average : the  state name
		ValueStateDescriptor<Tuple2<Long, Long>> descriptor = new ValueStateDescriptor<>("average", 
				TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
				}), // type information
				Tuple2.of(0L, 0L));
		// default value of the state, if nothing  was set
		sum = getRuntimeContext().getState(descriptor);
	}

	public static void main(String[] args) throws IOException {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// operate in Event-time
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		//env.setStateBackend(new FsStateBackend("hdfs://namenode:40010/flink/checkpoints"));

		// this can be used in a streaming program like this (assuming we have a
		// StreamExecutionEnvironment env)
		DataStreamSource<Tuple2<Long, Long>> fromE = env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L)); 
		env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE) ;
		//env.
		fromE
			.keyBy(0)
			.flatMap(new CountWindowAverage())
			.print();

		try {
			env.execute("-----state       +++");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
