package state;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.CheckpointedRestoring;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

//给个例子，stateful sinkfunction，在发送前先cache，
//http://www.cnblogs.com/fxjwind/p/5633302.html
@SuppressWarnings("deprecation")
public class BufferingSink implements SinkFunction<Tuple2<String, Integer>>, CheckpointedFunction, CheckpointedRestoring<ArrayList<Tuple2<String, Integer>>> {

 
	private static final long serialVersionUID = 1L;

	private final int threshold;

	private transient ListState<Tuple2<String, Integer>> checkpointedState;

	private List<Tuple2<String, Integer>> bufferedElements;

	public BufferingSink(int threshold) {
		this.threshold = threshold;
		this.bufferedElements = new ArrayList<>();
	}

	@Override
	public void invoke(Tuple2<String, Integer> value) throws Exception {
		bufferedElements.add(value);
		if (bufferedElements.size() == threshold) {
			for (Tuple2<String, Integer> element : bufferedElements) {
				// send it to the sink
				System.out.println(element);
			}
			bufferedElements.clear();
		}
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		checkpointedState = context.getOperatorStateStore().getSerializableListState("buffered-elements"); // 通过context初始化state

		if (context.isRestored()) { // 如果context中有可以restore的数据
			for (Tuple2<String, Integer> element : checkpointedState.get()) { // restore
				bufferedElements.add(element);
			}
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		checkpointedState.clear(); // 清空
		for (Tuple2<String, Integer> element : bufferedElements) {
			checkpointedState.add(element); // snapshot
		}
	}

	@Override
	public void restoreState(ArrayList<Tuple2<String, Integer>> state) throws Exception { // 这干嘛用的？
		// this is from the CheckpointedRestoring interface.
		this.bufferedElements.addAll(state);
	}
}