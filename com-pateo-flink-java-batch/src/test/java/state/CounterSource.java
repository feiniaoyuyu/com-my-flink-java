package state;

import java.util.Collections;
import java.util.List;

import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

// Stateful Source Functions
//对于有状态的source，有些不一样的是，在更新state和output时，注意要加锁来保证exactly-once，比如避免多个线程同时更新offset

public class CounterSource extends RichParallelSourceFunction<Long> implements ListCheckpointed<Long> {
 
	private static final long serialVersionUID = 1L;

	/** current offset for exactly once semantics */
	private Long offset;

	/** flag for job cancellation */
	private volatile boolean isRunning = true;

	@Override
	public void run(SourceContext<Long> ctx) {
		final Object lock = ctx.getCheckpointLock();

		while (isRunning) {
			// output and state update are atomic
			synchronized (lock) { // 加锁保证原子性
				ctx.collect(offset);
				offset += 1;
			}
		}
	}

	@Override
	public void cancel() {
		isRunning = false;
	}

	@Override
	public List<Long> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
		return Collections.singletonList(offset); // 不可变list，表示不可re-partitionable
	}

	@Override
	public void restoreState(List<Long> state) throws Exception {
		for (Long s : state)
			offset = s;
	}

}
