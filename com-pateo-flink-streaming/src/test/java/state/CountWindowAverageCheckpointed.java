package state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.util.Collector;

public class CountWindowAverageCheckpointed   extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>
implements Checkpointed<Tuple2<Long, Long>> {
	
	private static final long serialVersionUID = 1L;
	private Tuple2<Long, Long> sum = null;

	    @Override
	    public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {

	        // 更新count值
	        sum.f0 += 1;

	        // 将输入的count值累加到sum中
	        sum.f1 += input.f1;


	        // 如果 count 值 >=2，则输出count 及平均值，并且清空状态
	        if (sum.f0 >= 2) {
	            out.collect(new Tuple2<>(input.f0, sum.f1 / sum.f0));
	            sum = Tuple2.of(0L, 0L);
	        }
	    }

	    @Override
	    public void open(Configuration config) {
	        if (sum == null) {
	            // 仅当 sum 为null时创建状态变量，因为restoreState可能先于open()方法被调用
	            // 此时 sum 可能已经被设置为被恢复的状态值
	            sum = Tuple2.of(0L, 0L);
	        }
	    }

	    // 持续地持久化保存状态
	    @Override
	    public Tuple2<Long, Long> snapshotState(long checkpointId, long checkpointTimestamp) {
	        return sum;
	    }

	    // 出错时恢复状态
	    @Override
	    public void restoreState(Tuple2<Long, Long> state) {
	        sum = state;
	    }
//		@Override
//		public List<Tuple2<Long, Long>> snapshotState(long checkpointId,
//				long timestamp) throws Exception {
//			return null;
//		}
//		@Override
//		public void restoreState(List<Tuple2<Long, Long>> state)
//				throws Exception {
//		}
}
