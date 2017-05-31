package state;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CheckPointTest {

	public static void main(String[] args) throws IOException {
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();
		// start a checkpoint every 1000 ms
		env.enableCheckpointing(1000);

		// advanced options:

		// set mode to exactly-once (this is the default)
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

		//env.enableCheckpointing(5000,CheckpointingMode.EXACTLY_ONCE, true);
		
		// make sure 500 ms of progress happen between checkpoints
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

		// checkpoints have to complete within one minute, or are discarded
		env.getCheckpointConfig().setCheckpointTimeout(60000);

		// allow only one checkpoint to be in progress at the same time
		env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
		
		env.setStateBackend(new FsStateBackend("") );
		// option 
		// env.setRestartStrategy(RestartStrategies.noRestart());
		//
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.of(10, TimeUnit.SECONDS) ));
		env.setRestartStrategy(RestartStrategies.failureRateRestart(3, // max failures per interval
				  Time.of(5, TimeUnit.MINUTES), //time interval for measuring failure rate
				  Time.of(10, TimeUnit.SECONDS) // delay)
				  ));
		System.out.println(env.getExecutionPlan());


	}
}
