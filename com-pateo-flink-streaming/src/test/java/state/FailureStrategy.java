package state;

import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class FailureStrategy {

	public static void main(String[] args) {
//		This helpful for streaming programs which enable checkpointing
//		flink-conf.yaml
//		restart-strategy: fixed-delay
//		restart-strategy.fixed-delay.attempts: 3
//		restart-strategy.fixed-delay.delay: 10 s
		
//		restart-strategy: failure-rate
//		restart-strategy.failure-rate.max-failures-per-interval: 3
//		restart-strategy.failure-rate.failure-rate-interval: 5 min
//		restart-strategy.failure-rate.delay: 10 s
		
//		restart-strategy: none

		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();
		 
 		// option 
		env.setRestartStrategy(RestartStrategies.noRestart());
		
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.of(10, TimeUnit.SECONDS) ));
		env.setRestartStrategy(RestartStrategies.failureRateRestart(3, // max failures per interval
				  Time.of(5, TimeUnit.MINUTES), //time interval for measuring failure rate
				  Time.of(10, TimeUnit.SECONDS) // delay)
				  ));
	}
}
