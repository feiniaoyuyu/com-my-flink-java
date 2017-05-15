package state;

import java.io.File;
import java.net.URL;
import java.util.List;

import org.apache.flink.api.common.PlanExecutor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.RemoteExecutor;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class ParallelExecution {

//	 setParallelism
//	./bin/flink run -p 10 ../examples/*WordCount-java*.jar
//	Setting the Maximum Parallelism

	public static void main(String[] args) throws Exception {
		
		try {
//			PackagedProgram program = new PackagedProgram(new File(""), args);
//		    InetSocketAddress jobManagerAddress = RemoteExecutor.getInetFromHostport("localhost:6123");
//
//		    Client client = new Client(jobManagerAddress, config, program.getUserCodeClassLoader());
//
//		    // set the parallelism to 10 here
//		    client.run(program, 10, true);
			//("localhost:6123");
		    Configuration config = new Configuration();

			String hostname = "localhost";
			int port = 6123;
 			List<URL> jarFiles = null;
			List<URL> globalClasspaths = null;
			PlanExecutor createRemoteExecutor = RemoteExecutor.createRemoteExecutor(hostname, port, config, jarFiles, globalClasspaths);
			createRemoteExecutor.start();

		} finally {
			
		}
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();
		DataStreamSource<String> input = env.fromElements("hello you",
				"hello me", "hello world");
		env.setMaxParallelism(10); // Setting the Maximum Parallelism

		SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = input
				.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void flatMap(String value,
							Collector<Tuple2<String, Integer>> out)
							throws Exception {

						String[] split = value.split(" ");

						for (String string : split) {
							out.collect(new Tuple2<String, Integer>(string, 1));
							
						}
					}
				});
		flatMap.print() ;
		
		
		SingleOutputStreamOperator<Tuple2<String, Integer>> wordCounts = flatMap
				.keyBy(0).timeWindow(Time.seconds(5)).sum(1).setParallelism(2);

		wordCounts.print();
		
		env.execute("Word Count Example");

	}
}
