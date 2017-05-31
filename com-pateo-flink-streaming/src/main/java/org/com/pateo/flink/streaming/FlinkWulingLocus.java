package org.com.pateo.flink.streaming;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.connectors.fs.SequenceFileWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;

public class FlinkWulingLocus {

	private static final Logger logger = org.slf4j.LoggerFactory
			.getLogger(FlinkWulingLocus.class);

	public static void main(String[] args) throws IOException {

		final CharSequence obd_prifix = "P011002100002"; // P0110021000024
		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();

		// start a checkpoint every 1000 ms
		env.enableCheckpointing(1000);

		// advanced options:

		// set mode to exactly-once (this is the default)
		env.getCheckpointConfig().setCheckpointingMode(
				CheckpointingMode.EXACTLY_ONCE);

		// make sure 500 ms of progress happen between checkpoints
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

		// checkpoints have to complete within one minute, or are discarded
		env.getCheckpointConfig().setCheckpointTimeout(60000);

		// allow only one checkpoint to be in progress at the same time
		env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
		// env.setStateBackend(new
		// FsStateBackend("hdfs://namenode:40010/flink/checkpoints"));

		List<String> arrayList = new ArrayList<String>();
		arrayList.add("navitrack");
		SimpleStringSchema simpleStringSchema = new SimpleStringSchema();
		// FlinkKafkaConsumer.OffsetStore.FLINK_ZOOKEEPER
		// FlinkKafkaConsumer.FetcherType.LEGACY_LOW_LEVEL

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers",
				"10.1.3.17:9092,10.1.3.18:9092,10.1.3.19:9092");
		// only required for Kafka 0.8
		properties.setProperty("zookeeper.connect",
				"10.1.3.17,10.1.3.18,10.1.3.19:2181");
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,
				"KafkaSourceWordCount");
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		// properties.setProperty("auto.offset.reset","smallest");
		DataStream<String> messageStream = env
				.addSource(new FlinkKafkaConsumer08<>(arrayList,
						simpleStringSchema, properties))// .uid("1-source-uid")
		;
		// DataStream<String> messageStream = env.addSource(new
		// FlinkKafkaConsumer<String>("OAI",new SimpleStringSchema(),properties,
		// FlinkKafkaConsumer.OffsetStore.FLINK_ZOOKEEPER,
		// FlinkKafkaConsumer.FetcherType.LEGACY_LOW_LEVEL));

		// messageStream.print();

		// first step
		SingleOutputStreamOperator<String> filterStream = messageStream
				.filter(new FilterFunction<String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public boolean filter(String value) throws Exception {
						// filter data which not satisfy the condition
						return value != null && value.length() > 50;
					}
				})
		// .uid("2-filter-uid")
		;

		// filterStream.print();
		SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream = filterStream
				.map(new MapFunction<String, Tuple2<String, Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> map(String log)
							throws Exception {

						String[] split = null;
						if (log.substring(1, log.length() - 1).split(", ").length > 4) {
							split = log.substring(1, log.length() - 1).split(
									", ");
						} else {
							split = log.substring(1, log.length() - 1).split(
									",");
						}
						String deviceid = "0000000100168995";
						try {
							deviceid = split[4].split("=")[1];
						} catch (Exception e) {

						}
						return new Tuple2<String, Integer>(deviceid, 1);
					}
				})
		// .uid("3-map-uid")
		;
		BucketingSink<Tuple2<IntWritable, Text>> sink = new BucketingSink<Tuple2<IntWritable, Text>>(
				"/base/path");
		sink.setBucketer(new DateTimeBucketer<Tuple2<IntWritable, Text>>(
				"yyyy-MM-dd--HHmm"));
		sink.setWriter(new SequenceFileWriter<>());
		sink.setBatchSize(1024 * 1024 * 100); // this is 400 MB,

		WindowedStream<Tuple2<String, Integer>, Tuple, GlobalWindow> window = mapStream
				.keyBy(0).window(GlobalWindows.create());

		SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = window
				.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Tuple2<String, Integer> reduce(
							Tuple2<String, Integer> value1,
							Tuple2<String, Integer> value2) throws Exception {
						System.err.println("--" + value1.f0 + " "
								+ (value1.f1 + value2.f1));
						return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
					}
				});
		reduce.map(
				new MapFunction<Tuple2<String, Integer>, Tuple2<IntWritable, Text>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<IntWritable, Text> map(
							Tuple2<String, Integer> value) throws Exception {
						return new Tuple2<IntWritable, Text>(new IntWritable(
								value.f1), new Text(value.f0));
					}
				}).addSink(sink);

		System.out.println("===================add sink ================");
		try {
			env.execute("Read from Kafka example");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}