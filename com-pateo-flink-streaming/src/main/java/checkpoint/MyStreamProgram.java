package checkpoint;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer.FetcherType;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer.OffsetStore;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.HashMap;

public class MyStreamProgram {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();

		env.enableCheckpointing(4000);

		//env.setStateBackend(new FsStateBackend( "/flink/checkpoints"));

		List<String> arrayList = new ArrayList<String>();
		arrayList.add("navitrack");
		SimpleStringSchema simpleStringSchema = new SimpleStringSchema();

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers",
				"10.1.3.17:9092,10.1.3.18:9092,10.1.3.19:9092");
		// only required for Kafka 0.8
		properties.setProperty("zookeeper.connect",
				"10.1.3.17,10.1.3.18,10.1.3.19:2181");
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,
				"KafkaSourceWordCount");
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		// properties.setProperty("group.id", "test");

		// properties.setProperty("auto.offset.reset","smallest");
		DataStream<String> messageStream = env
				.addSource(new FlinkKafkaConsumer08<>(arrayList,
						simpleStringSchema, properties))// .uid("1-source-uid")
		;

		messageStream
				.map(new MapFunction<String, Tuple2<String, Integer>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> map(String arg)
							throws Exception {
						String[] line = arg.split(",");
						return new Tuple2<String, Integer>(line[0], Integer
								.parseInt(line[1]));
					}
				}).keyBy(0).timeWindow(Time.of(5, TimeUnit.SECONDS))
				.apply(new MyMaxWithState()).print();
		env.execute("Kafka example");

	}
}

class MyMaxWithState extends RichWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple, TimeWindow> {

	private static final long serialVersionUID = 1L;

	private ValueStateDescriptor<Map<String, Tuple2<String, Integer>>> stateDescriptor;

	@Override
	public void apply(Tuple key, TimeWindow window,
			Iterable<Tuple2<String, Integer>> input,
			Collector<Tuple2<String, Integer>> collector) throws Exception {

		// New values
		Iterator<Tuple2<String, Integer>> it = input.iterator();
		// State
		ValueState<Map<String, Tuple2<String, Integer>>> state = getRuntimeContext()
				.getState(stateDescriptor);
		// Values
		Map<String, Tuple2<String, Integer>> values = state.value();

		Tuple2<String, Integer> maxResult = values.get(key.toString());

		while (it.hasNext()) {
			Tuple2<String, Integer> tuple = it.next();

			if (maxResult == null) {
				maxResult = tuple;
			}

			if (tuple.f1 > maxResult.f1) {
				maxResult = tuple;
			}
		}

		values.put(key.toString(), maxResult);

		state.update(values);

		collector.collect(maxResult);
	}

	@Override
	public void open(Configuration conf) {
		this.stateDescriptor = new ValueStateDescriptor<>("last-result",
				new TypeHint<Map<String, Tuple2<String, Integer>>>() {
				}.getTypeInfo(), new HashMap<String, Tuple2<String, Integer>>());
	}

}