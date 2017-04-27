package flink.java;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class KafkaSourceWordCount {

	public static void main(String[] args) {
		
		CharSequence obd_prifix = "P01100210";
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		List<String> arrayList = new ArrayList<String>();
		arrayList.add("navitrack");
		SimpleStringSchema simpleStringSchema = new SimpleStringSchema();
		// FlinkKafkaConsumer.OffsetStore.FLINK_ZOOKEEPER
		// FlinkKafkaConsumer.FetcherType.LEGACY_LOW_LEVEL
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "10.1.3.17:9092,10.1.3.18:9092,10.1.3.19:9092");
		// only required for Kafka 0.8
		properties.setProperty("zookeeper.connect", "10.1.3.17,10.1.3.18,10.1.3.19:2181");
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "KafkaSourceWordCount");
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		// properties.setProperty("auto.offset.reset","smallest");
		DataStream<String> messageStream = env
				.addSource(new FlinkKafkaConsumer08<>(arrayList, simpleStringSchema, properties));
		// DataStream<String> messageStream = env.addSource(new
		// FlinkKafkaConsumer<String>("OAI",new SimpleStringSchema(),properties,
		// FlinkKafkaConsumer.OffsetStore.FLINK_ZOOKEEPER,
		// FlinkKafkaConsumer.FetcherType.LEGACY_LOW_LEVEL));

		SingleOutputStreamOperator<String> filter = messageStream.filter(new FilterFunction<String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public boolean filter(String value) throws Exception {
				// TODO Auto-generated method stub
				return value.length() > 50;
			}
		});
		DataStream<Map<String, String>> filterData = filter.map(new MapFunction<String, Map<String, String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Map<String, String> map(String line) throws Exception {
				Map<String, String> hashMap = new HashMap<>();

				String[] split = line.substring(1, line.length() - 1).split(", ");
				List<String> list = java.util.Arrays.asList(split);

				list.forEach(new Consumer<String>() {
					@Override
					public void accept(String t) {

						String[] kv= t.split("=");
						
						try {
							if (kv.length!=2) {
								hashMap.put("keynull", "valuenull");
							}else {
								hashMap.put(kv[0], kv[1]);
							}
						} catch (Exception e) {
							System.err.println("---------------java.lang.ArrayIndexOutOfBoundsException-------"+line);
							e.printStackTrace();
							System.exit(-1);
						}
					}
				});
				return hashMap;
			}

		}).filter(new FilterFunction<Map<String, String>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public boolean filter(Map<String, String> value) throws Exception {
				
				return value.get("deviceid").contains(obd_prifix ) && Integer.valueOf(value.get("gpsspeed") ) > 20;
			}
		}).rebalance(); // The DataStream with rebalance partitioning set.
		
		int parallelism = filterData.getParallelism();
		System.out.println("---parallelism:" + parallelism);
		AllWindowedStream<Map<String, String>, GlobalWindow> countWindowAll = filterData.countWindowAll(5);
		System.out.println("-------countWindowAll:"+countWindowAll);

		filterData.print();
		try {
			env.execute("Read from Kafka example");
		} catch (Exception e) {
 			e.printStackTrace();
		}

	}

}
