package flink.java;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class KafkaSourceWindowCount {

	public static void main(String[] args) {

		CharSequence obd_prifix = "P011002100002011";
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
				return value.length() > 50;
			}
		});
		SingleOutputStreamOperator<Tuple2<String, Integer>> map = filter
				.map(new MapFunction<String, Tuple2<String, Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> map(String line) throws Exception {
						int lastIndexOf = line.indexOf("deviceid=") + "deviceid=".length();
						// int length = "P011002100002011".length();
						int offsetx = line.indexOf(", offsetx");

						String deviceId = line.substring(lastIndexOf, offsetx);
						// String deviceId = line.substring(lastIndexOf,
						// lastIndexOf+length);
						// Specifying keys via field positions is only valid for
						// tuple data types. Type: String
						// 次出只能是返回tuple，因为下面是有keyBy operation
						return new Tuple2<String, Integer>(deviceId, 1);
					}
				}).filter(new FilterFunction<Tuple2<String,Integer>>() {
 
					private static final long serialVersionUID = 1L;

					@Override
					public boolean filter(Tuple2<String, Integer> value) throws Exception {
						
						return value.f0.contains(obd_prifix);
					}
				});
		map.print();
		
		AllWindowedStream<Tuple2<String, Integer>, TimeWindow> timeWindowAll = map
				.timeWindowAll(Time.of(5, TimeUnit.SECONDS)); // Time.of(1,
																// TimeUnit.SECONDS)
//		SingleOutputStreamOperator<Tuple2<String, Integer>> window = timeWindowAll.apply(new MyWindowAllFunction());
		SingleOutputStreamOperator<Integer> window = timeWindowAll.apply(new MyWindowAllFunction2());

		 window.print();

		 KeyedStream<Tuple2<String, Integer>, Tuple> keyBy = map.keyBy(0);
		 keyBy.timeWindow(Time.of(5, TimeUnit.SECONDS));
		// keyBy.print();

		try {
			env.execute("Read from Kafka example");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}

class PartialModelBuilder implements AllWindowFunction<Integer, Double[], TimeWindow> {
	private static final long serialVersionUID = 1L;

	protected Double[] buildPartialModel(Iterable<Integer> values) {
		return new Double[]{1.};
	}

	@Override
	public void apply(TimeWindow window, Iterable<Integer> values, Collector<Double[]> out) throws Exception {
		out.collect(buildPartialModel(values));
	}
}
class MyWindowAllFunction implements AllWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> {

	private static final long serialVersionUID = 1L;

	@Override
	public void apply(TimeWindow window, Iterable<Tuple2<String, Integer>> input,
			Collector<Tuple2<String, Integer>> collector) throws Exception {
		// New values
		Iterator<Tuple2<String, Integer>> it = input.iterator();
		while (it.hasNext()) {
			Tuple2<String, Integer> tuple = it.next();
			
			collector.collect(tuple);
			
		}
 
	}

}

class MyWindowAllFunction2 implements AllWindowFunction<Tuple2<String, Integer>, Integer , TimeWindow> {

	private static final long serialVersionUID = 1L;

	@Override
	public void apply(TimeWindow window, Iterable<Tuple2<String, Integer>> input,
			Collector<Integer> collector) throws Exception {
		// New values
		Iterator<Tuple2<String, Integer>> it = input.iterator();
		Integer cnt = 0;
		while (it.hasNext()) {
			Tuple2<String, Integer> tuple = it.next();
			cnt += 1 ;
		}
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		 
		System.out.println(sdf.format(new Date())+" MyWindowAllFunction2.apply()---cnt:" +cnt);
		collector.collect(cnt);
 
	}

}

