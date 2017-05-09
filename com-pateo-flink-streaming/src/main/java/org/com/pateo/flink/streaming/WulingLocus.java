package org.com.pateo.flink.streaming;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
//import org.apache.flink.contrib.streaming.DataStreamUtils ;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.io.PrintingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;

import com.pateo.df.utils.WLContants;
import com.pateo.telematic.utils.CordinateService;
import com.pateo.telematic.utils.StringUtils;

public class WulingLocus {

	   
	private static final Logger logger = org.slf4j.LoggerFactory.getLogger(WulingLocus.class) ;

	public static void main(String[] args) {

		final CharSequence obd_prifix = "P011000100"; //P0110021000024
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
		
		//messageStream.print(); 

		// first step
		SingleOutputStreamOperator<String> filterStream = messageStream.filter(new FilterFunction<String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public boolean filter(String value) throws Exception {
				// filter data which not satisfy the condition
				return value != null && value.length() > 50;
			}
		});
		//filterStream.print();
		
		// second step map every log map to a hash map that contain all the key  
		SingleOutputStreamOperator<HashMap<String, String>> mapStream = filterStream
				.map(new MapFunction<String, HashMap<String, String>>() {
					private static final long serialVersionUID = 1L;
					@Override
					public HashMap<String, String> map(String log) throws Exception {

						String[] split = null;
						if (log.substring(1, log.length() - 1).split(", ").length > 4) {
							split = log.substring(1, log.length() - 1).split(", ");
						} else {
							split = log.substring(1, log.length() - 1).split(",");
						}
						
						//List<String> asList = Arrays.asList(split);
						HashMap<String, String> kvMap = new HashMap<String, String>();
						
						for (String kv : split) {
							String[] kvSplit = kv.split("=");
							if (kvSplit.length == 2) {
								kvMap.put(kvSplit[0], kvSplit[1]);
							} else {
								kvMap.put(WLContants.LAT, "0.000000");
								
								System.out.println("------------------- need both key and value \n" + log);
							}
						}

						return kvMap;
					}
				});
		//mapStream.print() ; 
//
		 // third filter some data which value is not empty or not null and contain ob_prefix
		SingleOutputStreamOperator<HashMap<String, String>> mapedDataSet = mapStream.filter(new FilterFunction<HashMap<String, String>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public boolean filter(HashMap<String, String> pointMap) throws Exception {

				if (StringUtils.isEmpty(pointMap.get( WLContants.DEVICEID)) ||
						Float.valueOf(pointMap.get(WLContants.LAT)) < 10 || 
						Float.valueOf(pointMap.get(WLContants.LON)) < 60) {
					return false;
				} else {
					String dv = pointMap.get(WLContants.DEVICEID);
					try {
						dv.contains(obd_prifix) ;
					} catch (Exception e) {
						System.out.println("===========pointMap:\n"+pointMap);
						e.printStackTrace();
						System.exit(10);
					}
					return dv.contains(obd_prifix);
				}
			}
		}); 
		//mapedDataSet.print() ; 
//		// fourth. map to get bd and gaode longitude and latitude 
		mapedDataSet = mapedDataSet.map(new MapFunction<HashMap<String, String> ,HashMap<String, String> >(){
 
			private static final long serialVersionUID = 1L;

			@Override
			public HashMap<String, String> map(HashMap<String, String> jmap) throws Exception {
				// TODO Auto-generated method stub
				 String longitude = jmap.get(WLContants.LON) ;
				 String latitude = jmap.get(WLContants.LAT) ;
				 // gps -> gd
				 Map<String, Double> gps_gd = CordinateService.gcj_encrypt(Double.valueOf(latitude ),
						 Double.valueOf( longitude)) ;
				 String latitude_gd = gps_gd.get(CordinateService.Lat) + "" ;
				 String longitude_gd = gps_gd.get(CordinateService.Lon) + "" ;
				 // gs -> bd
				 Map<String, Double> gps_bd = CordinateService.bd_encrypt(Double.valueOf(latitude_gd),
						 Double.valueOf(longitude_gd )) ;
				 // gd gps
				 jmap.put(WLContants.GD_LON, longitude_gd) ;
				 jmap.put(WLContants.GD_LAT, latitude_gd) ;
				 //bd gps
				 jmap.put(WLContants.BD_LON, gps_bd.get(CordinateService.Lon) + "") ;
				 jmap.put(WLContants.BD_LAT, gps_bd.get(CordinateService.Lat) + "");
				 
				 jmap.remove("id") ; // 移除id 
				 logger.debug("--------- jmap:" + jmap) ;
				 // 时间处理成秒
				 jmap.put(WLContants.GPSTIME, jmap.get(WLContants.GPSTIME).substring(0, 10)) ;
				 jmap.put(WLContants.UPDATETIME, jmap.get(WLContants.UPDATETIME).substring(0, 10)) ; 
				return jmap;
			}
			
		} );
		mapedDataSet.printToErr() ;
		//mapedDataSet.writeUsingOutputFormat(new PrintingOutputFormat<>("-------------", true)) ;
		//env.setBufferTimeout(10000);
		mapedDataSet.addSink(new MySinkFunction()).setParallelism(6) ;
		
//		mapedDataSet.countWindowAll(1000, 1000).apply(new RichAllWindowFunction<HashMap<String,String>, String, GlobalWindow>() { 
//			private static final long serialVersionUID = 1L;
//			public void apply(GlobalWindow window, Iterable<HashMap<String, String>> values, Collector<String> out)
//					throws Exception {
//			}
//		});
 
//		mapedDataSet.writeUsingOutputFormat(new OutputFormat<HashMap<String,String>>() {
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void writeRecord(HashMap<String, String> record) throws IOException {
//				// TODO Auto-generated method stub
//				
//			}
//			
//			@Override
//			public void open(int taskNumber, int numTasks) throws IOException {
//				// TODO Auto-generated method stub
//				
//			}
//			
//			@Override
//			public void configure(Configuration parameters) {
//				// TODO Auto-generated method stub
//				
//			}
//			
//			@Override
//			public void close() throws IOException {
//				// TODO Auto-generated method stub
//				
//			}
//		}) ;
		System.out.println("===================add sink ================");
//		mapedDataSet.addSink(new MySinkFunction());
		
		
//		mapedDataSet.windowAll(new WindowAssigner<T, W>) ;
//		AllWindowedStream<HashMap<String, String>, TimeWindow>  window = mapedDataSet.timeWindowAll(Time.of(5, TimeUnit.SECONDS)) ;
//		SingleOutputStreamOperator<Tuple2<String, Integer>> map = filter
//				.map(new MapFunction<String, Tuple2<String, Integer>>() {
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Tuple2<String, Integer> map(String line) throws Exception {
//						int lastIndexOf = line.indexOf("deviceid=") + "deviceid=".length();
//						// int length = "P011002100002011".length();
//						int offsetx = line.indexOf(", offsetx");
//
//						String deviceId = line.substring(lastIndexOf, offsetx);
//						// String deviceId = line.substring(lastIndexOf,
//						// lastIndexOf+length);
//						// Specifying keys via field positions is only valid for
//						// tuple data types. Type: String
//						// 次出只能是返回tuple，因为下面是有keyBy operation
//						return new Tuple2<String, Integer>(deviceId, 1);
//					}
//				}).filter(new FilterFunction<Tuple2<String, Integer>>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public boolean filter(Tuple2<String, Integer> value) throws Exception {
//
//						return value.f0.contains(obd_prifix);
//					}
//				});
//		map.print();

//		AllWindowedStream<Tuple2<String, Integer>, TimeWindow> timeWindowAll = map
//				.timeWindowAll(Time.of(5, TimeUnit.SECONDS)); // Time.of(1,
//																// TimeUnit.SECONDS)
//		// SingleOutputStreamOperator<Tuple2<String, Integer>> window =
//		// timeWindowAll.apply(new MyWindowAllFunction());
//		SingleOutputStreamOperator<Integer> window = timeWindowAll.apply(new MyWindowAllFunction2());
//
//		window.print();
//
//		KeyedStream<Tuple2<String, Integer>, Tuple> keyBy = map.keyBy(0);
//		keyBy.timeWindow(Time.of(5, TimeUnit.SECONDS));
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
		return new Double[] { 1. };
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

class MyWindowAllFunction2 implements AllWindowFunction<Tuple2<String, Integer>, Integer, TimeWindow> {

	private static final long serialVersionUID = 1L;

	@Override
	public void apply(TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Integer> collector)
			throws Exception {
		// New values
		Iterator<Tuple2<String, Integer>> it = input.iterator();
		Integer cnt = 0;
		while (it.hasNext()) {
			Tuple2<String, Integer> tuple = it.next();
			cnt += 1;
		}
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

		System.out.println(sdf.format(new Date()) + " MyWindowAllFunction2.apply()---cnt:" + cnt);
		collector.collect(cnt);

	} 
	 
} 