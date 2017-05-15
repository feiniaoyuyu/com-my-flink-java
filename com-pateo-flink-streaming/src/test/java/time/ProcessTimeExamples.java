package time;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import time.ProcessTimeExamples.MyEvent;

public class ProcessTimeExamples {

	public static void main(String[] args) {
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		// alternatively:
		// env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		// env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

 		List<String> topic = new ArrayList<String>();
 		topic.add("navitrack");
		// FlinkKafkaConsumer.OffsetStore.FLINK_ZOOKEEPER
		// FlinkKafkaConsumer.FetcherType.LEGACY_LOW_LEVEL
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "10.1.3.17:9092,10.1.3.18:9092,10.1.3.19:9092");
		// only required for Kafka 0.8
		properties.setProperty("zookeeper.connect", "10.1.3.17,10.1.3.18,10.1.3.19:2181");
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "KafkaSourceWordCount");
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		 properties.setProperty("auto.offset.reset","smallest");
//		 SimpleStringSchema simpleStringSchema = new SimpleStringSchema();
//		DataStream<String> messageStream = env
//				.addSource(new FlinkKafkaConsumer08<String>(topic, simpleStringSchema, properties));
//		messageStream.print() ;
//		gpsspeed=7, bearing=196, lon=116.05722, gpsreliable=1, deviceid=P011002100007070, offsetx=620, carspeed=7, offsety=114, roadlevel=4, id=177907103, gpstime=1493344999842, updatetime=1493345030712, user=P011002100007070, lat=39.15276, direction=4}

		DataStream<MyEvent> stream = env.addSource(new FlinkKafkaConsumer08<MyEvent>(topic, new MyEventSchema(), properties)); 
//
//		stream
//		    .keyBy( (event) -> event.getUser() )
//		    .timeWindow(Time.hours(1))
//		    .reduce( (a, b) -> a.add(b) ) 
//		    .addSink(new SinkFunction<ProcessTimeExamples.MyEvent>() {
//
//
//				private static final long serialVersionUID = 1L;
//
//				@Override
//				public void invoke(MyEvent value) throws Exception {
//					System.out.println("value:"+ value);
//				}
//			});
		
		try {
			env.execute("====ProcessTimeExamples=====") ;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	static class User {
		
		String name = "" ;
		int  age = 0;
		public User(){
			super() ;
		}
		
		public User(String name, int age) {
			this.name = name ;
			this.age = age;
		}
	}
	public static final class MyEvent {
//		gpsspeed=7, bearing=196, lon=116.05722, gpsreliable=1, deviceid=P011002100007070, offsetx=620, carspeed=7, 
		//offsety=114, roadlevel=4, id=177907103, gpstime=1493344999842, updatetime=1493345030712, user=P011002100007070, 
		//lat=39.15276, direction=4}
		private static String time = null ;
		
		private static User user = null ;

		@Override
		public String toString() {
			return "MyEvent [toString()=" + super.toString() + "]";
		}
		public  User getUser() {
			return this.user;
		}

		public MyEvent add(MyEvent b) {
			return null;
		}
		public String getTime() {
			return this.time;
		}
		MyEvent (){
		}
		public static MyEvent fromString(String log) {
			
			
			String[] split = log.substring(1, log.length() -1 ).split("\\, ");
			HashMap<String, String> hashMap = new HashMap<String,String>();
			for (String kv: split ) {
				String[] kvSplit = kv.split("=");
				if (kvSplit.length ==2 ) {
					hashMap.put(kvSplit[0],kvSplit[1]) ;
				}else {
					
				}
			}
			 
			time = hashMap.get("gpstime"); 
	        user = new User();
			return null;
		}
		public Object severity() {
			// TODO Auto-generated method stub
			return null;
		}
		public Object getGroup() {
			// TODO Auto-generated method stub
			return null;
		}
		public long getCreationTime() {
			// TODO Auto-generated method stub
			return 0;
		}
		public boolean hasWatermarkMarker() {
			// TODO Auto-generated method stub
			return false;
		}

	}

}

class MyEventSchema implements DeserializationSchema<MyEvent>, SerializationSchema<MyEvent>{

	private static final long serialVersionUID = 1L;
	 
	@Override
	public byte[] serialize(MyEvent element) {
		return element.toString().getBytes();
	}

	@Override
	public TypeInformation<MyEvent> getProducedType() {
		return TypeExtractor.getForClass(MyEvent.class);
	}

	@Override
	public MyEvent deserialize(byte[] message) {
		return MyEvent.fromString(new String(message));
	}

	@Override
	public boolean isEndOfStream(MyEvent nextElement) {
		// TODO Auto-generated method stub
		return false;
	}
}
