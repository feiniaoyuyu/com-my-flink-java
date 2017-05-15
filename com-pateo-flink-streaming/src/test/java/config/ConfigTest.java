package config;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.Serializers;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class ConfigTest {

	public static void main(String[] args) {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();
		env.getConfig().setExecutionMode(ExecutionMode.PIPELINED);
		env.getConfig().setExecutionMode(ExecutionMode.BATCH);
		env.getConfig().setExecutionRetryDelay(5000);
		env.getConfig().enableClosureCleaner() ;
		env.getConfig().enableForceKryo() ;
		env.getConfig().enableObjectReuse() ;
		env.getConfig().enableSysoutLogging();
		env.getConfig().getGlobalJobParameters();

		env.getConfig().addDefaultKryoSerializer(String.class, MyClass.class);
		
	}
	class MyClass extends com.esotericsoftware.kryo.Serializer<String>{

		@Override
		public void write(Kryo kryo, Output output, String object) {
 			
		}

		@Override
		public String read(Kryo kryo, Input input, Class<String> type) {
 			return null;
		}
		
	}
}
