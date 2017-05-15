package serializer;

import org.apache.flink.api.java.ExecutionEnvironment;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.twitter.chill.protobuf.ProtobufSerializer;
import com.twitter.chill.thrift.TBaseSerializer;

class MyCustomType {
	
}

 class MySerializer extends Serializer<String>{

	@Override
	public void write(Kryo kryo, Output output, String object) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String read(Kryo kryo, Input input, Class<String> type) {
		// TODO Auto-generated method stub
		return null;
	}
	
}

class MyCustomSerializer extends Serializer<String>{

	@Override
	public void write(Kryo kryo, Output output, String object) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String read(Kryo kryo, Input input, Class<String> type) {
		// TODO Auto-generated method stub
		return null;
	}
	
}

public class CustomSerializer {


	
	public static void main(String[] args) {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// register the class of the serializer as serializer for a type
		env.getConfig().registerTypeWithKryoSerializer(MyCustomType.class, MyCustomSerializer.class);

		// register an instance as serializer for a type
		MySerializer mySerializer = new MySerializer();
//		env.getConfig().registerTypeWithKryoSerializer(MyCustomType.class,   (T) mySerializer);
		
		TBaseSerializer tBaseSerializer = new TBaseSerializer();
		
		env.getConfig().registerTypeWithKryoSerializer(MyCustomType.class, ProtobufSerializer.class);
		env.getConfig().registerTypeWithKryoSerializer(MyCustomType.class, TBaseSerializer.class);

	}
}
