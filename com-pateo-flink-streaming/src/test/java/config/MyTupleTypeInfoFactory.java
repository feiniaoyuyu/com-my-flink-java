package config;

import java.lang.reflect.Type;
import java.util.Map;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

public class MyTupleTypeInfoFactory extends
		TypeInfoFactory<MyTuple> {

	@Override
	public TypeInformation<MyTuple> createTypeInfo(Type t,
			Map<String, TypeInformation<?>> genericParameters) {
		// TODO Auto-generated method stub
		return null;
	}

 

//	@Override
//	public TypeInformation<Tuple2<String, String>> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
//		return new MyTupleTypeInfo(genericParameters.get("T0"), genericParameters.get("T1"));
//	}

}

class MyTuple<T0, T1> {
	  public T0 myfield0;
	  public T1 myfield1;
}
class MyTupleTypeInfo extends TypeInformation<String> {

	private static final long serialVersionUID = 1L;

	MyTupleTypeInfo() {

	}

	MyTupleTypeInfo(Object o1, Object o2) {
		super();
	}

	@Override
	public boolean isBasicType() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isTupleType() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int getArity() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getTotalFields() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Class<String> getTypeClass() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isKeyType() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public TypeSerializer<String> createSerializer(ExecutionConfig config) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean canEqual(Object obj) {
		// TODO Auto-generated method stub
		return false;
	}

}