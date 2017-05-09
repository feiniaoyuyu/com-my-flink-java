package jdbc;


import java.io.Serializable;

import  jdbc.JDBCInputFormat;

/** 
 * 
 * This splits generator actually does nothing but wrapping the query parameters
 * computed by the user before creating the {@link JDBCInputFormat} instance.
 * 
 * */
public class GenericParameterValuesProvider implements ParameterValuesProvider {

	private final Serializable[][] parameters;
	
	public GenericParameterValuesProvider(Serializable[][] parameters) {
		this.parameters = parameters;
	}

	@Override
	public Serializable[][] getParameterValues(){
		//do nothing...precomputed externally
		return parameters;
	}

}
