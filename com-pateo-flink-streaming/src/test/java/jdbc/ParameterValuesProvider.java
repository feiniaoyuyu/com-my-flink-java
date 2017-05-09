package jdbc;

import java.io.Serializable;

import jdbc.JDBCInputFormat;

/**
 * 
 * This interface is used by the {@link JDBCInputFormat} to compute the list of parallel query to run (i.e. splits).
 * Each query will be parameterized using a row of the matrix provided by each {@link ParameterValuesProvider} implementation
 * 
 * */
public interface ParameterValuesProvider {

	/** Returns the necessary parameters array to use for query in parallel a table */
	public Serializable[][] getParameterValues();
	
}
