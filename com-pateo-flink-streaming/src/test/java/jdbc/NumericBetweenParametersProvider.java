package jdbc;


import static org.apache.flink.util.Preconditions.checkArgument;

import java.io.Serializable;

/** 
 * 
 * This query parameters generator is an helper class to parameterize from/to queries on a numeric column.
 * The generated array of from/to values will be equally sized to fetchSize (apart from the last one),
 * ranging from minVal up to maxVal.
 * 
 * For example, if there's a table <CODE>BOOKS</CODE> with a numeric PK <CODE>id</CODE>, using a query like:
 * <PRE>
 *   SELECT * FROM BOOKS WHERE id BETWEEN ? AND ?
 * </PRE>
 *
 * you can take advantage of this class to automatically generate the parameters of the BETWEEN clause,
 * based on the passed constructor parameters.
 * 
 * */
public class NumericBetweenParametersProvider implements ParameterValuesProvider {

	private final long fetchSize;
	private final long minVal;
	private final long maxVal;
	
	/**
	 * NumericBetweenParametersProvider constructor.
	 * 
	 * @param fetchSize the max distance between the produced from/to pairs
	 * @param minVal the lower bound of the produced "from" values
	 * @param maxVal the upper bound of the produced "to" values
	 */
	public NumericBetweenParametersProvider(long fetchSize, long minVal, long maxVal) {
		checkArgument(fetchSize > 0, "Fetch size must be greater than 0.");
		checkArgument(minVal <= maxVal, "Min value cannot be greater than max value.");
		this.fetchSize = fetchSize;
		this.minVal = minVal;
		this.maxVal = maxVal;
	}

	@Override
	public Serializable[][] getParameterValues() {
		double maxElemCount = (maxVal - minVal) + 1;
		int numBatches = new Double(Math.ceil(maxElemCount / fetchSize)).intValue();
		Serializable[][] parameters = new Serializable[numBatches][2];
		int batchIndex = 0;
		for (long start = minVal; start <= maxVal; start += fetchSize, batchIndex++) {
			long end = start + fetchSize - 1;
			if (end > maxVal) {
				end = maxVal;
			}
			parameters[batchIndex] = new Long[]{start, end};
		}
		return parameters;
	}
	
}
