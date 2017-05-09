package org.com.pateo.flink.streaming;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigConstants;
public class HBaseFlinkTestConstants {


	public static final byte[] CF_SOME = "someCf".getBytes(); // ConfigConstants.DEFAULT_CHARSET
	public static final byte[] Q_SOME = "someQual".getBytes(); //ConfigConstants.DEFAULT_CHARSET
	public static final String TEST_TABLE_NAME = "test-table";
	public static final String TMP_DIR = "/tmp/test";

}
