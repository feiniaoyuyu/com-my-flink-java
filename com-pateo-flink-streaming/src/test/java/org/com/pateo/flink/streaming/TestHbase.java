package org.com.pateo.flink.streaming;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.pateo.df.utils.HBaseConnectionTool;

public class TestHbase {

	private static final String tableName_detail = "dongfeng:gps_locus_point_detail"; // 轨迹点详情
	// 包括经纬度poi信息
	private static final String tableName_last = "dongfeng:gps_locus_point_last"; // 每条轨迹最后一个点
	private static final String tableName_stop = "dongfeng:gps_locus_stop"; // 停留点
	private static final String tableName_city = "dongfeng:gps_city_distribute"; // 车辆城市分布
	private static Configuration myConf = HBaseConfiguration.create();

	private static  Connection conn = null;// 获取连接
	
	static Table table_detail = null;
	static Table table_last = null;
	static Table table_stop = null;
	static Table table_city = null;
	
	public static void main(String[] args) throws IOException {
		
		myConf.set("hbase.zookeeper.quorum", "10.172.10.168,10.172.10.169,10.172.10.170"); // zkQuorum
		myConf.set("hbase.zookeeper.property.clientPort", "2181");
		
		conn = HBaseConnectionTool.getConnection(myConf);
		try {
			table_city = conn.getTable(TableName.valueOf(tableName_city));
		} catch (IOException e1) {
 			e1.printStackTrace();
		}
		Get get = new Get(Bytes.toBytes("010_P000000000000000"));
		Result res = null;
		try {
			res = table_city.get(get);
		} catch (IOException e) {
			e.printStackTrace();
		}
		Cell[] rawCells = res.rawCells();
		for (Cell cell : rawCells) {
			// rowKey
			// byte[] cloneRow = CellUtil.cloneRow(cell);
			// cell value
			byte[] cloneValue = CellUtil.cloneValue(cell);
			// cf
			// val cloneFamily = CellUtil.cloneFamily(cell);
			// col
			byte[] cloneQualifier = CellUtil.cloneQualifier(cell);
			System.out.println(Bytes.toString(cloneQualifier)+"====="+Bytes.toString(cloneValue));

		}
		try {
			table_last = conn.getTable(TableName.valueOf(tableName_last));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
	}
}
