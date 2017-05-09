package com.pateo.df.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class HBaseConnectionTool {

	private static Connection instance = null;

	public static Connection getConnection(org.apache.hadoop.conf.Configuration conf) {
		if (instance == null) {

			try {
				if (conf == null) {
					Configuration configuration = HBaseConfiguration.create();
					// TODO 访问的时候需要配置host文件
					configuration.set("hbase.zookeeper.quorum", "wluat-hbase-srv4,wluat-hbase-srv3,wluat-hbase-srv2");
					configuration.set("hbase.zookeeper.property.clientPort", "2181");

					instance = ConnectionFactory.createConnection(configuration);
				} else {
					instance = ConnectionFactory.createConnection(conf);
				}

			} catch (Exception e) {

				System.err.println("==========get connection error");
				e.printStackTrace();

			}
		}
		return instance;
	}

}
