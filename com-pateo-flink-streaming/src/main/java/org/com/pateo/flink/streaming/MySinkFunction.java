package org.com.pateo.flink.streaming;

import java.beans.Transient;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.pateo.df.utils.DistanceUtils;
import com.pateo.df.utils.HBaseConnectionTool;
import com.pateo.df.utils.PoiUtils;
import com.pateo.df.utils.WLContants;
import com.pateo.telematic.utils.StringUtils;
import com.pateo.telematic.utils.TimeUtils;

public class MySinkFunction extends RichSinkFunction<HashMap<String, String>> {

	private static final long serialVersionUID = 1L;
	 
	private static  Configuration  myConf = HBaseConfiguration.create();
 
//	private static final String tableName_detail = "gps_locus_point_detail"; // 轨迹点详情
//	// 包括经纬度poi信息
//	private static final String tableName_last = "gps_locus_point_last"; // 每条轨迹最后一个点
//	private static final String tableName_stop = "gps_locus_stop"; // 停留点
//	private static final String tableName_city = "gps_city_distribute"; // 车辆城市分布

	private static final String tableName_detail = "test:gps_locus_point_detail"; // 轨迹点详情
	// 包括经纬度poi信息
	private static final String tableName_last = "test:gps_locus_point_last"; // 每条轨迹最后一个点
	private static final String tableName_stop = "test:gps_locus_stop"; // 停留点
	private static final String tableName_city = "test:gps_city_distribute"; // 车辆城市分布
	
	private static  Connection conn = null; // 获取连接
 
	static Table table_detail = null;
	static Table table_last = null;
	static Table table_stop = null;
	static Table table_city = null;
	// 获取表
	
	List<Put> put_detail = new ArrayList<Put>(); //
	List<Put> put_last = new ArrayList<Put>(); //
	List<Put> put_stop = new ArrayList<Put>(); //
	List<Put> put_city = new ArrayList<Put>(); //
	
	@Override
	public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
 		
		myConf.set("hbase.zookeeper.quorum", "qing-zookeeper-srv1,qing-zookeeper-srv2,qing-zookeeper-srv3"); // zkQuorum
		myConf.set("hbase.zookeeper.property.clientPort", "21810");
		
		conn = HBaseConnectionTool.getConnection(myConf);
		try {
			table_detail = conn.getTable(TableName.valueOf(tableName_detail));
			table_last = conn.getTable(TableName.valueOf(tableName_last));
			table_stop = conn.getTable(TableName.valueOf(tableName_stop));
			table_city = conn.getTable(TableName.valueOf(tableName_city));
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.err.println( "table_city================"+table_city);
		System.err.println( "table_detail================"+table_detail);
		super.open(parameters);
	}

	@Override
	public void close() throws Exception {
		// TODO Auto-generated method stub
		// close table
		table_city.close();
		table_detail.close();
		table_last.close();
		table_stop.close();
		System.err.println( "table_city================"+table_city);
		System.err.println( "table_detail================"+table_detail);
 
		super.close();
	}
  
	// 获取保存的最近的一个gps点
	private static HashMap<String, String> getObdGpsLast(org.apache.hadoop.hbase.client.Table tableSou, String obd_id) {
		HashMap<String, String> map = new HashMap<String, String>();
		// 遍历获取cell内容
		if (!StringUtils.isEmpty(obd_id)) { // 如果obd_id为空就返回空map

			Get get = new Get(Bytes.toBytes(obd_id));
			Result res = null;
			try {
				res = tableSou.get(get);
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
				map.put(Bytes.toString(cloneQualifier), Bytes.toString(cloneValue));

			}
			map.put(WLContants.DEVICEID, obd_id);
		}
		return map;
	}

	// (obd_id,longitude,latitude,speed,engine_speed,gps_stat,client_time)
	private static Put map2PutWithNewKey(java.util.Map<String, String> p1, Boolean keyTime) {
		Put p = null;
		if (keyTime) { // gps time ms

			String client_time = null;
			if (p1.get(WLContants.GPSTIME).length() > 11) {
				client_time = p1.get(WLContants.GPSTIME).substring(0, 10);
			} else {
				client_time = p1.get(WLContants.GPSTIME);
			}
			p = new Put(Bytes.toBytes(p1.get(WLContants.DEVICEID) + "_" + client_time));
		} else {
			p = new Put(Bytes.toBytes(p1.get(WLContants.DEVICEID)));
		}

		Iterator<String> ks = p1.keySet().iterator();
		while (ks.hasNext()) {
			String key = ks.next();
			String value = p1.get(key);
			if (value == null || value.isEmpty() ) {
				value = "" ;
			}
			//System.out.println("key:"+key +"----value" +p1.getOrDefault(key, "---"));
			System.out.println("table_detail===================:" + table_detail);
			p.addColumn("f1".getBytes(), key.getBytes(), Bytes.toBytes(value));
		}
		return p;
	}

	// can be merge by flag
	private static Put map2Put(java.util.Map<String, String> p1) {
		Put p = new Put(Bytes.toBytes(p1.get(WLContants.DEVICEID)));
		Iterator<String> ks = p1.keySet().iterator();
		while (ks.hasNext()) {
			String key = ks.next();
			String value = p1.get(key);
			if (value == null || value.isEmpty() ) {
				value = "" ;
			}
			p.addColumn("f1".getBytes(), key.getBytes(), Bytes.toBytes(value));
		}
		return p;
	}

	/**
	 * 以城市code为key的组合键
	 */
	private static Put map2CityDistributePut(java.util.Map<String, String> p1) {
		String obd_id = p1.getOrDefault(WLContants.DEVICEID, "P000000000000000");
		String citycode = p1.getOrDefault(WLContants.CITYCODE, "000");
		Put p = new Put(Bytes.toBytes(citycode + "_" + obd_id));
		String lat = p1.getOrDefault(WLContants.BD_LAT, "40.000000");
		String lng = p1.getOrDefault(WLContants.BD_LON, "120.00000");

		String city = p1.getOrDefault(WLContants.CITY, "北京");
		String client_time = p1.getOrDefault(WLContants.GPSTIME, "2017-00-00 00:00:00");

		p.addColumn("f1".getBytes(), WLContants.BD_LAT.getBytes(), Bytes.toBytes(lat));
		p.addColumn("f1".getBytes(), WLContants.BD_LON.getBytes(), Bytes.toBytes(lng));
		p.addColumn("f1".getBytes(), WLContants.CITY.getBytes(), Bytes.toBytes(city));
		p.addColumn("f1".getBytes(), WLContants.CLIENT_TIME.getBytes(), Bytes.toBytes(client_time));
		return p;
	}

	public java.util.Map<String, String> getPoi(java.util.Map<String, String> pointMap) {
		String longitude_gd = pointMap.get(WLContants.GD_LON);
		String latitude_gd = pointMap.get(WLContants.GD_LAT);
		Map<String, String> poiMap = PoiUtils.getPoi(longitude_gd, latitude_gd);
		// retry batchtime times
		for (int i = 0; i < 3 && (poiMap.getOrDefault(WLContants.VALID_POI, "0") == "0"); i++) {
			poiMap = PoiUtils.getPoi(longitude_gd, latitude_gd);
			pointMap.putAll(poiMap);// 合并Map

		}
 		return pointMap;

	}

	//// 处理开始点的 经纬度 poi信息
	public HashMap<String, String> processStartPoint(HashMap<String, String> pointMap) {

		java.util.Map<String, String> poiMap = getPoi(pointMap);

		pointMap.putAll(poiMap); // 合并Map

		pointMap.put(WLContants.DISTANCE, "0"); // 当前点和last点的距离不计入新的轨迹中，因为设备可能存在异常移动
		pointMap.put(WLContants.S_TIME,
				pointMap.getOrDefault(WLContants.GPSTIME, System.currentTimeMillis() / 1000 + ""));
		pointMap.put(WLContants.S_BD_LAT, pointMap.get(WLContants.BD_LAT));
		pointMap.put(WLContants.S_BD_LON, pointMap.get(WLContants.BD_LON));
		pointMap.put(WLContants.S_GD_LAT, pointMap.get(WLContants.GD_LAT));
		pointMap.put(WLContants.S_GD_LON, pointMap.get(WLContants.GD_LON));
		pointMap.put(WLContants.S_PROVINCE, pointMap.get(WLContants.PROVINCE));
		pointMap.put(WLContants.S_CITY, pointMap.get(WLContants.CITY));
		pointMap.put(WLContants.S_NAME, pointMap.get(WLContants.NAME));

		return pointMap;
	}

	@Override
	public void invoke(HashMap<String, String> PointMap) throws Exception {

		String client_time = TimeUtils.secondsToDate(PointMap.get(WLContants.GPSTIME).substring(0, 10), null);
		PointMap.put(WLContants.CLIENT_TIME, client_time);
		// 4. 将接收到的数据 插入到 locus_point_detail表中
		put_detail.add(map2PutWithNewKey(PointMap, true));
		// 3. 插入city表中

		put_city.add(map2CityDistributePut(PointMap));

		// 1. 得到的每个点 从 gps_locus_point_last 进行查询是否存在历史最新点
		HashMap<String, String> lastPointMap = getObdGpsLast(table_last, PointMap.get(WLContants.DEVICEID));
		System.out.println(" ==================================");
		if (lastPointMap.size() < 8) { // 1.1 this place depend on the date
			// field
			// length
			PointMap = processStartPoint(PointMap);// 1.1.1 never appear and
													// regard it as
			// locus start point
			Put put = map2PutWithNewKey(PointMap, false); // insert to last
															// table and
			// process as start point, false mean do not need time as part of
			// key
			put_last.add(put);
			System.out.println("\n---------first apperar :\n" + PointMap);
		} else { // 1.2 说明存在历史点

			// 2.0 get
			Integer timeDiff = 50;
			try {
				// unit:ms
				timeDiff = (Integer.valueOf(PointMap.get(WLContants.GPSTIME))
						- Integer.valueOf(lastPointMap.get(WLContants.GPSTIME)));
				// logger.info(" ---------timeDiff:" + timeDiff + "=" +
				// PointMap.get(WLContants.GPSTIME) + "--" +
				// lastPointMap.get(WLContants.GPSTIME))
			} catch (Exception e) {
				// t.printStackTrace();
				// logger.info("PointMap===========" + PointMap + "\n
				// lastPointMap-----" + lastPointMap);
			}

			if (timeDiff > 5 * 60) { // 2.1 if timeDiff >5m,then mark point from
				// last table as locus stop point,and regard the current point
				// as new locus
				// start point
				// 2.1.1 process the gps_last point and insert into stop table
				Put stop = map2PutWithNewKey(lastPointMap, true);
				put_stop.add(stop);

				// 2.1.2 拿出当前点作为 新的行程入库
				// process start point
				PointMap = processStartPoint(PointMap);

				// insert to last table
				put_last.add(map2Put(PointMap));

			} else if (timeDiff > 0) {

				// 2.2 时间差小于300秒则认为是正常的行驶轨迹点，update distance and current point
				// info to last table
				// update distance
				// 2.2.1 get distance
				String distance = DistanceUtils.getDistance(PointMap.get(WLContants.GD_LAT),
						PointMap.get(WLContants.GD_LON), lastPointMap.get(WLContants.GD_LAT),
						lastPointMap.get(WLContants.GD_LON));

				if (distance.isEmpty()) {
					distance = "0";
				} else if (Float.valueOf(distance) > 1000) {
					distance = "1000";
				} else {

				}

				// 2.2.2 update to last talbe
				PointMap.put(WLContants.DISTANCE,
						(Float.valueOf(distance) + Float.valueOf(lastPointMap.get(WLContants.DISTANCE))) + "");

				// logger.warn("distance===========" + distance + "---
				// accumulate" + PointMap.get(WLContants.DISTANCE));

				Put put = map2Put(PointMap);
				put_last.add(put);

			} else { // diffTime < 0 do not process the point

			}

		}

		if (put_detail.size() > 500) {
			table_detail.put(put_detail);
			put_detail = new ArrayList<Put>(); //
		}
		if (put_last.size() > 500) {
			table_last.put(put_last);
			put_last = new ArrayList<Put>(); //
		}
		if (put_stop.size() > 500) {
			table_stop.put(put_stop);
			put_stop = new ArrayList<Put>(); //
		}
		if (put_city.size() > 500) {
			table_city.put(put_city);
			put_city = new ArrayList<Put>(); //
		}

		// write to hbase
		if (put_detail.size() > 0) {
			table_detail.put(put_detail);
		}
		if (put_last.size() > 0) {
			table_last.put(put_last);
		}
		if (put_stop.size() > 0) {
			table_stop.put(put_stop);
		}
		if (put_city.size() > 0) {
			table_city.put(put_city);
		}

	}

}
