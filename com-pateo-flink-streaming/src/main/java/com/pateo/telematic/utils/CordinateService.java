package com.pateo.telematic.utils;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//WGS-84：是国际标准，GPS坐标（Google Earth使用、或者GPS模块）
//GCJ-02：中国坐标偏移标准，Google Map、高德、腾讯使用
//BD-09：百度坐标偏移标准，Baidu Map使用
public class CordinateService implements Serializable{
	 
	private static final long serialVersionUID = 1L;

	static Logger logger = LoggerFactory.getLogger("CordinateServiceLogger");

	static DecimalFormat format = new DecimalFormat("0.000000");

	public static String Lat = "lat";
	public static String Lon = "lon";

	private static double PI = 3.14159265358979324;
	private static double x_pi = PI * 3000.0 / 180.0;
	private static double a = 6378245.0;// a: 卫星椭球坐标投影到平面地图坐标系的投影因子。
	private static double ee = 0.00669342162296594323;// ee: 椭球的偏心率。
	
	/**
	 * 从gps点到高德坐标转换
	 * @param wgslat
	 * @param wgsLon
	 * @return
	 */
	public static Map<String, Double> gcj_encrypt(double wgslat, double wgsLon) {
		
		if (outOfChina(wgslat, wgsLon)) {
			Map<String, Double> map = new HashMap<String, Double>();

			map.put(Lat, wgslat);
			map.put(Lon, wgsLon);
			return map;
		}

		Map<String, Double> map = delta(wgslat, wgsLon);
		wgslat += map.get(Lat);
		wgsLon += map.get(Lon);

		map.put(Lat, Double.valueOf(format.format(wgslat)));
		map.put(Lon, Double.valueOf(format.format(wgsLon)));
		return map;
	}

	private static Boolean outOfChina(double lat, double lon) {
		if (lon < 72.004 || lon > 137.8347)
			return true;
		if (lat < 0.8293 || lat > 55.8271)
			return true;
		return false;
	}

	private static Map<String, Double> delta(double lat, double lon) {
		// Krasovsky 1940
		// a = 6378245.0, 1/f = 298.3
		// b = a * (1 - f)
		// ee = (a^2 - b^2) / a^2;
	
		
		double dLat = transformLat(lon - 105.0, lat - 35.0);
		double dLon = transformLon(lon - 105.0, lat - 35.0);
		double radLat = lat / 180.0 * PI;
		double magic = Math.sin(radLat);
		magic = 1 - ee * magic * magic;
		double sqrtMagic = Math.sqrt(magic);
		dLat = (dLat * 180.0) / ((a * (1 - ee)) / (magic * sqrtMagic) * PI);
		dLon = (dLon * 180.0) / (a / sqrtMagic * Math.cos(radLat) * PI);
		Map<String, Double> map = new HashMap<String, Double>(3);
		map.put(Lat, dLat);
		map.put(Lon, dLon);
		return map;
	}

	private static double transformLat(double x, double y) {
		double ret = -100.0 + 2.0 * x + 3.0 * y + 0.2 * y * y + 0.1 * x * y
				+ 0.2 * Math.sqrt(Math.abs(x));
		ret += (20.0 * Math.sin(6.0 * x * PI) + 20.0 * Math.sin(2.0 * x * PI)) * 2.0 / 3.0;
		ret += (20.0 * Math.sin(y * PI) + 40.0 * Math.sin(y / 3.0 * PI)) * 2.0 / 3.0;
		ret += (160.0 * Math.sin(y / 12.0 * PI) + 320 * Math.sin(y * PI / 30.0)) * 2.0 / 3.0;
		return ret;
	}

	private static double transformLon(double x, double y) {
		double ret = 300.0 + x + 2.0 * y + 0.1 * x * x + 0.1 * x * y + 0.1
				* Math.sqrt(Math.abs(x));
		ret += (20.0 * Math.sin(6.0 * x * PI) + 20.0 * Math.sin(2.0 * x * PI)) * 2.0 / 3.0;
		ret += (20.0 * Math.sin(x * PI) + 40.0 * Math.sin(x / 3.0 * PI)) * 2.0 / 3.0;
		ret += (150.0 * Math.sin(x / 12.0 * PI) + 300.0 * Math.sin(x / 30.0
				* PI)) * 2.0 / 3.0;
		return ret;
	}

	/**
	 * GCJ-02 to BD-09 bd_encrypt();
	 *   高德 到百度
	 * @param gcjLat
	 * @param gcjLon
	 * @return
	 */
	public static Map<String, Double> bd_encrypt(double gcjLat, double gcjLon) {
		double x = gcjLon;
		double y = gcjLat;
		double z = Math.sqrt(x * x + y * y) + 0.00002 * Math.sin(y * x_pi);
		double theta = Math.atan2(y, x) + 0.000003 * Math.cos(x * x_pi);
		double bdLon = z * Math.cos(theta) + 0.0065;
		double bdLat = z * Math.sin(theta) + 0.006;
		Map<String, Double> map = new HashMap<String, Double>(3);
		map.put(Lat, Double.valueOf(format.format(bdLat)));
		map.put(Lon, Double.valueOf(format.format(bdLon)));
		return map;
	}

//	private static String latitude = "39.993475";
//	private static String longitude = "116.481469";
	
	private static String latitude = "31.179373";
	private static String longitude = "121.419106";
 
	
	/**
	 * from gps  -> gc -> bd  从gps到高德再到百度
	 * @param wgsLon
	 * @param wgslat
	 * @return  Lon +|+ Lat
	 */
	public static String gpsConvert(String wgsLon, String wgslat ) {
		
		Map<String, Double> gcj_encrypt  = gcj_encrypt(Double.valueOf(wgslat), Double.valueOf(wgsLon));
//		logger.info(" -------WGS-84 to GCJ-02 " + gcj_encrypt.get(Lon) + "|" + gcj_encrypt.get(Lat));

//		double afterLong = gcj_encrypt.get(Lat);
//		double afterLat = gcj_encrypt.get(Lon);
		Map<String, Double> bd_encrypt = bd_encrypt(gcj_encrypt.get(Lat), gcj_encrypt.get(Lon));
//		logger.info(" -------GCJ-02 to BD-09 " + bd_encrypt.get(Lon) + "|" + bd_encrypt.get(Lat));
		return  bd_encrypt.get(Lon) + "|" + bd_encrypt.get(Lat) ;
	}
	public static void main(String[] args) {
		
//		+119.383619     +25.973577      
//		119.394995      25.976982
//		119.394995		25.976982
		System.out.println("-----gpsConvert----"+CordinateService.gpsConvert("119.383619","25.973577"));
		double beforeLong = Double.parseDouble(longitude);
		double beforeLat = Double.parseDouble(latitude);
		//1. WGS-84 to GCJ-02 从gps 到 纠偏
		Map<String, Double> gcj_encrypt = CordinateService.gcj_encrypt(beforeLat, beforeLong);
		logger.info(" -------WGS-84 to GCJ-02 " + gcj_encrypt.get(Lon)+ "|" + gcj_encrypt.get(Lat));

		//2. GCJ-02 to BD-09 : 116.494090|40.000717 从纠偏到百度
		double afterLong = gcj_encrypt.get(Lon);
		double afterLat = gcj_encrypt.get(Lat);
		
		Map<String, Double> bd_encrypt = bd_encrypt(afterLat, afterLong);
		logger.info(" -------GCJ-02 to BD-09 " + bd_encrypt.get(Lon) + "|" + bd_encrypt.get(Lat));

		logger.info("-------------gpsConvert " + gpsConvert(longitude, latitude));
		// -------WGS-84 to GCJ-02 116.487556|39.994754
		// -------GCJ-02 to BD-09 116.49409|40.000717

		//GCJ-02 to WGS-84 粗略
		//  gcj_decrypt();
		//
		 //GCJ-02 to WGS-84 精确(二分极限法)
		 // var threshold = 0.000000001;
		// 目前设置的是精确到小数点后9位，这个值越小，越精确，但是javascript中，浮点运算本身就不太精确，九位在GPS里也偏差不大了
		//  gcj_decrypt_exact();
		//
		// //GCJ-02 to BD-09
		//  bd_encrypt();
		//
		// //BD-09 to GCJ-02
		// bd_decrypt();
		//
		// //求距离
		// distance();

	}
	// 示例：
	// document.write("GPS: 39.933676862706776,116.35608315379092<br />");
	// var arr2 = GPS.gcj_encrypt(39.933676862706776, 116.35608315379092);
	// document.write("中国:" + arr2['lat']+","+arr2['lon']+'<br />');
	// var arr3 = GPS.gcj_decrypt_exact(arr2['lat'], arr2['lon']);
	// document.write('逆算:' + arr3['lat']+","+arr3['lon']+'
	// 需要和第一行相似（目前是小数点后9位相等）');
	// 本算法 gcj算法、bd算法都非常精确，已经测试通过。（BD转换的结果和百度提供的接口精确到小数点后4位）

	// var GPS = {
	// PI : 3.14159265358979324,
	// x_pi : 3.14159265358979324 * 3000.0 / 180.0,
	// delta : function (lat, lon) {
	// // Krasovsky 1940
	// //
	// // a = 6378245.0, 1/f = 298.3
	// // b = a * (1 - f)
	// // ee = (a^2 - b^2) / a^2;
	// var a = 6378245.0; // a: 卫星椭球坐标投影到平面地图坐标系的投影因子。
	// var ee = 0.00669342162296594323; // ee: 椭球的偏心率。
	// var dLat = this.transformLat(lon - 105.0, lat - 35.0);
	// var dLon = this.transformLon(lon - 105.0, lat - 35.0);
	// var radLat = lat / 180.0 * PI;
	// var magic = Math.sin(radLat);
	// magic = 1 - ee * magic * magic;
	// var sqrtMagic = Math.sqrt(magic);
	// dLat = (dLat * 180.0) / ((a * (1 - ee)) / (magic * sqrtMagic) * PI);
	// dLon = (dLon * 180.0) / (a / sqrtMagic * Math.cos(radLat) * PI);
	// return {'lat': dLat, 'lon': dLon};
	// },
	//
	// //WGS-84 to GCJ-02
	// gcj_encrypt : function (wgsLat, wgsLon) {
	// if (this.outOfChina(wgsLat, wgsLon))
	// return {'lat': wgsLat, 'lon': wgsLon};
	//
	// var d = this.delta(wgsLat, wgsLon);
	// return {'lat' : wgsLat + d.lat,'lon' : wgsLon + d.lon};
	// },
	// //GCJ-02 to WGS-84
	// gcj_decrypt : function (gcjLat, gcjLon) {
	// if (this.outOfChina(gcjLat, gcjLon))
	// return {'lat': gcjLat, 'lon': gcjLon};
	//
	// var d = this.delta(gcjLat, gcjLon);
	// return {'lat': gcjLat - d.lat, 'lon': gcjLon - d.lon};
	// },
	// //GCJ-02 to WGS-84 exactly
	// gcj_decrypt_exact : function (gcjLat, gcjLon) {
	// var initDelta = 0.01;
	// var threshold = 0.000000001;
	// var dLat = initDelta, dLon = initDelta;
	// var mLat = gcjLat - dLat, mLon = gcjLon - dLon;
	// var pLat = gcjLat + dLat, pLon = gcjLon + dLon;
	// var wgsLat, wgsLon, i = 0;
	// while (1) {
	// wgsLat = (mLat + pLat) / 2;
	// wgsLon = (mLon + pLon) / 2;
	// var tmp = this.gcj_encrypt(wgsLat, wgsLon)
	// dLat = tmp.lat - gcjLat;
	// dLon = tmp.lon - gcjLon;
	// if ((Math.abs(dLat) < threshold) && (Math.abs(dLon) < threshold))
	// break;
	//
	// if (dLat > 0) pLat = wgsLat; else mLat = wgsLat;
	// if (dLon > 0) pLon = wgsLon; else mLon = wgsLon;
	//
	// if (++i > 10000) break;
	// }
	// //console.log(i);
	// return {'lat': wgsLat, 'lon': wgsLon};
	// },

	// //GCJ-02 to BD-09
	// bd_encrypt : function (gcjLat, gcjLon) {
	// var x = gcjLon, y = gcjLat;
	// var z = Math.sqrt(x * x + y * y) + 0.00002 * Math.sin(y * this.x_pi);
	// var theta = Math.atan2(y, x) + 0.000003 * Math.cos(x * this.x_pi);
	// bdLon = z * Math.cos(theta) + 0.0065;
	// bdLat = z * Math.sin(theta) + 0.006;
	// return {'lat' : bdLat,'lon' : bdLon};
	// },
	// //BD-09 to GCJ-02
	// bd_decrypt : function (bdLat, bdLon) {
	// var x = bdLon - 0.0065, y = bdLat - 0.006;
	// var z = Math.sqrt(x * x + y * y) - 0.00002 * Math.sin(y * this.x_pi);
	// var theta = Math.atan2(y, x) - 0.000003 * Math.cos(x * this.x_pi);
	// var gcjLon = z * Math.cos(theta);
	// var gcjLat = z * Math.sin(theta);
	// return {'lat' : gcjLat, 'lon' : gcjLon};
	// },
	// //WGS-84 to Web mercator
	// //mercatorLat -> y mercatorLon -> x
	// mercator_encrypt : function(wgsLat, wgsLon) {
	// var x = wgsLon * 20037508.34 / 180.;
	// var y = Math.log(Math.tan((90. + wgsLat) * PI / 360.)) / (PI / 180.);
	// y = y * 20037508.34 / 180.;
	// return {'lat' : y, 'lon' : x};
	// /*
	// if ((Math.abs(wgsLon) > 180 || Math.abs(wgsLat) > 90))
	// return null;
	// var x = 6378137.0 * wgsLon * 0.017453292519943295;
	// var a = wgsLat * 0.017453292519943295;
	// var y = 3189068.5 * Math.log((1.0 + Math.sin(a)) / (1.0 - Math.sin(a)));
	// return {'lat' : y, 'lon' : x};
	// //*/
	// },
	// // Web mercator to WGS-84
	// // mercatorLat -> y mercatorLon -> x
	// mercator_decrypt : function(mercatorLat, mercatorLon) {
	// var x = mercatorLon / 20037508.34 * 180.;
	// var y = mercatorLat / 20037508.34 * 180.;
	// y = 180 / PI * (2 * Math.atan(Math.exp(y * PI / 180.)) - PI / 2);
	// return {'lat' : y, 'lon' : x};
	// /*
	// if (Math.abs(mercatorLon) < 180 && Math.abs(mercatorLat) < 90)
	// return null;
	// if ((Math.abs(mercatorLon) > 20037508.3427892) || (Math.abs(mercatorLat)
	// > 20037508.3427892))
	// return null;
	// var a = mercatorLon / 6378137.0 * 57.295779513082323;
	// var x = a - (Math.floor(((a + 180.0) / 360.0)) * 360.0);
	// var y = (1.5707963267948966 - (2.0 * Math.atan(Math.exp((-1.0 *
	// mercatorLat) / 6378137.0)))) * 57.295779513082323;
	// return {'lat' : y, 'lon' : x};
	// //*/
	// },
	// // two point's distance
	// distance : function (latA, lonA, latB, lonB) {
	// var earthR = 6371000.;
	// var x = Math.cos(latA * PI / 180.) * Math.cos(latB * PI / 180.) *
	// Math.cos((lonA - lonB) * PI / 180);
	// var y = Math.sin(latA * PI / 180.) * Math.sin(latB * PI / 180.);
	// var s = x + y;
	// if (s > 1) s = 1;
	// if (s < -1) s = -1;
	// var alpha = Math.acos(s);
	// var distance = alpha * earthR;
	// return distance;
	// },
	// outOfChina : function (lat, lon) {
	// if (lon < 72.004 || lon > 137.8347)
	// return true;
	// if (lat < 0.8293 || lat > 55.8271)
	// return true;
	// return false;
	// },
	// transformLat : function (x, y) {
	// var ret = -100.0 + 2.0 * x + 3.0 * y + 0.2 * y * y + 0.1 * x * y + 0.2 *
	// Math.sqrt(Math.abs(x));
	// ret += (20.0 * Math.sin(6.0 * x * PI) + 20.0 * Math.sin(2.0 * x * PI)) *
	// 2.0 / 3.0;
	// ret += (20.0 * Math.sin(y * PI) + 40.0 * Math.sin(y / 3.0 * PI)) * 2.0 /
	// 3.0;
	// ret += (160.0 * Math.sin(y / 12.0 * PI) + 320 * Math.sin(y * PI / 30.0))
	// * 2.0 / 3.0;
	// return ret;
	// },
	// transformLon : function (x, y) {
	// var ret = 300.0 + x + 2.0 * y + 0.1 * x * x + 0.1 * x * y + 0.1 *
	// Math.sqrt(Math.abs(x));
	// ret += (20.0 * Math.sin(6.0 * x * PI) + 20.0 * Math.sin(2.0 * x * PI)) *
	// 2.0 / 3.0;
	// ret += (20.0 * Math.sin(x * PI) + 40.0 * Math.sin(x / 3.0 * PI)) * 2.0 /
	// 3.0;
	// ret += (150.0 * Math.sin(x / 12.0 * PI) + 300.0 * Math.sin(x / 30.0 *
	// PI)) * 2.0 / 3.0;
	// return ret;
	// }
	// };
	// <?php
	// class GPS {
	// private $PI = 3.14159265358979324;
	// private $x_pi = 0;
	//
	// public function __construct()
	// {
	// $this->x_pi = 3.14159265358979324 * 3000.0 / 180.0;
	// }
	// //WGS-84 to GCJ-02
	// public function gcj_encrypt(wgslat, wgsLon) {
	// if ($this->outOfChina(wgslat, wgsLon))
	// return array('lat' => wgslat, 'lon' => wgsLon);
	//
	// $d = $this->delta(wgslat, wgsLon);
	// return array('lat' => wgslat + $d['lat'],'lon' => wgsLon + $d['lon']);
	// }
	// //GCJ-02 to WGS-84
	// public function gcj_decrypt($gcjLat, $gcjLon) {
	// if ($this->outOfChina($gcjLat, $gcjLon))
	// return array('lat' => $gcjLat, 'lon' => $gcjLon);
	//
	// $d = $this->delta($gcjLat, $gcjLon);
	// return array('lat' => $gcjLat - $d['lat'], 'lon' => $gcjLon - $d['lon']);
	// }
	// //GCJ-02 to WGS-84 exactly
	// public function gcj_decrypt_exact($gcjLat, $gcjLon) {
	// $initDelta = 0.01;
	// $threshold = 0.000000001;
	// $dLat = $initDelta; $dLon = $initDelta;
	// $mLat = $gcjLat - $dLat; $mLon = $gcjLon - $dLon;
	// $pLat = $gcjLat + $dLat; $pLon = $gcjLon + $dLon;
	// wgslat = 0; wgsLon = 0; $i = 0;
	// while (TRUE) {
	// wgslat = ($mLat + $pLat) / 2;
	// wgsLon = ($mLon + $pLon) / 2;
	// $tmp = $this->gcj_encrypt(wgslat, wgsLon);
	// $dLat = $tmp['lat'] - $gcjLat;
	// $dLon = $tmp['lon'] - $gcjLon;
	// if ((abs($dLat) < $threshold) && (abs($dLon) < $threshold))
	// break;
	//
	// if ($dLat > 0) $pLat = wgslat; else $mLat = wgslat;
	// if ($dLon > 0) $pLon = wgsLon; else $mLon = wgsLon;
	//
	// if (++$i > 10000) break;
	// }
	// //console.log(i);
	// return array('lat' => wgslat, 'lon'=> wgsLon);
	// }
	// //GCJ-02 to BD-09
	// public function bd_encrypt($gcjLat, $gcjLon) {
	// $x = $gcjLon; $y = $gcjLat;
	// $z = sqrt($x * $x + $y * $y) + 0.00002 * sin($y * $this->x_pi);
	// $theta = atan2($y, $x) + 0.000003 * cos($x * $this->x_pi);
	// $bdLon = $z * cos($theta) + 0.0065;
	// $bdLat = $z * sin($theta) + 0.006;
	// return array('lat' => $bdLat,'lon' => $bdLon);
	// }
	// //BD-09 to GCJ-02
	// public function bd_decrypt($bdLat, $bdLon)
	// {
	// $x = $bdLon - 0.0065; $y = $bdLat - 0.006;
	// $z = sqrt($x * $x + $y * $y) - 0.00002 * sin($y * $this->x_pi);
	// $theta = atan2($y, $x) - 0.000003 * cos($x * $this->x_pi);
	// $gcjLon = $z * cos($theta);
	// $gcjLat = $z * sin($theta);
	// return array('lat' => $gcjLat, 'lon' => $gcjLon);
	// }
	// //WGS-84 to Web mercator
	// //$mercatorLat -> y $mercatorLon -> x
	// public function mercator_encrypt(wgslat, wgsLon)
	// {
	// $x = wgsLon * 20037508.34 / 180.;
	// $y = log(tan((90. + wgslat) * $this->PI / 360.)) / ($this->PI / 180.);
	// $y = $y * 20037508.34 / 180.;
	// return array('lat' => $y, 'lon' => $x);
	// /*
	// if ((abs(wgsLon) > 180 || abs(wgslat) > 90))
	// return NULL;
	// $x = 6378137.0 * wgsLon * 0.017453292519943295;
	// $a = wgslat * 0.017453292519943295;
	// $y = 3189068.5 * log((1.0 + sin($a)) / (1.0 - sin($a)));
	// return array('lat' => $y, 'lon' => $x);
	// //*/
	// }
	// // Web mercator to WGS-84
	// // $mercatorLat -> y $mercatorLon -> x
	// public function mercator_decrypt($mercatorLat, $mercatorLon)
	// {
	// $x = $mercatorLon / 20037508.34 * 180.;
	// $y = $mercatorLat / 20037508.34 * 180.;
	// $y = 180 / $this->PI * (2 * atan(exp($y * $this->PI / 180.)) - $this->PI
	// / 2);
	// return array('lat' => $y, 'lon' => $x);
	// /*
	// if (abs($mercatorLon) < 180 && abs($mercatorLat) < 90)
	// return NULL;
	// if ((abs($mercatorLon) > 20037508.3427892) || (abs($mercatorLat) >
	// 20037508.3427892))
	// return NULL;
	// $a = $mercatorLon / 6378137.0 * 57.295779513082323;
	// $x = $a - (floor((($a + 180.0) / 360.0)) * 360.0);
	// $y = (1.5707963267948966 - (2.0 * atan(exp((-1.0 * $mercatorLat) /
	// 6378137.0)))) * 57.295779513082323;
	// return array('lat' => $y, 'lon' => $x);
	// //*/
	// }
	// // two point's distance
	// public function distance($latA, $lonA, $latB, $lonB)
	// {
	// $earthR = 6371000.;
	// $x = cos($latA * $this->PI / 180.) * cos($latB * $this->PI / 180.) *
	// cos(($lonA - $lonB) * $this->PI / 180);
	// $y = sin($latA * $this->PI / 180.) * sin($latB * $this->PI / 180.);
	// $s = $x + $y;
	// if ($s > 1) $s = 1;
	// if ($s < -1) $s = -1;
	// $alpha = acos($s);
	// $distance = $alpha * $earthR;
	// return $distance;
	// }
	//
	// private function delta($lat, $lon)
	// {
	// // Krasovsky 1940
	// //
	// // a = 6378245.0, 1/f = 298.3
	// // b = a * (1 - f)
	// // ee = (a^2 - b^2) / a^2;
	// $a = 6378245.0;// a: 卫星椭球坐标投影到平面地图坐标系的投影因子。
	// $ee = 0.00669342162296594323;// ee: 椭球的偏心率。
	// $dLat = $this->transformLat($lon - 105.0, $lat - 35.0);
	// $dLon = $this->transformLon($lon - 105.0, $lat - 35.0);
	// $radLat = $lat / 180.0 * $this->PI;
	// $magic = sin($radLat);
	// $magic = 1 - $ee * $magic * $magic;
	// $sqrtMagic = sqrt($magic);
	// $dLat = ($dLat * 180.0) / (($a * (1 - $ee)) / ($magic * $sqrtMagic) *
	// $this->PI);
	// $dLon = ($dLon * 180.0) / ($a / $sqrtMagic * cos($radLat) * $this->PI);
	// return array('lat' => $dLat, 'lon' => $dLon);
	// }
	//
	// private function outOfChina($lat, $lon)
	// {
	// if ($lon < 72.004 || $lon > 137.8347)
	// return TRUE;
	// if ($lat < 0.8293 || $lat > 55.8271)
	// return TRUE;
	// return FALSE;
	// }
	//
	// private function transformLat($x, $y) {
	// $ret = -100.0 + 2.0 * $x + 3.0 * $y + 0.2 * $y * $y + 0.1 * $x * $y + 0.2
	// * sqrt(abs($x));
	// $ret += (20.0 * sin(6.0 * $x * $this->PI) + 20.0 * sin(2.0 * $x *
	// $this->PI)) * 2.0 / 3.0;
	// $ret += (20.0 * sin($y * $this->PI) + 40.0 * sin($y / 3.0 * $this->PI)) *
	// 2.0 / 3.0;
	// $ret += (160.0 * sin($y / 12.0 * $this->PI) + 320 * sin($y * $this->PI /
	// 30.0)) * 2.0 / 3.0;
	// return $ret;
	// }
	//
	// private function transformLon($x, $y) {
	// $ret = 300.0 + $x + 2.0 * $y + 0.1 * $x * $x + 0.1 * $x * $y + 0.1 *
	// sqrt(abs($x));
	// $ret += (20.0 * sin(6.0 * $x * $this->PI) + 20.0 * sin(2.0 * $x *
	// $this->PI)) * 2.0 / 3.0;
	// $ret += (20.0 * sin($x * $this->PI) + 40.0 * sin($x / 3.0 * $this->PI)) *
	// 2.0 / 3.0;
	// $ret += (150.0 * sin($x / 12.0 * $this->PI) + 300.0 * sin($x / 30.0 *
	// $this->PI)) * 2.0 / 3.0;
	// return $ret;
	// }

}
