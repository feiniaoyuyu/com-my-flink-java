package com.pateo.telematic.utils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.DecimalFormat;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

/**
 * 
 * 坐标转换使用说明 使用限制 http://lbs.amap.com/api/webservice/guide/api/convert/#scene
 * 
 * @author sh04595
 *
 */
public class CorrectGpsUtils {

	static Log logger = LogFactory.getLog(CorrectGpsUtils.class.getSimpleName());
	
	private static String baseUrl = "http://restapi.amap.com/v3/assistant/coordinate/convert";
	private static String key = "e351bfc5ceec6c86c4ef747bbd9f0497";

	private static String longitude = "+121.438819";
	private static String latitude = "+31.193799";
//	private static String longitude = "120.481469";
//	private static String latitude = "33.993475";
	private static double x_pi = 3.14159265358979324 * 3000.0 / 180.0;

	public static void main(String[] arg) {
		
		double beforeLong = Double.parseDouble(longitude);
		double beforeLat = Double.parseDouble(latitude);
		//+121.438819  +31.193799  金融保险服务;银行;中国工商银行
		//121.443432 , 31.191917 
		//121.449832 , 31.198273  
		System.out.println("-------------------------------through native method----------------------------------");
		// WGS-84 to GCJ-02
		Map<String, Double> gcj_encrypt = CordinateService.gcj_encrypt(beforeLat, beforeLong);
		System.out.println(" -------WGS-84 to GCJ-02 " + gcj_encrypt.get("lon")+ "|" + gcj_encrypt.get("lat"));

		double afterLong = gcj_encrypt.get("lon");
		double afterLat = gcj_encrypt.get("lat") ;
		Map<String, Double> bd_encrypt = CordinateService.bd_encrypt(afterLat,afterLong);
		System.out.println(" -------GCJ-02 to BD-09  "+ bd_encrypt.get("lon") +"|" + bd_encrypt.get("lat")  );
		 
		System.out.println("---------------------------------through  高德    api --------------------------------");
		System.out.println(" ------GCJ-02 to BD-09  " + gpsConvert(longitude, latitude));
 
//		-------------------------------through native method----------------------------------
//		 -------WGS-84 to GCJ-02 120.486198|33.992216
//		 -------GCJ-02 to BD-09  120.49262|33.998564
//		---------------------------------through  高德    api --------------------------------
//		------ WGS-84 to GCJ-02 120.486198|33.992216
//		 ------GCJ-02 to BD-09  120.492620|33.998564
	}

	public static String gpsConvert(String lng, String lat) {

		StringBuilder urlSB = new StringBuilder(
				"http://restapi.amap.com/v3/assistant/coordinate/convert?locations=");

		urlSB.append(lng + "," + lat);
		urlSB.append("&coordsys=gps&output=json&key=e351bfc5ceec6c86c4ef747bbd9f0497");
		
		String parseCoordinate = parseCoordinate(sendGet(urlSB.toString(),"utf-8"));
		System.out.println("------ WGS-84 to GCJ-02 " + parseCoordinate);
		return bd_encrypt(Double.valueOf(parseCoordinate.split("\\|")[0]),Double.valueOf(parseCoordinate.split("\\|")[1]));
	}

	/**
	 * 
	 * @param result
	 * @return lng|lat
	 */
	private static String parseCoordinate(String result) {

		String coordinate = "";
		if (StringUtils.isEmpty(result)) {
			return "http请求结果为空";
		}
		// {"status":"1","info":"ok","infocode":"10000","locations":"116.487586,39.991755"}
		JSONObject jsonObject = new JSONObject(result);
		try {
			if ("10000".equalsIgnoreCase(jsonObject.getString("infocode"))) {
				coordinate = jsonObject.getString("locations");
			} else {
				return "";
			}

		} catch (Exception e) {
			logger.error("----------------------------json:" + result);
			e.printStackTrace();
		}

//		logger.info("----------------------------result:" + result);

		StringBuilder string = new StringBuilder();

//		logger.info("----------------------------coordinate:" + coordinate);
		return string.append(coordinate.split("\\,")[0]).append("|")
				.append(coordinate.split("\\,")[1]).toString();
	}

	/**
	 * 使用Get方式获取数据
	 * 
	 * @param url
	 *            URL包括参数，http://HOST/XX?XX=XX&XXX=XXX
	 * @param charset
	 * @return
	 */
	private static String sendGet(String url, String charset) {

		// logger.info("----------before:"+url);
		url = url.replaceAll("\\+", "").replaceAll(" ", "");
//		logger.info("-----------after:" + url);
		String result = "";
		BufferedReader in = null;
		try {
			URL realUrl = new URL(url);
			// 打开和URL之间的连接
			HttpURLConnection connection = (HttpURLConnection) realUrl
					.openConnection();

			// 设置通用的请求属性
			connection.setRequestProperty("accept", "*/*");
			connection.setRequestProperty("connection", "Keep-Alive");
			connection.setRequestProperty("user-agent",
					"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
			// 建立实际的连接
			connection.connect();

			// 定义 BufferedReader输入流来读取URL的响应
			in = new BufferedReader(new InputStreamReader(
					connection.getInputStream(), charset));

			// String line;
			// 获取的json字符串没有 \n 或者是 \r 结尾的 不必要在进行拼接出错导致重复拼接

			String line;
			while ((line = in.readLine()) != null) {
				result += line;
			}
			connection.disconnect();

		} catch (Exception e) {
			e.printStackTrace();
		}
		// 使用finally块来关闭输入流
		finally {
			try {
				if (in != null) {
					in.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}

		return result;
	}

	/**
	 * 返回格式为 lng +","+lat
	 * 
	 * @param gg_lon
	 * @param gg_lat
	 * @return
	 */
	private static String bd_encrypt(double gg_lon, double gg_lat) {
		DecimalFormat format = new DecimalFormat("0.000000");

		double x = gg_lon;
		double y = gg_lat;
		double z = Math.sqrt(x * x + y * y) + 0.00002 * Math.sin(y * x_pi);
		double theta = Math.atan2(y, x) + 0.000003 * Math.cos(x * x_pi);
		double bd_lon = z * Math.cos(theta) + 0.0065;
		double bd_lat = z * Math.sin(theta) + 0.006;

		return format.format(bd_lon) + "|" + format.format(bd_lat);
	}

	// ======================================================
	static double pi = 3.14159265358979324;
	static double a = 6378245.0;
	static double ee = 0.00669342162296594323;

	public static double[] wgs2bd(double lat, double lon) {
		double[] wgs2gcj = wgs2gcj(lat, lon);
		double[] gcj2bd = gcj2bd(wgs2gcj[0], wgs2gcj[1]);
		return gcj2bd;
	}

	public static double[] gcj2bd(double lat, double lon) {
		double x = lon, y = lat;
		double z = Math.sqrt(x * x + y * y) + 0.00002 * Math.sin(y * x_pi);
		double theta = Math.atan2(y, x) + 0.000003 * Math.cos(x * x_pi);
		double bd_lon = z * Math.cos(theta) + 0.0065;
		double bd_lat = z * Math.sin(theta) + 0.006;
		return new double[] { bd_lat, bd_lon };
	}

	public static double[] bd2gcj(double lat, double lon) {
		double x = lon - 0.0065, y = lat - 0.006;
		double z = Math.sqrt(x * x + y * y) - 0.00002 * Math.sin(y * x_pi);
		double theta = Math.atan2(y, x) - 0.000003 * Math.cos(x * x_pi);
		double gg_lon = z * Math.cos(theta);
		double gg_lat = z * Math.sin(theta);
		return new double[] { gg_lat, gg_lon };
	}

	public static double[] wgs2gcj(double lat, double lon) {
		double dLat = transformLat(lon - 105.0, lat - 35.0);
		double dLon = transformLon(lon - 105.0, lat - 35.0);
		double radLat = lat / 180.0 * pi;
		double magic = Math.sin(radLat);
		magic = 1 - ee * magic * magic;
		double sqrtMagic = Math.sqrt(magic);
		dLat = (dLat * 180.0) / ((a * (1 - ee)) / (magic * sqrtMagic) * pi);
		dLon = (dLon * 180.0) / (a / sqrtMagic * Math.cos(radLat) * pi);
		double mgLat = lat + dLat;
		double mgLon = lon + dLon;
		double[] loc = { mgLat, mgLon };
		return loc;
	}

	private static double transformLat(double lat, double lon) {
		double ret = -100.0 + 2.0 * lat + 3.0 * lon + 0.2 * lon * lon + 0.1
				* lat * lon + 0.2 * Math.sqrt(Math.abs(lat));
		ret += (20.0 * Math.sin(6.0 * lat * pi) + 20.0 * Math.sin(2.0 * lat
				* pi)) * 2.0 / 3.0;
		ret += (20.0 * Math.sin(lon * pi) + 40.0 * Math.sin(lon / 3.0 * pi)) * 2.0 / 3.0;
		ret += (160.0 * Math.sin(lon / 12.0 * pi) + 320 * Math.sin(lon * pi
				/ 30.0)) * 2.0 / 3.0;
		return ret;
	}

	private static double transformLon(double lat, double lon) {
		double ret = 300.0 + lat + 2.0 * lon + 0.1 * lat * lat + 0.1 * lat
				* lon + 0.1 * Math.sqrt(Math.abs(lat));
		ret += (20.0 * Math.sin(6.0 * lat * pi) + 20.0 * Math.sin(2.0 * lat
				* pi)) * 2.0 / 3.0;
		ret += (20.0 * Math.sin(lat * pi) + 40.0 * Math.sin(lat / 3.0 * pi)) * 2.0 / 3.0;
		ret += (150.0 * Math.sin(lat / 12.0 * pi) + 300.0 * Math.sin(lat / 30.0
				* pi)) * 2.0 / 3.0;
		return ret;
	}

}
