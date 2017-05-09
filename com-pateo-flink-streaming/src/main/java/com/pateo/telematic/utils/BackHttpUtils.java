package com.pateo.telematic.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
 
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pateo.telematic.utils.GeoHash;

/**
 * 请求 地理反编码 高德 api $key = "e351bfc5ceec6c86c4ef747bbd9f0497"; $url =
 * "http://restapi.amap.com/v3/geocode/regeo?location=$long,$lat&key=$key&radius=1000&extensions=all"
 * ;
 * 
 * 百度地图api
 * http://api.map.baidu.com/geocoder/v2/?ak=uXd1sQo9pvTXTOUeSmoAjy6b&location
 * =33.926468,116.790848&output=json
 *
 * 矫正 API 高德 http://restapi.amap.com/v3/assistant/coordinate/convert?locations=
 * 116.481499
 * ,39.990475&coordsys=gps&output=json&key=e351bfc5ceec6c86c4ef747bbd9f0497
 *
 */
public class BackHttpUtils {

	static Logger logger = LoggerFactory.getLogger(BackHttpUtils.class);

	/**
	 * 
	 * @param lat
	 * @param lng
	 * @return lng + lat
	 */
	public static String gpsCorrect(String lat, String lng) {
		// logger.error("------------------------lat:" + lat + "-lng:" + lng +
		// "-------------------");
		// 校验地址 是否为空
		if (StringUtils.isEmpty(lat) || StringUtils.isEmpty(lng)) {
			return "经纬度为空";
		}
		String url = "http://restapi.amap.com/v3/assistant/coordinate/convert?locations="
				+ lng
				+ ","
				+ lat
				+ "&coordsys=gps&output=json&key=e351bfc5ceec6c86c4ef747bbd9f0497";
		// StringBuilder urlSB =new
		// StringBuilder("http://restapi.amap.com/v3/geocode/regeo?location="+lng+
		// ","+lat+"&key=e351bfc5ceec6c86c4ef747bbd9f0497&radius=1000&extensions=all")
		// ;
		return parseCoordinate(sendGet(url, "utf-8"));
	}

	/**
	 * 先进行 gps的纠偏操作
	 * 
	 * @param lat
	 * @param lng
	 * @return
	 */
	public static String getAddress(String lat, String lng) {
		// 1. 校验地址 是否为空
		if (StringUtils.isEmpty(lat) || StringUtils.isEmpty(lng)) {
			return "经纬度为空";
		}
		if (Float.parseFloat(lat) == 0 || Float.parseFloat(lng) == 0) {
			return "经纬度为零";
		}
		System.out.println("-----------original   " + lat + " | " + lng);
		// 1. WGS-84 to GCJ-02 从gps 到 纠偏
		Map<String, Double> gcj_encrypt = CordinateService.gcj_encrypt(
				Double.parseDouble(lat), Double.parseDouble(lng));
		System.out.println("-----------native "
				+ gcj_encrypt.get(CordinateService.Lat) + "|"
				+ gcj_encrypt.get(CordinateService.Lon));

		// // 2. 进行经纬度的矫正 网络
		// String convertString = gpsCorrect(lat, lng);
		// lat = convertString.split("\\|")[1] ;
		// lng = convertString.split("\\|")[0] ;
		// System.out.println("-----------api   " +lat + " | " +lng);

		// 3. 请求API获取 name
		StringBuilder urlSB = new StringBuilder(
				"http://restapi.amap.com/v3/geocode/regeo?location="
						+ gcj_encrypt.get(CordinateService.Lon)
						+ ","
						+ gcj_encrypt.get(CordinateService.Lat)
						+ "&key=e351bfc5ceec6c86c4ef747bbd9f0497&radius=1000&extensions=all");
		return parseAddress(sendGet(urlSB.toString(), "utf-8"));
	}

	private static String parseAddress(String result) {

		String address = "";
		if (StringUtils.isEmpty(result)) {
			logger.error("---------error result isEmpty------------");
			return "http请求结果为空";
		}
		// if (!JsonValidUtils.validate(result)) {
		// JSONObject jsonObject = new JSONObject(result);
		// try {
		// address =
		// jsonObject.getJSONObject("regeocode").getJSONObject("addressComponent").getString("township");
		// address= StringUtils.isNotEmpty(address) ? address
		// :jsonObject.getJSONObject("regeocode").getString("formatted_address")
		// ;
		// if
		// (jsonObject.getJSONObject("regeocode").getJSONArray("pois").length()>0)
		// {
		// address =
		// (jsonObject.getJSONObject("regeocode").getJSONArray("pois").getJSONObject(0).getString("name")
		// ) ;
		// }
		//
		// } catch (Exception e) {
		// logger.error("---------- parse result error--" + result + "-------");
		// e.printStackTrace();
		// }
		// return address;
		// }
		JSONObject jsonObject = new JSONObject(result);
		try {
			address = jsonObject.getJSONObject("regeocode")
					.getJSONObject("addressComponent").getString("township");
			address = StringUtils.isNotEmpty(address) ? address : jsonObject
					.getJSONObject("regeocode").getString("formatted_address");
			if (jsonObject.getJSONObject("regeocode").getJSONArray("pois")
					.length() > 0) {
				address = (jsonObject.getJSONObject("regeocode")
						.getJSONArray("pois").getJSONObject(0)
						.getString("name"));
			}

		} catch (Exception e) {
			logger.error("---------- parse result error--" + result + "-------");
			e.printStackTrace();
		}
		return address.replaceAll("\\[", "").replaceAll("\\]", "");
	}

	// public static String getPosition(String lat, String lng) {
	//
	// logger.error("------------------------lat:" + lat + "-lng:" + lng +
	// "-------------------");
	// // 校验地址 是否为空
	// if (StringUtils.isEmpty(lat) || StringUtils.isEmpty(lng)) {
	// return "经纬度为空";
	// }
	// if (Float.parseFloat(lat) == 0 || Float.parseFloat(lng) == 0 ) {
	// return "经纬度为空";
	// }
	// StringBuilder urlSB = new StringBuilder(
	// "http://api.map.baidu.com/geocoder/v2/?ak=uXd1sQo9pvTXTOUeSmoAjy6b&location="+
	// lat + "," + lng + "&output=json");
	// return parsePosition(sendGet(urlSB.toString(), "utf-8"));
	// }

	/**
	 * 默认500米范围内的加油站
	 * 
	 * @param lat
	 * @param lng
	 * @return
	 */
	public static String getOilStationName(String lat, String lng) {

		// 校验地址 是否为空
		if (StringUtils.isEmpty(lat) || StringUtils.isEmpty(lng)) {
			// return "经纬度为空";
			return null;
		}
		// StringBuilder urlSB = new
		// StringBuilder("http://api.map.baidu.com/geocoder/v2/?ak=uXd1sQo9pvTXTOUeSmoAjy6b&location="+lat+","
		// +lng+"&output=json");
		// http://api.map.baidu.com/place/v2/search?query=加油站&scope=2&output=json&location=39.917969,116.447147&radius=1000&filter=sort_name:distance|sort_rule:1&ak=uXd1sQo9pvTXTOUeSmoAjy6b";
		StringBuilder urlSB = new StringBuilder(
				"http://api.map.baidu.com/place/v2/search?query=加油站&scope=2&output=json&location="
						+ lat
						+ ","
						+ lng
						+ "&radius=500&filter=sort_name:distance|sort_rule:1&ak=uXd1sQo9pvTXTOUeSmoAjy6b");
		return parseToGeoHash(sendGet(urlSB.toString(), "utf-8"));
	}

	public static String getOilStationName(String lat, String lng, String radius) {

		// 校验地址 是否为空
		if (StringUtils.isEmpty(lat) || StringUtils.isEmpty(lng)) {
			// return "经纬度为空";
			return null;
		}
		// String
		// url2="http://api.map.baidu.com/place/v2/search?query=加油站&location="+lat+","
		// +lng+"&radius="+radius+"&output=json&ak=uXd1sQo9pvTXTOUeSmoAjy6b&filter=sort_name:distance|sort_rule:1";
		StringBuilder urlSB = new StringBuilder(
				"http://api.map.baidu.com/place/v2/search?query=加油站&scope=1&output=json&location="
						+ lat
						+ ","
						+ lng
						+ "&radius="
						+ radius
						+ "&ak=uXd1sQo9pvTXTOUeSmoAjy6b");
		return parseToGeoHash(sendGet(urlSB.toString(), "utf-8"));
	}

	public static String getOilStationLatLng(String lat, String lng,
			String radius) {

		// 校验地址 是否为空
		if (StringUtils.isEmpty(lat) || StringUtils.isEmpty(lng)) {
			return null;
		}
		// String
		// url2="http://api.map.baidu.com/place/v2/search?query=加油站&location="+lat+","
		// +lng+"&radius="+radius+"&output=json&ak=uXd1sQo9pvTXTOUeSmoAjy6b&filter=sort_name:distance|sort_rule:1";
		StringBuilder urlSB = new StringBuilder(
				"http://api.map.baidu.com/place/v2/search?query=加油站&scope=2&output=json&location="
						+ lat
						+ ","
						+ lng
						+ "&radius="
						+ radius
						+ "&ak=uXd1sQo9pvTXTOUeSmoAjy6b&filter=sort_name:distance|sort_rule:1");
		return parseName2LatLng(sendGet(urlSB.toString(), "utf-8"));
	}

	public static String getCarMaintenceStation(String lat, String lng,
			String radius) {
		// 校验地址 是否为空
		if (StringUtils.isEmpty(lat) || StringUtils.isEmpty(lng)) {
			return null;
		}
		StringBuilder urlSB = new StringBuilder(
				"http://api.map.baidu.com/place/v2/search?query=汽车保养维修&scope=1&output=json&location="
						+ lat
						+ ","
						+ lng
						+ "&radius="
						+ radius
						+ "&ak=uXd1sQo9pvTXTOUeSmoAjy6b&filter=sort_name:distance|sort_rule:1");
		return parseToGeoHash(sendGet(urlSB.toString(), "utf-8"));
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

		url = StringUtils.replaceString(url, "\\+", "").replaceAll(" ", "");

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
			logger.error("发送GET请求出现异常！" + e);
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

	// private static String parsePosition(String result) {
	//
	// String position = "";
	// if (StringUtils.isEmpty(result)) {
	// logger.error("---------error result isEmpty------------");
	// return "http请求结果为空";
	// }
	// if (!JsonValidUtils.validate(result)) {
	// JSONObject jsonObject = new JSONObject(result);
	// try {
	// position = jsonObject.getJSONObject("result").getString(
	// "sematic_description");
	// //formatted_address
	// if (StringUtils.isEmpty(position)) {
	// position = jsonObject.getJSONObject("result").getString(
	// "formatted_address");
	// }
	// } catch (Exception e) {
	// logger.error("---------- parse result error--" + result + "-------");
	// e.printStackTrace();
	// }
	// return position;
	// }
	// JSONObject jsonObject = new JSONObject(result);
	// try {
	// position = jsonObject.getJSONObject("result").getString(
	// "sematic_description");
	// //formatted_address
	// if (StringUtils.isEmpty(position)) {
	// position = jsonObject.getJSONObject("result").getString(
	// "formatted_address");
	// }
	// } catch (Exception e) {
	// logger.error("---------- parse result error--" + result + "-------");
	// e.printStackTrace();
	// }
	// return position;
	// }

	private static String parseToGeoHash(String result) {

		String position = null;
		if (StringUtils.isEmpty(result)) {
			logger.error("---------error result isEmpty------------");
			return null;
		}
		if (!JsonValidUtils.validate(result)) {
			return null;
		}

		JSONObject jsonObject = new JSONObject(result);
		try {

			if (jsonObject.getJSONArray("results").length() == 0) {
				position = null;
			} else {

				// // position =
				// logger.info("---------------"+jsonObject.getJSONArray("results").getJSONObject(0).getString("name"));
				// logger.info("---------- parse result error--" + result +
				// "-------");

				JSONObject jsonObject2 = jsonObject.getJSONArray("results")
						.getJSONObject(0).getJSONObject("location");

				position = GeoHash.getGeoHash8(jsonObject2.get("lat")
						.toString(), jsonObject2.get("lng").toString());
			}
		} catch (Exception e) {
			logger.error("---------- parse result error--" + result + "-------");
			e.printStackTrace();
		}
		return position;
	}

	private static String parseName2LatLng(String result) {

		String position = null;
		if (StringUtils.isEmpty(result)) {
			logger.error("---------  result isEmpty------------");
			return null;
		}
		// if (!JsonValidUtils.validate(result)) {
		// if (!JsonValidUtils.validate(result)) {
		// return null;
		// } else {
		// //
		// }
		// }

		JSONObject jsonObject = new JSONObject(result);
		try {
			if (jsonObject.getJSONArray("results").length() == 0) {
				position = null;
			} else {

				JSONObject jsonObject2 = jsonObject.getJSONArray("results")
						.getJSONObject(0).getJSONObject("location");

				position = jsonObject2.get("lat").toString() + "_"
						+ jsonObject2.get("lng").toString();
			}
		} catch (Exception e) {
			logger.error("---------- parse result error--" + result + "-------");
			e.printStackTrace();
		}
		return position;
	}

	public static String gpsConvert(String lng, String lat) {
		// "http://restapi.amap.com/v3/assistant/coordinate/convert?locations=116.481499,39.990475&coordsys=gps&output=json&key=e351bfc5ceec6c86c4ef747bbd9f0497";
		StringBuilder urlSB = new StringBuilder(
				"http://restapi.amap.com/v3/assistant/coordinate/convert?locations="
						+ lng
						+ ","
						+ lat
						+ "&coordsys=gps&output=json&key=e351bfc5ceec6c86c4ef747bbd9f0497");
		return parseCoordinate(sendGet(urlSB.toString(), "utf-8"));

	}

	/**
	 * 返回 lng lat
	 * 
	 * @param result
	 * @return lng |lat 116.487586|39.991755
	 */
	private static String parseCoordinate(String result) {

		String coordinate = "";
		if (StringUtils.isEmpty(result)) {
			logger.error("---------error result isEmpty------------");
			return "http请求结果为空";
		}
		// {"status":"1","info":"ok","infocode":"10000","locations":"116.487586,39.991755"}
		JSONObject jsonObject = new JSONObject(result);
		try {
			coordinate = jsonObject.getString("locations");
		} catch (Exception e) {
			logger.error("---------- parse result error--" + result + "-------");
			e.printStackTrace();
		}
		StringBuilder string = new StringBuilder("");
		return string.append(coordinate.split(",")[0]).append("|")
				.append(coordinate.split(",")[1]).toString();
	}

	public static void main(String[] args) {
        String poiDetail = BackHttpUtils.getAddressDetail("+39.917969", "+116.447147");
        
        System.out.println("poiDetail--------"+poiDetail +"----"+poiDetail.split("\\|").length);
	 
	 
		String longitude = "+116.481469";
		String latitude = "+39.993475";
		logger.error("高德地图-----------------" + getAddress(latitude, longitude));
 
//		Map<String, Double> pois_query = CordinateService.gcj_encrypt(Double.valueOf(latitude), Double.valueOf(longitude));
		
		String resString  = getAddressDetail( latitude,longitude);
		System.out.println("----res --"+resString );
	}

	
	 public static String getAddressDetail(String latitude,String longitude) {
		 
//		logger.info("------------- latitude "+latitude+"---longitude " +longitude);
		Map<String, Double> pois_query_map = CordinateService.gcj_encrypt(Double.valueOf(latitude), Double.valueOf(longitude));

		String pois_query=pois_query_map.get(CordinateService.Lon )+","+ pois_query_map.get(CordinateService.Lat ) ;
		String query_url = "http://restapi.amap.com/v3/geocode/regeo?location=" + pois_query
				+ "&key=e351bfc5ceec6c86c4ef747bbd9f0497&radius=1000&extensions=all";
//		System.out.println("query_url-------" +query_url);
		String pois_response = sendGet(query_url,"utf-8");
		String formatted_address = " ";
		String province = " ";
		String city = " ";
		String name = " ";
		String type = " ";
		
		if (pois_response != null && !pois_response.equals("")) {
			ObjectMapper mapper = new ObjectMapper();
			JsonNode pois_json = null;
			try {
				pois_json = mapper.readTree(pois_response);
			} catch (JsonProcessingException e) {
 				e.printStackTrace();
			} catch (IOException e) {
 				e.printStackTrace();
			}
			if (pois_json.get("infocode").asText().equals("10000") && pois_json.get("regeocode") != null) {
				formatted_address = pois_json.get("regeocode").get("formatted_address").asText();
				province = pois_json.get("regeocode").get("addressComponent").get("province").asText();
				city = pois_json.get("regeocode").get("addressComponent").get("city").asText();
				if (!"".equals(	pois_json.get("regeocode").get("addressComponent").get("township").asText())) {
					name = pois_json.get("regeocode").get("addressComponent").get("township").asText();
				} else {
					name = formatted_address;
				}
				if (pois_json.get("regeocode").get("pois").size() > 0) {
					name = pois_json.get("regeocode").get("pois").get(0).get("name").asText();
					type = pois_json.get("regeocode").get("pois").get(0).get("type").asText();
				}
			}
 
			logger.info("----"+formatted_address + "|"+ province + "|"+ city + "|"+ name + "|"+ type );

		}
		
		return 	 formatted_address + "|"+ province + "|"+ city + "|"+ name + "|"+ type ;
	}
	public static String accessUrl(String urlStr) {
		URL url = null;
		BufferedReader in = null;
		StringBuffer sb = new StringBuffer();
		try {
			url = new URL(urlStr);
			HttpURLConnection urlCon = (HttpURLConnection) url.openConnection();
			urlCon.setConnectTimeout(3000);
			urlCon.setReadTimeout(5000);
			in = new BufferedReader(new InputStreamReader(
					urlCon.getInputStream(), "UTF-8"));
			String str = null;
			while ((str = in.readLine()) != null) {
				sb.append(str);
			}
		} catch (Exception ex) {
			logger.warn("accessUrl error" + ex);
		} finally {
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException ex) {
			}
		}
		String result = sb.toString();
		//logger.debug("----------result:" +result);

		return result;
	}

}