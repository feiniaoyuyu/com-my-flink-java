package com.pateo.df.utils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONObject;

import com.pateo.telematic.utils.BackHttpUtils;

/**
 * dongfeng poi 工具类
 * @author sh04595
 *
 */
public class PoiUtils implements Serializable{

	private static final long serialVersionUID = 1L;
	final static String urlPrefix = "http://restapi.amap.com/v3/geocode/regeo?location=";
	final static String urlSuffix = "&key=e351bfc5ceec6c86c4ef747bbd9f0497&radius=1000&extensions=all";
	//http://restapi.amap.com/v3/geocode/regeo?location=102.605735,37.943261&key=e351bfc5ceec6c86c4ef747bbd9f0497&radius=1000&extensions=all
	public static Map<String, String> getPoi(String lng, String lat) {

		if (Double.valueOf(lng) < 10 || Double.valueOf(lat) < 10) {
			HashMap<String, String> hashMap = new HashMap<String, String>();
			hashMap.put(WLContants.PROVINCE, "") ;
			hashMap.put(WLContants.CITY, "") ;
			hashMap.put(WLContants.CITYCODE, "");
			hashMap.put(WLContants.NAME, "") ;
			hashMap.put(WLContants.VALID_POI, "2") ; // 2 表示经纬度为 异常 
			return hashMap; 
		}
		StringBuilder urlStr = new StringBuilder(urlPrefix).append(lng.replaceAll("\\+", ""))
				.append(",").append(lat.replaceAll("\\+", "")).append(urlSuffix);

		String content = BackHttpUtils.accessUrl(urlStr.toString());
		Map<String, String> parsePoi = parsePoi(content.trim());

		return parsePoi;
	}

	/**
	 * 解析 省份城市等poi信息
	 * @param result
	 * @return map
	 */
	private static Map<String, String> parsePoi(String result) {
		String address = "";
		// 重庆 023 北京020 上海 021 天津 020 香港 1852 澳门区号1853 台湾1886 citycode
		HashMap<String, String> hashMap = new HashMap<String, String>();
		hashMap.put(WLContants.PROVINCE, "") ;
		hashMap.put(WLContants.CITY, "") ;
		hashMap.put(WLContants.CITYCODE, "");
		hashMap.put(WLContants.NAME, "") ;
		hashMap.put(WLContants.VALID_POI, "0") ; // 0 poi 请求异常  2 表示经纬度为 异常  1 表示正常
		
		JSONObject jsonObject = null ;

		if (result.length() < 30) {
			System.err.println("-------not a valid result"+ result);
			return hashMap ;
		}
		try {
			jsonObject = new JSONObject(result);
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("-------new json Exception:"+ result);
			return hashMap;
		}
		 
		try {
			JSONObject regeocode = jsonObject.getJSONObject("regeocode");

			// 解析出现问题的经纬度返回空字段
			try {
				regeocode.getJSONObject("addressComponent").getJSONArray(WLContants.PROVINCE);
				//System.err.println("==================== result " +result +"\n");
				return hashMap;

			} catch (Exception e) {
				//System.err.println("==================== err");
				//e.printStackTrace();
			}
			String province = regeocode.getJSONObject("addressComponent").getString("province");
			String citycode = regeocode.getJSONObject("addressComponent").getString("citycode");

			String city = "";
			
			try {
				city = regeocode.getJSONObject("addressComponent").getString("city");
			} catch (Exception e) {
				city = regeocode.getJSONObject("addressComponent").getString("province");
			}
			address =  regeocode.getString("formatted_address");
			
			hashMap.put(WLContants.PROVINCE, province) ;
			hashMap.put(WLContants.CITY, city) ;
			hashMap.put(WLContants.CITYCODE, citycode);
			hashMap.put(WLContants.NAME, address) ;
			hashMap.put(WLContants.VALID_POI,"1") ;
			
//			address = regeocode.getJSONObject("addressComponent").getString("township");
//			address = StringUtils.isNotEmpty(address) ? address : regeocode.getString("formatted_address");
//			jsonObject.getJSONObject("regeocode");
//			address = jsonObject.getJSONObject("regeocode")
//					.getJSONObject("addressComponent").getString("township");
//			address = StringUtils.isNotEmpty(address) ? address : jsonObject
//					.getJSONObject("regeocode").getString("formatted_address");
//			if (jsonObject.getJSONObject("regeocode").getJSONArray("pois")
//					.length() > 0) {
//				address = (jsonObject.getJSONObject("regeocode")
//						.getJSONArray("pois").getJSONObject(0)
//						.getString("name"));
//			}
//			hashMap.put("address", address) ;
		} catch (Exception e) {
			System.err.println("---------- parse result error--" + result);
			e.printStackTrace();
		}
//		System.out.println( address.replaceAll("\\[", "").replaceAll("\\]", ""));

		return hashMap;
	}

 	public static void main(String[] args) {
		Map<String, String> poi = getPoi("120.171568", "35.343697");
 		//Map<String, String> poi = getPoi("116.290259","40.0132917");
		System.out.println("---------------poi:"+poi);
	}
}
