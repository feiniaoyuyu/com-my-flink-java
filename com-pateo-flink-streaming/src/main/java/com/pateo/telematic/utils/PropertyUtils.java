package com.pateo.telematic.utils;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

import com.pateo.constant.Constant;

public class PropertyUtils {
	static Map<String, String> prop = null;
	static String  propertyName ="prop";
	/** 
	 * 获取指定配置文件中所以的数据 
	 * @param propertyName 
	 *        调用方式： 
	 *            1.配置文件放在resource源包下，不用加后缀 
	 *              PropertiesUtil.getAllMessage("message"); 
	 *            2.放在包里面的 
	 *              PropertiesUtil.getAllMessage("com.test.message"); 
	 * @return 
	 */  
	public static List<String> getAllMessage(String propertyName) {  
	    // 获得资源包  
	    ResourceBundle rb = ResourceBundle.getBundle(propertyName.trim());  
	    // 通过资源包拿到所有的key  
	    Enumeration<String> allKey = rb.getKeys();  
	    // 遍历key 得到 value  
	    List<String> valList = new ArrayList<String>();  
	    while (allKey.hasMoreElements()) {  
	        String key = allKey.nextElement();  
	        String value = (String) rb.getString(key);  
	        valList.add(value);  
	    }  
	    return valList;  
	}  
	
	public static Map<String,String> getAllMessageMap(String propertyName) {  
	    // 获得资源包  
	    ResourceBundle rb = ResourceBundle.getBundle(propertyName.trim()); 
	    // 通过资源包拿到所有的key  
	    Enumeration<String> allKey = rb.getKeys();  
	    // 遍历key 得到 value    
	    while (allKey.hasMoreElements()) {  
	        String key = allKey.nextElement();  
	        String value = (String) rb.getString(key);
	        prop.put(key, value);
	    }  
	    return prop;  
	}  
  
	public static String getValue(String key) {
		
		if (null == prop ) {
			prop = new HashMap<String, String>();
			getAllMessageMap(propertyName);
		}
		return  prop.get(key);
	}
	
	public static void main(String[] args) {
//		System.out.println(Constant.hbase_zookeeper_quorum);
        System.out.println( "zookeeper.quorum : " + PropertyUtils.getValue(Constant.hbase_zookeeper_quorum));;
	}
}

