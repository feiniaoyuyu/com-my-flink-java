package com.pateo.telematic.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class HttpRequestor {

	private String charset = "utf-8";
	private Integer connectTimeout = null;
	private Integer socketTimeout = null;
	private String proxyHost = null;
	private Integer proxyPort = null;

	/**
	 * Do GET request
	 * 
	 * @param url
	 * @return
	 * @throws Exception
	 * @throws IOException
	 */
	public String doGet(String url) throws Exception {

		URL localURL = new URL(url);

		URLConnection connection = openConnection(localURL);
		HttpURLConnection httpURLConnection = (HttpURLConnection) connection;

		httpURLConnection.setRequestProperty("Accept-Charset", charset);
		httpURLConnection.setRequestProperty("Content-Type",
				"application/x-www-form-urlencoded");

		InputStream inputStream = null;
		InputStreamReader inputStreamReader = null;
		BufferedReader reader = null;
		StringBuffer resultBuffer = new StringBuffer();
		String tempLine = null;
		// BufferedReader bufferedReader = new BufferedReader(new
		// InputStreamReader(in,"UTF-8"));
		if (httpURLConnection.getResponseCode() >= 300) {
			throw new Exception(
					"HTTP Request is not success, Response code is "
							+ httpURLConnection.getResponseCode());
		}

		try {
			inputStream = httpURLConnection.getInputStream();
			inputStreamReader = new InputStreamReader(inputStream, "UTF-8");
			reader = new BufferedReader(inputStreamReader);

			while ((tempLine = reader.readLine()) != null) {
				resultBuffer.append(tempLine);
			}

		} finally {

			if (reader != null) {
				reader.close();
			}

			if (inputStreamReader != null) {
				inputStreamReader.close();
			}

			if (inputStream != null) {
				inputStream.close();
			}

		}

		return resultBuffer.toString();
	}

	/**
	 * Do POST request
	 * 
	 * @param url
	 * @param parameterMap
	 * @return
	 * @throws Exception
	 */
	public String doPost(String url, Map<String, Object> parameterMap)
			throws Exception {

		/* Translate parameter map to parameter date string */
		StringBuffer parameterBuffer = new StringBuffer();
		if (parameterMap != null) {
			Iterator<String> iterator = parameterMap.keySet().iterator();
			String key = null;
			String value = null;
			while (iterator.hasNext()) {
				key = iterator.next();
				if (parameterMap.get(key) != null) {
					value = parameterMap.get(key).toString();
				} else {
					value = "";
				}

				parameterBuffer.append(key).append("=").append(value);
				if (iterator.hasNext()) {
					parameterBuffer.append("&");
				}
			}
		}

		System.out.println("POST parameter : " + parameterBuffer.toString());

		URL localURL = new URL(url);

		URLConnection connection = openConnection(localURL);
		HttpURLConnection httpURLConnection = (HttpURLConnection) connection;

		httpURLConnection.setDoOutput(true);
		httpURLConnection.setRequestMethod("POST");
		httpURLConnection.setRequestProperty("Accept-Charset", charset);
		httpURLConnection.setRequestProperty("Content-Type",
				"application/x-www-form-urlencoded");
		httpURLConnection.setRequestProperty("Content-Length",
				String.valueOf(parameterBuffer.length()));

		OutputStream outputStream = null;
		OutputStreamWriter outputStreamWriter = null;
		InputStream inputStream = null;
		InputStreamReader inputStreamReader = null;
		BufferedReader reader = null;
		StringBuffer resultBuffer = new StringBuffer();
		String tempLine = null;

		try {
			outputStream = httpURLConnection.getOutputStream();
			outputStreamWriter = new OutputStreamWriter(outputStream);

			outputStreamWriter.write(parameterBuffer.toString());
			outputStreamWriter.flush();

			if (httpURLConnection.getResponseCode() >= 300) {
				throw new Exception(
						"HTTP Request is not success, Response code is "
								+ httpURLConnection.getResponseCode());
			}

			inputStream = httpURLConnection.getInputStream();
			// inputStreamReader = new InputStreamReader(inputStream);
			inputStreamReader = new InputStreamReader(inputStream, "utf-8");
			reader = new BufferedReader(inputStreamReader);

			while ((tempLine = reader.readLine()) != null) {
				resultBuffer.append(tempLine);
			}

		} finally {

			if (outputStreamWriter != null) {
				outputStreamWriter.close();
			}

			if (outputStream != null) {
				outputStream.close();
			}

			if (reader != null) {
				reader.close();
			}

			if (inputStreamReader != null) {
				inputStreamReader.close();
			}

			if (inputStream != null) {
				inputStream.close();
			}

		}

		return resultBuffer.toString();
	}

	private URLConnection openConnection(URL localURL) throws IOException {
		URLConnection connection;
		if (proxyHost != null && proxyPort != null) {
			Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(
					proxyHost, proxyPort));
			connection = localURL.openConnection(proxy);
		} else {
			connection = localURL.openConnection();
		}
		return connection;
	}

	/**
	 * Render request according setting
	 * 
	 * @param request
	 */
	private void renderRequest(URLConnection connection) {

		if (connectTimeout != null) {
			connection.setConnectTimeout(connectTimeout);
		}

		if (socketTimeout != null) {
			connection.setReadTimeout(socketTimeout);
		}

	}

	/*
	 * Getter & Setter
	 */
	public Integer getConnectTimeout() {
		return connectTimeout;
	}

	public void setConnectTimeout(Integer connectTimeout) {
		this.connectTimeout = connectTimeout;
	}

	public Integer getSocketTimeout() {
		return socketTimeout;
	}

	public void setSocketTimeout(Integer socketTimeout) {
		this.socketTimeout = socketTimeout;
	}

	public String getProxyHost() {
		return proxyHost;
	}

	public void setProxyHost(String proxyHost) {
		this.proxyHost = proxyHost;
	}

	public Integer getProxyPort() {
		return proxyPort;
	}

	public void setProxyPort(Integer proxyPort) {
		this.proxyPort = proxyPort;
	}

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	
	private static String getGeotable(String geotableName) throws Exception {
		String url = "http://api.map.baidu.com/geodata/v3/geotable/create";
		Map<String, Object> parameters = new HashMap<>();
		parameters.put("name", geotableName);
		parameters.put("geotype", 1);
		parameters.put("is_published", 1);
		parameters.put("ak", "uXd1sQo9pvTXTOUeSmoAjy6b");
		
		return new HttpRequestor().doPost(url, parameters);
	}
	
	public static void main(String[] args) throws Exception {
		
//		geotablepateo  149648
//		System.out.println(" 得到geotable 每个 naem 只能获取一次   "+getGeotable("geotablepateo"));
 
		/* Get Request */
		System.out.println(new HttpRequestor().doGet("http://api.map.baidu.com/geodata/v3/poi/detail?geotable_id=149648&ak=uXd1sQo9pvTXTOUeSmoAjy6b"));
	}
	/**
//		0	正常	
//		1	服务器内部错误	该服务响应超时或系统内部错误，请留下联系方式
//		10	上传内容超过8M	Post上传数据不能超过8M
//		101	AK参数不存在	请求消息没有携带AK参数
//		102	MCODE参数不存在，mobile类型mcode参数必需	对于Mobile类型的应用请求需要携带mcode参数，该错误码代表服务器没有解析到mcode
//		200	APP不存在，AK有误请检查再重试	根据请求的ak，找不到对应的APP
//		201	APP被用户自己禁用，请在控制台解禁	
//		202	APP被管理员删除	恶意APP被管理员删除
//		203	APP类型错误	当前API控制台支持Server(类型1), Mobile(类型2, 新版控制台区分为Mobile_Android(类型21)及Mobile_IPhone（类型22））及Browser（类型3），除此之外其他类型认为是APP类型错误
//		210	APP IP校验失败	在申请SERVER类型应用的时候选择IP校验，需要填写IP白名单，如果当前请求的IP地址不在IP白名单或者不是0.0.0.0/0就认为IP校验失败
//		211	APP SN校验失败	SERVER类型APP有两种校验方式IP校验和SN校验，当用户请求的SN和服务端计算出来的SN不相等的时候提示SN校验失败
//		220	APP Referer校验失败	浏览器类型的APP会校验referer字段是否存且切在referer白名单里面，否则返回该错误码
//		230	APP Mcode码校验失败	服务器能解析到mcode，但和数据库中不一致，请携带正确的mcode
//		240	APP 服务被禁用	用户在API控制台中创建或设置某APP的时候禁用了某项服务
//		250	用户不存在	根据请求的user_id, 数据库中找不到该用户的信息，请携带正确的user_id
//		251	用户被自己删除	该用户处于未激活状态
//		252	用户被管理员删除	恶意用户被加入黑名单
//		260	服务不存在	服务器解析不到用户请求的服务名称
//		261	服务被禁用	该服务已下线
//		301	永久配额超限，限制访问	配额超限，如果想增加配额请联系我们
//		302	天配额超限，限制访问	配额超限，如果想增加配额请联系我们
//		401	当前并发量已经超过约定并发配额，限制访问	并发控制超限，请控制并发量或联系我们
//		402	当前并发量已经超过约定并发配额，并且服务总并发量也已经超过设定的总并发配额，限制访问	并发控制超限，请控制并发量或联系我们
	 */
}