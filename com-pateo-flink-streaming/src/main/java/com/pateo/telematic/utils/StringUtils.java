package com.pateo.telematic.utils;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.lang.Character.UnicodeBlock;

 
/**
 * 字符串工具类
 * 
 * @author Administrator
 *
 */
public class StringUtils implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * 判断字符串是否为空
	 * 
	 * @param str
	 *            字符串
	 * @return 是否为空
	 */
	public static boolean isEmpty(String str) {
		return str == null || "".equals(str);
	}

	/**
	 * 判断字符串是否不为空
	 * 
	 * @param str
	 *            字符串
	 * @return 是否不为空
	 */
	public static boolean isNotEmpty(String str) {
		return str != null && !"".equals(str);
	}

	/**
	 * 截断字符串两侧的逗号
	 * 
	 * @param str
	 *            字符串
	 * @return 字符串
	 */
	public static String trimComma(String str) {
		if (str.startsWith(",")) {
			str = str.substring(1);
		}
		if (str.endsWith(",")) {
			str = str.substring(0, str.length() - 1);
		}
		return str;
	}

	/**
	 * 补全两位数字
	 * 
	 * @param str
	 * @return
	 */
	public static String fulfuill(String str) {
		if (str.length() == 2) {
			return str;
		} else {
			return "0" + str;
		}
	}

	/**
	 * 从拼接的字符串中提取字段
	 * 
	 * @param str
	 *            字符串
	 * @param delimiter
	 *            分隔符
	 * @param field
	 *            字段
	 * @return 字段值
	 */
	public static String getFieldFromConcatString(String str, String delimiter,
			String field) {
		try {
			String[] fields = str.split(delimiter);
			for (String concatField : fields) {
				// searchKeywords=|clickCategoryIds=1,2,3
				if (concatField.split("=").length == 2) {
					String fieldName = concatField.split("=")[0];
					String fieldValue = concatField.split("=")[1];
					if (fieldName.equals(field)) {
						return fieldValue;
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 从拼接的字符串中给字段设置值
	 * 
	 * @param str
	 *            字符串
	 * @param delimiter
	 *            分隔符
	 * @param field
	 *            字段名
	 * @param newFieldValue
	 *            新的field值
	 * @return 字段值
	 */
	public static String setFieldInConcatString(String str, String delimiter,
			String field, String newFieldValue) {
		String[] fields = str.split(delimiter);

		for (int i = 0; i < fields.length; i++) {
			String fieldName = fields[i].split("=")[0];
			if (fieldName.equals(field)) {
				String concatField = fieldName + "=" + newFieldValue;
				fields[i] = concatField;
				break;
			}
		}

		StringBuffer buffer = new StringBuffer("");
		for (int i = 0; i < fields.length; i++) {
			buffer.append(fields[i]);
			if (i < fields.length - 1) {
				buffer.append("|");
			}
		}

		return buffer.toString();
	}

	// Convert from Unicode to UTF-8
	public static String convertUnicode2UTF8(String string) {

		try {
			byte[] utf8 = string.getBytes("UTF-8");
			string = new String(utf8, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return "";
		}
		return string;
	}

	/**
	 * utf-8 转换成 unicode
	 * 
	 * @param inStr
	 * @return
	 */
	public static String utf8ToUnicode(String inStr) {
		char[] myBuffer = inStr.toCharArray();
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < inStr.length(); i++) {
			UnicodeBlock ub = UnicodeBlock.of(myBuffer[i]);
			if (ub == UnicodeBlock.BASIC_LATIN) {
				// 英文及数字等
				sb.append(myBuffer[i]);
			} else if (ub == UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS) {
				// 全角半角字符
				int j = (int) myBuffer[i] - 65248;
				sb.append((char) j);
			} else {
				// 汉字
				short s = (short) myBuffer[i];
				String hexS = Integer.toHexString(s);
				String unicode = "\\u" + hexS;
				sb.append(unicode.toLowerCase());
			}
		}
		return sb.toString();
	}

	
 
	public static String replaceString(String src, String replace) {
		String replaceStr = src.replaceAll(replace, "").replaceAll(" ", "");
		return replaceStr;
	}

	public static String replaceString(String src, String toReplace,
			String replace) {
		String replaceStr = src.replaceAll(toReplace, replace).replaceAll(" ",
				"");
		return replaceStr;
	}

	public static String unicodeToUtf8(String theString) {
		char aChar;
		int len = theString.length();
		StringBuffer outBuffer = new StringBuffer(len);
		for (int x = 0; x < len;) {
			aChar = theString.charAt(x++);
			if (aChar == '\\') {
				aChar = theString.charAt(x++);
				if (aChar == 'u') {
					// Read the xxxx
					int value = 0;
					for (int i = 0; i < 4; i++) {
						aChar = theString.charAt(x++);
						switch (aChar) {
						case '0':
						case '1':
						case '2':
						case '3':
						case '4':
						case '5':
						case '6':
						case '7':
						case '8':
						case '9':
							value = (value << 4) + aChar - '0';
							break;
						case 'a':
						case 'b':
						case 'c':
						case 'd':
						case 'e':
						case 'f':
							value = (value << 4) + 10 + aChar - 'a';
							break;
						case 'A':
						case 'B':
						case 'C':
						case 'D':
						case 'E':
						case 'F':
							value = (value << 4) + 10 + aChar - 'A';
							break;
						default:
							throw new IllegalArgumentException( "Malformed   \\uxxxx   encoding.");
						}
					}
					outBuffer.append((char) value);
				} else {
					if (aChar == 't')
						aChar = '\t';
					else if (aChar == 'r')
						aChar = '\r';
					else if (aChar == 'n')
						aChar = '\n';
					else if (aChar == 'f')
						aChar = '\f';
					outBuffer.append(aChar);
				}
			} else
				outBuffer.append(aChar);
		}
		return outBuffer.toString();
	}
	
	public static void main(String[] args) {
		// // acc=\u003E02
		String string = "acc=\u003E02";
		
		System.out.println(convertUnicode2UTF8(string));
		System.out.println(utf8ToUnicode(string));
		System.out.println(unicodeToUtf8(string));
	}
}
