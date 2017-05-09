package com.pateo.telematic.utils;

import java.text.DecimalFormat;
import java.text.NumberFormat;

public class NumberUtils {

	private final static DecimalFormat decimalFormat = new DecimalFormat("0.000000");
	private static DecimalFormat customDecimalFormat = null;
	private static NumberFormat nf = NumberFormat.getNumberInstance();

	public static void main(String[] args) {
		//-13568, 十六进制数字声明前面加0x(零x),xabcd区写
//		 int iHex = 0x1000;
		 int iHex = 0xf;

		 System.out.println("ihex =" + iHex);
//		 System.out.println("toHexString---"+toHexString("f"));;
		 System.out.println("ihex -------------");
		 //16进制数字转换10进制数字parseInt参数10进制
		 int iToTen = Integer.parseInt("f",16);
		 int iValue = Integer.parseInt("30", 16);  //这样就对了，结果是48.

		 System.out.println("iToTen--"+iToTen);
		 System.out.println(Integer.parseInt("110", 2));
	}

	public static String toHexString(String s) {  
	        String str = "";  
	        for (int i = 0; i < s.length(); i++) {  
	            int ch = (int) s.charAt(i);  
	            String s4 = Integer.toHexString(ch);  
	            str = str + s4;  
	        }  
	        return "0x" + str;//0x表示十六进制  
	    }  
	  
	    //转换十六进制编码为字符串  
	    public static String toStringHex(String s) {  
	        if ("0x".equals(s.substring(0, 2))) {  
	            s = s.substring(2);  
	        }  
	        byte[] baKeyword = new byte[s.length() / 2];  
	        for (int i = 0; i < baKeyword.length; i++) {  
	            try {  
	                baKeyword[i] = (byte) (0xff & Integer.parseInt(s.substring(  
	                        i * 2, i * 2 + 2), 16));  
	            } catch (Exception e) {  
	                e.printStackTrace();  
	            }  
	        }  
	  
	        try {  
	            s = new String(baKeyword, "utf-8");//UTF-16le:Not  
	        } catch (Exception e1) {  
	            e1.printStackTrace();  
	        }  
	        return s;  
	    }  
	    
	    public static void printHexString(byte[] b) {  
	        for (int i = 0; i < b.length; i++) {  
	            String hex = Integer.toHexString(b[i] & 0xFF);  
	            if (hex.length() == 1) {  
	                hex = '0' + hex;  
	            }  
	            System.out.print(hex.toUpperCase());  
	        }  
	    }  
	/**
	 * @param iHex
	 * @return
	 */
	public static int parseHexToInt(int iHex ) {
		return Integer.parseInt(iHex+"",10);
	}
	public static int parseHexToInt(String iHex ) {
		return Integer.parseInt(iHex,10);
	}
	/**
	 * 默认格式化六位小数
	 * @param decimal
	 * @return
	 */
	public static double formatDecimal(String decimal) {
		return formatDecimal(Double.valueOf(decimal) );
	}
	public static double formatDecimal(double decimal) {
		return  Double.valueOf( decimalFormat.format(decimal));
	}
	/**
	 * 自定义格式化小数
	 * @param decimal 
	 * @param decimalLength 
	 * @return
	 */
	public static double formatDecimal(double decimal, int decimalLength) {
		 
		nf.setMaximumFractionDigits(decimalLength);
		 
		StringBuilder pattern = new StringBuilder("0.");
		for (int i = 0; i < decimalLength; i++) {
			pattern.append("0");
		}
		customDecimalFormat = new DecimalFormat(pattern.toString());
		return  Double.valueOf( customDecimalFormat.format( decimal) );
	}
	public static double formatDecimal(String decimal, int decimalLength) {
		return formatDecimal(Double.valueOf(decimal), decimalLength);
	}
}
