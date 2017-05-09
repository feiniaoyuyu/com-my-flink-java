package com.pateo.telematic.utils;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

 
public class TimeUtils implements Serializable{

	private static final long serialVersionUID = 1L;

	/**
	 * 默认的长时间格式 yyyy-MM-dd HH:mm:ss
	 */
	static SimpleDateFormat sdfLong = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	static DateFormat dateFormatLong = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	/**
	 * 短时间格式
	 */
	static SimpleDateFormat sdfShort = new SimpleDateFormat("yyyy-MM-dd");
	static DateFormat dateFormatShort = new SimpleDateFormat("yyyy-MM-dd");
	static DateFormat customdateFormatShort = null;


	 public static String clientTime2CreateTime(String date )  {
			    
	    if (StringUtils.isEmpty(date)|| !date.contains("-")) {
	      System.out.println("--------------err------------" + date) ;
	      return TimeUtils.getNowTime() ;
	    }else {
	    	return TimeUtils.addHour(date, 8);
	    }
//			
	  }
	/**
	 * 测试使用
	 * @param args
	 */
//	public static void main(String[] args) {
//		System.out.println("-------clientTime2CreateTime---" +clientTime2CreateTime("2016-08-04 02:00:00.0"));;
// 
//		System.out.println(dateConvertToMills("2016-08-04 02:00:00.0") / 1000);
//
//		System.out.println(secondsToDate(1470125039 + "", ""));
//
//		System.out.println("1464663461564 : " +millsToDate("1464663461564"));
//		System.out.println("1451577784 : " +secondsToDate("1451577784", ""));
//
// 		System.out.println("addHour-----------"+addHour("2016-08-05 10:00:00", 3));
// 		dateConvertToMills("2016-08-05 12:00:00");
// 		String string = "id,  obd_id ,  score_date ,  score ,  level ,  cid,shid, description ,  create_time ";
// 	//  id 编号,obd_id obd号,score_date 创建时间,score 得分,level 等级,cid 评语编号,shid 分享话术编号,description 描述,create_time 创建时间',
//		System.out.println("getNowTime---------"+getNowTime());
//
//		System.out.println("validateWeekend 2016-09-18 23:00:00 "+validateWeekend("2016-09-18 23:00:00"));
//		
//		Date date =new Date();
//		System.out.println("add day ------------"  +TimeUtil.addDay(TimeUtil.getDayOfToday(), -1)  );
//		System.out.println("---" +TimeUtil.addHour(TimeUtil.getNowTime(), -24).substring(0, 10) );
//	}

//	public static boolean validateTime(String time) {
//		// String[] split = time.split(" ");
//		// 54000
//		// -21600
//		return (hourConvertToSeconds(time.split(" ")[1]) > 5400 || hourConvertToSeconds(time
//				.split(" ")[1]) < -21600) ? true : false;
//	}

	/**
	 * @return 当前的时间
	 */
	public static String getNowTime() {
		return sdfLong.format(new Date());
	}
	public static String getDayOfToday() {
		return sdfShort.format(new Date());
	}

	/**
	 * data 格式为  yyyy-MM-dd
	 * @param date
	 * @return
	 */
	public static boolean validateWeekend(String date) {
		
 		Date bdate = null;
		try {
			if (date.length() > 10) {
				bdate = dateFormatLong.parse(date);
			}else if (date.length() == 10) {
				bdate = dateFormatShort.parse(date);
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		Calendar cal = Calendar.getInstance();
		cal.setTime(bdate);
		
		System.out.println(cal.get(Calendar.DAY_OF_WEEK));
		
		if (cal.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY
		 || cal.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) {
			return true;
		} else {
			return  false;
		}
	}
	/**
	 * 校验传递过来的格林尼治时间在 东八区 是否为周末
	 * @param date 格林尼治时间
	 * @return true  周末  返回以东八区为参考的时间是否为周末
	 */
	public static boolean validateWeekendWithGreenZone(String greenDate) {
 
		return validateWeekend(addHour(greenDate, 8));
	}

	public static String addHour(String dateTime ,int hours) {
		Date date=null;
		try {
			date = sdfLong.parse(dateTime);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		Calendar ca=Calendar.getInstance();
		ca.setTime(date);
		ca.add(Calendar.HOUR_OF_DAY, hours);
		return sdfLong.format(ca.getTime());
	}

	/**
	 * 传递的 date 格式为 yyyy-MM-dd
	 * @param dateTime
	 * @param days
	 * @return
	 */
	public static String addDay(String dateTime ,int days) {
		Date date=null;
		try {
			date = sdfShort.parse(dateTime);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		Calendar ca=Calendar.getInstance();
		ca.setTime(date);
		ca.add(Calendar.DATE, days);
		return sdfShort.format(ca.getTime());
	}
	
	/**
	 * 把时间转换为毫秒
	 * 
	 * @param date   需要转换的时间 比如 2016-01-01 00:03:04
	 * @param format 传递的时间的格式 需要和传递的时间对应 yyyy-MM-dd HH:mm:ss
	 * @return
	 */
	private static long dateConvertToMills(String date, String format) {

		Calendar c = Calendar.getInstance();
		try {
			c.setTime(new SimpleDateFormat(format).parse(date));
		} catch (java.text.ParseException e) {
			e.printStackTrace();
		}
		return c.getTimeInMillis();
	}

	public static long hourConvertToSeconds(String hourTime) {

		Calendar c = Calendar.getInstance();
		try {
			c.setTime(new SimpleDateFormat("HH:mm:ss").parse(hourTime));
		} catch (java.text.ParseException e) {
			e.printStackTrace();
		}

		return c.getTimeInMillis() / 1000;
	}

	/**
	 * @param dateTime
	 *            日期的格式為yyyy-MM-dd HH:mm:ss
	 *            如果格式和默认的不一致，采用dateConvertToMills(String date, String format)
	 * @return mills 返回毫秒
	 */
	public static long dateConvertToMills(String dateTime) {

		Calendar c = Calendar.getInstance();
		try {
			c.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
					.parse(dateTime));
		} catch (java.text.ParseException e) {
			e.printStackTrace();
		}

		return c.getTimeInMillis();
	}

	/**
	 * @param dateTime
	 *            日期的格式為yyyy-MM-dd HH:mm:ss
	 *            如果格式和默认的不一致，采用dateConvertToMills(String date, String format)
	 * @return seconds 返回秒
	 */
	public static long dateConvertToSeconds(String dateTime) {

		Calendar c = Calendar.getInstance();
		try {
			c.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
					.parse(dateTime));
		} catch (java.text.ParseException e) {
			e.printStackTrace();
		}
		return c.getTimeInMillis() / 1000;
	}

	public static long dateConvertToSeconds(String dateTime, String format) {

		Calendar c = Calendar.getInstance();
		if (format == null || format.length() == 0) {
			format = "yyyy-MM-dd HH:mm:ss";
		}
		try {
			c.setTime(new SimpleDateFormat("format").parse(dateTime));
		} catch (java.text.ParseException e) {

			e.printStackTrace();
		}
		return c.getTimeInMillis() / 1000;
	}

	/**
	 * 将毫秒数转化为时间
	 */
	public static String millsToDate(String mills) {

		Date date = new Date(Long.parseLong(mills));
		return sdfLong.format(date);
	}

	/**
	 * 把毫秒时间转换为日期
	 * 
	 * @param mills
	 *            需要转换的毫秒
	 * @param dateFormat
	 *            期望的日期格式 ，默认为 yyyy-MM-dd HH:mm:ss
	 * @return 得到毫秒相应的日期
	 */
	public static String millsToDate(String mills, String dateFormat) {

		if (dateFormat == null || dateFormat.length() == 0) {
			dateFormat = "yyyy-MM-dd HH:mm:ss";
		}
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		Date date = new Date(Long.parseLong(mills));
		return sdf.format(date);
	}

	/**
	 * 把秒时间转换为日期
	 * 
	 * @param seconds
	 *            需要转换的毫秒
	 * @param dateFormat
	 *            期望的日期格式 ，默认为 yyyy-MM-dd HH:mm:ss
	 * @return 得到秒相应的日期
	 */
	public static String secondsToDate(String seconds, String dateFormat) {

		if (Integer.valueOf(seconds) < 0) {
			return "时间有误";
		}
		if (dateFormat == null || dateFormat.length() == 0) {
			dateFormat = "yyyy-MM-dd HH:mm:ss";
		}
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		Date date = new Date(
				Long.parseLong((Long.valueOf(seconds) * 1000 + "")));
		return sdf.format(date);
	}
	
	public static String formateTimeToSeq( String dateStr) {
		
		String[] substring = dateStr.substring(11,16).split(":");
		if ( Integer.valueOf(substring[1])<30) {
			return substring[0]+":00-"+substring[0]+":30";
	      }else {
//	    	  23:30-00:00
	    	  if (Integer.valueOf(substring[0]) == 23) {
	    		  return substring[0]+":30-"+ ("00")+":00";
			} else {
				return  substring[0]+":30-"+(Integer.valueOf(substring[0]) + 1)+":00";
			}
	      }
	}  
}
